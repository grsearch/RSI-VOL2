'use strict';
// src/monitor.js — 核心监控引擎 V2
//
// 新增：
//   1. 量能过滤（VOL_MULT / VOL_BUY_RATIO / VOL_EXIT_CONSECUTIVE）
//   2. DRY_RUN 空跑模式（不实际交易，用 Birdeye 价格模拟盈亏）
//   3. 数据持久化（tick / trade / signal 写入磁盘）
//   4. 15秒K线 + RSI(7)

const EventEmitter = require('events');
const { evaluateSignal, buildCandles } = require('./rsi');
const trader    = require('./trader');
const birdeye   = require('./birdeye');
const logger    = require('./logger');
const wsHub     = require('./wsHub');
const dataStore = require('./dataStore');
const heliusWs  = require('./heliusWs');

const MONITOR_MINUTES = parseInt(process.env.TOKEN_MAX_AGE_MINUTES || '15', 10);
const FDV_EXIT        = parseFloat(process.env.FDV_EXIT_USD        || '10000');
const POLL_SEC        = parseInt(process.env.PRICE_POLL_SEC        || '1',  10);
const KLINE_SEC       = parseInt(process.env.KLINE_INTERVAL_SEC    || '15', 10);
const DRY_RUN         = (process.env.DRY_RUN || 'false') === 'true';
const TRADE_SOL       = parseFloat(process.env.TRADE_SIZE_SOL      || '0.2');

// 全局交易记录
const _allTradeRecords = [];

// 启动时加载持久化的交易记录
function _loadPersistedTrades() {
  try {
    const trades = dataStore.loadTrades();
    const cutoff = Date.now() - 24 * 3600 * 1000;
    trades.filter(r => r.buyAt > cutoff).forEach(r => _allTradeRecords.push(r));
    if (_allTradeRecords.length > 0) {
      logger.info('[Monitor] 从磁盘加载了 %d 条交易记录', _allTradeRecords.length);
    }
  } catch (_) {}
}

class TokenMonitor extends EventEmitter {
  constructor() {
    super();
    this._tokens   = new Map();
    this._pollTimer = null;
    this._started  = false;
  }

  start() {
    if (this._started) return;
    this._started = true;

    // 初始化数据存储
    dataStore.init();
    _loadPersistedTrades();
    dataStore.startFlush();

    // 启动 Helius WebSocket（链上交易数据）
    heliusWs.start();

    this._scheduleNextPoll();
    logger.info('[Monitor] 启动 | 轮询=%ds K线=%ds DRY_RUN=%s HeliusWS=%s',
      POLL_SEC, KLINE_SEC, DRY_RUN, heliusWs.isConnected() ? '已连接' : '连接中');
  }

  stop() {
    this._started = false;
    if (this._pollTimer) { clearTimeout(this._pollTimer); this._pollTimer = null; }
    dataStore.stopFlush();
    heliusWs.stop();
  }

  addToken(address, symbol, meta = {}) {
    if (this._tokens.has(address)) {
      logger.warn('[Monitor] %s 已在监控中，忽略', symbol);
      return false;
    }

    const now = Date.now();
    const state = {
      address,
      symbol,
      meta,
      fdv           : meta.fdv ?? null,
      lp            : meta.lp  ?? null,
      addedAt       : now,
      expiresAt     : now + MONITOR_MINUTES * 60 * 1000,
      ticks         : [],
      inPosition    : false,
      position      : null,
      tradeCount    : 0,
      shouldExit    : false,
      exitSent      : false,
      tradeLogs     : [],
      tradeRecords  : [],
      _prevRsiRealtime: NaN,
      _prevRsiTs      : 0,
      _lastBuyCandle  : -1,
      _lastSellCandle : -1,
    };

    this._tokens.set(address, state);

    // 订阅链上交易数据（Helius WebSocket）
    heliusWs.subscribe(address, symbol, (trade) => {
      this._onChainTrade(address, trade);
    });

    logger.info('[Monitor] ➕ 开始监控 %s (%s)，到期 %s | DRY_RUN=%s',
      symbol, address,
      new Date(state.expiresAt).toLocaleTimeString(),
      DRY_RUN);
    this._broadcastTokenList();
    return true;
  }

  async removeToken(address, reason = 'manual') {
    const state = this._tokens.get(address);
    if (!state) return;

    logger.info('[Monitor] ➖ 移除 %s，原因: %s', state.symbol, reason);

    if (state.inPosition && !state.exitSent) {
      logger.info('[Monitor] 📤 持仓中，先执行卖出...');
      await this._doSellExit(state, `FORCED_EXIT(${reason})`);
    }

    // 刷盘该 token 的 tick 数据
    dataStore.flushTicks();

    // 取消链上交易订阅
    heliusWs.unsubscribe(address);

    this._tokens.delete(address);
    birdeye.clearCache(address);
    this._broadcastTokenList();
  }

  getTokens() {
    return Array.from(this._tokens.values()).map(s => this._stateSnapshot(s));
  }

  getToken(address) {
    const s = this._tokens.get(address);
    return s ? this._stateSnapshot(s) : null;
  }

  // ── Helius 链上交易回调 ──────────────────────────────────────

  _onChainTrade(address, trade) {
    const state = this._tokens.get(address);
    if (!state || state.exitSent) return;

    const now = Date.now();
    // 把链上交易数据注入 tick 流（带 solAmount 和 isBuy）
    const tick = {
      price: trade.priceSol,  // SOL 计价的 token 价格
      ts: trade.ts || now,
      solAmount: trade.solAmount,  // 这笔交易的 SOL 成交额
      isBuy: trade.isBuy,         // 是否为买入
    };

    state.ticks.push(tick);

    // 持久化带链上数据的 tick
    dataStore.appendTick(address, {
      ...tick,
      symbol: state.symbol,
      signature: trade.signature,
      owner: trade.owner,
    });

    logger.debug('[HeliusTrade] %s %s %.4f SOL @ %.10f (%s)',
      state.symbol,
      trade.isBuy ? 'BUY' : 'SELL',
      trade.solAmount,
      trade.priceSol,
      trade.signature?.slice(0, 12) || '?');
  }

  // ── 主轮询 ──────────────────────────────────────────────────────

  _scheduleNextPoll() {
    if (!this._started) return;
    this._pollTimer = setTimeout(() => this._poll(), POLL_SEC * 1000);
  }

  async _poll() {
    const now = Date.now();
    const addresses = Array.from(this._tokens.keys());
    await Promise.allSettled(addresses.map(addr => this._pollOne(addr, now)));
    this._scheduleNextPoll();
  }

  async _pollOne(address, now) {
    const state = this._tokens.get(address);
    if (!state || state.exitSent) return;

    // 1. 到期检查
    if (now >= state.expiresAt) {
      await this.removeToken(address, 'EXPIRED');
      return;
    }

    // 2. 拉取价格
    let price;
    try {
      price = await birdeye.getPrice(address);
    } catch (err) {
      logger.warn('[Monitor] %s 价格拉取失败: %s', state.symbol, err.message);
      return;
    }

    // 3. FDV 检查
    const fdv = await birdeye.getFdv(address);
    if (fdv !== null && fdv !== undefined && Number.isFinite(fdv) && fdv < FDV_EXIT) {
      logger.warn('[Monitor] %s FDV=$%s < $%s，退出', state.symbol, fdv, FDV_EXIT);
      await this.removeToken(address, `FDV_TOO_LOW($${Math.round(fdv)})`);
      return;
    }

    // 4. 记录 tick（增强：含可选的交易方向和金额）
    const tick = { price, ts: now };
    state.ticks.push(tick);

    // 持久化 tick
    dataStore.appendTick(address, {
      price,
      ts: now,
      symbol: state.symbol,
    });

    // 只保留最近 30 分钟的 ticks
    const cutoff = now - 30 * 60 * 1000;
    while (state.ticks.length > 0 && state.ticks[0].ts < cutoff) state.ticks.shift();

    // 5. 聚合K线
    const { closed: closedCandles, current: currentCandle } = buildCandles(state.ticks, KLINE_SEC);

    // 6. RSI + 量能信号评估
    const realtimePrice = currentCandle ? currentCandle.close : price;
    const { rsi, prevRsi, signal, reason, volume } = evaluateSignal(closedCandles, realtimePrice, state);

    // 7. 记录信号（所有信号包括被过滤的）
    if (reason && reason !== '' && reason !== 'rsi_rebase') {
      dataStore.appendSignal({
        ts: now,
        address,
        symbol: state.symbol,
        price,
        rsi: Number.isFinite(rsi) ? parseFloat(rsi.toFixed(2)) : null,
        prevRsi: Number.isFinite(prevRsi) ? parseFloat(prevRsi.toFixed(2)) : null,
        signal,
        reason,
        volume,
        inPosition: state.inPosition,
      });
    }

    // 8. 广播实时数据
    wsHub.broadcast({
      type    : 'tick',
      address,
      symbol  : state.symbol,
      price,
      fdv,
      rsi     : Number.isFinite(rsi) ? parseFloat(rsi.toFixed(2)) : null,
      prevRsi : Number.isFinite(prevRsi) ? parseFloat(prevRsi.toFixed(2)) : null,
      signal,
      reason,
      closedCount: closedCandles.length,
      inPosition : state.inPosition,
      volume,
      dryRun  : DRY_RUN,
      ts      : now,
    });

    logger.debug('[RSI] %s price=%.6f rsi=%.2f prev=%.2f signal=%s reason=%s vol=%s',
      state.symbol, price, rsi, prevRsi, signal || 'none', reason,
      volume ? `${(volume.volMult || 0).toFixed(1)}x` : '-');

    // 9. 执行信号
    if (signal === 'BUY' && !state.inPosition && !state.shouldExit) {
      await this._doBuy(state, price, reason);
    } else if (signal === 'SELL' && state.inPosition) {
      await this._doSellExit(state, reason);
    }
  }

  // ── 交易执行 ────────────────────────────────────────────────────

  async _doBuy(state, price, reason) {
    logger.info('[Monitor] 🟢 BUY %s @ %.8f | %s | DRY_RUN=%s', state.symbol, price, reason, DRY_RUN);
    state.inPosition = true;

    if (DRY_RUN) {
      // 空跑模式：模拟买入
      const simulatedTokens = Math.floor(TRADE_SOL / price * 1e9); // 模拟 token 数量
      state.position = {
        entryPriceUsd : price,
        amountToken   : simulatedTokens,
        solIn         : TRADE_SOL,
        buyTxid       : `DRY_${Date.now()}`,
        buyTime       : Date.now(),
      };
      state.tradeCount++;
      this._addTradeLog(state, { type: 'BUY', symbol: state.symbol, price, reason, txid: state.position.buyTxid, solIn: TRADE_SOL, dryRun: true });
      this._createTradeRecord(state);
      logger.info('[Monitor] ✅ DRY_RUN BUY 模拟成功 %s @ %.8f  solIn=%.4f', state.symbol, price, TRADE_SOL);
    } else {
      try {
        const result = await trader.buy(state.address, state.symbol);
        state.position = {
          entryPriceUsd : price,
          amountToken   : result.amountOut,
          solIn         : result.solIn,
          buyTxid       : result.txid,
          buyTime       : Date.now(),
        };
        state.tradeCount++;
        this._addTradeLog(state, { type: 'BUY', symbol: state.symbol, price, reason, txid: result.txid, solIn: result.solIn });
        this._createTradeRecord(state);
        logger.info('[Monitor] ✅ BUY 成功 %s  solIn=%.4f SOL  txid=%s', state.symbol, result.solIn, result.txid);
      } catch (err) {
        logger.error('[Monitor] ❌ BUY 失败 %s: %s', state.symbol, err.message);
        state.inPosition = false;
      }
    }
  }

  async _doSellExit(state, reason) {
    if (state.exitSent) return;
    state.exitSent = true;

    logger.info('[Monitor] 🔴 SELL %s | %s | DRY_RUN=%s', state.symbol, reason, DRY_RUN);

    if (DRY_RUN) {
      // 空跑模式：用当前价格计算模拟盈亏
      let currentPrice;
      try {
        currentPrice = await birdeye.getPrice(state.address);
      } catch (_) {
        // 用最后一个 tick 的价格
        currentPrice = state.ticks.length > 0 ? state.ticks[state.ticks.length - 1].price : state.position?.entryPriceUsd || 0;
      }

      const solIn  = state.position?.solIn ?? TRADE_SOL;
      const entryP = state.position?.entryPriceUsd ?? 0;
      const solOut = entryP > 0 ? solIn * (currentPrice / entryP) : 0;
      const pnlPct = entryP > 0 ? (currentPrice - entryP) / entryP * 100 : 0;
      const pnlSol = solOut - solIn;

      state.inPosition = false;
      this._addTradeLog(state, { type: 'SELL', symbol: state.symbol, reason, txid: `DRY_${Date.now()}`, solOut, pnlSol, dryRun: true });
      this._finalizeTradeRecord(state, reason, solOut, pnlPct);

      logger.info('[Monitor] ✅ DRY_RUN SELL %s  solIn=%.4f  solOut=%.4f  pnl=%+.4f SOL (%+.1f%%)',
        state.symbol, solIn, solOut, pnlSol, pnlPct);
    } else {
      try {
        const result = await trader.sell(state.address, state.symbol, state.position);
        const solOut  = result.solOut ?? 0;
        const solIn   = state.position?.solIn ?? TRADE_SOL;
        const pnlPct  = solIn > 0 ? (solOut - solIn) / solIn * 100 : 0;
        const pnlSol  = solOut - solIn;

        state.inPosition = false;
        this._addTradeLog(state, { type: 'SELL', symbol: state.symbol, reason, txid: result.txid, solOut, pnlSol });
        this._finalizeTradeRecord(state, reason, solOut, pnlPct);

        logger.info('[Monitor] ✅ SELL 成功 %s  solIn=%.4f  solOut=%.4f  pnl=%+.4f SOL (%+.1f%%)  txid=%s',
          state.symbol, solIn, solOut, pnlSol, pnlPct, result.txid);
      } catch (err) {
        logger.error('[Monitor] ❌ SELL 失败 %s: %s', state.symbol, err.message);
        this._finalizeTradeRecord(state, `SELL_FAILED(${reason})`, 0, -100);
      }
    }

    logger.info('[Monitor] 🏁 %s 第%d笔完成，5s后退出监控', state.symbol, state.tradeCount);
    state.shouldExit = true;
    setTimeout(() => this.removeToken(state.address, 'TRADE_DONE'), 5000);
  }

  // ── 辅助工具 ────────────────────────────────────────────────────

  _addTradeLog(state, log) {
    state.tradeLogs.push({ ...log, ts: Date.now() });
    if (state.tradeLogs.length > 200) state.tradeLogs.shift();
    wsHub.broadcast({ type: 'trade_log', ...log, ts: Date.now() });
    this.emit('trade', log);
  }

  _createTradeRecord(state) {
    if (!state.position) return;
    const rec = {
      id         : `${state.address}_${state.tradeCount}_${Date.now()}`,
      address    : state.address,
      symbol     : state.symbol,
      buyAt      : state.position.buyTime,
      buyTxid    : state.position.buyTxid,
      entryPrice : state.position.entryPriceUsd,
      entryFdv   : state.fdv,
      entryLp    : state.lp,
      solIn      : state.position.solIn,
      dryRun     : DRY_RUN,
      exitAt     : null,
      exitReason : null,
      solOut     : null,
      pnlPct     : null,
      pnlSol     : null,
    };
    state.tradeRecords.push(rec);
    _allTradeRecords.unshift(rec);

    // 持久化
    dataStore.appendTrade(rec);

    // 只保留 24h 内
    const cutoff = Date.now() - 24 * 3600 * 1000;
    while (_allTradeRecords.length && _allTradeRecords[_allTradeRecords.length - 1].buyAt < cutoff) {
      _allTradeRecords.pop();
    }
    wsHub.broadcast({ type: 'trade_record', ...rec });
  }

  _finalizeTradeRecord(state, reason, solOut, pnlPct) {
    const rec = state.tradeRecords[state.tradeRecords.length - 1];
    if (!rec) return;
    rec.exitAt     = Date.now();
    rec.exitReason = reason;
    rec.solOut     = parseFloat(solOut.toFixed(6));
    rec.pnlPct     = parseFloat(pnlPct.toFixed(2));
    rec.pnlSol     = parseFloat((solOut - (state.position?.solIn ?? 0)).toFixed(6));

    // 更新持久化
    dataStore.updateTrade(rec.id, {
      exitAt: rec.exitAt,
      exitReason: rec.exitReason,
      solOut: rec.solOut,
      pnlPct: rec.pnlPct,
      pnlSol: rec.pnlSol,
    });

    wsHub.broadcast({ type: 'trade_record', ...rec });
  }

  _stateSnapshot(state) {
    return {
      address    : state.address,
      symbol     : state.symbol,
      addedAt    : state.addedAt,
      expiresAt  : state.expiresAt,
      inPosition : state.inPosition,
      tradeCount : state.tradeCount,
      shouldExit : state.shouldExit,
      tradeLogs  : state.tradeLogs,
      tradeRecords: state.tradeRecords,
      dryRun     : DRY_RUN,
    };
  }

  _broadcastTokenList() {
    wsHub.broadcast({ type: 'token_list', tokens: this.getTokens() });
  }
}

function getAllTradeRecords() {
  // 合并内存和磁盘数据
  const cutoff = Date.now() - 24 * 3600 * 1000;
  const memRecords = _allTradeRecords.filter(r => r.buyAt > cutoff);

  // 如果内存为空，从磁盘加载
  if (memRecords.length === 0) {
    const diskRecords = dataStore.loadTrades().filter(r => r.buyAt > cutoff);
    return diskRecords;
  }
  return memRecords;
}

const monitor = new TokenMonitor();
module.exports = monitor;
module.exports.getAllTradeRecords = getAllTradeRecords;
module.exports.DRY_RUN = DRY_RUN;
