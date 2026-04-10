'use strict';
// src/monitor.js — 核心监控引擎 V3
//
// V3 修复与改进：
//   1. 修复致命 BUG：链上交易 tick（SOL 计价）不再混入价格 tick（USD 计价）
//      - 价格 tick: { price, ts, source: 'price' }
//      - 链上 tick: { price: priceSol, ts, solAmount, isBuy, source: 'chain' }
//   2. Birdeye WS 实时价格推送驱动止损（延迟 <150ms）
//   3. 止损快速路径：价格回调 → checkStopLoss → 立即 sell（不等轮询）
//   4. 轮询周期可以放宽到 5s（只做 FDV 检查 + RSI 信号），实际止损由 WS 驱动

const EventEmitter = require('events');
const { evaluateSignal, buildCandles, filterValidCandles, checkStopLoss } = require('./rsi');
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
    this._tokens    = new Map();
    this._pollTimer = null;
    this._started   = false;
    // 止损锁：防止同一 token 并发触发多次止损
    this._stopLossLocks = new Set();
  }

  start() {
    if (this._started) return;
    this._started = true;

    dataStore.init();
    _loadPersistedTrades();
    dataStore.startFlush();

    // 启动 Birdeye WebSocket 价格流
    birdeye.priceStream.start();

    // 启动 Helius WebSocket（链上交易数据）
    heliusWs.start();

    this._scheduleNextPoll();
    logger.info('[Monitor] 启动 | 轮询=%ds K线=%ds DRY_RUN=%s',
      POLL_SEC, KLINE_SEC, DRY_RUN);
    logger.info('[Monitor]   BirdeyeWS=%s  HeliusWS=%s',
      birdeye.priceStream.isConnected() ? '已连接' : '连接中',
      heliusWs.isConnected() ? '已连接' : '连接中');
  }

  stop() {
    this._started = false;
    if (this._pollTimer) { clearTimeout(this._pollTimer); this._pollTimer = null; }
    birdeye.priceStream.stop();
    heliusWs.stop();
    dataStore.stopFlush();
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
      fdv            : meta.fdv ?? null,
      lp             : meta.lp  ?? null,
      addedAt        : now,
      expiresAt      : now + MONITOR_MINUTES * 60 * 1000,
      ticks          : [],     // 混合 tick 数组（带 source 标记）
      inPosition     : false,
      position       : null,
      tradeCount     : 0,
      shouldExit     : false,
      exitSent       : false,
      tradeLogs      : [],
      tradeRecords   : [],
      _prevRsiRealtime: NaN,
      _prevRsiTs      : 0,
      _lastBuyCandle  : -1,
      _lastSellCandle : -1,
      _lastPriceUsd   : null,  // 最新 USD 价格（Birdeye WS 推送）
      _lastPriceTs    : 0,
    };

    this._tokens.set(address, state);

    // 订阅 Birdeye WS 实时价格（USD，用于 RSI + 止损）
    birdeye.priceStream.subscribe(address, (price, ts, ohlcv) => {
      this._onBirdeyePrice(address, price, ts);
    });

    // 订阅 Helius WS 链上交易数据（SOL，用于量能分析）
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

    dataStore.flushTicks();

    // 取消订阅
    birdeye.priceStream.unsubscribe(address);
    heliusWs.unsubscribe(address);

    this._tokens.delete(address);
    this._stopLossLocks.delete(address);
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

  // ── Birdeye WS 实时价格回调（<150ms 延迟） ─────────────────────

  _onBirdeyePrice(address, price, ts) {
    const state = this._tokens.get(address);
    if (!state || state.exitSent) return;

    // 更新最新价格
    state._lastPriceUsd = price;
    state._lastPriceTs  = ts;

    // 记录价格 tick（source: 'price'，USD 计价）
    const tick = { price, ts, source: 'price' };
    state.ticks.push(tick);

    // 持久化
    dataStore.appendTick(address, {
      price, ts, source: 'price', symbol: state.symbol,
    });

    // ★ 快速止损检查（每个价格 tick 都检查，不等轮询）
    if (state.inPosition && !this._stopLossLocks.has(address)) {
      const sl = checkStopLoss(price, state);
      if (sl.shouldExit) {
        logger.info('[Monitor] ⚡ 快速止损触发 %s @ %.8f | %s', state.symbol, price, sl.reason);
        this._stopLossLocks.add(address);
        // 异步执行止损，不阻塞 WS 回调
        this._doSellExit(state, sl.reason).catch(err => {
          logger.error('[Monitor] 快速止损执行失败 %s: %s', state.symbol, err.message);
          this._stopLossLocks.delete(address);
        });
      }
    }
  }

  // ── Helius 链上交易回调 ──────────────────────────────────────

  _onChainTrade(address, trade) {
    const state = this._tokens.get(address);
    if (!state || state.exitSent) return;

    const now = Date.now();

    // 记录链上交易 tick（source: 'chain'，SOL 计价）
    // ★ 关键修复：标记 source='chain'，buildCandles 中只用于 volume，不用于 OHLC
    const tick = {
      price:     trade.priceSol,   // SOL 计价（不会混入 RSI 的 OHLC）
      ts:        trade.ts || now,
      solAmount: trade.solAmount,
      isBuy:     trade.isBuy,
      source:    'chain',          // ★ 关键标记
    };

    state.ticks.push(tick);

    // 持久化
    dataStore.appendTick(address, {
      ...tick,
      symbol:    state.symbol,
      signature: trade.signature,
      owner:     trade.owner,
    });

    logger.debug('[HeliusTrade] %s %s %.4f SOL @ %.10f (%s)',
      state.symbol,
      trade.isBuy ? 'BUY' : 'SELL',
      trade.solAmount,
      trade.priceSol,
      trade.signature?.slice(0, 12) || '?');
  }

  // ── 主轮询（RSI 信号 + FDV 检查 + 过期检查） ────────────────────

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

    // 2. 获取价格（优先 WS 缓存，降级 HTTP）
    let price;
    try {
      price = await birdeye.getPrice(address);
    } catch (err) {
      logger.warn('[Monitor] %s 价格拉取失败: %s', state.symbol, err.message);
      return;
    }

    // 如果 WS 价格不可用，用 HTTP 价格补一个 tick
    if (!state._lastPriceUsd || now - state._lastPriceTs > 5000) {
      const tick = { price, ts: now, source: 'price' };
      state.ticks.push(tick);
      state._lastPriceUsd = price;
      state._lastPriceTs  = now;

      dataStore.appendTick(address, { price, ts: now, source: 'price', symbol: state.symbol });
    }

    // 3. FDV 检查
    const fdv = await birdeye.getFdv(address);
    if (fdv !== null && fdv !== undefined && Number.isFinite(fdv) && fdv < FDV_EXIT) {
      logger.warn('[Monitor] %s FDV=$%s < $%s，退出', state.symbol, fdv, FDV_EXIT);
      await this.removeToken(address, `FDV_TOO_LOW($${Math.round(fdv)})`);
      return;
    }

    // 4. 裁剪 ticks（保留最近 30 分钟）
    const cutoff = now - 30 * 60 * 1000;
    while (state.ticks.length > 0 && state.ticks[0].ts < cutoff) state.ticks.shift();

    // 5. 聚合 K 线
    const { closed: rawClosedCandles, current: currentCandle } = buildCandles(state.ticks, KLINE_SEC);

    // ★ 过滤掉没有价格数据的 K 线（只有链上 tick 的 K 线 open 为 null）
    const closedCandles = filterValidCandles(rawClosedCandles);

    // 6. RSI + 量能信号评估
    const realtimePrice = currentCandle?.close ?? price;
    const { rsi, prevRsi, signal, reason, volume } = evaluateSignal(closedCandles, realtimePrice, state);

    // 7. 记录信号
    if (reason && reason !== '' && reason !== 'rsi_rebase') {
      dataStore.appendSignal({
        ts: now, address, symbol: state.symbol,
        price, rsi: Number.isFinite(rsi) ? parseFloat(rsi.toFixed(2)) : null,
        prevRsi: Number.isFinite(prevRsi) ? parseFloat(prevRsi.toFixed(2)) : null,
        signal, reason, volume, inPosition: state.inPosition,
      });
    }

    // 8. 广播实时数据
    wsHub.broadcast({
      type:        'tick',
      address,
      symbol:      state.symbol,
      price,
      fdv,
      rsi:         Number.isFinite(rsi) ? parseFloat(rsi.toFixed(2)) : null,
      prevRsi:     Number.isFinite(prevRsi) ? parseFloat(prevRsi.toFixed(2)) : null,
      signal,
      reason,
      closedCount: closedCandles.length,
      inPosition:  state.inPosition,
      volume,
      dryRun:      DRY_RUN,
      ts:          now,
      birdeyeWs:   birdeye.priceStream.isConnected(),
      heliusWs:    heliusWs.isConnected(),
      heliusStats: heliusWs.getStats(),
    });

    logger.debug('[RSI] %s price=%.6f rsi=%.2f prev=%.2f signal=%s reason=%s vol=%s',
      state.symbol, price, rsi, prevRsi, signal || 'none', reason,
      volume ? `buy=${(volume.buyVol||0).toFixed(2)}/sell=${(volume.sellVol||0).toFixed(2)}` : '-');

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
      const simulatedTokens = Math.floor(TRADE_SOL / price * 1e9);
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

    const isStopLoss = reason.includes('STOP_LOSS') || reason.includes('TAKE_PROFIT');
    logger.info('[Monitor] 🔴 SELL %s | %s | isStopLoss=%s | DRY_RUN=%s',
      state.symbol, reason, isStopLoss, DRY_RUN);

    if (DRY_RUN) {
      let currentPrice;
      try {
        currentPrice = await birdeye.getPrice(state.address);
      } catch (_) {
        currentPrice = state._lastPriceUsd || (state.ticks.length > 0 ? state.ticks[state.ticks.length - 1].price : state.position?.entryPriceUsd || 0);
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
        const result = await trader.sell(state.address, state.symbol, state.position, isStopLoss);
        const solOut  = result.solOut ?? 0;
        const solIn   = state.position?.solIn ?? TRADE_SOL;
        const pnlPct  = solIn > 0 ? (solOut - solIn) / solIn * 100 : 0;
        const pnlSol  = solOut - solIn;

        state.inPosition = false;
        this._addTradeLog(state, { type: 'SELL', symbol: state.symbol, reason, txid: result.txid, solOut, pnlSol, elapsedMs: result.elapsedMs });
        this._finalizeTradeRecord(state, reason, solOut, pnlPct);

        logger.info('[Monitor] ✅ SELL 成功 %s  solIn=%.4f  solOut=%.4f  pnl=%+.4f SOL (%+.1f%%)  耗时=%dms  txid=%s',
          state.symbol, solIn, solOut, pnlSol, pnlPct, result.elapsedMs || 0, result.txid);
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
      id:         `${state.address}_${state.tradeCount}_${Date.now()}`,
      address:    state.address,
      symbol:     state.symbol,
      buyAt:      state.position.buyTime,
      buyTxid:    state.position.buyTxid,
      entryPrice: state.position.entryPriceUsd,
      entryFdv:   state.fdv,
      entryLp:    state.lp,
      solIn:      state.position.solIn,
      dryRun:     DRY_RUN,
      exitAt:     null,
      exitReason: null,
      solOut:     null,
      pnlPct:    null,
      pnlSol:    null,
    };
    state.tradeRecords.push(rec);
    _allTradeRecords.unshift(rec);
    dataStore.appendTrade(rec);

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
    rec.pnlPct    = parseFloat(pnlPct.toFixed(2));
    rec.pnlSol    = parseFloat((solOut - (state.position?.solIn ?? 0)).toFixed(6));

    dataStore.updateTrade(rec.id, {
      exitAt:     rec.exitAt,
      exitReason: rec.exitReason,
      solOut:     rec.solOut,
      pnlPct:    rec.pnlPct,
      pnlSol:    rec.pnlSol,
    });

    wsHub.broadcast({ type: 'trade_record', ...rec });
  }

  _stateSnapshot(state) {
    return {
      address:      state.address,
      symbol:       state.symbol,
      addedAt:      state.addedAt,
      expiresAt:    state.expiresAt,
      inPosition:   state.inPosition,
      tradeCount:   state.tradeCount,
      shouldExit:   state.shouldExit,
      tradeLogs:    state.tradeLogs,
      tradeRecords: state.tradeRecords,
      dryRun:       DRY_RUN,
      lastPrice:    state._lastPriceUsd,
      lastPriceTs:  state._lastPriceTs,
    };
  }

  _broadcastTokenList() {
    wsHub.broadcast({ type: 'token_list', tokens: this.getTokens() });
  }
}

function getAllTradeRecords() {
  const cutoff = Date.now() - 24 * 3600 * 1000;
  const memRecords = _allTradeRecords.filter(r => r.buyAt > cutoff);
  if (memRecords.length === 0) {
    return dataStore.loadTrades().filter(r => r.buyAt > cutoff);
  }
  return memRecords;
}

const monitor = new TokenMonitor();
module.exports = monitor;
module.exports.getAllTradeRecords = getAllTradeRecords;
module.exports.DRY_RUN = DRY_RUN;
