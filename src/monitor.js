'use strict';
// src/monitor.js — 核心监控引擎 V3.2
//
// V3.2 改进：
//   1. 监控期 30 分钟（可配置），监控期内允许多次买卖
//   2. 卖出后不再退出监控，重置状态等待下一个买入信号
//   3. 卖出后有冷却期（SELL_COOLDOWN_SEC），防止同一信号反复触发
//   4. MAX_TRADES_PER_TOKEN 限制单个 token 最大交易次数
//   5. 到期时如仍持仓，强制卖出后移除
//
// 交易生命周期：
//   addToken → [BUY → SELL → 冷却 → BUY → SELL → ...] → EXPIRED/MAX_TRADES → removeToken

const EventEmitter = require('events');
const { evaluateSignal, buildCandles, filterValidCandles, checkStopLoss } = require('./rsi');
const trader    = require('./trader');
const birdeye   = require('./birdeye');
const logger    = require('./logger');
const wsHub     = require('./wsHub');
const dataStore = require('./dataStore');
const heliusWs  = require('./heliusWs');

const MONITOR_MINUTES   = parseInt(process.env.TOKEN_MAX_AGE_MINUTES || '30', 10);
const FDV_EXIT          = parseFloat(process.env.FDV_EXIT_USD        || '10000');
const POLL_SEC          = parseInt(process.env.PRICE_POLL_SEC        || '1',  10);
const KLINE_SEC         = parseInt(process.env.KLINE_INTERVAL_SEC    || '15', 10);
const DRY_RUN           = (process.env.DRY_RUN || 'false') === 'true';
const TRADE_SOL         = parseFloat(process.env.TRADE_SIZE_SOL      || '0.2');
const MAX_TRADES        = parseInt(process.env.MAX_TRADES_PER_TOKEN  || '5',  10);
const SELL_COOLDOWN_SEC = parseInt(process.env.SELL_COOLDOWN_SEC     || '30', 10);

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
    this._slPollTimer = null;  // 独立止损轮询
  }

  start() {
    if (this._started) return;
    this._started = true;

    dataStore.init();
    _loadPersistedTrades();
    dataStore.startFlush();

    birdeye.priceStream.start();
    heliusWs.start();

    this._scheduleNextPoll();
    this._startStopLossPoller();  // ★ 500ms 独立止损轮询
    logger.info('[Monitor] 启动 | 轮询=%ds K线=%ds 监控=%d分钟 最大交易=%d次 冷却=%ds DRY_RUN=%s',
      POLL_SEC, KLINE_SEC, MONITOR_MINUTES, MAX_TRADES, SELL_COOLDOWN_SEC, DRY_RUN);
    logger.info('[Monitor]   BirdeyeWS=%s  HeliusWS=%s  止损轮询=500ms',
      birdeye.priceStream.isConnected() ? '已连接' : '连接中',
      heliusWs.isConnected() ? '已连接' : '连接中');
  }

  stop() {
    this._started = false;
    if (this._pollTimer) { clearTimeout(this._pollTimer); this._pollTimer = null; }
    if (this._slPollTimer) { clearInterval(this._slPollTimer); this._slPollTimer = null; }
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
      fdv               : meta.fdv ?? null,
      lp                : meta.lp  ?? null,
      addedAt           : now,
      expiresAt         : now + MONITOR_MINUTES * 60 * 1000,
      ticks             : [],
      inPosition        : false,
      position          : null,
      tradeCount        : 0,       // 完成的买卖轮次数
      tradeLogs         : [],
      tradeRecords      : [],
      _prevRsiRealtime  : NaN,
      _prevRsiTs        : 0,
      _lastBuyCandle    : -1,
      _lastSellCandle   : -1,
      _lastPanicSellTs  : 0,       // RSI_PANIC 时间防抖（毫秒时间戳）
      _lastPriceUsd     : null,
      _lastPriceTs      : 0,
      // ★ 多次买卖相关
      _sellCooldownUntil: 0,       // 卖出后冷却到期时间戳
      _selling          : false,   // 正在执行卖出中（防并发）
    };

    this._tokens.set(address, state);

    birdeye.priceStream.subscribe(address, (price, ts, ohlcv) => {
      this._onBirdeyePrice(address, price, ts);
    });

    heliusWs.subscribe(address, symbol, (trade) => {
      this._onChainTrade(address, trade);
    });

    logger.info('[Monitor] ➕ 开始监控 %s (%s)，到期 %s | 最多%d笔 | DRY_RUN=%s',
      symbol, address,
      new Date(state.expiresAt).toLocaleTimeString(),
      MAX_TRADES, DRY_RUN);
    this._broadcastTokenList();
    return true;
  }

  async removeToken(address, reason = 'manual') {
    const state = this._tokens.get(address);
    if (!state) return;

    logger.info('[Monitor] ➖ 移除 %s，原因: %s (共完成%d笔交易)', state.symbol, reason, state.tradeCount);

    // 到期/手动移除时如仍持仓，强制卖出
    if (state.inPosition && !state._selling) {
      logger.info('[Monitor] 📤 持仓中，先执行卖出...');
      await this._doSell(state, `FORCED_EXIT(${reason})`);
    }

    dataStore.flushTicks();

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
    if (!state) return;

    state._lastPriceUsd = price;
    state._lastPriceTs  = ts;

    const tick = { price, ts, source: 'price' };
    state.ticks.push(tick);

    dataStore.appendTick(address, {
      price, ts, source: 'price', symbol: state.symbol,
    });

    // ★ 快速止损检查（持仓中 + 非冷却期 + 未在卖出中）
    if (state.inPosition && !state._selling && !this._stopLossLocks.has(address)) {
      const sl = checkStopLoss(price, state);
      if (sl.shouldExit) {
        logger.info('[Monitor] ⚡ 快速止损触发 %s @ %.8f | %s | 第%d笔',
          state.symbol, price, sl.reason, state.tradeCount + 1);
        this._stopLossLocks.add(address);
        this._doSell(state, sl.reason).catch(err => {
          logger.error('[Monitor] 快速止损执行失败 %s: %s', state.symbol, err.message);
        }).finally(() => {
          this._stopLossLocks.delete(address);
        });
      }
    }
  }

  // ── Helius 链上交易回调 ──────────────────────────────────────

  _onChainTrade(address, trade) {
    const state = this._tokens.get(address);
    if (!state) return;

    const now = Date.now();
    const tick = {
      price:     trade.priceSol,
      ts:        trade.ts || now,
      solAmount: trade.solAmount,
      isBuy:     trade.isBuy,
      source:    'chain',
    };

    state.ticks.push(tick);

    dataStore.appendTick(address, {
      ...tick,
      symbol:    state.symbol,
      signature: trade.signature,
      owner:     trade.owner,
    });

    // ★ 链上交易也触发止损检查（用链上价格 × SOL/USD 估算）
    // 链上交易比 Birdeye WS 更快到达，不浪费这个信号
    if (state.inPosition && !state._selling && !this._stopLossLocks.has(address)) {
      // 用最新的 Birdeye USD 价格做止损判断（链上 priceSol 单位不同，不能直接比）
      // 但如果有卖出交易且价格大幅下跌，说明市场在抛售
      const lastUsd = state._lastPriceUsd;
      if (lastUsd && trade.isBuy === false && trade.solAmount > 5) {
        // 大额卖出交易 → 触发紧急价格刷新
        this._urgentStopCheck(address, state);
      }
    }

    logger.debug('[HeliusTrade] %s %s %.4f SOL @ %.10f (%s)',
      state.symbol,
      trade.isBuy ? 'BUY' : 'SELL',
      trade.solAmount,
      trade.priceSol,
      trade.signature?.slice(0, 12) || '?');
  }

  // ── 紧急止损价格刷新（链上检测到大额卖出时触发）────────────
  async _urgentStopCheck(address, state) {
    if (state._selling || this._stopLossLocks.has(address)) return;
    try {
      // 绕过缓存直接拉最新价格
      const price = await birdeye.getPrice(address);
      if (!price || price <= 0) return;
      state._lastPriceUsd = price;
      state._lastPriceTs = Date.now();

      const sl = checkStopLoss(price, state);
      if (sl.shouldExit) {
        logger.info('[Monitor] ⚡ 链上大卖触发止损 %s @ %.8f | %s', state.symbol, price, sl.reason);
        this._stopLossLocks.add(address);
        this._doSell(state, sl.reason).catch(err => {
          logger.error('[Monitor] 紧急止损失败 %s: %s', state.symbol, err.message);
        }).finally(() => {
          this._stopLossLocks.delete(address);
        });
      }
    } catch (_) {}
  }

  // ── 独立止损轮询（每 500ms，不依赖 WS 推送） ─────────────────

  _startStopLossPoller() {
    if (this._slPollTimer) return;
    this._slPollTimer = setInterval(() => this._stopLossPoll(), 500);
  }

  async _stopLossPoll() {
    for (const [address, state] of this._tokens.entries()) {
      if (!state.inPosition || state._selling || this._stopLossLocks.has(address)) continue;

      try {
        const price = await birdeye.getPrice(address);
        if (!price || price <= 0) continue;

        state._lastPriceUsd = price;
        state._lastPriceTs = Date.now();

        const sl = checkStopLoss(price, state);
        if (sl.shouldExit) {
          const holdSec = state.position?.buyTime ? Math.round((Date.now() - state.position.buyTime) / 1000) : 0;
          logger.info('[Monitor] ⚡ 止损轮询触发 %s @ %.8f | %s | 持仓%ds',
            state.symbol, price, sl.reason, holdSec);
          this._stopLossLocks.add(address);
          this._doSell(state, sl.reason).catch(err => {
            logger.error('[Monitor] 止损执行失败 %s: %s', state.symbol, err.message);
          }).finally(() => {
            this._stopLossLocks.delete(address);
          });
        }
      } catch (_) {}
    }
  }

  // ── 主轮询 ────────────────────────────────────────────────────

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
    if (!state) return;

    // 正在卖出中，跳过此轮
    if (state._selling) return;

    // 1. 到期检查
    if (now >= state.expiresAt) {
      await this.removeToken(address, 'EXPIRED');
      return;
    }

    // 2. 最大交易次数检查（非持仓时检查，持仓中让它继续完成当前交易）
    if (state.tradeCount >= MAX_TRADES && !state.inPosition) {
      logger.info('[Monitor] %s 已达到最大交易次数 %d/%d，移除',
        state.symbol, state.tradeCount, MAX_TRADES);
      await this.removeToken(address, `MAX_TRADES(${state.tradeCount}/${MAX_TRADES})`);
      return;
    }

    // 3. 获取价格
    let price;
    try {
      price = await birdeye.getPrice(address);
    } catch (err) {
      logger.warn('[Monitor] %s 价格拉取失败: %s', state.symbol, err.message);
      return;
    }

    // WS 不可用时补 tick
    if (!state._lastPriceUsd || now - state._lastPriceTs > 5000) {
      const tick = { price, ts: now, source: 'price' };
      state.ticks.push(tick);
      state._lastPriceUsd = price;
      state._lastPriceTs  = now;
      dataStore.appendTick(address, { price, ts: now, source: 'price', symbol: state.symbol });
    }

    // 4. FDV 检查
    const fdv = await birdeye.getFdv(address);
    if (fdv !== null && fdv !== undefined && Number.isFinite(fdv) && fdv < FDV_EXIT) {
      logger.warn('[Monitor] %s FDV=$%s < $%s，退出', state.symbol, fdv, FDV_EXIT);
      await this.removeToken(address, `FDV_TOO_LOW($${Math.round(fdv)})`);
      return;
    }

    // 5. 裁剪 ticks（保留 45 分钟，大于监控期以留余量）
    const cutoff = now - 45 * 60 * 1000;
    while (state.ticks.length > 0 && state.ticks[0].ts < cutoff) state.ticks.shift();

    // 6. 聚合 K 线
    const { closed: rawClosedCandles, current: currentCandle } = buildCandles(state.ticks, KLINE_SEC);
    const closedCandles = filterValidCandles(rawClosedCandles);

    // 7. RSI + 量能信号评估
    const realtimePrice = currentCandle?.close ?? price;
    const { rsi, prevRsi, signal, reason, volume } = evaluateSignal(closedCandles, realtimePrice, state);

    // 8. 记录信号
    if (reason && reason !== '' && reason !== 'rsi_rebase') {
      dataStore.appendSignal({
        ts: now, address, symbol: state.symbol,
        price, rsi: Number.isFinite(rsi) ? parseFloat(rsi.toFixed(2)) : null,
        prevRsi: Number.isFinite(prevRsi) ? parseFloat(prevRsi.toFixed(2)) : null,
        signal, reason, volume, inPosition: state.inPosition,
        tradeCount: state.tradeCount,
      });
    }

    // 9. 广播实时数据
    wsHub.broadcast({
      type:        'tick',
      address,
      symbol:      state.symbol,
      price,
      fdv,
      lp:          state.lp,
      rsi:         Number.isFinite(rsi) ? parseFloat(rsi.toFixed(2)) : null,
      prevRsi:     Number.isFinite(prevRsi) ? parseFloat(prevRsi.toFixed(2)) : null,
      signal,
      reason,
      closedCount: closedCandles.length,
      inPosition:  state.inPosition,
      volume,
      tradeCount:  state.tradeCount,
      maxTrades:   MAX_TRADES,
      cooldown:    state._sellCooldownUntil > now ? Math.ceil((state._sellCooldownUntil - now) / 1000) : 0,
      dryRun:      DRY_RUN,
      ts:          now,
      birdeyeWs:   birdeye.priceStream.isConnected(),
      heliusWs:    heliusWs.isConnected(),
      heliusStats: heliusWs.getStats(),
    });

    logger.debug('[RSI] %s price=%.6f rsi=%.2f prev=%.2f signal=%s reason=%s trades=%d/%d inPos=%s cool=%ds',
      state.symbol, price, rsi, prevRsi, signal || 'none', reason,
      state.tradeCount, MAX_TRADES, state.inPosition,
      state._sellCooldownUntil > now ? Math.ceil((state._sellCooldownUntil - now) / 1000) : 0);

    // 10. 执行信号
    if (signal === 'BUY' && !state.inPosition && this._canBuy(state, now)) {
      // ★ 买入前强制刷新 FDV 检查
      const freshFdv = await birdeye.getFdv(address);
      if (freshFdv !== null && Number.isFinite(freshFdv) && freshFdv < FDV_EXIT) {
        logger.warn('[Monitor] %s 买入被拒: FDV=$%d < $%d', state.symbol, Math.round(freshFdv), FDV_EXIT);
      } else {
        state.fdv = freshFdv ?? state.fdv;  // 更新最新 FDV
        await this._doBuy(state, price, reason);
      }
    } else if (signal === 'SELL' && state.inPosition && !state._selling) {
      await this._doSell(state, reason);
    }
  }

  // ── 是否可以买入 ────────────────────────────────────────────────

  _canBuy(state, now) {
    // 已在持仓中
    if (state.inPosition) return false;
    // 正在卖出中
    if (state._selling) return false;
    // 已达最大交易次数
    if (state.tradeCount >= MAX_TRADES) return false;
    // 冷却期中
    if (now < state._sellCooldownUntil) {
      logger.debug('[Monitor] %s 冷却中，还剩 %ds',
        state.symbol, Math.ceil((state._sellCooldownUntil - now) / 1000));
      return false;
    }
    return true;
  }

  // ── 买入 ────────────────────────────────────────────────────────

  async _doBuy(state, price, reason) {
    const tradeNum = state.tradeCount + 1;
    logger.info('[Monitor] 🟢 BUY #%d %s @ %.8f | %s | DRY_RUN=%s',
      tradeNum, state.symbol, price, reason, DRY_RUN);
    state.inPosition = true;

    if (DRY_RUN) {
      const simulatedTokens = Math.floor(TRADE_SOL / price * 1e9);
      state.position = {
        entryPriceUsd : price,
        amountToken   : simulatedTokens,
        solIn         : TRADE_SOL,
        buyTxid       : `DRY_${Date.now()}`,
        buyTime       : Date.now(),
        buyReason     : reason,
        _peakPrice    : price,   // ★ 移动止损：初始峰值 = 买入价
      };
      state.tradeCount++;
      this._addTradeLog(state, { type: 'BUY', symbol: state.symbol, price, reason,
        txid: state.position.buyTxid, solIn: TRADE_SOL, dryRun: true, tradeNum });
      this._createTradeRecord(state);
      logger.info('[Monitor] ✅ DRY_RUN BUY #%d %s @ %.8f  solIn=%.4f',
        tradeNum, state.symbol, price, TRADE_SOL);
    } else {
      try {
        const result = await trader.buy(state.address, state.symbol);

        // ★ 买单成交后，等 500ms 再查一次实际成交价
        //   避免用"信号触发时价格"做止损基准（memecoin 滑点可能很大）
        let actualEntryPrice = price;
        try {
          await new Promise(r => setTimeout(r, 500));
          const postFillPrice = await birdeye.getPrice(state.address);
          if (postFillPrice && postFillPrice > 0) {
            actualEntryPrice = postFillPrice;
            if (Math.abs(postFillPrice - price) / price > 0.02) {
              logger.warn('[Monitor] ⚠️ BUY #%d %s 成交价偏差: 信号=%.6f 实际=%.6f (%.1f%%)',
                tradeNum, state.symbol, price, postFillPrice,
                (postFillPrice - price) / price * 100);
            }
          }
        } catch (_) { /* 查询失败保留信号价 */ }

        state.position = {
          entryPriceUsd : actualEntryPrice,  // ★ 用实际成交后价格，不用信号触发时价格
          signalPriceUsd: price,             // 保留信号价用于参考
          amountToken   : result.amountOut,
          solIn         : result.solIn,
          buyTxid       : result.txid,
          buyTime       : Date.now(),
          buyReason     : reason,
          _peakPrice    : actualEntryPrice,  // ★ 移动止损：初始峰值 = 实际成交价
        };
        state.tradeCount++;
        this._addTradeLog(state, { type: 'BUY', symbol: state.symbol,
          price: actualEntryPrice, signalPrice: price, reason,
          txid: result.txid, solIn: result.solIn, tradeNum });
        this._createTradeRecord(state);
        logger.info('[Monitor] ✅ BUY #%d %s  solIn=%.4f SOL  entryPrice=%.6f  txid=%s',
          tradeNum, state.symbol, result.solIn, actualEntryPrice, result.txid);
      } catch (err) {
        logger.error('[Monitor] ❌ BUY #%d %s 失败: %s', tradeNum, state.symbol, err.message);
        state.inPosition = false;
      }
    }
  }

  // ── 卖出（不再退出监控，重置状态等待下一轮） ────────────────────

  async _doSell(state, reason) {
    if (state._selling) return;  // 防并发
    state._selling = true;

    const isStopLoss = reason.includes('STOP_LOSS') || reason.includes('TAKE_PROFIT');
    const tradeNum = state.tradeCount;
    logger.info('[Monitor] 🔴 SELL #%d %s | %s | isStopLoss=%s | DRY_RUN=%s',
      tradeNum, state.symbol, reason, isStopLoss, DRY_RUN);

    if (DRY_RUN) {
      let currentPrice;
      try {
        currentPrice = await birdeye.getPrice(state.address);
      } catch (_) {
        currentPrice = state._lastPriceUsd
          || (state.ticks.length > 0 ? state.ticks[state.ticks.length - 1].price : 0)
          || state.position?.entryPriceUsd || 0;
      }

      const solIn  = state.position?.solIn ?? TRADE_SOL;
      const entryP = state.position?.entryPriceUsd ?? 0;
      const solOut = entryP > 0 ? solIn * (currentPrice / entryP) : 0;
      const pnlPct = entryP > 0 ? (currentPrice - entryP) / entryP * 100 : 0;
      const pnlSol = solOut - solIn;

      state.inPosition = false;
      this._addTradeLog(state, { type: 'SELL', symbol: state.symbol, reason,
        txid: `DRY_${Date.now()}`, solOut, pnlSol, dryRun: true, tradeNum });
      this._finalizeTradeRecord(state, reason, solOut, pnlPct);

      logger.info('[Monitor] ✅ DRY_RUN SELL #%d %s  solIn=%.4f  solOut=%.4f  pnl=%+.4f SOL (%+.1f%%)',
        tradeNum, state.symbol, solIn, solOut, pnlSol, pnlPct);
    } else {
      try {
        const result = await trader.sell(state.address, state.symbol, state.position, isStopLoss);
        const solOut  = result.solOut ?? 0;
        const solIn   = state.position?.solIn ?? TRADE_SOL;
        const pnlPct  = solIn > 0 ? (solOut - solIn) / solIn * 100 : 0;
        const pnlSol  = solOut - solIn;

        state.inPosition = false;
        this._addTradeLog(state, { type: 'SELL', symbol: state.symbol, reason,
          txid: result.txid, solOut, pnlSol, elapsedMs: result.elapsedMs, tradeNum });
        this._finalizeTradeRecord(state, reason, solOut, pnlPct);

        logger.info('[Monitor] ✅ SELL #%d %s  solIn=%.4f  solOut=%.4f  pnl=%+.4f SOL (%+.1f%%)  耗时=%dms  txid=%s',
          tradeNum, state.symbol, solIn, solOut, pnlSol, pnlPct, result.elapsedMs || 0, result.txid);
      } catch (err) {
        logger.error('[Monitor] ❌ SELL #%d %s 失败: %s', tradeNum, state.symbol, err.message);
        state.inPosition = false;
        this._finalizeTradeRecord(state, `SELL_FAILED(${reason})`, 0, -100);
      }
    }

    // ★ 重置状态，准备下一轮交易
    state._selling = false;
    state.position = null;

    // ★ 设置冷却期
    state._sellCooldownUntil = Date.now() + SELL_COOLDOWN_SEC * 1000;
    // 重置 RSI 穿越防抖（允许新的穿越信号）
    state._lastBuyCandle  = -1;
    state._lastSellCandle = -1;
    state._lastPanicSellTs = 0;

    const remaining = Math.max(0, MAX_TRADES - state.tradeCount);
    const timeLeft  = Math.max(0, Math.ceil((state.expiresAt - Date.now()) / 60000));
    logger.info('[Monitor] 🔄 %s 第%d笔完成 | 剩余额度=%d笔 | 剩余时间=%d分钟 | 冷却=%ds',
      state.symbol, tradeNum, remaining, timeLeft, SELL_COOLDOWN_SEC);

    // 如果已达最大交易次数，立即移除
    if (state.tradeCount >= MAX_TRADES) {
      logger.info('[Monitor] 🏁 %s 已达最大交易次数 %d，移除监控', state.symbol, MAX_TRADES);
      // 延迟一点移除，让广播先发出
      setTimeout(() => this.removeToken(state.address, `MAX_TRADES(${state.tradeCount})`), 2000);
    }
  }

  // ── 辅助工具 ────────────────────────────────────────────────────

  _addTradeLog(state, log) {
    state.tradeLogs.push({ ...log, ts: Date.now() });
    if (state.tradeLogs.length > 500) state.tradeLogs.shift();
    wsHub.broadcast({ type: 'trade_log', ...log, ts: Date.now() });
    this.emit('trade', log);
  }

  _createTradeRecord(state) {
    if (!state.position) return;
    const rec = {
      id:         `${state.address}_${state.tradeCount}_${Date.now()}`,
      address:    state.address,
      symbol:     state.symbol,
      tradeNum:   state.tradeCount,
      buyAt:      state.position.buyTime,
      buyTxid:    state.position.buyTxid,
      entryPrice: state.position.entryPriceUsd,
      entryFdv:   state.fdv,
      entryLp:    state.lp,
      solIn:      state.position.solIn,
      buyReason:  state.position.buyReason || '',
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
    const now = Date.now();
    return {
      address:      state.address,
      symbol:       state.symbol,
      addedAt:      state.addedAt,
      expiresAt:    state.expiresAt,
      timeLeftMin:  Math.max(0, Math.ceil((state.expiresAt - now) / 60000)),
      inPosition:   state.inPosition,
      tradeCount:   state.tradeCount,
      maxTrades:    MAX_TRADES,
      cooldown:     state._sellCooldownUntil > now ? Math.ceil((state._sellCooldownUntil - now) / 1000) : 0,
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
