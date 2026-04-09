'use strict';
// src/rsi.js — RSI 计算 + 量能过滤 + BUY/SELL 信号逻辑
//
// V2 策略（15秒K线 + 1秒轮询）：
//   BUY : RSI(7) 上穿 30 + 滚动窗口内 buyVolume > sellVolume
//   SELL: RSI 下穿 70 / RSI > 80 / 止盈 / 止损 / 量能萎缩出场

const RSI_PERIOD   = parseInt(process.env.RSI_PERIOD       || '7',  10);
const RSI_BUY      = parseFloat(process.env.RSI_BUY_LEVEL  || '30');
const RSI_SELL     = parseFloat(process.env.RSI_SELL_LEVEL  || '70');
const RSI_PANIC    = parseFloat(process.env.RSI_PANIC_LEVEL || '80');
const KLINE_SEC    = parseInt(process.env.KLINE_INTERVAL_SEC || '15', 10);

// 量能参数
const VOL_ENABLED         = (process.env.VOL_ENABLED || 'true') === 'true';
const VOL_WINDOW_SEC      = parseInt(process.env.VOL_WINDOW_SEC       || '30', 10); // 买入确认窗口（秒），30秒=2根K线
const VOL_EXIT_CONSECUTIVE = parseInt(process.env.VOL_EXIT_CONSECUTIVE || '2', 10);
const VOL_EXIT_RATIO      = parseFloat(process.env.VOL_EXIT_RATIO     || '1.0');
const VOL_EXIT_LOOKBACK   = parseInt(process.env.VOL_EXIT_LOOKBACK    || '4', 10);
const SKIP_FIRST_CANDLES  = parseInt(process.env.SKIP_FIRST_CANDLES   || '8', 10);

// 止盈止损
const TAKE_PROFIT_PCT = parseFloat(process.env.TAKE_PROFIT_PCT || '50');
const STOP_LOSS_PCT   = parseFloat(process.env.STOP_LOSS_PCT   || '-10');

// ── Wilder RSI 计算 ────────────────────────────────────────────────

function calcRSIWithState(closes, period = RSI_PERIOD) {
  const rsiArray = new Array(closes.length).fill(NaN);
  if (closes.length < period + 1) return { rsiArray, avgGain: NaN, avgLoss: NaN };

  let gainSum = 0, lossSum = 0;
  for (let i = 1; i <= period; i++) {
    const diff = closes[i] - closes[i - 1];
    if (diff > 0) gainSum += diff;
    else lossSum += Math.abs(diff);
  }
  let avgGain = gainSum / period;
  let avgLoss = lossSum / period;
  rsiArray[period] = avgLoss === 0 ? 100 : 100 - 100 / (1 + avgGain / avgLoss);

  for (let i = period + 1; i < closes.length; i++) {
    const diff = closes[i] - closes[i - 1];
    const gain = diff > 0 ? diff : 0;
    const loss = diff < 0 ? Math.abs(diff) : 0;
    avgGain = (avgGain * (period - 1) + gain) / period;
    avgLoss = (avgLoss * (period - 1) + loss) / period;
    rsiArray[i] = avgLoss === 0 ? 100 : 100 - 100 / (1 + avgGain / avgLoss);
  }

  return { rsiArray, avgGain, avgLoss };
}

function stepRSI(avgGain, avgLoss, lastClose, newPrice, period = RSI_PERIOD) {
  if (!Number.isFinite(avgGain) || !Number.isFinite(avgLoss)) return NaN;
  const diff = newPrice - lastClose;
  const gain = diff > 0 ? diff : 0;
  const loss = diff < 0 ? Math.abs(diff) : 0;
  const ag = (avgGain * (period - 1) + gain) / period;
  const al = (avgLoss * (period - 1) + loss) / period;
  return al === 0 ? 100 : 100 - 100 / (1 + ag / al);
}

// ── 量能检测 ─────────────────────────────────────────────────────

/**
 * 检查买入时的量能条件：
 *   滚动窗口（VOL_WINDOW_SEC 秒）内的 buyVolume > sellVolume
 *
 * @param {Object[]} closedCandles - 已收盘K线数组
 * @param {Object|null} currentCandle - 当前正在形成的K线
 * @returns {{ pass: boolean, reason: string, buyVol: number, sellVol: number, ratio: number }}
 */
function checkBuyVolume(closedCandles, currentCandle) {
  if (!VOL_ENABLED) return { pass: true, reason: 'VOL_DISABLED', buyVol: 0, sellVol: 0, ratio: 0 };

  // 回看K线数 = 窗口秒数 / K线秒数，至少1根
  const windowBars = Math.max(1, Math.ceil(VOL_WINDOW_SEC / KLINE_SEC));

  // 收集窗口内的K线（已收盘 + 当前未收盘）
  const allCandles = [...closedCandles];
  if (currentCandle) allCandles.push(currentCandle);

  if (allCandles.length < windowBars) {
    return { pass: false, reason: 'VOL_INSUFFICIENT_DATA', buyVol: 0, sellVol: 0, ratio: 0 };
  }

  // 取最近 windowBars 根K线
  const windowCandles = allCandles.slice(-windowBars);

  let totalBuy  = 0;
  let totalSell = 0;
  for (const c of windowCandles) {
    totalBuy  += (c.buyVolume  || 0);
    totalSell += (c.sellVolume || 0);
  }

  const total = totalBuy + totalSell;
  const ratio = total > 0 ? totalBuy / total : 0;

  // 没有链上方向数据（Helius 未连接时 buyVolume/sellVolume 全为0）
  if (total === 0) {
    return {
      pass: true,  // 无方向数据时放行，退化为纯 RSI
      reason: 'VOL_NO_DIRECTION_DATA',
      buyVol: 0, sellVol: 0, ratio: 0,
    };
  }

  // 核心条件：buy > sell
  if (totalBuy > totalSell) {
    return {
      pass: true,
      reason: `BUY>SELL(${totalBuy.toFixed(2)}>${totalSell.toFixed(2)},${(ratio*100).toFixed(0)}%,${VOL_WINDOW_SEC}s)`,
      buyVol: totalBuy, sellVol: totalSell, ratio,
    };
  }

  return {
    pass: false,
    reason: `SELL≥BUY(buy=${totalBuy.toFixed(2)},sell=${totalSell.toFixed(2)},${(ratio*100).toFixed(0)}%,${VOL_WINDOW_SEC}s)`,
    buyVol: totalBuy, sellVol: totalSell, ratio,
  };
}

/**
 * 检查持仓期间的量能萎缩出场条件
 * @param {Object[]} closedCandles - 已收盘K线数组
 * @param {Object} tokenState - token状态
 * @returns {{ shouldExit: boolean, reason: string }}
 */
function checkVolumeDecay(closedCandles, tokenState) {
  if (!VOL_ENABLED) return { shouldExit: false, reason: '' };
  if (closedCandles.length < VOL_EXIT_LOOKBACK + VOL_EXIT_CONSECUTIVE) {
    return { shouldExit: false, reason: 'INSUFFICIENT_DATA' };
  }

  // 计算均量（不含最近的 VOL_EXIT_CONSECUTIVE 根）
  const avgEnd = closedCandles.length - VOL_EXIT_CONSECUTIVE;
  const avgStart = Math.max(0, avgEnd - VOL_EXIT_LOOKBACK);
  const avgCandles = closedCandles.slice(avgStart, avgEnd);
  const avgVol = avgCandles.reduce((s, c) => s + (c.volume || 0), 0) / avgCandles.length;

  if (avgVol <= 0) return { shouldExit: false, reason: 'AVG_VOL_ZERO' };

  // 检查最近 N 根是否都低于阈值
  const recentCandles = closedCandles.slice(-VOL_EXIT_CONSECUTIVE);
  const allDecayed = recentCandles.every(c => (c.volume || 0) < avgVol * VOL_EXIT_RATIO);

  if (allDecayed) {
    const recentVols = recentCandles.map(c => (c.volume || 0).toFixed(0)).join(',');
    return {
      shouldExit: true,
      reason: `VOL_DECAY(recent=[${recentVols}]<avg=${avgVol.toFixed(0)}×${VOL_EXIT_RATIO})`,
    };
  }

  return { shouldExit: false, reason: '' };
}

// ── 主信号函数 ─────────────────────────────────────────────────────

function evaluateSignal(closedCandles, realtimePrice, tokenState) {
  const MIN_CANDLES = RSI_PERIOD + 2;
  if (!closedCandles || closedCandles.length < MIN_CANDLES) {
    return { rsi: NaN, prevRsi: NaN, signal: null, reason: 'warming_up', volume: {} };
  }

  // 跳过前N根K线（噪音过滤）
  if (closedCandles.length < SKIP_FIRST_CANDLES) {
    return { rsi: NaN, prevRsi: NaN, signal: null, reason: `skip_first(${closedCandles.length}/${SKIP_FIRST_CANDLES})`, volume: {} };
  }

  const closes = closedCandles.map(c => c.close);
  const len    = closes.length;

  const { rsiArray, avgGain, avgLoss } = calcRSIWithState(closes, RSI_PERIOD);
  const lastClosedRsi = rsiArray[len - 1];
  const lastClose     = closes[len - 1];

  const rsiRealtime = stepRSI(avgGain, avgLoss, lastClose, realtimePrice, RSI_PERIOD);

  if (!Number.isFinite(lastClosedRsi) || !Number.isFinite(rsiRealtime)) {
    return { rsi: NaN, prevRsi: NaN, signal: null, reason: 'rsi_nan', volume: {} };
  }

  const nowMs        = Date.now();
  const lastCandleTs = closedCandles[len - 1].openTime;
  const lastBuyCandle  = tokenState._lastBuyCandle  ?? -1;
  const lastSellCandle = tokenState._lastSellCandle ?? -1;

  const prevRsiRaw = tokenState._prevRsiRealtime;
  const prevTs     = tokenState._prevRsiTs ?? 0;
  const isStale    = !Number.isFinite(prevRsiRaw) || (nowMs - prevTs) > 10000;
  const prevRsi    = isStale ? lastClosedRsi : prevRsiRaw;

  const updateState = () => {
    tokenState._prevRsiRealtime = rsiRealtime;
    tokenState._prevRsiTs       = nowMs;
  };

  // 当前窗口的量能信息（用于广播）
  const latestCandle = closedCandles[len - 1];
  const windowBars = Math.max(1, Math.ceil(VOL_WINDOW_SEC / KLINE_SEC));
  const windowCandles = closedCandles.slice(-windowBars);
  let winBuy = 0, winSell = 0;
  for (const c of windowCandles) {
    winBuy  += (c.buyVolume  || 0);
    winSell += (c.sellVolume || 0);
  }
  const winTotal = winBuy + winSell;
  const volumeInfo = {
    currentVol: latestCandle.volume || 0,
    buyVol: winBuy,
    sellVol: winSell,
    buyRatio: winTotal > 0 ? winBuy / winTotal : 0,
    windowSec: VOL_WINDOW_SEC,
  };

  // ── SELL 优先（持仓中） ────────────────────────────────────────
  if (tokenState.inPosition) {

    // 1. RSI > 80 恐慌卖
    if (rsiRealtime > RSI_PANIC && lastCandleTs !== lastSellCandle) {
      tokenState._lastSellCandle = lastCandleTs;
      updateState();
      return { rsi: rsiRealtime, prevRsi, signal: 'SELL',
               reason: `RSI_PANIC(${rsiRealtime.toFixed(1)}>${RSI_PANIC})`, volume: volumeInfo };
    }

    // 2. RSI 下穿 70
    if (prevRsi >= RSI_SELL && rsiRealtime < RSI_SELL && lastCandleTs !== lastSellCandle) {
      tokenState._lastSellCandle = lastCandleTs;
      updateState();
      return { rsi: rsiRealtime, prevRsi, signal: 'SELL',
               reason: `RSI_CROSS_DOWN_70(${prevRsi.toFixed(1)}→${rsiRealtime.toFixed(1)})`, volume: volumeInfo };
    }

    // 3. 止盈 / 止损
    if (tokenState.position && tokenState.position.entryPriceUsd) {
      const pnl = (realtimePrice - tokenState.position.entryPriceUsd)
                / tokenState.position.entryPriceUsd * 100;
      if (pnl >= TAKE_PROFIT_PCT) {
        updateState();
        return { rsi: rsiRealtime, prevRsi, signal: 'SELL',
                 reason: `TAKE_PROFIT(+${pnl.toFixed(1)}%≥${TAKE_PROFIT_PCT}%)`, volume: volumeInfo };
      }
      if (pnl <= STOP_LOSS_PCT) {
        updateState();
        return { rsi: rsiRealtime, prevRsi, signal: 'SELL',
                 reason: `STOP_LOSS(${pnl.toFixed(1)}%≤${STOP_LOSS_PCT}%)`, volume: volumeInfo };
      }
    }

    // 4. 量能萎缩出场（新增）
    const volDecay = checkVolumeDecay(closedCandles, tokenState);
    if (volDecay.shouldExit) {
      updateState();
      return { rsi: rsiRealtime, prevRsi, signal: 'SELL',
               reason: volDecay.reason, volume: volumeInfo };
    }
  }

  // ── BUY ────────────────────────────────────────────────────────
  // 方案B：RSI 处于超卖区间（≤30） + 窗口内 buyVolume > sellVolume → 买入
  //   逻辑：价格在超卖区 + 资金开始净流入 = 入场
  //   每根K线只检查一次（防抖）
  if (!tokenState.inPosition) {
    if (rsiRealtime <= RSI_BUY && lastCandleTs !== lastBuyCandle) {
      const volCheck = checkBuyVolume(closedCandles, null);
      volumeInfo.buyVol = volCheck.buyVol;
      volumeInfo.sellVol = volCheck.sellVol;
      volumeInfo.buyRatio = volCheck.ratio;

      if (volCheck.pass) {
        tokenState._lastBuyCandle = lastCandleTs;
        updateState();
        return { rsi: rsiRealtime, prevRsi, signal: 'BUY',
                 reason: `RSI_OVERSOLD(${rsiRealtime.toFixed(1)}≤${RSI_BUY})+${volCheck.reason}`, volume: volumeInfo };
      }
      // RSI 在超卖区但 buy ≤ sell，继续等待，不标记 lastBuyCandle（下根K线再检查）
    }
  }

  updateState();
  return { rsi: rsiRealtime, prevRsi, signal: null, reason: isStale ? 'rsi_rebase' : '', volume: volumeInfo };
}

// ── K线聚合（增强：加入 volume 统计） ─────────────────────────────

/**
 * 把 ticks 聚合成 OHLCV K线。
 * tick 格式：{ price, ts, solAmount?, isBuy? }
 *   solAmount: 该笔交易的 SOL 金额（可选，用于量能计算）
 *   isBuy: 是否为买入（可选，用于买压比计算）
 *
 * 如果没有 solAmount，volume 退化为 tick count。
 */
function buildCandles(ticks, intervalSec = KLINE_SEC) {
  if (!ticks || ticks.length === 0) return { closed: [], current: null };

  const intervalMs = intervalSec * 1000;
  const candles    = [];
  let current      = null;

  for (const tick of ticks) {
    const bucket = Math.floor(tick.ts / intervalMs) * intervalMs;
    if (!current || current.openTime !== bucket) {
      if (current) candles.push(current);
      current = {
        openTime  : bucket,
        closeTime : bucket + intervalMs,
        open      : tick.price,
        high      : tick.price,
        low       : tick.price,
        close     : tick.price,
        volume    : tick.solAmount || 1,      // SOL 成交额或 tick count
        buyVolume : (tick.isBuy ? (tick.solAmount || 1) : 0),
        sellVolume: (!tick.isBuy ? (tick.solAmount || 1) : 0),
        tickCount : 1,
      };
    } else {
      if (tick.price > current.high) current.high = tick.price;
      if (tick.price < current.low)  current.low  = tick.price;
      current.close = tick.price;
      current.volume    += (tick.solAmount || 1);
      current.buyVolume += (tick.isBuy ? (tick.solAmount || 1) : 0);
      current.sellVolume+= (!tick.isBuy ? (tick.solAmount || 1) : 0);
      current.tickCount++;
    }
  }

  if (!current) return { closed: candles, current: null };

  const now = Date.now();
  if (now >= current.closeTime) {
    candles.push(current);
    return { closed: candles, current: null };
  }

  return { closed: candles, current };
}

module.exports = {
  evaluateSignal,
  buildCandles,
  calcRSIWithState,
  stepRSI,
  checkBuyVolume,
  checkVolumeDecay,
  // 导出配置供回测使用
  CONFIG: {
    RSI_PERIOD, RSI_BUY, RSI_SELL, RSI_PANIC,
    VOL_ENABLED, VOL_WINDOW_SEC,
    VOL_EXIT_CONSECUTIVE, VOL_EXIT_RATIO, VOL_EXIT_LOOKBACK,
    SKIP_FIRST_CANDLES,
    TAKE_PROFIT_PCT, STOP_LOSS_PCT, KLINE_SEC,
  },
};
