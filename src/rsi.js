'use strict';
// src/rsi.js — RSI 计算 + 量能过滤 + BUY/SELL 信号逻辑 (V3)
//
// V3 修复：
//   1. buildCandles 区分「价格 tick」和「链上交易 tick」
//      - 价格 tick（来自 Birdeye WS/HTTP，USD 计价）→ 构成 OHLC
//      - 链上 tick（来自 Helius WS，SOL 计价） → 只贡献 volume/buyVolume/sellVolume
//      - 解决了 V2 中 SOL 价格和 USD 价格混入同一数组导致 RSI 错乱的致命 BUG
//
//   2. 止损检查独立于 K 线周期，每个 tick 都检查（快速止损）
//
// 策略：
//   BUY : RSI(7) < 35（超卖区）+ totalVol ≥ 15 SOL + buyVol ≥ 1.2×sellVol
//   SELL: RSI 下穿 70 / RSI > 80 / 止盈 / 止损 / 量能萎缩出场

const RSI_PERIOD   = parseInt(process.env.RSI_PERIOD       || '9',  10);
const RSI_BUY      = parseFloat(process.env.RSI_BUY_LEVEL  || '30');
const RSI_SELL     = parseFloat(process.env.RSI_SELL_LEVEL  || '70');
const RSI_PANIC    = parseFloat(process.env.RSI_PANIC_LEVEL || '80');
const KLINE_SEC    = parseInt(process.env.KLINE_INTERVAL_SEC || '15', 10);

// ★ RSI 峰值回落卖出（RSI Peak Drop）
//   持仓后追踪 RSI 最高点，当 RSI 从峰值回落超过阈值时卖出
//   解决 RSI 没到 70 就开始回落的场景
const RSI_PEAK_DROP_ENABLED  = (process.env.RSI_PEAK_DROP_ENABLED  || 'true') === 'true';
const RSI_PEAK_DROP_ACTIVATE = parseFloat(process.env.RSI_PEAK_DROP_ACTIVATE || '70');  // RSI 峰值须 ≥ 此值才激活
const RSI_PEAK_DROP_DELTA    = parseFloat(process.env.RSI_PEAK_DROP_DELTA    || '10');  // 从峰值回落 ≥ 此值触发卖出
const RSI_PEAK_DROP_FLOOR    = parseFloat(process.env.RSI_PEAK_DROP_FLOOR    || '50');  // 回落后 RSI 须 ≤ 此值才卖（防止高位微抖）

// 量能参数
const VOL_ENABLED         = (process.env.VOL_ENABLED || 'true') === 'true';
const VOL_BUY_MULT        = parseFloat(process.env.VOL_BUY_MULT          || '1.2');
const VOL_SELL_MULT       = parseFloat(process.env.VOL_SELL_MULT         || '1.2'); // sellVol >= N × buyVol 触发卖出
const VOL_MIN_TOTAL       = parseFloat(process.env.VOL_MIN_TOTAL         || '15');  // 最低总成交量(SOL) // buyVol >= N × sellVol 才买入
const VOL_WINDOW_SEC      = parseInt(process.env.VOL_WINDOW_SEC       || '30', 10);
const VOL_EXIT_CONSECUTIVE = parseInt(process.env.VOL_EXIT_CONSECUTIVE || '2', 10);
const VOL_EXIT_RATIO      = parseFloat(process.env.VOL_EXIT_RATIO     || '1.0');
const VOL_EXIT_LOOKBACK   = parseInt(process.env.VOL_EXIT_LOOKBACK    || '4', 10);
const SKIP_FIRST_CANDLES  = parseInt(process.env.SKIP_FIRST_CANDLES   || '8', 10);

// 止盈止损
const TAKE_PROFIT_PCT = parseFloat(process.env.TAKE_PROFIT_PCT || '50');
const STOP_LOSS_PCT   = parseFloat(process.env.STOP_LOSS_PCT   || '-30');

// 移动止损（Trailing Stop）
const TRAILING_STOP_ENABLED  = (process.env.TRAILING_STOP_ENABLED  || 'true') === 'true';
const TRAILING_STOP_ACTIVATE = parseFloat(process.env.TRAILING_STOP_ACTIVATE || '30'); // 上涨 30% 后激活
const TRAILING_STOP_PCT      = parseFloat(process.env.TRAILING_STOP_PCT      || '-20'); // 峰值回撤 20% 清仓

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

function checkBuyVolume(closedCandles, currentCandle) {
  if (!VOL_ENABLED) return { pass: true, reason: 'VOL_DISABLED', buyVol: 0, sellVol: 0, ratio: 0 };

  const windowBars = Math.max(1, Math.ceil(VOL_WINDOW_SEC / KLINE_SEC));

  const allCandles = [...closedCandles];
  if (currentCandle) allCandles.push(currentCandle);

  if (allCandles.length < windowBars) {
    return { pass: false, reason: 'VOL_INSUFFICIENT_DATA', buyVol: 0, sellVol: 0, ratio: 0 };
  }

  const windowCandles = allCandles.slice(-windowBars);

  let totalBuy  = 0;
  let totalSell = 0;
  for (const c of windowCandles) {
    totalBuy  += (c.buyVolume  || 0);
    totalSell += (c.sellVolume || 0);
  }

  const total = totalBuy + totalSell;
  const ratio = total > 0 ? totalBuy / total : 0;

  // 没有链上方向数据 → 拒绝买入
  if (total === 0) {
    return { pass: false, reason: 'VOL_NO_DIRECTION_DATA', buyVol: 0, sellVol: 0, ratio: 0 };
  }

  // ★ 最低总成交量门槛
  if (total < VOL_MIN_TOTAL) {
    const mult = totalSell > 0 ? (totalBuy / totalSell).toFixed(1) : '∞';
    return {
      pass: false,
      reason: `VOL_TOO_LOW(${total.toFixed(1)}<${VOL_MIN_TOTAL}SOL,${mult}x,${VOL_WINDOW_SEC}s)`,
      buyVol: totalBuy, sellVol: totalSell, ratio,
    };
  }

  // 核心条件：buyVol >= VOL_BUY_MULT × sellVol
  if (totalBuy >= totalSell * VOL_BUY_MULT) {
    const mult = totalSell > 0 ? (totalBuy / totalSell).toFixed(1) : '∞';
    return {
      pass: true,
      reason: `BUY≥${VOL_BUY_MULT}xSELL(${totalBuy.toFixed(2)}>=${(totalSell*VOL_BUY_MULT).toFixed(2)},${mult}x,${(ratio*100).toFixed(0)}%,${VOL_WINDOW_SEC}s)`,
      buyVol: totalBuy, sellVol: totalSell, ratio,
    };
  }

  const mult = totalSell > 0 ? (totalBuy / totalSell).toFixed(1) : '0';
  return {
    pass: false,
    reason: `BUY<${VOL_BUY_MULT}xSELL(buy=${totalBuy.toFixed(2)},sell=${totalSell.toFixed(2)},${mult}x,${VOL_WINDOW_SEC}s)`,
    buyVol: totalBuy, sellVol: totalSell, ratio,
  };
}

function checkVolumeDecay(closedCandles, tokenState) {
  if (!VOL_ENABLED) return { shouldExit: false, reason: '' };
  if (closedCandles.length < VOL_EXIT_LOOKBACK + VOL_EXIT_CONSECUTIVE) {
    return { shouldExit: false, reason: 'INSUFFICIENT_DATA' };
  }

  const avgEnd = closedCandles.length - VOL_EXIT_CONSECUTIVE;
  const avgStart = Math.max(0, avgEnd - VOL_EXIT_LOOKBACK);
  const avgCandles = closedCandles.slice(avgStart, avgEnd);
  const avgVol = avgCandles.reduce((s, c) => s + (c.volume || 0), 0) / avgCandles.length;

  if (avgVol <= 0) return { shouldExit: false, reason: 'AVG_VOL_ZERO' };

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

// ── 快速止损检查（独立于 K 线，每个 tick 调用） ──────────────────

/**
 * 快速止损/止盈检查，不依赖 RSI，直接看价格偏离
 * @returns {{ shouldExit: boolean, reason: string }}
 */
function checkStopLoss(currentPrice, tokenState) {
  if (!tokenState.inPosition || !tokenState.position?.entryPriceUsd) {
    return { shouldExit: false, reason: '' };
  }

  const entryPrice = tokenState.position.entryPriceUsd;
  const pnl = (currentPrice - entryPrice) / entryPrice * 100;

  // ── 移动止损（Trailing Stop）────────────────────────────────────
  if (TRAILING_STOP_ENABLED && tokenState.position) {
    // 每个 tick 更新峰值价格
    if (!tokenState.position._peakPrice || currentPrice > tokenState.position._peakPrice) {
      tokenState.position._peakPrice = currentPrice;
    }
    const peakPrice = tokenState.position._peakPrice;
    const peakPnl   = (peakPrice - entryPrice) / entryPrice * 100;

    // 上涨达到激活线后，从峰值回撤超过阈值则清仓
    if (peakPnl >= TRAILING_STOP_ACTIVATE) {
      const dropFromPeak = (currentPrice - peakPrice) / peakPrice * 100;
      if (dropFromPeak <= TRAILING_STOP_PCT) {
        return {
          shouldExit: true,
          reason: `TRAILING_STOP(峰值+${peakPnl.toFixed(1)}%,回撤${dropFromPeak.toFixed(1)}%≤${TRAILING_STOP_PCT}%)`
        };
      }
    }
  }

  // ── 固定止盈 / 固定止损 ───────────────────────────────────────
  if (pnl >= TAKE_PROFIT_PCT) {
    return { shouldExit: true, reason: `TAKE_PROFIT(+${pnl.toFixed(1)}%≥${TAKE_PROFIT_PCT}%)` };
  }
  if (pnl <= STOP_LOSS_PCT) {
    return { shouldExit: true, reason: `STOP_LOSS(${pnl.toFixed(1)}%≤${STOP_LOSS_PCT}%)` };
  }

  return { shouldExit: false, reason: '', pnl };
}

// ── RSI 峰值回落检测 ─────────────────────────────────────────────
//
//  持仓后持续追踪 RSI 的最高值（_rsiPeakSinceEntry），
//  当 RSI 从峰值回落超过 RSI_PEAK_DROP_DELTA 且当前 RSI ≤ RSI_PEAK_DROP_FLOOR 时卖出。
//
//  例：RSI 峰值 = 65，回落到 52 → delta = 13 > 10 且 52 ≤ 50？不满足 floor
//       RSI 峰值 = 65，回落到 48 → delta = 17 > 10 且 48 ≤ 50 → 卖出 ✓
//       RSI 峰值 = 45，回落到 30 → 峰值 45 < 55(activate) → 不激活
//
function checkRsiPeakDrop(rsiRealtime, tokenState) {
  if (!RSI_PEAK_DROP_ENABLED) return { shouldExit: false, reason: '' };
  if (!tokenState.inPosition) return { shouldExit: false, reason: '' };

  // 更新 RSI 峰值
  if (!Number.isFinite(tokenState._rsiPeakSinceEntry) || rsiRealtime > tokenState._rsiPeakSinceEntry) {
    tokenState._rsiPeakSinceEntry = rsiRealtime;
  }

  const peak = tokenState._rsiPeakSinceEntry;
  if (!Number.isFinite(peak)) return { shouldExit: false, reason: '' };

  // 峰值太低，没形成有效上涨动能，不激活
  if (peak < RSI_PEAK_DROP_ACTIVATE) return { shouldExit: false, reason: '' };

  const drop = peak - rsiRealtime;

  // 从峰值回落超过阈值，且当前 RSI 已跌到 floor 以下
  if (drop >= RSI_PEAK_DROP_DELTA && rsiRealtime <= RSI_PEAK_DROP_FLOOR) {
    return {
      shouldExit: true,
      reason: `RSI_PEAK_DROP(peak=${peak.toFixed(1)},now=${rsiRealtime.toFixed(1)},drop=${drop.toFixed(1)}≥${RSI_PEAK_DROP_DELTA})`,
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

  if (closedCandles.length < SKIP_FIRST_CANDLES) {
    return { rsi: NaN, prevRsi: NaN, signal: null, reason: `skip_first(${closedCandles.length}/${SKIP_FIRST_CANDLES})`, volume: {} };
  }

  const closes = closedCandles.map(c => c.close);
  const len    = closes.length;

  const { rsiArray, avgGain, avgLoss } = calcRSIWithState(closes, RSI_PERIOD);
  const lastClosedRsi = rsiArray[len - 1];
  const lastClose     = closes[len - 1];

  // ★ 缓存到 tokenState，供 WS tick 快速下穿检测使用（避免重复计算）
  const lastCandleTs = closedCandles[len - 1].openTime;
  if (lastCandleTs !== tokenState._rsiLastCandleTs) {
    tokenState._rsiAvgGain      = avgGain;
    tokenState._rsiAvgLoss      = avgLoss;
    tokenState._rsiLastClose    = lastClose;
    tokenState._rsiLastCandleTs = lastCandleTs;
  }

  const rsiRealtime = stepRSI(avgGain, avgLoss, lastClose, realtimePrice, RSI_PERIOD);

  if (!Number.isFinite(lastClosedRsi) || !Number.isFinite(rsiRealtime)) {
    return { rsi: NaN, prevRsi: NaN, signal: null, reason: 'rsi_nan', volume: {},
             avgGain: NaN, avgLoss: NaN, lastClose: NaN };
  }

  const nowMs          = Date.now();
  // lastCandleTs 已在上方 RSI 缓存块里声明，直接复用
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

  // 量能信息
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
    buyVol:  winBuy,
    sellVol: winSell,
    buyRatio: winTotal > 0 ? winBuy / winTotal : 0,
    windowSec: VOL_WINDOW_SEC,
  };

  // ── SELL 优先（持仓中） ────────────────────────────────────────
  if (tokenState.inPosition) {

    // ★ 持续更新 RSI 峰值（每次 evaluateSignal 都更新）
    checkRsiPeakDrop(rsiRealtime, tokenState);  // 内部会更新 _rsiPeakSinceEntry

    // 1. RSI > 80 恐慌卖
    //    ★ 不受 lastSellCandle 限制（修复：K线防抖会导致同K线内卖出失败后无法重试）
    //    ★ 改用 2 秒时间防抖，避免每个 tick 都重复触发日志，但保证卖出失败后能重试
    if (rsiRealtime > RSI_PANIC) {
      const lastPanicTs = tokenState._lastPanicSellTs ?? 0;
      if (nowMs - lastPanicTs >= 2000) {
        tokenState._lastPanicSellTs = nowMs;
        updateState();
        return { rsi: rsiRealtime, prevRsi, signal: 'SELL',
                 reason: `RSI_PANIC(${rsiRealtime.toFixed(1)}>${RSI_PANIC})`, volume: volumeInfo };
      }
    }

    // 2. RSI 下穿 70
    if (prevRsi >= RSI_SELL && rsiRealtime < RSI_SELL && lastCandleTs !== lastSellCandle) {
      tokenState._lastSellCandle = lastCandleTs;
      updateState();
      return { rsi: rsiRealtime, prevRsi, signal: 'SELL',
               reason: `RSI_CROSS_DOWN_70(${prevRsi.toFixed(1)}→${rsiRealtime.toFixed(1)})`, volume: volumeInfo };
    }

    // 2.5 ★ RSI 峰值回落卖出（RSI 没到 70 也能卖）
    const peakDrop = checkRsiPeakDrop(rsiRealtime, tokenState);
    if (peakDrop.shouldExit) {
      updateState();
      return { rsi: rsiRealtime, prevRsi, signal: 'SELL',
               reason: peakDrop.reason, volume: volumeInfo };
    }

    // 3. 止盈 / 止损（也在 evaluateSignal 中保留，双重保险）
    const sl = checkStopLoss(realtimePrice, tokenState);
    if (sl.shouldExit) {
      updateState();
      return { rsi: rsiRealtime, prevRsi, signal: 'SELL',
               reason: sl.reason, volume: volumeInfo };
    }

    // 4. 卖压超过买压：sellVol >= VOL_SELL_MULT × buyVol
    if (VOL_ENABLED && winTotal > 0 && winSell >= winBuy * VOL_SELL_MULT && lastCandleTs !== lastSellCandle) {
      const mult = winBuy > 0 ? (winSell / winBuy).toFixed(1) : '∞';
      tokenState._lastSellCandle = lastCandleTs;
      updateState();
      return { rsi: rsiRealtime, prevRsi, signal: 'SELL',
               reason: `SELL_PRESSURE(sell=${winSell.toFixed(2)}>=${(winBuy*VOL_SELL_MULT).toFixed(2)},${mult}x,${VOL_WINDOW_SEC}s)`, volume: volumeInfo };
    }

    // 5. 量能萎缩出场
    const volDecay = checkVolumeDecay(closedCandles, tokenState);
    if (volDecay.shouldExit) {
      updateState();
      return { rsi: rsiRealtime, prevRsi, signal: 'SELL',
               reason: volDecay.reason, volume: volumeInfo };
    }
  }

  // ── BUY ────────────────────────────────────────────────────────
  // ★ RSI < 30（超卖区）+ buyVol >= 1.2 × sellVol
  if (!tokenState.inPosition) {
    if (rsiRealtime < RSI_BUY && lastCandleTs !== lastBuyCandle) {
      const volCheck = checkBuyVolume(closedCandles, null);
      volumeInfo.buyVol   = volCheck.buyVol;
      volumeInfo.sellVol  = volCheck.sellVol;
      volumeInfo.buyRatio = volCheck.ratio;

      if (volCheck.pass) {
        tokenState._lastBuyCandle = lastCandleTs;
        updateState();
        return { rsi: rsiRealtime, prevRsi, signal: 'BUY',
                 reason: `RSI_OVERSOLD(${rsiRealtime.toFixed(1)}<${RSI_BUY})+${volCheck.reason}`, volume: volumeInfo };
      }
      // 量能不达标，不标记 lastBuyCandle，下根K线继续检查
    }
  }

  updateState();
  return { rsi: rsiRealtime, prevRsi, signal: null, reason: isStale ? 'rsi_rebase' : '', volume: volumeInfo };
}

// ── K线聚合（V3：分离价格 tick 和链上交易 tick） ─────────────────
//
// tick 格式（两种来源共存于同一数组，用 source 字段区分）：
//
//   价格 tick（Birdeye WS/HTTP）:
//     { price: USD价格, ts, source: 'price' }
//     → 构成 OHLC
//
//   链上交易 tick（Helius WS）:
//     { price: 忽略(SOL计价), ts, solAmount, isBuy, source: 'chain' }
//     → 只贡献 volume / buyVolume / sellVolume
//     → 不参与 OHLC（单位不同！）
//

function buildCandles(ticks, intervalSec = KLINE_SEC) {
  if (!ticks || ticks.length === 0) return { closed: [], current: null };

  const intervalMs = intervalSec * 1000;
  const candles    = [];
  let current      = null;

  for (const tick of ticks) {
    const bucket = Math.floor(tick.ts / intervalMs) * intervalMs;

    const isChainTick = tick.source === 'chain';

    if (!current || current.openTime !== bucket) {
      // 新 K 线
      if (current) candles.push(current);

      if (isChainTick) {
        // 链上 tick 开始一根新K线，但没有价格数据
        // 创建空价格K线，等下一个价格 tick 填入
        current = {
          openTime:   bucket,
          closeTime:  bucket + intervalMs,
          open:       null,    // 等待价格 tick 填入
          high:       null,
          low:        null,
          close:      null,
          volume:     tick.solAmount || 0,
          buyVolume:  tick.isBuy  ? (tick.solAmount || 0) : 0,
          sellVolume: !tick.isBuy ? (tick.solAmount || 0) : 0,
          tickCount:  1,
          priceTickCount: 0,
        };
      } else {
        // 价格 tick 开始新K线
        current = {
          openTime:   bucket,
          closeTime:  bucket + intervalMs,
          open:       tick.price,
          high:       tick.price,
          low:        tick.price,
          close:      tick.price,
          volume:     0,          // volume 只来自链上交易
          buyVolume:  0,
          sellVolume: 0,
          tickCount:  1,
          priceTickCount: 1,
        };
      }
    } else {
      // 同一根 K 线内追加
      if (isChainTick) {
        // 链上 tick：只更新 volume
        current.volume     += (tick.solAmount || 0);
        current.buyVolume  += tick.isBuy  ? (tick.solAmount || 0) : 0;
        current.sellVolume += !tick.isBuy ? (tick.solAmount || 0) : 0;
        current.tickCount++;
      } else {
        // 价格 tick：更新 OHLC
        if (current.open === null) {
          // 这根K线之前只有链上 tick，现在才拿到价格
          current.open  = tick.price;
          current.high  = tick.price;
          current.low   = tick.price;
          current.close = tick.price;
        } else {
          if (tick.price > current.high) current.high = tick.price;
          if (tick.price < current.low)  current.low  = tick.price;
          current.close = tick.price;
        }
        current.tickCount++;
        current.priceTickCount++;
      }
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

/**
 * 过滤掉 open 为 null 的 K 线（只有链上 tick，没有价格数据的 K 线）
 * RSI 计算前调用
 */
function filterValidCandles(candles) {
  return candles.filter(c => c.open !== null && c.close !== null);
}

module.exports = {
  evaluateSignal,
  buildCandles,
  filterValidCandles,
  calcRSIWithState,
  stepRSI,
  checkBuyVolume,
  checkVolumeDecay,
  checkStopLoss,
  checkRsiPeakDrop,
  CONFIG: {
    RSI_PERIOD, RSI_BUY, RSI_SELL, RSI_PANIC,
    RSI_PEAK_DROP_ENABLED, RSI_PEAK_DROP_ACTIVATE, RSI_PEAK_DROP_DELTA, RSI_PEAK_DROP_FLOOR,
    VOL_ENABLED, VOL_BUY_MULT, VOL_SELL_MULT, VOL_MIN_TOTAL, VOL_WINDOW_SEC,
    VOL_EXIT_CONSECUTIVE, VOL_EXIT_RATIO, VOL_EXIT_LOOKBACK,
    SKIP_FIRST_CANDLES,
    TAKE_PROFIT_PCT, STOP_LOSS_PCT, KLINE_SEC,
    TRAILING_STOP_ENABLED, TRAILING_STOP_ACTIVATE, TRAILING_STOP_PCT,
  },
};
