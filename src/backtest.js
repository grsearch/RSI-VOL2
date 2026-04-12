'use strict';
// src/backtest.js — 回测引擎
//
// 使用空跑模式收集的 tick 数据，用不同参数回测策略。
//
// 用法：
//   node src/backtest.js                         # 用默认参数回测所有数据
//   node src/backtest.js --rsi-buy=25 --vol-mult=1.5
//   node src/backtest.js --address=TOKEN_ADDRESS  # 只回测某个 token
//   node src/backtest.js --grid                   # 网格搜索最优参数
//
// 所有参数都可通过命令行覆盖 .env 中的值。

try { require('dotenv').config(); } catch (_) {}

const fs   = require('fs');
const path = require('path');

// 解析命令行参数
const args = {};
process.argv.slice(2).forEach(arg => {
  const m = arg.match(/^--([a-z-]+)=(.+)$/);
  if (m) args[m[1]] = m[2];
  else if (arg.match(/^--([a-z-]+)$/)) args[RegExp.$1] = 'true';
});

const DATA_DIR  = process.env.DRY_RUN_DATA_DIR || './data';
const TICKS_DIR = path.join(DATA_DIR, 'ticks');

// ── 回测核心 ─────────────────────────────────────────────────────

function runBacktest(ticks, params) {
  const {
    rsiPeriod      = 4,
    rsiBuy         = 35,
    rsiSell        = 70,
    rsiPanic       = 80,
    klineSec       = 5,
    volEnabled     = true,
    volWindowSec   = 15,
    volExitConsecutive = 3,
    volExitRatio   = 0.3,
    volExitLookback = 4,
    skipFirstCandles = 8,
    takeProfitPct  = 99999,
    stopLossPct    = -10,
    tradeSizeSol   = 0.2,
    maxTrades      = 5,
    volBuyMult     = 1.2,
    volMinTotal    = 5,
    sellCooldownSec = 30,
  } = params;

  if (!ticks || ticks.length === 0) return null;

  // ── Wilder RSI 工具 ──────────────────────────────────────────
  function stepRSI(ag, al, lastClose, newPrice, period) {
    if (!Number.isFinite(ag) || !Number.isFinite(al)) return NaN;
    const d = newPrice - lastClose;
    const nag = (ag * (period-1) + (d>0?d:0)) / period;
    const nal = (al * (period-1) + (d<0?Math.abs(d):0)) / period;
    return nal === 0 ? 100 : 100 - 100 / (1 + nag / nal);
  }

  // ── 构建 K 线（区分 price / chain tick） ───────────────────
  const intervalMs = klineSec * 1000;
  const allCandles = [];
  let curCandle = null;

  for (const tick of ticks) {
    const bucket = Math.floor(tick.ts / intervalMs) * intervalMs;
    const isChain = tick.source === 'chain' || (tick.solAmount && tick.source !== 'price');

    if (!curCandle || curCandle.openTime !== bucket) {
      if (curCandle) allCandles.push(curCandle);
      curCandle = {
        openTime: bucket, closeTime: bucket + intervalMs,
        open: isChain ? null : tick.price,
        high: isChain ? null : tick.price,
        low:  isChain ? null : tick.price,
        close: isChain ? null : tick.price,
        volume:     isChain ? (tick.solAmount || 0) : 0,
        buyVolume:  isChain && tick.isBuy  ? (tick.solAmount || 0) : 0,
        sellVolume: isChain && !tick.isBuy ? (tick.solAmount || 0) : 0,
      };
    } else {
      if (isChain) {
        curCandle.volume     += (tick.solAmount || 0);
        curCandle.buyVolume  += tick.isBuy  ? (tick.solAmount || 0) : 0;
        curCandle.sellVolume += !tick.isBuy ? (tick.solAmount || 0) : 0;
      } else {
        if (curCandle.open === null) {
          curCandle.open = curCandle.high = curCandle.low = curCandle.close = tick.price;
        } else {
          if (tick.price > curCandle.high) curCandle.high = tick.price;
          if (tick.price < curCandle.low)  curCandle.low  = tick.price;
          curCandle.close = tick.price;
        }
      }
    }
  }
  if (curCandle) allCandles.push(curCandle);

  const candles = allCandles.filter(c => c.open !== null && c.close !== null);
  if (candles.length < rsiPeriod + 2) return null;

  // ── 计算每根已收盘 K 线的 RSI + avgGain/avgLoss ──────────
  const closes = candles.map(c => c.close);
  const rsiArray = new Array(closes.length).fill(NaN);
  const agArray  = new Array(closes.length).fill(NaN);
  const alArray  = new Array(closes.length).fill(NaN);

  if (closes.length >= rsiPeriod + 1) {
    let gs = 0, ls = 0;
    for (let i = 1; i <= rsiPeriod; i++) {
      const d = closes[i] - closes[i-1];
      if (d > 0) gs += d; else ls += Math.abs(d);
    }
    let ag = gs / rsiPeriod, al = ls / rsiPeriod;
    rsiArray[rsiPeriod] = al === 0 ? 100 : 100 - 100 / (1 + ag / al);
    agArray[rsiPeriod] = ag; alArray[rsiPeriod] = al;

    for (let i = rsiPeriod + 1; i < closes.length; i++) {
      const d = closes[i] - closes[i-1];
      ag = (ag * (rsiPeriod-1) + (d>0?d:0)) / rsiPeriod;
      al = (al * (rsiPeriod-1) + (d<0?Math.abs(d):0)) / rsiPeriod;
      rsiArray[i] = al === 0 ? 100 : 100 - 100 / (1 + ag / al);
      agArray[i] = ag; alArray[i] = al;
    }
  }

  // ── 提取价格 tick 序列 ──────────────────────────────────────
  const priceTicks = ticks.filter(t => {
    if (t.source === 'chain') return false;
    if (t.solAmount && t.source !== 'price') return false;
    return t.price && t.price > 0;
  });

  if (priceTicks.length === 0) return null;

  // ── 逐 tick 模拟（与 live evaluateSignal 完全一致） ────────
  const trades = [];
  let inPosition = false;
  let entryPrice = 0, entryTime = 0;
  let prevRsiRT  = NaN;
  let lastBuyCandle = -1, lastSellCandle = -1;
  let cooldownUntil = 0;
  let volDecayCount = 0;
  const windowBars = Math.max(1, Math.ceil(volWindowSec / klineSec));

  // 查找 ts 对应的最新已收盘 K 线索引
  function closedIdx(ts) {
    for (let i = candles.length - 1; i >= 0; i--) {
      if (candles[i].closeTime <= ts) return i;
    }
    return -1;
  }

  for (const tick of priceTicks) {
    const price = tick.price;
    const ts    = tick.ts;

    const ci = closedIdx(ts);
    if (ci < rsiPeriod + 1 || ci < skipFirstCandles) continue;

    // ★ stepRSI 实时外推（与 live 一致）
    const rsiRT = stepRSI(agArray[ci], alArray[ci], closes[ci], price, rsiPeriod);
    if (!Number.isFinite(rsiRT)) { prevRsiRT = rsiRT; continue; }

    const prevRsi = Number.isFinite(prevRsiRT) ? prevRsiRT : rsiArray[ci];
    const candleOT = candles[ci].openTime;

    // ── SELL ──────────────────────────────────────────────
    if (inPosition) {
      let exitReason = null;

      // RSI > panic
      if (rsiRT > rsiPanic && candleOT !== lastSellCandle) {
        lastSellCandle = candleOT;
        exitReason = `RSI_PANIC(${rsiRT.toFixed(1)})`;
      }
      // RSI 下穿 sell
      if (!exitReason && prevRsi >= rsiSell && rsiRT < rsiSell && candleOT !== lastSellCandle) {
        lastSellCandle = candleOT;
        exitReason = `RSI_CROSS_DOWN(${prevRsi.toFixed(1)}→${rsiRT.toFixed(1)})`;
      }
      // 止盈/止损（逐 tick）
      if (!exitReason) {
        const pnl = (price - entryPrice) / entryPrice * 100;
        if (pnl >= takeProfitPct) exitReason = `TAKE_PROFIT(${pnl.toFixed(1)}%)`;
        else if (pnl <= stopLossPct) exitReason = `STOP_LOSS(${pnl.toFixed(1)}%)`;
      }
      // 量能萎缩
      if (!exitReason && volEnabled && ci >= volExitLookback + volExitConsecutive) {
        const ae = ci - volExitConsecutive + 1;
        const as = Math.max(0, ae - volExitLookback);
        const avgVol = candles.slice(as, ae).reduce((s,c) => s + (c.volume||0), 0) / (ae - as);
        if (avgVol > 0 && (candles[ci].volume||0) < avgVol * volExitRatio) {
          volDecayCount++;
          if (volDecayCount >= volExitConsecutive) {
            const rv = candles.slice(ci-volExitConsecutive+1, ci+1).map(c=>(c.volume||0).toFixed(0));
            exitReason = `VOL_DECAY(recent=[${rv}])`;
          }
        } else { volDecayCount = 0; }
      }

      if (exitReason) {
        const solOut = tradeSizeSol * (price / entryPrice);
        trades.push({
          entryPrice, exitPrice: price, entryTime, exitTime: ts,
          holdMs: ts - entryTime,
          solIn: tradeSizeSol, solOut,
          pnlSol: solOut - tradeSizeSol,
          pnlPct: (price - entryPrice) / entryPrice * 100,
          exitReason, tradeNum: trades.length + 1,
        });
        inPosition = false;
        volDecayCount = 0;
        cooldownUntil = ts + sellCooldownSec * 1000;
        lastBuyCandle = lastSellCandle = -1;
        if (trades.length >= maxTrades) break;
      }
    }

    // ── BUY ──────────────────────────────────────────────
    if (!inPosition && trades.length < maxTrades) {
      if (rsiRT < rsiBuy && ts >= cooldownUntil && candleOT !== lastBuyCandle) {
        if (volEnabled) {
          // 检查量能
          const start = Math.max(0, ci - windowBars + 1);
          const wc = candles.slice(start, ci + 1);
          let tb = 0, tsl = 0;
          for (const c of wc) { tb += (c.buyVolume||0); tsl += (c.sellVolume||0); }
          const tv = tb + tsl;
          if (tv > 0 && tv >= volMinTotal && tb >= tsl * volBuyMult) {
            lastBuyCandle = candleOT;
            inPosition = true; entryPrice = price; entryTime = ts; volDecayCount = 0;
          }
          // 量能不达标：不标记 lastBuyCandle
        } else {
          lastBuyCandle = candleOT;
          inPosition = true; entryPrice = price; entryTime = ts; volDecayCount = 0;
        }
      }
    }

    prevRsiRT = rsiRT;
  }

  // 到期未平仓
  if (inPosition && priceTicks.length > 0) {
    const last = priceTicks[priceTicks.length - 1];
    const solOut = tradeSizeSol * (last.price / entryPrice);
    trades.push({
      entryPrice, exitPrice: last.price, entryTime, exitTime: last.ts,
      holdMs: last.ts - entryTime,
      solIn: tradeSizeSol, solOut,
      pnlSol: solOut - tradeSizeSol,
      pnlPct: (last.price - entryPrice) / entryPrice * 100,
      exitReason: 'EXPIRED', tradeNum: trades.length + 1,
    });
  }

  return { trades, candleCount: candles.length, tickCount: priceTicks.length };
}

// ── 汇总统计 ─────────────────────────────────────────────────────

function summarize(allResults) {
  const allTrades = allResults.flatMap(r => r ? r.trades : []);
  if (allTrades.length === 0) {
    return { totalTokens: allResults.length, totalTrades: 0, message: '无交易' };
  }

  const wins   = allTrades.filter(t => t.pnlSol > 0);
  const losses = allTrades.filter(t => t.pnlSol <= 0);
  const totalPnlSol = allTrades.reduce((s, t) => s + t.pnlSol, 0);
  const avgPnlPct   = allTrades.reduce((s, t) => s + t.pnlPct, 0) / allTrades.length;
  const avgWinPct   = wins.length > 0 ? wins.reduce((s, t) => s + t.pnlPct, 0) / wins.length : 0;
  const avgLossPct  = losses.length > 0 ? losses.reduce((s, t) => s + t.pnlPct, 0) / losses.length : 0;
  const avgHoldBars = allTrades.reduce((s, t) => s + t.holdBars, 0) / allTrades.length;

  // 盈亏比
  const profitFactor = avgLossPct !== 0 ? Math.abs(avgWinPct / avgLossPct) : Infinity;

  return {
    totalTokens : allResults.length,
    tokensTraded: allResults.filter(r => r && r.trades.length > 0).length,
    totalTrades : allTrades.length,
    wins        : wins.length,
    losses      : losses.length,
    winRate     : (wins.length / allTrades.length * 100).toFixed(1) + '%',
    totalPnlSol : totalPnlSol.toFixed(4),
    avgPnlPct   : avgPnlPct.toFixed(2) + '%',
    avgWinPct   : avgWinPct.toFixed(2) + '%',
    avgLossPct  : avgLossPct.toFixed(2) + '%',
    profitFactor: profitFactor === Infinity ? '∞' : profitFactor.toFixed(2),
    avgHoldBars : avgHoldBars.toFixed(1),
    exitReasons : countBy(allTrades, t => t.exitReason.replace(/\(.*\)/, '')),
  };
}

function countBy(arr, fn) {
  const counts = {};
  arr.forEach(item => {
    const key = fn(item);
    counts[key] = (counts[key] || 0) + 1;
  });
  return counts;
}

// ── 网格搜索 ─────────────────────────────────────────────────────

function gridSearch(tickFiles) {
  const paramGrid = {
    rsiBuy:           [25, 30, 35],
    volMult:          [1.5, 2.0, 2.5, 3.0],
    volBuyRatio:      [0.5, 0.6, 0.7],
    skipFirstCandles: [4, 8, 12],
    stopLossPct:      [-8, -10, -15],
    takeProfitPct:    [30, 50, 80],
  };

  // 加载所有 tick 数据
  const allTicks = tickFiles.map(f => {
    try {
      return { address: f.address, ticks: JSON.parse(fs.readFileSync(f.file, 'utf-8')) };
    } catch (_) {
      return null;
    }
  }).filter(Boolean);

  if (allTicks.length === 0) {
    console.log('❌ 没有可用的 tick 数据');
    return;
  }

  console.log(`\n📊 网格搜索 | ${allTicks.length} 个 token 的数据\n`);

  // 生成参数组合
  const combos = [];
  for (const rsiBuy of paramGrid.rsiBuy) {
    for (const volMult of paramGrid.volMult) {
      for (const volBuyRatio of paramGrid.volBuyRatio) {
        for (const skipFirst of paramGrid.skipFirstCandles) {
          for (const stopLoss of paramGrid.stopLossPct) {
            for (const takeProfit of paramGrid.takeProfitPct) {
              combos.push({
                rsiBuy, volMult, volBuyRatio,
                skipFirstCandles: skipFirst,
                stopLossPct: stopLoss,
                takeProfitPct: takeProfit,
              });
            }
          }
        }
      }
    }
  }

  console.log(`  参数组合总数: ${combos.length}`);

  const results = [];

  for (const combo of combos) {
    const params = {
      rsiPeriod: 7, rsiSell: 70, rsiPanic: 80,
      klineSec: 15, volEnabled: true, volLookback: 4,
      volExitConsecutive: 2, volExitRatio: 1.0,
      tradeSizeSol: 0.2, maxTrades: 5,
      ...combo,
    };

    const btResults = allTicks.map(d => runBacktest(d.ticks, params));
    const summary = summarize(btResults);

    if (summary.totalTrades > 0) {
      results.push({ params: combo, summary });
    }
  }

  // 按总盈亏 SOL 排序
  results.sort((a, b) => parseFloat(b.summary.totalPnlSol) - parseFloat(a.summary.totalPnlSol));

  // 显示 top 20
  console.log('\n🏆 Top 20 参数组合（按总盈亏SOL排序）：\n');
  console.log('排名 | RSI买入 | 量能倍数 | 买压比 | 跳过K线 | 止损 | 止盈 | 交易数 | 胜率 | 总PnL(SOL) | 盈亏比 | 平均Win% | 平均Loss%');
  console.log('-'.repeat(130));

  results.slice(0, 20).forEach((r, i) => {
    const p = r.params;
    const s = r.summary;
    console.log(
      `#${String(i + 1).padStart(2)} | RSI≤${String(p.rsiBuy).padStart(2)} | Vol≥${p.volMult.toFixed(1)}x | Buy≥${(p.volBuyRatio * 100).toFixed(0)}% | Skip${String(p.skipFirstCandles).padStart(2)} | ${p.stopLossPct}% | +${p.takeProfitPct}% | ${String(s.totalTrades).padStart(4)} | ${s.winRate.padStart(5)} | ${s.totalPnlSol.padStart(8)} | ${String(s.profitFactor).padStart(5)} | ${s.avgWinPct.padStart(7)} | ${s.avgLossPct.padStart(8)}`
    );
  });

  // 也显示 worst 5
  if (results.length > 5) {
    console.log('\n📉 Worst 5：');
    results.slice(-5).reverse().forEach((r, i) => {
      const p = r.params;
      const s = r.summary;
      console.log(
        `#${results.length - 4 + i} | RSI≤${p.rsiBuy} Vol≥${p.volMult}x Buy≥${(p.volBuyRatio * 100).toFixed(0)}% Skip${p.skipFirstCandles} SL${p.stopLossPct}% TP+${p.takeProfitPct}% | 交易${s.totalTrades} 胜率${s.winRate} PnL=${s.totalPnlSol}SOL 盈亏比${s.profitFactor}`
      );
    });
  }

  return results;
}

// ── 主函数 ───────────────────────────────────────────────────────

function main() {
  console.log('═══════════════════════════════════════════════════');
  console.log('  SOL RSI+量能 回测引擎 V2');
  console.log('═══════════════════════════════════════════════════\n');

  // 检查数据目录
  if (!fs.existsSync(TICKS_DIR)) {
    console.log('❌ 找不到 tick 数据目录: %s', TICKS_DIR);
    console.log('   请先运行 DRY_RUN=true 模式收集数据\n');
    process.exit(1);
  }

  const tickFiles = fs.readdirSync(TICKS_DIR)
    .filter(f => f.endsWith('.json'))
    .map(f => ({
      address: f.replace('.json', ''),
      file: path.join(TICKS_DIR, f),
      size: fs.statSync(path.join(TICKS_DIR, f)).size,
    }))
    .filter(f => f.size > 10); // 跳过空文件

  console.log(`📁 数据目录: ${TICKS_DIR}`);
  console.log(`📊 找到 ${tickFiles.length} 个 token 的 tick 数据\n`);

  if (tickFiles.length === 0) {
    console.log('❌ 没有可用数据，请先空跑收集\n');
    process.exit(1);
  }

  // 网格搜索模式
  if (args.grid === 'true') {
    gridSearch(tickFiles);
    return;
  }

  // 构建参数
  const params = {
    rsiPeriod:          parseInt(args['rsi-period'] || process.env.RSI_PERIOD || '7', 10),
    rsiBuy:             parseFloat(args['rsi-buy']  || process.env.RSI_BUY_LEVEL || '30'),
    rsiSell:            parseFloat(args['rsi-sell'] || process.env.RSI_SELL_LEVEL || '70'),
    rsiPanic:           parseFloat(args['rsi-panic']|| process.env.RSI_PANIC_LEVEL || '80'),
    klineSec:           parseInt(args['kline-sec']  || process.env.KLINE_INTERVAL_SEC || '15', 10),
    volEnabled:         (args['vol-enabled'] || process.env.VOL_ENABLED || 'true') === 'true',
    volMult:            parseFloat(args['vol-mult']       || process.env.VOL_MULT || '2.0'),
    volLookback:        parseInt(args['vol-lookback']     || process.env.VOL_LOOKBACK || '4', 10),
    volBuyRatio:        parseFloat(args['vol-buy-ratio']  || process.env.VOL_BUY_RATIO || '0.60'),
    volExitConsecutive: parseInt(args['vol-exit-consec']  || process.env.VOL_EXIT_CONSECUTIVE || '2', 10),
    volExitRatio:       parseFloat(args['vol-exit-ratio'] || process.env.VOL_EXIT_RATIO || '1.0'),
    skipFirstCandles:   parseInt(args['skip-first']       || process.env.SKIP_FIRST_CANDLES || '8', 10),
    takeProfitPct:      parseFloat(args['take-profit']    || process.env.TAKE_PROFIT_PCT || '50'),
    stopLossPct:        parseFloat(args['stop-loss']      || process.env.STOP_LOSS_PCT || '-10'),
    tradeSizeSol:       parseFloat(args['trade-size']     || process.env.TRADE_SIZE_SOL || '0.2'),
    maxTrades:          parseInt(args['max-trades']       || process.env.MAX_TRADES_PER_TOKEN || '5', 10),
  };

  console.log('📋 回测参数：');
  console.log('   RSI: period=%d buy≤%d sell≥%d panic>%d', params.rsiPeriod, params.rsiBuy, params.rsiSell, params.rsiPanic);
  console.log('   K线: %d秒', params.klineSec);
  console.log('   量能: enabled=%s mult≥%sx lookback=%d buyRatio≥%s%%',
    params.volEnabled, params.volMult, params.volLookback, (params.volBuyRatio * 100).toFixed(0));
  console.log('   出场: volDecay=%d根连续 ratio<%sx  TP=%+d%% SL=%d%%',
    params.volExitConsecutive, params.volExitRatio, params.takeProfitPct, params.stopLossPct);
  console.log('   跳过前 %d 根K线  最大交易 %d 次/token\n', params.skipFirstCandles, params.maxTrades);

  // 筛选 token
  let filesToTest = tickFiles;
  if (args.address) {
    filesToTest = tickFiles.filter(f => f.address === args.address);
    if (filesToTest.length === 0) {
      console.log('❌ 找不到 address=%s 的数据\n', args.address);
      process.exit(1);
    }
  }

  // 执行回测
  const results = [];
  for (const f of filesToTest) {
    try {
      const ticks = JSON.parse(fs.readFileSync(f.file, 'utf-8'));
      const result = runBacktest(ticks, params);
      if (result) {
        results.push({ address: f.address, ...result });
      }
    } catch (err) {
      console.log('⚠️  %s 回测失败: %s', f.address.slice(0, 8), err.message);
    }
  }

  // 打印每个 token 的结果
  console.log('─'.repeat(80));
  console.log('📊 逐 Token 结果：\n');

  for (const r of results) {
    if (r.trades.length === 0) continue;
    for (const t of r.trades) {
      const dirStr = t.pnlSol >= 0 ? '🟢' : '🔴';
      console.log(
        `${dirStr} ${r.address.slice(0, 8)}... | 入场K线#${t.entryIdx} @ ${t.entryPrice.toFixed(8)} → 出场K线#${t.exitIdx} @ ${t.exitPrice.toFixed(8)} | 持仓${t.holdBars}根 | PnL: ${t.pnlSol >= 0 ? '+' : ''}${t.pnlSol.toFixed(4)} SOL (${t.pnlPct >= 0 ? '+' : ''}${t.pnlPct.toFixed(1)}%) | ${t.exitReason}`
      );
    }
  }

  // 汇总
  const summary = summarize(results);
  console.log('\n═══════════════════════════════════════════════════');
  console.log('  📈 回测汇总');
  console.log('═══════════════════════════════════════════════════');
  console.log('  Token 总数    : %d', summary.totalTokens);
  console.log('  发生交易数    : %d', summary.tokensTraded);
  console.log('  总交易笔数    : %d', summary.totalTrades);
  console.log('  胜 / 负       : %d / %d', summary.wins, summary.losses);
  console.log('  胜率          : %s', summary.winRate);
  console.log('  总盈亏(SOL)   : %s', summary.totalPnlSol);
  console.log('  平均盈亏%%     : %s', summary.avgPnlPct);
  console.log('  平均赢利%%     : %s', summary.avgWinPct);
  console.log('  平均亏损%%     : %s', summary.avgLossPct);
  console.log('  盈亏比        : %s', summary.profitFactor);
  console.log('  平均持仓K线数  : %s', summary.avgHoldBars);
  console.log('  出场原因分布  :', JSON.stringify(summary.exitReasons));
  console.log('═══════════════════════════════════════════════════\n');

  // 对比：纯 RSI（无量能）
  if (params.volEnabled) {
    console.log('─ 对比：关闭量能过滤 ─────────────────────────────\n');
    const noVolParams = { ...params, volEnabled: false };
    const noVolResults = [];
    for (const f of filesToTest) {
      try {
        const ticks = JSON.parse(fs.readFileSync(f.file, 'utf-8'));
        const result = runBacktest(ticks, noVolParams);
        if (result) noVolResults.push(result);
      } catch (_) {}
    }
    const noVolSummary = summarize(noVolResults);
    console.log('  [纯RSI] 交易=%d  胜率=%s  PnL=%s SOL  盈亏比=%s  avgWin=%s  avgLoss=%s',
      noVolSummary.totalTrades, noVolSummary.winRate, noVolSummary.totalPnlSol,
      noVolSummary.profitFactor, noVolSummary.avgWinPct, noVolSummary.avgLossPct);
    console.log('  [RSI+量能] 交易=%d  胜率=%s  PnL=%s SOL  盈亏比=%s  avgWin=%s  avgLoss=%s',
      summary.totalTrades, summary.winRate, summary.totalPnlSol,
      summary.profitFactor, summary.avgWinPct, summary.avgLossPct);
    console.log('');
  }
}

// 导出供 API 使用
module.exports = { runBacktest };

// 只在直接运行时执行 main
if (require.main === module) {
  main();
}
