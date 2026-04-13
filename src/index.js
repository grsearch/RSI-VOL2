'use strict';
require('dotenv').config();

const http    = require('http');
const express = require('express');
const path    = require('path');

const logger    = require('./logger');
const monitor   = require('./monitor');
const reporter  = require('./reporter');
const wsHub     = require('./wsHub');
const dataStore = require('./dataStore');
const heliusWs  = require('./heliusWs');
const birdeye   = require('./birdeye');

const webhookRouter   = require('./routes/webhook');
const dashboardRouter = require('./routes/dashboard');

const PORT    = parseInt(process.env.PORT || '3001', 10);
const DRY_RUN = (process.env.DRY_RUN || 'false') === 'true';

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, '..', 'public')));

// ── 路由 ──────────────────────────────────────────────────────────
app.use('/webhook', webhookRouter);
app.use('/api',     dashboardRouter);

app.get('/api/reports', (_req, res) => res.json(reporter.listReports()));

app.get('/api/backtest/data', (_req, res) => {
  const files = dataStore.listTickFiles();
  const trades = dataStore.loadTrades();
  const signals = dataStore.loadSignals();
  res.json({
    tickFiles: files.map(f => ({ address: f.address, size: f.size })),
    tradeCount: trades.length,
    signalCount: signals.length,
  });
});

// Helius WS 状态 API
app.get('/api/helius-stats', (_req, res) => {
  res.json(heliusWs.getStats());
});

// Birdeye WS 状态 API
app.get('/api/birdeye-status', (_req, res) => {
  res.json({
    wsConnected: birdeye.priceStream.isConnected(),
  });
});

// ── 回测 API ──────────────────────────────────────────────────────
const { runBacktest: btRun } = require('./backtest');

app.post('/api/backtest/run', (req, res) => {
  try {
    const params = req.body || {};
    const files = dataStore.listTickFiles();
    if (files.length === 0) {
      return res.json({ error: '无 tick 数据', results: [], summary: null });
    }

    const results = [];
    for (const f of files) {
      try {
        const ticks = dataStore.loadTicks(f.address);
        if (!ticks || ticks.length < 10) continue;
        const result = btRun(ticks, params);
        if (result && result.trades.length > 0) {
          results.push({ address: f.address, ...result });
        }
      } catch (_) {}
    }

    // 汇总
    const allTrades = results.flatMap(r => r.trades);
    const wins   = allTrades.filter(t => t.pnlSol > 0);
    const losses = allTrades.filter(t => t.pnlSol < 0);
    const totalPnlSol = allTrades.reduce((s, t) => s + t.pnlSol, 0);
    const avgPnl = allTrades.length > 0 ? allTrades.reduce((s, t) => s + t.pnlPct, 0) / allTrades.length : 0;

    const summary = {
      totalTokens:  files.length,
      tokensTraded: results.length,
      totalTrades:  allTrades.length,
      wins:         wins.length,
      losses:       losses.length,
      winRate:      allTrades.length > 0 ? (wins.length / (wins.length + losses.length) * 100) : 0,
      totalPnlSol:  parseFloat(totalPnlSol.toFixed(4)),
      avgPnlPct:    parseFloat(avgPnl.toFixed(2)),
      avgWinPct:    wins.length > 0 ? parseFloat((wins.reduce((s, t) => s + t.pnlPct, 0) / wins.length).toFixed(2)) : 0,
      avgLossPct:   losses.length > 0 ? parseFloat((losses.reduce((s, t) => s + t.pnlPct, 0) / losses.length).toFixed(2)) : 0,
    };

    res.json({ results, summary, params });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ── 服务器 ────────────────────────────────────────────────────────
const server = http.createServer(app);
wsHub.init(server);

server.listen(PORT, () => {
  logger.info('🚀 SOL RSI+量能 Monitor V3 启动，端口 %d', PORT);
  logger.info('   模式: %s', DRY_RUN ? '🔵 空跑(DRY_RUN)' : '🔴 实盘(LIVE)');
  logger.info('   K线=%ds  轮询=%ds  RSI周期=%s  买≤%s  卖≥%s  恐慌>%s',
    process.env.KLINE_INTERVAL_SEC || 5,
    process.env.PRICE_POLL_SEC     || 1,
    process.env.RSI_PERIOD         || 7,
    process.env.RSI_BUY_LEVEL      || 30,
    process.env.RSI_SELL_LEVEL     || 70,
    process.env.RSI_PANIC_LEVEL    || 80);
  logger.info('   量能: enabled=%s window=%ss',
    process.env.VOL_ENABLED        || 'true',
    process.env.VOL_WINDOW_SEC     || '30');
  logger.info('   止盈=%s%%  止损=%s%%  跳过前%s根K线',
    process.env.TAKE_PROFIT_PCT    || '50',
    process.env.STOP_LOSS_PCT      || '-10',
    process.env.SKIP_FIRST_CANDLES || '8');
  logger.info('   监控=%s分钟  最大交易=%s次/token  卖出冷却=%ss',
    process.env.TOKEN_MAX_AGE_MINUTES || '30',
    process.env.MAX_TRADES_PER_TOKEN  || '5',
    process.env.SELL_COOLDOWN_SEC     || '30');

  // 连接信息
  const birdeyeKey = process.env.BIRDEYE_API_KEY || '';
  logger.info('   Birdeye: %s (B-05 WS 实时价格)',
    birdeyeKey ? '✅ API Key 已配置' : '⚠️ 未配置');

  const heliusLaser = process.env.HELIUS_LASERSTREAM_URL || '';
  const heliusGK    = process.env.HELIUS_GATEKEEPER_URL || '';
  const heliusWss   = process.env.HELIUS_WSS_URL || '';
  const heliusKey   = process.env.HELIUS_API_KEY || '';
  const heliusRpc   = process.env.HELIUS_RPC_URL || '';

  if (heliusGK) {
    logger.info('   Helius WS: ✅ Gatekeeper Beta WSS（最低延迟 WebSocket）');
  } else if (heliusWss) {
    logger.info('   Helius WS: ✅ Enhanced WebSocket');
  } else if (heliusKey || heliusRpc.includes('api-key=')) {
    logger.info('   Helius WS: ✅ 统一端点 WebSocket');
  } else {
    logger.info('   Helius WS: ⚠️ 未配置，量能数据不可用');
  }
  const subMode = process.env.HELIUS_SUB_MODE || 'token';
  logger.info('   Helius 订阅: %s', subMode === 'pump'
    ? '🟡 Pump AMM 单订阅（本地过滤）'
    : '🟢 按 Token 精准订阅（最省 credits）');

  if (heliusLaser) {
    logger.info('   Helius RPC: ✅ LaserStream gRPC（仅用于 sendTransaction 加速）');
  }
  if (heliusGK) {
    logger.info('   Helius RPC: ✅ Gatekeeper Beta（最低延迟发单）');
  } else if (heliusRpc) {
    logger.info('   Helius RPC: ✅ 标准 RPC');
  }

  if (!DRY_RUN) {
    logger.info('   Jupiter: Ultra API  %s  Key=%s',
      process.env.JUPITER_API_URL || 'https://api.jup.ag',
      process.env.JUPITER_API_KEY ? '已配置' : '⚠️ 未配置');
  } else {
    logger.info('   📁 数据目录: %s', process.env.DRY_RUN_DATA_DIR || './data');
  }

  logger.info('');
  logger.info('   ⚡ 止损路径: BirdeyeWS(1s价格) → 本地判断 → 立即卖出（目标<500ms）');
  logger.info('   📊 RSI路径:  BirdeyeWS + 轮询兜底 → K线聚合 → RSI信号');
  logger.info('   📈 量能路径: HeliusWS(链上交易) → buyVol/sellVol → 买入确认');
  logger.info('');

  monitor.start();
  reporter.scheduleDaily(() => monitor.getAllTradeRecords());
});

// 优雅退出
process.on('SIGTERM', graceful);
process.on('SIGINT',  graceful);

async function graceful() {
  logger.info('[Main] 收到退出信号，清理...');
  monitor.stop();
  const tokens = monitor.getTokens();
  await Promise.allSettled(tokens.map(t => monitor.removeToken(t.address, 'SHUTDOWN')));
  process.exit(0);
}
