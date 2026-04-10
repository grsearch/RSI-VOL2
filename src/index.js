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

// ── 服务器 ────────────────────────────────────────────────────────
const server = http.createServer(app);
wsHub.init(server);

server.listen(PORT, () => {
  logger.info('🚀 SOL RSI+量能 Monitor V3 启动，端口 %d', PORT);
  logger.info('   模式: %s', DRY_RUN ? '🔵 空跑(DRY_RUN)' : '🔴 实盘(LIVE)');
  logger.info('   K线=%ds  轮询=%ds  RSI周期=%s  买≤%s  卖≥%s  恐慌>%s',
    process.env.KLINE_INTERVAL_SEC || 15,
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

  // 连接信息
  const birdeyeKey = process.env.BIRDEYE_API_KEY || '';
  logger.info('   Birdeye: %s (B-05 WS 实时价格)',
    birdeyeKey ? '✅ API Key 已配置' : '⚠️ 未配置');

  const heliusLaser = process.env.HELIUS_LASERSTREAM_URL || '';
  const heliusGK    = process.env.HELIUS_GATEKEEPER_URL || '';
  const heliusWss   = process.env.HELIUS_WSS_URL || '';
  const heliusKey   = process.env.HELIUS_API_KEY || '';
  const heliusRpc   = process.env.HELIUS_RPC_URL || '';

  if (heliusLaser) {
    logger.info('   Helius: ✅ LaserStream（shred 级延迟）');
  } else if (heliusWss) {
    logger.info('   Helius: ✅ Enhanced WebSocket');
  } else if (heliusKey || heliusRpc.includes('api-key=')) {
    logger.info('   Helius: ✅ 标准 WebSocket');
  } else {
    logger.info('   Helius: ⚠️ 未配置，量能退化为 tick count');
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
