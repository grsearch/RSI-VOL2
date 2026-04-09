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

// 回测 API（通过 HTTP 触发回测）
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

// ── 服务器 ────────────────────────────────────────────────────────
const server = http.createServer(app);
wsHub.init(server);

server.listen(PORT, () => {
  logger.info('🚀 SOL RSI+量能 Monitor V2 启动，端口 %d', PORT);
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

  if (!DRY_RUN) {
    logger.info('   Jupiter: Ultra API  %s  Key=%s',
      process.env.JUPITER_API_URL || 'https://api.jup.ag',
      process.env.JUPITER_API_KEY ? '已配置' : '⚠️ 未配置');
  } else {
    logger.info('   📁 数据目录: %s', process.env.DRY_RUN_DATA_DIR || './data');
  }

  // Helius WS 状态
  const heliusKey = process.env.HELIUS_API_KEY || '';
  const heliusRpc = process.env.HELIUS_RPC_URL || '';
  const hasHelius = heliusKey || heliusRpc.includes('api-key=');
  logger.info('   Helius WS: %s（链上成交量+买卖方向）',
    hasHelius ? '✅ 已配置' : '⚠️ 未配置，量能退化为 tick count');

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
