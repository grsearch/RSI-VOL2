'use strict';
const express   = require('express');
const router    = express.Router();
const monitor   = require('../monitor');
const reporter  = require('../reporter');
const dataStore = require('../dataStore');

router.get('/dashboard', (_req, res) => res.json({
  tokens: monitor.getTokens(),
  dryRun: monitor.DRY_RUN,
}));

router.get('/tokens', (_req, res) => res.json(monitor.getTokens()));

router.get('/tokens/:address', (req, res) => {
  const t = monitor.getToken(req.params.address);
  if (!t) return res.status(404).json({ error: 'not found' });
  res.json(t);
});

router.delete('/tokens/:address', async (req, res) => {
  await monitor.removeToken(req.params.address, 'manual_delete');
  res.json({ ok: true });
});

router.get('/trades', (_req, res) => {
  const logs = monitor.getTokens().flatMap(t => t.tradeLogs || []);
  logs.sort((a, b) => b.ts - a.ts);
  res.json(logs.slice(0, 200));
});

router.get('/trade-records', (_req, res) => {
  res.json(monitor.getAllTradeRecords());
});

// 持久化数据统计
router.get('/data-stats', (_req, res) => {
  const files = dataStore.listTickFiles();
  const trades = dataStore.loadTrades();
  const signals = dataStore.loadSignals();
  res.json({
    tickFiles: files.length,
    totalTicks: files.reduce((s, f) => s + Math.floor(f.size / 50), 0), // 估算
    tradeCount: trades.length,
    signalCount: signals.length,
    dataDir: dataStore.DATA_DIR,
  });
});

// 信号历史
router.get('/signals', (req, res) => {
  const limit = parseInt(req.query.limit || '100', 10);
  const signals = dataStore.loadSignals();
  res.json(signals.slice(-limit));
});

router.get('/reports', (_req, res) => res.json(reporter.listReports()));

module.exports = router;
