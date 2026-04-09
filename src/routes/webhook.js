'use strict';
// src/routes/webhook.js

const express = require('express');
const router  = express.Router();
const monitor = require('../monitor');
const logger  = require('../logger');

const MIN_FDV = parseFloat(process.env.MIN_FDV_USD || '15000');
const MIN_LP  = parseFloat(process.env.MIN_LP_USD  || '5000');

router.post('/add-token', (req, res) => {
  const { address, symbol, network, fdv, lp } = req.body || {};

  if (!address || !symbol) {
    return res.status(400).json({ error: '缺少 address 或 symbol' });
  }
  if (network && network !== 'solana') {
    return res.status(400).json({ error: '仅支持 solana' });
  }

  if (fdv !== undefined && fdv !== null) {
    if (Number(fdv) < MIN_FDV) {
      logger.info('[Webhook] 忽略 %s：FDV=$%s < $%s', symbol, fdv, MIN_FDV);
      return res.status(200).json({ ok: false, skip: true, reason: `FDV_TOO_LOW($${fdv}<$${MIN_FDV})` });
    }
  }
  if (lp !== undefined && lp !== null) {
    if (Number(lp) < MIN_LP) {
      logger.info('[Webhook] 忽略 %s：LP=$%s < $%s', symbol, lp, MIN_LP);
      return res.status(200).json({ ok: false, skip: true, reason: `LP_TOO_LOW($${lp}<$${MIN_LP})` });
    }
  }

  const added = monitor.addToken(address, symbol, { network: 'solana', fdv, lp });
  if (!added) {
    return res.status(409).json({ error: '代币已在监控中', address });
  }

  logger.info('[Webhook] ✅ 收到新代币 %s (%s) FDV=$%s LP=$%s', symbol, address, fdv ?? '?', lp ?? '?');
  res.json({ ok: true, address, symbol });
});

module.exports = router;
