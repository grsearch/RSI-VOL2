'use strict';
// src/trader.js — Jupiter Ultra API 交易封装
//
// DRY_RUN 模式下，trader 不会被调用（monitor.js 直接模拟），
// 但保留此模块以便切换到实盘时使用。

const {
  Keypair, VersionedTransaction, LAMPORTS_PER_SOL,
} = require('@solana/web3.js');
const bs58   = require('bs58');
const fetch  = require('node-fetch');
const logger = require('./logger');

const JUP_API          = process.env.JUPITER_API_URL           || 'https://api.jup.ag';
const JUP_API_KEY      = process.env.JUPITER_API_KEY           || '';
const SLIPPAGE_BPS     = parseInt(process.env.SLIPPAGE_BPS     || '500');
const TRADE_SOL        = parseFloat(process.env.TRADE_SIZE_SOL || '0.2');
const SLIPPAGE_MAX_BPS = parseInt(process.env.SLIPPAGE_MAX_BPS || '2000');
const MAX_RETRY        = parseInt(process.env.TRADE_MAX_RETRY  || '5');

const SOL_MINT = 'So11111111111111111111111111111111111111112';

function jupHeaders() {
  const h = { 'Content-Type': 'application/json' };
  if (JUP_API_KEY) h['x-api-key'] = JUP_API_KEY;
  return h;
}

let _keypair = null;
function getKeypair() {
  if (_keypair) return _keypair;
  const pk = process.env.WALLET_PRIVATE_KEY;
  if (!pk) throw new Error('WALLET_PRIVATE_KEY not set (DRY_RUN 模式下不需要设置)');
  _keypair = Keypair.fromSecretKey(bs58.decode(pk));
  return _keypair;
}

async function getSwapOrder({ inputMint, outputMint, amount, slippageBps }) {
  const params = new URLSearchParams({
    inputMint,
    outputMint,
    amount:      Math.floor(amount).toString(),
    slippageBps: (slippageBps ?? SLIPPAGE_BPS).toString(),
    taker:       getKeypair().publicKey.toBase58(),
  });
  const url = `${JUP_API}/ultra/v1/order?${params}`;
  const res = await fetch(url, { headers: jupHeaders(), timeout: 10000 });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Ultra order failed: ${res.status} ${text}`);
  }
  return res.json();
}

async function executeSwapOrder({ requestId, signedTransaction }) {
  const url = `${JUP_API}/ultra/v1/execute`;
  const res = await fetch(url, {
    method : 'POST',
    headers: jupHeaders(),
    body   : JSON.stringify({ requestId, signedTransaction }),
    timeout: 30000,
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Ultra execute failed: ${res.status} ${text}`);
  }
  return res.json();
}

function signTx(base64Tx) {
  const kp  = getKeypair();
  const buf = Buffer.from(base64Tx, 'base64');
  const tx  = VersionedTransaction.deserialize(buf);
  tx.sign([kp]);
  return Buffer.from(tx.serialize()).toString('base64');
}

async function executeWithRetry(orderFn) {
  let slippage = SLIPPAGE_BPS;

  for (let attempt = 1; attempt <= MAX_RETRY; attempt++) {
    try {
      const order = await orderFn(slippage);
      if (!order.transaction) {
        throw new Error(`Ultra order 缺少 transaction 字段，keys: ${Object.keys(order).join(', ')}`);
      }

      const signed = signTx(order.transaction);
      const result = await executeSwapOrder({
        requestId        : order.requestId,
        signedTransaction: signed,
      });

      if (result.status === 'Success') {
        return result;
      }

      logger.warn('[Trader] swap status="%s" attempt=%d/%d slippage=%dbps',
        result.status, attempt, MAX_RETRY, slippage);

    } catch (err) {
      logger.warn('[Trader] attempt=%d/%d slippage=%dbps 错误: %s',
        attempt, MAX_RETRY, slippage, err.message);
    }

    slippage = Math.min(Math.floor(slippage * 1.5), SLIPPAGE_MAX_BPS);
    if (attempt < MAX_RETRY) await sleep(1500 * attempt);
  }

  throw new Error(`交易失败，已重试 ${MAX_RETRY} 次`);
}

async function buy(tokenAddress, symbol) {
  const solLamports = Math.floor(TRADE_SOL * LAMPORTS_PER_SOL);
  logger.info('[Trader] 🟢 BUY %s  solLamports=%d  slippage=%dbps(%.1f%%)',
    symbol, solLamports, SLIPPAGE_BPS, SLIPPAGE_BPS / 100);

  const result = await executeWithRetry((slipBps) =>
    getSwapOrder({
      inputMint  : SOL_MINT,
      outputMint : tokenAddress,
      amount     : solLamports,
      slippageBps: slipBps,
    })
  );

  const amountOut = parseInt(result.outputAmountResult  || '0', 10);
  const solIn     = parseInt(result.inputAmountResult   || String(solLamports), 10) / LAMPORTS_PER_SOL;

  logger.info('[Trader] ✅ BUY 成功 %s  sig=%s  tokens=%d  solIn=%.4f',
    symbol, result.signature?.slice(0, 12), amountOut, solIn);

  return { txid: result.signature, amountOut, solIn };
}

async function sell(tokenAddress, symbol, position) {
  const amountToken = position?.amountToken;
  if (!amountToken || amountToken <= 0) throw new Error('amountToken 无效');

  const sellSlippage = Math.min(SLIPPAGE_BPS * 2, SLIPPAGE_MAX_BPS);
  logger.info('[Trader] 🔴 SELL %s  amount=%d  slippage=%dbps(%.1f%%)',
    symbol, amountToken, sellSlippage, sellSlippage / 100);

  const result = await executeWithRetry((slipBps) =>
    getSwapOrder({
      inputMint  : tokenAddress,
      outputMint : SOL_MINT,
      amount     : amountToken,
      slippageBps: slipBps,
    })
  );

  const solOut = parseInt(result.outputAmountResult || '0', 10) / LAMPORTS_PER_SOL;

  logger.info('[Trader] ✅ SELL 成功 %s  sig=%s  solOut=%.4f SOL',
    symbol, result.signature?.slice(0, 12), solOut);

  return { txid: result.signature, solOut, priceUsd: null };
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

module.exports = { buy, sell };
