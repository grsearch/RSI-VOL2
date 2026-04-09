'use strict';
// src/birdeye.js — Birdeye API 封装
//
// 价格：/defi/price          — 每次轮询都调用（1秒）
// FDV ：/defi/token_overview — 每 30 秒查一次，结果缓存
//
// 注意：/defi/price 不含 FDV 字段，必须用 token_overview 单独查

const fetch  = require('node-fetch');
const logger = require('./logger');

const BIRDEYE_KEY  = process.env.BIRDEYE_API_KEY || '';
const BASE         = 'https://public-api.birdeye.so';
const FDV_CACHE_MS = 30 * 1000;  // FDV 缓存 30 秒

// FDV 缓存：address → { fdv, ts }
const _fdvCache = new Map();

// ── 获取实时价格 ──────────────────────────────────────────────────

async function getPrice(address) {
  const url = `${BASE}/defi/price?address=${address}`;
  const res = await fetch(url, {
    headers: { 'X-API-KEY': BIRDEYE_KEY, 'x-chain': 'solana' },
    timeout: 5000,
  });
  if (!res.ok) throw new Error(`Birdeye price error: ${res.status}`);
  const json = await res.json();
  if (!json.success || !json.data) throw new Error('Birdeye price 返回异常');
  return json.data.value;   // 只返回价格数字
}

// ── 获取 FDV（带缓存，30秒更新一次） ─────────────────────────────

async function getFdv(address) {
  const cached = _fdvCache.get(address);
  if (cached && Date.now() - cached.ts < FDV_CACHE_MS) {
    return cached.fdv;
  }

  try {
    const url = `${BASE}/defi/token_overview?address=${address}`;
    const res = await fetch(url, {
      headers: { 'X-API-KEY': BIRDEYE_KEY, 'x-chain': 'solana' },
      timeout: 5000,
    });
    if (!res.ok) {
      logger.warn('[Birdeye] token_overview %s 返回 %d', address, res.status);
      return cached?.fdv ?? null;  // 请求失败时用旧缓存
    }
    const json = await res.json();
    const fdv = json?.data?.fdv ?? json?.data?.mc ?? null;  // fdv 或 mc（市值）
    _fdvCache.set(address, { fdv, ts: Date.now() });
    return fdv;
  } catch (err) {
    logger.warn('[Birdeye] getFdv %s 失败: %s', address, err.message);
    return cached?.fdv ?? null;
  }
}

// 主动清除某个地址的缓存（移除代币时调用）
function clearCache(address) {
  _fdvCache.delete(address);
}

module.exports = { getPrice, getFdv, clearCache };
