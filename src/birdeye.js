'use strict';
// src/birdeye.js — Birdeye API 封装 (V3 — WebSocket 实时价格 + HTTP 兜底)
//
// 架构：
//   1. 主力：Birdeye WebSocket SUBSCRIBE_PRICE（1s OHLCV 推送，延迟 50-150ms）
//   2. 兜底：HTTP /defi/price（WS 断开时自动切换）
//   3. FDV：/defi/token_overview（30秒缓存）
//
// B-05 级别支持 WebSocket + 100 token 并发订阅

const WebSocket = require('ws');
const fetch     = require('node-fetch');
const logger    = require('./logger');

const BIRDEYE_KEY  = process.env.BIRDEYE_API_KEY || '';
const BASE         = 'https://public-api.birdeye.so';
const FDV_CACHE_MS = 5 * 1000;  // FDV 缓存 5 秒（快速响应 FDV 下跌）

// ── WebSocket 价格流 ──────────────────────────────────────────────

const WS_URL = `wss://public-api.birdeye.so/socket/solana?x-api-key=${BIRDEYE_KEY}`;
const PING_INTERVAL_MS  = 25000;
const RECONNECT_BASE_MS = 1000;
const MAX_RECONNECT_MS  = 30000;

class BirdeyePriceStream {
  constructor() {
    this._ws         = null;
    this._connected  = false;
    this._pingTimer  = null;
    this._retryCount = 0;
    this._stopping   = false;

    // address → { price, ts, callbacks: Set<fn> }
    this._subscriptions = new Map();
  }

  start() {
    if (!BIRDEYE_KEY) {
      logger.warn('[BirdeyeWS] ⚠️ BIRDEYE_API_KEY 未设置，仅用 HTTP 轮询');
      return;
    }
    this._stopping = false;
    this._connect();
  }

  stop() {
    this._stopping = true;
    if (this._pingTimer) { clearInterval(this._pingTimer); this._pingTimer = null; }
    if (this._ws) {
      try { this._ws.close(); } catch (_) {}
      this._ws = null;
    }
    this._connected = false;
  }

  /**
   * 订阅 token 价格推送
   * @param {string} address - token mint 地址
   * @param {function} onPrice - 回调 (price, ts, ohlcv) => void
   */
  subscribe(address, onPrice) {
    if (!this._subscriptions.has(address)) {
      this._subscriptions.set(address, { price: null, ts: 0, callbacks: new Set() });
    }
    this._subscriptions.get(address).callbacks.add(onPrice);

    if (this._connected && this._ws?.readyState === WebSocket.OPEN) {
      this._sendSubscribe(address);
    }

    logger.info('[BirdeyeWS] 📌 订阅价格 %s，当前 %d 个', address.slice(0, 8) + '...', this._subscriptions.size);
  }

  unsubscribe(address) {
    if (this._connected && this._ws?.readyState === WebSocket.OPEN) {
      this._sendUnsubscribe(address);
    }
    this._subscriptions.delete(address);
    logger.info('[BirdeyeWS] 🔕 取消价格订阅 %s，剩余 %d 个', address.slice(0, 8) + '...', this._subscriptions.size);
  }

  /** 获取最新缓存价格（WS 推送的），10秒内有效 */
  getCachedPrice(address) {
    const sub = this._subscriptions.get(address);
    if (!sub || !sub.price || Date.now() - sub.ts > 10000) return null;
    return sub.price;
  }

  isConnected() { return this._connected; }

  // ── 连接管理 ────────────────────────────────────────────────

  _connect() {
    if (this._stopping) return;

    const safeUrl = WS_URL.replace(/x-api-key=[^&]+/, 'x-api-key=***');
    logger.info('[BirdeyeWS] 连接 %s ...', safeUrl);

    this._ws = new WebSocket(WS_URL);

    this._ws.on('open', () => {
      logger.info('[BirdeyeWS] ✅ WebSocket 已连接');
      this._connected  = true;
      this._retryCount = 0;

      this._pingTimer = setInterval(() => {
        if (this._ws?.readyState === WebSocket.OPEN) this._ws.ping();
      }, PING_INTERVAL_MS);

      // 重新订阅所有 token
      for (const address of this._subscriptions.keys()) {
        this._sendSubscribe(address);
      }
    });

    this._ws.on('message', (data) => this._handleMessage(data));
    this._ws.on('pong', () => {});

    this._ws.on('error', (err) => {
      logger.error('[BirdeyeWS] 错误: %s', err.message);
    });

    this._ws.on('close', () => {
      logger.warn('[BirdeyeWS] 连接关闭');
      this._connected = false;
      if (this._pingTimer) { clearInterval(this._pingTimer); this._pingTimer = null; }

      if (!this._stopping) {
        this._retryCount++;
        const delay = Math.min(RECONNECT_BASE_MS * Math.pow(1.5, this._retryCount - 1), MAX_RECONNECT_MS);
        logger.info('[BirdeyeWS] %ds 后重连 (第%d次)', (delay / 1000).toFixed(0), this._retryCount);
        setTimeout(() => this._connect(), delay);
      }
    });
  }

  _sendSubscribe(address) {
    if (!this._ws || this._ws.readyState !== WebSocket.OPEN) return;
    this._ws.send(JSON.stringify({
      type: 'SUBSCRIBE_PRICE',
      data: { queryType: 'simple', chartType: '1s', address, currency: 'usd' },
    }));
    logger.debug('[BirdeyeWS] 📡 SUBSCRIBE_PRICE %s', address.slice(0, 8) + '...');
  }

  _sendUnsubscribe(address) {
    if (!this._ws || this._ws.readyState !== WebSocket.OPEN) return;
    this._ws.send(JSON.stringify({
      type: 'UNSUBSCRIBE_PRICE',
      data: { queryType: 'simple', chartType: '1s', address, currency: 'usd' },
    }));
  }

  _handleMessage(rawData) {
    let msg;
    try { msg = JSON.parse(rawData.toString('utf8')); } catch (_) { return; }

    if (msg.type === 'PRICE_DATA' && msg.data) {
      const { address } = msg.data;
      const close = parseFloat(msg.data.c);
      if (!address || !Number.isFinite(close) || close <= 0) return;

      const sub = this._subscriptions.get(address);
      if (!sub) return;

      const now = Date.now();
      sub.price = close;
      sub.ts    = now;

      const ohlcv = {
        open:   parseFloat(msg.data.o || close),
        high:   parseFloat(msg.data.h || close),
        low:    parseFloat(msg.data.l || close),
        volume: parseFloat(msg.data.v || 0),
      };

      for (const cb of sub.callbacks) {
        try { cb(close, now, ohlcv); } catch (err) {
          logger.error('[BirdeyeWS] 价格回调错误: %s', err.message);
        }
      }
    }
  }
}

// 单例
const priceStream = new BirdeyePriceStream();

// ── FDV 缓存 ──────────────────────────────────────────────────────

const _fdvCache = new Map();

// ── HTTP 兜底函数 ──────────────────────────────────────────────────

async function getPrice(address) {
  // 优先用 WS 缓存
  const cached = priceStream.getCachedPrice(address);
  if (cached !== null) return cached;

  // HTTP 兜底（带正确的超时）
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 5000);
  try {
    const url = `${BASE}/defi/price?address=${address}`;
    const res = await fetch(url, {
      headers: { 'X-API-KEY': BIRDEYE_KEY, 'x-chain': 'solana' },
      signal: controller.signal,
    });
    if (!res.ok) throw new Error(`Birdeye price error: ${res.status}`);
    const json = await res.json();
    if (!json.success || !json.data) throw new Error('Birdeye price 返回异常');
    return json.data.value;
  } finally {
    clearTimeout(timeout);
  }
}

async function getFdv(address) {
  const overview = await getTokenOverview(address);
  return overview?.fdv ?? null;
}

/**
 * 获取 token 的 FDV 和 LP（流动性），共用同一个缓存
 * @returns {{ fdv: number|null, lp: number|null }}
 */
async function getTokenOverview(address) {
  const cached = _fdvCache.get(address);
  if (cached && Date.now() - cached.ts < FDV_CACHE_MS) return { fdv: cached.fdv, lp: cached.lp };

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 5000);
  try {
    const url = `${BASE}/defi/token_overview?address=${address}`;
    const res = await fetch(url, {
      headers: { 'X-API-KEY': BIRDEYE_KEY, 'x-chain': 'solana' },
      signal: controller.signal,
    });
    if (!res.ok) {
      logger.warn('[Birdeye] token_overview %s 返回 %d', address, res.status);
      return { fdv: cached?.fdv ?? null, lp: cached?.lp ?? null };
    }
    const json = await res.json();
    const fdv = json?.data?.fdv ?? json?.data?.mc ?? null;
    const lp  = json?.data?.liquidity ?? null;
    _fdvCache.set(address, { fdv, lp, ts: Date.now() });
    return { fdv, lp };
  } catch (err) {
    logger.warn('[Birdeye] getTokenOverview %s 失败: %s', address, err.message);
    return { fdv: cached?.fdv ?? null, lp: cached?.lp ?? null };
  } finally {
    clearTimeout(timeout);
  }
}

function clearCache(address) {
  _fdvCache.delete(address);
}

module.exports = { getPrice, getFdv, getTokenOverview, clearCache, priceStream };
