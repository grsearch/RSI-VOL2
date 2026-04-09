'use strict';
// src/heliusWs.js — Helius Enhanced WebSocket 链上交易监听
//
// 架构：
//   一个 WebSocket 连接 → 订阅 Pump AMM program
//   → 收到所有 Pump AMM 的 swap 交易
//   → 从 preTokenBalances/postTokenBalances 中匹配当前监控的 token
//   → 提取 SOL 成交额 + 买/卖方向
//   → 回调通知 monitor
//
// 优势：
//   - 只需一个 WebSocket 连接（不需要每个 token 单独订阅）
//   - 实时拿到链上真实成交数据
//   - 支持 Enhanced WebSocket URL (atlas-mainnet)

const WebSocket = require('ws');
const logger    = require('./logger');

// ── 配置 ────────────────────────────────────────────────────────

// 优先使用 HELIUS_WSS_URL（Enhanced WebSocket URL）
// 其次从 HELIUS_RPC_URL 中提取 api-key 拼接
// 最后用 HELIUS_API_KEY
const HELIUS_WSS_URL = process.env.HELIUS_WSS_URL || '';
const HELIUS_API_KEY = process.env.HELIUS_API_KEY || '';
const HELIUS_RPC_URL = process.env.HELIUS_RPC_URL || '';

function getWsUrl() {
  // 1. 直接配置了 Enhanced WSS URL
  if (HELIUS_WSS_URL) return HELIUS_WSS_URL;

  // 2. 从 RPC URL 提取 api-key
  const m = HELIUS_RPC_URL.match(/api-key=([a-f0-9-]+)/i);
  const apiKey = HELIUS_API_KEY || (m ? m[1] : '');
  if (!apiKey) return '';

  // 用 atlas-mainnet（Enhanced WebSocket 端点）
  return `wss://atlas-mainnet.helius-rpc.com/?api-key=${apiKey}`;
}

// Pump AMM Program ID
const PUMP_AMM_PROGRAM = 'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA';

const LAMPORTS     = 1e9;
const PING_MS      = 30000;    // 30秒 ping 保活
const RECONNECT_MS = 3000;
const MAX_RETRIES  = 999;      // 持续重连

// ── HeliusTradeStream ───────────────────────────────────────────

class HeliusTradeStream {
  constructor() {
    this._ws          = null;
    this._pingTimer   = null;
    this._connected   = false;
    this._retryCount  = 0;
    this._subId       = null;    // Pump AMM 的全局订阅 ID

    // 当前监控的 token: address → { symbol, onTrade }
    this._tokens = new Map();

    // 统计
    this._stats = { txReceived: 0, txMatched: 0, txParsed: 0 };
  }

  // ── 生命周期 ────────────────────────────────────────────────

  start() {
    const wsUrl = getWsUrl();
    if (!wsUrl) {
      logger.warn('[HeliusWS] ⚠️ 未配置 Helius WebSocket URL，链上量能数据不可用');
      logger.warn('[HeliusWS]    设置 HELIUS_WSS_URL=wss://atlas-mainnet.helius-rpc.com/?api-key=YOUR_KEY');
      logger.warn('[HeliusWS]    或设置 HELIUS_API_KEY / HELIUS_RPC_URL');
      return;
    }

    this._connect(wsUrl);
  }

  stop() {
    this._connected = false;
    this._retryCount = MAX_RETRIES + 1; // 阻止重连
    if (this._pingTimer) { clearInterval(this._pingTimer); this._pingTimer = null; }
    if (this._ws) {
      try { this._ws.close(); } catch (_) {}
      this._ws = null;
    }
  }

  // ── 连接管理 ────────────────────────────────────────────────

  _connect(wsUrl) {
    // 日志中隐藏 api-key
    const safeUrl = wsUrl.replace(/api-key=[a-f0-9-]+/i, 'api-key=***');
    logger.info('[HeliusWS] 连接 %s ...', safeUrl);

    this._ws = new WebSocket(wsUrl);

    this._ws.on('open', () => {
      logger.info('[HeliusWS] ✅ Enhanced WebSocket 已连接');
      this._connected  = true;
      this._retryCount = 0;

      // 心跳
      this._pingTimer = setInterval(() => {
        if (this._ws && this._ws.readyState === WebSocket.OPEN) {
          this._ws.ping();
        }
      }, PING_MS);

      // 订阅 Pump AMM program 的所有交易
      this._subscribePumpAmm();
    });

    this._ws.on('message', (data) => {
      this._handleMessage(data);
    });

    this._ws.on('error', (err) => {
      logger.error('[HeliusWS] 错误: %s', err.message);
    });

    this._ws.on('close', () => {
      logger.warn('[HeliusWS] 连接关闭');
      this._connected = false;
      this._subId = null;
      if (this._pingTimer) { clearInterval(this._pingTimer); this._pingTimer = null; }

      // 自动重连
      if (this._retryCount < MAX_RETRIES) {
        this._retryCount++;
        const delay = Math.min(RECONNECT_MS * Math.pow(1.5, this._retryCount - 1), 30000);
        logger.info('[HeliusWS] %ds 后重连 (第%d次)', (delay / 1000).toFixed(0), this._retryCount);
        setTimeout(() => {
          const url = getWsUrl();
          if (url) this._connect(url);
        }, delay);
      }
    });
  }

  // ── Pump AMM 全局订阅 ──────────────────────────────────────

  _subscribePumpAmm() {
    if (!this._ws || this._ws.readyState !== WebSocket.OPEN) return;

    const request = {
      jsonrpc: '2.0',
      id: 1,
      method: 'transactionSubscribe',
      params: [
        {
          accountInclude: [PUMP_AMM_PROGRAM],
          failed: false,
        },
        {
          commitment: 'confirmed',
          encoding: 'jsonParsed',
          transactionDetails: 'full',
          maxSupportedTransactionVersion: 0,
        },
      ],
    };

    this._ws.send(JSON.stringify(request));
    logger.info('[HeliusWS] 📡 订阅 Pump AMM (%s)', PUMP_AMM_PROGRAM.slice(0, 8) + '...');
  }

  // ── Token 注册（不发送额外订阅，只注册回调） ────────────────

  /**
   * 注册一个 token 的回调，当链上有该 token 的交易时触发
   */
  subscribe(tokenAddress, symbol, onTrade) {
    this._tokens.set(tokenAddress, { symbol, onTrade });
    logger.info('[HeliusWS] 📌 注册 token %s (%s)，当前监控 %d 个',
      symbol, tokenAddress.slice(0, 8) + '...', this._tokens.size);
  }

  unsubscribe(tokenAddress) {
    this._tokens.delete(tokenAddress);
    logger.info('[HeliusWS] 🔕 移除 token %s，剩余 %d 个',
      tokenAddress.slice(0, 8) + '...', this._tokens.size);
  }

  // ── 消息处理 ──────────────────────────────────────────────

  _handleMessage(rawData) {
    let msg;
    try {
      msg = JSON.parse(rawData.toString('utf8'));
    } catch (_) {
      return;
    }

    // 订阅确认
    if (msg.id && msg.result !== undefined) {
      this._subId = msg.result;
      logger.info('[HeliusWS] ✅ Pump AMM 订阅确认，subId=%s', msg.result);
      return;
    }

    // 交易通知
    if (msg.method === 'transactionNotification' && msg.params?.result) {
      this._stats.txReceived++;
      this._parseTransaction(msg.params.result);
    }
  }

  // ── 交易解析 ──────────────────────────────────────────────

  _parseTransaction(result) {
    try {
      const { transaction: txWrapper, signature } = result;
      if (!txWrapper) return;

      const meta = txWrapper.meta;
      const txData = txWrapper.transaction;
      if (!meta || meta.err) return; // 跳过失败交易

      const postTokenBals = meta.postTokenBalances || [];
      if (postTokenBals.length === 0) return;

      // 找出这笔交易涉及的 token mints
      const involvedMints = new Set(postTokenBals.map(b => b.mint).filter(Boolean));

      // 跟当前监控列表匹配
      for (const mint of involvedMints) {
        const tokenInfo = this._tokens.get(mint);
        if (!tokenInfo) continue; // 不是我们监控的 token

        this._stats.txMatched++;
        const trade = this._extractTrade(mint, meta, txData, signature);
        if (trade) {
          this._stats.txParsed++;
          tokenInfo.onTrade(trade);
        }
      }
    } catch (err) {
      logger.debug('[HeliusWS] 解析交易失败: %s', err.message);
    }
  }

  /**
   * 从交易中提取 swap 数据
   *
   * 关键逻辑：
   *   preTokenBalances/postTokenBalances 中找 mint === tokenAddress 的条目
   *   → token 余额变化 > 0 = 用户买入（BUY）
   *   → token 余额变化 < 0 = 用户卖出（SELL）
   *   对应的 SOL（lamports）变化量 = 成交金额
   */
  _extractTrade(tokenAddress, meta, txData, signature) {
    const preTokenBals  = meta.preTokenBalances  || [];
    const postTokenBals = meta.postTokenBalances  || [];
    const preBalances   = meta.preBalances  || [];
    const postBalances  = meta.postBalances || [];

    // account keys
    let accountKeys = [];
    if (txData?.message?.accountKeys) {
      accountKeys = txData.message.accountKeys.map(k =>
        typeof k === 'string' ? k : k.pubkey
      );
    }

    // 找该 token 的 post entries
    const postEntries = postTokenBals.filter(b => b.mint === tokenAddress);
    const preEntries  = preTokenBals.filter(b => b.mint === tokenAddress);

    if (postEntries.length === 0) return null;

    for (const postEntry of postEntries) {
      const owner = postEntry.owner;
      if (!owner) continue;

      // 跳过已知的 AMM/pool 地址（owner 通常在第 0 或 1 位，是 signer/fee payer）
      // Pool 的 owner 一般不会是交易的 signer
      const ownerIndex = accountKeys.indexOf(owner);
      if (ownerIndex < 0 || ownerIndex >= preBalances.length) continue;

      // 找 pre entry
      const preEntry = preEntries.find(
        b => b.accountIndex === postEntry.accountIndex || b.owner === owner
      );

      // token 变化量
      const postAmt = parseFloat(postEntry.uiTokenAmount?.uiAmount ?? '0');
      const preAmt  = preEntry ? parseFloat(preEntry.uiTokenAmount?.uiAmount ?? '0') : 0;
      const tokenDelta = postAmt - preAmt;

      if (Math.abs(tokenDelta) < 1e-12) continue;

      // SOL 变化量（lamports → SOL）
      const solDelta = (postBalances[ownerIndex] - preBalances[ownerIndex]) / LAMPORTS;

      // 判断方向
      // BUY: token↑ SOL↓（用户花 SOL 买 token）
      // SELL: token↓ SOL↑（用户卖 token 得 SOL）
      const isBuy  = tokenDelta > 0 && solDelta < 0;
      const isSell = tokenDelta < 0 && solDelta > 0;

      if (!isBuy && !isSell) continue;

      const solAmount   = Math.abs(solDelta);
      const tokenAmount = Math.abs(tokenDelta);
      const priceSol    = tokenAmount > 0 ? solAmount / tokenAmount : 0;

      return {
        ts: Date.now(),
        signature,
        tokenAddress,
        owner,
        isBuy,
        solAmount,
        tokenAmount,
        priceSol,
      };
    }

    return null;
  }

  // ── 状态查询 ──────────────────────────────────────────────

  isConnected() {
    return this._connected;
  }

  getSubscriptionCount() {
    return this._tokens.size;
  }

  getStats() {
    return {
      connected: this._connected,
      subscribed: this._subId !== null,
      tokens: this._tokens.size,
      retryCount: this._retryCount,
      ...this._stats,
    };
  }
}

// 单例
const heliusWs = new HeliusTradeStream();
module.exports = heliusWs;
