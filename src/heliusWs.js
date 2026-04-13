'use strict';
// src/heliusWs.js — Helius Enhanced WebSocket 链上交易监听
//
// V3.4 — 两种订阅模式可选（通过 HELIUS_SUB_MODE 配置）
//
// ★ 模式 "token"（默认，最省 credits）：
//   每个监控 token 单独 transactionSubscribe(accountInclude: [mint])
//   只收到该 token 相关交易，credits 消耗最低
//   适合同时监控 ≤20 个 token
//
// ★ 模式 "pump"（单订阅，适合 Pump.fun 迁移币）：
//   一个 transactionSubscribe(accountInclude: [PumpAMM])
//   收到所有 Pump AMM 的交易，本地按 token mint 过滤
//   优点：添加/移除 token 无需重新订阅，延迟更稳定
//   缺点：Pump AMM 每秒 ~50-100 笔交易全量推送，credits 比精准模式多
//
// credits 对比（监控 5 个 Pump AMM token）：
//   "token" 模式：~10 笔/秒 × ~0.5KB ≈ 100 credits/s ≈ 860万/天
//   "pump"  模式：~80 笔/秒 × ~0.5KB ≈ 800 credits/s ≈ 6900万/天
//   旧三 program：~600 笔/秒 × ~0.5KB ≈ 6000 credits/s ≈ 5.2亿/天

const WebSocket = require('ws');
const logger    = require('./logger');

// ── 配置 ────────────────────────────────────────────────────────

const HELIUS_WSS_URL         = process.env.HELIUS_WSS_URL || '';
const HELIUS_GATEKEEPER_URL  = process.env.HELIUS_GATEKEEPER_URL || '';
const HELIUS_API_KEY         = process.env.HELIUS_API_KEY || '';
const HELIUS_RPC_URL         = process.env.HELIUS_RPC_URL || '';
const SUB_MODE               = (process.env.HELIUS_SUB_MODE || 'token').toLowerCase(); // "token" | "pump"

function getWsUrl() {
  if (HELIUS_GATEKEEPER_URL) {
    let url = HELIUS_GATEKEEPER_URL;
    if (url.startsWith('https://')) url = url.replace('https://', 'wss://');
    if (!url.startsWith('wss://')) url = `wss://${url}`;
    return { url, type: 'gatekeeper' };
  }
  if (HELIUS_WSS_URL) {
    return { url: HELIUS_WSS_URL, type: 'enhanced' };
  }
  const apiKey = HELIUS_API_KEY || extractApiKey(HELIUS_RPC_URL);
  if (!apiKey) return { url: '', type: 'none' };
  return { url: `wss://mainnet.helius-rpc.com/?api-key=${apiKey}`, type: 'enhanced' };
}

function extractApiKey(rpcUrl) {
  const m = (rpcUrl || '').match(/api-key=([a-f0-9-]+)/i);
  return m ? m[1] : '';
}

const PUMP_AMM_PROGRAM = 'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA';

const LAMPORTS     = 1e9;
const PING_MS      = 25000;
const RECONNECT_MS = 2000;
const MAX_RETRIES  = 999;

// ── HeliusTradeStream ───────────────────────────────────────────

class HeliusTradeStream {
  constructor() {
    this._ws          = null;
    this._pingTimer   = null;
    this._connected   = false;
    this._retryCount  = 0;
    this._connType    = 'none';

    // token → { symbol, onTrade, subId, rpcId }
    this._tokens = new Map();

    // rpcId → tokenAddress | '__pump__'（用于匹配订阅确认）
    this._pendingSubs = new Map();
    this._nextRpcId = 100;

    // pump 模式的 subId
    this._pumpSubId = null;

    this._stats = { txReceived: 0, txMatched: 0, txParsed: 0, txSkipped: 0, connType: 'none', subMode: SUB_MODE };
  }

  // ── 生命周期 ────────────────────────────────────────────────

  start() {
    const { url, type } = getWsUrl();
    if (!url) {
      logger.warn('[HeliusWS] ⚠️ 未配置 Helius WebSocket URL，链上量能数据不可用');
      return;
    }
    this._connType = type;
    this._stats.connType = type;
    logger.info('[HeliusWS] 订阅模式: %s', SUB_MODE === 'pump' ? 'Pump AMM 单订阅' : '按 Token 精准订阅');
    this._connect(url);
  }

  stop() {
    this._connected = false;
    this._retryCount = MAX_RETRIES + 1;
    if (this._pingTimer) { clearInterval(this._pingTimer); this._pingTimer = null; }
    if (this._ws) {
      try { this._ws.close(); } catch (_) {}
      this._ws = null;
    }
  }

  // ── 连接管理 ────────────────────────────────────────────────

  _connect(wsUrl) {
    const safeUrl = wsUrl.replace(/api-key=[a-f0-9-]+/i, 'api-key=***');
    logger.info('[HeliusWS] 连接 %s (类型: %s) ...', safeUrl, this._connType);

    this._ws = new WebSocket(wsUrl);

    this._ws.on('open', () => {
      logger.info('[HeliusWS] ✅ %s WebSocket 已连接 (模式: %s)', this._connType.toUpperCase(), SUB_MODE);
      this._connected  = true;
      this._retryCount = 0;

      this._pingTimer = setInterval(() => {
        if (this._ws?.readyState === WebSocket.OPEN) this._ws.ping();
      }, PING_MS);

      // 重连后恢复订阅
      if (SUB_MODE === 'pump') {
        this._subscribePumpAmm();
      } else {
        for (const [address, info] of this._tokens.entries()) {
          info.subId = null;
          this._subscribeToken(address);
        }
      }
    });

    this._ws.on('message', (data) => this._handleMessage(data));
    this._ws.on('pong', () => {});
    this._ws.on('error', (err) => logger.error('[HeliusWS] 错误: %s', err.message));

    this._ws.on('close', () => {
      logger.warn('[HeliusWS] 连接关闭');
      this._connected = false;
      this._pendingSubs.clear();
      this._pumpSubId = null;
      if (this._pingTimer) { clearInterval(this._pingTimer); this._pingTimer = null; }

      if (this._retryCount < MAX_RETRIES) {
        this._retryCount++;
        const delay = Math.min(RECONNECT_MS * Math.pow(1.5, this._retryCount - 1), 30000);
        logger.info('[HeliusWS] %ds 后重连 (第%d次)', (delay / 1000).toFixed(0), this._retryCount);
        setTimeout(() => {
          const { url } = getWsUrl();
          if (url) this._connect(url);
        }, delay);
      }
    });
  }

  // ── 订阅：Pump AMM 模式 ───────────────────────────────────

  _subscribePumpAmm() {
    if (!this._ws || this._ws.readyState !== WebSocket.OPEN) return;

    const rpcId = this._nextRpcId++;
    this._pendingSubs.set(rpcId, '__pump__');

    this._ws.send(JSON.stringify({
      jsonrpc: '2.0',
      id: rpcId,
      method: 'transactionSubscribe',
      params: [
        { accountInclude: [PUMP_AMM_PROGRAM], failed: false },
        {
          commitment: 'confirmed',
          encoding: 'jsonParsed',
          transactionDetails: 'full',
          maxSupportedTransactionVersion: 0,
        },
      ],
    }));
    logger.info('[HeliusWS] 📡 订阅 Pump AMM program (单订阅模式)');
  }

  // ── 订阅：按 Token 精准模式 ───────────────────────────────

  _subscribeToken(tokenAddress) {
    if (!this._ws || this._ws.readyState !== WebSocket.OPEN) return;

    const rpcId = this._nextRpcId++;
    this._pendingSubs.set(rpcId, tokenAddress);

    this._ws.send(JSON.stringify({
      jsonrpc: '2.0',
      id: rpcId,
      method: 'transactionSubscribe',
      params: [
        { accountInclude: [tokenAddress], failed: false },
        {
          commitment: 'confirmed',
          encoding: 'jsonParsed',
          transactionDetails: 'full',
          maxSupportedTransactionVersion: 0,
        },
      ],
    }));

    const info = this._tokens.get(tokenAddress);
    if (info) info.rpcId = rpcId;
    logger.info('[HeliusWS] 📡 订阅 token %s (%s)',
      info?.symbol || '?', tokenAddress.slice(0, 8) + '...');
  }

  _unsubscribeToken(tokenAddress) {
    const info = this._tokens.get(tokenAddress);
    if (!info?.subId) return;

    if (this._ws?.readyState === WebSocket.OPEN) {
      const rpcId = this._nextRpcId++;
      this._ws.send(JSON.stringify({
        jsonrpc: '2.0',
        id: rpcId,
        method: 'transactionUnsubscribe',
        params: [info.subId],
      }));
      logger.info('[HeliusWS] 🔕 取消订阅 %s subId=%s', tokenAddress.slice(0, 8) + '...', info.subId);
    }
    info.subId = null;
  }

  // ── 外部接口 ──────────────────────────────────────────────

  subscribe(tokenAddress, symbol, onTrade) {
    this._tokens.set(tokenAddress, { symbol, onTrade, subId: null, rpcId: null });

    // token 模式：立即发送独立订阅
    // pump 模式：不需要额外操作，全局订阅已覆盖
    if (SUB_MODE !== 'pump' && this._connected) {
      this._subscribeToken(tokenAddress);
    }

    logger.info('[HeliusWS] 📌 注册 token %s (%s)，当前监控 %d 个 (模式=%s)',
      symbol, tokenAddress.slice(0, 8) + '...', this._tokens.size, SUB_MODE);
  }

  unsubscribe(tokenAddress) {
    // token 模式：取消独立订阅
    if (SUB_MODE !== 'pump') {
      this._unsubscribeToken(tokenAddress);
    }
    this._tokens.delete(tokenAddress);
    logger.info('[HeliusWS] 🔕 移除 token %s，剩余 %d 个',
      tokenAddress.slice(0, 8) + '...', this._tokens.size);
  }

  // ── 消息处理 ──────────────────────────────────────────────

  _handleMessage(rawData) {
    let msg;
    try { msg = JSON.parse(rawData.toString('utf8')); } catch (_) { return; }

    // 订阅确认
    if (msg.id && msg.result !== undefined) {
      const key = this._pendingSubs.get(msg.id);
      if (key === '__pump__') {
        this._pumpSubId = msg.result;
        this._pendingSubs.delete(msg.id);
        logger.info('[HeliusWS] ✅ Pump AMM 订阅确认 subId=%s', msg.result);
      } else if (key) {
        this._pendingSubs.delete(msg.id);
        const info = this._tokens.get(key);
        if (info) {
          info.subId = msg.result;
          logger.info('[HeliusWS] ✅ 订阅确认 %s (%s) subId=%s',
            info.symbol, key.slice(0, 8) + '...', msg.result);
        }
      }
      return;
    }

    // 交易通知
    if (msg.method === 'transactionNotification' && msg.params?.result) {
      this._stats.txReceived++;
      this._parseTransaction(msg.params.result, msg.params.subscription);
    }
  }

  // ── 交易解析 ──────────────────────────────────────────────

  _parseTransaction(result, subscriptionId) {
    try {
      const { transaction: txWrapper, signature } = result;
      if (!txWrapper) return;

      const meta = txWrapper.meta;
      const txData = txWrapper.transaction;
      if (!meta || meta.err) return;

      const postTokenBals = meta.postTokenBalances || [];
      if (postTokenBals.length === 0) return;

      if (SUB_MODE === 'pump') {
        // ── pump 模式：从交易中提取 mint，匹配监控列表 ──
        const involvedMints = new Set(postTokenBals.map(b => b.mint).filter(Boolean));
        let matched = false;

        for (const mint of involvedMints) {
          const tokenInfo = this._tokens.get(mint);
          if (!tokenInfo) continue;

          matched = true;
          this._stats.txMatched++;
          const trade = this._extractTrade(mint, meta, txData, signature);
          if (trade) {
            this._stats.txParsed++;
            tokenInfo.onTrade(trade);
          }
        }

        if (!matched) this._stats.txSkipped++;
      } else {
        // ── token 模式：通过 subscriptionId 精准匹配 ──
        let targetToken = null;
        for (const [addr, info] of this._tokens.entries()) {
          if (info.subId === subscriptionId) {
            targetToken = { address: addr, ...info };
            break;
          }
        }

        if (targetToken) {
          this._stats.txMatched++;
          const trade = this._extractTrade(targetToken.address, meta, txData, signature);
          if (trade) {
            this._stats.txParsed++;
            targetToken.onTrade(trade);
          }
          return;
        }

        // 兜底：subscriptionId 匹配不到时用 mint 匹配
        const involvedMints = new Set(postTokenBals.map(b => b.mint).filter(Boolean));
        for (const mint of involvedMints) {
          const tokenInfo = this._tokens.get(mint);
          if (!tokenInfo) continue;
          this._stats.txMatched++;
          const trade = this._extractTrade(mint, meta, txData, signature);
          if (trade) {
            this._stats.txParsed++;
            tokenInfo.onTrade(trade);
          }
        }
      }
    } catch (err) {
      logger.debug('[HeliusWS] 解析交易失败: %s', err.message);
    }
  }

  _extractTrade(tokenAddress, meta, txData, signature) {
    const preTokenBals  = meta.preTokenBalances  || [];
    const postTokenBals = meta.postTokenBalances  || [];
    const preBalances   = meta.preBalances  || [];
    const postBalances  = meta.postBalances || [];

    let accountKeys = [];
    if (txData?.message?.accountKeys) {
      accountKeys = txData.message.accountKeys.map(k =>
        typeof k === 'string' ? k : k.pubkey
      );
    }

    const postEntries = postTokenBals.filter(b => b.mint === tokenAddress);
    const preEntries  = preTokenBals.filter(b => b.mint === tokenAddress);
    if (postEntries.length === 0) return null;

    for (const postEntry of postEntries) {
      const owner = postEntry.owner;
      if (!owner) continue;

      const ownerIndex = accountKeys.indexOf(owner);
      if (ownerIndex < 0 || ownerIndex >= preBalances.length) continue;

      const preEntry = preEntries.find(
        b => b.accountIndex === postEntry.accountIndex || b.owner === owner
      );

      const postAmt = parseFloat(postEntry.uiTokenAmount?.uiAmount ?? '0');
      const preAmt  = preEntry ? parseFloat(preEntry.uiTokenAmount?.uiAmount ?? '0') : 0;
      const tokenDelta = postAmt - preAmt;
      if (Math.abs(tokenDelta) < 1e-12) continue;

      const solDelta = (postBalances[ownerIndex] - preBalances[ownerIndex]) / LAMPORTS;
      const isBuy  = tokenDelta > 0 && solDelta < 0;
      const isSell = tokenDelta < 0 && solDelta > 0;
      if (!isBuy && !isSell) continue;

      return {
        ts: Date.now(),
        signature,
        tokenAddress,
        owner,
        isBuy,
        solAmount:   Math.abs(solDelta),
        tokenAmount: Math.abs(tokenDelta),
        priceSol:    Math.abs(tokenDelta) > 0 ? Math.abs(solDelta) / Math.abs(tokenDelta) : 0,
      };
    }
    return null;
  }

  // ── 状态查询 ──────────────────────────────────────────────

  isConnected() { return this._connected; }
  getSubscriptionCount() { return this._tokens.size; }

  getStats() {
    let confirmedSubs = 0;
    for (const info of this._tokens.values()) {
      if (info.subId) confirmedSubs++;
    }
    return {
      connected:     this._connected,
      connType:      this._connType,
      subMode:       SUB_MODE,
      tokens:        this._tokens.size,
      confirmedSubs: SUB_MODE === 'pump' ? (this._pumpSubId ? 1 : 0) : confirmedSubs,
      retryCount:    this._retryCount,
      ...this._stats,
    };
  }
}

const heliusWs = new HeliusTradeStream();
module.exports = heliusWs;
