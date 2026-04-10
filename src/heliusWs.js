'use strict';
// src/heliusWs.js — Helius Enhanced WebSocket 链上交易监听
//
// V3.1 修复：
//   1. 修复 LaserStream URL 处理 — LaserStream 是 gRPC 服务，不能用 ws 库直接连
//      LaserStream URL 仅用于 trader.js 的 RPC 发单，不用于 WebSocket 订阅
//   2. WebSocket 连接优先级：
//      Gatekeeper WSS (beta, 最快) > HELIUS_WSS_URL > 统一端点
//   3. 同时订阅 Pump AMM + Raydium V4 + Raydium CPMM
//   4. 10分钟 inactivity 保活（Helius Enhanced WS 的断线阈值）
//   5. 支持 accountInclude 最多 50,000 地址

const WebSocket = require('ws');
const logger    = require('./logger');

// ── 配置 ────────────────────────────────────────────────────────

const HELIUS_WSS_URL         = process.env.HELIUS_WSS_URL || '';
const HELIUS_GATEKEEPER_URL  = process.env.HELIUS_GATEKEEPER_URL || '';
const HELIUS_API_KEY         = process.env.HELIUS_API_KEY || '';
const HELIUS_RPC_URL         = process.env.HELIUS_RPC_URL || '';

function getWsUrl() {
  // 注意：LaserStream (HELIUS_LASERSTREAM_URL) 是 gRPC 协议，
  // 不能用 ws 库连接，仅用于 trader.js 的 sendTransaction。
  // WebSocket 订阅必须用 Enhanced WSS 端点。

  // 1. Gatekeeper WSS (beta) — 最低延迟 WebSocket
  //    文档: wss://beta.helius-rpc.com/?api-key=YOUR_KEY
  if (HELIUS_GATEKEEPER_URL) {
    let url = HELIUS_GATEKEEPER_URL;
    // 将 https:// 转为 wss://（Gatekeeper 支持 WebSocket 协议）
    if (url.startsWith('https://')) url = url.replace('https://', 'wss://');
    if (!url.startsWith('wss://')) url = `wss://${url}`;
    return { url, type: 'gatekeeper' };
  }

  // 2. 直接配置的 Enhanced WebSocket URL
  if (HELIUS_WSS_URL) {
    return { url: HELIUS_WSS_URL, type: 'enhanced' };
  }

  // 3. 从 API Key / RPC URL 拼接统一端点
  //    统一端点 = Standard + Enhanced WSS 合一
  const apiKey = HELIUS_API_KEY || extractApiKey(HELIUS_RPC_URL);
  if (!apiKey) return { url: '', type: 'none' };

  return {
    url: `wss://mainnet.helius-rpc.com/?api-key=${apiKey}`,
    type: 'enhanced',
  };
}

function extractApiKey(rpcUrl) {
  const m = (rpcUrl || '').match(/api-key=([a-f0-9-]+)/i);
  return m ? m[1] : '';
}

// 已知 DEX Program IDs
const PUMP_AMM_PROGRAM  = 'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA';
const RAYDIUM_V4_PROGRAM = '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8';
const RAYDIUM_CPMM       = 'CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C';

const LAMPORTS     = 1e9;
const PING_MS      = 25000;    // 25秒 ping
const RECONNECT_MS = 2000;
const MAX_RETRIES  = 999;

// ── HeliusTradeStream ───────────────────────────────────────────

class HeliusTradeStream {
  constructor() {
    this._ws          = null;
    this._pingTimer   = null;
    this._connected   = false;
    this._retryCount  = 0;
    this._subIds      = new Map();  // program → subId
    this._connType    = 'none';     // laserstream / enhanced

    // 当前监控的 token: address → { symbol, onTrade }
    this._tokens = new Map();

    // 统计
    this._stats = { txReceived: 0, txMatched: 0, txParsed: 0, connType: 'none' };
  }

  // ── 生命周期 ────────────────────────────────────────────────

  start() {
    const { url, type } = getWsUrl();
    if (!url) {
      logger.warn('[HeliusWS] ⚠️ 未配置 Helius WebSocket URL，链上量能数据不可用');
      logger.warn('[HeliusWS]    设置 HELIUS_WSS_URL 或 HELIUS_GATEKEEPER_URL 或 HELIUS_API_KEY');
      return;
    }

    this._connType = type;
    this._stats.connType = type;
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
      logger.info('[HeliusWS] ✅ %s WebSocket 已连接', this._connType.toUpperCase());
      this._connected  = true;
      this._retryCount = 0;

      this._pingTimer = setInterval(() => {
        if (this._ws?.readyState === WebSocket.OPEN) this._ws.ping();
      }, PING_MS);

      // 订阅多个 DEX program
      this._subscribeProgram(PUMP_AMM_PROGRAM, 'PumpAMM', 1);
      this._subscribeProgram(RAYDIUM_V4_PROGRAM, 'RaydiumV4', 2);
      this._subscribeProgram(RAYDIUM_CPMM, 'RaydiumCPMM', 3);
    });

    this._ws.on('message', (data) => this._handleMessage(data));
    this._ws.on('pong', () => {});

    this._ws.on('error', (err) => {
      logger.error('[HeliusWS] 错误: %s', err.message);
    });

    this._ws.on('close', () => {
      logger.warn('[HeliusWS] 连接关闭');
      this._connected = false;
      this._subIds.clear();
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

  // ── DEX Program 订阅 ──────────────────────────────────────

  _subscribeProgram(programId, label, rpcId) {
    if (!this._ws || this._ws.readyState !== WebSocket.OPEN) return;

    const request = {
      jsonrpc: '2.0',
      id: rpcId,
      method: 'transactionSubscribe',
      params: [
        {
          accountInclude: [programId],
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
    logger.info('[HeliusWS] 📡 订阅 %s (%s)', label, programId.slice(0, 8) + '...');
  }

  // ── Token 注册 ────────────────────────────────────────────

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
    try { msg = JSON.parse(rawData.toString('utf8')); } catch (_) { return; }

    // 订阅确认
    if (msg.id && msg.result !== undefined) {
      this._subIds.set(msg.id, msg.result);
      logger.info('[HeliusWS] ✅ 订阅确认 id=%d subId=%s', msg.id, msg.result);
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
      if (!meta || meta.err) return;

      const postTokenBals = meta.postTokenBalances || [];
      if (postTokenBals.length === 0) return;

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

  isConnected() { return this._connected; }
  getSubscriptionCount() { return this._tokens.size; }

  getStats() {
    return {
      connected:  this._connected,
      connType:   this._connType,
      subscribed: this._subIds.size > 0,
      programs:   this._subIds.size,
      tokens:     this._tokens.size,
      retryCount: this._retryCount,
      ...this._stats,
    };
  }
}

// 单例
const heliusWs = new HeliusTradeStream();
module.exports = heliusWs;
