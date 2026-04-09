# SOL RSI+量能 Monitor V2

Solana PUMP 新迁移币 RSI(7)+链上量能 策略监控机器人。

**15秒K线 · 1秒轮询 · Helius 链上成交数据 · 15分钟监控窗口 · 空跑/回测/实盘**

---

## 策略逻辑

### 买入条件（同时满足）

| # | 条件 | 说明 |
|---|------|------|
| 1 | RSI(7) ≤ 30 | 当前处于超卖区间 |
| 2 | 过去30秒内 buyVolume > sellVolume | 链上资金净流入（Helius WS 实时数据） |
| 3 | 已过前8根K线（约2分钟） | 跳过迁移后初期噪音 |

> 不等 RSI "上穿" 30，只要 RSI 在超卖区 + 资金开始净流入就立即入场。

### 卖出条件（优先级从高到低，命中即卖）

| 优先级 | 条件 | 行为 |
|--------|------|------|
| 1 | RSI(7) > 80 | 恐慌卖出 |
| 2 | RSI(7) 下穿 70 | 全仓卖出 |
| 3 | 涨幅 ≥ +50% | 止盈 |
| 4 | 跌幅 ≤ -10% | 止损 |
| 5 | 连续2根K线量能萎缩（< 均量） | 量能出场 |
| 6 | FDV 跌破 $10,000 | 清仓退出 |
| 7 | 监控满15分钟 | 到期退出 |

### 仓位管理

- 监控期内只做一笔交易
- 卖出后立即退出该代币监控

---

## 数据来源

| 数据 | 来源 | 用途 |
|------|------|------|
| USD 价格 | Birdeye API（1秒轮询） | RSI 计算、止盈止损 |
| 链上成交量 + 买卖方向 | Helius Enhanced WebSocket | buyVolume / sellVolume（量能过滤） |
| FDV / LP | Birdeye token_overview（30秒缓存） | 入场过滤、FDV 退出 |

> Helius 未配置时，量能过滤自动退化为纯 RSI（buy/sell 数据为0时条件放行）。

---

## 快速开始

### 1. 配置

```bash
cp .env.example .env
```

必填：
```
BIRDEYE_API_KEY=你的Key
HELIUS_WSS_URL=wss://atlas-mainnet.helius-rpc.com/?api-key=你的Key
HELIUS_RPC_URL=https://mainnet.helius-rpc.com/?api-key=你的Key
```

### 2. 空跑模式（推荐先跑一天）

```bash
# .env 中确保：
DRY_RUN=true
# WALLET_PRIVATE_KEY 可留空

npm install
npm start
```

### 3. 回测

```bash
# 默认参数
npm run backtest

# 自定义参数
node src/backtest.js --rsi-buy=25 --vol-window-sec=60 --stop-loss=-8

# 网格搜索最优参数
node src/backtest.js --grid

# 只回测某个 token
node src/backtest.js --address=TOKEN_ADDRESS
```

### 4. 切换实盘

```bash
# .env 修改：
DRY_RUN=false
WALLET_PRIVATE_KEY=你的私钥
JUPITER_API_KEY=你的Jupiter Key

npm start
```

### 5. Dashboard

```
http://YOUR_SERVER:3001
```

---

## 回测参数

| 参数 | 命令行 | 默认值 | 说明 |
|------|--------|--------|------|
| RSI 超卖阈值 | `--rsi-buy=25` | 30 | RSI ≤ 此值视为超卖区 |
| 量能窗口 | `--vol-window-sec=60` | 30 | buy>sell 的统计窗口（秒） |
| 跳过K线 | `--skip-first=4` | 8 | 跳过前N根K线 |
| 止损 | `--stop-loss=-8` | -10 | 止损百分比 |
| 止盈 | `--take-profit=30` | 50 | 止盈百分比 |
| K线宽度 | `--kline-sec=10` | 15 | K线秒数 |
| 关闭量能 | `--vol-enabled=false` | true | 对比纯RSI效果 |

---

## 环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `DRY_RUN` | `true` | 空跑模式（不实际交易） |
| `DRY_RUN_DATA_DIR` | `./data` | 数据持久化目录 |
| `HELIUS_WSS_URL` | - | Helius Enhanced WebSocket URL |
| `KLINE_INTERVAL_SEC` | `15` | K线宽度（秒） |
| `VOL_ENABLED` | `true` | 启用量能过滤 |
| `VOL_WINDOW_SEC` | `30` | buyVol>sellVol 统计窗口（秒） |
| `VOL_EXIT_CONSECUTIVE` | `2` | 量能萎缩连续K线数 |
| `VOL_EXIT_RATIO` | `1.0` | 萎缩阈值（当前量 < 均量×此值） |
| `VOL_EXIT_LOOKBACK` | `4` | 萎缩出场均量回看K线数 |
| `SKIP_FIRST_CANDLES` | `8` | 跳过前N根K线 |
| `TAKE_PROFIT_PCT` | `50` | 止盈% |
| `STOP_LOSS_PCT` | `-10` | 止损% |
| `RSI_PERIOD` | `7` | RSI 周期 |
| `RSI_BUY_LEVEL` | `30` | 超卖阈值 |
| `RSI_SELL_LEVEL` | `70` | RSI 下穿此值卖出 |
| `RSI_PANIC_LEVEL` | `80` | RSI 超过此值立即卖出 |
| `MIN_FDV_USD` | `15000` | 入场FDV过滤 |
| `MIN_LP_USD` | `5000` | 入场LP过滤 |
| `FDV_EXIT_USD` | `10000` | 监控中FDV退出阈值 |
| `TOKEN_MAX_AGE_MINUTES` | `15` | 最大监控时长 |
| `TRADE_SIZE_SOL` | `0.2` | 每笔买入SOL |

---

## 目录结构

```
sol-rsi/
├── src/
│   ├── index.js          # 主入口
│   ├── monitor.js        # 核心引擎（DRY_RUN + 数据持久化）
│   ├── rsi.js            # RSI + 量能过滤（buy>sell）+ 信号
│   ├── heliusWs.js       # Helius Enhanced WS（链上成交数据）
│   ├── trader.js         # Jupiter Ultra API 交易
│   ├── birdeye.js        # Birdeye 价格/FDV
│   ├── dataStore.js      # 数据持久化
│   ├── backtest.js       # 回测引擎 + 网格搜索
│   ├── reporter.js       # 每日CSV报告
│   ├── wsHub.js          # Dashboard WebSocket
│   ├── logger.js         # 日志
│   └── routes/
│       ├── webhook.js    # POST /webhook/add-token
│       └── dashboard.js  # REST API
├── public/
│   └── index.html        # 实时 Dashboard
├── data/                  # 空跑数据（自动创建）
│   ├── ticks/
│   ├── trades.json
│   └── signals.json
├── .env.example
└── package.json
```

---

## 建议工作流

1. **Day 1**：`DRY_RUN=true` 空跑收集数据
2. **Day 2**：`npm run backtest -- --grid` 网格搜索
3. **调参**：根据回测结果调整 `.env`
4. **Day 3+**：确认盈亏比 > 1 后 `DRY_RUN=false` 实盘
