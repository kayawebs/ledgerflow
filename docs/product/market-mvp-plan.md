# LedgerFlow Market MVP 开发计划

日期：2026-09-17
分支：`feat/market-mvp`（基线 tag：`market-mvp-baseline`）
范围：Robinhood Chain（4663）Market Web（`src/market_web`，端口 3001）及其后台数据链路
数据源约束：**只使用本地 RPC（robinhood-nitro `127.0.0.1:8547` / rpc-gateway）和 LedgerFlow 自有数据库**，不引入第三方行情/索引 API。

---

## 1. 现状审计（2026-09-17 实测）

### 1.1 数据面

| 数据 | 规模 / 状态 | 结论 |
|---|---|---|
| Pons 发币 `protocol_token_launches` | 786k（V1 266k 已停止于 08-12；V2 520k，每日 1–3 万新币，8.4k 已毕业） | MVP 聚焦 **Pons V2** |
| 曲线成交 `protocol_launch_trade_facts` | 2400 万条，46 GB；24h 约 44 万笔、2.7 万币、2.5 万地址；游标实时 | 数据充足 |
| 指标快照 `protocol_launch_metric_snapshots` | 每分钟追加一行/币/窗口，无保留策略；“24h 窗口”在币停止交易后不会衰减 | **有正确性问题**，需要替换 |
| 参与者盈亏 `protocol_launch_participant_outcomes` | 仅 24h 窗口、仅 `estimated`，报价单位（非 USD），无未实现盈亏 | 需要重做为持仓模型 |
| 报价资产 | ETH(0x0) 28 万、WETH 27 万、USDG 6 万、NVDA/SPCX/SPY 等股票代币若干 | **跨币的量不能直接相加 → 必须统一换算 USD** |
| USD 价格 `price_observations` | price-worker 从本地链上成交推导 WETH/USDG/股票代币 USD 价格，24h 内持续更新 | 可作为 USD 换算来源（仅本地数据） |
| 毕业后行情 | V2 毕业进入 Uniswap V4 池，`pool_id` 未推导，无毕业后成交 | 涨幅榜对“跑出来的币”失真，需补 |
| 套利 `arbitrage_candidates` | 245 万候选，12 万/天；已验证 468 条，`insufficient_evidence` 2.1 万 | 只有 468 条带金额，**无法支撑排行榜** |
| 清算 | Robinhood 无数据；本地 RPC 实测 Morpho Blue `Liquidate` 事件近 1000 万块约 126 条（3 个合约） | 需新建清算索引器 |
| 集群 `protocol_scoped_clusters` | 2.1 万 issuer cohort + 21 个套利集群，无收益字段 | 需要集群收益聚合与排序 |

### 1.2 UI 问题（截图见 `docs/product/screenshots/mvp-before-*`）

1. 发币列表展示 30 位小数的原始报价单位数字，无 USD 价格、市值、成交额、持有人、毕业进度；按“资金流速”单一排序。
2. 没有涨幅榜 / 成交额榜 / 新币榜 / 即将毕业榜，也没有实时成交、大单、刚毕业等 FOMO 信息。
3. 地址情报页是 500 张相同的 “Issuer Cohort” 卡片，没有收益、没有排名。
4. 套利页“收益排行”在 24h 窗口为空，已验证执行是 0.000004 WETH 这类原始数值列表。
5. 清算页 Robinhood 为空。
6. 所有页面无自动刷新、无走势图、无数字缩写（$1.2K、+35.4%）。

---

## 2. 产品目标（MVP 定义）

一个“交易所行情页 + 链上赚钱排行榜”风格的公开站点，打开即可回答：

1. **现在什么币在涨、在放量、刚发、快毕业、刚毕业？**（发币）
2. **谁在 Pons 上赚钱？赚了多少、胜率、最佳一笔？**（聪明钱 / 地址集群）
3. **哪些套利机器人赚得最多？最新的大额套利？**（套利）
4. **谁在清算、清算奖励多少？**（清算）
5. **最新识别出的地址集群是谁、这个集群赚了多少？**（三个领域各自的新集群 + 收益）

### 2.1 可信度分层（替代原先“只显示 verified”的策略）

原设计只展示已验证数据，导致页面几乎为空。MVP 采用三层标注，**所有金额都带标签**，既有 FOMO 又不虚假：

| 标签 | 含义 | 示例 |
|---|---|---|
| `已验证 Verified` | 确定性对账（receipt + trace + 余额 + gas） | economic verifier 结果 |
| `估算 Est.` | 基于本地链上成交的确定性计算，但范围有限（如仅曲线/池内成交、平均成本法、USD 按成交时价格） | 钱包 PnL、套利 receipt 净流入 |
| `未知` | 缺少价格或输入 | 无 USD 价格的报价资产 |

每个榜单页脚注明方法版本（`methodology_version`）和已知限制；不同资产在换算为 USD 后才合并，无法换算的不进入 USD 总额。

---

## 3. 技术方案

### 3.1 架构原则

- 继续使用现有 Rust/Axum 服务端渲染（不引入 Node 构建链），交互用少量原生 JS：定时拉取 JSON API 局部刷新、列排序、Tab 切换、数字闪烁动画；走势图用服务端生成的内联 SVG。
- **页面只读预计算表**，禁止在请求路径上扫描 `protocol_launch_trade_facts`。目标：热页面 TTFB p95 < 300 ms。
- 所有新增 schema 通过 SQLx 迁移（`0056+`），只做增量，不破坏现有表。
- 每个后台 worker 使用持久游标（`onchain_event_cursors` 或专用 checkpoint 表），可重启、幂等。
- 不修改生产 `robinhood-arb-*` 容器；RPC 访问经 rpc-gateway 限流。

### 3.2 新数据模型（计划）

| 表 | 粒度 | 生产者 |
|---|---|---|
| `market_asset_usd_prices_1m` | 报价资产 × 分钟 | `market-price-rollup`（从 `price_observations` 汇总，ETH(0x0)→WETH，同名仿冒币按规范地址白名单过滤） |
| `market_token_candles_1m` | 币 × 分钟：OHLC（报价单位 + USD）、买卖量 USD、成交笔数、买家/卖家数 | `market-candle-projector`（按 trade fact 自增游标增量） |
| `market_token_stats` | 币一行：价格 USD、市值、5m/1h/6h/24h 涨跌、成交额、笔数、买家数、持有人估算、毕业进度、ATH、24 点迷你走势、热度分 | `market-stats-projector`（增量 + 定期衰减扫描，修复窗口不衰减问题） |
| `market_wallet_positions` | 钱包 × 币：买入/卖出数量、成本 USD、收入 USD、已实现 PnL（平均成本法）、未实现 PnL（按最新价） | `market-pnl-projector` |
| `market_wallet_leaderboard` | 钱包 × 领域 × 窗口（24h/7d/30d）：PnL、胜率、交易币数、最佳一笔倍数 | `market-pnl-projector` |
| `market_cluster_leaderboard` | 集群 × 窗口：成员 PnL 汇总、活跃成员、最近活动、首次发现时间 | `market-pnl-projector` |
| `arbitrage_execution_estimates` | 套利交易：执行者/合约净流入（按 token）、USD、gas、净利 | `arbitrage-estimator`（本地 RPC receipt） |
| `lending_liquidations` | 清算事件：市场、借/抵押资产、偿还/没收数量、USD、奖励 | `liquidation-indexer`（本地 RPC `eth_getLogs`） |
| `market_token_pool_swaps`（M7） | 毕业后 V4 池成交 | `pons-v4-swap-indexer` |

旧的 `protocol_launch_metric_snapshots` 停止追加（保留历史表，加保留清理），页面迁移到新表。

### 3.3 关键算法

- **价格**：每笔 CurveBuy/CurveSell 的成交价 = quote/token；USD 价 = 成交价 × 该分钟报价资产 USD 价。
- **市值**：USD 价 × totalSupply（元数据补抓 `totalSupply()`，Pons V2 固定供应）。
- **毕业进度**：曲线累计净报价流入 ÷ `graduation_threshold_raw`，上限 100%。
- **持有人估算**：曲线内净买入 > 0 的地址数（标注“曲线持有人，不含转账”）。
- **钱包 PnL**：平均成本法；已实现 = 卖出收入 − 卖出部分成本；未实现 = 剩余数量 × 最新价 − 剩余成本；手续费计入成本；不含 gas（标注）。
- **热度分**：`log(1h 成交额) + 买家增速 + 价格动量`，版本化参数，写入 `methodology_version`。
- **套利估算**：receipt 中 Transfer 日志 → 发送者 + 入口合约 + 已知结算地址的逐 token 净变化 → 按区块时间 USD 价换算 → 减去 `gasUsed × effectiveGasPrice`（含 L1 部分）。只有全部 token 可定价时给出 USD 净利，否则 `未知`。
- **清算**：Morpho Blue `Liquidate(id, caller, borrower, repaidAssets, repaidShares, seizedAssets, badDebtAssets, badDebtShares)`；`idToMarketParams(id)` 取 loan/collateral 资产；奖励 USD = seized × 抵押价 − repaid × 借款价。合约先经 `eth_getCode` 与 `idToMarketParams` 接口校验后才入库。

---

## 4. 里程碑

每个里程碑：独立提交 → `cargo fmt/clippy/test` 通过 → 更新验收报告 → 打 tag `market-mvp-mN`。

### M0 基线与工程化（0.5 天）
- [x] 快照提交现有未提交工作，tag `market-mvp-baseline`，建分支 `feat/market-mvp`
- [x] 生成开发计划与验收报告
- [ ] `CLAUDE.md`（仓库导航、运行/部署命令、数据约束）
- [ ] 改版前截图存档；开发实例 `market-web-dev`（端口 3401）与生产 3001 并行，读取同一数据库

### M1 行情数据底座（2 天）
- 迁移：`market_asset_usd_prices_1m`、`market_token_candles_1m`、`market_token_stats`、`totalSupply` 字段
- `ledgerflow_market_projector`（新二进制，内含 price-rollup / candle / stats 三个阶段，各自游标）
- 历史回补：最近 30 天 V2 成交生成 1m K 线
- 停止旧快照追加，加保留清理

### M2 发币 FOMO 前端（2 天）
- 全局：顶部行情条（ETH 价格、24h Pons 成交额、1h 新币数、24h 毕业数、今日最大涨幅）、数字缩写、涨跌色、自动刷新（15s）、更新闪烁
- 总览：四榜并列（🔥热门 / 🚀涨幅 / 🆕新币 / 🎓即将毕业）+ 大单实时流 + 刚毕业
- 发币页：交易所式表格（价格、5m/1h/24h 涨跌、成交额、市值、买家、毕业进度条、24h 迷你走势），Tab 与列排序，搜索（symbol/地址），分页
- 币详情：价格 K 线/折线（1m/5m/1h）、核心指标、最近成交、大户盈亏榜、创建者与关联集群

### M3 地址 / 集群盈亏（2 天）
- `market_wallet_positions`、`market_wallet_leaderboard`、`market_cluster_leaderboard` 投影
- 集群质量过滤（最少成员、最少交易）与“新发现集群”（按 first_seen_at）

### M4 聪明钱前端（1.5 天）
- 聪明钱页：领域 Tab（发币/套利/清算）× 窗口；前三名领奖台卡片（PnL、胜率、最佳一笔“$120 → $4.3K (36x)”）；完整排行榜；新发现集群流（含集群收益）
- 地址详情：PnL 汇总、持仓表、交易记录、所属集群
- 集群详情：成员贡献排行、收益曲线、证据交易

### M5 套利估算 + 前端（2 天）
- `arbitrage-estimator` worker（本地 RPC receipt，有界并发，7 天回补）
- 套利页：24h 估算总利润 / 执行数 / 活跃机器人；机器人排行（USD 净利、次数、胜率、最大一笔）；最大单笔套利榜；实时套利流；已验证记录单独标识

### M6 清算索引 + 前端（1.5 天）
- `liquidation-indexer`（Morpho Blue 事件，全历史回补，实时跟进）
- 清算页：统计卡、清算人排行、最新清算、最大奖励榜

### M7 毕业后行情（2 天）
- V2 毕业仓位 → PositionManager `getPoolAndPositionInfo` → PoolKey → PoolId
- V4 PoolManager `Swap` 事件（按已知 PoolId 过滤）→ 与曲线成交统一进入 K 线和统计
- 已毕业 Tab、毕业后涨幅

### M8 加固与验收（1.5 天）
- 索引与查询计划审查，页面 p95 压测，worker 资源占用与延迟监控（`/metrics`）
- 中英文、移动端 400px、空状态与降级
- 24h 浸泡运行；最终验收报告与截图；经确认后切换生产 3001

**总计约 15 个工作日。** 顺序按“尽早出可见价值”排列：M1→M2 先让发币页焕然一新，再做 M3→M4 赚钱榜，再补套利、清算和毕业后行情。

---

## 5. 风险与对策

| 风险 | 对策 |
|---|---|
| 2400 万成交全量扫描拖垮 PostgreSQL（生产共用） | 所有投影按自增游标小批次；回补限速；只回补 30 天；新增索引用 `CREATE INDEX CONCURRENTLY` 由运维窗口执行 |
| 仿冒 USDC/USDG 等同名代币污染 USD 换算 | 只信任规范地址白名单（官方 registry + 价格来源地址），不按 symbol 匹配 |
| 本地 RPC 负载 | 统一经 rpc-gateway 限流；receipt/日志请求有界并发；不做全链 trace |
| 估算 PnL 被误读为真实收益 | 全站 `估算` 标签 + 方法说明页；已验证与估算分开展示 |
| 毕业后 PoolId 推导失败 | M7 独立，失败时已毕业币显示“毕业后行情未索引”，不影响 M1–M6 |
| 生产 3001 在改版期间受影响 | 开发实例 3401 并行验证，验收通过并经确认后再切换 |

---

## 6. 版本管理

- 分支：`feat/market-mvp`，基于 `codex/chainactors-intelligence-v2` 的快照提交 `9f9841e`
- 每个里程碑一个或多个聚焦提交，tag `market-mvp-m0` … `market-mvp-m8`
- 迁移编号从 `0056` 连续递增，每个迁移配套 `.down.sql`
- 验收报告 `docs/acceptance/market-mvp-acceptance.md` 随里程碑同步更新
- 推送远端与切换生产部署需人工确认
