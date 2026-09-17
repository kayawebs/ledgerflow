# Robinhood Chain 持续采集与补数计划

更新时间：2026-08-04

## 实施状态

- 阶段一已完成并运行：`live` 与 `lazy` worker 分离，canonical blocks、完整交易/receipt logs、持久化 worker 进度和 reorg 回退已经接入；重启续跑和链头延迟已实测。
- 阶段二已完成并运行：数据库驱动的 source/endpoint/run/snapshot/cursor 已接入，官方 `/assets`、`/corporate-actions`、Token Contracts 与 Protocol Contracts 会持续同步，并由本机 RPC 校验后生成版本化数据集。
- 2026-08-04 首次冷启动验收为 96 个 Stock Token deployment、2 个官方 Token、21 个 Robinhood 主网协议/系统合约和 11 条 corporate action；全部来源均保留原始快照与 64 位 SHA-256，第二次抓取命中 checksum 去重。
- 阶段三已完成并运行：canonical ERC-8056 事件投影、V2/V3/Solidly factory 事件发现、接口与 factory 关系回查、版本化 pool/factory 数据集、重组回滚和 Token 元数据补齐均已接入。
- 2026-08-04 阶段三验收为 595 个已验证池（381 V2、214 V3）、13 个被接口校验拒绝的伪候选，以及 600/600 个池关联 Token 元数据成功；发现游标持续贴近链头。
- 阶段四的地址链路已运行：`address-producer` 从 ClickHouse canonical 交易和已知 ABI 事件中持续提取地址，保存窗口证据、first/last seen block 和独立游标，再投递去重任务。首次保留窗口验收写入 65,492 个真实观察地址。
- 阶段四的行为与复核链路已实现：`behavior-worker` 只对 RPC 已确认的 EOA 聚合规范发送者窗口，生成带窗口、样本交易、规则版本、置信度和 TTL 的 `actor:automation_candidate`；accept/reject/retract 决策分别保留人工 assertion、撤回和审计事件，不会自动声称 bot/searcher/team 身份。
- 阶段四运行验收生成了 6 个真实候选和 6 条待复核记录；重复窗口只刷新 assertion，不重复创建 review。
- 阶段六已按最低优先级启动：`robinhood_history` 的范围固定为 `0..27251940`，不会追逐移动链头。首批 20 个区块验收包含 41 笔交易和 61 条日志，41/41 receipt 状态完整，游标重试数为 0；同期 live 延迟保持在 2～4 个块。
- 下一项是持续观察行为规则误报率、历史吞吐和 RPC 压力。历史地址投影暂不启用，须先把现有地址任务 backlog 降到安全水位，避免历史地址淹没实时任务。

## 目标与原则

目标是让 LedgerFlow 的页面只展示可追溯的真实记录，并持续补齐交易、地址、Token、协议、价格和参与者关系。

- 本机 Robinhood Nitro RPC 是链上事实的主来源；任何外部 API 都只能做补充或交叉验证。
- 地址、Token 和协议合约不写进 Rust 常量，也不在服务启动时偷偷插入。它们必须进入 PostgreSQL，并带 `source_key`、数据集版本、校验和、证据和生命周期。
- 检测结果与确认结果分开。套利候选不能直接变成已确认利润，行为相似不能直接变成同一团队。
- 所有游标持久化，任务幂等，失败可重试，重组可回滚，旧版本可审计。
- 每条链使用独立的数据库/索引进度；所有唯一键必须包含 `chain_id`。

## 当前基线

现有项目已经具备可复用的基础：

- `rpc-gateway` 统一限制本机 RPC 的并发与速率。
- `trace-monitor` 持续读取区块和 DEX 日志，并写入监控游标、multi-swap 与套利候选。
- PostgreSQL 已有 `chain_addresses`、`effective_address_labels`、`erc20_token_metadata`、`protocol_deployments`、价格缓存和带来源的 V2 assertion 模型。
- `address-worker`、`price-worker` 和 `backfill-worker` 已能消费任务。
- ClickHouse indexer 已运行独立 live/lazy/history worker、canonical raw facts、重组检测和持久化进度；全历史覆盖正在低优先级补齐，尚未宣称完成。
- Web 的 Today 页已经改为实时查询 PostgreSQL、索引器进度和本机 RPC；没有数据时显示空状态。

Robinhood 官方确认主网 Chain ID 为 `4663`、原生 Gas 资产为 ETH，并提供标准 JSON-RPC、WebSocket 和 Sequencer Feed。官方也说明公共 RPC 有限流，不建议作为生产主数据源，因此本项目继续以本机节点为主。[连接 Robinhood Chain](https://docs.robinhood.com/chain/connecting/)

## 目标数据流

```text
本机 Nitro RPC / Sequencer Feed
            │
            ├── live block + receipt + logs ──> ClickHouse 原始事实层
            ├── trace / eth_call ─────────────> PostgreSQL 分析与元数据层
            └── canonical head / block hash ──> 重组与新鲜度检查

Robinhood 官方合约页 / Stock Token API / 协议官方 registry
            │
            └── source snapshot -> 校验 -> versioned dataset import
                                      │
                                      └── assertions / tokens / deployments

事实层 + 元数据层
            └── recognizers -> candidates -> confirmations -> Web/API
```

## 阶段一：稳定的实时链事实层（第 1 周）

### 1. 双游标索引

把索引器拆成两个独立 worker：

- `robinhood_live`：从 `head - safety_window` 开始，持续追块，目标延迟不超过 5 个块。
- `robinhood_history`：按固定范围从已确认的链起点向前补历史，低优先级运行，不影响 live worker。

两个 worker 都将游标、范围、重试次数、RPC 节点和最后成功时间持久化。history 首次启动时从 live worker 的持久化 `range_start - 1` 冻结上界，重启后复用该上界。本机 Nitro 对旧区块的 compact header、单笔交易与单笔 receipt 可用，但旧区块的 full-transaction block 和 `eth_getBlockReceipts` 会超时，因此历史 worker 采用逐交易降级路径并设置硬超时；只有整批 receipt 完整时才推进游标。本机节点不具备相应历史能力时，再配置独立 archive 备用源。[连接 Robinhood Chain](https://docs.robinhood.com/chain/connecting/)

### 2. ClickHouse 原始表扩充

增加或完善以下事实表：

- `canonical_blocks(chain_id, number, hash, parent_hash, timestamp, finality_state)`
- `transactions_raw(chain_id, block_number, tx_index, tx_hash, from, to, value, status)`
- `logs_raw(chain_id, block_number, tx_hash, log_index, address, topics, data)`
- `traces_raw` 仅保存需要深度分析的交易，避免全链 trace 成本失控
- `address_tx_refs` 保留地址方向索引，供地址和聚类页面查询

写入键必须幂等，重复抓取同一区块不会产生重复事实。

### 3. 重组与确认层级

保存最近至少 128 个块的 hash 链，并在每轮轮询时校验父 hash。发生重组时，把分叉后的事实标记为 orphan，再从共同祖先重放。

Robinhood Chain 的最终性分为 sequencer soft confirmation、已发布到 Ethereum 和 Ethereum finality。数据库中增加 `soft_confirmed`、`l1_posted`、`l1_finalized`，高价值告警和利润确认不得只依赖 soft confirmation。[交易最终性](https://docs.robinhood.com/chain/transaction-finality/)

### 4. 验收标准

- live worker 的 p95 链头延迟不超过 5 个块。
- 进程重启后从数据库游标继续，不漏块、不重复写。
- 人工制造 20～50 个块的回退后能自动回滚并重放。
- Today 页显示的 RPC head、monitor cursor 和 indexer cursor 可以相互对账。

## 阶段二：Token 与官方合约持续同步（第 2 周）

### 1. 新增 registry sync worker

增加 `ledgerflow_registry_sync`，由数据库中的 source 配置驱动，不把地址写进代码。建议新增：

- `intelligence_source_endpoints`：来源 URL、类型、抓取周期、启用状态和信任等级
- `intelligence_sync_runs`：开始/结束时间、HTTP 状态、内容校验和、解析数量和错误
- `intelligence_source_snapshots`：原始响应摘要、ETag/Last-Modified、SHA-256 和对象存储位置
- `intelligence_sync_cursors`：分页游标和上次成功版本

每次抓取先保存原始快照并计算 checksum，再生成 `IntelligenceDatasetManifest`，最后复用现有事务型 importer。新版本 supersede 旧版本，但不删除历史 assertion。

### 2. Robinhood 官方 Token 来源

优先接入以下官方来源：

- 官方 Token Contracts 页面，用于 WETH、USDG 和官方 Stock Token registry 的身份校验。[Token Contracts](https://docs.robinhood.com/chain/contracts/)
- `GET https://api.robinhood.com/rhj/assets`，同步 Stock Token 的 UID、symbol、name、4663 部署地址、状态和 multiplier。该接口是只读官方来源，字段包含每条链的部署地址。[Stock Token APIs](https://docs.robinhood.com/chain/stock-token-apis/)
- 本机 RPC 的 `eth_getCode`、`symbol()`、`name()`、`decimals()`、`totalSupply()`，用于校验每个导入地址确实是当前链合约。

抓取节奏建议：`/assets` 每 15 分钟；合约页每天一次；发现内容 checksum 变化时立即生成新数据集版本。

### 3. Stock Token 专用结构

在通用 ERC-20 元数据之外增加：

- `stock_token_assets(uid, symbol, name, status)`
- `stock_token_deployments(uid, chain_id, address, current_multiplier, pending_multiplier, effective_at)`
- `stock_token_corporate_actions(action_id, uid, type, status, process_date, payload)`
- `stock_token_multiplier_events(chain_id, address, block_number, tx_hash, old_value, new_value, effective_at)`

Stock Token 是 18 decimals 的 ERC-20，并实现 ERC-8056。应监听 `UIMultiplierUpdated` 和 `TransferWithUIAmount`；原始余额不能因为 multiplier 变化而被重写。[ERC-8056](https://eips.ethereum.org/EIPS/eip-8056)

### 4. 价格口径

- 链上 Chainlink feed 为首选确认价格，并保存 `round_id`、`updated_at` 和 staleness。
- Robinhood `/prices` 只做报价补充；官方说明其值是未应用 multiplier 的底层股票报价。
- 若使用 REST 报价计算 Token 价值，必须应用 `/assets.currentMultiplier`；不能再对已经包含 multiplier 的链上 Chainlink 价格重复应用。
- 所有估值保存 `source`、`observed_at`、`block_number`、`confidence` 和计算公式版本。

## 阶段三：协议与池自动发现（第 2～3 周）

### 1. 官方基础协议数据集

每天同步 Robinhood 官方 Protocol Contracts 页面中的 L2 bridge、WETH gateway、Multicall、Permit2 和 precompile。L1 合约只能进入 Ethereum 数据平面，不能作为 4663 地址写入 Robinhood 数据库。[Protocol Contracts](https://docs.robinhood.com/chain/protocol-contracts/)

### 2. Factory/event 发现

对已验证 factory 建立事件订阅：

- Uniswap V2/Solidly 类：`PairCreated`
- Uniswap V3 类：`PoolCreated`
- vault/lending/RFQ settlement：使用各协议官方 registry 或明确的部署事件

发现新池后进入 `contract_discovery_candidates`，先完成以下校验再提升为 active deployment：

1. `eth_getCode` 非空；
2. factory 回查与事件来源一致；
3. token0/token1/fee/stable 等接口探测一致；
4. 部署区块和创建交易可追溯；
5. 协议来源、method version 和证据写入 assertion。

只凭函数 selector、相似 bytecode 或频繁交互得出的协议归属，必须标记为 `model/onchain` 低置信 assertion，不能覆盖官方来源。

### 3. Blockscout 补充

Robinhood 官方把 `robinhoodchain.blockscout.com` 列为主网浏览器。可定期抓取 verified contract name、ABI 和创建者作为补充证据，但仍用本机 RPC 校验 bytecode 和链 ID；浏览器数据不能成为唯一链上事实来源。[连接 Robinhood Chain](https://docs.robinhood.com/chain/connecting/)

## 阶段四：地址、Bots 与 Teams 持续补全（第 3～4 周）

### 1. 地址任务来源

下列新地址自动进入去重任务队列：

- 新区块交易的 from/to；
- logs emitter 和 indexed address；
- trace caller/callee、资金发送者/接收者；
- 新池的 factory、token、router；
- 高频套利候选发送者和利润归集地址；
- 用户创建的 cluster seed。

任务优先级：用户 seed > 高频候选 > 新协议/Token > 普通参与者。每个地址记录 `first_seen_block`、`last_seen_block` 和触发任务的证据。

### 2. 行为标签

- EOA/contract/system 是物理类型，由 RPC code 与系统地址规则决定。
- bot、searcher、market maker、liquidator 是行为 assertion，必须包含统计窗口、样本交易、规则版本和置信度。
- team 是关系推断，至少需要资金源、调用序列、利润归集、时间相关性中的两类独立证据。
- 自动模型 assertion 设置 TTL；规则或模型升级后生成新版本并 supersede 旧版本。
- 增加人工审核队列，支持 accept、reject、retract，并保留审计记录。

### 3. 利润确认

只有在以下条件同时满足时才进入“已确认利润”：

- receipt 成功且交易处于要求的 finality 层级；
- 主体地址的原生币和 Token 净变化完成会计平衡；
- Token decimals 和价格来源有效且未过期；
- gas、内部转账和归集地址均已纳入；
- 无法定价的资产单独列出，不用 0 美元代替。

## 阶段五：运行质量和告警（贯穿全程）

增加以下可观测指标并展示在开发者页：

- `rpc_head - live_cursor`、`rpc_head - trace_cursor`
- 每分钟 blocks/tx/logs/traces 写入量
- 缺块、重复块、parent hash 不一致和 orphan 数
- 地址任务 backlog、失败率和重试年龄
- Token 元数据成功率、协议校验通过率、未知合约比例
- 每个 source 的最后成功时间、checksum 变化和过期数据集数量
- 价格 staleness、无价资产比例、利润确认覆盖率

建议 SLO：

- 实时区块 p95 延迟 ≤ 5 blocks；
- source 同步成功率 ≥ 99%；
- 已发现 ERC-20 的元数据完成率 ≥ 95%；
- 任意页面展示的记录都能追溯到 block/tx 或 source snapshot；
- 页面不得使用 minimum floor、示例地址或静态盈利值填补空数据。

## 交付顺序

1. 先把 ClickHouse live worker 从纯 lazy 状态升级为持续追块，并补 reorg/canonical block 支持。
2. 开发数据库驱动的 `registry_sync` 和 source/run/snapshot/cursor 四张表。
3. 接入 Robinhood `/assets`、Token Contracts、Protocol Contracts，并通过现有 versioned importer 入库。
4. 增加 ERC-8056、factory/pool 和官方协议事件同步。
5. 扩展地址任务生产者、行为 assertion 与人工审核。
6. 已开启固定边界的低优先级历史 backfill；继续以 live 延迟、RPC 错误率和地址 backlog 作为提速门槛，任何时候历史任务都不能挤占 live RPC 配额。

Token/协议冷启动数据库已经不再依赖一次性人工整理；当前系统已形成“实时链事实 + 官方注册表 + 可审计行为推断 + 可续跑历史事实”的持续数据闭环。历史覆盖率和人工审核质量仍需作为长期运行指标，而不能把 worker 已启动等同于全量完成。
