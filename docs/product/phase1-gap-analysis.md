# LedgerFlow Economic Monitoring Pilot v0.1 — Phase 1 Gap Analysis

## 1. 已完成功能

- Base 链索引与回填任务、交易/receipt/block 查询 API。
- PostgreSQL、ClickHouse、Redis、RPC gateway 运行基础设施。
- 交易标准化、token transfer/日志抽取、基础 gas 与状态记录。
- 已有 protocol recognizer、transaction explanation、arbitrage/liquidation 分类器。
- 已有 Evidence Pack builder/serializer/hashing，以及 trace report 与 monitor reporter。
- 已有 Telegram/OpenClaw 通知接入骨架和分享链接概念。
- 已有 Docker Compose、迁移机制、基础 health endpoint。

## 2. 部分完成功能

- 事件识别目前集中在分析器/报告器，尚未统一为 Pilot 的八类事件模型；large flow、treasury、revenue、失败重试和 unknown 需要统一规则化实现。
- Evidence Pack 目前是分析内部模型，缺少 Pilot v1 顶层字段、JSON Schema 及严格校验。
- Telegram 目前可发送确认信号，但缺少持久化队列、去重、重试、限流、分组、投递状态和 dead-letter queue。
- 当前报告以请求/cluster snapshot 为主，尚未生成按项目周周期落盘的 Markdown/HTML 报告。
- 现有地址 cluster 配置不能替代 project/entity/rule/threshold 的客户配置系统。
- health 已存在，但 readiness、业务 metrics、checkpoint/recovery 指标尚不完整。
- Base 是默认生产目标，但需要显式 Base-only Pilot adapter；Robinhood Chain 只能作为可选 adapter。

## 3. 缺失功能

- project/entity/rule/Telegram destination 数据模型及 YAML 示例。
- 八类 Pilot event taxonomy、持久化事件表和确定性 dedup key。
- Pilot Evidence Pack v1 JSON Schema 与 validation。
- DB-backed Telegram delivery worker（retry/rate limit/grouping/status/DLQ）。
- weekly_report.md 与 weekly_report.html 自动生成和存档。
- 至少 100 笔真实 Base 交易的固定 regression dataset 及分类精度/金额可靠性报告。
- 公开 Demo 配置、20 个真实事件、arbitrage/liquidation/complex flow 示例证据。
- readiness/metrics、备份恢复和 7 天 soak test 脚本/指标文档。
- 逐条 PASS/FAIL/NOT IMPLEMENTED 的 Phase 1 acceptance 结果。

## 4. 可直接复用模块

- `src/indexer`、`src/services/backfill*`、`src/repositories` 的区块/交易持久化和 checkpoint 逻辑。
- `src/analysis`、`src/protocols`、`src/evidence_pack` 的确定性解析和证据构建。
- `src/services/monitor_reporter.rs` 的 Telegram client、分享链接和已有 signal persistence。
- 现有 SQLx migration runner、Postgres pool、ClickHouse writer、RPC client。
- 现有 API 的 transaction/receipt/block 查询和 trace monitor 路由。

## 5. 需要重构模块

- 把分析结果映射为独立的 `pilot_events`，将事件分类与语言表达解耦；事实字段只能来自链上输入。
- 将 Evidence Pack 扩展为版本化 Pilot schema，同时保持旧分析 API 向后兼容。
- 将 Telegram 直发改为 outbox/worker 模式，保留现有 notifier 作为 transport。
- 把 cluster snapshot 报告抽象为可复用的周报聚合器。
- 将运行配置从代码常量迁移到 YAML/数据库，并显式校验 `chain_id = 8453`。

## 6. 数据质量风险

- trace 节点不可用或未归档时，调用路径、内部转账和利润只能输出 unknown/null。
- token decimals、价格源或历史价格缺失会影响 USD 金额；不得由 LLM 猜测。
- reorg/finality、RPC 分页、checkpoint 恢复可能造成遗漏或重复。
- 新协议/代理合约未被 recognizer 覆盖时必须归类为 unknown_material_interaction。
- arbitrage profit 必须同时扣除 gas、借贷/协议费用；缺任何输入就降低 confidence 并列出 limitation。
- 外部 Telegram API 失败、限流或 bot 权限变化需要可重试且最终进入 DLQ。

## 7. Phase 1 实现顺序

1. Pilot migration、配置 loader、Base-only 校验和事件/Evidence Pack schema。
2. 从已有 TransactionAnalysis 生成八类确定性 Pilot events 并持久化。
3. DB-backed Telegram outbox、去重、重试、限流、分组、投递状态和 DLQ。
4. weekly Markdown/HTML 报告、分享链接和 report persistence。
5. readiness/metrics、demo 配置、regression dataset、acceptance/soak/backup 文档。
6. 用真实 Base 数据运行回归和 pilot smoke test；未满足项保持 NOT IMPLEMENTED。

## 8. 预计修改文件

- `Cargo.toml`, `Cargo.lock`
- `src/pilot/{mod.rs,config.rs,events.rs,evidence.rs,alerts.rs,report.rs}`
- `src/api/mod.rs`、相关 worker/bin 与 `docker-compose.yml`
- `migrations/0027_pilot_monitoring.{up,down}.sql`
- `schemas/evidence-pack-v1.schema.json`
- `config/pilots/example-base-project.yaml`
- `scripts/soak_test.sh`
- `tests/fixtures/base_regression_dataset.jsonl`、`tests/pilot_*`
- `docs/acceptance/phase1-results.md`、`docs/operations/{backup-recovery,soak-test}.md`

## Implementation update — 2026-08-11

The implementation above has now landed in the `codex/chainactors-intelligence-v2` worktree without replacing the existing indexer, analysis, recognizer, Evidence Pack, or Searcher paths. Migrations `0027`–`0029` add the Pilot control plane, event/outbox/report storage, delivery state, and ruleset-aware checkpoints. Config import also creates a stable, Pilot-scoped watcher in the existing cluster tracking system, preserving the exact customer entity list while reusing indexer range splitting, deduplication and continuous backfill. The Base-only YAML/DB configuration importer, eight deterministic classifiers, schema-validated Pilot Evidence Pack, event worker, Telegram worker, completed-week report worker, Pilot API routes, metrics, 100-transaction fixture, and public Demo artifacts are present.

The remaining acceptance gaps are external/elapsed-time gates rather than hidden implementation claims:

The executable order, owner inputs and exit criteria are maintained in `docs/product/phase1-remaining-plan.md`.

- A real Telegram bot/chat delivery has not been executed because no Pilot credential or destination was supplied; transport success and `429 retry_after` are covered with a local mock.
- The prepared seven-day soak is running against the isolated Base deployment but cannot pass until seven elapsed days and 10,080 samples exist.
- Database-backed protocol evidence and the actual recognizer now pass a 95-row reviewed direct-deployment corpus: 86 positives across all nine required protocol families plus 9 negative controls, with no false positives or false negatives. Internal-only calls, proxies and new deployments remain an explicit coverage limitation.
- Archive receipt/trace/balance reconciliation now proves numeric profit on 3 reviewed Base arbitrages and safely returns null on 3 incomplete/native-mismatch cases. Broader settlement patterns remain fail-closed rather than guessed.
