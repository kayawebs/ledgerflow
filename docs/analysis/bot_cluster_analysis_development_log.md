# Bot Cluster Analysis Development Log

This log records implementation decisions and verification for the bot-oriented cluster analysis redesign.

## 2026-07-03

### Product Decision

The product should prioritize targeted bot/cluster reports over generic analysis.

Primary user questions:

- What does this address/cluster do?
- How much did it make?
- How much gas did it spend?
- How reliable is the strategy?
- Which protocols, routes, tokens, and addresses matter?

The constrained agent remains useful, but only after structured reports exist. It should query bounded report outputs rather than raw chain data.

### Engineering Boundary

Phase 1 must not call RPC or start additional workers. It should reuse existing classified transactions and in-memory tests.

Allowed in Phase 1:

- typed report structs
- service-level aggregation
- API response expansion
- web rendering of the new report fields
- unit tests using `InMemoryAnalysisRepository`

Deferred:

- new database tables
- report refresh worker
- raw trace parsing expansion
- agent query planner

### First Implementation Target

Upgrade `ClusterStrategyAnalysisService` into a bot-oriented report payload while preserving the existing endpoint:

`GET /clusters/{cluster_id}/strategy-analysis`

The payload should remain backward compatible with the existing web tab and include:

- behavior profile
- financial/performance summary
- arbitrage report
- liquidation report
- representative transactions

### Verification Plan

Use only static checks and local tests:

- `cargo check --bins --locked`
- `cargo test strategy_analysis_rolls_up_cluster_arbitrage_behavior --locked`
- targeted tests for new report fields

No Docker restart is required until API/UI changes need to be exposed in the running app.

### Implemented

Expanded `ClusterStrategyAnalysis` with a bot-oriented report contract:

- `behavior_profile`
- `performance`
- `arbitrage_report`
- `liquidation_report`
- `representative_transactions`

The existing response fields remain in place for compatibility:

- `tx_count`
- `arbitrage_tx_count`
- `arbitrage_success_rate`
- `top_routes`
- `top_protocols`
- `address_roles`
- `strategy_breakdown`

The first implementation still aggregates from `classified_transactions`, which is acceptable for the MVP and keeps Phase 1 free of RPC/backfill side effects. The new fields are shaped so a later materialized report table can populate the same API contract.

### Report Semantics

Current success logic:

- A classified transaction is considered successful when `estimated_pnl_usd >= 0`.
- Transactions without PnL are treated as successful for now.

This is a temporary approximation. Phase 2 should split success into:

- execution status
- strategy profitability
- reverted/failed classification
- failure reason

Current route logic:

- Route is derived from `token_out -> token_in`.
- Known Base token addresses are shortened to symbols for common assets.
- Missing route data becomes `unknown_route`.

This should be replaced by normalized path facts once `tx_strategy_facts` exists.

### Verification Results

Passed:

- `cargo check --bins --locked`
- `cargo test strategy_analysis_rolls_up_cluster_arbitrage_behavior --locked`

No Docker services were started or restarted for this implementation step. No RPC calls were made by the tests.

## 2026-07-03 Phase 2 Start

### Implemented

Added a pure strategy fact normalization layer:

- `src/services/strategy_facts.rs`
- `tests/strategy_facts.rs`

The fact builder converts a `ClassifiedTransaction` into `TransactionStrategyFact`:

- `strategy_type`
- `strategy_subtype`
- `protocol_name`
- `protocol_family`
- normalized route key and token refs
- outcome/profitability/failure reason
- PnL-to-gas ratio

`ClusterStrategyAnalysisService` now consumes `TransactionStrategyFact` for route, protocol, strategy type, and report success. This removes duplicate local string inference from report aggregation and gives future materialized report tables a stable normalization contract.

### Current Strategy Fact Rules

Arbitrage subtype:

- `token_pair_arbitrage` when both token in and token out are present.
- `arbitrage_unknown_route` when route data is incomplete.

Liquidation subtype:

- `{protocol_family}_liquidation` when the protocol family is recognized.
- `liquidation_unknown_protocol` when it is not.

Outcome:

- `profitable = estimated_pnl_usd >= 0` when PnL exists.
- `report_success = profitable.unwrap_or(true)` for compatibility with existing MVP reports.
- `failure_reason = negative_estimated_pnl` for negative PnL.

Protocol families currently recognized:

- uniswap
- aerodrome
- aave
- morpho
- compound
- curve
- balancer
- pancake

### Verification Results

Passed:

- `cargo check --bins --locked`
- `cargo test strategy_fact --locked`
- `cargo test strategy_analysis_rolls_up_cluster_arbitrage_behavior --locked`

No RPC calls are made by the new tests. No service restart is required until this Phase 2 code is deployed.

## 2026-07-03 Phase 3 Start

### Implemented

Added the first materialization boundary for cluster strategy reports:

- `ClusterStrategyReportSnapshot` entity
- `cluster_strategy_report_snapshots` migration
- repository methods for upsert/read by `(cluster_id, window_key)`
- in-memory repository implementation
- SQLx repository implementation
- `ClusterStrategyAnalysisService::refresh_cluster_report_snapshot`
- `ClusterStrategyAnalysisService::get_cached_cluster_report_snapshot`
- API endpoints:
  - `GET /clusters/{cluster_id}/strategy-analysis/snapshot`
  - `POST /clusters/{cluster_id}/strategy-analysis/refresh`

The snapshot stores the full typed report payload as JSON with stable window metadata. This keeps Phase 3 compatible with the current report contract while making it possible to move report generation off the request path later.

### Window Key

Snapshot uniqueness uses:

```text
{window_start_or_*}..{window_end_or_*}
```

Examples:

- `*..*`
- `2026-07-01T00:00:00+00:00..*`
- `*..2026-07-03T00:00:00+00:00`
- `2026-07-01T00:00:00+00:00..2026-07-03T00:00:00+00:00`

This avoids nullable unique-index behavior differences and gives the future worker a stable upsert key.

### Deferred

The report worker is still deferred. The current refresh endpoint is explicit and bounded by cluster/window. It reads existing classified transactions only and does not trigger RPC, backfill, trace, or indexer work.

## 2026-07-03 Phase 4 Start

### Implemented

Started the report-first cluster strategy UI:

- expanded the web `ClusterStrategyAnalysisPayload` to consume the full typed report contract
- added behavior profile and performance KPI rendering
- added dedicated arbitrage and liquidation report sections
- added route/protocol/sample transaction tables inside each strategy report
- kept global route/protocol/address drilldowns for cross-strategy inspection
- added sample transaction actions that post to the existing single-tx analysis flow

This is still server-rendered HTML inside the existing dashboard. It does not create new workers, does not request RPC, and does not trigger backfill or trace analysis by itself.

### Product Semantics

The Strategy tab now leads with the user-facing bot questions:

- what the cluster mainly does
- how reliable it is
- estimated PnL and gas cost
- PnL-to-gas ratio
- which routes, protocols, addresses, and sample transactions explain the behavior

The UI still uses the request-time report endpoint for now. The Phase 3 snapshot endpoint is available for a later refresh/read-through flow.

## 2026-07-03 Phase 5 Start

### Implemented

Added the first constrained report-agent substrate:

- `ReportAgentService`
- typed `ReportAgentQuery`
- allowlisted sections:
  - `overview`
  - `behavior_profile`
  - `performance`
  - `arbitrage`
  - `liquidation`
  - `routes`
  - `protocols`
  - `addresses`
  - `samples`
- filters for strategy, protocol, route, and address
- mandatory window scope, either explicit start/end or `all_time=true`
- mandatory `cost_budget`
- bounded `row_limit`, max 50
- deterministic answer rendering with cited rows
- sample transaction de-duplication across requested sections
- API endpoint:
  - `POST /clusters/{cluster_id}/report-agent/query`

The service only reads `cluster_strategy_report_snapshots`; it does not call RPC, trigger backfills, scan traces, run free-form SQL, or access secrets.

### API Shape

Example body:

```json
{
  "all_time": true,
  "sections": ["overview", "performance", "arbitrage", "samples"],
  "filters": {
    "strategy": "arbitrage"
  },
  "row_limit": 10,
  "cost_budget": 50
}
```

For bounded windows, use `window_start` and/or `window_end` instead of `all_time=true`. The query reads a matching report snapshot, so the caller must refresh/create the snapshot first.

### Verification Results

Passed:

- `cargo check --bins --locked`
- `cargo test report_agent --locked`
- `cargo test strategy_analysis_rolls_up_cluster_arbitrage_behavior --locked`

No test performs RPC, Docker, backfill, trace, or indexer work.

## 2026-07-03 Phase 6 Start

### Product Decision

Standalone cluster discovery is no longer the primary user flow. A user should create a cluster by entering one or more seed addresses, then let the worker do the first-pass related-address discovery, candidate classification, conservative auto-add, historical backfill, and report refresh.

This matches the collector-address workflow: the seed can be a liquidation/arbitrage proceeds address, while related executor/source wallets are inferred from indexed top-level activity.

### Implemented

Replaced the user-facing discovery workflow with seed-based cluster onboarding:

- removed the web dashboard Cluster Discovery tab, route, and panel
- removed the public `/discovery/cluster` API route
- changed cluster create/update inputs so submitted addresses are marked as `seed`
- extended bounded discovery requests with `max_nodes` and `max_windows`
- injected `ClusterDiscoveryService` into app/backfill-worker backfill services
- added worker-side cluster expansion before backfill analysis
- auto-adds only high-confidence direct `inbound_to_seed` EOA/Unknown candidates
- stores all candidate suggestions under `cluster.metadata.auto_discovery`
- refreshes cluster strategy report snapshots after backfill
- shows auto-discovery status, auto-added count, suggested count, and candidate rows on the cluster Overview tab

### Safety Bounds

Worker auto-discovery uses intentionally conservative defaults:

- `max_hops = 1`
- `max_nodes = 40`
- `max_windows = 3`
- `max_auto_add = 12`
- `min_auto_add_confidence = 0.75`

Only inbound-to-seed candidates are auto-added. Outbound recipients, protocol contracts, exchanges, and lower-confidence counterparties stay as suggestions in cluster metadata for review.

### Verification Results

Passed:

- `cargo check --bins --locked`
- `cargo test cluster_backfill --locked`
- `cargo test cluster_discovery --locked`
- `cargo test report_agent --locked`
- `cargo test strategy_analysis_rolls_up_cluster_arbitrage_behavior --locked`

Added `cluster_backfill_auto_discovers_seed_related_addresses_before_analysis`, which verifies that a seed collector address can auto-add an inbound EOA executor before analysis while retaining outbound candidates as non-auto-add suggestions.

No Docker services were started or restarted for this implementation step. The tests use in-memory repositories and mock RPC clients only.

### Deployment Note

Deploying this change requires restarting the web/app and backfill worker processes. The worker will run bounded discovery during cluster backfill, so deployment should use the existing RPC gateway/rate-limit settings and should not be paired with an unbounded historical backfill spike.
