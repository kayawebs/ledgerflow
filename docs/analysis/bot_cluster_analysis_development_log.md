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
