# LedgerFlow Analysis Tool Roadmap

This document defines the next development tasks for the analysis tool now that the standalone indexer is available.

It only covers the consumer side. It does not require modifying indexer code from this thread.

## Executive Summary

The highest-priority gap is cluster transaction discovery and backfill.

Today the system can:

- create clusters
- analyze a single transaction
- persist transaction analyses
- summarize spend for a cluster from already-persisted analyses

Today the system cannot yet:

- automatically discover cluster transaction history after cluster creation
- explain coverage clearly
- show which txs were discovered but not analyzed
- backfill or continuously analyze cluster txs from the indexer

As a result, the cluster summary can be numerically correct for persisted data while still being operationally misleading.

## Recommendation On Indexer Consumption

The analysis tool should not directly query the indexer's ClickHouse tables in its core runtime.

Recommended order:

1. Consume the indexer via a documented HTTP API.
2. If current API limits are too strict, request an analysis-consumer contract from the indexer thread:
   - cursor or range pagination
   - sufficiently high or unbounded export for trusted consumers
   - stable response DTOs
3. Only use direct ClickHouse reads as a temporary operational bridge, not as the long-term analysis architecture.

Reasoning:

- Direct ClickHouse coupling leaks producer storage internals into the consumer.
- It makes future indexer storage changes expensive.
- It weakens ownership boundaries.
- It complicates testing the analysis tool in isolation.

If you need an immediate stopgap, the least-bad fallback is a dedicated read-only consumer account for bulk export jobs, while keeping the main analysis service contract API-based.

## Current Diagnosis

### Cluster flow problem

The main product problem is not that cluster creation itself is broken.

The real problem is:

- cluster creation stores addresses but does not trigger discovery/backfill
- spend summary reads only persisted analyses
- coverage is implicit rather than explicit

That means:

- a new cluster can show zero or partial spend even when indexer coverage is complete
- operators cannot distinguish "no spend" from "not analyzed yet"
- there is no durable cluster-level progress model

### Single-tx analysis problem

The current recognizer system is structurally sound but still shallow.

It can detect:

- selector presence
- token/native transfer activity
- simple batch-router heuristics
- a narrow approval-then-swap-like scenario

It cannot yet reliably separate:

- protocol classification
- transaction intent
- execution pattern
- accounting interpretation

These concerns need clearer layering if the system is going to grow safely.

## Roadmap

## Phase 1: Finish Cluster Discovery And Backfill

Goal: a cluster can be created, backfilled from the indexer, and summarized with explicit coverage.

Tasks:

1. Add an analysis-side `IndexerClient`.
2. Add stable DTOs in the analysis project for:
   - address tx ref rows
   - address tx hash pages
   - progress/health reads
3. Add `ClusterBackfillRun` and `ClusterBackfillCursor` domain models.
4. Add PostgreSQL tables for:
   - cluster backfill runs
   - per-address backfill cursors
   - discovered cluster tx refs or queue records
5. Add `ClusterBackfillService` that:
   - loads cluster addresses
   - pulls tx refs from the indexer by address and block window
   - deduplicates by `chain_id + tx_hash`
   - records discovered txs
   - schedules or directly runs per-tx analysis
6. Add an idempotent analysis submission path so the same tx hash can be safely rediscovered.
7. Add APIs:
   - `POST /clusters/{cluster_id}/backfill`
   - `GET /clusters/{cluster_id}/backfill-runs`
   - `GET /clusters/{cluster_id}/coverage`
8. Update the web UI to show:
   - discovered tx count
   - analyzed tx count
   - failed tx count
   - pending tx count
   - latest backfill status

Acceptance criteria:

- creating a cluster no longer implies immediate full correctness
- a backfill can be triggered explicitly
- the system can explain whether a summary is partial because analysis is incomplete
- repeated backfills are safe and idempotent

## Phase 2: Fix Cluster Spend Semantics

Goal: cluster summaries become operationally interpretable for bot/EOA groups.

Tasks:

1. Add summary coverage metadata:
   - discovery source
   - block/time window
   - discovered tx count
   - analyzed tx count
   - failed tx count
   - last analyzed block or timestamp
2. Split spend reporting into clearly named buckets:
   - gas burn
   - external native outflow
   - external token outflow
   - internal transfers
   - incoming transfers
   - unresolved or unsupported value movement
3. Add transaction-level summary fields for:
   - analysis status
   - recognizer labels
   - failure reason if analysis failed
   - unresolved count
4. Add cluster-level warnings when:
   - coverage is partial
   - trace support is missing
   - unsupported recognizers or flow derivation gaps affect totals
5. Add tests for:
   - partially analyzed clusters
   - duplicate tx discovery across multiple cluster addresses
   - mixed internal and external flows
   - failed tx analyses that should not disappear from coverage

Acceptance criteria:

- spend summaries are no longer ambiguous
- operators can tell the difference between true zero and incomplete coverage
- one failed tx does not silently disappear from cluster accounting visibility

## Phase 3: Introduce A Real Analysis Job Pipeline

Goal: cluster backfill and single-tx analysis share a durable application pipeline.

Tasks:

1. Add durable analysis jobs in PostgreSQL.
2. Separate:
   - discovery
   - analysis scheduling
   - tx analysis execution
   - cluster rollup refresh
3. Add retry policies for transient RPC failures.
4. Add per-job status and timestamps.
5. Add a runner that can execute synchronously in dev and asynchronously in production.
6. Add APIs and UI visibility for job status.

Acceptance criteria:

- cluster backfill does not block request/response APIs
- analysis retries are controlled and visible
- tx analysis throughput can scale without changing the domain model

## Phase 4: Expand Single-Transaction Intent Recognition

Goal: improve recognition quality without turning the system into a monolithic hardcoded classifier.

### 4.1 Split Recognition Into Layers

Add clear layers:

1. Protocol classification
2. execution pattern recognition
3. intent recognition
4. scenario adaptation
5. accounting interpretation

Do not let one recognizer try to do all five.

### 4.2 Add Better Intermediate Facts

The current recognizers mostly look at selector, logs, and derived flows. Add richer normalized facts:

- touched contracts with code identity
- ordered call timeline derived from callTracer frames
- per-call trace address, call type, selector, from, to, native value, and parent/child relationship
- log and token-flow attachments to the nearest call frame when evidence allows it
- top-level and internal call motifs
- approval relationships
- token in/out net deltas by address
- router/aggregator hints
- failure/revert location hints
- counterparty role hints

These facts should remain evidence-first and versioned.

### 4.2.1 Call Timeline Projection

The UI timeline should not be only an asset-flow list.

Target model:

1. Always start with the top-level execution entry:
   - tx `from`
   - tx `to`
   - selector
   - native value
   - gas payer
2. Render internal calls from `normalized_execution_facts.call_tree` when trace is available.
3. Attach ERC20/native `derived_flows` to the relevant call step when possible.
4. Show calls even when they do not emit token transfers, because these calls often explain router, pool, vault, or protocol state transitions.
5. Fall back to the current flow-only timeline when trace is unavailable.

Current data support:

- supported when `LEDGERFLOW_TRACE_ENABLED=true` and the RPC returns `debug_traceTransaction` callTracer data
- partially supported today because `call_tree` is already persisted in `TransactionAnalysis`
- not enough for full diagnostics yet because the stored trace frame currently omits output data, revert reason, gas, and error fields

Implementation tasks:

1. Add a normalized `execution_timeline` projection from `call_tree`.
2. Preserve stable ordering with `trace_address`.
3. Add a lightweight call-frame DTO for web rendering.
4. Add a flow-to-call attachment heuristic:
   - exact `from/to` match first
   - token log address / pool address match second
   - fallback to nearest protocol/router frame
5. Update web timeline to display:
   - call step
   - attached token/native transfers
   - no-transfer call steps as compact rows
6. Add tests for:
   - top-level tx entry plus no-transfer internal call
   - router call with attached ERC20 transfers
   - trace-disabled fallback

### 4.3 Introduce Recognizer Families

Add recognizer categories:

- fact recognizers
- protocol recognizers
- intent recognizers
- scenario recognizers
- warnings/anomaly recognizers

Each result should identify its family so downstream logic can merge or prioritize them safely.

### 4.4 Improve Output Shape

Extend recognizer outputs with:

- result family
- subject address or contract if applicable
- stronger structured attributes
- explicit negative evidence or ambiguity reasons
- optional parent recognizer dependency

This helps later composition and conflict resolution.

### 4.5 Add Ruleset-Driven Signature And Protocol Knowledge

Move static knowledge out of recognizer code over time:

- selector candidate registry
- common event signatures
- known router or vault contract families
- protocol-specific address sets or code hashes

These should be versioned data assets, not hardcoded everywhere.

### 4.5.1 Protocol / Venue Intelligence Registry

Add a general protocol intelligence layer before adding many protocol-specific recognizers.

Do not build this as "DEX-only" infrastructure. DEX pools are the first target because they unlock swap and arbitrage explanations, but the same registry must support lending, vaults, bridges, routers, aggregators, liquidators, and future protocol families.

Core concepts:

- `address_profiles` remain the broad address-level identity store
- add protocol-specific metadata records keyed by `chain_id + address`
- classify addresses with `protocol_family`, `protocol_name`, `contract_role`, `confidence`, `source`, and `metadata`
- keep adapter output versioned so a wrong adapter can be replayed and corrected

Priority 1: DEX / Venue Intelligence

- known router and aggregator addresses:
  - Uniswap routers
  - Aerodrome routers
  - 0x / 1inch / OKX style routers when observed
- pool probes:
  - Uniswap V2-compatible pair: `token0()`, `token1()`, `getReserves()`, `factory()`
  - Uniswap V3-compatible pool: `token0()`, `token1()`, `fee()`, `slot0()`, `factory()`
  - Solidly/Aerodrome-style pool: `token0()`, `token1()`, `stable()`, `factory()`
  - concentrated-liquidity variants where Base usage warrants it
- event signatures:
  - V2 `Swap`
  - V3 `Swap`
  - Mint/Burn/Sync where relevant
- store pool metadata:
  - pool type
  - token0/token1
  - fee or stable flag
  - factory
  - first seen / last seen

Priority 2: Lending / Liquidation Intelligence

- protocol contracts and market contracts
- liquidation event signatures
- borrow / repay / supply / withdraw event signatures
- known protocol-specific market metadata where available

Priority 3: Vault / Bridge / LP Intelligence

- ERC4626-like vault probes
- LP token mint/burn relationships
- bridge escrow/router/message contracts
- canonical token or wrapped asset mappings

Implementation tasks:

1. Add protocol metadata tables or extend address intelligence with a separate `address_protocol_metadata` table.
2. Add protocol adapter trait:
   - `probe(address) -> Option<ProtocolAddressMetadata>`
   - `classify_log(log) -> Option<ProtocolEvent>`
   - `classify_call(call_frame) -> Option<ProtocolCall>`
3. Run adapters from the address worker when new contracts are discovered.
4. Cache negative probe results with TTL to avoid repeatedly calling unsupported methods.
5. Feed protocol metadata into:
   - tx timeline labels
   - scenario recognizers
   - route reconstruction
   - arbitrage/liquidation accounting
6. Add replay support so existing analyses can be reinterpreted after the registry learns new addresses.

### 4.6 Add Concrete New Recognizers

Recommended next recognizers:

1. `erc20_approval_recognizer`
2. `swap_flow_recognizer`
3. `router_call_pattern_recognizer`
4. `bridge_like_transfer_recognizer`
5. `deposit_withdraw_pattern_recognizer`
6. `failed_execution_classifier`
7. `value_recycling_or_internal_shuffling_recognizer`

Each should be narrow and evidence-bound.

### 4.7 Add Recognizer Composition

Add a merge layer that can produce:

- top candidate intent
- supporting secondary labels
- unresolved competing interpretations

Do not force a single label when evidence is mixed.

Acceptance criteria:

- recognizers are easier to extend without editing many files
- outputs become more composable
- protocol-specific logic can grow via plugins and rulesets rather than core rewrites

## Phase 5: Prepare For Ongoing Cluster Monitoring

Goal: move from one-time backfill to continuous cluster coverage.

Tasks:

1. Add cluster watch state:
   - enabled/disabled
   - last discovery cursor
   - last analyzed block
2. Add a polling or event-driven discovery loop on the analysis side.
3. Refresh spend rollups incrementally.
4. Add simple operator alerts for stalled coverage or repeated failures.

Acceptance criteria:

- once a cluster is onboarded, coverage can continue without manual re-import
- historical backfill and forward monitoring use the same consumer model

## Phase 6: Monitor-Driven Opportunity Detection

Goal: turn the live monitor into a practical "large liquidation / arbitrage candidate" detector without waiting for perfect protocol coverage.

### 6.1 Reduce Alert False Negatives

Current reporter behavior only sends email when a tx is already recognized as liquidation/arbitrage and has positive strategy net. That is too strict while protocol adapters are still incomplete.

Tasks:

1. Split monitor findings into levels:
   - `confirmed`: recognized liquidation/arbitrage with positive strategy net
   - `probable`: recognized liquidation/arbitrage or strong multi-leg route, but PnL is unknown or partially unresolved
   - `interesting`: high-volume complex value movement worth human review
2. Send email for all three levels, with the level clearly shown in subject/body.
3. Keep the existing top-N and min score gates to avoid unlimited spam.

### 6.2 Feed Address Intelligence From Monitor Candidates

The monitor sees fresh high-volume txs before a user manually opens them. Those txs should enrich the address knowledge base automatically.

Tasks:

1. For every inspected top tx, enqueue address intelligence candidates for:
   - tx sender
   - tx recipient/executor
   - net delta addresses
   - ERC20 token contracts
2. Store metadata with `source=trace_monitor`, `tx_hash`, `block_number`, and observed roles.
3. Let `ledgerflow_address_worker` classify these asynchronously; do not block monitor reporting on address analysis.

### 6.3 Protocol / Venue Intelligence Growth Loop

DEX is the first priority, but the mechanism must support other protocol families.

Tasks:

1. Continue using `address_profiles` and address labels as the broad identity layer.
2. Add a protocol metadata table or versioned metadata record for:
   - `protocol_family`
   - `protocol_name`
   - `contract_role`
   - adapter version
   - confidence
   - raw probe metadata
3. Start with DEX pool/router probes:
   - V2-compatible pool
   - V3-compatible pool
   - Solidly/Aerodrome-compatible pool
   - known routers and aggregators
4. Add later protocol families:
   - lending/liquidation
   - vaults
   - bridges
   - LP managers

### 6.4 Intent Recognition Without Full Protocol Decode

Do not require full protocol identification before surfacing useful findings.

Tasks:

1. Add intermediate labels:
   - `swap_like`
   - `route_like`
   - `arbitrage_like`
   - `liquidation_like`
   - `complex_value_movement`
2. Use derived flows and execution timeline to detect:
   - multi-token routes
   - repeated pool/venue touches
   - subject net gain with price unknown
   - protocol repayment + collateral receipt patterns
3. Keep unknown PnL explicit instead of converting missing price into loss.

### 6.5 Monitor Email Quality

Tasks:

1. Include share link for every reportable tx.
2. Include level, candidate kind, volume score, block, and short reason.
3. Include prepared X post text, but do not auto-post until API credentials and approval flow are explicit.

Acceptance criteria:

- the monitor sends emails for high-value probable candidates even when exact profit is unresolved
- top txs automatically seed address intelligence
- DEX/pool/router labels improve over time without manual per-address analysis
- confirmed liquidation/arbitrage still appears as the strongest signal, not the only signal

### 6.6 Lazy Pricing System

Goal: convert transaction and cluster flows into USDC/USDT-denominated estimates without building a full-chain price index.

Tasks:

1. Store raw price facts in `price_observations`.
2. Store query-ready bucketed prices in `token_price_cache`.
3. Store unresolved work in `price_requests`.
4. Extract high-confidence `tx_execution` prices only when a token flow is paired with USDC/USDT/USDbC in the same transaction.
5. Queue missing token prices instead of blocking user-facing analysis.
6. Later add a worker that resolves `price_requests` from:
   - nearby DEX swap observations
   - pool spot prices
   - WETH/USDC route composition
   - optional external APIs as a fallback

Performance constraints:

1. Never scan all tokens.
2. Deduplicate requests by token + quote + block bucket.
3. Use block buckets instead of per-block cache rows.
4. Prefer tx-local facts over external market APIs.

## Suggested Execution Order

Build in this order:

1. Phase 1 cluster discovery/backfill
2. Phase 2 spend semantics and coverage
3. Phase 3 durable analysis jobs
4. Phase 4 recognizer expansion
5. Phase 5 ongoing monitoring

This order matters. Better intent recognizers will not fix the product if cluster discovery is still missing.

## Concrete Task List For The Next Sprint

If you want the shortest path to a materially better product, do these first:

1. Add `IndexerClient` in the analysis project.
2. Add cluster backfill run tables and repository methods.
3. Add `POST /clusters/{cluster_id}/backfill`.
4. Add discovered/analyzed/failed/pending coverage reporting.
5. Show coverage in the web UI.
6. Add one new recognizer family split:
   - keep `selector_presence`
   - add a dedicated `erc20_approval_recognizer`
   - add a dedicated `swap_flow_recognizer`
7. Add tests covering:
   - cluster create -> backfill -> analysis -> summary
   - duplicate tx discovery across addresses
   - partial coverage visibility
   - recognizer composition with mixed evidence
