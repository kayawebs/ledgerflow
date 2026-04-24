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
- top-level and internal call motifs
- approval relationships
- token in/out net deltas by address
- router/aggregator hints
- failure/revert location hints
- counterparty role hints

These facts should remain evidence-first and versioned.

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
