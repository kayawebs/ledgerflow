# LedgerFlow Analysis Tool Project

This project context is for the transaction analysis and cluster spend tooling only.

## Current Scope

The analysis tool consumes transaction candidates and raw transaction evidence, then produces normalized analyses, evidence packs, recognizer outputs, flows, and cluster spend summaries.

In the current conversation/thread, work is limited to:

- FastAPI/Axum application API under `src/api/`
- analysis orchestration under `src/services/analysis.rs`
- cluster and spend summary logic under `src/services/cluster.rs`
- domain models under `src/domain/`
- ingestion, normalization, flows, recognition, accounting, validation, evidence pack, repository, and web UI modules
- PostgreSQL migrations related to analysis, clusters, attempts, opportunities, flows, rollups, and evidence packs
- docs and tests for the analysis product

## Explicit Non-Scope

Do not modify the standalone indexer implementation from this thread.

Files and modules considered indexer-owned:

- `src/indexer/**`
- `src/bin/ledgerflow_indexer.rs`
- ClickHouse table definitions and indexer storage logic
- indexer runtime configuration variables prefixed with `LEDGERFLOW_INDEXER_`
- indexer worker scheduling, RPC fetch strategy, retry strategy, and progress handling

If analysis-tool work needs new behavior from the indexer, treat it as a producer contract request and document the required API or schema change. Implement indexer changes in the separate indexer thread.

## Producer / Consumer Boundary

The indexer is a producer of top-level address transaction references.

The analysis tool is a consumer of transaction candidates and must not assume implementation details of the indexer storage engine.

Allowed coupling:

- HTTP client calls to a documented indexer API
- stable DTOs for address transaction references
- optional health/progress reads for operator visibility

Disallowed coupling:

- direct ClickHouse queries from analysis services
- importing `src/indexer/*` from analysis code
- sharing mutable progress state between analysis and indexer
- making cluster spend correctness depend on indexer internals

## Intended Integration

The intended bridge is an analysis-side client and service:

- `IndexerClient`: talks to the indexer HTTP API.
- `ClusterBackfillService`: resolves cluster addresses to tx candidates via the indexer, deduplicates tx hashes, and submits them to the existing analysis pipeline.
- `ClusterBackfillJob`: tracks backfill status without blocking cluster creation or spend summary reads.

The first implementation should prefer explicit user-triggered backfill, for example:

- `POST /clusters/{cluster_id}/backfill`
- `GET /clusters/{cluster_id}/backfill-runs/{run_id}`

Cluster spend summaries should continue to read from persisted PostgreSQL analyses. The indexer only helps discover which transactions should be analyzed.

## Product Goal

The analysis product should answer:

- which transactions touched this address cluster
- which transactions have been analyzed
- what gas/native/token spend was attributable to the cluster
- what remains unanalyzed or unresolved
- what evidence supports each label or spend item

Partial coverage is acceptable if the response makes coverage explicit.

## Engineering Rules

- Keep route handlers thin.
- Keep business logic in services.
- Keep the analysis schema stable and versioned.
- Preserve evidence-first semantics.
- Every inference must link back to evidence refs where possible.
- Unknown and unresolved fields are first-class results.
- Do not block cluster creation on historical backfill.
- Do not let one failed tx analysis abort an entire cluster backfill run.
- Commit analysis-tool changes separately from indexer changes.

## Near-Term Plan

1. Add an analysis-side indexer HTTP client with narrow DTOs.
2. Add cluster backfill job models in PostgreSQL.
3. Add a service that pulls tx candidates from the indexer, deduplicates them, and enqueues or runs analysis.
4. Expose backfill trigger and status endpoints.
5. Update the web UI to show coverage: discovered, analyzed, failed, pending.
6. Keep spend summaries based on persisted analysis outputs only.

## Current Product Gaps

The current cluster flow is structurally incomplete, not just cosmetically weak.

- Cluster creation only persists cluster metadata and addresses.
- Cluster spend summary only aggregates already-persisted analyses.
- There is no analysis-side transaction discovery or backfill job yet.
- There is no coverage model showing discovered vs analyzed vs failed txs.
- There is no operator-visible backlog or retry surface for cluster analysis.

This is why a newly created cluster can appear to have "wrong" or empty spend output even when the addresses are valid: the summary is currently correct only for the subset of transactions that were already analyzed before the summary request.

## Current Priority

Use the indexer as a producer and finish the consumer side in the analysis tool before adding more UI polish.

The next implementation work should follow the roadmap in `docs/analysis/roadmap.md`.
