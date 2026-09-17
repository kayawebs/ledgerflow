# LedgerFlow Searcher Cluster Intelligence v1 — Gap Analysis

Date: 2026-08-20

## Scope and product contract

This phase is limited to Base (`8453`) and Robinhood Chain (`4663`) and to execution-oriented seeds: funding/collector addresses, worker EOAs, executor contracts, settlement recipients, high-frequency DEX actors, and candidate searcher clusters. An observation seed is an entry point, not a claim of ownership or a complete economic entity.

The target data flow is:

```text
Observation seed
  -> runtime source attestation
  -> seed role classification
  -> bounded 1-hop / 2-hop relationship discovery
  -> candidate execution cluster
  -> execution reconstruction
  -> deterministic economic verification
  -> 24h / 7d / 30d summaries and change detection
  -> incremental observation
```

## P0 runtime data-source audit

### Runtime topology

| Network | Control/API data plane | Indexer data plane | Runtime finding |
|---|---|---|---|
| Robinhood Chain | Dedicated PostgreSQL/API, configured chain `4663` | Dedicated ClickHouse/indexer, RPC reports `4663` | Live and chain-scoped |
| Base | Dedicated PostgreSQL/API, configured chain `8453` | Dedicated ClickHouse/indexer, RPC reports `8453` | Live and chain-scoped |

The Web control plane stores two chain routes. Its request adapter resolves the selected chain to a distinct API and indexer route, sends `x-ledgerflow-chain-id`, and rejects a response that explicitly returns a different `chain_id`.

### Indexed range measured during audit

| Network | Live indexed from | Live indexed through | Chain head | Live lag | Full-history status |
|---|---:|---:|---:|---:|---|
| Robinhood Chain | 27,251,941 | 41,830,036 | 41,830,039 | 3 blocks | Historical range is partial; a large gap remains |
| Base | 49,850,596 | 50,240,310 | 50,240,312 | 2 blocks | No contiguous full-history worker; history is address-lazy |

These values are point-in-time audit evidence and must not be hard-coded into product behavior.

### Base provenance verification

- The Base API readiness check returned configured/RPC `chain_id=8453`.
- The Base indexer progress endpoint returned `chain_id=8453` and a live worker near the Base chain head.
- A transaction in the current observed collector address timeline was independently resolved through the Base RPC gateway with the same hash and block number.
- The Robinhood API did not return that Base observation; the Base API did not return it when queried as chain `4663`.
- The Base observed-object and activity rows reside in the Base PostgreSQL data plane, separate from Robinhood rows.

### P0 gap

The runtime routes are genuinely separate, but the API currently exposes only a top-level `chain_id`. It does not return a complete, auditable source-attestation object, and read endpoints do not reject a caller-supplied chain that differs from the data plane before querying PostgreSQL. The Web validates response chain IDs only when the response contains one. Therefore the system is mostly chain-scoped but not yet fail-closed end to end.

Required remediation in this phase:

- add a source-attestation contract with `chain_id`, `data_source`, indexed range, chain head, lag, and update time;
- validate RPC chain, indexer chain, API plane chain, and selected chain as one invariant;
- return an unavailable response instead of zero/unknown intelligence when the invariant fails;
- display source range and freshness without exposing RPC URLs or infrastructure hostnames.

## Existing reusable capabilities

### Complete or reliable enough to reuse

- Chain-scoped Base and Robinhood live indexers.
- Bounded address-lazy discovery without requiring a full chain replay.
- Transaction, receipt, block, log, trace, and token-transfer ingestion.
- `observed_address_objects` analysis queue with checkpoint, retry, deduplication, and incremental updates.
- Deterministic per-address activity, behavior, relationship, economic, change, timeline, and brief tables.
- Protocol/pool evidence attached to transaction analyses.
- Exact receipt gas accounting, including Base L1 fee when present.
- Fail-closed deterministic profit verifier for an explicit accounting scope.
- Web chain registry and per-chain API/indexer routing.
- Automatic observation and notification control-plane wiring.

### Partial capabilities

- Seed relationships exist, but current names are address-centric and do not explain cluster role or evidence reason.
- Existing relationship rows cap supporting hashes and omit transferred-value aggregates.
- Current action classification correctly avoids assigning an enclosing arbitrage label to a transfer-only recipient, but does not follow the transaction sender/executor behind that receipt transfer.
- Economic verification works for an explicit scope but current Observed Object scope contains only the seed.
- 24h/7d/30d summaries exist per address, not per candidate execution cluster.
- Existing `address_clusters` are internal/manual objects and are not evidence-scored candidate clusters.

## Missing capabilities

1. Runtime source attestation and strict read-side chain mismatch rejection.
2. Seed role classifier with supporting transactions, transfer edges, confidence, and limitations.
3. Controlled relationship expansion that distinguishes workers, executors, settlement recipients, protocols, routers, and pools.
4. Evidence-scored candidate cluster membership and manual overrides.
5. Cluster execution records joining worker, executor, protocols, pools, route, settlement, gas, and verification state.
6. Cluster accounting separating execution profit, settlement inflow, funding, and failed gas.
7. Cluster 24h/7d/30d summaries, baseline, and changes.
8. Automatic incremental refresh for cluster members.
9. Private ground-truth regression harness and discovery-quality metrics.
10. Product rendering that answers the seven required questions from cluster facts rather than seed-only facts.

## Data-quality risks

- A receipt transfer into a collector proves a transfer, not ownership, common control, or profit.
- The outer transaction sender can be a user, relayer, worker, or unrelated caller; repeated co-execution is required before cluster candidacy.
- A called contract can be an executor, router, pool, protocol, or generic application contract. Protocol registry evidence and contract role must exclude infrastructure from the control/economic scope.
- Base full history is not globally indexed. Address-specific transfer log discovery can cover a requested range, while top-level transaction discovery is limited to existing indexed blocks unless reached through related transaction evidence.
- Candidate member recall and false-positive rate depend on available transfer, call, trace, and registry evidence. Similar gas or route patterns are weak signals only.
- Token inflow, funding, settlement, and inventory appreciation must never be presented as profit.
- A verified token surplus and native gas cost cannot be collapsed into one scalar net result unless the assets are deterministically comparable.
- Archive state or trace unavailability must fail economic verification closed.

## Implementation order

1. Source attestation and fail-closed chain contract.
2. Migration-backed cluster state, members, relationships, executions, economics, windows, changes, and overrides.
3. Seed role classification.
4. Bounded relationship expansion and cluster scoring.
5. Transaction reconstruction for candidate worker/executor paths.
6. Deterministic cluster accounting and exclusions.
7. Window summaries, baseline, incremental refresh, checkpoint, and retry.
8. Cluster intelligence API and Observed Object/Workspace rendering.
9. Manual member correction endpoints.
10. Private ground-truth and external-seed regression.
11. Runtime performance, build, tests, screenshots, and acceptance report.

## Expected files

- `migrations/0050_searcher_cluster_intelligence_v1.{up,down}.sql`
- `src/domain/value_objects.rs`
- `src/domain/interfaces.rs`
- `src/services/indexer_client.rs`
- `src/services/searcher_cluster_intelligence.rs`
- `src/services/mod.rs`
- `src/bin/ledgerflow_searcher_cluster_worker.rs`
- `src/api/mod.rs`
- `src/main.rs`
- `src/web/mod.rs`
- `docker-compose.yml`
- `.env.example`
- `schemas/searcher-cluster-intelligence-v1.schema.json`
- `scripts/verify_searcher_cluster_intelligence.mjs`
- `tests/searcher_cluster_intelligence.rs`
- `docs/product/base-runtime-data-source-audit.md`
- `docs/product/searcher-cluster-intelligence-results.md`

## Explicit non-goals

- No website, navigation, naming, or generic dashboard changes.
- No new chain or protocol classifier.
- No full-chain historical replay requirement.
- No generic wallet PnL, offchain cost basis, CEX accounting, or ownership attribution.
- No LLM-generated role, relationship, or economic fact.
- No production Searcher modification or interruption.
