# LedgerFlow Cold-Start Seed Intelligence S2A Results

Date: 2026-08-15 UTC  
Chain: Robinhood Chain (`4663`)  
Status: implemented and deployed; bounded evidence queue is actively draining

The scope and evidence boundaries are defined in
[`cold-start-seed-intelligence-s2a.md`](cold-start-seed-intelligence-s2a.md).

## Acceptance results

| Requirement | Result | Evidence |
|---|---|---|
| Migration-backed first-hop relationships | PASS | Migration 0038 is applied successfully; relationships, indexes, evidence digests, run counters, and expiry are persisted. |
| Neutral, deterministic relationship semantics | PASS | Only `top_level_call_target`, `transaction_pool`, and confirmation-gated `confirmed_profit_recipient` are permitted. Evidence contains explicit limitations. |
| Existing candidate facts are the relationship source | PASS | Extractor reads persisted `arbitrage_candidates` and `arbitrage_candidate_pools`; it performs no protocol, ownership, intent, or profit guess. |
| Bounded evidence transaction selection | PASS | Latest eight explicit candidate hashes per MEV seed over 30 days; address worker hard-bounds the list to 16 and never calls full history for this mode. |
| Existing deterministic transaction analysis reused | PASS | Live worker logs show `fetch_bundle` for selected evidence hashes and `transaction_analysis` coverage increased after deployment. |
| Missing transaction-analysis service fails closed | PASS | An evidence request without an analysis service returns an invariant error instead of completing the job. Compose also provides the Indexer URL for existing history workflows. |
| Incomplete completed jobs recover automatically | PASS | Planner detected the pre-fix incomplete jobs and requeued all 1,000 selected MEV seed jobs; completed jobs remain eligible until all selected evidence hashes are persisted. |
| Retry does not redo successful evidence | PASS | Existing hashes are checked in `transaction_analysis`; only absent hashes are retried through the existing failed-job backoff. |
| Related addresses receive bounded priority | PASS | 3,427 distinct first-hop addresses were queued with relationship metadata and priorities 22-28; cap is configuration-driven. |
| Relationship and coverage APIs | PASS | Live endpoints return persisted evidence, coverage, semantic boundaries, and explicit `full_history_coverage: false`. |
| Coverage metrics | PASS | Four S2A gauges are exposed; expensive counts use the latest completed planner snapshot. |
| Candidate-only trust boundary | PASS | All 1,000 MEV seeds remain candidate-only; no S2A code promotes an event, actor, identity, or profit conclusion. |
| Planner idempotence | PASS | A controlled stable consecutive run produced zero changes for seed, relationship, and queue mutation counters. |
| Readiness follows configured adapter | PASS | Readiness now verifies RPC chain ID against the configured chain ID instead of a hard-coded Base ID. |
| Canonical Indexer uninterrupted | PASS | Container ID `aa71cce2...6144b` and start time `2026-08-05T00:28:48Z` remained unchanged. |
| Searcher untouched | PASS | No Searcher process, container, database, or configuration was changed. |
| Typecheck and automated tests | PASS | `cargo check --all-targets` passed; `cargo test --all-targets` passed 139 tests with zero failures. |

## Live relationship snapshot

Recorded on 2026-08-15 after migration and deployment:

| Relationship type | Rows | Seeds covered | Distinct related addresses |
|---|---:|---:|---:|
| `top_level_call_target` | 1,644 | 987 | 388 |
| `transaction_pool` | 20,222 | 1,000 | 3,039 |
| `confirmed_profit_recipient` | 0 | 0 | 0 |
| **Total** | **21,866** | **1,000** | **3,427 unique across types** |

Zero confirmed profit-recipient rows is the correct fail-closed result: the
current candidate dataset has no economically confirmed recipient fact that
meets the relationship gate.

Quality checks on the persisted rows:

- zero self-relationships;
- zero rows without sample transaction hashes;
- all 1,000 MEV seeds have at least one observed first-hop relationship;
- every relationship is chain-scoped, digest-backed, expiring, and source
  attributed.

## Evidence-analysis snapshot

| Measure | Value |
|---|---:|
| Selected MEV seeds | 1,000 |
| Distinct bounded evidence transactions | 5,418 |
| Previously analyzed before corrected worker path | 197 |
| Analyzed at latest planner snapshot | 775 |
| Coverage at latest planner snapshot | 14.30% |
| Completed / pending / running evidence jobs | 100 / 898 / 2 |
| Evidence jobs failed at that snapshot | 0 |

The increase from 197 to 775, together with live `fetch_bundle` logs, verifies
that the corrected bounded transaction path is active. The remaining work is a
recoverable background queue, not a claim of completed history coverage. S2A
does not wait for or promise full-history catch-up.

## API and runtime verification

Observed local timings after deployment:

| Endpoint | HTTP | Time |
|---|---:|---:|
| `/health` | 200 | 1.18 ms |
| `/ready` | 200 | 2.09 ms |
| `/intelligence/seeds/summary?chain_id=4663` | 200 | 10.76 ms |
| `/intelligence/seeds/relationships?chain_id=4663&limit=100` | 200 | 29.31 ms |
| `/intelligence/seeds/coverage?chain_id=4663&limit=100` | 200 | 229.60 ms |
| Indexer `/health` | 200 | 0.83 ms |
| Web `/` | 200 | 0.61 ms |

Readiness checks the configured adapter's chain ID plus PostgreSQL
connectivity.

Metrics exposed:

```text
ledgerflow_seed_relationships 21866
ledgerflow_seeds_with_relationships 1000
ledgerflow_seed_evidence_transactions 5418
ledgerflow_seed_evidence_transactions_analyzed <latest planner snapshot>
```

## Defect found during live verification

The first deployed address worker consumed evidence-tagged seed jobs without
an Indexer URL. Its constructor therefore omitted the transaction analysis
service and silently completed those jobs after address profiling. Live
coverage correctly revealed that no new transaction analysis had appeared.

The final implementation fixes this at three layers:

1. Compose supplies the address worker's Indexer URL for existing history
   workflows.
2. Explicit evidence analysis is attached even when no Indexer history source
   is configured.
3. Missing analysis capability now fails closed, and the planner requeues any
   completed seed whose selected evidence remains absent.

After redeployment the planner requeued 1,000 incomplete MEV seed jobs and live
coverage began increasing. This defect is not hidden or counted as a successful
pre-fix run.

## Automated checks

- `cargo fmt --all`: PASS
- `cargo check --all-targets`: PASS
- `cargo test --all-targets`: PASS, 139 passed, 0 failed
- Docker release build: PASS
- Compose configuration validation: PASS
- Migration 0038: PASS

Existing dead-code warnings remain in legacy web handlers. They do not fail the
build and were not expanded by this backend-focused stage.

## Known limits

- Evidence coverage is intentionally bounded and still draining; it is not
  full address history.
- No confirmed profit-recipient relationship is emitted without confirmed
  settlement evidence.
- Pool-touch observations can be numerous and do not establish strategy or
  causality.
- S2A does not inspect internal trace calls, funding provenance, or ownership
  clusters.
- The coverage endpoint's representative 100-row query is about 230 ms. It is
  suitable for an operational page but remains an optimization candidate.
- The five-minute planner still has off-path queries observed around 1.9-3.6
  seconds on the production dataset.
- Metrics are snapshot-based and can temporarily lag the live coverage API by
  one planner interval.
- S2A APIs are not yet surfaced as a dedicated seed-coverage panel in the web
  UI.
- Full historical block coverage is still **not complete** and is not claimed
  by S2A. The follow-up performance work is recorded in
  `docs/product/indexer-performance-s2b-results.md`.
- Indexer `/progress` now uses worker checkpoints by default and leaves exact
  `FINAL` storage counts plus the missing-endpoint join behind an explicit
  diagnostic parameter. The deployed default endpoint completed in 5.96 ms in
  the first post-change measurement.

## Next bounded stage: S2B

1. First address historical Indexer throughput/coverage and the slow progress
   endpoint, because canonical block completeness is a prerequisite shared by
   later intelligence stages.
2. Add deterministic trace/token-transfer relationships where source data is
   complete, keeping funding and ownership unknown unless proven.
3. Add a human review/promotion gate that requires economic and ordering
   evidence before an actor or event can become `Verified`.
4. Surface relationship and bounded coverage state in the Execution
   Intelligence entity/event workflow.
5. Optimize the planner and coverage query only after measuring queue and UI
   pressure.

S2B is not marked complete by this document.
