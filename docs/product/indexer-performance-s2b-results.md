# LedgerFlow Indexer Performance S2B — Corrected Results

Date: 2026-08-15 UTC

## Decision

Full historical indexing is not a product prerequisite and is excluded from
normal Compose startup. LedgerFlow keeps the receipt/log-complete live indexer,
uses explicit transaction hashes for seed intelligence, and allows only bounded
lazy historical ranges. The optional history worker is stopped with its cursor
and already written data preserved.

The earlier 1,196 blocks/second result measured a canonical block plus full
transaction-body pass with receipts and logs disabled. It must not be described
as complete historical indexing or economic-analysis coverage.

## Measured estimates

At stop time the optional history cursor was block `6,069,674` in the fixed
range `0..=27,251,940`, leaving `21,182,266` blocks.

| Workload | Measured rate | Remaining-time estimate | Evidence quality |
| --- | ---: | ---: | --- |
| Canonical block + transaction body fast pass | about 1,105 sustained blocks/s; 1,196 blocks/s in a 106.65-second sample | theoretical 5.3 hours; allow 5–8 hours as density rises | No receipt status, gas, logs, token transfers, or trace |
| Receipt/log-complete pass, original settings | about 0.8 blocks/s | about 306 days | Receipt/log facts, no trace |
| Receipt/log-complete pass, tuned gateway and per-transaction receipts | about 2.63 blocks/s | about 93 days | Receipt/log facts, no trace |
| Practical receipt/log-complete historical plan | density-adjusted estimate | about 3–10 months | Still requires selective traces for internal value flow |
| Full historical trace enrichment | not benchmarked | unknown and longer | Potentially complete internal-call evidence |

These are extrapolations from the measured local RPC and current chain data,
not completion promises. Later blocks are denser, and node, ClickHouse, and RPC
contention affect sustained rates.

## Why full history is not required now

The existing S2A seed path selects at most eight explicit evidence transaction
hashes per seed, then retrieves the transaction, receipt, block, logs, and
optional trace directly by hash. It does not depend on the address-history
indexer. Live monitoring can be made complete and auditable from a recorded
activation block forward.

Full history is required only for claims such as:

- no missing lifetime transaction for an address;
- complete lifetime P&L, failure cost, or counterparty graph;
- exhaustive historical protocol revenue;
- chain-wide retrospective backtests.

Until those jobs are run, the product must say `complete since block X`,
`bounded lookback`, or `unknown`, rather than imply lifetime completeness.

## Current bottleneck

The cold-start database already contains approximately 1,900 seeds, 5,478
selected evidence transactions, 5,419 analyzed evidence transactions, and
22,238 first-hop relationships. About 98.9% of selected evidence transactions
already have a transaction analysis row. However, roughly 31,000 arbitrage
candidates remain unverified because the background analysis path does not run
the conservative economic confirmation/promotion step used by the on-demand
transaction detail path.

The bottleneck is therefore deterministic verification and product projection,
not discovery volume or raw historical block coverage.

## Fastest product sequence

1. **Bound indexing scope (completed):** live remains on; history is an opt-in
   profile; lazy requests above the configured range cap fail without scanning.
2. **Economic verifier:** consume the highest-value seed evidence transactions,
   calculate deterministic balance deltas, receipt status, gas, closed-loop
   profit when provable, and persist `verified`, `insufficient_evidence`, or
   `dismissed` with ruleset/input digests.
3. **Live completeness contract:** record each Monitor's activation block,
   analyze every relevant live transaction from that point, and expose lag and
   coverage in health/UI.
4. **Verified entity rollups:** derive success rate, verified profit, protocols,
   failures, and relationships only from verified evidence.
5. **Selective enrichment:** fetch receipt/trace data only for evidence hashes
   needed by a seed, alert, or investigation. Keep full history as an offline
   research operation.

This sequence should produce honestly `Verified` Overview events sooner than a
months-long historical evidence backfill.

## Acceptance status

| Check | Status | Evidence |
| --- | --- | --- |
| Live indexer remains current | PASS | Durable live cursor stayed two blocks behind head during measurement. |
| Optional history stopped without data loss | PASS | Container stopped; ClickHouse rows and cursor retained. |
| Normal Compose startup excludes history | PASS | `indexer-history` uses the `full-history` profile. |
| Oversized lazy ranges guarded | PASS | Configuration and service guard added; existing oversized jobs are failed by the worker. |
| Fast-pass ETA distinguished from evidence ETA | PASS | Separate measured estimates above. |
| Full historical canonical fast pass complete | NOT IMPLEMENTED | Intentionally stopped; not a product prerequisite. |
| Full historical receipt/log coverage | NOT IMPLEMENTED | Estimated in months, not scheduled for the product critical path. |
| Historical trace coverage | NOT IMPLEMENTED | No reliable duration benchmark; selective evidence traces are preferred. |

## Files changed in this correction

- `src/indexer/config.rs`
- `src/indexer/service.rs`
- `docker-compose.yml`
- `.env.example`
- `README.md`
- `docs/indexer.md`
- `docs/usage.md`
- `docs/product/cold-start-seed-intelligence-s1.md`
- `docs/product/indexer-performance-s2b-results.md`
