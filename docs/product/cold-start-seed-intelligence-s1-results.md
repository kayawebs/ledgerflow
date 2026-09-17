# LedgerFlow Cold-Start Seed Intelligence S1 Results

Date: 2026-08-14 UTC  
Chain: Robinhood Chain (`4663`)  
Status: completed within the bounded S1 development cycle

The implementation scope and trust rules are defined in
[`cold-start-seed-intelligence-s1.md`](cold-start-seed-intelligence-s1.md).

## Acceptance results

| Requirement | Result | Evidence |
|---|---|---|
| Migration-backed seed registry and run history | PASS | Migrations 0034-0037 are applied successfully. |
| Official, protocol, pool and asset seeds are database-driven | PASS | `intelligence_seed_addresses` and `effective_intelligence_seeds`; no address list is embedded in worker code. |
| Conservative MEV seed selection | PASS | 1,000 `mev_actor` rows are `candidate`; none were automatically promoted to verified. |
| Evidence and limitations persist with every candidate | PASS | Candidate count, route count, sample transactions, prior automation signal and explicit limitations are stored as JSONB. |
| Seed jobs preempt generic address work | PASS | Production worker ceiling is priority 39; producer ceiling is priority 20. |
| Generic address facts/backlog remain recoverable | PASS | 7,329,467 generic pending/failed jobs remained deferred at the recorded snapshot. No queue deletion was performed. |
| Seed planner is idempotent | PASS | A stable consecutive run reported zero changes across all six mutation counters. |
| Seed list, summary and metrics APIs | PASS | Live API returned real database data; summary response completed in 14.86 ms. |
| High-frequency claim path is bounded | PASS | Partial migration index reduced the representative claim plan from seconds to 0.162 ms. |
| Canonical Indexer is uninterrupted | PASS | Container ID and start time remained unchanged; health was OK and observed lag was two blocks. |
| Searcher remains untouched | PASS | No Searcher service, configuration or process was changed. |
| Typecheck and automated tests | PASS | `cargo check --all-targets` and `cargo test --all-targets` completed successfully. |

## Live data snapshot

Recorded after final deployment at approximately 2026-08-14 11:12 UTC:

| Measure | Value |
|---|---:|
| Effective seed addresses | 1,886 |
| Active effective seeds | 886 |
| Candidate effective seeds | 1,000 |
| Official contracts | 21 |
| Protocol contracts / active pools | 535 |
| Distinct active assets | 331 |
| MEV actor candidates | 1,000 |
| Duplicate effective addresses | 0 |
| Targeted pending/failed jobs | 355 |
| Deferred generic pending/failed jobs | 7,329,467 |

Seed-kind row counts should not be added to obtain the effective count because
one address can have evidence from more than one seed kind or source. The
effective view deterministically collapses these records.

## Runtime verification

- `/intelligence/seeds/summary?chain_id=4663`: HTTP success, 14.86 ms.
- `/intelligence/seeds`: returned persisted evidence and explicit candidate
  limitations.
- `/metrics`: exposed seed totals, MEV candidate totals, targeted queue depth,
  deferred generic depth and last-run age.
- Address worker: four slots, maximum priority 39, successfully consuming seed
  EOAs and contracts.
- Seed EOA analyses complete in roughly 70-200 ms on the observed live path.
- The seed claim plan is 0.162 ms. DEX asset lookup indexes reduce the raw
  wrapped-native pool lookup to 76.5 ms; replacing the correlated stable-token
  label check reduces the complete worst-case lookup from 5.11 s to 155.9 ms
  on cold buffers.
- Address producer: maximum enqueue priority 20; canonical address facts still
  persist independently of heavy-analysis scheduling.
- Seed worker: five-minute polling; a stable iteration produced zero changes.
- Indexer: same container since 2026-08-05, `/health` returned OK in 0.67 ms;
  observed live worker block 36,207,333 versus RPC head 36,207,335.

## Automated checks

`cargo test --all-targets` passed all 137 tests with zero failures. This includes
the seed ceiling integration test, address fact preservation test, deterministic
evidence/profit regression tests and the 100-transaction Base regression
fixture. `cargo check --all-targets` also passed.

There are existing dead-code warnings in legacy web handlers. They do not fail
the build and were not expanded in this backend-focused stage.

## Operational notes and known limits

- Exact generic queue depth is calculated once per seed-worker iteration and
  persisted in the run record. API and metrics paths read that snapshot instead
  of scanning millions of rows on every request.
- DEX pool pricing uses database-maintained token-pair expression indexes and a
  one-time stable-token set. No token or pool address is hard-coded in the
  pricing query.
- The seed planning iteration currently spends roughly 3-6 seconds scanning
  curated evidence and calculating the exact queue snapshot every five minutes.
  This is off the interactive path but remains an S2 optimization candidate.
- One pre-deployment validation run failed while the MEV aggregation SQL was
  being corrected. It remains in run history for auditability; subsequent
  production runs completed successfully.
- Candidate selection proves repeated multi-pool execution patterns only. It
  does not prove MEV intent, ownership, realized profit or economic causality.

## Next bounded stage: S2

S2 should be separately timeboxed and should consume, not replace, S1:

1. Build seed-scoped EOA → executor → pool/router → profit-recipient and funding
   relationships from deterministic transaction, log and trace evidence.
2. Schedule bounded transaction-history accounting only for seeds and their
   evidence-backed first-hop relationships.
3. Add a review/promotion gate for verified automated/MEV actors; profit remains
   unknown unless deterministic asset-flow and gas accounting closes.
4. Expose seed coverage, relationship coverage and promotion limitations in the
   Execution Intelligence UI.

These S2 capabilities are not marked complete by S1.
