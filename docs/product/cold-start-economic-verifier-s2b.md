# LedgerFlow Cold-Start Economic Verifier S2B

Date: 2026-08-15 UTC

Timebox: one bounded development stage, targeted at one to two development
days. The stage processes representative transactions for database-backed MEV
seeds; it does not promise full-chain or lifetime coverage.

## Objective

Convert the most useful existing `Detected` multi-pool signals into auditable
economic verification outcomes:

```text
MEV candidate seed + explicit evidence transaction
                         |
                         v
transaction / receipt / block / logs / trace by hash
                         |
                         v
archive balance + exact gas reconciliation
                         |
             +-----------+----------------+
             |           |                |
          verified  insufficient       dismissed
                       evidence
```

The verifier is a background consumer. It does not depend on full historical
indexing and never queries unbounded address history.

## Trust boundary

- The accounting scope is the recorded candidate transaction sender only.
- An observed call target is not added to the scope because a top-level call
  does not prove common ownership or executor identity.
- `verified` requires a successful transaction, at least two deterministic
  swap-log emitters, receipt/trace availability, exact gas reconciliation, and
  one positive token balance delta with no principal or native outflow for the
  explicit scope.
- `dismissed` is reserved for contradictory evidence such as a reverted
  transaction or fewer than two actual swap pools.
- Missing archive state, trace, token-standard support, or ambiguous settlement
  produces `insufficient_evidence`, never zero profit or a guessed value.
- `verified` proves the recorded positive token surplus and exact native gas
  separately. It does not claim cross-asset net profit without deterministic
  conversion evidence.
- LLM output is not part of scheduling, accounting, classification, status, or
  persistence.

## Bounded scheduling

- Only `mev_actor` seeds in `candidate` or `active` state are eligible.
- Only existing `arbitrage_candidates` with an existing
  `transaction_analysis` row are scheduled.
- At most two representative candidate transactions per seed are considered
  per scheduling pass.
- New jobs per pass, batch size, concurrency, attempts, and retry delay are
  configuration values. Defaults are 10 new jobs per pass, 50 pending jobs,
  two sequential claims per iteration, and two candidates per seed.
- A digest change requeues the job; a process restart recovers stale claims.
- When trace-enriched analysis changes its digest during verification, job
  completion synchronizes the final digest so the same transaction is not
  immediately scheduled twice.

## Persistence

Migration `0039_economic_verifier` adds:

- `economic_verification_jobs` for durable scheduling, retry, and recovery;
- `economic_verification_results` for versioned deterministic outcomes;
- an `economic_verifier` intelligence source record.

The existing `arbitrage_candidates.confirmation_status` remains the compatible
read projection used by the current UI. The versioned result table is the
source of verification evidence.

## Acceptance target

- Migration is additive and reversible.
- Worker validates RPC chain ID before processing.
- Seed-first scheduling is bounded and deterministic.
- Restarted workers recover stale `running` jobs.
- A verified token surplus requires complete deterministic reconciliation;
  cross-asset net profit remains unknown unless it can also be reconciled.
- Unknown inputs remain null with explicit limitations.
- Results persist ruleset, method version, input digest, evidence, confidence,
  exact gas, and optional profit fields.
- Candidate projection uses `verified_profit`, `insufficient_evidence`, or
  `dismissed` without overwriting an existing stronger verified status.
- Execution Event UI renders verified and dismissed states from real records.
- Metrics expose queue, retry/failure, verified, insufficient, and dismissed
  counts.
- Unit tests, integration tests, type check, release build, migration, and a
  bounded real-data run pass.

## Non-goals

- Full historical indexing or trace backfill.
- New arbitrage, liquidation, or protocol classifier.
- Inferring ownership between sender and entry contract.
- Multi-address accounting scope discovery.
- USD valuation or cross-token profit comparison.
- Searcher changes.
