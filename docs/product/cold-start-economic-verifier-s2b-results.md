# LedgerFlow Cold-Start Economic Verifier S2B Results

Date: 2026-08-15 UTC

Scope: bounded Robinhood Chain seed analysis on `chain_id=4663`. Full historical
indexing was not started and is not an acceptance dependency.

## Outcome

The stage is operational. A durable Economic Verifier now selects at most two
representative multi-pool candidates per database-backed `mev_actor` seed and
reconciles each candidate from transaction, receipt, logs, trace, archive
balances, and exact gas. Results flow into the production Execution Event
adapter and operational metrics.

At the 2026-08-15 03:55 UTC observation point:

| Result | Count |
| --- | ---: |
| `verified` | 1 |
| `insufficient_evidence` | 25 |
| `dismissed` | 0 |
| failed jobs | 0 |
| pending/running jobs | 74 |

The initial deployment admitted 100 jobs before the tuned defaults were
installed. That one-time queue is draining sequentially. The deployed defaults
now cap new scheduling at 10 jobs per pass and normal pending capacity at 50;
they do not delete already persisted work.

## Verified real-chain case

- Transaction:
  `0x22ca0757690af3e98faa60ce75ed37729353b2cb06f75caf511be7217071d652`
- Explicit accounting scope:
  `0x8529e9b7799339ddf2f70e2adf4ad9aec1b0dd54` (recorded sender only)
- Receipt status: success
- Deterministic swap-log emitters: 14 distinct pools
- Verified token: `0x0bd7d308f8e1639fab988df18a8011f41eacad73`
  (`WETH`, 18 decimals from persisted token metadata)
- Positive token surplus: `47166241900664` raw units
- Exact native gas cost: `221864292984000` raw units
- Ruleset: `ledgerflow.economic-verifier.v1`
- Method: `seed_sender_accounting_v1` using
  `archive_balance_snapshot_receipt_trace_reconciliation_v1`
- Confidence: `0.98`

The event is `Verified` because the sender-scoped token delta reconciles across
receipt Transfers and block-before/block-after archive balances, native trace
flows reconcile with the exact receipt gas, and the execution has more than
one deterministic pool emitter. The UI exposes the token surplus as gross
captured value and gas separately. It deliberately leaves net searcher profit
unknown because it has no deterministic conversion between unlike assets.

## Safe incomplete-evidence case

Transaction
`0x48ae62665c10a77a860bad12f20579a44cccc22a489032882adc26bb9532443a`
was persisted as `insufficient_evidence` with the limitation
`no standard ERC-20 scope flows`. Profit amount and gas result fields remained
null. This confirms that a strong detector score or available trace does not
promote an unsupported economic conclusion.

## Architecture

```text
effective_intelligence_seeds (mev_actor only)
                    |
                    v
ranked existing arbitrage candidates + existing analyses
                    |
          bounded durable job queue
                    |
                    v
transaction / receipt / logs / trace / archive balances
                    |
          deterministic sender accounting
                    |
        +-----------+--------------------+
        |           |                    |
     verified  insufficient_evidence  dismissed
        |           |                    |
        +-----------+--------------------+
                    |
 economic_verification_results + compatible candidate projection
                    |
       ExecutionEvent adapter + /metrics + seed summary
```

## Performance and recovery observations

- A direct bounded one-shot verified the real case in about 1.3 seconds from
  worker start to persisted outcome.
- The previously running RPC gateway image allowed an expensive trace request
  to wait excessively under concurrent load. Recreating it with the current
  fair token-bucket/timeout implementation reduced the same gateway trace probe
  to 1.141 seconds.
- After gateway replacement, the worker completed jobs continuously with no
  failed job at the observation point.
- Restart recovery was exercised against an abandoned `running` claim; the new
  worker reported `recovered_jobs=1` and completed the claim.
- Completion now synchronizes the trace-enriched analysis digest. Live SQL
  verification showed `digest_current=true` for all post-fix completed jobs,
  preventing immediate duplicate scheduling.
- Live indexer lag was 2 blocks at the observation point. The history worker
  remained stopped; its preserved cursor at block 6,069,674 is diagnostic state,
  not an active process or a full-coverage claim.

## Verification commands

| Check | Result |
| --- | --- |
| `cargo test --all-targets` | PASS — 146 tests, 0 failed |
| Economic Verifier focused tests | PASS — 3 tests, 0 failed |
| Execution Event adapter focused tests | PASS — 4 tests, 0 failed |
| `cargo check --all-targets` | PASS (pre-existing dead-code warnings remain) |
| `cargo fmt --all` | PASS |
| `git diff --check` | PASS |
| `docker compose config --quiet` | PASS |
| release Docker build | PASS |
| PostgreSQL migration `0039` | PASS on the running database |
| API `/health` | PASS |
| API `/ready` | PASS — PostgreSQL and chain-4663 RPC ready |
| API `/metrics` Economic Verifier series | PASS |
| current gateway trace probe | PASS — HTTP 200 |
| live Searcher process preservation | PASS — PID 2998703 unchanged |

## Acceptance status

| Requirement | Status | Evidence |
| --- | --- | --- |
| Additive migration and reversible schema | PASS | `0039_economic_verifier.up.sql` and `.down.sql` |
| RPC chain validation before work | PASS | Worker refuses a chain-ID mismatch |
| Database-backed seed selection | PASS | `effective_intelligence_seeds`; no compiled seed addresses |
| Bounded scheduling | PASS | 10 new/pass, 50 normal pending, 2 candidates/seed |
| Durable retry and stale-claim recovery | PASS | persisted jobs plus observed `recovered_jobs=1` |
| Deterministic verified outcome | PASS | real transaction and exact evidence above |
| Unknown remains null | PASS | real insufficient-evidence case and unit test |
| No immediate post-trace duplicate | PASS | final job digest synchronization verified live |
| UI reads persisted verification | PASS | SQL join plus typed adapter tests |
| Verified and Detected are visually distinct | PASS | typed status model and production renderer |
| Operational metrics | PASS | queue/failure/outcome/age metrics are live |
| Real-chain dismissed example | NOT IMPLEMENTED | Dismissal unit path passes; no real dismissed sample observed yet |
| Cross-address executor ownership inference | NOT IMPLEMENTED | Deliberately outside the trust boundary |
| Cross-asset net-profit conversion | NOT IMPLEMENTED | Displayed as `Unknown`, not guessed |
| Full-history backfill | NOT IMPLEMENTED | Explicit non-goal and not running |

## Known limitations and next priority

The sender-only scope is intentionally conservative. Many true MEV executions
settle into a separate executor or recipient, so they remain
`insufficient_evidence` until ownership/role relationships are proven from
independent deterministic evidence. The next highest-value stage is therefore
not full history: it is bounded relationship proof for the existing seed set,
followed by re-verification of only the affected evidence transactions.

The legacy full `TodaySnapshot` warm-up still contains inventory aggregates
that can take several seconds on a cold web-process start. It is cached and
stale-while-revalidate, so normal overview requests do not repeat that cost,
but splitting the Execution Overview status read into a lean snapshot remains
a worthwhile frontend latency follow-up.
