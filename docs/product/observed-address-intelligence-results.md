# Observed Address Intelligence v1 results

Date: 2026-08-20  
Scope: Base (`8453`) and Robinhood Chain (`4663`), execution-oriented addresses  
Contract: `observed-address-intelligence.v1`

This report separates implemented behavior from live acceptance. `PASS` means
the result is backed by code plus an executed test or completed chain result.
An analysis that is still backfilling is `PARTIAL`, not `PASS`.

## 1. Current and new data flow

Previous flow:

```text
Observe
  -> workspace_entities
  -> small address-analysis sample
  -> Material/Pilot Events + labels
  -> profile cards
```

Implemented v1 flow:

```text
Observe once
  -> workspace observation (enabled by default)
  -> chain-plane observed_address_objects queue
  -> resolve EOA / contract from bytecode
  -> exact 30-day block boundary by block timestamps
  -> discover existing indexed top-level refs + cached analyses
  -> address-scoped ERC-20 Transfer log scan
  -> explicit newest-10,000 cap and coverage limitation
  -> restartable per-transaction activity ledger
       -> full receipt/trace analysis for execution endpoints
       -> tx/receipt/block/log path for transfer-only participants
  -> 24h / 7d / 30d deterministic summaries
  -> evidence-backed behavior + relationships
  -> fail-closed economic verification + exclusions
  -> previous-7d baseline + change signals
  -> automatic 24h / weekly / monthly briefs
  -> ready; continuously re-queued when the sealed head advances
```

The private Web application stores workspace ownership and preferences. Base
and Robinhood remain isolated data planes with their own PostgreSQL, indexer,
RPC gateway, API, and observation worker. One control-plane notification worker
polls evidence-backed change signals and handles deduplication/retry/DLQ.

Status: **PASS** for architecture and deployed queue flow.

## 2. Modified files

Core implementation:

- `src/services/observation_intelligence.rs`
- `src/bin/ledgerflow_observation_worker.rs`
- `src/bin/ledgerflow_observation_alert_worker.rs`
- `src/api/mod.rs`
- `src/bin/ledgerflow_indexer.rs`
- `src/indexer/api.rs`
- `src/services/indexer_client.rs`
- `src/services/mod.rs`
- `src/lib.rs`
- `src/web/mod.rs`
- `src/web/execution_intelligence.rs`
- `Dockerfile`
- `docker-compose.yml`
- `.env.example`
- `config/pilots/example-base.env`
- `migrations/0046_observed_address_intelligence_v1.{up,down}.sql`
- `migrations/0047_observation_notification_delivery.{up,down}.sql`
- `migrations/0048_stable_observation_briefs.{up,down}.sql`
- `migrations/0049_transfer_participant_action_attribution.{up,down}.sql`

Contract, tests, and operations:

- `schemas/observed-address-intelligence-v1.schema.json`
- `tests/observed_address_intelligence.rs`
- `tests/fixtures/observed_address_intelligence_v1.json`
- `scripts/verify_observed_address_intelligence.mjs`
- `docs/product/observed-address-intelligence-gap-analysis.md`
- `docs/product/observed-address-intelligence-results.md`
- `docs/operations/startup-recovery.md`
- `README.md`

Status: **PASS**.

## 3. Migrations

- `0046_observed_address_intelligence_v1`: object state, analysis runs,
  activity ledger, window summaries, behaviors, relationships, economics,
  changes, briefs, and workspace observation state.
- `0047_observation_notification_delivery`: stable signal/destination dedup,
  due time, claim recovery, retry metadata, and DLQ support on the existing
  delivery table.
- `0048_stable_observation_briefs`: keeps one stable current brief ID per
  object/brief type and removes legacy aggregate change signals that lack
  transaction evidence so workers can rebuild them correctly.
- `0049_transfer_participant_action_attribution`: prevents token-flow-only
  participants from inheriting the enclosing transaction's execution label;
  raw evidence is unchanged.

All migrations through `0049` were applied by SQLx to the Base and Robinhood
data-plane databases. Existing entity, Pilot, monitor, investigation, and
report tables were retained.

Status: **PASS**.

## 4. Intelligence Contract v1

The versioned response contains:

- root identity: `contract_version`, `chain_id`, `address`;
- object state: status, progress, timestamps, checkpoints, retry/error,
  monitoring state, actual/partial coverage;
- deterministic `activity_windows` for 24h, 7d, 30d and comparison windows;
- behaviors with confidence, counts, first/last observation, evidence hashes;
- relationships with evidence state, counts, timestamps, supporting hashes;
- economics with `verified | estimated | unknown`, verified counts, excluded
  counts/reasons, observed token flow, and coverage ratio;
- change signals, complete activity timeline, generated briefs, limitations.

JSON Schema validation and invalid-state/address rejection tests pass.

Status: **PASS**.

## 5. Real regression objects

Live checkpoint captured during bounded dual-chain backfill. These are
behavioral candidates selected from public chain evidence; no ownership or
“same team” attribution is made.

| Fixture | Chain | Address | Live state | Coverage checkpoint | Result |
|---|---:|---|---|---:|---|
| Base execution EOA candidate | 8453 | `0xaa3242…72968d` | complete profile/refreshing | 1,020 initial + 4 incremental | PASS |
| Base executor candidate | 8453 | `0x47c553…1f4e98` | backfilling | 2,012 / 10,000 at checkpoint | PARTIAL |
| Base transfer aggregation candidate | 8453 | `0x0629da…0fb056` | backfilling | 1,568 / 10,000 at checkpoint | PARTIAL |
| Robinhood HFT candidate A | 4663 | `0x0a0256…3ba015` | backfilling | 2,908 / 10,000 at checkpoint | PARTIAL |
| Robinhood HFT candidate B | 4663 | `0x6746e5…514e4c` | backfilling | 2,209 / 10,000 at checkpoint | PARTIAL |
| Base low-activity asset-transfer control | 8453 | `0x17eb9c…356271` | complete profile/refreshing | 9 / 9, full discovered coverage | PASS |

The previous low-activity fixture `0xe903…c1abf` was rejected after live
discovery proved it was a transfer-heavy address exceeding the 10,000-row cap.
Its derived run was stopped and preserved as a failed diagnostic record; no
chain or customer data was deleted. It is not counted as an acceptance fixture.

The requested “self-owned Searcher Worker” attribution is **NOT IMPLEMENTED**:
the repository does not contain safe ownership proof for these addresses.
Substituting a behavioral candidate prevents an unsupported ownership claim.

The user-supplied `0x0629…b056` no longer renders as “no data.” At the final
checkpoint it exposed 10,000 explicitly partial discovered rows and 1,568
analyzed inbound-transfer activities. An online audit found that the original
classifier had projected 291 swap and 5 candidate-arbitrage labels from the
enclosing transactions onto this recipient. Migration `0049` corrected all
1,568 analyzed rows to `asset_transfer`: the address had token-flow evidence
but was neither top-level sender nor target. It has zero sender-gas rows.
Unprocessed rows render `analysis_pending` instead of false zero values.

## 6. 24h / 7d / 30d examples

Live Base execution-EOA result for `0xaa3242…72968d`:

| Window | Transactions | Successful | Reverted | Sender gas (wei) | Protocols | Counterparties |
|---|---:|---:|---:|---:|---:|---:|
| 24h | 264 | 51 | 213 | 833,611,172,440,519 | 1 | 4 |
| 7d | 683 | 163 | 520 | 1,996,444,317,093,665 | 1 | 9 |
| 30d | 1,020 | 293 | 727 | 2,575,605,931,725,311 | 1 | 32 |

Live Base transfer-control result for `0x17eb9c…356271`:

| Window | Transactions | Successful | Reverted | Sender gas (wei) | Counterparties |
|---|---:|---:|---:|---:|---:|
| 24h | 3 | 3 | 0 | 230,322,681,466 | 3 |
| 7d | 7 | 7 | 0 | 448,183,625,880 | 4 |
| 30d | 9 | 9 | 0 | 657,481,479,772 | 4 |

Both results include raw token inflow/outflow evidence and last-active time.
The four capped HFT/executor objects expose their analyzed timeline while the
remaining rows continue in the background.

Status: **PASS** for the query contract and two completed live profiles;
**PARTIAL** for completion of every capped fixture.

## 7. Relationship extraction

Implemented evidence types include observed top-level call targets, candidate
funding sources, candidate settlement recipients, candidate executors for
execution-classified calls, co-occurring counterparties, routers, pools, and
protocol contracts. Every persisted relationship requires at least one
supporting transaction hash. “Owned by” and “same team” are prohibited.

`0xaa3242…72968d` produced 64 evidence-backed relationships. Examples:

- `0x47c553…1f4e98`: frequent call target and executor candidate, 715
  interactions, confidence 0.95/0.75, 100 returned supporting hashes;
- `0xa4caeb…849e01`: frequent call target and executor candidate, 243
  interactions, 100 returned supporting hashes;
- `0x498581…652b2b`: pool relationship, 181 interactions, confidence 0.98.

The transfer control produced eight relationships: frequent call target,
candidate funding sources, candidate settlement recipients, and co-occurring
addresses, each with one or more transaction hashes. The API and UI explicitly
render relationship confidence and evidence counts.

Status: **PASS** for persisted evidence constraints and completed live
objects; **PARTIAL** for the four unfinished high-frequency fixtures.

## 8. Economic verification and coverage

- Exact sender gas is recorded only when the observed address paid it.
- Token movement is aggregated as observed flow, never profit.
- Arbitrage surplus is `verified` only after receipt, trace, archive balance,
  swap-emitter, token-flow, and gas reconciliation for the explicit address
  scope.
- Unlike-asset gas conversion is not invented. Verified gross token surplus
  and native gas remain separate; deterministic total net result can remain
  unavailable with a reason.
- Every window reports verified count, exclusions/reasons, and coverage ratio.

Live economics for `0xaa3242…72968d` are correctly `unknown`, not zero-profit:

- 24h: 0 economically verified, 264 excluded, exact sender gas
  `833611172440519` wei;
- 7d: 0 verified, 683 excluded, exact sender gas `1996444317093665` wei;
- 30d: 0 verified, 1,020 excluded, exact sender gas `2575605931725311` wei;
- exclusions distinguish ordinary non-economic activity, insufficient
  distinct swap emitters, and absence of standard ERC-20 scope flows.

The transfer control reports token movement separately, 0% economic coverage,
and the reason “not an economically verifiable execution.” It does not turn
USDC receipts, forwarding, or inventory into profit.

Status: **PASS** for fail-closed semantics, exact gas, exclusion reasons, and
coverage reporting. A verified positive PnL result for all fixed live objects
remains **PARTIAL** because no completed fixture passed deterministic surplus
verification; no value is fabricated to satisfy the example.

## 9. Change detection

Implemented comparison signals: activity change, failure-rate change, sender
gas-cost change, strategy-distribution change, new counterparty, new executor,
new settlement recipient, and new protocol/pool/router interaction. A missing
comparison window renders “Baseline is still being built”; a 10,000-row cap
renders a partial-baseline limitation.

Live `0xaa3242…72968d` comparisons against its previous seven-day window:

- transaction activity: `+126.2%`;
- failure rate: `+7.6` percentage points;
- verified sender gas cost: `+244.7%`;
- new counterparty/funding/contract relationships with evidence hashes.

Each aggregate signal now carries up to 100 relevant transaction hashes.
Migration `0048` removed old evidence-empty aggregate rows; online validation
found zero remaining change signals without transaction evidence. When no
completed comparison exists, the contract/UI says the baseline is being built.

Status: **PASS** for completed objects and evidence traceability; **PARTIAL**
for unfinished fixtures. Verified-profit change is **NOT IMPLEMENTED** when
unlike-asset accounting prevents a comparable verified result.

## 10. Backfill performance

Runtime limits are 32 transactions per batch, eight concurrent analyses per
chain, and a 10,000-transaction cap. Each chain has one worker. Transfer-log
discovery also has a resumable block cursor and a 64-chunk per-iteration
budget, so a low-activity 30-day scan cannot monopolize the queue. During the
live run, each data plane sustained about one analyzed transaction/second while
the live indexers continued normally.

The abandoned-claim threshold is 120 seconds. A real Robinhood claim left by a
rolling restart was marked `recovered_stale_claim`, reclaimed automatically,
and advanced from 1,976 to 2,017 analyzed rows without duplicate activity
keys. The previous 900-second default was rejected as too slow for operations.

At the final checkpoint, observation workers used 21–27 MiB. RH indexer used
about 55 MiB; Base indexer used about 335 MiB. Observation-worker CPU was below
1% in the sampled interval. The initial 1,020-row execution-EOA profile
completed in roughly 24 minutes while both live indexers remained within 2–4
blocks of their RPC heads.

The transfer-only path was verified both in the large aggregation candidate and
the nine-row control. Each lightweight row contains tx, receipt, block, and
exact log evidence, and recipient-only rows do not inherit outer-transaction
gas.

Status: **PASS** for bounded operation and the lightweight evidence path;
**PARTIAL** for end-to-end 10,000-row completion time.

## 11. Incremental update delay

Ready objects are marked stale when the sealed head advances beyond the
configured two-block lag and are reprocessed from `last_processed_block + 1`.
The UI now keeps the last complete profile visible while the current status is
discovering/backfilling/classifying. Brief IDs remain stable across refreshes.

The control profile completed at `08:34:46Z`, was picked up for an incremental
run by `08:46:18Z`, and continued to expose its prior 24h/7d/30d profile during
refresh. Both live indexers stayed healthy at approximately 2–4 blocks of lag.
After multiple refreshes, the execution-EOA's three brief IDs remained
unchanged while `generated_at` and input digests advanced; duplicate activity,
relationship-without-evidence, change-without-evidence, and duplicate-brief
online checks all returned zero.
Its live weekly brief returned 4,205 bytes of Markdown and 4,244 bytes of HTML
with an inputs digest; no manual report creation was required.
This proves scheduling/recovery and stale-while-refresh, but the measured
end-to-end summary update exceeded the two-block product target because one
worker also serves bounded initial backfills.

Status: **PARTIAL** — correct incremental path and data visibility; the strict
“summary updated within two blocks” latency target is not yet met under the
current mixed initial-backfill workload.

## 12. Automated test results

- `cargo fmt --check`: PASS
- `cargo test --all-targets`: PASS (129 library tests plus every bin and
  integration test; pre-existing dead-code warnings remain)
- transfer-participant attribution test/migration: PASS; token-flow-only
  recipients do not inherit enclosing swap/arbitrage labels
- Intelligence Contract/migration/fixture tests: PASS, 4/4
- hidden automatic observation delivery-route test: PASS, 1/1
- soft live fixture verifier: PASS for reachability, chain isolation, state,
  progress, default observation, evidence constraints, completed profiles, and
  explicit partial coverage
- health/readiness: PASS, HTTP 200 for both APIs, both Web processes, and both
  indexers
- strict live fixture verifier: FAIL for four fixed 10,000-row objects still
  backfilling; two completed profiles pass while refreshing

Run strict acceptance with:

```bash
node scripts/verify_observed_address_intelligence.mjs
```

Use `LEDGERFLOW_OBSERVATION_ALLOW_IN_PROGRESS=1` only for an operational
checkpoint; it does not turn in-progress objects into acceptance passes.

## 13. Unfinished items

- **PARTIAL:** completed 24h/7d/30d, relationships, economics, changes, and
  briefs for all fixed high-frequency objects; live jobs remain in progress.
- **NOT IMPLEMENTED:** deterministic cross-asset total PnL without a proven
  conversion basis.
- **NOT IMPLEMENTED:** complete arbitrary top-level historical enumeration when
  the requested block range was never indexed and the RPC exposes neither an
  address-history method nor an address trace filter.
- **NOT IMPLEMENTED:** ownership proof for “self-owned” fixture labels.
- **PARTIAL:** Telegram delivery code/retry/dedup/rate-limit/DLQ is running, but
  real delivery cannot pass until an operator supplies the bot credential.
- **NOT IMPLEMENTED:** Email and Webhook delivery.

## 14. Known limitations

1. ERC-20 Transfer history is address-scoped and can be scanned through RPC.
   Top-level endpoint history is complete only for the explicitly reported
   indexed/cached coverage; LedgerFlow does not silently run a full-chain replay.
2. The newest 10,000 transaction cap is explicit. Further continuation is
   resumable infrastructure work, not silently claimed coverage.
3. Trace/archive availability can reduce economic verification coverage.
4. Protocol names come only from registry/decoder evidence; unknown contracts
   stay unknown.
5. A relationship is an observed or candidate interaction, never proof of
   ownership.
6. The initial high-frequency backfill is intentionally subordinate to live
   block indexing, so it completes over hours rather than saturating the node.

## Acceptance checklist

### Data

- PASS — Observe automatically queues historical analysis.
- PASS — deterministic activity exists independently of Material Events.
- PASS — 24h/7d/30d query contract and aggregation implementation.
- PARTIAL — complete top-level range coverage; indexed/cached coverage is
  explicit, but arbitrary missing history cannot be enumerated by standard RPC.
- PASS — status, gas attribution, and standard Token flow derivation.
- PASS — persisted relationships require supporting transactions.
- PASS — economics distinguish verified/estimated/unknown and expose coverage.

### Product and UI

- PASS — one Observe action creates the object, queues analysis, enables
  watching, and creates a hidden internal delivery route.
- PASS — progress replaces misleading zero/Unknown cards while analysis runs.
- PASS — the completed object page answers activity, economics, behavior,
  relationships, changes, and exposes discovered activity with bounded paging.
- PASS — Workspace Overview renders state per object rather than global zeros.
- PASS — notification channels live under Settings and briefs are automatic.

### Stability

- PASS — transaction/object primary keys make replay idempotent.
- PASS — stale claims and interrupted `analyzing` rows recover to retry; a
  real Robinhood restart recovery continued from 1,976 to 2,017 rows.
- PASS — Base and Robinhood workers are chain-isolated and resource-bounded.
- PARTIAL — incremental scheduling, recovery, and stale-while-refresh are live;
  strict two-block summary-update latency is not met under initial-backfill load.
- PARTIAL — real Telegram send blocked by missing server credential.

Overall live acceptance: **PARTIAL**. The product/data path is implemented and
running, but high-frequency historical jobs and their strict real-object output
must finish before the full v1 acceptance can be marked PASS.
