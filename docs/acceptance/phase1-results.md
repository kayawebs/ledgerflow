# LedgerFlow Economic Monitoring Pilot v0.1 — Phase 1 results

Run date: 2026-08-11 through 2026-08-12 UTC. `PASS` means an executable test or isolated runtime check succeeded. `FAIL` means the acceptance target was exercised but not met. `NOT IMPLEMENTED` means the required external or elapsed-time validation has not happened. No item below is promoted based only on code presence.

## Product and data plane

| Requirement | Status | Evidence |
|---|---|---|
| Base production scope (`chain_id=8453`) | PASS | YAML validation, DB constraint, Base RPC fixture and `/ready` all assert 8453 |
| Robinhood does not block Base | PASS | Pilot config rejects non-8453; existing Robinhood adapter/runtime was not changed or stopped |
| Project/entity/rule/filter/threshold configuration without code edits | PASS | `config/pilots/example-base-project.yaml`, importer and `pilot_*` DB tables |
| 10–30 monitored entities | PASS | Config validator enforces 10–30; supplied examples use 11 and 19 |
| Continuous monitored-transaction acquisition | PASS | Config import creates a scoped existing-cluster watcher at `monitoring_start_block`; the existing indexer/backfill worker discovers, deduplicates and analyzes later transactions |
| Eight event types implemented | PASS | Deterministic classifier plus `all_eight_event_types_emit_schema_valid_packs` |
| Facts only from transaction/receipt/log/trace/flows/accounting | PASS | Classifier consumes persisted `TransactionAnalysis`; language text does not add facts |
| Unknown/null and limitations when evidence is absent | PASS | Trace, USD valuation and profit limitations are emitted; Demo arbitrage profit is null |

## Evidence, alerts and reports

| Requirement | Status | Evidence |
|---|---|---|
| Versioned Evidence Pack JSON Schema validation | PASS | `schemas/evidence-pack-v1.schema.json`; all persisted packs validate before insert |
| Deterministic Evidence Pack | PASS | Stable v5 event IDs/input digest unit test and 0 invalid persisted pack shapes |
| Telegram required fields | PASS | Formatter field test covers Project through Confidence |
| Telegram deduplication | PASS | Stable dedup test; DB unique constraints; isolated replay produced 0 duplicates |
| Telegram retry/rate limit/grouping/status/DLQ | PASS | Durable outbox worker, bounded backoff, local per-destination limiter/grouping, stale claim recovery and mock HTTP 429 test |
| Real Telegram delivery to a Pilot group | NOT IMPLEMENTED | No real bot token/chat destination was supplied; no external message was sent |
| Weekly Markdown and HTML reports | PASS | Both formats include every required section and transaction links; completed-UTC-week worker persists and writes them |
| Shareable explanation/Evidence API | PASS | `/pilot/evidence-packs/{event_id}`, project event list and latest weekly report routes |

## Regression and recovery

| Requirement | Status | Evidence |
|---|---|---|
| Fixed dataset of at least 100 Base transactions | PASS | Exactly 100 unique RPC-verified rows in `tests/fixtures/base_regression_dataset.jsonl` |
| Required protocol/case coverage | PASS | V2/V3/V4, Aerodrome, PancakeSwap, Balancer, Morpho, Aave, Moonwell, transfers, reverted, arbitrage, liquidation and complex flow are present |
| No missing monitored transactions | PASS | Isolated run persisted 100/100 fixture transactions and 100 successful Pilot checkpoints |
| No duplicate events | PASS | 39 events over 36 transactions; replay preserved 39 and duplicate query returned 0 |
| Exact amount/gas/status | PASS | Fixture v3 stores RPC native value, status, execution gas, Base receipt `l1Fee`, total gas and every exact 32-byte ERC-20 Transfer value; tests recompute execution plus L1 cost for all 100 |
| Classification precision report | PASS | The configured-path check remains 34/34; the independent direct-deployment corpus is 95 reviewed rows with 86 true positives, 9 negative controls, 0 false positives and 0 false negatives |
| Independent recognizer precision | PASS | `tests/protocol_precision.rs` executes the actual DB-evidence recognizer across Uniswap V2/V3/V4, Aerodrome, PancakeSwap, Balancer, Morpho, Aave and Moonwell; `base-protocol-precision.json` records precision/recall 1.0 in the explicitly scoped direct-call corpus |
| Numeric arbitrage profit reliability | PASS | `base-arbitrage-profit-reliability.json` records six archive Base cases: 3 exact positive token deltas and 3 incomplete/native-mismatch cases that fail closed to null; exact execution gas plus L1 fee is asserted in every row |
| Restart/checkpoint recovery | PASS | Ruleset/source digests persisted; second worker run made no new events; `scripts/verify_pilot_runtime.sh` automates the check |
| Telegram dedup/retry automation | PASS | Unit/mock transport tests plus DB constraints |
| Weekly report automation | PASS | Report unit tests and `ledgerflow_pilot_report_worker` completed-week scheduler |

## Runtime and operations

| Requirement | Status | Evidence |
|---|---|---|
| Compose, env example and migrations | PASS | Compose validates; `.env.example`; migrations `0027`–`0029` |
| Health/readiness/metrics | PASS | Isolated live checks returned health 200; Base RPC readiness 200; Robinhood RPC readiness 503 with actual chain 4663; metrics exposed project/monitor/error/event/checkpoint/outbox counters |
| Backup/recovery documentation | PASS | `docs/operations/backup-recovery.md` |
| Startup/recovery documentation | PASS | `docs/operations/startup-recovery.md` |
| Public Demo with 20+ real events and required cases | PASS | 27 weekly / 39 total events plus named arbitrage, liquidation and complex-flow hashes in `demo/base-public` |
| Seven-day soak elapsed acceptance | NOT IMPLEMENTED | Final-image run started `2026-08-12T00:16:17Z`, first sample was health/readiness 200/200 with zero monitor errors, analysis failures, retries or DLQ; completion cannot be judged before `2026-08-19T00:16:17Z` and 10,080 samples |

## Measured isolated-host result

- Target worktree `codex/chainactors-intelligence-v2`: `cargo test --all-targets` passed all 121 tests (73 library, 3 binary and 45 integration tests); `cargo check --all-targets` and formatting passed.
- Isolated target-branch PostgreSQL: migrations 27/28/29 applied; config import persisted 1 Base project, 11 entities, 8 rules and an active 11-address watcher.
- Isolated target-branch API: health 200, readiness 200/Postgres ok, and Pilot metrics reported chain 8453, one project and one active monitor.
- Fixture analysis: 100/100 persisted; Pilot runs 100 successful, 0 failed.
- Fixture v3: 100 unique hashes, 189 exact 32-byte Transfer-log raw values and exact Base L1 fees.
- Events: 39 total across 36 transactions; 0 duplicate dedup keys.
- Event mix: 32 `large_asset_flow`, 3 `high_cost_failure`, 2 `liquidation`, 1 `dex_arbitrage`, 1 `repeated_failed_attempts`.
- 100-request localhost API samples: health p95 0.901 ms, metrics p95 1.416 ms and 39-event-list p95 28.556 ms. Readiness now includes a live RPC chain-ID call, so its latency is upstream-dependent and the earlier DB-only sample is excluded. These are functional observations, not production capacity claims.
- Two versioned Base intelligence datasets imported 15 reviewed address rows and 11 active protocol deployments. Live API reanalysis matched all 9 required protocol families (9/9); an ordinary USDC call produced no protocol evidence or label.
- Deterministic profit runtime evidence includes `0xe529…f915`: USDC raw profit `3896`, execution gas `1359117744778`, L1 fee `1433298939`, total gas `1360551043717`, with archive balances, receipt logs and trace reconciled.
- Formal soak uses the final image and the supplied Base RPC at runtime. It started `2026-08-12T00:16:17Z`; the API and event worker were healthy with zero restarts at the baseline. The status remains `NOT IMPLEMENTED` until elapsed acceptance completes.
- Target-branch `ledgerflow:phase1-base` release image build, intelligence import, Pilot config import and runtime checks passed without restarting the production Searcher stack.
