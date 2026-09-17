# LedgerFlow Economic Monitoring Pilot v0.1 — handoff

## Outcome

The current workspace implements a Base-only first-Pilot path without replacing or stopping the existing indexer, analysis pipeline, cluster backfill, recognizers, or Searcher. The exact acceptance record is in `docs/acceptance/phase1-results.md`; the original and updated gap analysis is in `docs/product/phase1-gap-analysis.md`.

## Phase 1 file set

- Runtime/config: `Cargo.toml`, `Cargo.lock`, `.env.example`, `Dockerfile`, `docker-compose.yml`, `README.md`
- API/runtime wiring: `src/main.rs`, `src/lib.rs`, `src/api/mod.rs`
- Pilot core: `src/pilot/mod.rs`, `classifier.rs`, `repository.rs`, `alerts.rs`, `report.rs`
- Pilot binaries: `ledgerflow_pilot_config`, `ledgerflow_pilot_event_worker`, `ledgerflow_pilot_alert_worker`, `ledgerflow_pilot_report`, `ledgerflow_pilot_report_worker`
- Deterministic recognizer correction: `src/recognition/builtins.rs`
- Migrations: `0027_pilot_monitoring`, `0028_pilot_runtime`, `0029_pilot_ruleset_reprocessing` up/down pairs
- Schema/config: `schemas/evidence-pack-v1.schema.json`, `config/pilots/example-base-project.yaml`, `config/pilots/example-base.env`, `config/pilots/demo-base-public.yaml`
- Regression/operations: `tests/pilot_regression_dataset.rs`, `tests/protocol_precision.rs`, `tests/arbitrage_profit_regression.rs`, the Base/protocol/profit JSONL fixtures, their generator/report scripts, `scripts/verify_pilot_runtime.sh`, and `scripts/soak_test.sh`
- Product/acceptance/operations/Demo documents under `docs/product`, `docs/acceptance`, `docs/operations`, and `demo/base-public`

The worktree already contained other authentication, metadata, intelligence and web changes; they are not presented here as newly authored Phase 1 scope.

## Architecture

See `docs/product/phase1-architecture.md`. In short: Base RPC → existing indexer → existing scoped backfill/checkpoint path → existing deterministic transaction analysis → Pilot classifier/schema validation → event store/outbox → API, Telegram and weekly reports.

## Start

Verify the RPC returns Base `0x2105`, configure `.env`, validate the YAML, then run. For a separate stack beside Robinhood, use `config/pilots/example-base.env` and Compose project name `ledgerflow-base-pilot` as documented in startup/recovery.

```bash
docker compose --profile pilot up -d --build
curl -fsS http://127.0.0.1:3000/ready
curl -fsS http://127.0.0.1:3000/metrics
```

Web is `127.0.0.1:3001`; the analysis API is `127.0.0.1:3000`. Set `LEDGERFLOW_WEB_BASE_URL` to the nginx public origin. Full startup and recovery instructions are in `docs/operations/startup-recovery.md`.

## Configure a new Pilot

Copy the example YAML, choose a stable ID, add 10–30 Base entities/roles, set the first accepted `monitoring_start_block`, enable continuous monitoring, configure thresholds/severity/filters, add Telegram chat plus an environment-variable `secret_ref`, validate, and import. No code change is required. Config import stores the project/rules/entities/destination in PostgreSQL and creates a stable watcher in the existing cluster tracking system.

## Demo

There is no hosted URL. Local/static artifacts are in `demo/base-public`: 27 events in the shown week and 39 in the complete Evidence Pack export, including the named arbitrage, liquidation and complex-flow cases. The arbitrage profit is intentionally null because complete accounting evidence was unavailable.

## Regression and performance

- 100 unique Base transactions, 100 persisted analyses, 100 successful Pilot checkpoints, 0 failed checkpoints.
- 189 exact 32-byte Transfer-log raw values plus exact native value, execution gas, Base L1 fee, total gas and receipt status.
- 39 deterministic events across 36 transactions; replay remained 39 with 0 duplicate dedup keys.
- Target branch passed all 121 Rust tests: 73 library, 3 binary and 45 integration tests.
- Independent protocol evidence passed 95 reviewed rows: 86 positives across all 9 required protocol families and 9 negative controls, with precision/recall 1.0 in the direct-deployment scope. Live API replay matched 9/9 representatives and rejected an ordinary token-call negative.
- Deterministic archive accounting passed 6 Base arbitrage cases: 3 exact positive profits and 3 safe-null incomplete cases. Every row proves execution gas plus Base L1 fee.
- An isolated target-branch database applied migrations 27/28/29 and imported 1 Base project, 11 entities, 8 rules and an active 11-address watcher; isolated health/readiness/metrics checks passed.
- Local 100-request p95: health 0.901 ms, metrics 1.416 ms and 39-event list 28.556 ms. Readiness now makes a live RPC chain-ID call and is intentionally upstream-dependent, so the prior DB-only timing is excluded. These are isolated observations, not a capacity guarantee.
- Compose release image build and in-image Pilot binary/config smoke test passed.

## Seven-day soak

```bash
SOAK_DURATION_SECONDS=604800 SOAK_INTERVAL_SECONDS=60 \
  LEDGERFLOW_URL=http://127.0.0.1:3000 ./scripts/soak_test.sh
```

Use `docs/operations/soak-test.md` for pass criteria. A two-sample script smoke test passed; seven elapsed days have not run.

The formal final-image run started `2026-08-12T00:16:17Z` and can finish no earlier than `2026-08-19T00:16:17Z`. Its first sample was health/readiness 200/200 with zero monitor errors, failed analyses, retries and dead letters; it remains `NOT IMPLEMENTED` until all 10,080 samples and operational checks pass.

## Open items and known risks

- Real Telegram delivery is not validated without an approved bot token and Pilot group.
- Protocol precision currently covers reviewed direct calls to known deployments; proxy upgrades, internal-only paths and unseen contracts require continued dataset refresh and remain fail-closed.
- Numeric profit currently supports complete single-positive-settlement-asset accounting scopes. Multi-asset, fee-on-transfer or unreconciled native cases remain null with limitations.
- Seven-day soak is not complete.
- Missing trace or historical USD price inputs reduce confidence and are surfaced as limitations.
- `monitoring_start_block` is operationally significant: set it deliberately before first import. Later historical expansion should use an explicit approved backfill rather than silently resetting the persisted checkpoint.
- The host-local `127.0.0.1:18555` observed during development was Robinhood chain 4663, not Base. Do not point the Base Pilot at it unless that adapter is intentionally selected outside Phase 1.
- `/ready` now rejects both a non-8453 configured chain and a live RPC whose `eth_chainId` is not 8453; this was verified against Base (200) and the local Robinhood RPC (503, chain 4663).

## Acceptance checklist

- Base-only config and continuous watcher: PASS
- Eight event types and schema-validated deterministic Evidence Packs: PASS
- Durable Telegram mechanics: PASS; real customer-group delivery: NOT IMPLEMENTED
- Markdown/HTML weekly automation: PASS
- 100-transaction coverage, exact raw facts, replay/dedup/recovery: PASS
- Configured and independent direct-deployment protocol precision: PASS
- Numeric arbitrage P&L reliability for complete supported scopes: PASS
- Runtime health/readiness/metrics, Compose and migrations: PASS
- Public artifact Demo: PASS
- Seven-day soak: NOT IMPLEMENTED

See `docs/acceptance/phase1-results.md` for requirement-by-requirement evidence.
The two remaining external/elapsed-time gates and their exact exit criteria are in `docs/product/phase1-remaining-plan.md`.
