# Pilot startup and recovery

## Preconditions

Use a Base mainnet RPC and verify it before starting. The response must be `0x2105` (8453), not Robinhood Chain `0x1237`:

```bash
curl -sS -X POST "$LEDGERFLOW_RPC_URL" \
  -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]}'
```

Copy `.env.example` to `.env`, set the Base RPC, a public `LEDGERFLOW_WEB_BASE_URL`, Postgres credentials, Pilot YAML path, and Telegram bot token referenced by the YAML. Do not put a bot token, private RPC URL, customer data, or private key in a committed YAML file.
For an isolated Base stack beside the existing Robinhood stack, start from `config/pilots/example-base.env`; its example host ports do not collide. Override the public Base RPC from the shell or secret manager rather than committing a private URL.

Validate configuration without touching a database:

```bash
cargo run --bin ledgerflow_pilot_config -- config/pilots/example-base-project.yaml --check
```

Start the Base stack and Pilot workers. SQLx applies every database change through migrations:

```bash
docker compose --profile pilot up -d --build \
  db clickhouse rpc-gateway app web indexer indexer-lazy address-worker observation-worker \
  pilot-event-worker pilot-alert-worker pilot-report-worker
```

Import the versioned Base token/protocol registry before accepting traffic. This is idempotent and stores protocol identity in PostgreSQL rather than application constants:

```bash
docker compose --profile bootstrap run --rm intelligence-import-base
```

To run the collision-free Base example as a separate Compose project:

```bash
LEDGERFLOW_IMAGE=ledgerflow:phase1-base \
LEDGERFLOW_RPC_GATEWAY_UPSTREAM_URL=https://your-approved-base-rpc.example \
LEDGERFLOW_ANALYSIS_COMPOSE_RPC_URL=https://your-approved-base-rpc.example \
LEDGERFLOW_PILOT_COMPOSE_RPC_URL=https://your-approved-base-rpc.example \
docker compose --env-file config/pilots/example-base.env \
  -p ledgerflow-base-pilot --profile pilot up -d --build \
  db clickhouse rpc-gateway app web indexer indexer-lazy address-worker \
  pilot-event-worker pilot-alert-worker pilot-report-worker
```

Do not add the broad `address-producer` to the Base command by default. It is a
cold-start discovery producer that materializes every canonical transaction and
known indexed-event address into PostgreSQL; on Base that is materially more
work than an explicit observation needs. An Observe request is priority 5 and
the Base `address-worker` is limited to one concurrent job with a 50,000-block
recent window and at most 32 analyzed transactions. It locates direct
transactions in the existing live index and ERC-20 Transfer activity through
bounded, sequential `eth_getLogs` topic queries. Robinhood may continue to run
the producer for chain-wide seed discovery in its independent data plane.

Do not start `backfill-worker` for the default Base data plane either. That
worker processes historical cluster scopes and can continually enqueue work;
it is not needed for live block completeness or the bounded Observe path.
Start it only for an explicitly approved, bounded historical recovery job, and
stop it again after that job reaches its checkpoint. `indexer-lazy` may stay
running: it rejects ranges larger than 50,000 blocks and is idle when no bounded
gap request exists.

`observation-worker` is the targeted historical path for Observed Objects. Run
exactly one instance per chain data plane. It discovers existing indexed
top-level references plus address-scoped ERC-20 Transfer logs, caps the initial
analysis at 10,000 newest transactions, and persists per-transaction state so a
restart resumes instead of recounting. Transfer-only recipient activity uses
transaction/receipt/block/log evidence; execution-oriented sender activity also
uses trace-backed analysis. This does not schedule a whole-range indexer replay.

Run `observation-alert-worker` only in the control-plane Compose project. It
reads hidden automatic observation routes, queues change-signal deliveries with
deduplication, and applies retry, rate-limit, and dead-letter handling. Telegram
delivery remains unavailable until `LEDGERFLOW_TELEGRAM_BOT_TOKEN` is present;
the worker records that configuration error rather than reporting delivery.

The HTTP endpoint is sufficient for the current polling/indexing path. Keep any WebSocket endpoint in runtime secrets for an optional adapter; no RPC endpoint is committed to project configuration.

Check the private API:

```bash
curl -fsS http://127.0.0.1:3000/health
curl -fsS http://127.0.0.1:3000/ready
curl -fsS http://127.0.0.1:3000/metrics
```

`/ready` performs a live `eth_chainId` check and returns 503 unless both configured and RPC chain IDs are 8453. With the collision-free example, use API port 3200 instead.

The web process listens on `127.0.0.1:3001`; nginx should proxy the public hostname to that port. `LEDGERFLOW_WEB_BASE_URL` must be the public origin so Telegram links do not point at a recipient's localhost. The web process itself calls `http://app:3000` inside Compose; nginx should not expose the analysis API.

## Add or update a Pilot

1. Copy the example YAML and choose a stable `project_id` using letters, numbers, hyphens or underscores.
2. Add 10–30 Base addresses, roles and labels. Set `monitoring_start_block` to the first accepted Base block and `continuous_monitoring: true`; configure the eight rule types, raw/USD thresholds, severity, include/exclude filters and Telegram destination.
3. Put the token in an environment variable and set `secret_ref` in YAML to that variable name.
4. Run the `--check` command, then import with `ledgerflow_pilot_config`. The event worker also imports `LEDGERFLOW_PILOT_CONFIG` on startup.
5. Restart only the Pilot event worker after changing the selected YAML. The importer updates the existing indexer/backfill watch scope, with automatic neighbor discovery disabled. A changed ruleset digest reprocesses relevant analyses without duplicate events.

No Rust change or migration is required for a new customer.

## Recovery

- Transaction discovery reuses `cluster_tracking_state`, `cluster_backfill_runs` and `cluster_discovered_txs`. It range-splits busy addresses, deduplicates hashes and advances `last_processed_block` only through the existing backfill worker.
- Event recovery is keyed by project, chain, transaction source digest and ruleset digest in `pilot_analysis_runs`. Restarting resumes unprocessed or changed inputs.
- Event IDs and outbox rows are deterministic/unique, so replay does not duplicate an event or destination delivery.
- Alert claims stuck in `sending` for five minutes return to `retry`. Repeated failures back off and eventually become `dead_letter`; inspect `last_error` before requeueing operationally.
- The completed-week report worker upserts by project/week and can safely rerun.
- After a restore, run migrations, start API, then event worker, report worker, and alert worker in that order. Verify readiness, failed checkpoint count, duplicate dedup count, retries and DLQ before enabling customer traffic.

For an approved non-production database with the PostgreSQL client installed:

```bash
LEDGERFLOW_DATABASE_URL=postgres://... \
LEDGERFLOW_PILOT_CONFIG=config/pilots/example-base-project.yaml \
LEDGERFLOW_PILOT_PROJECT_ID=example-base-project \
./scripts/verify_pilot_runtime.sh
```
