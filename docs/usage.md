# Usage Guide

This document explains how to run LedgerFlow locally and how to use the starter API in practice.

If you only want the standalone top-level address indexer, read [docs/indexer.md](indexer.md).

The main operator workflow is now:

1. open the web product shell
2. register by email verification, or sign in with a password or email code
3. create a monitor or address cluster for the relevant bot and contract set
4. let the seed workers analyze bounded evidence transactions while the live indexer keeps following the head
5. review verified events, explicit evidence limitations, entity relationships, and single-tx detail from the dashboard

## What this starter is for

LedgerFlow is not a generic explorer. It is a backend analysis core that:

- fetches raw EVM evidence for a transaction
- normalizes the execution into stable facts
- derives flows
- runs recognizers
- builds a replayable Evidence Pack

The first version is intentionally narrow. If a transaction is only partially understood, the API returns partial results, unresolved items, and validation messages instead of inventing semantics.

## Prerequisites

- Rust 1.87+ or current stable toolchain
- Docker and Docker Compose if you want the quickest local setup
- PostgreSQL 15+ if you want to run the app outside Docker
- network access to the local Robinhood Nitro JSON-RPC endpoint

## Configuration

LedgerFlow reads configuration from environment variables.

| Variable | Purpose | Example |
| --- | --- | --- |
| `LEDGERFLOW_BIND_ADDR` | HTTP bind address for Axum | `127.0.0.1:3000` for host runs, `0.0.0.0:8080` in Docker |
| `LEDGERFLOW_DATABASE_URL` | PostgreSQL connection string | `postgres://postgres:postgres@127.0.0.1:5532/ledgerflow` for host runs |
| `LEDGERFLOW_RPC_URL` | Robinhood Chain RPC endpoint | Local Nitro for host runs. Docker Compose routes ordinary workers through `http://rpc-gateway:8545` by default. |
| `LEDGERFLOW_INDEXER_API_BASE_URL` | Indexer API base URL used by analysis-side cluster backfill | `http://127.0.0.1:8090` for host runs, `http://indexer:8090` in Docker |
| `LEDGERFLOW_INDEXER_API_BEARER_TOKEN` | Optional Bearer token sent to the indexer API | unset |
| `LEDGERFLOW_DEFAULT_CHAIN_ID` | Default chain for callers that omit it | `4663` |
| `LEDGERFLOW_TRACE_ENABLED` | Enables trace fetch attempts | `false` |
| `LEDGERFLOW_CLUSTER_BACKFILL_QUERY_LIMIT` | Per-request limit used when analysis reads tx refs from the indexer | `10000` |
| `LEDGERFLOW_CLUSTER_AUTO_BACKFILL_ON_CREATE` | Start a background historical backfill immediately after cluster creation | `true` |
| `LEDGERFLOW_CLUSTER_BACKFILL_WORKER_POLL_INTERVAL_MS` | Poll interval for the standalone backfill worker | `120000` |
| `LEDGERFLOW_ENABLED_RECOGNIZERS` | Comma-separated recognizer ids to run by default | `selector_presence,erc20_transfer_flow,...` |
| `LEDGERFLOW_PLUGINS` | Comma-separated plugin packs to load | `sample_builtin_pack` |
| `LEDGERFLOW_WEB_BIND_ADDR` | Web process bind address | `127.0.0.1:3001` for host runs, `0.0.0.0:3000` in Docker |
| `LEDGERFLOW_WEB_BASE_URL` | Public base URL used in verification email links | `http://127.0.0.1:3001` for host runs |
| `LEDGERFLOW_API_BASE_URL` | Upstream analysis API used by the web process | `http://127.0.0.1:3000` for host runs, `http://app:3000` in Docker |
| `LEDGERFLOW_SESSION_COOKIE_NAME` | Session cookie name for the web product | `ledgerflow_session` |
| `LEDGERFLOW_EMAIL_DELIVERY_MODE` | Email delivery mode: `auto`, `log`, `postmark`, or `resend` | `auto` |
| `LEDGERFLOW_EMAIL_FROM` | Sender address when using real email delivery | `noreply@yourdomain.xyz` |
| `LEDGERFLOW_POSTMARK_SERVER_TOKEN` | Optional Postmark server token for verification emails | unset |
| `LEDGERFLOW_POSTMARK_MESSAGE_STREAM` | Optional Postmark message stream | `outbound` |
| `LEDGERFLOW_RESEND_API_KEY` | Optional Resend API key for verification emails | unset |
| `LEDGERFLOW_VERIFICATION_TTL_MINUTES` | Verification code lifetime | `15` |
| `LEDGERFLOW_SESSION_TTL_DAYS` | Authenticated session lifetime | `30` |
| `LEDGERFLOW_REGISTRY_SYNC_POLL_INTERVAL_MS` | How often the registry worker checks PostgreSQL for due endpoints | `60000` |
| `LEDGERFLOW_REGISTRY_SYNC_RPC_CONCURRENCY` | Concurrent on-chain validations for official deployments | `4` |
| `LEDGERFLOW_REGISTRY_SYNC_MAX_ENDPOINTS_PER_TICK` | Maximum due database endpoints claimed per tick | `32` |
| `LEDGERFLOW_REGISTRY_SYNC_RUN_ONCE` | Exit after one due-source pass; useful for manual verification | `false` |
| `LEDGERFLOW_DISCOVERY_BATCH_SIZE` | Canonical blocks projected per factory/ERC-8056 discovery pass | `500` |
| `LEDGERFLOW_DISCOVERY_LOOKBACK_BLOCKS` | Reorg reconciliation depth for discovery datasets | `128` |
| `LEDGERFLOW_ADDRESS_PRODUCER_BATCH_SIZE` | Canonical blocks aggregated into each address evidence window | `250` |
| `LEDGERFLOW_ADDRESS_PRODUCER_REORG_LOOKBACK_BLOCKS` | Address observation rewind depth after a canonical hash change | `128` |
| `LEDGERFLOW_ADDRESS_WORKER_CONCURRENCY` | Concurrent address jobs sharing the throttled RPC gateway | `4` |
| `LEDGERFLOW_ECONOMIC_VERIFIER_BATCH_SIZE` | Maximum jobs claimed sequentially per verifier iteration | `2` |
| `LEDGERFLOW_ECONOMIC_VERIFIER_MAX_NEW_JOBS_PER_PASS` | Maximum new seed evidence jobs scheduled per pass | `10` |
| `LEDGERFLOW_ECONOMIC_VERIFIER_MAX_PENDING_JOBS` | Queue/running/retry capacity guard | `50` |
| `LEDGERFLOW_ECONOMIC_VERIFIER_MAX_CANDIDATES_PER_SEED` | Representative transactions considered per MEV seed | `2` |
| `LEDGERFLOW_ECONOMIC_VERIFIER_MAX_ATTEMPTS` | Bounded attempts before a failed job stops being claimable | `3` |
| `LEDGERFLOW_ECONOMIC_VERIFIER_STALE_CLAIM_SECONDS` | Restart recovery threshold for abandoned running claims | `120` |

If `LEDGERFLOW_EMAIL_DELIVERY_MODE=auto`, the web process prefers Postmark when both `LEDGERFLOW_EMAIL_FROM` and `LEDGERFLOW_POSTMARK_SERVER_TOKEN` are configured; otherwise it falls back to Resend when `LEDGERFLOW_EMAIL_FROM` and `LEDGERFLOW_RESEND_API_KEY` are configured. If neither provider is configured, it falls back to log-only mode; `/health` exposes that operational status while the account UI stays provider-neutral. You can also use unprefixed aliases `EMAIL_FROM`, `POSTMARK_SERVER_TOKEN`, and `RESEND_API_KEY`.

The checked-in `.env.example` is the easiest starting point. Copy it to `.env` before using Docker Compose, because the compose stack reads `.env` as the runtime config file.


### RPC gateway variables

Docker Compose includes an `rpc-gateway` service between LedgerFlow processes and the local Robinhood Nitro RPC. It smooths bursts from the indexer, analysis API, and workers while allowing sustained indexing within the node's capacity.

| Variable | Purpose | Default |
| --- | --- | --- |
| `LEDGERFLOW_RPC_GATEWAY_UPSTREAM_URL` | Upstream Robinhood RPC endpoint reached by the gateway | `http://host.docker.internal:18545` |
| `LEDGERFLOW_RPC_GATEWAY_HOST_PORT` | Optional host port for inspecting the gateway | `18555` |
| `LEDGERFLOW_RPC_GATEWAY_MAX_IN_FLIGHT` | Maximum simultaneous upstream RPC requests | `8` |
| `LEDGERFLOW_RPC_GATEWAY_RPS` | Token refill rate for upstream requests | `20` |
| `LEDGERFLOW_RPC_GATEWAY_BURST` | Token bucket burst capacity | `20` |
| `LEDGERFLOW_RPC_GATEWAY_UPSTREAM_TIMEOUT_MS` | Upstream RPC request timeout | `30000` |
| `LEDGERFLOW_COMPOSE_RPC_URL` | Optional override for analysis/worker containers | `http://rpc-gateway:8545` |
| `LEDGERFLOW_INDEXER_COMPOSE_RPC_URL` | Optional override for the indexer container | `http://rpc-gateway:8545` |

The gateway weights expensive JSON-RPC methods more heavily: traces, block receipts, full transaction blocks, logs, and `eth_call` consume more tokens than cheap methods such as `eth_blockNumber`. This limits sudden fan-out without disabling long-running indexing.

### Standalone indexer variables

If you are running only `ledgerflow indexer`, the main variables are:

| Variable | Purpose | Example |
| --- | --- | --- |
| `LEDGERFLOW_INDEXER_BIND_ADDR` | Indexer API bind address | `0.0.0.0:8090` |
| `LEDGERFLOW_INDEXER_RPC_URL` | RPC endpoint used by this worker | local Nitro directly for `live`; throttled gateway/archive endpoint for lazy/history |
| `LEDGERFLOW_INDEXER_CLICKHOUSE_URL` | ClickHouse HTTP endpoint | `http://127.0.0.1:8123` |
| `LEDGERFLOW_INDEXER_CLICKHOUSE_DATABASE` | ClickHouse database name | `ledgerflow_indexer` |
| `LEDGERFLOW_INDEXER_CLICKHOUSE_USER` | ClickHouse username | `ledgerflow` in Docker |
| `LEDGERFLOW_INDEXER_CLICKHOUSE_PASSWORD` | ClickHouse password | `ledgerflow` in Docker |
| `LEDGERFLOW_INDEXER_START_BLOCK` | Default lazy-query lower bound, or continuous-scan start when lazy mode is disabled | `0` |
| `LEDGERFLOW_INDEXER_END_BLOCK` | Optional bounded stop block | unset |
| `LEDGERFLOW_INDEXER_BATCH_SIZE` | Blocks claimed and committed per indexing batch | `200` |
| `LEDGERFLOW_INDEXER_RPC_CONCURRENCY` | Parallel block fetch count inside a batch | `4` |
| `LEDGERFLOW_INDEXER_RPC_BATCH_REQUEST_SIZE` | Basic/lazy block requests grouped per JSON-RPC call; keep `1` when the node does not support JSON-RPC batches | `1` for local Robinhood Nitro |
| `LEDGERFLOW_INDEXER_RPC_REQUEST_DELAY_MS` | Per-request delay before a block fetch task runs | `25` |
| `LEDGERFLOW_INDEXER_RPC_REQUEST_TIMEOUT_MS` | Hard timeout for one RPC HTTP request | `30000` |
| `LEDGERFLOW_INDEXER_RPC_RETRY_BASE_MS` | Base retry delay after rate-limit responses | `1000` |
| `LEDGERFLOW_INDEXER_RPC_RETRY_MAX_MS` | Maximum retry delay | `15000` |
| `LEDGERFLOW_INDEXER_RPC_MAX_RETRIES` | Retry count for rate-limit responses | `10` |
| `LEDGERFLOW_INDEXER_WORKER_MODE` | Worker role: `live`, `lazy`, or `history` | `live` for the API service |
| `LEDGERFLOW_INDEXER_API_ENABLED` | Bind and serve the indexer HTTP API | `true`; worker-only services set `false` |
| `LEDGERFLOW_INDEXER_LAZY_ENABLED` | Allow address queries to enqueue uncovered ranges | `true` |
| `LEDGERFLOW_INDEXER_LAZY_WORKER_ENABLED` | Consume the lazy queue inside this process | `false` on live; Compose runs `indexer-lazy` separately |
| `LEDGERFLOW_INDEXER_LIVE_FOLLOW` | Continue polling after the configured target is reached | `true` |
| `LEDGERFLOW_INDEXER_LIVE_HEAD_LAG_BLOCKS` | Safety distance behind the RPC head | `2` |
| `LEDGERFLOW_INDEXER_LIVE_BOOTSTRAP_BLOCKS` | Canonical window indexed on a fresh live cursor | `128` |
| `LEDGERFLOW_INDEXER_HISTORY_HEAD_LAG_BLOCKS` | Minimum distance of a history worker from head | `128` |
| `LEDGERFLOW_INDEXER_HISTORY_BOUNDARY_WORKER_ID` | Live progress row whose persisted range start freezes a new history worker's upper bound | `robinhood_live` |
| `LEDGERFLOW_INDEXER_HISTORY_RECEIPT_MODE` | Receipt strategy: block-wide RPC or one transaction receipt at a time | `transaction` for local Robinhood Nitro |
| `LEDGERFLOW_INDEXER_REORG_LOOKBACK_BLOCKS` | Maximum common-ancestor search depth | `128` |
| `LEDGERFLOW_INDEXER_INTER_BATCH_DELAY_MS` | Low-priority delay after each committed batch | `0` live; set a positive value for history |
| `LEDGERFLOW_INDEXER_WORKER_ID` | Persisted progress key | `robinhood_live` |

## Run with Docker

This is the shortest path to a local instance.

1. Copy `.env.example` to `.env`. Docker Compose reads `.env` as the runtime config file.
2. Start the stack:

```bash
docker compose up -d --build
```

The services will be available at:

- API: `http://127.0.0.1:3000`
- Web: `http://127.0.0.1:3001`
- Indexer API: `http://127.0.0.1:8090`

The API service runs migrations on startup. The web service is a separate process that proxies the API and exposes a minimal operator UI.

If you only want the standalone top-level indexer:

```bash
docker compose up --build clickhouse rpc-gateway indexer indexer-lazy
```

Full history is not required for seed intelligence or live monitoring. Start
the optional historical fact pass only for a deliberate research workload:

```bash
docker compose --profile full-history up -d indexer-history
```

On its first successful start, `indexer-history` persists an immutable
`range_end` from the live worker's `range_start - 1`; subsequent restarts reuse
that value instead of chasing the moving head. Its default fast mode does not
fetch receipts or logs and therefore is not sufficient for profit, gas,
failure, protocol, or asset-flow conclusions. Lazy address requests are capped
by `LEDGERFLOW_INDEXER_LAZY_MAX_RANGE_BLOCKS` and must not be used as an
implicit genesis-to-head scan.

The `registry-sync` service has no public port. Its endpoint URLs, chain IDs,
parser kinds, intervals, cursors, snapshots, and run history live in PostgreSQL.
To execute a single due-source pass on the host, run:

```bash
LEDGERFLOW_REGISTRY_SYNC_RUN_ONCE=true cargo run --bin ledgerflow_registry_sync
```

The `discovery-worker`, `address-producer`, and `behavior-worker` services also have no public ports.
They read canonical facts from ClickHouse and persist independent PostgreSQL
cursors. To run one address-production pass on the host:

```bash
LEDGERFLOW_ADDRESS_PRODUCER_RUN_ONCE=true \
  cargo run --bin ledgerflow_address_fact_producer
```

To evaluate one statistical behavior window on the host:

```bash
LEDGERFLOW_BEHAVIOR_WORKER_RUN_ONCE=true \
  cargo run --bin ledgerflow_behavior_worker
```

The behavior rule is intentionally candidate-only. Defaults require at least
100 sender transactions across 8 producer windows, a 1,000-block span, and 2
transactions per 1,000 blocks within the latest 50,000 blocks. Configure these
with `LEDGERFLOW_BEHAVIOR_MIN_TRANSACTION_COUNT`,
`LEDGERFLOW_BEHAVIOR_MIN_ACTIVE_WINDOWS`,
`LEDGERFLOW_BEHAVIOR_MIN_SPAN_BLOCKS`,
`LEDGERFLOW_BEHAVIOR_MIN_TX_PER_1000_BLOCKS`, and
`LEDGERFLOW_BEHAVIOR_LOOKBACK_BLOCKS`. Change
`LEDGERFLOW_BEHAVIOR_RULE_VERSION` whenever rule semantics change; model claims
expire according to `LEDGERFLOW_BEHAVIOR_ASSERTION_TTL_HOURS`.

The private analysis API exposes the review workflow:

```bash
curl 'http://127.0.0.1:3000/behavior-reviews?chain_id=4663&status=pending&limit=50'

curl -X POST 'http://127.0.0.1:3000/behavior-reviews/REVIEW_ID/decision' \
  -H 'content-type: application/json' \
  -d '{"decision":"accept","reviewer":"operator@example.com","rationale":"Reviewed canonical samples"}'
```

Valid decisions are `accept`, `reject`, and `retract`. Keep port 3000 private;
the decision endpoint is an operator surface, not a public Web endpoint.

Only event signatures and ABI field positions are defined in code. Factories,
pools, Tokens, protocol contracts, and participant addresses are discovered at
runtime and stored in PostgreSQL; unknown event topics are not treated as
addresses without ABI evidence.

## Run locally with Cargo

If you already have PostgreSQL and the indexer running in Docker already, this is the recommended way to run analysis on the host:

1. Create a database named `ledgerflow`.
2. Export environment variables:

```bash
export LEDGERFLOW_DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5532/ledgerflow
export LEDGERFLOW_RPC_URL=http://127.0.0.1:8545
export LEDGERFLOW_INDEXER_API_BASE_URL=http://127.0.0.1:8090
export LEDGERFLOW_INDEXER_API_BEARER_TOKEN=your-long-random-secret
export LEDGERFLOW_BIND_ADDR=127.0.0.1:3000
```

3. Run migrations:

```bash
make migrate
```

4. Start the API server:

```bash
cargo run --bin ledgerflow
```

5. In another terminal, start the backfill worker:

```bash
cargo run --bin ledgerflow_backfill_worker
```

6. In another terminal, start the web server:

```bash
export LEDGERFLOW_API_BASE_URL=http://127.0.0.1:3000
export LEDGERFLOW_WEB_BIND_ADDR=127.0.0.1:3001
cargo run --bin web
```

When you run the API directly with Cargo, it listens on `http://127.0.0.1:3000` by default. The backfill worker is a separate process and must stay running if you want newly created clusters to keep making progress.

To run only the standalone top-level indexer:

```bash
export LEDGERFLOW_INDEXER_CLICKHOUSE_URL=http://127.0.0.1:8123
export LEDGERFLOW_INDEXER_CLICKHOUSE_USER=ledgerflow
export LEDGERFLOW_INDEXER_CLICKHOUSE_PASSWORD=ledgerflow
export LEDGERFLOW_INDEXER_RPC_URL=http://127.0.0.1:18545
export LEDGERFLOW_INDEXER_BATCH_SIZE=200
export LEDGERFLOW_INDEXER_RPC_CONCURRENCY=4
export LEDGERFLOW_INDEXER_RPC_BATCH_REQUEST_SIZE=1
export LEDGERFLOW_INDEXER_RPC_REQUEST_DELAY_MS=25
make run-indexer
```

The indexer-specific endpoints are:

- `GET /health`
- `GET /progress`
- `GET /addresses/{address}/txs?limit=&from_block=&to_block=&direction=`
- `GET /addresses/{address}/tx-hashes?limit=&from_block=&to_block=&direction=`

## Health check

```bash
curl http://localhost:8180/health
```

Expected response:

```json
{"status":"ok"}
```

## Discover installed recognizers

```bash
curl http://localhost:8180/recognizers | jq
```

Use this endpoint to confirm:

- which recognizers are installed
- which recognizers are enabled by default
- which plugin packs were loaded at startup

## Use the web console

Open:

```text
http://localhost:3000
```

The web process is intentionally product-first. It is not a second analysis engine. It does four things:

- gives you a homepage and pricing surface
- owns registration, login, and sessions
- renders the operator dashboard
- calls the internal analysis API on behalf of authenticated users

Current web console actions:

- homepage
- pricing page
- email-verified registration plus password or email-code login
- create an address cluster
- inspect cluster coverage
- load a cluster spend summary
- analyze a transaction
- load an already persisted analysis
- replay with a recognizer subset

## Email verification delivery

By default, the verification sender falls back to structured logs. That is enough for local development.

If you want real email delivery, set:

```bash
export LEDGERFLOW_EMAIL_FROM=noreply@yourdomain.xyz
export LEDGERFLOW_RESEND_API_KEY=re_xxx
```

Then the web process will send verification emails through Resend.

## Create an address cluster

```bash
curl -X POST http://localhost:8180/clusters \
  -H 'content-type: application/json' \
  -d '{
    "chain_id": 8453,
    "owner_label": "ops_team",
    "name": "primary_searcher",
    "description": "bot + treasury EOAs",
    "addresses": [
      {
        "address": "0x1111111111111111111111111111111111111111",
        "label": "bot-1",
        "role": "bot"
      },
      {
        "address": "0x5555555555555555555555555555555555555555",
        "label": "treasury",
        "role": "eoa"
      }
    ]
  }' | jq
```

The response returns the `cluster_id`.

If `LEDGERFLOW_CLUSTER_AUTO_BACKFILL_ON_CREATE=true`, cluster creation immediately enqueues a historical backfill job. The API request returns quickly; discovery and tx analysis happen later in the standalone backfill worker process.

## Cluster backfill queue

After creating a cluster, the worker will:

- query the standalone indexer for address-related tx refs
- deduplicate tx hashes across cluster addresses
- analyze missing transactions
- update cluster coverage

If the indexer API requires auth, set `LEDGERFLOW_INDEXER_API_BEARER_TOKEN` in the analysis service environment. Backfill requests will then include `Authorization: Bearer <token>`.

The normal path is automatic queueing on cluster creation. There is still an API endpoint for manually enqueueing another backfill run:

```bash
curl -X POST http://localhost:8180/clusters/YOUR_CLUSTER_ID/backfill \
  -H 'content-type: application/json' \
  -d '{
    "start_block": 0,
    "end_block": 45000000,
    "force_reanalyze": false,
    "query_limit": 10000
  }' | jq
```

Useful notes:

- `end_block` is optional. If omitted, the analysis service uses the indexer's latest indexed block.
- `query_limit` is the per-request ceiling used when reading an address range from the indexer.
- This endpoint now enqueues work. It does not synchronously analyze the whole cluster in the request path.
- The worker recursively splits block ranges when the per-request limit is hit, so it can backfill large address histories without direct ClickHouse reads.

## Trace net-delta monitor

Run the monitor when you want automatic "who ended up with more assets" detection. By default this is a low-cost receipt/log scanner: it stores ERC20 transfer-log deltas and top-level native value deltas, then leaves transaction explanation to the analysis API.

```bash
cargo run --bin ledgerflow_trace_monitor
```

It also exposes a lightweight HTTP API on `LEDGERFLOW_TRACE_MONITOR_BIND_ADDR`, defaulting to `127.0.0.1:3010`.

Health check:

```bash
curl http://127.0.0.1:3010/health
```

Top positive address net deltas for a time range:

```bash
curl "http://127.0.0.1:3010/top-address-deltas?start=2026-05-12T00:00:00Z&end=2026-05-12T01:00:00Z&top_n=20" | jq
```

The response is intentionally small. Each item contains the address, asset type, token address, raw `in / out / net` amounts, tx count, and up to 50 contributing tx hashes. It does not return full tx payloads; pass the tx hashes to the analysis API when you want interpretation.

On every process start, the monitor anchors its progress to the current confirmed block and only scans blocks that arrive after startup. It does not replay an old persisted monitor position.

The fast arbitrage path is enabled by `LEDGERFLOW_TRACE_MONITOR_FAST_ARBITRAGE_ENABLED=true`. It scans new block ranges for known DEX swap topics, groups logs by transaction hash, and writes `arbitrage_candidate` rows when one transaction has at least `LEDGERFLOW_TRACE_MONITOR_FAST_ARBITRAGE_MIN_SWAPS` swap logs across distinct pools. These rows are fast candidate labels, not confirmed PnL.

`LEDGERFLOW_TRACE_MONITOR_FAST_ARBITRAGE_SOURCE` controls the log source:

- `rpc_logs`: self-hosted fallback. The monitor uses one `eth_getLogs` call per block range. This is simple but can be slow on Reth when the node must generate filtered logs live.
- `cryptohouse`: high-throughput mode. The monitor queries CryptoHouse ClickHouse logs, aggregates candidate txs there, and only writes compact candidate rows locally. Set `LEDGERFLOW_TRACE_MONITOR_FAST_ARBITRAGE_SOURCE_LAG_BLOCKS` to keep the scan behind the RPC head until the external log source has ingested the block.

Docker Compose defaults the live arbitrage monitor to `rpc_logs` through `LEDGERFLOW_TRACE_MONITOR_COMPOSE_FAST_ARBITRAGE_SOURCE=rpc_logs`, so live capture does not depend on the public CryptoHouse quota.

The structured arbitrage tables are:

| Table | Purpose |
| --- | --- |
| `arbitrage_candidates` | one row per candidate tx, including sender, entry contract, swap count, pool count, route, and route fingerprint |
| `arbitrage_candidate_pools` | one row per candidate tx/pool pair, including first log index and per-pool swap counts |

Trace monitor exposes read-only arbitrage endpoints:

```bash
curl "http://127.0.0.1:3010/arbitrage/recent?minutes=60&limit=50" | jq
curl "http://127.0.0.1:3010/arbitrage/recent?minutes=60&limit=50&confirmed_only=true" | jq
curl "http://127.0.0.1:3010/arbitrage/hot-pools?minutes=60&limit=25" | jq
curl "http://127.0.0.1:3010/arbitrage/hot-routes?minutes=60&limit=25" | jq
```

`arbitrage_candidates.confirmation_status` separates fast candidates from conservative confirmations:

- `candidate`: multiple known DEX swap events across distinct pools in one transaction. This is not proof of realized profit.
- `confirmed_stable_surplus_to_sender`: the transaction sender has positive USDC/USDbC ERC20 Transfer net flow inside the same tx, with no USDC/USDbC net outflow. This is intentionally incomplete but high-confidence.

The web portal exposes the same local data at `/app/arbitrage`, plus a session-protected JSON aggregation endpoint:

```bash
curl "http://127.0.0.1:3001/api/arbitrage?minutes=60&limit=50" \
  -H "Authorization: Bearer <indexer_api_key>" | jq
```

It returns `recent_candidates`, `hot_pools`, and `hot_routes`. `minutes` is limited to 1-1,440 and `limit` to 10-200. The endpoint accepts either a logged-in `ledgerflow_session` cookie or an active Indexer API key through `Authorization: Bearer <key>` or `x-api-key: <key>`.

When `rpc_logs` is active, the monitor fetches transaction endpoints only for up to 16 newly discovered candidates per block range, sequentially through the RPC gateway. This fills `sender_address` and `entry_contract` without a parallel request spike. Candidates written before this behavior was deployed retain null endpoints unless they are later reprocessed.

The web portal also exposes a CryptoHouse-backed block range swap API through the web port:

```bash
curl "http://127.0.0.1:3001/api/swaps/block-range?from_block=48428000&to_block=48428500&min_swaps=1&tx_limit=100&pool_limit=50" \
  -H "Cookie: ledgerflow_session=<session>" | jq
```

The API returns `summary`, `swap_txs`, and `hot_pools`. Block ranges are capped at 2,000 blocks per request so ad-hoc UI/API usage does not produce unbounded CryptoHouse scans.

The broader net-delta path is controlled by `LEDGERFLOW_TRACE_MONITOR_LIGHT_DELTAS_ENABLED`. When enabled, the monitor also fetches each transaction and receipt without trace, derives visible top-level native transfers plus ERC20 transfer logs, and writes those deltas for broad coverage. For a high-throughput candidate-only live detector, set it to `false`.

| Table | Purpose |
| --- | --- |
| `trace_address_asset_deltas` | per tx, per address, per asset `in / out / net` |
| `trace_cluster_asset_deltas` | the same deltas collapsed over existing cluster addresses |
| `trace_monitor_candidates` | lightweight liquidation and multi-swap candidates detected from receipts |

The default retention is 7 days. It is configured by `LEDGERFLOW_TRACE_MONITOR_RETENTION_DAYS`. Cleanup is intentionally low-priority: the live monitor performs bounded cleanup batches roughly every 15 minutes instead of deleting old rows on every scan iteration.

The monitor intentionally does not call trace RPC. When you need transaction interpretation or internal native movement, pass the returned tx hashes to the transaction analysis API.

### Confirmed alert delivery

The periodic report path confirms only selected candidates through the existing transaction analysis API. Confirmed alerts are delivered through Telegram only; unconfirmed candidates are not sent.

- `liquidation`: a recognized liquidation with a priced asset movement.
- `arbitrage`: an arbitrage-like route with positive estimated subject value change.
- `large-transfer`: a priced movement over `LEDGERFLOW_TRACE_MONITOR_NOTIFY_MIN_USD`.
- `whale-buy`: a priced swap over that threshold initiated by an address carrying the `actor:whale` address label.

`ledgerflow_address_worker` automatically promotes an EOA to
`actor:whale` once the priced balance lower bound of tokens already observed in its analyzed
history exceeds `LEDGERFLOW_ADDRESS_WHALE_THRESHOLD_USD` (default `$100,000`). Token prices are
resolved lazily through known DEX pools and `ledgerflow_price_worker`; contracts, routers, pools,
tokens, and protocol contracts are not promoted. No additional schema is required.

You can also maintain the same label manually:

```sql
insert into address_labels (
  chain_id, address, label, source, confidence, evidence_json, created_at, updated_at
)
values (
  8453,
  lower('0xYOUR_WHALE_ADDRESS'),
  'actor:whale',
  'manual',
  1.0,
  '{"reason":"monitor watchlist"}'::jsonb,
  now(),
  now()
)
on conflict (chain_id, address, label, source) do update set
  confidence = excluded.confidence,
  evidence_json = excluded.evidence_json,
  updated_at = now();
```

Configure Telegram delivery with:

```bash
LEDGERFLOW_TELEGRAM_BOT_TOKEN=your-bot-token
LEDGERFLOW_TELEGRAM_CHAT_ID=@your_public_channel_username
```

For a private channel, use its numeric Bot API channel ID instead of `@username`. The bot must be a channel administrator with permission to post messages.

Each Telegram alert contains the confirmed activity type, an explicitly labeled profit or observed value estimate, the main subject address, known address labels when available, transfer direction for large-transfer alerts, a copyable transaction hash, and the public share link. Liquidation and arbitrage alerts use the same subject-net-value basis shown on the shared analysis page when that value is available; observed movement value is used only as a fallback. Email provider settings remain relevant only to website login/register verification codes.

## Check cluster coverage

```bash
curl http://localhost:8180/clusters/YOUR_CLUSTER_ID/coverage | jq
```

Important coverage fields:

| Field | Meaning |
| --- | --- |
| `discovered_tx_count` | unique tx hashes discovered for the cluster |
| `analyzed_tx_count` | txs already analyzed and persisted |
| `failed_tx_count` | txs whose analysis failed during backfill |
| `pending_tx_count` | discovered txs not yet analyzed |
| `latest_run_status` | latest backfill status |

## Inspect backfill runs

```bash
curl http://localhost:8180/clusters/YOUR_CLUSTER_ID/backfill-runs | jq
```

## Request a cluster spend summary

```bash
curl "http://localhost:8180/clusters/YOUR_CLUSTER_ID/spend-summary?start=2026-04-01T00:00:00Z&end=2026-04-30T23:59:59Z" | jq
```

This summary is computed from already analyzed transactions whose top-level `from_address` or `to_address` matches the cluster address set.

### Important cluster summary fields

| Field | Meaning |
| --- | --- |
| `total_gas_cost_raw` | total gas spent by top-level transactions sent from the cluster |
| `total_external_native_outflow_raw` | native asset sent from cluster addresses to outside addresses |
| `total_internal_native_transfer_raw` | native asset moved between addresses inside the same cluster |
| `total_incoming_native_raw` | native asset received into the cluster from outside |
| `breakdowns` | per-category, per-asset, per-counterparty aggregation |
| `transactions` | tx-level summary rows for drill-down |

### What the starter counts as spend right now

- gas burn paid by cluster senders
- native outflows from cluster addresses to outside addresses
- ERC20 outflows from cluster addresses to outside addresses
- internal transfers, tracked separately so they do not get mistaken for external spend

### Important limitation

Cluster spend summary still reads from persisted analyses only.

That means:

- create cluster
- keep the backfill worker running
- wait for coverage to move
- then read spend summary

If coverage is partial, the spend summary metadata will reflect that partiality rather than pretending full completeness.

## Analyze a transaction

Submit a transaction hash and chain id:

```bash
curl -X POST http://localhost:8180/analyses/tx \
  -H 'content-type: application/json' \
  -d '{
    "chain_id": 8453,
    "tx_hash": "0xYOUR_TX_HASH"
  }' | jq
```

The response includes:

- `analysis`: the normalized transaction analysis
- `evidence_pack`: the replay/export artifact built from that analysis

### Important fields in `analysis`

| Field | Meaning |
| --- | --- |
| `raw_evidence_refs` | what raw objects were fetched and referenced |
| `normalized_execution_facts.selector` | extracted selector, if present |
| `derived_flows` | native and ERC20 flows derived from evidence |
| `recognizer_results` | recognizer outputs with confidence and evidence refs |
| `analysis_status` | `completed`, `partial`, or `failed` |
| `unresolved_items` | things the pipeline could not resolve safely |
| `validation_results` | replay/completeness/sanity checks |
| `analysis_version_metadata` | ruleset, pipeline, flow, and recognizer versions |
| `input_digest` | deterministic digest for replay identity |

### Important fields in `evidence_pack`

| Field | Meaning |
| --- | --- |
| `pack_id` | stable id for later lookup |
| `ruleset_id` | ruleset used to build the pack |
| `recognizer_set_versions` | recognizer version map used in this run |
| `facts` | normalized fact export |
| `labels` | recognizer labels in export-friendly form |
| `flows` | exported flow data |
| `unresolved` | unresolved facts carried into the pack |
| `validation_results` | validation output captured in the pack |

## Fetch an existing analysis

```bash
curl "http://localhost:8180/analyses/0xYOUR_TX_HASH?chain_id=8453" | jq
```

This reads the persisted analysis and its latest evidence pack from PostgreSQL.

## Fetch an evidence pack directly

```bash
curl http://localhost:8180/evidence-packs/YOUR_PACK_ID | jq
```

Use this endpoint when you want the export artifact without the full analysis envelope.

## Replay a transaction

Replay uses the same pipeline shape but lets you request a recognizer subset and validate ruleset availability.

```bash
curl -X POST http://localhost:8180/replay/tx \
  -H 'content-type: application/json' \
  -d '{
    "chain_id": 8453,
    "tx_hash": "0xYOUR_TX_HASH",
    "ruleset_id": "base.ruleset.v0",
    "recognizer_ids": [
      "selector_presence",
      "native_transfer"
    ]
  }' | jq
```

Notes:

- the current starter only exposes one installed ruleset
- replay will reject recognizer ids that are not installed
- replay still fetches evidence again in v0; it does not yet pin remote snapshots or archive raw payload bodies outside the database

## Common outcomes and how to read them

### `analysis_status = completed`

Validations passed with no warnings. This does not mean the transaction is fully understood. It means the current pipeline completed cleanly.

### `analysis_status = partial`

The analysis is usable but something remained unresolved or a validator emitted a warning. This is common in early-stage recognizer coverage.

### `analysis_status = failed`

A validator marked the result as failed or a hard pipeline error occurred.

### Recognizer confidence

Recognizer results always include a `confidence` score. The starter uses confidence to express narrow heuristics and partial pattern matches. Do not treat a high-confidence label as proof beyond the evidence refs that back it.

## Trace behavior

`LEDGERFLOW_TRACE_ENABLED=false` is the safest default. With tracing disabled:

- the fetcher does not request traces
- flow derivation relies on transaction value and logs
- some heuristic recognizers will stay less certain

If you enable tracing against an RPC that does not support trace methods, the API may return a conflict-style error describing unsupported trace availability.

## Known v0 limitations

- only a small set of recognizers is implemented
- no protocol-specific decoding pipeline yet
- no state-diff-based conservation accounting yet
- replay determinism is version-based, but raw remote evidence retention is still minimal
- task/opportunity/attempt rollups are scaffolded but not yet fully wired into API usage

## Development workflow

### Seed-first cold start

The `seed-worker` has no public port. It periodically projects official labels,
protocol deployments, active pools/assets, and repeated MEV-like execution
evidence into `intelligence_seed_addresses`, then enqueues only effective
priority 0-39 addresses for expensive enrichment. `mev_actor` rows with
`candidate` status are selection signals, never verified identities.

S2A also persists neutral first-hop observations and schedules bounded evidence
analysis. For each selected MEV seed, at most the latest eight explicit
candidate transaction hashes are sent through the existing deterministic
analysis pipeline. It does not request full address history. Related call
targets and pools are queued at priorities 22-28. A completed job with missing
evidence analysis is requeued automatically; a transaction failure uses the
normal failed-job retry path.

```bash
LEDGERFLOW_SEED_WORKER_RUN_ONCE=true \
  cargo run --bin ledgerflow_seed_worker

curl -fsS 'http://127.0.0.1:3000/intelligence/seeds/summary?chain_id=4663'
curl -fsS 'http://127.0.0.1:3000/intelligence/seeds?chain_id=4663&seed_kind=mev_actor&status=candidate&limit=25'
curl -fsS 'http://127.0.0.1:3000/intelligence/seeds/relationships?chain_id=4663&relationship_type=transaction_pool&limit=25'
curl -fsS 'http://127.0.0.1:3000/intelligence/seeds/coverage?chain_id=4663&seed_kind=mev_actor&limit=25'
```

The relationship API proves only the named observation. It must not be read as
ownership, identity, intent, profit, or economic causality. Coverage responses
set `full_history_coverage` to `false`; an absent analysis remains unknown, not
zero.

Relevant limits:

```bash
LEDGERFLOW_SEED_MAX_EVIDENCE_TRANSACTIONS_PER_SEED=8
LEDGERFLOW_SEED_MAX_RELATED_ADDRESSES=5000
LEDGERFLOW_SEED_MAX_QUEUED_PRIORITY=39
LEDGERFLOW_ADDRESS_WORKER_MAX_PRIORITY=39
```

The Compose address worker receives `LEDGERFLOW_INDEXER_API_BASE_URL` for
existing full-history workflows. The explicit evidence path itself does not
depend on an Indexer history request.

Useful commands:

```bash
make check
make test
make fmt
```

If you want both processes locally without Docker, run `make run` and `make run-web` in separate terminals.

## Where to go next

- For system shape and stage boundaries, read `docs/architecture.md`.
- For schema design, read `docs/schema.md`.
- For version and digest behavior, read `docs/replay_and_versioning.md`.
- For adding recognizers or validators, read `docs/plugin_development.md`.
