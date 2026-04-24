# Usage Guide

This document explains how to run LedgerFlow locally and how to use the starter API in practice.

If you only want the standalone top-level address indexer, read [docs/indexer.md](indexer.md).

The main operator workflow is now:

1. analyze transactions into the local store
2. create an address cluster for your bot and EOA set
3. request a spend summary for that cluster over a time window

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
- network access to a Base-compatible RPC endpoint

## Configuration

LedgerFlow reads configuration from environment variables.

| Variable | Purpose | Example |
| --- | --- | --- |
| `LEDGERFLOW_BIND_ADDR` | HTTP bind address for Axum | `0.0.0.0:8080` |
| `LEDGERFLOW_DATABASE_URL` | PostgreSQL connection string | `postgres://postgres:postgres@localhost:5432/ledgerflow` |
| `LEDGERFLOW_RPC_URL` | Base RPC endpoint | `http://host.docker.internal:18545` in Docker via nginx proxy, `http://127.0.0.1:18545` for local runs on the host |
| `LEDGERFLOW_INDEXER_API_BASE_URL` | Indexer API base URL used by analysis-side cluster backfill | `http://indexer:8090` |
| `LEDGERFLOW_INDEXER_API_BEARER_TOKEN` | Optional Bearer token sent to the indexer API | unset |
| `LEDGERFLOW_DEFAULT_CHAIN_ID` | Default chain for callers that omit it | `8453` |
| `LEDGERFLOW_TRACE_ENABLED` | Enables trace fetch attempts | `false` |
| `LEDGERFLOW_CLUSTER_BACKFILL_QUERY_LIMIT` | Per-request limit used when analysis reads tx refs from the indexer | `10000` |
| `LEDGERFLOW_CLUSTER_AUTO_BACKFILL_ON_CREATE` | Start a background historical backfill immediately after cluster creation | `true` |
| `LEDGERFLOW_ENABLED_RECOGNIZERS` | Comma-separated recognizer ids to run by default | `selector_presence,erc20_transfer_flow,...` |
| `LEDGERFLOW_PLUGINS` | Comma-separated plugin packs to load | `sample_builtin_pack` |
| `LEDGERFLOW_WEB_BIND_ADDR` | Web process bind address | `0.0.0.0:3000` |
| `LEDGERFLOW_API_BASE_URL` | Upstream analysis API used by the web process | `http://app:8080` |

The checked-in `.env.example` is the easiest starting point. Copy it to `.env` before using Docker Compose, because the compose stack reads `.env` as the runtime config file.

### Standalone indexer variables

If you are running only `ledgerflow indexer`, the main variables are:

| Variable | Purpose | Example |
| --- | --- | --- |
| `LEDGERFLOW_INDEXER_BIND_ADDR` | Indexer API bind address | `0.0.0.0:8090` |
| `LEDGERFLOW_INDEXER_RPC_URL` | Base RPC endpoint used for block polling | `http://host.docker.internal:18545` in Docker via nginx proxy, `http://127.0.0.1:18545` for local runs on the host |
| `LEDGERFLOW_INDEXER_CLICKHOUSE_URL` | ClickHouse HTTP endpoint | `http://127.0.0.1:8123` |
| `LEDGERFLOW_INDEXER_CLICKHOUSE_DATABASE` | ClickHouse database name | `ledgerflow_indexer` |
| `LEDGERFLOW_INDEXER_CLICKHOUSE_USER` | ClickHouse username | `ledgerflow` in Docker |
| `LEDGERFLOW_INDEXER_CLICKHOUSE_PASSWORD` | ClickHouse password | `ledgerflow` in Docker |
| `LEDGERFLOW_INDEXER_START_BLOCK` | Initial backfill block | `0` |
| `LEDGERFLOW_INDEXER_END_BLOCK` | Optional bounded stop block | unset |
| `LEDGERFLOW_INDEXER_BATCH_SIZE` | Blocks claimed and committed per indexing batch | `200` |
| `LEDGERFLOW_INDEXER_RPC_CONCURRENCY` | Parallel block fetch count inside a batch | `4` |
| `LEDGERFLOW_INDEXER_RPC_REQUEST_DELAY_MS` | Per-request delay before a block fetch task runs | `25` |
| `LEDGERFLOW_INDEXER_RPC_RETRY_BASE_MS` | Base retry delay after rate-limit responses | `1000` |
| `LEDGERFLOW_INDEXER_RPC_RETRY_MAX_MS` | Maximum retry delay | `15000` |
| `LEDGERFLOW_INDEXER_RPC_MAX_RETRIES` | Retry count for rate-limit responses | `10` |
| `LEDGERFLOW_INDEXER_LIVE_FOLLOW` | Continue following new blocks | `true` |
| `LEDGERFLOW_INDEXER_WORKER_ID` | Progress key | `base_mainnet_top_level` |

## Run with Docker

This is the shortest path to a local instance.

1. Copy `.env.example` to `.env`. Docker Compose reads `.env` as the runtime config file.
2. Start the stack:

```bash
docker-compose up --build
```

The services will be available at:

- API: `http://localhost:8180`
- Web: `http://localhost:3000`
- Indexer API: `http://localhost:8090`

The API service runs migrations on startup. The web service is a separate process that proxies the API and exposes a minimal operator UI.

If you only want the standalone top-level indexer:

```bash
docker-compose up --build clickhouse indexer
```

## Run locally with Cargo

If you already have PostgreSQL running locally:

1. Create a database named `ledgerflow`.
2. Export environment variables:

```bash
export LEDGERFLOW_DATABASE_URL=postgres://postgres:postgres@localhost:5432/ledgerflow
export LEDGERFLOW_RPC_URL=http://127.0.0.1:18545
export LEDGERFLOW_BIND_ADDR=0.0.0.0:8080
```

3. Run migrations:

```bash
make migrate
```

4. Start the API server:

```bash
make run
```

5. In another terminal, start the web server:

```bash
export LEDGERFLOW_API_BASE_URL=http://127.0.0.1:8080
make run-web
```

When you run the API directly with Cargo, it still listens on `http://localhost:8080` unless you override `LEDGERFLOW_BIND_ADDR`.

To run only the standalone top-level indexer:

```bash
export LEDGERFLOW_INDEXER_CLICKHOUSE_URL=http://127.0.0.1:8123
export LEDGERFLOW_INDEXER_CLICKHOUSE_USER=ledgerflow
export LEDGERFLOW_INDEXER_CLICKHOUSE_PASSWORD=ledgerflow
export LEDGERFLOW_INDEXER_RPC_URL=http://127.0.0.1:18545
export LEDGERFLOW_INDEXER_BATCH_SIZE=200
export LEDGERFLOW_INDEXER_RPC_CONCURRENCY=4
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

The web process is intentionally thin. It is not a second analysis engine. It does three things:

- gives you a basic external-facing query surface
- proxies browser requests to the analysis API
- makes it easier to evolve toward a real product frontend later

Current web console actions:

- create an address cluster
- load a cluster spend summary
- analyze a transaction
- load an already persisted analysis
- replay with a recognizer subset
- inspect installed recognizers

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

If `LEDGERFLOW_CLUSTER_AUTO_BACKFILL_ON_CREATE=true`, cluster creation also starts a background historical backfill immediately. You can then poll `/clusters/{cluster_id}/coverage` or `/clusters/{cluster_id}/backfill-runs` to watch progress.

## Backfill a cluster from the indexer

After creating a cluster, trigger backfill so the analysis service can:

- query the standalone indexer for address-related tx refs
- deduplicate tx hashes across cluster addresses
- analyze missing transactions
- update cluster coverage

If the indexer API requires auth, set `LEDGERFLOW_INDEXER_API_BEARER_TOKEN` in the analysis service environment. Backfill requests will then include `Authorization: Bearer <token>`.

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
- The analysis service recursively splits block ranges when the per-request limit is hit, so it can backfill large address histories without direct ClickHouse reads.

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
- run cluster backfill
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
