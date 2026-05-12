# LedgerFlow Indexer

This document covers the standalone `ledgerflow indexer` process.

It is intentionally minimal. It only indexes top-level Base transactions into a global address reference table so you can answer:

- which transaction hashes touched this address at the top level
- whether the address was the `from` or the `to`
- which block and tx position each hit came from

It does not index:

- internal calls
- logs
- traces
- ERC20 transfer participants
- cluster semantics

Large net asset-change monitoring lives outside the indexer in `ledgerflow_trace_monitor`. That process uses trace RPC and PostgreSQL because it is intentionally heavier than the top-level address index.

## What it stores

The ClickHouse table is:

`address_tx_refs`

Each top-level transaction can emit up to two rows:

- one row for `from`
- one row for `to`, if `to` is present

The table stores:

- `chain_id`
- `address`
- `tx_hash`
- `block_number`
- `tx_index`
- `direction`

The indexer also keeps a tiny progress table:

`indexer_progress`

That table records the last fully indexed block for a named worker.

It also keeps an `api_keys` table for externally issued client credentials. Public query endpoints accept either:

- `Authorization: Bearer <api_key>`
- `x-api-key: <api_key>`

Administrative key issuance and revocation should stay behind the internal portal or an internal bearer token.

## Configuration

| Variable | Purpose | Default |
| --- | --- | --- |
| `LEDGERFLOW_INDEXER_BIND_ADDR` | HTTP bind address | `0.0.0.0:8090` |
| `LEDGERFLOW_INDEXER_RPC_URL` | Base RPC endpoint used for `eth_blockNumber` and `eth_getBlockByNumber(..., true)` | `http://host.docker.internal:18545` in Docker via nginx proxy, `http://127.0.0.1:18545` on the host |
| `LEDGERFLOW_INDEXER_CLICKHOUSE_URL` | ClickHouse HTTP endpoint | `http://127.0.0.1:8123` |
| `LEDGERFLOW_INDEXER_CLICKHOUSE_DATABASE` | ClickHouse database name | `ledgerflow_indexer` |
| `LEDGERFLOW_INDEXER_CLICKHOUSE_USER` | ClickHouse username | `ledgerflow` in Docker, `default` fallback outside Docker if unset |
| `LEDGERFLOW_INDEXER_CLICKHOUSE_PASSWORD` | ClickHouse password | `ledgerflow` in Docker |
| `LEDGERFLOW_INDEXER_CHAIN_ID` | chain id stored in rows | `8453` |
| `LEDGERFLOW_INDEXER_START_BLOCK` | initial backfill block when no progress exists | `0` |
| `LEDGERFLOW_INDEXER_END_BLOCK` | optional stop block for bounded backfills | unset |
| `LEDGERFLOW_INDEXER_BATCH_SIZE` | number of blocks claimed and committed per batch | `200` |
| `LEDGERFLOW_INDEXER_RPC_CONCURRENCY` | number of blocks fetched in parallel inside a batch | `4` |
| `LEDGERFLOW_INDEXER_RPC_REQUEST_DELAY_MS` | per-request delay before each block fetch task runs | `25` |
| `LEDGERFLOW_INDEXER_RPC_RETRY_BASE_MS` | base retry backoff after rate-limit errors | `1000` |
| `LEDGERFLOW_INDEXER_RPC_RETRY_MAX_MS` | maximum retry backoff | `15000` |
| `LEDGERFLOW_INDEXER_RPC_MAX_RETRIES` | retry count for rate-limit errors | `10` |
| `LEDGERFLOW_INDEXER_LIVE_FOLLOW` | keep polling after catch-up | `true` |
| `LEDGERFLOW_INDEXER_POLL_INTERVAL_MS` | polling delay when caught up or retrying | `2000` |
| `LEDGERFLOW_INDEXER_WORKER_ID` | progress row key | `base_mainnet_top_level` |
| `LEDGERFLOW_INDEXER_QUERY_LIMIT_DEFAULT` | default query limit | `1000` |
| `LEDGERFLOW_INDEXER_QUERY_LIMIT_MAX` | hard query limit cap | `10000` |
| `LEDGERFLOW_INDEXER_INTERNAL_API_TOKEN` | internal bearer token allowed to call admin API-key endpoints and portal lookups | unset |

## Run with Docker

Start only the indexer stack:

```bash
docker-compose up --build clickhouse indexer
```

Local endpoints:

- ClickHouse HTTP: `http://localhost:8123`
- Indexer API: `http://localhost:8090`

## Run locally with Cargo

1. Start ClickHouse.
2. Export environment variables:

```bash
export LEDGERFLOW_INDEXER_CLICKHOUSE_URL=http://127.0.0.1:8123
export LEDGERFLOW_INDEXER_CLICKHOUSE_USER=ledgerflow
export LEDGERFLOW_INDEXER_CLICKHOUSE_PASSWORD=ledgerflow
export LEDGERFLOW_INDEXER_RPC_URL=http://127.0.0.1:18545
export LEDGERFLOW_INDEXER_BIND_ADDR=0.0.0.0:8090
export LEDGERFLOW_INDEXER_BATCH_SIZE=200
export LEDGERFLOW_INDEXER_RPC_CONCURRENCY=4
export LEDGERFLOW_INDEXER_RPC_REQUEST_DELAY_MS=25
```

3. Start the process:

```bash
make run-indexer
```

On startup the indexer will:

1. create the ClickHouse database if needed
2. create `address_tx_refs` and `indexer_progress` if needed
3. resume from `indexer_progress` if a row exists
4. otherwise start from `LEDGERFLOW_INDEXER_START_BLOCK`
5. backfill until head
6. continue polling if `LEDGERFLOW_INDEXER_LIVE_FOLLOW=true`

## Query API

### Health

```bash
curl http://localhost:8090/health
```

### Progress

```bash
curl http://localhost:8090/progress
```

Example response:

```json
{
  "chain_id": 8453,
  "worker_id": "base_mainnet_top_level",
  "last_indexed_block": 44740000
}
```

### Query deduplicated transaction hashes for an address

```bash
curl "http://localhost:8090/addresses/0x1111111111111111111111111111111111111111/tx-hashes?limit=100"
```

This endpoint returns a deduplicated tx hash list for the address, ordered by newest block first.

Optional filters:

- `limit`
- `from_block`
- `to_block`
- `direction=from`
- `direction=to`

Example response shape:

```json
{
  "address": "0x1111111111111111111111111111111111111111",
  "tx_hashes": [
    "0xabc...",
    "0xdef..."
  ]
}
```

### Query full reference rows for an address

```bash
curl "http://localhost:8090/addresses/0x1111111111111111111111111111111111111111/txs?limit=100"
```

This endpoint returns the full stored rows, including `block_number`, `tx_index`, and `direction`.

Optional filters:

- `limit`
- `from_block`
- `to_block`
- `direction=from`
- `direction=to`

Example:

```bash
curl "http://localhost:8090/addresses/0x1111111111111111111111111111111111111111/txs?from_block=44000000&to_block=44700000&direction=from&limit=200"
```

Example response shape:

```json
{
  "address": "0x1111111111111111111111111111111111111111",
  "refs": [
    {
      "chain_id": 8453,
      "address": "0x1111111111111111111111111111111111111111",
      "tx_hash": "0xabc...",
      "block_number": 44739991,
      "tx_index": 3,
      "direction": "from"
    }
  ]
}
```

## Operational notes

- Docker defaults are set up to target a host nginx RPC proxy at `host.docker.internal:18545`.
- The nginx proxy should forward to the real node on `127.0.0.1:8545` and allow only Docker subnet traffic.
- If your node is on another machine or port, override `LEDGERFLOW_INDEXER_RPC_URL`.
- The worker uses HTTP RPC polling right now, not WebSocket subscriptions.
- Backfill fetches are concurrent within each batch; block order does not matter as long as every block is covered.
- It indexes only top-level transaction `from` and `to`.
- Contract creation transactions emit only the `from` row.
- Self-sends emit two rows for the same address with different `direction` values.
- `/tx-hashes` deduplicates self-sends down to one transaction hash.
- If a block fetch returns `null`, the worker does not advance progress for that range. It retries later instead of silently skipping coverage.

## Throughput tuning

The main throughput knobs are:

- `LEDGERFLOW_INDEXER_BATCH_SIZE`
- `LEDGERFLOW_INDEXER_RPC_CONCURRENCY`
- `LEDGERFLOW_INDEXER_RPC_REQUEST_DELAY_MS`

For a self-hosted Base node behind the local nginx proxy, a reasonable starting point is:

- `LEDGERFLOW_INDEXER_BATCH_SIZE=200`
- `LEDGERFLOW_INDEXER_RPC_CONCURRENCY=4`
- `LEDGERFLOW_INDEXER_RPC_REQUEST_DELAY_MS=25`

Then increase concurrency gradually to `16` or `32` if your node and disk can absorb it.

These variables do not materially affect indexing throughput:

- `LEDGERFLOW_INDEXER_POLL_INTERVAL_MS`
  Only matters when the worker is caught up or sleeping after a batch-level failure.
- `LEDGERFLOW_INDEXER_WORKER_ID`
  Only identifies the progress row.
- `LEDGERFLOW_INDEXER_QUERY_LIMIT_DEFAULT`
  Only affects API responses.
- `LEDGERFLOW_INDEXER_QUERY_LIMIT_MAX`
  Only affects API responses.

## Scope boundary

This indexer is a reusable address-to-tx primitive. It is not the full analysis engine and it is not a cluster product by itself.
