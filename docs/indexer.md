# LedgerFlow Indexer

This document covers the standalone `ledgerflow indexer` process and its `live`, `lazy`, and `history` roles on Robinhood Chain.

It maintains a canonical raw-fact layer and a global top-level address reference table so you can answer:

- which transaction hashes touched this address at the top level
- whether the address was the `from` or the `to`
- which block and tx position each hit came from

The `live` role persists complete block transactions and receipt logs from its
activation range forward. `history` is an optional, explicitly enabled
full-history pass; it is not part of normal startup and is not required by seed
intelligence. The Robinhood history profile stores canonical blocks and full
transaction bodies but defers receipts and logs. The `lazy` role fills only
explicit, bounded ranges requested by API callers. The indexer does not
persist:

- internal calls
- traces
- ERC20 transfer participants
- cluster semantics

Large net asset-change monitoring lives outside the indexer in `ledgerflow_trace_monitor`. That process uses trace RPC and PostgreSQL because it is intentionally heavier than the top-level address index.

## What it stores

The canonical fact tables are:

- `canonical_blocks`: block hashes, parent hashes, timestamps, L1 references, finality state, canonical/orphan state, and the observing worker
- `transactions_raw`: full top-level transactions plus receipt status, gas, effective gas price, and created-contract address
- `logs_raw`: receipt logs with topics, data, transaction position, and removal state
- `address_tx_refs`: top-level sender and recipient directions used by the query API
- `indexed_blocks`: global block-completion markers shared by lazy requests

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

`indexer_worker_progress` records each worker's role, fixed range, last block/hash, RPC endpoint, retry state, head observation, lag, and last success time. The older `indexer_progress` table remains for compatibility with pre-canonical lazy coverage.

It also keeps an `api_keys` table for externally issued client credentials. Public query endpoints accept either:

- `Authorization: Bearer <api_key>`
- `x-api-key: <api_key>`

Administrative key issuance and revocation should stay behind the internal portal or an internal bearer token.

## Configuration

| Variable | Purpose | Default |
| --- | --- | --- |
| `LEDGERFLOW_INDEXER_BIND_ADDR` | HTTP bind address | `0.0.0.0:8090` |
| `LEDGERFLOW_INDEXER_RPC_URL` | Robinhood RPC endpoint used by this worker | `http://rpc-gateway:8545` for throttled Docker workers |
| `LEDGERFLOW_INDEXER_CLICKHOUSE_URL` | ClickHouse HTTP endpoint | `http://127.0.0.1:8123` |
| `LEDGERFLOW_INDEXER_CLICKHOUSE_DATABASE` | ClickHouse database name | `ledgerflow_indexer` |
| `LEDGERFLOW_INDEXER_CLICKHOUSE_USER` | ClickHouse username | `ledgerflow` in Docker, `default` fallback outside Docker if unset |
| `LEDGERFLOW_INDEXER_CLICKHOUSE_PASSWORD` | ClickHouse password | `ledgerflow` in Docker |
| `LEDGERFLOW_INDEXER_CHAIN_ID` | chain id stored in rows | `4663` in this deployment |
| `LEDGERFLOW_INDEXER_START_BLOCK` | default lower bound for lazy queries; continuous-scan start when lazy mode is disabled | `0` |
| `LEDGERFLOW_INDEXER_END_BLOCK` | optional stop block for bounded backfills | unset |
| `LEDGERFLOW_INDEXER_BATCH_SIZE` | number of blocks claimed and committed per batch | `200` |
| `LEDGERFLOW_INDEXER_RPC_CONCURRENCY` | number of blocks fetched in parallel inside a batch | `4` |
| `LEDGERFLOW_INDEXER_RPC_BATCH_REQUEST_SIZE` | number of block requests packed into one JSON-RPC batch request, without increasing total block concurrency | `1` |
| `LEDGERFLOW_INDEXER_RPC_REQUEST_DELAY_MS` | per-request delay before each block fetch task runs | `25` |
| `LEDGERFLOW_INDEXER_RPC_REQUEST_TIMEOUT_MS` | hard timeout for a single RPC HTTP request | `30000` |
| `LEDGERFLOW_INDEXER_RPC_RETRY_BASE_MS` | base retry backoff after rate-limit errors | `1000` |
| `LEDGERFLOW_INDEXER_RPC_RETRY_MAX_MS` | maximum retry backoff | `15000` |
| `LEDGERFLOW_INDEXER_RPC_MAX_RETRIES` | retry count for rate-limit errors | `10` |
| `LEDGERFLOW_INDEXER_WORKER_MODE` | worker role: `live`, `lazy`, or `history` | inferred from lazy mode when unset |
| `LEDGERFLOW_INDEXER_API_ENABLED` | serve the HTTP query API in this process | `true` |
| `LEDGERFLOW_INDEXER_LAZY_ENABLED` | queue missing block ranges from address queries instead of scanning history on startup | `true` |
| `LEDGERFLOW_INDEXER_LAZY_MAX_RANGE_BLOCKS` | maximum inclusive range accepted by one lazy request; prevents an address lookup becoming an implicit full-chain scan | `250000` |
| `LEDGERFLOW_INDEXER_LIVE_FOLLOW` | keep polling after catch-up when lazy mode is disabled | `true` |
| `LEDGERFLOW_INDEXER_LIVE_HEAD_LAG_BLOCKS` | safety distance behind the RPC head | `2` |
| `LEDGERFLOW_INDEXER_LIVE_BOOTSTRAP_BLOCKS` | initial live canonical window | `128` |
| `LEDGERFLOW_INDEXER_HISTORY_BOUNDARY_WORKER_ID` | live progress row used to freeze a new history range | `robinhood_live` |
| `LEDGERFLOW_INDEXER_HISTORY_INCLUDE_RECEIPTS` | include receipt/status/gas/log evidence during the canonical history pass | `true` generally; Robinhood Compose fast pass uses `false` |
| `LEDGERFLOW_INDEXER_HISTORY_RECEIPT_MODE` | `block` for `eth_getBlockReceipts`, or `transaction` for individual receipts | `transaction`; use `block` only after validating RPC support |
| `LEDGERFLOW_INDEXER_REORG_LOOKBACK_BLOCKS` | common-ancestor search depth for live reorg recovery | `128` |
| `LEDGERFLOW_INDEXER_INTER_BATCH_DELAY_MS` | delay after a committed batch | `0`; Robinhood Compose history uses `200` |
| `LEDGERFLOW_INDEXER_POLL_INTERVAL_MS` | polling delay when caught up or retrying | `2000` |
| `LEDGERFLOW_INDEXER_WORKER_ID` | progress row key | `base_mainnet_top_level` |
| `LEDGERFLOW_INDEXER_QUERY_LIMIT_DEFAULT` | default query limit | `1000` |
| `LEDGERFLOW_INDEXER_QUERY_LIMIT_MAX` | hard query limit cap | `10000` |
| `LEDGERFLOW_INDEXER_INTERNAL_API_TOKEN` | internal bearer token allowed to call admin API-key endpoints and portal lookups | unset |

The optional Robinhood Compose history worker reads the same validated upstream
as the live worker. Its first pass is useful only for broad historical research:
it stores block headers and complete transaction bodies, while leaving
receipt-derived status, gas, contract address, and logs unavailable.
Unavailable values remain `null`; canonical progress must not be interpreted as
receipt/log analysis completeness. The starting profile is 200 blocks per
commit, 16-block total concurrency using four-block JSON-RPC batches, no request
delay, and a 100 ms post-commit delay.

The RPC URL remains environment-configurable, but an override must first be
validated for chain ID and block-hash continuity at the current checkpoint.
Receipt/log enrichment is a separate, restartable follow-up workload. Neither
the fast pass nor enrichment blocks seed-address analysis, which consumes
explicit transaction hashes and current live facts.

## Run with Docker

Start only the indexer stack:

```bash
docker compose up --build clickhouse rpc-gateway indexer indexer-lazy
```

Start the historical worker only as an explicit research/operations job:

```bash
docker compose --profile full-history up -d indexer-history
```

The history worker freezes `range_end` from the persisted live worker's
`range_start - 1` on first start, and then reuses its saved range on every
restart. Stop it without deleting its container or ClickHouse data to preserve
the cursor. Normal `docker compose up` does not start it.

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
export LEDGERFLOW_INDEXER_CHAIN_ID=4663
export LEDGERFLOW_INDEXER_WORKER_MODE=lazy
export LEDGERFLOW_INDEXER_BATCH_SIZE=200
export LEDGERFLOW_INDEXER_RPC_CONCURRENCY=4
export LEDGERFLOW_INDEXER_RPC_BATCH_REQUEST_SIZE=1
export LEDGERFLOW_INDEXER_RPC_REQUEST_DELAY_MS=25
export LEDGERFLOW_INDEXER_LAZY_MAX_RANGE_BLOCKS=250000
```

3. Start the process:

```bash
make run-indexer
```

With the default `LEDGERFLOW_INDEXER_LAZY_ENABLED=true`, startup will:

1. create the ClickHouse database if needed
2. create the reference, global block coverage, and durable request tables
3. reuse any old contiguous `indexer_progress` coverage
4. wait for address queries without calling block RPC methods
5. reject unbounded or oversized address ranges and process only missing blocks
   from an accepted bounded range

Set `LEDGERFLOW_INDEXER_WORKER_MODE=live` and `LEDGERFLOW_INDEXER_LAZY_ENABLED=false` for a continuous canonical worker. Use `history` only as a separate bounded worker with a distinct `LEDGERFLOW_INDEXER_WORKER_ID`.

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
  "chain_id": 4663,
  "last_indexed_block": 36711605,
  "rpc_head_block": 36711607,
  "live_lag_blocks": 2,
  "contiguous_indexed_through_block": 794789,
  "coverage": {
    "configured_start_block": 0,
    "target_block": 27251940,
    "contiguous_indexed_through_block": 794789,
    "remaining_blocks": 26457151,
    "history_backfill_complete": false,
    "live_indexed_from_block": 27251941,
    "live_indexed_through_block": 36711605,
    "unindexed_gap_to_live_blocks": 26457151
  },
  "exact_storage_stats_included": false
}
```

`last_indexed_block` is the newest block stored by any continuous worker and
must not be interpreted as full historical coverage. The nested `coverage`
object reports the independently verifiable history range and the separate live
range. `contiguous_indexed_through_block` advances from the configured history
start and never jumps to the live tip while a gap exists.

Default progress requests do not scan raw fact tables or run the expensive
missing-endpoint join, so the endpoint stays on the interactive path. Exact
`FINAL` storage counts, the maximum legacy/lazy block, and the missing-endpoint
count are administrative diagnostics and can be requested explicitly:

```bash
curl 'http://localhost:8090/progress?include_exact_storage_stats=true'
```

That opt-in query can be slow on a large ClickHouse dataset. Coverage status is
derived from durable worker intervals and does not depend on those counts.

### Query deduplicated transaction hashes for an address

```bash
curl "http://localhost:8090/addresses/0x1111111111111111111111111111111111111111/tx-hashes?limit=100"
```

This endpoint returns a deduplicated tx hash list for the address, ordered by newest block first.

Address queries also request lazy coverage. A fully indexed range returns `200 OK`.
An incomplete range returns `202 Accepted`, partial stored results, and a `coverage`
object. Retry the same fixed `from_block` and `to_block` until `coverage.complete`
is true.

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
  ],
  "coverage": {
    "from_block": 44000000,
    "to_block": 44700000,
    "requested_block_count": 700001,
    "indexed_block_count": 700001,
    "complete": true,
    "request_id": null
  }
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

- Docker defaults are set up to target the configured host RPC endpoint.
- The history and live workers must resolve to the same chain ID and canonical block hashes at the persisted checkpoint.
- If your node is on another machine or port, override `LEDGERFLOW_INDEXER_RPC_URL`.
- The worker uses HTTP RPC polling right now, not WebSocket subscriptions.
- Live and lazy fetches may run concurrently inside a batch. The Robinhood history fast pass uses bounded JSON-RPC batching and a short inter-batch delay; monitor live lag and ClickHouse delayed inserts before raising it further.
- Lazy requests share global block completion markers, so later addresses do not refetch completed blocks.
- Empty blocks receive completion markers even though they produce no address reference rows.
- Address references cover only top-level transaction `from` and `to`; raw receipt logs remain available separately for ABI-aware downstream producers.
- Contract creation transactions emit only the `from` row.
- Self-sends emit two rows for the same address with different `direction` values.
- `/tx-hashes` deduplicates self-sends down to one transaction hash.
- If a block fetch returns `null`, the worker does not advance progress for that range. It retries later instead of silently skipping coverage.

The full lazy indexing contract is documented in
[docs/indexer/lazy_indexing.md](indexer/lazy_indexing.md).

## Throughput tuning

The main throughput knobs are:

- `LEDGERFLOW_INDEXER_BATCH_SIZE`
- `LEDGERFLOW_INDEXER_RPC_CONCURRENCY`
- `LEDGERFLOW_INDEXER_RPC_BATCH_REQUEST_SIZE`
- `LEDGERFLOW_INDEXER_RPC_REQUEST_DELAY_MS`

For the local Robinhood Nitro node behind the RPC gateway, the live/lazy profile can start with:

- `LEDGERFLOW_INDEXER_BATCH_SIZE=200`
- `LEDGERFLOW_INDEXER_RPC_CONCURRENCY=4`
- `LEDGERFLOW_INDEXER_RPC_BATCH_REQUEST_SIZE=1`
- `LEDGERFLOW_INDEXER_RPC_REQUEST_DELAY_MS=25`

`LEDGERFLOW_INDEXER_RPC_BATCH_REQUEST_SIZE` reduces HTTP round trips by packing multiple block requests into one JSON-RPC batch. It does not increase total block concurrency; for example, concurrency `8` with request size `4` runs two upstream HTTP batch requests at a time.

Then increase concurrency gradually to `16` or `32` if your node and disk can absorb it.

Do not apply those values directly to history. The deployed history profile starts at batch size `5`, block concurrency `1`, per-transaction delay `250ms`, request timeout `15s`, and inter-batch delay `1s`. Increase history throughput only after confirming that live lag stays within five blocks, gateway errors remain zero, and the PostgreSQL address queue is not growing uncontrollably.

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
