# LedgerFlow Indexer Public API

Production base URL:

```text
https://indexer.ledgerflow.top
```

Local development:

```text
http://127.0.0.1:8090
```

LedgerFlow Indexer is currently Base-only and indexes top-level transaction `from` / `to` references. It is intentionally not a full explorer clone. Use it to discover transaction candidates quickly, then enrich or analyze them with the RPC convenience endpoints or a downstream evidence-first transaction analyzer.

## Authentication

All `/v1/...` endpoints require a live API key:

```http
Authorization: Bearer lf_live_xxx
```

`GET /health` does not require authentication.

Dashboard `/admin/...` endpoints use the `ledgerflow_session` HttpOnly cookie set by the login/register flow. They do not accept public API keys.

Optional internal bypass for trusted local consumers:

```text
LEDGERFLOW_INDEXER_INTERNAL_API_TOKEN=some-long-random-secret
```

If this variable is set, the same bearer token can be used against `/v1/...` to bypass subscription and quota checks. This is intended for trusted internal analysis services, not public users.

Built-in public demo token for low-risk trials:

```text
lf_test_ledgerflow_public_demo
```

This token works without additional setup and can be used across the public `/v1/...` API surface.

The public demo token is:

- rate-limited per client IP
- capped to a lower `limit` than paid users
- meant for quick evaluation, not production use

Operator overrides still exist if you want to change the built-in behavior:

```text
LEDGERFLOW_INDEXER_TEST_API_TOKEN=
LEDGERFLOW_INDEXER_TEST_API_RPS=1
LEDGERFLOW_INDEXER_TEST_API_BURST=3
LEDGERFLOW_INDEXER_TEST_API_MAX_LIMIT=50
```

## Metering And Quota

The service records successful and failed authenticated `/v1` requests per API key, endpoint, usage source, and outcome.

Quota priority:

1. If the user has an active subscription, requests consume subscription quota.
2. If there is no active subscription, requests consume signup/referral bonus credits.
3. Signup bonus is `1,000` requests.
4. Referral bonus is `25,000` requests per verified referral, capped at `250,000` bonus requests per month.

Default query limit:

```text
LEDGERFLOW_INDEXER_QUERY_LIMIT_DEFAULT=1000
```

Maximum query limit:

```text
LEDGERFLOW_INDEXER_QUERY_LIMIT_MAX=10000
```

The server clamps user-supplied `limit` to `[1, LEDGERFLOW_INDEXER_QUERY_LIMIT_MAX]`.

For the public demo token, `limit` is clamped a second time to:

```text
LEDGERFLOW_INDEXER_TEST_API_MAX_LIMIT
```

For public deployments, keep:

```text
LEDGERFLOW_INDEXER_ENABLE_LEGACY_API=false
```

This leaves only `/v1/...`, `/admin/...`, and `/health` mounted.

## Common Types

### AddressTxRef

```json
{
  "chain_id": 8453,
  "address": "0x1111111111111111111111111111111111111111",
  "tx_hash": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  "block_number": 31050099,
  "tx_index": 14,
  "direction": "from"
}
```

Fields:

| Field | Type | Description |
| --- | --- | --- |
| `chain_id` | number | EVM chain id. Base mainnet is `8453`. |
| `address` | string | Lowercase `0x` address used for this reference row. |
| `tx_hash` | string | Transaction hash. |
| `block_number` | number | Block number containing the transaction. |
| `tx_index` | number | Transaction index inside the block. |
| `direction` | string | `from` when the address is top-level sender, `to` when it is top-level recipient. |

### Error Response

```json
{
  "error": "missing Authorization header"
}
```

Common status codes:

| Status | Meaning |
| --- | --- |
| `400` | Malformed address, cursor, block number, tx hash, or RPC evidence. |
| `401` | Missing, malformed, invalid, or expired authentication. |
| `402` | Subscription required, subscription quota exhausted, or bonus quota exhausted. |
| `429` | Public demo token rate limit exceeded for this client IP. |
| `409` | Request conflicts with product rules, for example too many batch addresses. |
| `500` | ClickHouse, Postgres, or upstream RPC failure. |

## Endpoint Summary

| Method | Path | Auth | Metered | Purpose |
| --- | --- | --- | --- | --- |
| `GET` | `/health` | no | no | Service health. |
| `GET` | `/v1/chains/base/progress` | API key | yes | Last indexed Base block. |
| `GET` | `/v1/base/address/{address}/txs` | API key | yes | Paginated top-level tx refs for one address. |
| `GET` | `/v1/base/address/{address}/tx-hashes` | API key | yes | Deduplicated tx hashes for one address. |
| `POST` | `/v1/base/addresses/txs` | API key | yes | Batch top-level tx refs for up to 50 addresses. |
| `GET` | `/v1/base/block/latest` | API key | yes | Latest Base block number from the configured reth/RPC node. |
| `GET` | `/v1/base/block/{block_number}` | API key | yes | Block with full transaction objects from RPC. |
| `GET` | `/v1/base/tx/{tx_hash}` | API key | yes | Transaction object from RPC. |
| `GET` | `/v1/base/tx/{tx_hash}/receipt` | API key | yes | Transaction receipt from RPC. |

Public demo token access is available across the public `/v1/...` API surface, but it is still rate-limited per client IP and uses a smaller maximum `limit`.

## GET /health

Health check for load balancers and uptime checks.

Request:

```http
GET /health
```

Authentication:

```text
none
```

Parameters:

```text
none
```

Response `200`:

```json
{
  "status": "ok",
  "service": "ledgerflow-indexer"
}
```

Notes:

- Not metered.
- Does not prove ClickHouse, Postgres, or RPC health yet. It only proves the HTTP process is up.

## GET /v1/chains/base/progress

Returns the last Base block fully processed by the configured indexer worker.

Request:

```http
GET /v1/chains/base/progress
Authorization: Bearer lf_live_xxx
```

Demo token request:

```http
GET /v1/chains/base/progress
Authorization: Bearer lf_test_ledgerflow_public_demo
```

Path parameters:

```text
none
```

Query parameters:

```text
none
```

Response `200`:

```json
{
  "chain": "base",
  "chain_id": 8453,
  "worker_id": "base_mainnet_top_level",
  "last_indexed_block": 31050099
}
```

Fields:

| Field | Type | Description |
| --- | --- | --- |
| `chain` | string | Fixed string `base`. |
| `chain_id` | number | Fixed Base mainnet chain id `8453`, unless configured otherwise. |
| `worker_id` | string | Worker progress namespace. Defaults to `base_mainnet_top_level`. |
| `last_indexed_block` | number or null | Highest indexed block. `null` means no progress row exists yet. |

Usage:

```bash
curl "https://indexer.ledgerflow.top/v1/chains/base/progress" \
  -H "Authorization: Bearer lf_live_xxx"
```

## GET /v1/base/address/{address}/txs

Returns top-level transaction references for one address.

This is the primary endpoint for high-volume addresses because it supports cursor pagination.

Request:

```http
GET /v1/base/address/{address}/txs?from_block=30000000&to_block=32000000&direction=from&limit=100&cursor=31050099:14:1
Authorization: Bearer lf_live_xxx
```

Demo token request:

```http
GET /v1/base/address/{address}/txs?limit=20
Authorization: Bearer lf_test_ledgerflow_public_demo
```

Path parameters:

| Name | Type | Required | Description |
| --- | --- | --- | --- |
| `address` | string | yes | EVM address. Must be a `0x` hex address with 40 hex chars. Input is normalized to lowercase. |

Query parameters:

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `from_block` | integer | no | none | Inclusive lower block bound. |
| `to_block` | integer | no | none | Inclusive upper block bound. |
| `direction` | string | no | any | `from`, `to`, `any`, or omitted. `from` returns txs where address is top-level sender. `to` returns txs where address is top-level recipient. |
| `limit` | integer | no | `LEDGERFLOW_INDEXER_QUERY_LIMIT_DEFAULT` | Max refs returned. Clamped to server max. |
| `cursor` | string | no | none | Pagination cursor returned as `next_cursor` by the previous response. Treat as opaque. |

Ordering:

```text
block_number desc, tx_index desc, direction asc
```

Demo token notes:

- requests are rate-limited per client IP
- `limit` is additionally capped by `LEDGERFLOW_INDEXER_TEST_API_MAX_LIMIT`
- use this path for manual testing or a lightweight product demo only

Response `200`:

```json
{
  "chain": "base",
  "chain_id": 8453,
  "address": "0x1111111111111111111111111111111111111111",
  "limit": 100,
  "next_cursor": "31050099:14:1",
  "items": [
    {
      "chain_id": 8453,
      "address": "0x1111111111111111111111111111111111111111",
      "tx_hash": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "block_number": 31050099,
      "tx_index": 14,
      "direction": "from"
    }
  ]
}
```

Fields:

| Field | Type | Description |
| --- | --- | --- |
| `chain` | string | Fixed string `base`. |
| `chain_id` | number | Chain id. |
| `address` | string | Normalized lowercase address. |
| `limit` | number | Effective server-clamped limit. |
| `next_cursor` | string or null | Cursor for next page. Missing/null means no next page was detected. |
| `items` | array | Array of `AddressTxRef`. |

Pagination:

1. Call first page without `cursor`.
2. If `next_cursor` is non-null, pass it as `cursor` in the next request.
3. Stop when `next_cursor` is null.

Example:

```bash
curl "https://indexer.ledgerflow.top/v1/base/address/0x1111111111111111111111111111111111111111/txs?direction=from&limit=100" \
  -H "Authorization: Bearer lf_live_xxx"
```

Next page:

```bash
curl "https://indexer.ledgerflow.top/v1/base/address/0x1111111111111111111111111111111111111111/txs?direction=from&limit=100&cursor=31050099:14:1" \
  -H "Authorization: Bearer lf_live_xxx"
```

Implementation note:

- Current cursor format is `block_number:tx_index:direction_rank`.
- `direction_rank` is `1` for `from` and `2` for `to`.
- Clients should treat the cursor as opaque because this may change.

## GET /v1/base/address/{address}/tx-hashes

Returns deduplicated transaction hashes for one address.

Use this when you only need tx hashes and do not need direction or block/index metadata. This endpoint currently does not expose cursor pagination; for very high-volume addresses use `/txs`.

Request:

```http
GET /v1/base/address/{address}/tx-hashes?from_block=30000000&to_block=32000000&direction=to&limit=100
Authorization: Bearer lf_live_xxx
```

Demo token request:

```http
GET /v1/base/address/{address}/tx-hashes?limit=20
Authorization: Bearer lf_test_ledgerflow_public_demo
```

Path parameters:

| Name | Type | Required | Description |
| --- | --- | --- | --- |
| `address` | string | yes | EVM address. Must be a `0x` hex address with 40 hex chars. |

Query parameters:

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `from_block` | integer | no | none | Inclusive lower block bound. |
| `to_block` | integer | no | none | Inclusive upper block bound. |
| `direction` | string | no | any | `from`, `to`, `any`, or omitted. |
| `limit` | integer | no | `LEDGERFLOW_INDEXER_QUERY_LIMIT_DEFAULT` | Max hashes returned. Clamped to server max. |

Demo token notes:

- requests are rate-limited per client IP
- `limit` is additionally capped by `LEDGERFLOW_INDEXER_TEST_API_MAX_LIMIT`

Ordering:

```text
block_number desc, tx_index desc
```

Response `200`:

```json
{
  "chain": "base",
  "chain_id": 8453,
  "address": "0x1111111111111111111111111111111111111111",
  "limit": 100,
  "tx_hashes": [
    "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  ]
}
```

Fields:

| Field | Type | Description |
| --- | --- | --- |
| `chain` | string | Fixed string `base`. |
| `chain_id` | number | Chain id. |
| `address` | string | Normalized lowercase address. |
| `limit` | number | Effective server-clamped limit. |
| `tx_hashes` | array | Deduplicated tx hashes. |

Example:

```bash
curl "https://indexer.ledgerflow.top/v1/base/address/0x1111111111111111111111111111111111111111/tx-hashes?limit=1000" \
  -H "Authorization: Bearer lf_live_xxx"
```

## POST /v1/base/addresses/txs

Returns top-level transaction references for multiple addresses.

Use this for moderate address clusters. Do not use it to export extremely high-volume exchange/router addresses. For that, call the single-address `/txs` endpoint with cursor pagination.

Request:

```http
POST /v1/base/addresses/txs
Authorization: Bearer lf_live_xxx
Content-Type: application/json
```

JSON body:

```json
{
  "addresses": [
    "0x1111111111111111111111111111111111111111",
    "0x2222222222222222222222222222222222222222"
  ],
  "from_block": 30000000,
  "to_block": 32000000,
  "direction": "any",
  "limit": 1000
}
```

Body fields:

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `addresses` | array<string> | yes | none | EVM addresses. Maximum 50 addresses. |
| `from_block` | integer | no | none | Inclusive lower block bound, applied to each address query. |
| `to_block` | integer | no | none | Inclusive upper block bound, applied to each address query. |
| `direction` | string | no | any | `from`, `to`, `any`, or omitted. |
| `limit` | integer | no | `LEDGERFLOW_INDEXER_QUERY_LIMIT_DEFAULT` | Max refs per address. Clamped to server max. |

Limits:

- Maximum `50` addresses per request.
- `limit` is per address, not total response limit.
- Response items are globally sorted by newest block and transaction index after per-address queries.
- This endpoint does not expose cursor pagination. Use single-address `/txs` for very large exports.

Response `200`:

```json
{
  "chain": "base",
  "chain_id": 8453,
  "items": [
    {
      "chain_id": 8453,
      "address": "0x1111111111111111111111111111111111111111",
      "tx_hash": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "block_number": 31050099,
      "tx_index": 14,
      "direction": "from"
    }
  ],
  "per_address_counts": {
    "0x1111111111111111111111111111111111111111": 1,
    "0x2222222222222222222222222222222222222222": 0
  }
}
```

Fields:

| Field | Type | Description |
| --- | --- | --- |
| `chain` | string | Fixed string `base`. |
| `chain_id` | number | Chain id. |
| `items` | array | Combined `AddressTxRef` rows. |
| `per_address_counts` | object | Map from normalized address to number of refs returned for that address. |

Example:

```bash
curl "https://indexer.ledgerflow.top/v1/base/addresses/txs" \
  -H "Authorization: Bearer lf_live_xxx" \
  -H "content-type: application/json" \
  -d '{
    "addresses": [
      "0x1111111111111111111111111111111111111111",
      "0x2222222222222222222222222222222222222222"
    ],
    "from_block": 30000000,
    "direction": "any",
    "limit": 1000
  }'
```

## GET /v1/base/block/latest

Returns the latest Base block number from the configured reth/RPC node.

Request:

```http
GET /v1/base/block/latest
Authorization: Bearer lf_live_xxx
```

Parameters:

```text
none
```

Response `200`:

```json
{
  "chain": "base",
  "chain_id": 8453,
  "block_number": 44820000
}
```

Fields:

| Field | Type | Description |
| --- | --- | --- |
| `chain` | string | Fixed string `base`. |
| `chain_id` | number | Chain id. |
| `block_number` | number | Latest block number returned by `eth_blockNumber`. |

Example:

```bash
curl "https://indexer.ledgerflow.top/v1/base/block/latest" \
  -H "Authorization: Bearer lf_live_xxx"
```

## GET /v1/base/block/{block_number}

Returns a Base block from the configured reth/RPC node with full transaction objects.

Request:

```http
GET /v1/base/block/{block_number}
Authorization: Bearer lf_live_xxx
```

Path parameters:

| Name | Type | Required | Description |
| --- | --- | --- | --- |
| `block_number` | integer | yes | Decimal block number. The server converts it to hex for `eth_getBlockByNumber`. |

Query parameters:

```text
none
```

Response `200`:

```json
{
  "chain": "base",
  "chain_id": 8453,
  "block": {
    "number": "0x1d9f33",
    "hash": "0x...",
    "parentHash": "0x...",
    "timestamp": "0x...",
    "transactions": [
      {
        "hash": "0x...",
        "from": "0x...",
        "to": "0x...",
        "value": "0x0",
        "input": "0x"
      }
    ]
  }
}
```

`block` is the raw JSON-RPC result from `eth_getBlockByNumber(block_number, true)`.

Example:

```bash
curl "https://indexer.ledgerflow.top/v1/base/block/31050099" \
  -H "Authorization: Bearer lf_live_xxx"
```

## GET /v1/base/tx/{tx_hash}

Returns a transaction object from the configured reth/RPC node.

Request:

```http
GET /v1/base/tx/{tx_hash}
Authorization: Bearer lf_live_xxx
```

Path parameters:

| Name | Type | Required | Description |
| --- | --- | --- | --- |
| `tx_hash` | string | yes | Transaction hash. Must be a `0x` hash accepted by the upstream RPC. |

Query parameters:

```text
none
```

Response `200`:

```json
{
  "chain": "base",
  "chain_id": 8453,
  "transaction": {
    "hash": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "from": "0x1111111111111111111111111111111111111111",
    "to": "0x2222222222222222222222222222222222222222",
    "blockNumber": "0x1d9f33",
    "transactionIndex": "0xe",
    "value": "0x0",
    "input": "0x"
  }
}
```

`transaction` is the raw JSON-RPC result from `eth_getTransactionByHash`.

Example:

```bash
curl "https://indexer.ledgerflow.top/v1/base/tx/0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" \
  -H "Authorization: Bearer lf_live_xxx"
```

## GET /v1/base/tx/{tx_hash}/receipt

Returns a transaction receipt from the configured reth/RPC node.

Request:

```http
GET /v1/base/tx/{tx_hash}/receipt
Authorization: Bearer lf_live_xxx
```

Path parameters:

| Name | Type | Required | Description |
| --- | --- | --- | --- |
| `tx_hash` | string | yes | Transaction hash. Must be a `0x` hash accepted by the upstream RPC. |

Query parameters:

```text
none
```

Response `200`:

```json
{
  "chain": "base",
  "chain_id": 8453,
  "receipt": {
    "transactionHash": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "status": "0x1",
    "blockNumber": "0x1d9f33",
    "transactionIndex": "0xe",
    "gasUsed": "0x5208",
    "logs": []
  }
}
```

`receipt` is the raw JSON-RPC result from `eth_getTransactionReceipt`.

Example:

```bash
curl "https://indexer.ledgerflow.top/v1/base/tx/0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/receipt" \
  -H "Authorization: Bearer lf_live_xxx"
```

## Dashboard Admin Endpoints

These endpoints are used by the LedgerFlow web dashboard and require the `ledgerflow_session` cookie. They are documented here so product behavior is explicit, but external API customers should use `/v1/...`.

### GET /admin/progress

Returns index progress for a logged-in dashboard user. This exists so dashboard progress does not require a customer API key.

Response:

```json
{
  "chain": "base",
  "chain_id": 8453,
  "worker_id": "base_mainnet_top_level",
  "last_indexed_block": 31050099
}
```

### GET /admin/account/summary

Returns current subscription and quota state for the logged-in user.

Response:

```json
{
  "summary": {
    "has_active_subscription": true,
    "plan_id": "launch",
    "current_period_end": "2026-05-21T00:00:00Z",
    "subscription_quota": 1000000,
    "subscription_used_this_month": 1200,
    "subscription_remaining_this_month": 998800,
    "signup_bonus_requests": 1000,
    "referral_bonus_requests_this_month": 25000,
    "bonus_used_this_month": 0,
    "bonus_remaining_this_month": 26000
  }
}
```

### GET /admin/usage/summary

Returns per-endpoint usage stats and recent API failures for the logged-in user.

Response:

```json
{
  "stats": [
    {
      "endpoint": "GET /v1/base/address/{address}/txs",
      "usage_source": "subscription",
      "outcome": "success",
      "request_count": 42
    }
  ],
  "recent_failures": [
    {
      "endpoint": "GET /v1/base/address/{address}/txs",
      "status_code": 400,
      "error": "malformed evidence: invalid address",
      "created_at": "2026-04-21T00:00:00Z"
    }
  ]
}
```

### GET /admin/referral

Returns referral code and reward totals for the logged-in user.

Response:

```json
{
  "referral": {
    "referral_code": "lf15a58fa664",
    "reward_per_verified_referral": 25000,
    "monthly_reward_cap": 250000,
    "rewarded_referrals_this_month": 1,
    "bonus_requests_this_month": 25000
  }
}
```

## Product Positioning

This API deliberately focuses on address-to-top-level-transaction lookup. It is optimized for:

- bot operators finding historical txs for controlled EOAs
- analytics developers building fast address activity workflows
- protocol teams auditing project wallets and operational spend
- analysis systems that need tx candidate discovery before deeper transaction interpretation

It does not infer protocol semantics, decode arbitrary calldata, or replace an evidence-first transaction analyzer.
