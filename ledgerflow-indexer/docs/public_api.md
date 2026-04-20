# Public API

Production base URL:

```text
https://indexer.ledgerflow.top
```

Local development:

```text
http://127.0.0.1:8090
```

Authentication is required for `/v1/...` endpoints:

```http
Authorization: Bearer lf_live_xxx
```

The service records successful and failed authenticated usage per API key. `/health` and legacy internal endpoints are not part of the public metered API surface.

Quota priority:

1. If the user has an active subscription, requests consume subscription quota.
2. If there is no active subscription, requests consume signup/referral bonus credits.
3. Signup bonus is 1,000 requests.
4. Referral bonus is 25,000 requests per verified referral, capped at 250,000 bonus requests per month.

For public deployments, keep `LEDGERFLOW_INDEXER_ENABLE_LEGACY_API=false` so only `/v1/...`, `/admin/...`, and `/health` are mounted.

## Health

```http
GET /health
```

## Index Progress

```http
GET /v1/chains/base/progress
Authorization: Bearer lf_live_xxx
```

Response:

```json
{
  "chain": "base",
  "chain_id": 8453,
  "worker_id": "base_mainnet_top_level",
  "last_indexed_block": 31050099
}
```

## Address Transaction Hashes

```http
GET /v1/base/address/{address}/tx-hashes
Authorization: Bearer lf_live_xxx
```

Query parameters:

| Name | Description |
| --- | --- |
| `from_block` | Inclusive lower block bound |
| `to_block` | Inclusive upper block bound |
| `direction` | `from`, `to`, or omitted for both |
| `limit` | Max hashes returned, clamped by server config |

Response:

```json
{
  "chain": "base",
  "chain_id": 8453,
  "address": "0x1111111111111111111111111111111111111111",
  "limit": 100,
  "tx_hashes": ["0x..."]
}
```

## Address Transaction References

```http
GET /v1/base/address/{address}/txs
Authorization: Bearer lf_live_xxx
```

Query parameters:

| Name | Description |
| --- | --- |
| `from_block` | Inclusive lower block bound |
| `to_block` | Inclusive upper block bound |
| `direction` | `from`, `to`, or omitted for both |
| `limit` | Max refs returned, clamped by server config |
| `cursor` | Optional pagination cursor returned by the previous response |

Response:

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
      "tx_hash": "0x...",
      "block_number": 31050099,
      "tx_index": 14,
      "direction": "from"
    }
  ]
}
```

Cursor format is intentionally opaque to clients even though v1 currently uses `block_number:tx_index:direction_rank`. Treat it as a token and pass it back unchanged.

## Batch Address Transaction References

```http
POST /v1/base/addresses/txs
Authorization: Bearer lf_live_xxx
```

Request:

```json
{
  "addresses": ["0x1111111111111111111111111111111111111111"],
  "from_block": 30000000,
  "to_block": 32000000,
  "direction": "from",
  "limit": 1000
}
```

Limits:

- Maximum `50` addresses per batch request.
- `limit` is per address and is clamped by server config.
- Large addresses should use single-address pagination with `cursor`; batch lookup is for moderate cluster fan-out, not exporting an entire exchange wallet in one call.

Response:

```json
{
  "chain": "base",
  "chain_id": 8453,
  "items": [
    {
      "address": "0x1111111111111111111111111111111111111111",
      "tx_hash": "0x...",
      "block_number": 31050099,
      "tx_index": 14,
      "direction": "from"
    }
  ]
}
```

## Base RPC Convenience Endpoints

These endpoints proxy a narrow set of Base JSON-RPC reads through the configured reth node. They are metered like the indexer endpoints.

```http
GET /v1/base/block/latest
GET /v1/base/block/{block_number}
GET /v1/base/tx/{tx_hash}
GET /v1/base/tx/{tx_hash}/receipt
Authorization: Bearer lf_live_xxx
```

Use these to enrich indexed transaction refs without configuring a separate RPC provider.

## Dashboard Admin Endpoints

These are session-cookie endpoints for the LedgerFlow web dashboard, not public API-key endpoints:

```http
GET /admin/progress
GET /admin/account/summary
GET /admin/usage/summary
GET /admin/referral
```

The dashboard intentionally does not call `/v1/chains/base/progress` because `/v1` is a customer API-key surface.

## Product Positioning

This API deliberately focuses on address-to-top-level-transaction lookup. It is not a full Etherscan clone. It is optimized for:

- bot operators finding historical txs for controlled EOAs
- analytics developers building fast address activity workflows
- protocol teams auditing project wallets and operational spend
- analysis systems that need tx candidate discovery before deeper transaction interpretation
