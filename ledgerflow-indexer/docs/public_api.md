# Public API

Base URL:

```text
https://api.ledgerflow.dev
```

Local development:

```text
http://127.0.0.1:8090
```

Authentication is required for `/v1/...` endpoints:

```http
Authorization: Bearer lf_live_xxx
```

The service records daily usage per API key and requires an active subscription. `/health` and legacy internal endpoints are not part of the public metered API surface.

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
| `limit` | Max hashes returned |

Response:

```json
{
  "chain": "base",
  "chain_id": 8453,
  "address": "0x1111111111111111111111111111111111111111",
  "tx_hashes": ["0x..."]
}
```

## Address Transaction References

```http
GET /v1/base/address/{address}/txs
Authorization: Bearer lf_live_xxx
```

Response:

```json
{
  "chain": "base",
  "chain_id": 8453,
  "address": "0x1111111111111111111111111111111111111111",
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

## Product Positioning

This API deliberately focuses on address-to-top-level-transaction lookup. It is not a full Etherscan clone. It is optimized for:

- bot operators finding historical txs for controlled EOAs
- analytics developers building fast address activity workflows
- protocol teams auditing project wallets and operational spend
- analysis systems that need tx candidate discovery before deeper transaction interpretation
