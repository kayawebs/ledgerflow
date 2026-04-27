# Indexer Consumer Guide

This document is for consumers of the LedgerFlow Indexer API.

It answers one practical question:

```text
How should I use the indexer as a discovery layer inside my own app, bot, analytics system, or transaction analysis pipeline?
```

The short answer:

```text
Use the indexer to discover candidate tx hashes quickly.
Do not ask the indexer to do full semantic transaction understanding.
```

## What The Indexer Is Good At

LedgerFlow Indexer currently indexes one narrow primitive on Base:

- top-level `from` references
- top-level `to` references
- address -> transaction lookup

This makes it useful for:

- bot wallet history discovery
- EOA and treasury address backfills
- cluster transaction discovery
- generating tx candidate lists before deeper analysis
- exporting address activity to downstream accounting or analytics jobs

## What The Indexer Does Not Do

Do not treat it as a full chain semantics engine.

It does not currently provide:

- ERC20 transfer indexing as a first-class indexed table
- internal calls / trace indexing as a first-class indexed table
- protocol-level decoding
- swap / bridge / liquidation understanding
- cluster labeling
- PnL or cost attribution
- transaction intent recognition

Those belong in a separate analysis layer.

## Recommended Architecture

The healthiest consumer pattern is:

```text
address set or cluster
    -> indexer discovers tx hashes / tx refs
    -> consumer deduplicates tx hashes
    -> consumer fetches tx / receipt / block / trace as needed
    -> consumer performs analysis, accounting, labeling, or reporting
```

In other words:

```text
Indexer = discovery layer
Analysis system = interpretation layer
```

## Authentication

Public `/v1/...` endpoints require:

```http
Authorization: Bearer lf_live_xxx
```

Example:

```bash
export LEDGERFLOW_API_KEY=lf_live_xxx
curl -s "https://indexer.ledgerflow.top/v1/chains/base/progress" \
  -H "Authorization: Bearer $LEDGERFLOW_API_KEY"
```

### Public Demo Token

The public trial credential is built in, so you can use:

```http
Authorization: Bearer lf_test_ledgerflow_public_demo
```

This is intentionally constrained:

- only `GET /v1/chains/base/progress`
- only `GET /v1/base/address/{address}/txs`
- only `GET /v1/base/address/{address}/tx-hashes`
- per-IP rate-limited
- lower maximum `limit`

Treat it as a try-before-you-buy credential, not something to embed into a production integration.

### Internal Consumer Token

If you control the deployment and want your own local analysis tool to bypass subscription and request quota, you can set:

```text
LEDGERFLOW_INDEXER_INTERNAL_API_TOKEN=some-long-random-secret
```

Then your internal consumer can call the same `/v1/...` endpoints with:

```http
Authorization: Bearer some-long-random-secret
```

Behavior:

- skips subscription checks
- skips quota exhaustion checks
- skips customer usage metering
- still requires authentication via the token

This is intended for trusted internal consumers only.

Do not enable it on a public deployment unless you are sure the token is kept private and only reachable from trusted network paths.

## Core Consumer Flows

### 1. Single Address Backfill

Use this when you want all top-level tx refs for one address.

Endpoint:

```http
GET /v1/base/address/{address}/txs
```

Example:

```bash
curl -s "https://indexer.ledgerflow.top/v1/base/address/0x1111111111111111111111111111111111111111/txs?limit=1000" \
  -H "Authorization: Bearer $LEDGERFLOW_API_KEY"
```

Important response fields:

- `items`: array of top-level tx refs
- `next_cursor`: pagination cursor
- `limit`: effective server-side limit after clamping

You should paginate until `next_cursor` is null.

Pseudo-flow:

```text
cursor = null
loop:
  call /txs with cursor
  append items
  break if next_cursor is null
  cursor = next_cursor
```

### 2. Address Cluster Discovery

Use this when you have a small or medium set of addresses and want a quick combined view.

Endpoint:

```http
POST /v1/base/addresses/txs
```

Example:

```bash
curl -s "https://indexer.ledgerflow.top/v1/base/addresses/txs" \
  -H "Authorization: Bearer $LEDGERFLOW_API_KEY" \
  -H "content-type: application/json" \
  -d '{
    "addresses": [
      "0x1111111111111111111111111111111111111111",
      "0x2222222222222222222222222222222222222222"
    ],
    "from_block": 30000000,
    "to_block": 32000000,
    "direction": "any",
    "limit": 500
  }'
```

Use batch lookup for:

- small bot clusters
- treasury wallet sets
- quick previews
- one-shot exports

Do not use batch lookup for very large history extraction of high-volume addresses. Use single-address pagination instead.

### 3. Deduplicated Hash Discovery

If your next step only needs tx hashes, not per-address refs, use:

```http
GET /v1/base/address/{address}/tx-hashes
```

Example:

```bash
curl -s "https://indexer.ledgerflow.top/v1/base/address/0x1111111111111111111111111111111111111111/tx-hashes?direction=from&limit=1000" \
  -H "Authorization: Bearer $LEDGERFLOW_API_KEY"
```

This is especially useful when your own downstream system stores txs keyed by `tx_hash`.

### 4. Enrichment After Discovery

Once you have tx hashes, call the convenience RPC endpoints if you do not want a second RPC provider integration:

```http
GET /v1/base/tx/{tx_hash}
GET /v1/base/tx/{tx_hash}/receipt
GET /v1/base/block/latest
GET /v1/base/block/{block_number}
```

Example:

```bash
curl -s "https://indexer.ledgerflow.top/v1/base/tx/0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" \
  -H "Authorization: Bearer $LEDGERFLOW_API_KEY"
```

This is enough to compute things like:

- tx status
- gas used
- effective gas price
- fee paid
- block timestamp
- top-level value sent

## Pagination Guidance

The most important pagination rule is:

```text
Treat next_cursor as opaque.
```

Current behavior:

- `/v1/base/address/{address}/txs` supports cursor pagination
- `/v1/base/address/{address}/tx-hashes` does not currently expose cursor pagination
- batch lookup does not expose cursor pagination

The cursor is currently derived from:

- `block_number`
- `tx_index`
- `direction_rank`

But consumers should not rely on the exact format.

## Query Parameters

Shared query parameters used by address lookup endpoints:

| Name | Type | Meaning |
| --- | --- | --- |
| `from_block` | integer | inclusive lower block bound |
| `to_block` | integer | inclusive upper block bound |
| `direction` | string | `from`, `to`, or omitted / `any` |
| `limit` | integer | per-request max rows returned, clamped by server config |

Additional for `/txs`:

| Name | Type | Meaning |
| --- | --- | --- |
| `cursor` | string | pagination token returned by previous response |

## Request Limits

Current practical limits:

- default limit: `1000`
- max limit by default config: `10000`
- batch addresses per request: `50`

Consumer guidance:

- use `limit=1000` as a safe default
- increase only when you actually need larger pages
- for large clusters, split into multiple requests
- for large single-address history, use paginated `/txs`

## Recommended Consumer Patterns

### Pattern A: Bot Wallet Spend Discovery

Goal:

```text
Find all txs touching bot EOAs for a time range.
```

Flow:

1. keep your bot EOAs in a cluster list
2. call batch `/v1/base/addresses/txs`
3. deduplicate tx hashes
4. call `/v1/base/tx/{tx_hash}/receipt` for each
5. compute gas spend and failed tx cost in your own analysis system

### Pattern B: User-Initiated Historical Backfill

Goal:

```text
User adds one address and wants its historical txs.
```

Flow:

1. call `/v1/base/address/{address}/txs`
2. keep paginating with `next_cursor`
3. store refs in your own DB
4. asynchronously enrich tx hashes later

### Pattern C: Analysis Tool Integration

Goal:

```text
Use the indexer as the first stage of a larger evidence-first analysis pipeline.
```

Flow:

1. analysis service receives address cluster and time range
2. indexer returns refs or hashes
3. analysis service fetches tx / receipt / optional trace
4. analysis service derives flows, spend summaries, recognizers, reports

This is the recommended long-term usage model.

## Data Interpretation Guidance

Remember what a tx ref means:

```text
It only means the address was the top-level sender or recipient of the transaction.
```

It does not mean:

- the address spent tokens inside the tx
- the address was the final beneficiary
- the tx was a swap, bridge, mint, or liquidation
- the tx belongs to a particular cluster or strategy

Those are interpretation questions for your consumer or analysis system.

## Common Mistakes

### Mistake 1: Treating `/txs` as a full semantic transaction API

Wrong:

```text
This address has a top-level tx ref, so this was definitely a spend / swap / revenue event.
```

Correct:

```text
This address touched this transaction at the top level. I still need tx / receipt / logs / trace or business rules to interpret it.
```

### Mistake 2: Using batch lookup for extremely large exports

Batch lookup is convenient, but it is not a replacement for single-address pagination.

### Mistake 3: Ignoring deduplication

If you query multiple addresses in one cluster, the same transaction may appear multiple times across different addresses. Deduplicate by `tx_hash` before downstream tx-level analysis.

### Mistake 4: Over-trusting `direction`

`direction=to` only means the address was the top-level `to`.

It does not mean:

- the address received ETH
- the address was credited tokens
- the address profited

## Error Handling

Error format:

```json
{
  "error": "invalid address"
}
```

Typical causes:

| Status | Typical cause |
| --- | --- |
| `400` | malformed address, malformed cursor, malformed block number, malformed tx hash |
| `401` | missing or invalid API key |
| `402` | active subscription missing, subscription quota exhausted, or bonus quota exhausted |
| `409` | request shape violates product rules, for example too many batch addresses |
| `500` | ClickHouse, Postgres, or upstream RPC error |

Consumer recommendation:

- retry `500` with backoff
- do not retry `400` without fixing the request
- treat `402` as a product/account state issue
- treat `401` as key/session/auth configuration issue

## Quota-Aware Consumer Design

If you are building a paid internal tool or a customer-facing analytics app on top of the indexer:

- cache results for previously queried addresses or tx hashes
- avoid repeated full backfills for the same address
- store `next_cursor` checkpoints for resumable backfills
- separate discovery jobs from enrichment jobs

Good pattern:

```text
discover once
cache refs
enrich only missing tx hashes
```

Bad pattern:

```text
every dashboard refresh re-downloads the full address history
```

## Best Fit Use Cases

The indexer is strongest for:

- Base bot operators
- wallet operations teams
- protocol treasury monitoring
- address-cluster transaction discovery
- transaction backfill pipelines
- pre-analysis candidate generation

## Not The Best Fit

It is not the right standalone product for:

- full explorer replacement
- protocol-level analytics by itself
- semantic tx classification by itself
- complete wallet portfolio / balance products

## Where To Go Next

If you want to build the next layer above the indexer, build it as a separate consumer or analysis service:

- gas spend reports
- failed tx cost attribution
- cluster-level spend summaries
- token flow analysis
- evidence packs
- opportunity / attempt rollups
- transaction recognizers

The indexer should stay narrow and stable.
