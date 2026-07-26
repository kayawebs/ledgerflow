# Lazy block indexing

## Goal

The indexer should not scan Base history on a cold start. Historical work starts when
an address query asks for a block range. Completed block work is global: once a block
has been indexed, later address queries reuse every address reference extracted from
that block.

This remains a block index, not an address-specific RPC crawler. EVM JSON-RPC cannot
enumerate every top-level transaction for an arbitrary address without inspecting
blocks or using another address index.

## Storage

`indexed_blocks` records successful block completion:

- `chain_id`
- `block_number`
- `block_hash`
- `indexed_at`

A block is recorded only after its address references have been written. Empty blocks
are recorded too, which is required to distinguish "indexed with no hits" from "not
indexed".

`indexing_requests` is a durable range queue:

- deterministic `request_id` for `(chain_id, from_block, to_block)`
- requested address for operator context only
- requested range and next cursor
- `queued`, `running`, or `completed` status
- timestamps and the latest error

Requests may overlap. Before every RPC batch, the worker removes blocks already present
in `indexed_blocks`, so overlapping requests do not repeat completed work.

## Legacy progress

An existing `indexer_progress` row proves that the old worker completely indexed the
continuous range from `LEDGERFLOW_INDEXER_START_BLOCK` through
`last_indexed_block`. Lazy coverage treats that interval as complete without inserting
millions of migration rows into `indexed_blocks`.

Lazy work does not advance the legacy contiguous progress row.

## Query contract

`GET /addresses/{address}/txs` and
`GET /addresses/{address}/tx-hashes` resolve their requested range as:

- `from_block`: query value or `LEDGERFLOW_INDEXER_START_BLOCK`
- `to_block`: query value or the current RPC head, bounded by
  `LEDGERFLOW_INDEXER_END_BLOCK` when configured

If the range is fully covered, the endpoint returns `200 OK`.

If blocks are missing, the endpoint queues the range and returns `202 Accepted`.
The response still contains currently stored results and adds:

```json
{
  "coverage": {
    "from_block": 0,
    "to_block": 123,
    "requested_block_count": 124,
    "indexed_block_count": 20,
    "complete": false,
    "request_id": "..."
  }
}
```

Clients that require complete history must retry after `202`; they must not interpret
the partial result as an address with no transactions.

## Worker modes

`LEDGERFLOW_INDEXER_LAZY_ENABLED=true` is the default. The worker polls the durable
range queue and makes no RPC request while the queue is empty.

`LEDGERFLOW_INDEXER_LAZY_ENABLED=false` retains the old continuous scan and live-follow
behavior for operators that explicitly need it.

## Failure and idempotency

- A failed batch leaves its request cursor unchanged.
- Retrying reference inserts is safe because `address_tx_refs` is a
  `ReplacingMergeTree`.
- A block completion marker is written only after its references.
- Re-querying an incomplete range reuses its deterministic request.
- Completed blocks are removed from every later batch before RPC calls are created.
