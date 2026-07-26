# Schema Rationale

## Tables

### `transactions`

Stable transaction identity and raw transaction payload. `chain_id + tx_hash` is the primary key.

### `transaction_evidence`

Stores fetched receipt, trace, code, and future evidence objects as JSONB with stable `kind` and `reference`.

### `transaction_analysis`

Stores replay-critical analysis output:

- version metadata
- input digest
- normalized facts
- scenario outputs
- accounting outputs
- unresolved items
- validation results
- full payload snapshot

### `recognizer_results`

One row per recognizer result so recognizer output can be audited independently as rules evolve.

### `flows`

Normalized asset movement facts with stable queryable columns and JSONB payload extension space.

### `evidence_packs`

Replayable export artifacts keyed by `pack_id`, preserving facts, labels, flows, unresolved items, and validations.

### `tasks`, `opportunities`, `attempts`, `rollups`

Forward scaffolding for higher-level grouping and rollup logic. These remain intentionally light in v0.

### `address_clusters`, `cluster_addresses`, `cluster_spend_summaries`

Cluster-facing money-ops scaffolding:

- `address_clusters`
  Stores the operator-facing cluster identity such as owner label and cluster name.
- `cluster_addresses`
  Stores normalized addresses that belong to the cluster, with optional label and role.
- `cluster_spend_summaries`
  Reserved for cached summary snapshots. The current starter computes summaries on demand, but the table exists so periodic materialization can be added without reshaping the schema.

`transaction_analysis` also stores explicit `block_timestamp`, `from_address`, `to_address`, and `tx_status` columns so cluster summaries can filter and aggregate efficiently without depending on ad hoc JSONB extraction.

## JSONB usage

JSONB is reserved for fields with real extension pressure:

- raw evidence payloads
- normalized fact payloads
- recognizer attributes
- unresolved items
- evidence packs
- rollup payloads

Stable filter and join fields remain explicit SQL columns.

## ClickHouse indexer tables

The standalone `ledgerflow indexer` keeps its own narrow ClickHouse storage because the access pattern is append-heavy and address-centric.

### `address_tx_refs`

Stores one top-level address reference row per `from` or `to` hit:

- `chain_id`
- `address`
- `tx_hash`
- `block_number`
- `tx_index`
- `direction`

This table is optimized for `address -> tx hashes` lookups rather than full transaction analysis.

### `indexer_progress`

Stores the last fully indexed block per worker id so backfills and live follow can resume deterministically.

In lazy mode this remains legacy contiguous coverage and is not advanced by sparse
query-driven indexing.

### `indexed_blocks`

Stores one global completion marker per successfully indexed block, including empty
blocks. Address queries share these markers, so overlapping requests only fetch blocks
that have not already been completed.

### `indexing_requests`

Stores durable lazy range requests and their next-block cursor. Requests are keyed by a
deterministic chain/range id; the requested address is retained only for operator
context because the indexed block output is shared by every address.
