# LedgerFlow Indexer Project

This project context is for the standalone LedgerFlow indexer only.

## Current Scope

The indexer is a producer of top-level address transaction references. It scans Base blocks, extracts only top-level transaction `from` and `to` address hits, and exposes address-to-transaction lookup APIs.

Indexer-owned areas:

- `src/indexer/**`
- `src/bin/ledgerflow_indexer.rs`
- ClickHouse schema for `address_tx_refs` and `indexer_progress`
- indexer worker progress, retry, concurrency, and RPC fetch behavior
- indexer API endpoints:
  - `GET /health`
  - `GET /progress`
  - `GET /addresses/{address}/txs`
  - `GET /addresses/{address}/tx-hashes`
- Docker/runtime settings prefixed with `LEDGERFLOW_INDEXER_`

## Explicit Non-Scope

The indexer must not become the transaction analysis system.

Do not add these responsibilities to the indexer:

- intent recognition
- evidence pack generation
- ERC20/native flow derivation beyond top-level tx references
- cluster spend calculation
- opportunity/attempt accounting
- recognizer execution
- PostgreSQL analysis persistence

Those belong to the analysis tool.

## Producer Contract

The indexer produces address transaction references with this stable meaning:

- `chain_id`: chain identifier, currently Base mainnet `8453`
- `address`: normalized lowercase EVM address
- `tx_hash`: lowercase transaction hash
- `block_number`: block containing the transaction
- `tx_index`: top-level transaction index in the block
- `direction`: whether the queried address appeared as top-level `from` or `to`

Consumers should treat the indexer as an HTTP service, not as a library or database.

## Consumer Boundary

The analysis tool may consume:

- deduplicated tx hashes for an address
- full reference rows for backfill pagination
- indexer progress for operator visibility

The analysis tool should not:

- query ClickHouse directly
- import indexer Rust modules
- depend on indexer worker internals
- mutate indexer progress state

## Operational Goal

The indexer optimizes one primitive:

Given an address and optional block range, return the top-level transactions that touched that address.

This enables fast cluster backfill without rescanning historical blocks for every new user or address set.

## Engineering Rules

- Keep schema narrow.
- Prefer append-friendly storage.
- Keep address lookup fast.
- Keep retries idempotent.
- Make progress observable.
- Avoid adding semantic assumptions.
- Commit indexer changes separately from analysis-tool changes.

## Near-Term Plan

Future indexer work should happen in a separate thread from analysis-tool work.

Useful future improvements:

- stronger retry handling for transient gateway/node failures
- range pagination for large address histories
- optional cursor-based query API
- progress and throughput metrics
- documented service-level contract for analysis consumers
- operational guidance for safe nginx/RPC-node deployment
