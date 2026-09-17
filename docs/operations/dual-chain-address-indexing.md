# Dual-chain address intelligence

LedgerFlow runs Base (`8453`) and Robinhood Chain (`4663`) as independent data
planes. Each plane owns its RPC gateway, live indexer, ClickHouse database,
PostgreSQL database, checkpoints, queues and workers. The web/control plane
selects the correct API and indexer from the chain registry; a worker must never
read one chain while writing the other chain id.

## Observe path

1. The workspace stores the observed object and requests its chain-specific
   `/addresses/summary` endpoint.
2. The API creates or refreshes an address job at priority 5. Cold-start seed
   jobs have lower scheduling priority. Job claiming is filtered by the
   worker's configured chain id before the lease is acquired; a Robinhood
   worker cannot consume a Base job, even from a shared control database.
3. The address worker uses the live indexer's last sealed block as its upper
   boundary. Direct sender/recipient transactions are read from indexed address
   references.
4. Transfer-only activity is located with sequential, topic-filtered
   `eth_getLogs` calls over at most the latest 50,000 blocks. The search walks
   newest-first in 5,000-block chunks and stops after a bounded evidence sample.
5. At most 32 newest unique transactions are analyzed. Amounts, status, gas,
   flows and protocol evidence still come from transaction, receipt, logs and
   trace; the activity locator makes no economic classification.
6. Analyses are associated with the observed object when it appears in a
   deterministic derived flow, even if it is not the outer transaction sender
   or recipient.

This is lazy activity discovery, not historical block replay. Current blocks
remain the responsibility of the live indexer. A missing unbounded range must
never be interpreted as block zero through head.

## Resource boundaries

- Base address worker concurrency: `1`.
- Observe/API job priority ceiling: `10`.
- Recent lookback: `50,000` blocks.
- Analysis cap: `32` transactions per run.
- RPC log queries: sequential, `5,000` blocks per request, newest first.
- Web indexer queries always include explicit `from_block` and `to_block`.
- Lazy indexer requests are capped at `50,000` blocks.
- The default `/progress` response reads checkpoints and lag only. Exact table
  counts over the large Base ClickHouse datasets are opt-in diagnostics and are
  not part of the Web request path.

The broad `address-producer` remains suitable for Robinhood chain-wide seed
discovery. It is intentionally not part of the default Base startup set because
materializing all Base event-address observations would create millions of
PostgreSQL rows without improving a newly added observed object as quickly as
the bounded, address-targeted path.

## Coverage limitation

Standard RPC can locate indexed-topic activity such as ERC-20 Transfers without
scanning every block. It cannot enumerate arbitrary historical EOA transactions
that are neither direct endpoints nor indexed log participants. Those remain
`Unknown` outside the bounded indexed window unless a separately authorized
archive/indexing source is added.
