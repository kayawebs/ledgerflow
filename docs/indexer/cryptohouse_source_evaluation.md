# CryptoHouse as an indexer source

Date: 2026-07-10

## Recommendation

Use CryptoHouse as an acceleration and enrichment source, not as the only production source of truth for LedgerFlow indexing.

It is a good fit for:

- live arbitrage/log monitoring where a small delay is acceptable;
- hot pool and hot route aggregation;
- backfill acceleration for address refs, logs, receipts, and candidate discovery;
- cross-checking local RPC/indexer results.

It is not a good fit as the sole indexer backend unless LedgerFlow accepts public-service dependency risk:

- public endpoint availability and rate limits are outside our control;
- table schemas and query limits may change;
- no local replay guarantees or SLA are implied by the public endpoint;
- correctness-sensitive accounting still needs deterministic local verification.

## Practical design

Keep the current local indexer path as canonical:

1. local RPC or node-backed ingestion writes compact address refs and local state;
2. CryptoHouse-backed jobs can fill large historical ranges faster;
3. every imported range records source, query window, lag, row counts, and verification status;
4. sampled ranges are reconciled against local RPC or local ClickHouse before promotion.

For the Base live arbitrage monitor, CryptoHouse is already appropriate because the monitor writes candidate labels only. The candidate is later confirmable through LedgerFlow transaction analysis when profit, gas, and asset deltas matter.

## If we use it for backfill

Add a separate worker/source mode instead of replacing the existing indexer:

- `LEDGERFLOW_INDEXER_SOURCE=rpc|cryptohouse|hybrid`
- `cryptohouse` reads bounded block windows from `base.transactions` and `base.logs`;
- `hybrid` uses CryptoHouse for historical ranges and local RPC for recent/final verification;
- imported rows carry `source='cryptohouse'`;
- range checkpoints must be independent from RPC indexer checkpoints.

This gives us speed without making public infrastructure a hard dependency for every product path.

## Implemented path

The first production path is `ledgerflow_cryptohouse_indexer_backfill`.

It imports `base.transactions` into the existing local ClickHouse `address_tx_refs` table and records progress under a separate worker id. It does not replace the normal RPC indexer and it does not update the canonical RPC worker checkpoint.

Operational controls:

| Env var | Purpose |
| --- | --- |
| `LEDGERFLOW_CRYPTOHOUSE_INDEXER_BACKFILL_ENABLED` | must be `true` before the worker imports rows |
| `LEDGERFLOW_CRYPTOHOUSE_INDEXER_BACKFILL_START_BLOCK` | lower bound for historical fill |
| `LEDGERFLOW_CRYPTOHOUSE_INDEXER_BACKFILL_END_BLOCK` | upper bound; required for bounded runs |
| `LEDGERFLOW_CRYPTOHOUSE_INDEXER_BACKFILL_BATCH_SIZE` | block window per CryptoHouse query, default `500` |
| `LEDGERFLOW_CRYPTOHOUSE_INDEXER_BACKFILL_REQUEST_DELAY_MS` | delay between batches, default `100` |
| `LEDGERFLOW_CRYPTOHOUSE_INDEXER_BACKFILL_QUOTA_BACKOFF_SECS` | retry backoff after public endpoint quota errors, default `600` |
| `LEDGERFLOW_CRYPTOHOUSE_INDEXER_BACKFILL_WORKER_ID` | independent checkpoint id |

The imported shape is intentionally limited to top-level transaction `from`/`to` address refs, because that is the current indexer contract. Logs, receipts, and token-accounting evidence should be added as separate tables before promoting CryptoHouse to a broader historical evidence source.

The public `crypto` user is useful for validation but has a small hourly query quota. Full historical completion should use a dedicated ClickHouse/CryptoHouse credential, a larger batch size proven by load tests, or a bulk export path.
