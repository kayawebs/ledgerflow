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
