# CryptoHouse as an indexer source

Date: 2026-07-10

## Recommendation

Use CryptoHouse as an acceleration and enrichment source, not as the only production source of truth for LedgerFlow indexing.

It is a good fit for:

- ad-hoc swap and pool aggregation over bounded block windows;
- hot pool and hot route aggregation;
- cross-checking local RPC/indexer results.

It is not a good fit as the sole indexer backend unless LedgerFlow accepts public-service dependency risk:

- public endpoint availability and rate limits are outside our control;
- the public `crypto` user currently has small query and result-size quotas;
- table schemas and query limits may change;
- no local replay guarantees or SLA are implied by the public endpoint;
- correctness-sensitive accounting still needs deterministic local verification.

## Practical design

Keep the current local indexer path as canonical:

1. local RPC or node-backed ingestion writes compact address refs and local state;
2. CryptoHouse-backed jobs can fill large historical ranges faster;
3. every imported range records source, query window, lag, row counts, and verification status;
4. sampled ranges are reconciled against local RPC or local ClickHouse before promotion.

For the Base live arbitrage monitor, the default production path is local `rpc_logs` through the RPC gateway. CryptoHouse can still be useful for bounded manual queries, but the public endpoint should not be the live monitor's primary source while it has tight hourly quotas.

## If we use it for backfill

Add a separate worker/source mode instead of replacing the existing indexer:

- `LEDGERFLOW_INDEXER_SOURCE=rpc|cryptohouse|hybrid`
- `cryptohouse` reads bounded block windows from `base.transactions` and `base.logs`;
- `hybrid` uses CryptoHouse for historical ranges and local RPC for recent/final verification;
- imported rows carry `source='cryptohouse'`;
- range checkpoints must be independent from RPC indexer checkpoints.

This gives us speed without making public infrastructure a hard dependency for every product path. The public endpoint should not be used for full historical indexer completion; that path needs a dedicated ClickHouse/CryptoHouse credential or a bulk export source first.
