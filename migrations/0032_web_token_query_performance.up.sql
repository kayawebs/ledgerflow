-- no-transaction

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_erc20_token_metadata_chain_status_fetched
    ON erc20_token_metadata (chain_id, fetch_status, fetched_at DESC);
