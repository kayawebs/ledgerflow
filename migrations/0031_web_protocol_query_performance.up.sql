-- no-transaction

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_protocol_deployments_chain_status_updated
    ON protocol_deployments (chain_id, status, updated_at DESC);
