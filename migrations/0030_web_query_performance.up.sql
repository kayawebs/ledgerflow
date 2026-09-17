-- no-transaction

-- The Web dashboard filters these large registries by chain and recency/status.
-- Build concurrently so applying the migration does not block the live writers.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_address_labels_chain_updated
    ON address_labels (chain_id, updated_at DESC);
