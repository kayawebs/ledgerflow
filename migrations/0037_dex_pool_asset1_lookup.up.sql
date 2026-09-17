-- no-transaction

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_address_protocol_dex_pool_token1
    ON address_protocol_metadata (
      chain_id, (lower(metadata->>'token1')), confidence DESC, updated_at DESC
    )
    WHERE protocol_family = 'dex' AND contract_role = 'dex_pool';
