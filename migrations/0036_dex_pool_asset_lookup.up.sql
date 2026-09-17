-- no-transaction

-- Pricing resolves candidate pools by either side of the token pair. These
-- partial expression indexes cover only identified DEX pools and avoid a JSON
-- scan for every seed portfolio valuation.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_address_protocol_dex_pool_token0
    ON address_protocol_metadata (
      chain_id, (lower(metadata->>'token0')), confidence DESC, updated_at DESC
    )
    WHERE protocol_family = 'dex' AND contract_role = 'dex_pool';
