-- no-transaction

-- The seed-first worker never needs to inspect the multi-million-row generic
-- backlog. Keep this partial index deliberately bounded to the S1 priority
-- band, and build it concurrently so live producers remain writable.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_address_analysis_jobs_seed_claim
    ON address_analysis_jobs (priority ASC, first_seen_at ASC)
    INCLUDE (chain_id, address, status, available_at, claimed_at, updated_at)
    WHERE priority <= 39
      AND status IN ('pending', 'failed', 'running');
