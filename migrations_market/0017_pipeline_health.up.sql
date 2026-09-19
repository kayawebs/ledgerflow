-- Automated answers to "is this data still true?".
--
-- Three silent defects reached production in two days — a sampled block scan
-- presented as a count, a cursor window that skipped most of its input, and
-- dust trades setting prices — and all three were found by hand. Each of them
-- violated an invariant that can be asserted continuously; this table is where
-- those assertions record their verdict.
create table if not exists market.pipeline_health (
    check_key text primary key,
    chain_id bigint not null,
    -- 'ok', 'warn', 'fail', or 'unknown' when the check could not run.
    status text not null,
    -- What the check measured and what it expected, both for display.
    observed double precision,
    threshold double precision,
    detail text,
    -- Set while a check is failing, so a flapping check is visible.
    failing_since timestamptz,
    checked_at timestamptz not null default now()
);

create index if not exists idx_market_pipeline_health_status
    on market.pipeline_health (chain_id, status);
