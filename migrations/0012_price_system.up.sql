create table if not exists price_observations (
    observation_id uuid primary key,
    chain_id bigint not null,
    token_address text not null,
    quote_token text not null,
    quote_symbol text not null,
    block_number bigint,
    block_timestamp timestamptz,
    block_bucket bigint,
    price_decimal numeric not null,
    source text not null,
    confidence real not null,
    pool_address text,
    tx_hash text,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now()
);

create index if not exists idx_price_observations_lookup
    on price_observations (chain_id, token_address, quote_symbol, block_bucket, confidence desc);

create index if not exists idx_price_observations_tx
    on price_observations (chain_id, tx_hash);

create table if not exists token_price_cache (
    chain_id bigint not null,
    token_address text not null,
    quote_symbol text not null,
    block_bucket bigint not null,
    price_decimal numeric not null,
    confidence real not null,
    source text not null,
    observed_block bigint,
    observed_at timestamptz,
    expires_at timestamptz,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (chain_id, token_address, quote_symbol, block_bucket)
);

create index if not exists idx_token_price_cache_lookup
    on token_price_cache (chain_id, token_address, quote_symbol, block_bucket, confidence desc);

create index if not exists idx_token_price_cache_expiry
    on token_price_cache (expires_at);

create table if not exists price_requests (
    request_id uuid primary key,
    chain_id bigint not null,
    token_address text not null,
    quote_symbol text not null,
    target_block bigint,
    block_bucket bigint,
    target_timestamp timestamptz,
    priority integer not null,
    reason text not null,
    status text not null default 'pending',
    attempts integer not null default 0,
    last_error text,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create unique index if not exists idx_price_requests_pending_dedupe
    on price_requests (chain_id, token_address, quote_symbol, block_bucket)
    where status in ('pending', 'processing');

create index if not exists idx_price_requests_queue
    on price_requests (status, priority asc, created_at asc);
