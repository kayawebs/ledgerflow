-- Post-graduation market data. A graduated Pons V2 launch is mapped to its
-- Uniswap V4 pool from the PoolManager Initialize event in its graduation
-- transaction; swaps are then read from local-RPC logs filtered by pool id.

create table if not exists market.token_pools (
    launch_id uuid primary key,
    chain_id bigint not null,
    status text not null check (status in ('mapped', 'unmapped')),
    pool_manager text,
    pool_id text,
    currency0 text,
    currency1 text,
    token_is_currency0 boolean,
    fee integer,
    tick_spacing integer,
    hooks text,
    init_block bigint,
    reason text,
    mapped_at timestamptz not null default now()
);

create unique index if not exists idx_market_token_pools_pool
    on market.token_pools (chain_id, pool_id) where pool_id is not null;
create index if not exists idx_market_token_pools_init
    on market.token_pools (chain_id, init_block) where status = 'mapped';

create table if not exists market.pool_swap_facts (
    chain_id bigint not null,
    tx_hash text not null,
    log_index bigint not null,
    launch_id uuid not null,
    pool_id text not null,
    block_number bigint not null,
    occurred_at timestamptz,
    trade_side text not null check (trade_side in ('buy', 'sell')),
    sender_address text not null,
    token_amount_raw numeric not null,
    quote_amount_raw numeric not null,
    sqrt_price_x96 numeric not null,
    created_at timestamptz not null default now(),
    primary key (chain_id, tx_hash, log_index)
);

create index if not exists idx_market_pool_swap_facts_block
    on market.pool_swap_facts (chain_id, block_number);
create index if not exists idx_market_pool_swap_facts_launch
    on market.pool_swap_facts (launch_id, block_number desc);
