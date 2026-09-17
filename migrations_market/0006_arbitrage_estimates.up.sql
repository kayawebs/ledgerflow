-- Estimated arbitrage outcomes. Each detected candidate's receipt is read from
-- the local RPC; ERC-20 Transfer deltas are netted over the executing scope
-- (transaction sender + entry contract), valued in USD and reduced by gas.
-- Deterministic verification results remain in public.economic_verification_results.

create table if not exists market.arbitrage_estimates (
    chain_id bigint not null,
    tx_hash text not null,
    block_number bigint not null,
    block_time timestamptz,
    sender_address text,
    entry_contract text,
    bot_address text not null,
    swap_count integer not null default 0,
    pool_count integer not null default 0,
    tx_status text not null check (tx_status in ('success', 'reverted', 'missing')),
    token_count integer not null default 0,
    gas_native numeric,
    gas_usd numeric,
    gross_usd numeric,
    net_usd numeric,
    status text not null check (status in ('estimated', 'unknown')),
    reason text,
    is_verified boolean not null default false,
    methodology_version text not null,
    computed_at timestamptz not null default now(),
    primary key (chain_id, tx_hash)
);

create index if not exists idx_market_arbitrage_estimates_time
    on market.arbitrage_estimates (chain_id, block_time desc);
create index if not exists idx_market_arbitrage_estimates_bot
    on market.arbitrage_estimates (chain_id, bot_address, block_time desc);
create index if not exists idx_market_arbitrage_estimates_profit
    on market.arbitrage_estimates (chain_id, net_usd desc nulls last)
    where status = 'estimated';

create table if not exists market.arbitrage_estimate_deltas (
    chain_id bigint not null,
    tx_hash text not null,
    token_address text not null,
    amount_raw numeric not null,
    amount numeric,
    usd numeric,
    primary key (chain_id, tx_hash, token_address)
);

create table if not exists market.arbitrage_bot_leaderboard (
    chain_id bigint not null,
    window_key text not null check (window_key in ('24h', '7d', '30d')),
    bot_address text not null,
    rank integer not null,
    address_kind text,
    net_usd numeric not null,
    gross_usd numeric not null,
    gas_usd numeric not null,
    executions integer not null,
    profitable_executions integer not null,
    unknown_executions integer not null,
    senders integer not null,
    verified_executions integer not null,
    best_tx_hash text,
    best_net_usd numeric,
    last_execution_at timestamptz,
    cluster_id uuid,
    methodology_version text not null,
    computed_at timestamptz not null default now(),
    primary key (chain_id, window_key, bot_address)
);

create index if not exists idx_market_arbitrage_bot_leaderboard_rank
    on market.arbitrage_bot_leaderboard (chain_id, window_key, rank);
