-- Per-wallet launch positions and ranked earnings. Positions aggregate exact
-- curve trade facts; PnL is derived at ranking time from the current token
-- price with average-cost accounting and is always labelled as an estimate.

create table if not exists market.wallet_positions (
    launch_id uuid not null,
    address text not null check (address ~ '^0x[0-9a-f]{40}$'),
    chain_id bigint not null,
    domain_id uuid not null,
    creator_address text,
    bought_token numeric not null default 0,
    sold_token numeric not null default 0,
    cost_quote numeric not null default 0,
    proceeds_quote numeric not null default 0,
    -- Null when any contributing trade could not be priced in USD.
    cost_usd numeric,
    proceeds_usd numeric,
    buy_count integer not null default 0,
    sell_count integer not null default 0,
    first_trade_at timestamptz,
    last_trade_at timestamptz,
    last_block bigint not null,
    updated_at timestamptz not null default now(),
    primary key (launch_id, address)
);

create index if not exists idx_market_wallet_positions_address
    on market.wallet_positions (chain_id, address, last_trade_at desc);
create index if not exists idx_market_wallet_positions_recent
    on market.wallet_positions (chain_id, last_trade_at desc);
create index if not exists idx_market_wallet_positions_creator
    on market.wallet_positions (chain_id, creator_address, last_trade_at desc);

create table if not exists market.wallet_leaderboard (
    chain_id bigint not null,
    window_key text not null check (window_key in ('24h','7d','30d')),
    address text not null,
    rank integer not null,
    address_kind text,
    pnl_usd numeric not null,
    realized_usd numeric not null,
    unrealized_usd numeric not null,
    cost_usd numeric not null,
    tokens_traded integer not null,
    winning_tokens integer not null,
    trades integer not null,
    best_launch_id uuid,
    best_symbol text,
    best_pnl_usd numeric,
    best_cost_usd numeric,
    best_multiple numeric,
    last_trade_at timestamptz,
    cluster_count integer not null default 0,
    methodology_version text not null,
    computed_at timestamptz not null default now(),
    primary key (chain_id, window_key, address)
);

create index if not exists idx_market_wallet_leaderboard_rank
    on market.wallet_leaderboard (chain_id, window_key, rank);

create table if not exists market.cluster_leaderboard (
    chain_id bigint not null,
    window_key text not null check (window_key in ('24h','7d','30d')),
    cluster_id uuid not null,
    rank integer not null,
    domain_key text not null,
    cluster_kind text not null,
    subject_address text,
    subject_symbol text,
    member_count integer not null,
    active_members integer not null,
    pnl_usd numeric not null,
    realized_usd numeric not null,
    unrealized_usd numeric not null,
    cost_usd numeric not null,
    tokens_traded integer not null,
    winning_members integer not null,
    best_launch_id uuid,
    best_symbol text,
    best_pnl_usd numeric,
    last_trade_at timestamptz,
    first_seen_at timestamptz,
    methodology_version text not null,
    computed_at timestamptz not null default now(),
    primary key (chain_id, window_key, cluster_id)
);

create index if not exists idx_market_cluster_leaderboard_rank
    on market.cluster_leaderboard (chain_id, window_key, rank);
create index if not exists idx_market_cluster_leaderboard_new
    on market.cluster_leaderboard (chain_id, window_key, first_seen_at desc);
