-- Market MVP read models. They live in the `market` schema with their own
-- migration history so binaries built from older code (which run the public
-- migrator at startup) never see unknown migrations. Every table here is
-- derived from LedgerFlow facts and can be rebuilt.

create table if not exists market.projector_checkpoints (
    checkpoint_key text primary key,
    last_position bigint not null default 0,
    last_run_at timestamptz,
    last_success_at timestamptz,
    last_error text,
    metadata jsonb not null default '{}'::jsonb,
    updated_at timestamptz not null default now()
);

-- Per-minute USD price of every quote asset used by market facts. Native ETH
-- (the zero address) is materialized from the chain's wrapped native asset.
create table if not exists market.asset_usd_prices_1m (
    chain_id bigint not null,
    asset_address text not null check (asset_address ~ '^0x[0-9a-f]{40}$'),
    bucket_start timestamptz not null,
    price_usd numeric not null check (price_usd > 0),
    observation_count integer not null check (observation_count > 0),
    min_confidence real not null,
    source text not null,
    updated_at timestamptz not null default now(),
    primary key (chain_id, asset_address, bucket_start)
);

-- One-minute candles over deterministic trade facts. Prices are execution
-- prices in quote-asset units; `quote_usd_price` is the quote asset's USD price
-- in effect for the bucket (null when no price was available).
create table if not exists market.token_candles_1m (
    launch_id uuid not null,
    bucket_start timestamptz not null,
    chain_id bigint not null,
    domain_id uuid not null,
    open_price_quote numeric,
    high_price_quote numeric,
    low_price_quote numeric,
    close_price_quote numeric,
    quote_usd_price numeric,
    buy_quote numeric not null default 0,
    sell_quote numeric not null default 0,
    buy_token numeric not null default 0,
    sell_token numeric not null default 0,
    fee_quote numeric not null default 0,
    curve_reserve_delta_quote numeric not null default 0,
    trade_count integer not null default 0,
    buy_count integer not null default 0,
    sell_count integer not null default 0,
    unique_traders integer not null default 0,
    first_block bigint not null,
    last_block bigint not null,
    methodology_version text not null,
    updated_at timestamptz not null default now(),
    primary key (launch_id, bucket_start)
);

create index if not exists idx_market_token_candles_recent
    on market.token_candles_1m (chain_id, bucket_start desc);
create index if not exists idx_market_token_candles_updated
    on market.token_candles_1m (updated_at);

-- Current market state per launch, read directly by the web.
create table if not exists market.token_stats (
    launch_id uuid primary key,
    chain_id bigint not null,
    domain_id uuid not null,
    token_address text not null,
    symbol text,
    name text,
    creator_address text,
    quote_token_address text,
    quote_symbol text,
    protocol_version text,
    lifecycle_phase text not null,
    launched_at timestamptz,
    graduated_at timestamptz,
    total_supply numeric,
    total_supply_status text not null default 'pending'
        check (total_supply_status in ('pending','ready','error')),
    price_quote numeric,
    quote_usd_price numeric,
    price_usd numeric,
    market_cap_usd numeric,
    change_5m_pct numeric,
    change_1h_pct numeric,
    change_6h_pct numeric,
    change_24h_pct numeric,
    volume_5m_usd numeric not null default 0,
    volume_1h_usd numeric not null default 0,
    volume_6h_usd numeric not null default 0,
    volume_24h_usd numeric not null default 0,
    buy_volume_24h_usd numeric not null default 0,
    sell_volume_24h_usd numeric not null default 0,
    net_flow_1h_usd numeric not null default 0,
    net_flow_24h_usd numeric not null default 0,
    trades_5m integer not null default 0,
    trades_1h integer not null default 0,
    trades_24h integer not null default 0,
    buys_24h integer not null default 0,
    sells_24h integer not null default 0,
    buyers_24h integer,
    sellers_24h integer,
    holders_estimate integer,
    curve_reserve_quote numeric,
    graduation_threshold_quote numeric,
    bonding_progress_pct numeric,
    ath_price_quote numeric,
    ath_market_cap_usd numeric,
    sparkline_24h numeric[] not null default array[]::numeric[],
    first_trade_at timestamptz,
    last_trade_at timestamptz,
    hot_score double precision not null default 0,
    usd_status text not null default 'unpriced'
        check (usd_status in ('priced','stale','unpriced')),
    methodology_version text not null,
    refreshed_at timestamptz not null default now()
);

create index if not exists idx_market_token_stats_hot
    on market.token_stats (chain_id, hot_score desc) where trades_24h > 0;
create index if not exists idx_market_token_stats_volume
    on market.token_stats (chain_id, volume_24h_usd desc) where trades_24h > 0;
create index if not exists idx_market_token_stats_change
    on market.token_stats (chain_id, change_24h_pct desc nulls last) where trades_24h > 0;
create index if not exists idx_market_token_stats_new
    on market.token_stats (chain_id, launched_at desc);
create index if not exists idx_market_token_stats_graduating
    on market.token_stats (chain_id, bonding_progress_pct desc nulls last)
    where lifecycle_phase = 'curve_trading';
create index if not exists idx_market_token_stats_graduated
    on market.token_stats (chain_id, graduated_at desc nulls last)
    where graduated_at is not null;
create index if not exists idx_market_token_stats_mcap
    on market.token_stats (chain_id, market_cap_usd desc nulls last);
create index if not exists idx_market_token_stats_symbol
    on market.token_stats (chain_id, lower(symbol));
create index if not exists idx_market_token_stats_token
    on market.token_stats (chain_id, token_address);
create index if not exists idx_market_token_stats_decay
    on market.token_stats (refreshed_at) where trades_24h > 0;
