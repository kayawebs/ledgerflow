-- Registry of assets that need an hourly USD price series. Launch quote assets
-- and tokens seen in arbitrage deltas register here; new assets get a bounded
-- historical backfill before joining the incremental price rollup.
create table if not exists market.price_assets (
    chain_id bigint not null,
    asset_address text not null check (asset_address ~ '^0x[0-9a-f]{40}$'),
    source text not null,
    backfilled boolean not null default false,
    registered_at timestamptz not null default now(),
    backfilled_at timestamptz,
    primary key (chain_id, asset_address)
);

create index if not exists idx_market_price_assets_pending
    on market.price_assets (chain_id, registered_at) where not backfilled;
