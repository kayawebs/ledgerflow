-- Who actually launched a token, and what their track record looks like.
--
-- The Pons launch event reports `msg.sender` as the creator, which is a
-- contract whenever the caller went through a router or multicall: the most
-- prolific "creator" on the chain is Multicall3. The launch transaction's
-- sender is the account that actually acted, so it is resolved separately and
-- kept beside the event value rather than overwriting it.
create table if not exists market.launch_origins (
    launch_id uuid primary key,
    chain_id bigint not null,
    launch_block bigint not null,
    event_creator text,
    origin_address text,
    resolved_at timestamptz not null default now()
);

create index if not exists idx_market_launch_origins_origin
    on market.launch_origins (chain_id, origin_address)
    where origin_address is not null;
create index if not exists idx_market_launch_origins_block
    on market.launch_origins (chain_id, launch_block);

-- Track record per creator. Rebuilt in full: the aggregates span a creator's
-- whole history, so an incremental update would have to revisit it anyway.
create table if not exists market.creator_profiles (
    chain_id bigint not null,
    creator_address text not null,
    -- 'origin' when the launch transaction sender is known, 'event' when the
    -- profile still rests on the protocol's msg.sender.
    attribution text not null,
    launches integer not null,
    graduated integer not null,
    graduation_rate numeric,
    launches_7d integer not null,
    launches_24h integer not null,
    first_launch_at timestamptz,
    last_launch_at timestamptz,
    best_ath_mcap_usd numeric,
    median_ath_mcap_usd numeric,
    traded_launches integer not null,
    dead_launches integer not null,
    buyers integer not null,
    losing_buyers integer not null,
    buyer_cost_usd numeric,
    buyer_realized_usd numeric,
    computed_at timestamptz not null default now(),
    primary key (chain_id, creator_address)
);

create index if not exists idx_market_creator_profiles_launches
    on market.creator_profiles (chain_id, launches desc);
create index if not exists idx_market_creator_profiles_damage
    on market.creator_profiles (chain_id, buyer_realized_usd asc)
    where buyer_realized_usd is not null;
