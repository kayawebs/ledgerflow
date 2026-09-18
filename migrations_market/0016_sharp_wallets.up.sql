-- Wallets that have closed several large wins recently. The set is small and
-- rebuilt in full, so the live "what did they just buy" feed can be a cheap
-- lateral join against the trader index rather than a scan.
create table if not exists market.sharp_wallets (
    chain_id bigint not null,
    address text not null,
    rank integer not null,
    big_wins integer not null,
    wins integer not null,
    positions integer not null,
    realized_usd numeric,
    best_multiple numeric,
    last_win_at timestamptz,
    computed_at timestamptz not null default now(),
    primary key (chain_id, address)
);

create index if not exists idx_market_sharp_wallets_rank
    on market.sharp_wallets (chain_id, rank);
