-- Lending liquidations read directly from local-RPC logs. Emitters are
-- accepted only when they answer idToMarketParams(bytes32) for the event's
-- market id, so arbitrary contracts cannot inject liquidation records.

create table if not exists market.lending_markets (
    chain_id bigint not null,
    venue_address text not null,
    market_id text not null,
    protocol text not null,
    loan_token text not null,
    collateral_token text not null,
    oracle text,
    irm text,
    lltv_raw numeric,
    verified_at timestamptz not null default now(),
    primary key (chain_id, venue_address, market_id)
);

create table if not exists market.lending_liquidations (
    chain_id bigint not null,
    tx_hash text not null,
    log_index bigint not null,
    block_number bigint not null,
    block_time timestamptz,
    protocol text not null,
    venue_address text not null,
    market_id text not null,
    liquidator text not null,
    borrower text not null,
    loan_token text not null,
    collateral_token text not null,
    repaid_raw numeric not null,
    seized_raw numeric not null,
    bad_debt_raw numeric not null,
    repaid_amount numeric,
    seized_amount numeric,
    repaid_usd numeric,
    seized_usd numeric,
    bonus_usd numeric,
    status text not null check (status in ('estimated', 'unknown')),
    reason text,
    methodology_version text not null,
    computed_at timestamptz not null default now(),
    primary key (chain_id, tx_hash, log_index)
);

create index if not exists idx_market_lending_liquidations_time
    on market.lending_liquidations (chain_id, block_time desc);
create index if not exists idx_market_lending_liquidations_liquidator
    on market.lending_liquidations (chain_id, liquidator, block_time desc);
