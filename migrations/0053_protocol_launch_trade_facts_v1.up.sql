alter table protocol_token_launches
    add column if not exists metadata_status text not null default 'pending';
alter table protocol_token_launches
    add column if not exists metadata_attempted_at timestamptz;
alter table protocol_token_launches
    add column if not exists metadata_error text;

alter table protocol_token_launches
    drop constraint if exists protocol_token_launches_metadata_status;
alter table protocol_token_launches
    add constraint protocol_token_launches_metadata_status check (
        metadata_status in ('pending', 'ready', 'error')
    );

create index if not exists idx_protocol_token_launches_metadata_pending
    on protocol_token_launches (chain_id, metadata_status, metadata_attempted_at, launch_block)
    where metadata_status <> 'ready';

create table if not exists protocol_launch_trade_facts (
    trade_fact_id uuid primary key default gen_random_uuid(),
    launch_id uuid not null,
    domain_id uuid not null,
    chain_id bigint not null,
    protocol_version text not null,
    lifecycle_phase text not null,
    curve_address text not null,
    token_address text not null,
    trader_address text not null,
    recipient_address text not null,
    trade_side text not null,
    quote_amount_raw text not null,
    token_amount_raw text not null,
    fee_amount_raw text not null,
    tax_amount_raw text not null,
    block_number bigint not null,
    block_hash text not null,
    tx_hash text not null,
    tx_index bigint not null,
    log_index bigint not null,
    occurred_at timestamptz,
    data_source text not null default 'ledgerflow_rpc',
    is_canonical boolean not null default true,
    evidence jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    foreign key (launch_id, domain_id, chain_id)
        references protocol_token_launches(launch_id, domain_id, chain_id) on delete cascade,
    unique (chain_id, tx_hash, log_index),
    constraint protocol_launch_trade_curve check (
        curve_address = lower(curve_address) and curve_address ~ '^0x[0-9a-f]{40}$'
    ),
    constraint protocol_launch_trade_token check (
        token_address = lower(token_address) and token_address ~ '^0x[0-9a-f]{40}$'
    ),
    constraint protocol_launch_trade_trader check (
        trader_address = lower(trader_address) and trader_address ~ '^0x[0-9a-f]{40}$'
    ),
    constraint protocol_launch_trade_recipient check (
        recipient_address = lower(recipient_address) and recipient_address ~ '^0x[0-9a-f]{40}$'
    ),
    constraint protocol_launch_trade_side check (trade_side in ('buy', 'sell')),
    constraint protocol_launch_trade_raw_amounts check (
        quote_amount_raw ~ '^[0-9]+$' and token_amount_raw ~ '^[0-9]+$'
        and fee_amount_raw ~ '^[0-9]+$' and tax_amount_raw ~ '^[0-9]+$'
    ),
    constraint protocol_launch_trade_block_hash check (
        block_hash = lower(block_hash) and block_hash ~ '^0x[0-9a-f]{64}$'
    ),
    constraint protocol_launch_trade_tx_hash check (
        tx_hash = lower(tx_hash) and tx_hash ~ '^0x[0-9a-f]{64}$'
    )
);

create index if not exists idx_protocol_launch_trade_facts_launch
    on protocol_launch_trade_facts
        (launch_id, block_number desc, tx_index desc, log_index desc)
    where is_canonical;

create index if not exists idx_protocol_launch_trade_facts_trader
    on protocol_launch_trade_facts
        (chain_id, domain_id, trader_address, block_number desc)
    where is_canonical;

comment on table protocol_launch_trade_facts is
    'LedgerFlow-direct Pons curve trades decoded from RPC logs. Searcher is not the source for launch-curve facts.';
