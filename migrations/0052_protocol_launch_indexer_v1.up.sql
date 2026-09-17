alter table protocol_seed_addresses
    drop constraint if exists protocol_seed_addresses_role;
alter table protocol_seed_addresses
    add constraint protocol_seed_addresses_role check (
        seed_role in (
            'factory', 'launch_factory', 'router', 'pool_manager', 'position_manager',
            'locker', 'quoter', 'quote_asset', 'fee_recipient', 'oracle', 'lending_pool'
        )
    );

alter table protocol_token_launches
    add column if not exists protocol_version text;
alter table protocol_token_launches
    add column if not exists curve_address text;
alter table protocol_token_launches
    add column if not exists lifecycle_phase text not null default 'unknown';
alter table protocol_token_launches
    add column if not exists launch_config_id text;
alter table protocol_token_launches
    add column if not exists graduation_threshold_raw text;
alter table protocol_token_launches
    add column if not exists graduation_tx_hash text;
alter table protocol_token_launches
    add column if not exists graduation_block bigint;
alter table protocol_token_launches
    add column if not exists graduated_at timestamptz;
alter table protocol_token_launches
    add column if not exists position_id text;
alter table protocol_token_launches
    add column if not exists pool_id text;
alter table protocol_token_launches
    add column if not exists last_lifecycle_block bigint;
alter table protocol_token_launches
    add column if not exists data_source text not null default 'ledgerflow_rpc';
alter table protocol_token_launches
    add column if not exists last_indexed_at timestamptz;

alter table protocol_token_launches
    drop constraint if exists protocol_token_launches_curve;
alter table protocol_token_launches
    add constraint protocol_token_launches_curve check (
        curve_address is null or
        (curve_address = lower(curve_address) and curve_address ~ '^0x[0-9a-f]{40}$')
    );

alter table protocol_token_launches
    drop constraint if exists protocol_token_launches_graduation_tx;
alter table protocol_token_launches
    add constraint protocol_token_launches_graduation_tx check (
        graduation_tx_hash is null or
        (graduation_tx_hash = lower(graduation_tx_hash)
         and graduation_tx_hash ~ '^0x[0-9a-f]{64}$')
    );

create index if not exists idx_protocol_token_launches_lifecycle
    on protocol_token_launches
        (chain_id, domain_id, lifecycle_phase, last_lifecycle_block desc);

create table if not exists protocol_launch_lifecycle_events (
    lifecycle_event_id uuid primary key default gen_random_uuid(),
    domain_id uuid not null,
    chain_id bigint not null,
    protocol_version text not null,
    token_address text not null,
    curve_address text,
    event_type text not null,
    block_number bigint not null,
    block_hash text not null,
    tx_hash text not null,
    tx_index bigint not null,
    log_index bigint not null,
    occurred_at timestamptz,
    data_source text not null default 'ledgerflow_rpc',
    is_canonical boolean not null default true,
    facts jsonb not null default '{}'::jsonb,
    evidence jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    foreign key (domain_id, chain_id)
        references protocol_intelligence_domains(domain_id, chain_id) on delete cascade,
    unique (chain_id, tx_hash, log_index, event_type),
    constraint protocol_launch_lifecycle_token check (
        token_address = lower(token_address) and token_address ~ '^0x[0-9a-f]{40}$'
    ),
    constraint protocol_launch_lifecycle_curve check (
        curve_address is null or
        (curve_address = lower(curve_address) and curve_address ~ '^0x[0-9a-f]{40}$')
    ),
    constraint protocol_launch_lifecycle_block_hash check (
        block_hash = lower(block_hash) and block_hash ~ '^0x[0-9a-f]{64}$'
    ),
    constraint protocol_launch_lifecycle_tx_hash check (
        tx_hash = lower(tx_hash) and tx_hash ~ '^0x[0-9a-f]{64}$'
    ),
    constraint protocol_launch_lifecycle_event_type check (
        event_type in ('token_launched', 'launch_swept', 'pool_graduated')
    )
);

create index if not exists idx_protocol_launch_lifecycle_recent
    on protocol_launch_lifecycle_events
        (chain_id, domain_id, block_number desc, tx_index desc, log_index desc)
    where is_canonical;

comment on table protocol_launch_lifecycle_events is
    'LedgerFlow-owned deterministic launch/graduation projection. Searcher market facts are not authoritative for protocol lifecycle.';
