insert into intelligence_sources (
    source_key, display_name, source_kind, trust_score, owner, homepage, metadata
)
values
    ('onchain_contract_discovery', 'Canonical factory event discovery', 'onchain', 0.90,
     'LedgerFlow', null, '{"method":"canonical_factory_events_v1"}'::jsonb),
    ('robinhood_stock_token_events', 'Robinhood Stock Token on-chain events', 'onchain', 0.99,
     'Robinhood', 'https://eips.ethereum.org/EIPS/eip-8056',
     '{"chain_id":4663,"standard":"ERC-8056"}'::jsonb)
on conflict (source_key) do update set
    display_name = excluded.display_name,
    source_kind = excluded.source_kind,
    trust_score = excluded.trust_score,
    owner = excluded.owner,
    homepage = excluded.homepage,
    status = 'active',
    metadata = intelligence_sources.metadata || excluded.metadata,
    updated_at = now();

create table if not exists onchain_event_cursors (
    worker_id text primary key,
    chain_id bigint not null,
    source_table text not null,
    last_processed_block bigint,
    last_processed_block_hash text,
    scan_from_block bigint,
    scan_to_block bigint,
    lookback_blocks bigint not null default 128,
    status text not null default 'idle',
    retry_count integer not null default 0,
    last_error text,
    last_event_count bigint not null default 0,
    last_success_at timestamptz,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    constraint onchain_event_cursors_status check (status in ('idle', 'running', 'error')),
    constraint onchain_event_cursors_range check (
        scan_from_block is null or scan_to_block is null or scan_from_block <= scan_to_block
    )
);

create index if not exists idx_onchain_event_cursors_chain
    on onchain_event_cursors (chain_id, updated_at desc);

create table if not exists contract_discovery_candidates (
    candidate_id uuid primary key default gen_random_uuid(),
    chain_id bigint not null,
    address text not null,
    candidate_kind text not null,
    discovery_method text not null,
    event_name text not null,
    event_signature text not null,
    source_address text not null,
    source_key text not null references intelligence_sources (source_key),
    block_number bigint not null,
    block_hash text not null,
    tx_hash text not null,
    tx_index bigint not null,
    log_index bigint not null,
    protocol_family text,
    contract_role text,
    token0_address text,
    token1_address text,
    fee integer,
    tick_spacing integer,
    stable boolean,
    status text not null,
    confidence double precision not null,
    validation_error text,
    code_hash text,
    dataset_key text,
    dataset_version text,
    is_canonical boolean not null default true,
    evidence jsonb not null default '{}'::jsonb,
    first_seen_at timestamptz not null default now(),
    last_seen_at timestamptz not null default now(),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (chain_id, tx_hash, log_index, address, candidate_kind),
    constraint contract_discovery_candidate_address check (address = lower(address)),
    constraint contract_discovery_source_address check (source_address = lower(source_address)),
    constraint contract_discovery_candidate_status check (
        status in ('pending', 'validated', 'rejected', 'error', 'orphaned')
    ),
    constraint contract_discovery_candidate_confidence check (
        confidence >= 0 and confidence <= 1
    ),
    constraint contract_discovery_candidate_dataset_pair check (
        (dataset_key is null and dataset_version is null)
        or (dataset_key is not null and dataset_version is not null)
    )
);

create index if not exists idx_contract_discovery_candidates_status
    on contract_discovery_candidates (chain_id, status, block_number desc);
create index if not exists idx_contract_discovery_candidates_factory
    on contract_discovery_candidates (chain_id, source_address, protocol_family, block_number desc);
create index if not exists idx_contract_discovery_candidates_canonical
    on contract_discovery_candidates (chain_id, block_number, is_canonical);

create table if not exists dex_factory_metadata (
    chain_id bigint not null,
    factory_address text not null,
    protocol_key text not null,
    protocol_family text not null,
    display_name text not null,
    code_hash text not null,
    validation_status text not null,
    source_key text not null references intelligence_sources (source_key),
    first_event_block bigint not null,
    last_event_block bigint not null,
    observed_event_count bigint not null default 1,
    dataset_key text,
    dataset_version text,
    metadata jsonb not null default '{}'::jsonb,
    first_seen_at timestamptz not null default now(),
    last_seen_at timestamptz not null default now(),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (chain_id, factory_address, protocol_family),
    constraint dex_factory_metadata_address check (factory_address = lower(factory_address)),
    constraint dex_factory_metadata_status check (validation_status in ('active', 'error')),
    constraint dex_factory_metadata_block_order check (first_event_block <= last_event_block)
);

create index if not exists idx_dex_factory_metadata_status
    on dex_factory_metadata (chain_id, validation_status, last_event_block desc);

create table if not exists dex_pool_metadata (
    chain_id bigint not null,
    pool_address text not null,
    factory_address text not null,
    protocol_key text not null,
    pool_variant text not null,
    token0_address text not null,
    token1_address text not null,
    fee integer,
    tick_spacing integer,
    stable boolean,
    creation_block bigint not null,
    creation_block_hash text not null,
    creation_tx_hash text not null,
    creation_log_index bigint not null,
    code_hash text not null,
    validation_status text not null,
    source_key text not null references intelligence_sources (source_key),
    dataset_key text not null,
    dataset_version text not null,
    metadata jsonb not null default '{}'::jsonb,
    first_seen_at timestamptz not null default now(),
    last_seen_at timestamptz not null default now(),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (chain_id, pool_address),
    constraint dex_pool_metadata_address check (pool_address = lower(pool_address)),
    constraint dex_pool_metadata_factory_address check (factory_address = lower(factory_address)),
    constraint dex_pool_metadata_token0_address check (token0_address = lower(token0_address)),
    constraint dex_pool_metadata_token1_address check (token1_address = lower(token1_address)),
    constraint dex_pool_metadata_status check (
        validation_status in ('active', 'orphaned', 'error')
    )
);

create index if not exists idx_dex_pool_metadata_factory
    on dex_pool_metadata (chain_id, factory_address, validation_status, creation_block desc);
create index if not exists idx_dex_pool_metadata_tokens
    on dex_pool_metadata (chain_id, token0_address, token1_address, validation_status);

alter table stock_token_multiplier_events
    add column if not exists block_hash text not null default '';
alter table stock_token_multiplier_events
    add column if not exists tx_index bigint not null default 0;
alter table stock_token_multiplier_events
    add column if not exists event_signature text not null default '';
alter table stock_token_multiplier_events
    add column if not exists is_canonical boolean not null default true;

create index if not exists idx_stock_token_multiplier_events_block
    on stock_token_multiplier_events (chain_id, block_number, is_canonical);

create table if not exists stock_token_ui_transfer_events (
    chain_id bigint not null,
    contract_address text not null,
    block_number bigint not null,
    block_hash text not null,
    tx_hash text not null,
    tx_index bigint not null,
    log_index bigint not null,
    from_address text not null,
    to_address text not null,
    raw_amount text not null,
    ui_amount text not null,
    event_signature text not null,
    source_key text not null references intelligence_sources (source_key),
    is_canonical boolean not null default true,
    metadata jsonb not null default '{}'::jsonb,
    observed_at timestamptz not null default now(),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (chain_id, tx_hash, log_index),
    constraint stock_token_ui_transfer_contract check (contract_address = lower(contract_address)),
    constraint stock_token_ui_transfer_from check (from_address = lower(from_address)),
    constraint stock_token_ui_transfer_to check (to_address = lower(to_address))
);

create index if not exists idx_stock_token_ui_transfer_events_token
    on stock_token_ui_transfer_events (chain_id, contract_address, block_number desc)
    where is_canonical;
