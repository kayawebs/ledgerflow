insert into intelligence_sources (
    source_key, display_name, source_kind, trust_score, owner, metadata
)
values (
    'canonical_address_facts',
    'Canonical chain address observations',
    'onchain',
    0.95,
    'LedgerFlow',
    '{"source":"clickhouse_canonical_facts","method_version":"address_fact_producer_v1"}'::jsonb
)
on conflict (source_key) do update set
    display_name = excluded.display_name,
    source_kind = excluded.source_kind,
    trust_score = excluded.trust_score,
    owner = excluded.owner,
    status = 'active',
    metadata = intelligence_sources.metadata || excluded.metadata,
    updated_at = now();

create table if not exists address_observation_windows (
    worker_id text not null,
    chain_id bigint not null,
    window_start_block bigint not null,
    window_end_block bigint not null,
    address text not null,
    observation_role text not null,
    first_seen_block bigint not null,
    last_seen_block bigint not null,
    observation_count bigint not null,
    sample_block_hash text not null,
    sample_tx_hash text not null,
    sample_log_index bigint,
    source_key text not null references intelligence_sources (source_key),
    metadata jsonb not null default '{}'::jsonb,
    observed_at timestamptz not null default now(),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (
        worker_id, chain_id, window_start_block, address, observation_role
    ),
    constraint address_observation_windows_address check (address = lower(address)),
    constraint address_observation_windows_window check (
        window_start_block <= window_end_block
    ),
    constraint address_observation_windows_seen_range check (
        window_start_block <= first_seen_block
        and first_seen_block <= last_seen_block
        and last_seen_block <= window_end_block
    ),
    constraint address_observation_windows_count check (observation_count > 0)
);

create index if not exists idx_address_observation_windows_address
    on address_observation_windows (chain_id, address, first_seen_block, last_seen_block);

create index if not exists idx_address_observation_windows_range
    on address_observation_windows (
        worker_id, chain_id, window_start_block, window_end_block
    );

create index if not exists idx_address_observation_windows_role
    on address_observation_windows (
        chain_id, observation_role, last_seen_block desc
    );
