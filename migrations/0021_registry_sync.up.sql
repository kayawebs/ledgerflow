create table if not exists intelligence_source_endpoints (
    endpoint_key text primary key,
    source_key text not null references intelligence_sources (source_key),
    chain_id bigint,
    endpoint_url text not null,
    parser_kind text not null,
    sync_interval_seconds bigint not null check (sync_interval_seconds >= 60),
    enabled boolean not null default true,
    http_headers jsonb not null default '{}'::jsonb,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (source_key, endpoint_url)
);

comment on column intelligence_source_endpoints.http_headers is
    'Non-secret request headers only. Credentials belong in the deployment secret store.';

create table if not exists intelligence_sync_runs (
    run_id uuid primary key default gen_random_uuid(),
    endpoint_key text not null references intelligence_source_endpoints (endpoint_key),
    status text not null,
    http_status integer,
    checksum_sha256 text,
    snapshot_id uuid,
    fetched_item_count bigint not null default 0,
    validated_item_count bigint not null default 0,
    imported_item_count bigint not null default 0,
    rejected_item_count bigint not null default 0,
    dataset_key text,
    dataset_version text,
    error_message text,
    started_at timestamptz not null default now(),
    finished_at timestamptz,
    metadata jsonb not null default '{}'::jsonb,
    constraint intelligence_sync_runs_status check (
        status in ('running', 'succeeded', 'unchanged', 'not_modified', 'failed')
    )
);

create index if not exists idx_intelligence_sync_runs_endpoint
    on intelligence_sync_runs (endpoint_key, started_at desc);
create index if not exists idx_intelligence_sync_runs_status
    on intelligence_sync_runs (status, started_at desc);

create table if not exists intelligence_source_snapshots (
    snapshot_id uuid primary key default gen_random_uuid(),
    endpoint_key text not null references intelligence_source_endpoints (endpoint_key),
    run_id uuid not null unique references intelligence_sync_runs (run_id) on delete cascade,
    http_status integer not null,
    content_type text,
    etag text,
    last_modified text,
    checksum_sha256 text not null,
    body_size_bytes bigint not null,
    body_text text not null,
    response_headers jsonb not null default '{}'::jsonb,
    fetched_at timestamptz not null default now()
);

create index if not exists idx_intelligence_source_snapshots_endpoint
    on intelligence_source_snapshots (endpoint_key, fetched_at desc);
create index if not exists idx_intelligence_source_snapshots_checksum
    on intelligence_source_snapshots (endpoint_key, checksum_sha256);

create table if not exists intelligence_sync_cursors (
    endpoint_key text primary key references intelligence_source_endpoints (endpoint_key) on delete cascade,
    etag text,
    last_modified text,
    last_checksum_sha256 text,
    pagination_cursor text,
    next_sync_at timestamptz not null default now(),
    last_success_at timestamptz,
    last_run_id uuid references intelligence_sync_runs (run_id),
    consecutive_failures integer not null default 0,
    last_error text,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create index if not exists idx_intelligence_sync_cursors_due
    on intelligence_sync_cursors (next_sync_at);

create table if not exists stock_token_assets (
    asset_uid text primary key,
    token_symbol text not null,
    token_name text not null,
    status text not null,
    current_multiplier text not null,
    pending_multiplier text,
    pending_multiplier_effective_at timestamptz,
    logo_url text,
    trading_capabilities jsonb,
    source_key text not null references intelligence_sources (source_key),
    snapshot_id uuid references intelligence_source_snapshots (snapshot_id),
    metadata jsonb not null default '{}'::jsonb,
    first_seen_at timestamptz not null default now(),
    last_seen_at timestamptz not null default now(),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create index if not exists idx_stock_token_assets_symbol
    on stock_token_assets (token_symbol);
create index if not exists idx_stock_token_assets_status
    on stock_token_assets (status, updated_at desc);

create table if not exists stock_token_deployments (
    asset_uid text not null references stock_token_assets (asset_uid),
    chain_id bigint not null,
    contract_address text not null,
    current_multiplier text not null,
    pending_multiplier text,
    pending_multiplier_effective_at timestamptz,
    validation_status text not null,
    validation_error text,
    code_hash text,
    onchain_symbol text,
    onchain_name text,
    onchain_decimals integer,
    source_key text not null references intelligence_sources (source_key),
    snapshot_id uuid references intelligence_source_snapshots (snapshot_id),
    validated_at timestamptz,
    first_seen_at timestamptz not null default now(),
    last_seen_at timestamptz not null default now(),
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (asset_uid, chain_id, contract_address),
    unique (chain_id, contract_address),
    constraint stock_token_deployments_address check (contract_address = lower(contract_address)),
    constraint stock_token_deployments_validation check (
        validation_status in ('validated', 'rejected', 'error')
    )
);

create index if not exists idx_stock_token_deployments_chain
    on stock_token_deployments (chain_id, validation_status, updated_at desc);

create table if not exists stock_token_corporate_actions (
    action_id text primary key,
    token_symbol text not null,
    action_type text not null,
    status text not null,
    process_date date,
    deployments jsonb not null default '[]'::jsonb,
    details jsonb not null default '{}'::jsonb,
    source_key text not null references intelligence_sources (source_key),
    snapshot_id uuid references intelligence_source_snapshots (snapshot_id),
    first_seen_at timestamptz not null default now(),
    last_seen_at timestamptz not null default now(),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create index if not exists idx_stock_token_corporate_actions_symbol
    on stock_token_corporate_actions (token_symbol, process_date desc);
create index if not exists idx_stock_token_corporate_actions_status
    on stock_token_corporate_actions (status, process_date desc);

create table if not exists stock_token_multiplier_events (
    chain_id bigint not null,
    contract_address text not null,
    block_number bigint not null,
    tx_hash text not null,
    log_index bigint not null,
    old_value text,
    new_value text not null,
    effective_at timestamptz,
    source_key text not null references intelligence_sources (source_key),
    metadata jsonb not null default '{}'::jsonb,
    observed_at timestamptz not null default now(),
    created_at timestamptz not null default now(),
    primary key (chain_id, tx_hash, log_index),
    constraint stock_token_multiplier_events_address check (contract_address = lower(contract_address))
);

insert into intelligence_sources (
    source_key, display_name, source_kind, trust_score, owner, homepage, metadata
)
values
    ('robinhood_stock_token_api', 'Robinhood Stock Token API', 'official', 1.0,
     'Robinhood', 'https://docs.robinhood.com/chain/stock-token-apis/',
     '{"chain_id":4663,"data_class":"stock_tokens"}'::jsonb),
    ('robinhood_token_contracts', 'Robinhood Chain Token Contracts', 'official', 1.0,
     'Robinhood', 'https://docs.robinhood.com/chain/contracts/',
     '{"chain_id":4663,"data_class":"canonical_tokens"}'::jsonb),
    ('robinhood_protocol_contracts', 'Robinhood Chain Protocol Contracts', 'official', 1.0,
     'Robinhood', 'https://docs.robinhood.com/chain/protocol-contracts/',
     '{"chain_id":4663,"data_class":"protocol_contracts"}'::jsonb)
on conflict (source_key) do update set
    display_name = excluded.display_name,
    source_kind = excluded.source_kind,
    trust_score = excluded.trust_score,
    owner = excluded.owner,
    homepage = excluded.homepage,
    status = 'active',
    metadata = intelligence_sources.metadata || excluded.metadata,
    updated_at = now();

insert into intelligence_source_endpoints (
    endpoint_key, source_key, chain_id, endpoint_url, parser_kind,
    sync_interval_seconds, metadata
)
values
    ('robinhood_stock_assets', 'robinhood_stock_token_api', 4663,
     'https://api.robinhood.com/rhj/assets', 'robinhood_stock_assets', 900,
     '{"official_docs":"https://docs.robinhood.com/chain/stock-token-apis/"}'::jsonb),
    ('robinhood_corporate_actions', 'robinhood_stock_token_api', 4663,
     'https://api.robinhood.com/rhj/corporate-actions', 'robinhood_corporate_actions', 3600,
     '{"official_docs":"https://docs.robinhood.com/chain/stock-token-apis/"}'::jsonb),
    ('robinhood_token_contracts', 'robinhood_token_contracts', 4663,
     'https://docs.robinhood.com/chain/contracts/', 'robinhood_token_contracts_html', 86400,
     '{}'::jsonb),
    ('robinhood_protocol_contracts', 'robinhood_protocol_contracts', 4663,
     'https://docs.robinhood.com/chain/protocol-contracts/', 'robinhood_protocol_contracts_html', 86400,
     '{}'::jsonb)
on conflict (endpoint_key) do update set
    source_key = excluded.source_key,
    chain_id = excluded.chain_id,
    endpoint_url = excluded.endpoint_url,
    parser_kind = excluded.parser_kind,
    sync_interval_seconds = excluded.sync_interval_seconds,
    metadata = intelligence_source_endpoints.metadata || excluded.metadata,
    updated_at = now();

insert into intelligence_sync_cursors (endpoint_key, next_sync_at)
select endpoint_key, now()
from intelligence_source_endpoints
on conflict (endpoint_key) do nothing;
