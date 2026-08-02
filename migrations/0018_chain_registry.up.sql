create table if not exists chain_registry (
    chain_id bigint primary key check (chain_id > 0),
    slug text not null unique,
    display_name text not null,
    native_symbol text not null,
    explorer_base_url text,
    api_base_url text not null,
    indexer_api_base_url text,
    enabled boolean not null default false,
    capabilities jsonb not null default '{}'::jsonb,
    sort_order integer not null default 100,
    config_version bigint not null default 1 check (config_version > 0),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

comment on table chain_registry is
    'Control-plane routing metadata for isolated per-chain data-plane APIs. Credentials live in the deployment secret store, never in this table.';
