create table if not exists address_protocol_metadata (
    chain_id bigint not null,
    address text not null,
    protocol_family text not null,
    protocol_name text not null,
    contract_role text not null,
    source text not null,
    adapter_version text not null,
    confidence real not null,
    metadata jsonb not null default '{}'::jsonb,
    first_seen_at timestamptz not null default now(),
    last_seen_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (chain_id, address, protocol_family, protocol_name, contract_role)
);

create index if not exists idx_address_protocol_metadata_lookup
    on address_protocol_metadata (chain_id, address);

create index if not exists idx_address_protocol_metadata_family
    on address_protocol_metadata (chain_id, protocol_family, protocol_name, contract_role);
