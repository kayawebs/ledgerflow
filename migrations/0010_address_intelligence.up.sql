create table address_profiles (
    chain_id bigint not null,
    address text not null,
    address_type text not null default 'unknown',
    code_hash text,
    bytecode_length bigint,
    first_seen_at timestamptz not null default now(),
    last_seen_at timestamptz not null default now(),
    last_analyzed_at timestamptz,
    confidence double precision not null default 0,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (chain_id, address)
);

create index idx_address_profiles_type on address_profiles (chain_id, address_type);
create index idx_address_profiles_updated_at on address_profiles (updated_at desc);

create table address_labels (
    chain_id bigint not null,
    address text not null,
    label text not null,
    source text not null,
    confidence double precision not null default 0,
    evidence_json jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (chain_id, address, label, source)
);

create index idx_address_labels_label on address_labels (chain_id, label);
create index idx_address_labels_address on address_labels (chain_id, address);

create table address_analysis_jobs (
    chain_id bigint not null,
    address text not null,
    status text not null default 'pending',
    attempts integer not null default 0,
    last_error text,
    priority integer not null default 100,
    first_seen_at timestamptz not null default now(),
    last_seen_at timestamptz not null default now(),
    available_at timestamptz not null default now(),
    claimed_at timestamptz,
    completed_at timestamptz,
    updated_at timestamptz not null default now(),
    metadata jsonb not null default '{}'::jsonb,
    primary key (chain_id, address)
);

create index idx_address_analysis_jobs_pending
    on address_analysis_jobs (status, available_at, priority, first_seen_at);
