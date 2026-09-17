create table if not exists user_monitors (
    monitor_id uuid primary key default gen_random_uuid(),
    user_id uuid not null references app_users(user_id) on delete cascade,
    chain_id bigint not null check (chain_id > 0),
    name text not null check (length(trim(name)) > 0),
    status text not null default 'active'
        check (status in ('active', 'paused', 'archived')),
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create index if not exists idx_user_monitors_user_chain
    on user_monitors (user_id, chain_id, updated_at desc);

create table if not exists user_monitor_entities (
    monitor_entity_id uuid primary key default gen_random_uuid(),
    monitor_id uuid not null references user_monitors(monitor_id) on delete cascade,
    address text not null check (address ~ '^0x[0-9a-fA-F]{40}$'),
    entity_kind text not null default 'address'
        check (entity_kind in ('address', 'contract')),
    role text,
    label text,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    unique (monitor_id, address)
);

create index if not exists idx_user_monitor_entities_address
    on user_monitor_entities (lower(address), monitor_id);

create table if not exists user_monitor_rules (
    rule_id uuid primary key default gen_random_uuid(),
    monitor_id uuid not null references user_monitors(monitor_id) on delete cascade,
    event_type text not null,
    threshold_value numeric,
    threshold_asset text,
    severity text not null default 'medium'
        check (severity in ('low', 'medium', 'high', 'critical')),
    enabled boolean not null default true,
    include_filters jsonb not null default '{}'::jsonb,
    exclude_filters jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create index if not exists idx_user_monitor_rules_monitor
    on user_monitor_rules (monitor_id, enabled, event_type);

create table if not exists user_monitor_destinations (
    destination_id uuid primary key default gen_random_uuid(),
    monitor_id uuid not null references user_monitors(monitor_id) on delete cascade,
    destination_type text not null check (destination_type in ('telegram')),
    display_name text not null,
    destination_ref text,
    enabled boolean not null default true,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create index if not exists idx_user_monitor_destinations_monitor
    on user_monitor_destinations (monitor_id, enabled);

create table if not exists user_monitor_delivery_history (
    delivery_id uuid primary key default gen_random_uuid(),
    monitor_id uuid not null references user_monitors(monitor_id) on delete cascade,
    event_id text,
    destination_id uuid references user_monitor_destinations(destination_id) on delete set null,
    status text not null check (status in ('pending', 'delivered', 'retrying', 'dead_letter')),
    attempt_count integer not null default 0 check (attempt_count >= 0),
    last_error text,
    delivered_at timestamptz,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create index if not exists idx_user_monitor_delivery_history_monitor
    on user_monitor_delivery_history (monitor_id, created_at desc);

create table if not exists execution_investigations (
    investigation_id uuid primary key default gen_random_uuid(),
    user_id uuid not null references app_users(user_id) on delete cascade,
    chain_id bigint not null check (chain_id > 0),
    title text not null check (length(trim(title)) > 0),
    status text not null default 'open'
        check (status in ('open', 'in_review', 'resolved', 'archived')),
    working_conclusion text,
    open_questions text,
    notes text,
    related_event_ids text[] not null default array[]::text[],
    related_entities text[] not null default array[]::text[],
    evidence jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create index if not exists idx_execution_investigations_user
    on execution_investigations (user_id, status, updated_at desc);
