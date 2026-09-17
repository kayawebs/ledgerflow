create table if not exists intelligence_workspaces (
    workspace_id uuid primary key default gen_random_uuid(),
    user_id uuid not null references app_users(user_id) on delete cascade,
    name text not null check (length(trim(name)) between 1 and 160),
    workspace_type text not null
        check (workspace_type in ('protocol', 'chain', 'searcher', 'fund_research', 'custom', 'demo')),
    chain_id bigint not null check (chain_id > 0),
    status text not null default 'draft'
        check (status in ('draft', 'active', 'archived')),
    is_demo boolean not null default false,
    legacy_cluster_id uuid references address_clusters(cluster_id) on delete set null,
    onboarding_completed_at timestamptz,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (user_id, legacy_cluster_id)
);

create index if not exists idx_intelligence_workspaces_user
    on intelligence_workspaces (user_id, status, updated_at desc);

create unique index if not exists idx_intelligence_workspaces_user_demo
    on intelligence_workspaces (user_id)
    where is_demo and status <> 'archived';

create table if not exists workspace_entities (
    workspace_entity_id uuid primary key default gen_random_uuid(),
    workspace_id uuid not null references intelligence_workspaces(workspace_id) on delete cascade,
    address text not null check (address ~ '^0x[0-9a-fA-F]{40}$'),
    entity_kind text not null default 'address'
        check (entity_kind in ('address', 'contract', 'pool', 'protocol')),
    role text,
    label text,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (workspace_id, address)
);

create index if not exists idx_workspace_entities_lookup
    on workspace_entities (workspace_id, lower(address));

create table if not exists workspace_monitoring_goals (
    workspace_id uuid not null references intelligence_workspaces(workspace_id) on delete cascade,
    goal_type text not null check (length(trim(goal_type)) between 1 and 80),
    enabled boolean not null default true,
    configuration jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (workspace_id, goal_type)
);

create table if not exists workspace_notification_destinations (
    destination_id uuid primary key default gen_random_uuid(),
    workspace_id uuid not null references intelligence_workspaces(workspace_id) on delete cascade,
    destination_type text not null
        check (destination_type in ('telegram', 'webhook', 'email')),
    display_name text not null check (length(trim(display_name)) between 1 and 160),
    destination_ref text,
    enabled boolean not null default true,
    operational boolean not null default false,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create index if not exists idx_workspace_destinations_workspace
    on workspace_notification_destinations (workspace_id, enabled);

alter table user_monitors
    add column if not exists workspace_id uuid references intelligence_workspaces(workspace_id) on delete set null;

alter table execution_investigations
    add column if not exists workspace_id uuid references intelligence_workspaces(workspace_id) on delete set null;

create index if not exists idx_user_monitors_workspace
    on user_monitors (workspace_id, updated_at desc);

create index if not exists idx_execution_investigations_workspace
    on execution_investigations (workspace_id, updated_at desc);

-- Preserve existing customer clusters as completed private workspaces. This is
-- a presentation/access migration only; it does not alter indexing or analysis.
insert into intelligence_workspaces (
    user_id, name, workspace_type, chain_id, status, is_demo,
    legacy_cluster_id, onboarding_completed_at, metadata
)
select links.user_id, clusters.name, 'custom', clusters.chain_id, 'active', false,
       clusters.cluster_id, now(), jsonb_build_object('source', 'legacy_cluster')
from app_user_clusters links
join address_clusters clusters on clusters.cluster_id = links.cluster_id
on conflict (user_id, legacy_cluster_id) do nothing;

insert into workspace_entities (
    workspace_id, address, entity_kind, role, label, metadata
)
select workspaces.workspace_id, lower(addresses.address),
       case
           when lower(coalesce(addresses.role, '')) like '%pool%' then 'pool'
           when lower(coalesce(addresses.role, '')) in ('contract', 'router', 'treasury', 'fee_collector') then 'contract'
           else 'address'
       end,
       addresses.role, addresses.label,
       addresses.metadata || jsonb_build_object('source', 'legacy_cluster')
from intelligence_workspaces workspaces
join cluster_addresses addresses on addresses.cluster_id = workspaces.legacy_cluster_id
on conflict (workspace_id, address) do nothing;

update user_monitors monitors
set workspace_id = workspaces.workspace_id
from intelligence_workspaces workspaces
where monitors.workspace_id is null
  and workspaces.user_id = monitors.user_id
  and workspaces.chain_id = monitors.chain_id
  and workspaces.status <> 'archived'
  and workspaces.workspace_id = (
      select candidate.workspace_id
      from intelligence_workspaces candidate
      where candidate.user_id = monitors.user_id
        and candidate.chain_id = monitors.chain_id
        and candidate.status <> 'archived'
      order by candidate.is_demo, candidate.created_at
      limit 1
  );

comment on table intelligence_workspaces is
    'Customer-scoped intelligence configuration. Public Evidence Explorer data is not stored here.';

comment on column workspace_notification_destinations.operational is
    'True only when a supported delivery backend is connected; captured onboarding preferences remain false.';
