alter table app_users
    add column if not exists workspace_plan text not null default 'free';

alter table app_users
    add constraint app_users_workspace_plan_check
    check (workspace_plan in ('free', 'pilot', 'enterprise'));

create table if not exists pilot_interest_submissions (
    submission_id uuid primary key default gen_random_uuid(),
    name text not null check (length(trim(name)) between 1 and 120),
    email text not null check (length(trim(email)) between 3 and 320),
    chain_id bigint not null check (chain_id in (8453, 4663)),
    addresses text[] not null check (
        cardinality(addresses) between 1 and 30
    ),
    use_case text not null check (length(trim(use_case)) between 1 and 4000),
    status text not null default 'new'
        check (status in ('new', 'reviewing', 'contacted', 'closed')),
    source text not null default 'public_website',
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create index if not exists idx_pilot_interest_submissions_status_created
    on pilot_interest_submissions (status, created_at desc);

comment on table pilot_interest_submissions is
    'Public Pilot intake only. This is not a CRM, billing system, or customer workspace.';
