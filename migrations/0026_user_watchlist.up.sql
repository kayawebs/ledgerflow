create table if not exists user_watchlist_entries (
    entry_id uuid primary key default gen_random_uuid(),
    user_id uuid not null references app_users(user_id) on delete cascade,
    chain_id bigint not null,
    subject_type text not null,
    subject_key text not null,
    label text,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    constraint user_watchlist_subject_type check (
        subject_type in ('address', 'transaction', 'cluster')
    ),
    unique (user_id, chain_id, subject_type, subject_key)
);

create index if not exists idx_user_watchlist_entries_user
    on user_watchlist_entries (user_id, created_at desc);
create index if not exists idx_user_watchlist_entries_subject
    on user_watchlist_entries (chain_id, subject_type, subject_key);
