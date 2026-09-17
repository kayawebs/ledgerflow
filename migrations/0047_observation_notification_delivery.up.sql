alter table user_monitor_delivery_history
    add column if not exists next_attempt_at timestamptz not null default now(),
    add column if not exists claimed_at timestamptz,
    add column if not exists metadata jsonb not null default '{}'::jsonb;

create unique index if not exists idx_user_monitor_delivery_signal_destination
    on user_monitor_delivery_history (monitor_id, event_id, destination_id)
    where event_id is not null and destination_id is not null;

create index if not exists idx_user_monitor_delivery_due
    on user_monitor_delivery_history (status, next_attempt_at, created_at)
    where status in ('pending', 'retrying');

comment on column user_monitor_delivery_history.event_id is
    'Stable upstream event or object change-signal identifier used for delivery deduplication.';
