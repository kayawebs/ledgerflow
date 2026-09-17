drop index if exists idx_user_monitor_delivery_due;
drop index if exists idx_user_monitor_delivery_signal_destination;

alter table user_monitor_delivery_history
    drop column if exists metadata,
    drop column if exists claimed_at,
    drop column if exists next_attempt_at;
