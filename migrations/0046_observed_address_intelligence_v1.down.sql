alter table workspace_entities
    drop column if exists last_observation_sync_at,
    drop column if exists observation_error,
    drop column if exists observation_progress,
    drop column if exists observation_status,
    drop column if exists observation_enabled;

drop table if exists object_briefs;
drop table if exists object_change_signals;
drop table if exists object_economic_summaries;
drop table if exists object_relationships;
drop table if exists object_behavior_profiles;
drop table if exists object_activity_summaries;
drop table if exists object_activity_transactions;
drop table if exists observation_analysis_runs;
drop table if exists observed_address_objects;
