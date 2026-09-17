drop table if exists candidate_cluster_member_overrides;
drop table if exists candidate_cluster_change_signals;
drop table if exists candidate_cluster_window_summaries;
drop table if exists candidate_cluster_executions;
drop table if exists candidate_execution_cluster_relationships;
drop table if exists candidate_execution_cluster_members;

alter table observed_address_objects
    drop constraint if exists observed_address_objects_candidate_cluster_fk,
    drop constraint if exists observed_address_objects_seed_confidence,
    drop constraint if exists observed_address_objects_seed_role,
    drop column if exists candidate_cluster_id,
    drop column if exists analysis_scope,
    drop column if exists seed_role_evidence,
    drop column if exists seed_role_confidence,
    drop column if exists seed_role,
    drop column if exists seed_address;

drop table if exists candidate_execution_clusters;
