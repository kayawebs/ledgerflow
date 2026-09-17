alter table seed_intelligence_runs
    drop column if exists relationship_address_jobs_changed,
    drop column if exists relationship_changes;

drop table if exists intelligence_seed_relationships;

delete from intelligence_sources
where source_key = 'seed_relationship_extractor';
