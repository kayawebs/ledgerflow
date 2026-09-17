drop view if exists effective_intelligence_seeds;
drop table if exists seed_intelligence_runs;
drop table if exists intelligence_seed_addresses;

delete from label_definitions
where label_key = 'actor:mev_candidate';

delete from intelligence_sources
where source_key = 'mev_seed_classifier';

