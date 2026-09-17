drop index if exists idx_economic_verification_results_subject;
drop index if exists idx_economic_verification_results_recent;
drop table if exists economic_verification_results;

drop index if exists idx_economic_verification_jobs_subject;
drop index if exists idx_economic_verification_jobs_claim;
drop table if exists economic_verification_jobs;

delete from intelligence_sources where source_key = 'economic_verifier';
