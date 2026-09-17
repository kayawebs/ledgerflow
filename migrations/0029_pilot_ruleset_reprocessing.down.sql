DROP INDEX IF EXISTS pilot_analysis_runs_ruleset_idx;
ALTER TABLE pilot_analysis_runs DROP COLUMN IF EXISTS ruleset_id;
