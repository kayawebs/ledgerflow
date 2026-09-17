ALTER TABLE pilot_analysis_runs ADD COLUMN IF NOT EXISTS ruleset_id TEXT NOT NULL DEFAULT '';
CREATE INDEX IF NOT EXISTS pilot_analysis_runs_ruleset_idx ON pilot_analysis_runs(project_id,ruleset_id,processed_at DESC);
