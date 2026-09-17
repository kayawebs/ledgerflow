DROP INDEX IF EXISTS pilot_analysis_runs_project_processed_idx;
DROP INDEX IF EXISTS pilot_alert_deliveries_destination_sent_idx;
DROP INDEX IF EXISTS pilot_alert_deliveries_due_idx;
ALTER TABLE pilot_alert_deliveries
    DROP COLUMN IF EXISTS updated_at,
    DROP COLUMN IF EXISTS created_at,
    DROP COLUMN IF EXISTS group_key,
    DROP COLUMN IF EXISTS claimed_at;
DROP TABLE IF EXISTS pilot_analysis_runs;
