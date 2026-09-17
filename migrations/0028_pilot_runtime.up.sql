CREATE TABLE IF NOT EXISTS pilot_analysis_runs (
    project_id TEXT NOT NULL REFERENCES pilot_projects(project_id) ON DELETE CASCADE,
    chain_id BIGINT NOT NULL,
    tx_hash TEXT NOT NULL,
    source_input_digest TEXT NOT NULL,
    event_count INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'completed',
    last_error TEXT,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY(project_id, chain_id, tx_hash)
);

ALTER TABLE pilot_alert_deliveries
    ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS group_key TEXT,
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();

CREATE INDEX IF NOT EXISTS pilot_alert_deliveries_due_idx
    ON pilot_alert_deliveries(status, next_attempt_at)
    WHERE status IN ('queued', 'retry');

CREATE INDEX IF NOT EXISTS pilot_alert_deliveries_destination_sent_idx
    ON pilot_alert_deliveries(destination_id, sent_at DESC)
    WHERE status = 'sent';

CREATE INDEX IF NOT EXISTS pilot_analysis_runs_project_processed_idx
    ON pilot_analysis_runs(project_id, processed_at DESC);
