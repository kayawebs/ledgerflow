CREATE TABLE IF NOT EXISTS pilot_projects (
    project_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    chain_id BIGINT NOT NULL,
    config JSONB NOT NULL DEFAULT '{}'::jsonb,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT pilot_projects_base_only CHECK (chain_id = 8453)
);
CREATE TABLE IF NOT EXISTS pilot_entities (
    entity_id UUID PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES pilot_projects(project_id) ON DELETE CASCADE,
    address TEXT NOT NULL,
    role TEXT NOT NULL,
    label TEXT,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    UNIQUE(project_id, address)
);
CREATE TABLE IF NOT EXISTS pilot_rules (
    rule_id UUID PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES pilot_projects(project_id) ON DELETE CASCADE,
    event_type TEXT NOT NULL,
    severity TEXT NOT NULL,
    threshold JSONB NOT NULL DEFAULT '{}'::jsonb,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    ruleset_id TEXT NOT NULL,
    UNIQUE(project_id, event_type)
);
CREATE TABLE IF NOT EXISTS pilot_telegram_destinations (
    destination_id UUID PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES pilot_projects(project_id) ON DELETE CASCADE,
    chat_id TEXT NOT NULL,
    secret_ref TEXT,
    enabled BOOLEAN NOT NULL DEFAULT TRUE
);
CREATE TABLE IF NOT EXISTS pilot_events (
    event_id UUID PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES pilot_projects(project_id) ON DELETE CASCADE,
    chain_id BIGINT NOT NULL,
    block_number BIGINT,
    tx_hash TEXT NOT NULL,
    event_type TEXT NOT NULL,
    severity TEXT NOT NULL,
    subject JSONB NOT NULL DEFAULT '{}'::jsonb,
    conclusion TEXT NOT NULL,
    evidence_pack JSONB NOT NULL,
    inputs_digest TEXT NOT NULL,
    dedup_key TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'new',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(project_id, dedup_key)
);
CREATE INDEX IF NOT EXISTS pilot_events_project_created_idx ON pilot_events(project_id, created_at DESC);
CREATE TABLE IF NOT EXISTS pilot_alert_deliveries (
    delivery_id UUID PRIMARY KEY,
    event_id UUID NOT NULL REFERENCES pilot_events(event_id) ON DELETE CASCADE,
    destination_id UUID NOT NULL REFERENCES pilot_telegram_destinations(destination_id) ON DELETE CASCADE,
    status TEXT NOT NULL DEFAULT 'queued',
    attempt_count INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_error TEXT,
    telegram_message_id TEXT,
    sent_at TIMESTAMPTZ,
    UNIQUE(event_id, destination_id)
);
CREATE TABLE IF NOT EXISTS pilot_weekly_reports (
    report_id UUID PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES pilot_projects(project_id) ON DELETE CASCADE,
    week_start DATE NOT NULL,
    week_end DATE NOT NULL,
    markdown TEXT NOT NULL,
    html TEXT NOT NULL,
    inputs_digest TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(project_id, week_start)
);
