create table if not exists cluster_tracking_state (
  cluster_id uuid primary key references address_clusters(cluster_id) on delete cascade,
  chain_id bigint not null,
  tracking_status text not null default 'paused',
  last_processed_block bigint,
  last_processed_at timestamptz,
  last_updated_at timestamptz,
  error_message text,
  tracked_address_count integer not null default 0,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists idx_cluster_tracking_state_status
  on cluster_tracking_state (tracking_status, updated_at desc);

create table if not exists classified_transactions (
  classified_transaction_id uuid primary key default gen_random_uuid(),
  cluster_id uuid not null references address_clusters(cluster_id) on delete cascade,
  chain_id bigint not null,
  tx_hash text not null,
  address text not null,
  block_number bigint not null,
  block_timestamp timestamptz,
  behavior_tag text not null default 'unknown',
  protocol text,
  token_in text,
  token_out text,
  amount_in text,
  amount_out text,
  value_usd double precision,
  gas_cost_usd double precision,
  estimated_pnl_usd double precision,
  raw_json jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (cluster_id, tx_hash)
);

create index if not exists idx_classified_transactions_cluster_timestamp
  on classified_transactions (cluster_id, block_timestamp desc, block_number desc);

create table if not exists cluster_metrics_snapshots (
  snapshot_id uuid primary key,
  cluster_id uuid not null references address_clusters(cluster_id) on delete cascade,
  chain_id bigint not null,
  window text not null,
  snapshot_timestamp timestamptz not null,
  tx_count integer not null default 0,
  volume_usd double precision,
  gas_cost_usd double precision,
  pnl_usd double precision,
  roi double precision,
  active_addresses_count integer not null default 0,
  behavior_breakdown_json jsonb not null default '{}'::jsonb,
  metadata jsonb not null default '{}'::jsonb
);

create index if not exists idx_cluster_metrics_snapshots_cluster_window
  on cluster_metrics_snapshots (cluster_id, window, snapshot_timestamp desc);

create table if not exists cluster_alerts (
  alert_id uuid primary key,
  cluster_id uuid not null references address_clusters(cluster_id) on delete cascade,
  chain_id bigint not null,
  alert_type text not null,
  severity text not null,
  title text not null,
  message text not null,
  related_metric_json jsonb not null default '{}'::jsonb,
  triggered_at timestamptz not null,
  is_read boolean not null default false,
  read_at timestamptz,
  metadata jsonb not null default '{}'::jsonb
);

create index if not exists idx_cluster_alerts_cluster_triggered
  on cluster_alerts (cluster_id, triggered_at desc);
