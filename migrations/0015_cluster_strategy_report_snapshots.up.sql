create table if not exists cluster_strategy_report_snapshots (
  snapshot_id uuid primary key,
  cluster_id uuid not null references address_clusters(cluster_id) on delete cascade,
  chain_id bigint not null,
  window_start timestamptz,
  window_end timestamptz,
  window_key text not null,
  generated_at timestamptz not null,
  report_json jsonb not null,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (cluster_id, window_key)
);

create index if not exists idx_cluster_strategy_report_snapshots_cluster_generated
  on cluster_strategy_report_snapshots (cluster_id, generated_at desc);
