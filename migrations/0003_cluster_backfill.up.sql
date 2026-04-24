create table if not exists cluster_backfill_runs (
  run_id uuid primary key,
  cluster_id uuid not null references address_clusters(cluster_id) on delete cascade,
  chain_id bigint not null,
  start_block bigint,
  end_block bigint,
  status text not null,
  requested_at timestamptz not null default now(),
  started_at timestamptz,
  finished_at timestamptz,
  address_count integer not null default 0,
  discovered_tx_count integer not null default 0,
  analyzed_tx_count integer not null default 0,
  failed_tx_count integer not null default 0,
  metadata jsonb not null default '{}'::jsonb
);

create index if not exists idx_cluster_backfill_runs_cluster_requested_at
  on cluster_backfill_runs (cluster_id, requested_at desc);

create table if not exists cluster_discovered_txs (
  cluster_discovered_tx_id uuid primary key default gen_random_uuid(),
  cluster_id uuid not null references address_clusters(cluster_id) on delete cascade,
  chain_id bigint not null,
  tx_hash text not null,
  block_number bigint,
  processing_status text not null default 'pending',
  latest_run_id uuid references cluster_backfill_runs(run_id) on delete set null,
  first_seen_at timestamptz not null default now(),
  last_seen_at timestamptz not null default now(),
  analysis_attempted_at timestamptz,
  analyzed_at timestamptz,
  last_error text,
  metadata jsonb not null default '{}'::jsonb,
  unique (cluster_id, tx_hash)
);

create index if not exists idx_cluster_discovered_txs_cluster_status
  on cluster_discovered_txs (cluster_id, processing_status);

create index if not exists idx_cluster_discovered_txs_cluster_block
  on cluster_discovered_txs (cluster_id, block_number);
