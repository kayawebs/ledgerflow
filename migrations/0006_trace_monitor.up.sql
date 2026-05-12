create table if not exists trace_monitor_progress (
  worker_id text primary key,
  chain_id bigint not null,
  last_processed_block bigint,
  last_processed_at timestamptz,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists trace_address_asset_deltas (
  chain_id bigint not null,
  tx_hash text not null,
  block_number bigint not null,
  block_timestamp timestamptz,
  address text not null,
  asset_type text not null,
  token_address text not null,
  amount_in_raw numeric(78,0) not null default 0,
  amount_out_raw numeric(78,0) not null default 0,
  net_raw numeric(78,0) not null default 0,
  flow_count integer not null default 0,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (chain_id, tx_hash, address, asset_type, token_address)
);

create index if not exists idx_trace_address_asset_deltas_recent
  on trace_address_asset_deltas (block_timestamp desc, block_number desc);

create index if not exists idx_trace_address_asset_deltas_positive
  on trace_address_asset_deltas (chain_id, asset_type, token_address, net_raw desc, block_timestamp desc)
  where net_raw > 0;

create table if not exists trace_cluster_asset_deltas (
  chain_id bigint not null,
  cluster_id uuid not null,
  tx_hash text not null,
  block_number bigint not null,
  block_timestamp timestamptz,
  asset_type text not null,
  token_address text not null,
  amount_in_raw numeric(78,0) not null default 0,
  amount_out_raw numeric(78,0) not null default 0,
  net_raw numeric(78,0) not null default 0,
  contributing_address_count integer not null default 0,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (chain_id, cluster_id, tx_hash, asset_type, token_address)
);

create index if not exists idx_trace_cluster_asset_deltas_recent
  on trace_cluster_asset_deltas (cluster_id, block_timestamp desc, block_number desc);
