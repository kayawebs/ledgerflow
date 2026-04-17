alter table transaction_analysis
  add column if not exists block_timestamp timestamptz,
  add column if not exists from_address text,
  add column if not exists to_address text,
  add column if not exists tx_status text;

create index if not exists idx_transaction_analysis_from_address
  on transaction_analysis (chain_id, from_address);

create index if not exists idx_transaction_analysis_to_address
  on transaction_analysis (chain_id, to_address);

create index if not exists idx_transaction_analysis_block_timestamp
  on transaction_analysis (chain_id, block_timestamp);

create table if not exists address_clusters (
  cluster_id uuid primary key,
  chain_id bigint not null,
  owner_label text not null,
  name text not null,
  description text,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists cluster_addresses (
  cluster_address_id uuid primary key default gen_random_uuid(),
  cluster_id uuid not null references address_clusters(cluster_id) on delete cascade,
  address text not null,
  label text,
  role text,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  unique (cluster_id, address)
);

create index if not exists idx_cluster_addresses_address
  on cluster_addresses (address);

create table if not exists cluster_spend_summaries (
  summary_id uuid primary key default gen_random_uuid(),
  cluster_id uuid not null references address_clusters(cluster_id) on delete cascade,
  chain_id bigint not null,
  window_start timestamptz,
  window_end timestamptz,
  payload jsonb not null,
  created_at timestamptz not null default now()
);
