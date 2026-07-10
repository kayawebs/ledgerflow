create table if not exists arbitrage_candidates (
  chain_id bigint not null,
  tx_hash text not null,
  block_number bigint not null,
  block_timestamp timestamptz,
  detection_source text not null,
  sender_address text,
  entry_contract text,
  tx_index bigint,
  score integer not null default 0,
  swap_log_count integer not null default 0,
  v2_swap_count integer not null default 0,
  v3_swap_count integer not null default 0,
  distinct_pool_count integer not null default 0,
  route text not null default '',
  route_fingerprint text not null default '',
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (chain_id, tx_hash)
);

create index if not exists idx_arbitrage_candidates_recent
  on arbitrage_candidates (chain_id, block_timestamp desc, score desc);

create index if not exists idx_arbitrage_candidates_sender_recent
  on arbitrage_candidates (chain_id, sender_address, block_timestamp desc)
  where sender_address is not null;

create index if not exists idx_arbitrage_candidates_route_recent
  on arbitrage_candidates (chain_id, route_fingerprint, block_timestamp desc)
  where route_fingerprint <> '';

create table if not exists arbitrage_candidate_pools (
  chain_id bigint not null,
  tx_hash text not null,
  pool_address text not null,
  block_number bigint not null,
  block_timestamp timestamptz,
  first_log_index bigint not null default 0,
  swap_log_count integer not null default 0,
  v2_swap_count integer not null default 0,
  v3_swap_count integer not null default 0,
  route_position integer,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (chain_id, tx_hash, pool_address),
  foreign key (chain_id, tx_hash)
    references arbitrage_candidates (chain_id, tx_hash)
    on delete cascade
);

create index if not exists idx_arbitrage_candidate_pools_hot
  on arbitrage_candidate_pools (chain_id, pool_address, block_timestamp desc);
