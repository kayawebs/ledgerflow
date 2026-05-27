create table if not exists trace_monitor_candidates (
  chain_id bigint not null,
  tx_hash text not null,
  candidate_kind text not null,
  block_number bigint not null,
  block_timestamp timestamptz,
  score integer not null default 0,
  dex_swap_count integer not null default 0,
  liquidation_event_count integer not null default 0,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (chain_id, tx_hash, candidate_kind)
);

create index if not exists idx_trace_monitor_candidates_recent
  on trace_monitor_candidates (chain_id, block_timestamp desc, score desc);
