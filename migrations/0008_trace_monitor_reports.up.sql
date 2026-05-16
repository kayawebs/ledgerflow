create table if not exists trace_monitor_tx_reports (
  report_id uuid primary key default gen_random_uuid(),
  chain_id bigint not null,
  tx_hash text not null,
  window_start timestamptz not null,
  window_end timestamptz not null,
  activity_kind text not null,
  volume_score_raw numeric(78,0) not null default 0,
  share_id uuid references tx_analysis_shares(share_id) on delete set null,
  tweet_text text not null,
  email_to text not null,
  sent_at timestamptz,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  unique (chain_id, tx_hash)
);

create index if not exists idx_trace_monitor_tx_reports_recent
  on trace_monitor_tx_reports (created_at desc);
