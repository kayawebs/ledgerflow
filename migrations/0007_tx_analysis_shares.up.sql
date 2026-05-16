create table if not exists tx_analysis_shares (
  share_id uuid primary key default gen_random_uuid(),
  chain_id bigint not null,
  tx_hash text not null,
  title text not null,
  summary text not null,
  analysis_snapshot jsonb not null,
  created_by uuid references app_users(user_id) on delete set null,
  created_at timestamptz not null default now(),
  expires_at timestamptz,
  revoked_at timestamptz
);

create index if not exists idx_tx_analysis_shares_tx
  on tx_analysis_shares (chain_id, tx_hash, created_at desc);

create index if not exists idx_tx_analysis_shares_public
  on tx_analysis_shares (share_id)
  where revoked_at is null;
