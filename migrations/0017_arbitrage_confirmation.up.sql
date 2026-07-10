alter table arbitrage_candidates
  add column if not exists confirmation_status text not null default 'candidate',
  add column if not exists profit_recipient text,
  add column if not exists profit_token_address text,
  add column if not exists profit_amount_raw text,
  add column if not exists confirmation_metadata jsonb not null default '{}'::jsonb;

create index if not exists idx_arbitrage_candidates_confirmation_recent
  on arbitrage_candidates (chain_id, confirmation_status, block_timestamp desc);
