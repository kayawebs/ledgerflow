drop index if exists idx_arbitrage_candidates_confirmation_recent;

alter table arbitrage_candidates
  drop column if exists confirmation_metadata,
  drop column if exists profit_amount_raw,
  drop column if exists profit_token_address,
  drop column if exists profit_recipient,
  drop column if exists confirmation_status;
