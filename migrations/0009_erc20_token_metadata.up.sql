create table if not exists erc20_token_metadata (
  chain_id bigint not null,
  token_address text not null,
  symbol text,
  name text,
  decimals integer,
  fetch_status text not null default 'ok',
  error_message text,
  fetched_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (chain_id, token_address)
);

create index if not exists idx_erc20_token_metadata_status
  on erc20_token_metadata (fetch_status, updated_at desc);
