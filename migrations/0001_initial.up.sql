create extension if not exists "pgcrypto";

create table if not exists transactions (
  chain_id bigint not null,
  tx_hash text not null,
  block_number bigint,
  from_address text not null,
  to_address text,
  tx_status text not null,
  value_raw text not null,
  input_data text not null,
  payload jsonb not null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (chain_id, tx_hash)
);

create table if not exists transaction_analysis (
  analysis_id uuid not null,
  chain_id bigint not null,
  tx_hash text not null,
  analysis_status text not null,
  ruleset_id text not null,
  pipeline_version text not null,
  flow_derivation_version text not null,
  config_version text not null,
  input_digest text not null,
  normalized_execution_facts jsonb not null,
  scenario_outputs jsonb not null,
  accounting_outputs jsonb not null,
  unresolved_items jsonb not null,
  validation_results jsonb not null,
  payload jsonb not null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (chain_id, tx_hash)
);

create table if not exists evidence_packs (
  pack_id uuid primary key,
  tx_hash text,
  attempt_id uuid,
  ruleset_id text not null,
  input_digest text not null,
  recognizer_set_versions jsonb not null,
  facts jsonb not null,
  labels jsonb not null,
  flows jsonb not null,
  unresolved jsonb not null,
  validation_results jsonb not null,
  metadata jsonb not null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists transaction_evidence (
  evidence_id uuid primary key default gen_random_uuid(),
  chain_id bigint not null,
  tx_hash text not null,
  kind text not null,
  reference text not null,
  payload jsonb not null,
  created_at timestamptz not null default now()
);

create table if not exists recognizer_results (
  result_id uuid primary key default gen_random_uuid(),
  chain_id bigint not null,
  tx_hash text not null,
  recognizer_id text not null,
  recognizer_version text not null,
  label text not null,
  confidence double precision not null,
  payload jsonb not null,
  created_at timestamptz not null default now()
);

create table if not exists flows (
  flow_row_id uuid primary key default gen_random_uuid(),
  chain_id bigint not null,
  tx_hash text not null,
  flow_id text not null,
  asset_type text not null,
  token_address text,
  from_address text not null,
  to_address text not null,
  amount_raw text not null,
  derivation_method text not null,
  confidence double precision not null,
  payload jsonb not null,
  created_at timestamptz not null default now()
);

create table if not exists tasks (
  task_id uuid primary key,
  external_ref text,
  metadata jsonb not null,
  created_at timestamptz not null default now()
);

create table if not exists opportunities (
  opportunity_id uuid primary key,
  task_id uuid,
  label text,
  metadata jsonb not null,
  created_at timestamptz not null default now()
);

create table if not exists attempts (
  attempt_id uuid primary key,
  opportunity_id uuid,
  metadata jsonb not null,
  created_at timestamptz not null default now()
);

create table if not exists rollups (
  rollup_id uuid primary key default gen_random_uuid(),
  opportunity_id uuid,
  attempt_id uuid,
  chain_id bigint not null,
  rollup_type text not null,
  ruleset_id text not null,
  payload jsonb not null,
  created_at timestamptz not null default now()
);

