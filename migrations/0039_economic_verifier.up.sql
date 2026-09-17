insert into intelligence_sources (
    source_key, display_name, source_kind, trust_score, owner, metadata
)
values (
    'economic_verifier',
    'Deterministic seed economic verifier',
    'onchain',
    0.98,
    'LedgerFlow',
    '{"inputs":["transaction_analysis","transaction","receipt","logs","trace","archive_balances"],"method_version":"seed_sender_accounting_v1","llm_used":false}'::jsonb
)
on conflict (source_key) do update set
    display_name = excluded.display_name,
    source_kind = excluded.source_kind,
    trust_score = excluded.trust_score,
    owner = excluded.owner,
    status = 'active',
    metadata = intelligence_sources.metadata || excluded.metadata,
    updated_at = now();

create table if not exists economic_verification_jobs (
    chain_id bigint not null,
    tx_hash text not null,
    subject_address text not null,
    event_type text not null default 'dex_arbitrage',
    priority integer not null default 10000,
    status text not null default 'queued',
    attempts integer not null default 0,
    source_digest text not null,
    available_at timestamptz not null default now(),
    claimed_at timestamptz,
    completed_at timestamptz,
    last_error text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (chain_id, tx_hash),
    constraint economic_verification_jobs_subject check (
        subject_address = lower(subject_address)
        and subject_address ~ '^0x[0-9a-f]{40}$'
    ),
    constraint economic_verification_jobs_event_type check (
        event_type in ('dex_arbitrage')
    ),
    constraint economic_verification_jobs_priority check (
        priority between 0 and 1000000
    ),
    constraint economic_verification_jobs_status check (
        status in ('queued', 'running', 'completed', 'failed')
    ),
    constraint economic_verification_jobs_attempts check (attempts >= 0)
);

create index if not exists idx_economic_verification_jobs_claim
    on economic_verification_jobs (priority, available_at, created_at)
    where status in ('queued', 'failed');

create index if not exists idx_economic_verification_jobs_subject
    on economic_verification_jobs (chain_id, subject_address, status, updated_at desc);

create table if not exists economic_verification_results (
    verification_id uuid primary key default gen_random_uuid(),
    chain_id bigint not null,
    tx_hash text not null,
    subject_address text not null,
    event_type text not null,
    result_status text not null,
    severity text not null,
    confidence double precision not null,
    conclusion text not null,
    profit_recipient text,
    profit_token_address text,
    profit_amount_raw text,
    gas_cost_raw text,
    ruleset_id text not null,
    method_version text not null,
    inputs_digest text not null,
    evidence jsonb not null,
    limitations text[] not null default array[]::text[],
    verified_at timestamptz,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (chain_id, tx_hash),
    constraint economic_verification_results_subject check (
        subject_address = lower(subject_address)
        and subject_address ~ '^0x[0-9a-f]{40}$'
    ),
    constraint economic_verification_results_event_type check (
        event_type in ('dex_arbitrage')
    ),
    constraint economic_verification_results_status check (
        result_status in ('verified', 'insufficient_evidence', 'dismissed')
    ),
    constraint economic_verification_results_severity check (
        severity in ('low', 'medium', 'high', 'critical')
    ),
    constraint economic_verification_results_confidence check (
        confidence >= 0 and confidence <= 1
    ),
    constraint economic_verification_results_verified_profit check (
        result_status <> 'verified'
        or (profit_token_address is not null and profit_amount_raw is not null)
    )
);

create index if not exists idx_economic_verification_results_recent
    on economic_verification_results (chain_id, result_status, updated_at desc);

create index if not exists idx_economic_verification_results_subject
    on economic_verification_results (
        chain_id, subject_address, result_status, updated_at desc
    );

comment on table economic_verification_results is
    'Versioned deterministic economic outcomes for explicit seed scopes. Unknown evidence remains null and is recorded as insufficient_evidence.';

