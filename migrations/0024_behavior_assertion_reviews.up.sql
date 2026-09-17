insert into intelligence_sources (
    source_key, display_name, source_kind, trust_score, owner, metadata
)
values
    (
        'behavior_window_classifier',
        'Canonical-window behavior classifier',
        'model',
        0.72,
        'LedgerFlow',
        '{"input":"address_observation_windows","claim_scope":"candidate_only"}'::jsonb
    ),
    (
        'manual_behavior_review',
        'Manual behavior review',
        'manual',
        1.0,
        'LedgerFlow',
        '{"workflow":"behavior_assertion_review_v1"}'::jsonb
    )
on conflict (source_key) do update set
    display_name = excluded.display_name,
    source_kind = excluded.source_kind,
    trust_score = excluded.trust_score,
    owner = excluded.owner,
    status = 'active',
    metadata = intelligence_sources.metadata || excluded.metadata,
    updated_at = now();

insert into label_definitions (
    label_key, display_name, dimension, description, metadata
)
values (
    'actor:automation_candidate',
    'Automation candidate',
    'behavior',
    'A time-bounded statistical candidate. This is not a confirmed bot, searcher, or team identity.',
    '{"requires_review":true,"candidate_only":true}'::jsonb
)
on conflict (label_key) do update set
    display_name = excluded.display_name,
    dimension = excluded.dimension,
    description = excluded.description,
    status = 'active',
    metadata = label_definitions.metadata || excluded.metadata,
    updated_at = now();

create table if not exists behavior_analysis_runs (
    run_id uuid primary key default gen_random_uuid(),
    worker_id text not null,
    source_worker_id text not null,
    chain_id bigint not null,
    rule_key text not null,
    rule_version text not null,
    window_start_block bigint not null,
    window_end_block bigint not null,
    status text not null default 'running',
    evaluated_address_count bigint not null default 0,
    candidate_count bigint not null default 0,
    assertion_count bigint not null default 0,
    queued_review_count bigint not null default 0,
    thresholds jsonb not null default '{}'::jsonb,
    last_error text,
    started_at timestamptz not null default now(),
    completed_at timestamptz,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    constraint behavior_analysis_runs_status check (
        status in ('running', 'completed', 'failed')
    ),
    constraint behavior_analysis_runs_window check (
        window_start_block <= window_end_block
    )
);

create index if not exists idx_behavior_analysis_runs_worker
    on behavior_analysis_runs (worker_id, chain_id, started_at desc);
create index if not exists idx_behavior_analysis_runs_status
    on behavior_analysis_runs (status, started_at desc);

create table if not exists behavior_assertion_reviews (
    review_id uuid primary key default gen_random_uuid(),
    assertion_id uuid not null unique
        references address_label_assertions (assertion_id) on delete cascade,
    chain_id bigint not null,
    address text not null,
    label_key text not null references label_definitions (label_key),
    status text not null default 'pending',
    priority integer not null default 50,
    evidence_snapshot jsonb not null default '{}'::jsonb,
    resolved_by text,
    resolution_reason text,
    reviewed_at timestamptz,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    constraint behavior_assertion_reviews_status check (
        status in ('pending', 'accepted', 'rejected', 'retracted')
    ),
    constraint behavior_assertion_reviews_address check (address = lower(address))
);

create index if not exists idx_behavior_assertion_reviews_queue
    on behavior_assertion_reviews (status, priority, created_at);
create index if not exists idx_behavior_assertion_reviews_address
    on behavior_assertion_reviews (chain_id, address, updated_at desc);

create table if not exists behavior_assertion_review_events (
    event_id uuid primary key default gen_random_uuid(),
    review_id uuid not null
        references behavior_assertion_reviews (review_id) on delete cascade,
    assertion_id uuid not null
        references address_label_assertions (assertion_id) on delete cascade,
    action text not null,
    actor text not null,
    rationale text,
    previous_status text,
    new_status text not null,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    constraint behavior_assertion_review_events_action check (
        action in ('queued', 'accept', 'reject', 'retract')
    )
);

create index if not exists idx_behavior_assertion_review_events_review
    on behavior_assertion_review_events (review_id, created_at desc);
