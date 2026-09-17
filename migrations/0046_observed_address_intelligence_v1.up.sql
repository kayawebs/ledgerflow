create table if not exists observed_address_objects (
    chain_id bigint not null check (chain_id > 0),
    address text not null,
    object_type text not null default 'unknown',
    current_status text not null default 'queued',
    progress jsonb not null default '{"percent":0,"transactions_discovered":0,"transactions_analyzed":0,"receipts_processed":0,"traces_processed":0,"economic_verified":0}'::jsonb,
    analysis_started_at timestamptz,
    analysis_completed_at timestamptz,
    history_start_block bigint,
    target_block bigint,
    last_processed_block bigint,
    last_incremental_update timestamptz,
    coverage_start_at timestamptz,
    coverage_end_at timestamptz,
    discovered_transaction_count bigint not null default 0,
    analyzed_transaction_count bigint not null default 0,
    approximate_total_transaction_count bigint,
    coverage_is_partial boolean not null default false,
    monitoring_enabled boolean not null default true,
    error text,
    retry_count integer not null default 0,
    available_at timestamptz not null default now(),
    claimed_at timestamptz,
    last_activity_at timestamptz,
    contract_version text not null default 'observed-address-intelligence.v1',
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (chain_id, address),
    constraint observed_address_objects_address check (
        address = lower(address) and address ~ '^0x[0-9a-f]{40}$'
    ),
    constraint observed_address_objects_status check (
        current_status in (
            'queued', 'discovering', 'backfilling', 'classifying',
            'verifying_economics', 'building_baseline', 'ready', 'stale', 'failed'
        )
    ),
    constraint observed_address_objects_counts check (
        discovered_transaction_count >= 0 and analyzed_transaction_count >= 0
        and retry_count >= 0
    )
);

create index if not exists idx_observed_address_objects_claim
    on observed_address_objects (chain_id, available_at, updated_at)
    where current_status in (
        'queued', 'discovering', 'backfilling', 'classifying',
        'verifying_economics', 'building_baseline', 'stale', 'failed'
    );

create index if not exists idx_observed_address_objects_incremental
    on observed_address_objects (chain_id, last_processed_block, last_incremental_update)
    where current_status = 'ready' and monitoring_enabled;

create table if not exists observation_analysis_runs (
    run_id uuid primary key default gen_random_uuid(),
    chain_id bigint not null,
    address text not null,
    run_kind text not null default 'initial',
    status text not null default 'queued',
    stage text not null default 'queued',
    start_block bigint,
    end_block bigint,
    discovered_transaction_count bigint not null default 0,
    analyzed_transaction_count bigint not null default 0,
    excluded_transaction_count bigint not null default 0,
    progress jsonb not null default '{}'::jsonb,
    error text,
    started_at timestamptz,
    completed_at timestamptz,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    foreign key (chain_id, address)
        references observed_address_objects(chain_id, address) on delete cascade,
    constraint observation_analysis_runs_kind check (
        run_kind in ('initial', 'incremental', 'retry', 'continuation')
    ),
    constraint observation_analysis_runs_status check (
        status in ('queued', 'running', 'completed', 'failed')
    )
);

create index if not exists idx_observation_analysis_runs_object
    on observation_analysis_runs (chain_id, address, created_at desc);

create table if not exists object_activity_transactions (
    chain_id bigint not null,
    address text not null,
    tx_hash text not null,
    block_number bigint not null,
    tx_index integer not null default 0,
    block_timestamp timestamptz,
    processing_status text not null default 'discovered',
    transaction_status text,
    from_address text,
    to_address text,
    subject_role text not null default 'observed_participant',
    action_label text,
    action_confidence double precision,
    protocol text,
    counterparty text,
    native_value_raw text,
    gas_cost_raw text,
    token_inflows jsonb not null default '[]'::jsonb,
    token_outflows jsonb not null default '[]'::jsonb,
    touched_contracts jsonb not null default '[]'::jsonb,
    touched_pools jsonb not null default '[]'::jsonb,
    protocol_relationships jsonb not null default '[]'::jsonb,
    economic_result_status text not null default 'unknown',
    verified_gross_surplus_raw text,
    verified_gas_cost_raw text,
    verified_net_result_raw text,
    verified_result_asset text,
    exclusion_reason text,
    evidence_refs jsonb not null default '[]'::jsonb,
    analysis_input_digest text,
    attempts integer not null default 0,
    last_error text,
    discovered_at timestamptz not null default now(),
    processed_at timestamptz,
    updated_at timestamptz not null default now(),
    primary key (chain_id, address, tx_hash),
    foreign key (chain_id, address)
        references observed_address_objects(chain_id, address) on delete cascade,
    constraint object_activity_transactions_status check (
        processing_status in ('discovered', 'analyzing', 'analyzed', 'excluded', 'failed')
    ),
    constraint object_activity_transactions_economic check (
        economic_result_status in ('verified', 'estimated', 'unknown')
    )
);

create index if not exists idx_object_activity_transactions_timeline
    on object_activity_transactions (chain_id, address, block_timestamp desc, block_number desc);

create index if not exists idx_object_activity_transactions_pending
    on object_activity_transactions (chain_id, address, block_number desc)
    where processing_status in ('discovered', 'failed');

create table if not exists object_activity_summaries (
    chain_id bigint not null,
    address text not null,
    window_key text not null,
    window_start timestamptz not null,
    window_end timestamptz not null,
    transaction_count bigint not null default 0,
    success_count bigint not null default 0,
    reverted_count bigint not null default 0,
    gas_cost_raw text,
    native_inflow_raw text,
    native_outflow_raw text,
    token_inflows jsonb not null default '[]'::jsonb,
    token_outflows jsonb not null default '[]'::jsonb,
    top_call_targets jsonb not null default '[]'::jsonb,
    touched_contracts jsonb not null default '[]'::jsonb,
    touched_pools jsonb not null default '[]'::jsonb,
    protocol_count bigint not null default 0,
    counterparty_count bigint not null default 0,
    first_seen_at timestamptz,
    last_active_at timestamptz,
    coverage jsonb not null default '{}'::jsonb,
    inputs_digest text not null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (chain_id, address, window_key),
    foreign key (chain_id, address)
        references observed_address_objects(chain_id, address) on delete cascade,
    constraint object_activity_summaries_window check (
        window_key in ('24h', '7d', '30d', 'previous_7d', 'previous_30d')
    )
);

create table if not exists object_behavior_profiles (
    chain_id bigint not null,
    address text not null,
    label text not null,
    confidence double precision not null,
    supporting_transaction_count bigint not null,
    supporting_tx_hashes text[] not null default array[]::text[],
    first_observed_at timestamptz,
    last_observed_at timestamptz,
    evidence_coverage double precision not null default 0,
    method_version text not null,
    metadata jsonb not null default '{}'::jsonb,
    updated_at timestamptz not null default now(),
    primary key (chain_id, address, label),
    foreign key (chain_id, address)
        references observed_address_objects(chain_id, address) on delete cascade,
    constraint object_behavior_profiles_confidence check (
        confidence >= 0 and confidence <= 1 and evidence_coverage >= 0 and evidence_coverage <= 1
    )
);

create table if not exists object_relationships (
    chain_id bigint not null,
    address text not null,
    related_address text not null,
    relationship_type text not null,
    evidence_state text not null default 'observed',
    interaction_count bigint not null,
    supporting_tx_hashes text[] not null default array[]::text[],
    first_seen_at timestamptz,
    last_seen_at timestamptz,
    confidence double precision not null,
    method_version text not null,
    metadata jsonb not null default '{}'::jsonb,
    updated_at timestamptz not null default now(),
    primary key (chain_id, address, related_address, relationship_type),
    foreign key (chain_id, address)
        references observed_address_objects(chain_id, address) on delete cascade,
    constraint object_relationships_type check (
        relationship_type in (
            'frequent_call_target', 'funding_source', 'executor_candidate',
            'settlement_recipient', 'profit_recipient_candidate', 'router',
            'pool', 'protocol', 'co_occurring_address'
        )
    ),
    constraint object_relationships_evidence_state check (
        evidence_state in ('observed', 'possible', 'candidate')
    ),
    constraint object_relationships_confidence check (
        confidence >= 0 and confidence <= 1
    )
);

create index if not exists idx_object_relationships_rank
    on object_relationships (chain_id, address, interaction_count desc, last_seen_at desc);

create table if not exists object_economic_summaries (
    chain_id bigint not null,
    address text not null,
    window_key text not null,
    result_status text not null default 'unknown',
    verified_gross_surplus jsonb not null default '[]'::jsonb,
    verified_gas_cost_raw text,
    verified_net_result jsonb not null default '[]'::jsonb,
    observed_net_token_flow jsonb not null default '[]'::jsonb,
    economically_verified_transaction_count bigint not null default 0,
    excluded_transaction_count bigint not null default 0,
    exclusion_reasons jsonb not null default '{}'::jsonb,
    coverage_ratio double precision not null default 0,
    limitations text[] not null default array[]::text[],
    inputs_digest text not null,
    updated_at timestamptz not null default now(),
    primary key (chain_id, address, window_key),
    foreign key (chain_id, address)
        references observed_address_objects(chain_id, address) on delete cascade,
    constraint object_economic_summaries_status check (
        result_status in ('verified', 'estimated', 'unknown')
    ),
    constraint object_economic_summaries_coverage check (
        coverage_ratio >= 0 and coverage_ratio <= 1
    )
);

create table if not exists object_change_signals (
    signal_id uuid primary key default gen_random_uuid(),
    chain_id bigint not null,
    address text not null,
    signal_type text not null,
    status text not null default 'detected',
    severity text not null default 'low',
    conclusion text not null,
    current_value jsonb,
    baseline_value jsonb,
    supporting_tx_hashes text[] not null default array[]::text[],
    confidence double precision not null,
    detected_at timestamptz not null default now(),
    inputs_digest text not null,
    metadata jsonb not null default '{}'::jsonb,
    foreign key (chain_id, address)
        references observed_address_objects(chain_id, address) on delete cascade,
    unique (chain_id, address, signal_type, inputs_digest),
    constraint object_change_signals_status check (
        status in ('detected', 'acknowledged', 'resolved', 'dismissed')
    ),
    constraint object_change_signals_severity check (
        severity in ('low', 'medium', 'high', 'critical')
    )
);

create index if not exists idx_object_change_signals_recent
    on object_change_signals (chain_id, address, detected_at desc);

create table if not exists object_briefs (
    brief_id uuid primary key default gen_random_uuid(),
    chain_id bigint not null,
    address text not null,
    brief_type text not null,
    period_start timestamptz not null,
    period_end timestamptz not null,
    title text not null,
    markdown_content text not null,
    html_content text not null,
    inputs_digest text not null,
    generated_at timestamptz not null default now(),
    metadata jsonb not null default '{}'::jsonb,
    foreign key (chain_id, address)
        references observed_address_objects(chain_id, address) on delete cascade,
    unique (chain_id, address, brief_type, period_start, period_end, inputs_digest),
    constraint object_briefs_type check (
        brief_type in ('24h_summary', 'weekly', 'monthly')
    )
);

alter table workspace_entities
    add column if not exists observation_enabled boolean not null default true,
    add column if not exists observation_status text not null default 'queued',
    add column if not exists observation_progress jsonb not null default '{"percent":0}'::jsonb,
    add column if not exists observation_error text,
    add column if not exists last_observation_sync_at timestamptz;

comment on table observed_address_objects is
    'Chain-plane reusable Observed Address Intelligence v1 state. It is independent of customer workspace ownership.';

comment on table object_activity_transactions is
    'Deterministic object activity ledger. Rows exist independently of Material Events.';

comment on table object_economic_summaries is
    'Verified/estimated/unknown object economics. Funding and token inflow are never implicitly profit.';
