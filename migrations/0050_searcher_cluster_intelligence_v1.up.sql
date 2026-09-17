create table if not exists candidate_execution_clusters (
    cluster_id uuid primary key default gen_random_uuid(),
    chain_id bigint not null check (chain_id > 0),
    seed_address text not null,
    cluster_kind text not null default 'searcher_candidate',
    current_status text not null default 'queued',
    progress jsonb not null default '{"percent":0,"stage":"queued","message":"Queued for seed classification"}'::jsonb,
    checkpoint jsonb not null default '{}'::jsonb,
    analysis_started_at timestamptz,
    analysis_completed_at timestamptz,
    last_processed_block bigint,
    last_incremental_update timestamptz,
    retry_count integer not null default 0,
    available_at timestamptz not null default now(),
    claimed_at timestamptz,
    error text,
    contract_version text not null default 'searcher-cluster-intelligence.v1',
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (chain_id, seed_address),
    foreign key (chain_id, seed_address)
        references observed_address_objects(chain_id, address) on delete cascade,
    constraint candidate_execution_clusters_seed check (
        seed_address = lower(seed_address) and seed_address ~ '^0x[0-9a-f]{40}$'
    ),
    constraint candidate_execution_clusters_kind check (
        cluster_kind in ('searcher_candidate', 'execution_candidate', 'unresolved')
    ),
    constraint candidate_execution_clusters_status check (
        current_status in (
            'queued', 'seed_classification', 'relationship_discovery',
            'cluster_building', 'execution_reconstruction',
            'economic_verification', 'baseline_building',
            'ready', 'stale', 'failed'
        )
    ),
    constraint candidate_execution_clusters_retry check (retry_count >= 0)
);

create index if not exists idx_candidate_execution_clusters_claim
    on candidate_execution_clusters (chain_id, available_at, updated_at)
    where current_status not in ('ready', 'failed');
create index if not exists idx_candidate_execution_clusters_incremental
    on candidate_execution_clusters (chain_id, last_processed_block, last_incremental_update)
    where current_status = 'ready';

alter table observed_address_objects
    add column if not exists seed_address text,
    add column if not exists seed_role text,
    add column if not exists seed_role_confidence double precision,
    add column if not exists seed_role_evidence jsonb not null default '{}'::jsonb,
    add column if not exists analysis_scope text not null default 'seed_and_candidate_execution_cluster',
    add column if not exists candidate_cluster_id uuid;

update observed_address_objects set seed_address = address where seed_address is null;
alter table observed_address_objects alter column seed_address set not null;

do $$ begin
    alter table observed_address_objects
        add constraint observed_address_objects_seed_role check (
            seed_role is null or seed_role in (
                'funding_treasury_candidate', 'worker_candidate',
                'executor_candidate', 'settlement_recipient_candidate',
                'router_candidate', 'ordinary_address', 'unknown'
            )
        );
exception when duplicate_object then null; end $$;

do $$ begin
    alter table observed_address_objects
        add constraint observed_address_objects_seed_confidence check (
            seed_role_confidence is null or
            (seed_role_confidence >= 0 and seed_role_confidence <= 1)
        );
exception when duplicate_object then null; end $$;

do $$ begin
    alter table observed_address_objects
        add constraint observed_address_objects_candidate_cluster_fk
        foreign key (candidate_cluster_id)
        references candidate_execution_clusters(cluster_id) on delete set null;
exception when duplicate_object then null; end $$;

create table if not exists candidate_execution_cluster_members (
    cluster_id uuid not null references candidate_execution_clusters(cluster_id) on delete cascade,
    chain_id bigint not null,
    address text not null,
    candidate_role text not null,
    membership_state text not null default 'candidate',
    evidence_state text not null default 'candidate',
    membership_confidence double precision not null,
    evidence jsonb not null,
    supporting_tx_hashes text[] not null default array[]::text[],
    include_in_economic_scope boolean not null default false,
    include_in_behavior_scope boolean not null default true,
    discovery_depth smallint not null default 0,
    first_seen_at timestamptz,
    last_seen_at timestamptz,
    user_overridden boolean not null default false,
    override_note text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (cluster_id, address),
    constraint candidate_execution_cluster_members_address check (
        address = lower(address) and address ~ '^0x[0-9a-f]{40}$'
    ),
    constraint candidate_execution_cluster_members_role check (
        candidate_role in (
            'seed', 'funding_treasury_candidate', 'worker_candidate',
            'executor_candidate', 'settlement_recipient_candidate',
            'profit_recipient_candidate', 'operational_contract_candidate',
            'unresolved'
        )
    ),
    constraint candidate_execution_cluster_members_state check (
        membership_state in ('candidate', 'included', 'excluded')
    ),
    constraint candidate_execution_cluster_members_evidence_state check (
        evidence_state in ('observed', 'candidate', 'inferred', 'unverified')
    ),
    constraint candidate_execution_cluster_members_confidence check (
        membership_confidence >= 0 and membership_confidence <= 1
    ),
    constraint candidate_execution_cluster_members_depth check (
        discovery_depth between 0 and 2
    )
);

create index if not exists idx_candidate_cluster_members_address
    on candidate_execution_cluster_members (chain_id, address, membership_state);
create index if not exists idx_candidate_cluster_members_role
    on candidate_execution_cluster_members (cluster_id, candidate_role, membership_confidence desc);

create table if not exists candidate_execution_cluster_relationships (
    relationship_id uuid primary key default gen_random_uuid(),
    cluster_id uuid not null references candidate_execution_clusters(cluster_id) on delete cascade,
    chain_id bigint not null,
    source_address text not null,
    related_address text not null,
    relationship_type text not null,
    evidence_state text not null default 'candidate',
    interaction_count bigint not null,
    value_transferred jsonb not null default '[]'::jsonb,
    first_seen_at timestamptz,
    last_seen_at timestamptz,
    supporting_tx_hashes text[] not null default array[]::text[],
    confidence double precision not null,
    evidence_reason text not null,
    evidence jsonb not null default '{}'::jsonb,
    discovery_depth smallint not null default 1,
    method_version text not null default 'cluster-relationship-expansion.v1',
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (cluster_id, source_address, related_address, relationship_type),
    constraint candidate_cluster_relationships_addresses check (
        source_address = lower(source_address)
        and source_address ~ '^0x[0-9a-f]{40}$'
        and related_address = lower(related_address)
        and related_address ~ '^0x[0-9a-f]{40}$'
        and source_address <> related_address
    ),
    constraint candidate_cluster_relationships_type check (
        relationship_type in (
            'funded_address', 'funded_by', 'frequent_call_target', 'called_by',
            'contract_deployer', 'executor_candidate', 'worker_candidate',
            'settlement_recipient_candidate', 'profit_recipient_candidate',
            'router', 'pool', 'protocol', 'repeated_counterparty',
            'co_execution_relationship'
        )
    ),
    constraint candidate_cluster_relationships_evidence_state check (
        evidence_state in ('observed', 'candidate', 'inferred', 'unverified')
    ),
    constraint candidate_cluster_relationships_count check (interaction_count > 0),
    constraint candidate_cluster_relationships_confidence check (
        confidence >= 0 and confidence <= 1
    ),
    constraint candidate_cluster_relationships_depth check (
        discovery_depth between 1 and 2
    ),
    constraint candidate_cluster_relationships_reason check (
        length(trim(evidence_reason)) > 0
    )
);

create index if not exists idx_candidate_cluster_relationships_rank
    on candidate_execution_cluster_relationships
    (cluster_id, relationship_type, interaction_count desc, confidence desc);

create table if not exists candidate_cluster_executions (
    cluster_id uuid not null references candidate_execution_clusters(cluster_id) on delete cascade,
    chain_id bigint not null,
    tx_hash text not null,
    block_number bigint not null,
    block_timestamp timestamptz,
    worker_address text,
    executor_address text,
    execution_type text not null,
    transaction_status text not null,
    protocols jsonb not null default '[]'::jsonb,
    pools jsonb not null default '[]'::jsonb,
    route jsonb not null default '[]'::jsonb,
    input_assets jsonb not null default '[]'::jsonb,
    output_assets jsonb not null default '[]'::jsonb,
    gas_cost_raw text,
    settlement_transfers jsonb not null default '[]'::jsonb,
    candidate_profit_recipient text,
    economic_verification_status text not null default 'unknown',
    verified_gross_profit jsonb not null default '[]'::jsonb,
    verified_gas_cost_raw text,
    verified_net_result jsonb not null default '[]'::jsonb,
    exclusion_reason text,
    evidence_refs jsonb not null default '[]'::jsonb,
    inputs_digest text not null,
    reconstructed_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (cluster_id, tx_hash),
    constraint candidate_cluster_executions_type check (
        execution_type in (
            'dex_swap', 'multi_pool_swap', 'closed_cycle_arbitrage',
            'liquidation', 'liquidation_attempt', 'failed_execution',
            'funding', 'settlement', 'rebalance', 'approval',
            'contract_deployment', 'unknown_interaction'
        )
    ),
    constraint candidate_cluster_executions_status check (
        transaction_status in ('success', 'failure', 'pending')
    ),
    constraint candidate_cluster_executions_economic check (
        economic_verification_status in ('verified', 'estimated', 'unknown', 'excluded')
    )
);

create index if not exists idx_candidate_cluster_executions_timeline
    on candidate_cluster_executions (cluster_id, block_timestamp desc, block_number desc);
create index if not exists idx_candidate_cluster_executions_economics
    on candidate_cluster_executions
    (cluster_id, economic_verification_status, block_timestamp desc);

create table if not exists candidate_cluster_window_summaries (
    cluster_id uuid not null references candidate_execution_clusters(cluster_id) on delete cascade,
    chain_id bigint not null,
    window_key text not null,
    window_start timestamptz not null,
    window_end timestamptz not null,
    execution_count bigint not null default 0,
    success_count bigint not null default 0,
    failure_count bigint not null default 0,
    protocol_count bigint not null default 0,
    pool_count bigint not null default 0,
    gas_cost_raw text,
    verified_gross_profit jsonb not null default '[]'::jsonb,
    verified_gas_cost_raw text,
    verified_net_result jsonb not null default '[]'::jsonb,
    verified_execution_count bigint not null default 0,
    observed_settlement_inflows jsonb not null default '[]'::jsonb,
    capital_sent_to_workers jsonb not null default '[]'::jsonb,
    capital_returned jsonb not null default '[]'::jsonb,
    net_funding_flow jsonb not null default '[]'::jsonb,
    failed_tx_count bigint not null default 0,
    failed_gas_cost_raw text,
    spray_attempt_cost_raw text,
    strategy_distribution jsonb not null default '[]'::jsonb,
    top_workers jsonb not null default '[]'::jsonb,
    top_executors jsonb not null default '[]'::jsonb,
    top_protocols jsonb not null default '[]'::jsonb,
    top_routes jsonb not null default '[]'::jsonb,
    new_relationship_count bigint not null default 0,
    last_active_at timestamptz,
    included_transaction_count bigint not null default 0,
    excluded_transaction_count bigint not null default 0,
    exclusion_reasons jsonb not null default '[]'::jsonb,
    coverage_ratio double precision not null default 0,
    coverage jsonb not null default '{}'::jsonb,
    inputs_digest text not null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (cluster_id, window_key),
    constraint candidate_cluster_window_key check (
        window_key in ('24h', '7d', '30d', 'previous_7d', 'previous_30d')
    ),
    constraint candidate_cluster_window_counts check (
        execution_count >= 0 and success_count >= 0 and failure_count >= 0
        and verified_execution_count >= 0 and included_transaction_count >= 0
        and excluded_transaction_count >= 0 and failed_tx_count >= 0
    ),
    constraint candidate_cluster_window_coverage check (
        coverage_ratio >= 0 and coverage_ratio <= 1
    )
);

create table if not exists candidate_cluster_change_signals (
    signal_id uuid primary key default gen_random_uuid(),
    cluster_id uuid not null references candidate_execution_clusters(cluster_id) on delete cascade,
    chain_id bigint not null,
    signal_type text not null,
    status text not null default 'detected',
    severity text not null,
    conclusion text not null,
    current_value jsonb,
    baseline_value jsonb,
    supporting_tx_hashes text[] not null default array[]::text[],
    confidence double precision not null,
    inputs_digest text not null,
    detected_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (cluster_id, signal_type, inputs_digest),
    constraint candidate_cluster_change_type check (
        signal_type in (
            'activity_change', 'new_protocol', 'new_counterparty', 'new_worker',
            'new_executor', 'new_settlement_recipient', 'strategy_change',
            'success_rate_change', 'gas_cost_change', 'verified_profit_change',
            'failure_rate_change'
        )
    ),
    constraint candidate_cluster_change_status check (
        status in ('detected', 'acknowledged', 'resolved', 'dismissed')
    ),
    constraint candidate_cluster_change_severity check (
        severity in ('low', 'medium', 'high', 'critical')
    ),
    constraint candidate_cluster_change_confidence check (
        confidence >= 0 and confidence <= 1
    )
);

create table if not exists candidate_cluster_member_overrides (
    override_id uuid primary key default gen_random_uuid(),
    cluster_id uuid not null references candidate_execution_clusters(cluster_id) on delete cascade,
    chain_id bigint not null,
    address text not null,
    action text not null,
    previous_role text,
    selected_role text,
    previous_membership_state text,
    selected_membership_state text,
    note text,
    actor_user_id uuid,
    created_at timestamptz not null default now(),
    constraint candidate_cluster_member_overrides_action check (
        action in ('include', 'exclude', 'mark_role', 'keep_candidate')
    )
);

comment on table candidate_execution_clusters is
    'Evidence-scored candidate execution systems. A row is not an ownership or same-team assertion.';
comment on table candidate_execution_cluster_members is
    'Candidate operational membership with independent behavior/economic scope flags and evidence.';
comment on table candidate_cluster_executions is
    'Cluster-scoped execution reconstruction. Funding and settlement remain distinct from verified profit.';

