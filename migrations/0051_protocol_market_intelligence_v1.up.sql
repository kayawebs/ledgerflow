create table if not exists protocol_intelligence_domains (
    domain_id uuid primary key default gen_random_uuid(),
    domain_key text not null unique,
    chain_id bigint not null check (chain_id > 0),
    protocol_key text not null references protocols(protocol_key),
    display_name_en text not null,
    display_name_zh text not null,
    domain_kind text not null,
    status text not null default 'active',
    cluster_method_version text not null,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (domain_id, chain_id),
    constraint protocol_intelligence_domains_kind check (
        domain_kind in ('token_launch', 'dex_execution', 'liquidation')
    ),
    constraint protocol_intelligence_domains_status check (
        status in ('active', 'paused', 'retired')
    )
);

create index if not exists idx_protocol_intelligence_domains_chain
    on protocol_intelligence_domains (chain_id, status, domain_kind);

create table if not exists protocol_seed_addresses (
    seed_id uuid primary key default gen_random_uuid(),
    domain_id uuid not null,
    chain_id bigint not null,
    address text not null,
    seed_role text not null,
    label_en text not null,
    label_zh text not null,
    evidence_state text not null default 'observed',
    confidence double precision not null,
    provenance_url text not null,
    source_key text not null references intelligence_sources(source_key),
    valid_from_block bigint,
    valid_to_block bigint,
    evidence jsonb not null default '{}'::jsonb,
    status text not null default 'active',
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    foreign key (domain_id, chain_id)
        references protocol_intelligence_domains(domain_id, chain_id) on delete cascade,
    unique (domain_id, address, seed_role),
    constraint protocol_seed_addresses_address check (
        address = lower(address) and address ~ '^0x[0-9a-f]{40}$'
    ),
    constraint protocol_seed_addresses_role check (
        seed_role in (
            'factory', 'router', 'pool_manager', 'position_manager', 'locker',
            'quoter', 'quote_asset', 'fee_recipient', 'oracle', 'lending_pool'
        )
    ),
    constraint protocol_seed_addresses_evidence_state check (
        evidence_state in ('observed', 'candidate', 'inferred', 'unverified')
    ),
    constraint protocol_seed_addresses_confidence check (
        confidence >= 0 and confidence <= 1
    ),
    constraint protocol_seed_addresses_status check (
        status in ('active', 'superseded', 'retired')
    ),
    constraint protocol_seed_addresses_block_order check (
        valid_from_block is null or valid_to_block is null
        or valid_from_block <= valid_to_block
    )
);

create index if not exists idx_protocol_seed_addresses_lookup
    on protocol_seed_addresses (chain_id, domain_id, seed_role, status);

create table if not exists protocol_cluster_runs (
    run_id uuid primary key default gen_random_uuid(),
    domain_id uuid not null,
    chain_id bigint not null,
    cursor_block bigint,
    status text not null default 'queued',
    progress jsonb not null default '{}'::jsonb,
    error text,
    started_at timestamptz,
    completed_at timestamptz,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    foreign key (domain_id, chain_id)
        references protocol_intelligence_domains(domain_id, chain_id) on delete cascade,
    constraint protocol_cluster_runs_status check (
        status in ('queued', 'discovering', 'clustering', 'accounting', 'ready', 'failed')
    )
);

create table if not exists protocol_scoped_clusters (
    cluster_id uuid primary key default gen_random_uuid(),
    domain_id uuid not null,
    chain_id bigint not null,
    cluster_key text not null,
    cluster_kind text not null,
    subject_address text,
    evidence_state text not null default 'candidate',
    confidence double precision not null,
    method_version text not null,
    evidence jsonb not null default '{}'::jsonb,
    status text not null default 'active',
    first_seen_at timestamptz,
    last_seen_at timestamptz,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    foreign key (domain_id, chain_id)
        references protocol_intelligence_domains(domain_id, chain_id) on delete cascade,
    unique (domain_id, cluster_key),
    unique (cluster_id, domain_id, chain_id),
    constraint protocol_scoped_clusters_kind check (
        cluster_kind in ('issuer_cohort', 'buyer_cohort', 'searcher', 'liquidator')
    ),
    constraint protocol_scoped_clusters_subject check (
        subject_address is null or
        (subject_address = lower(subject_address) and subject_address ~ '^0x[0-9a-f]{40}$')
    ),
    constraint protocol_scoped_clusters_evidence_state check (
        evidence_state in ('observed', 'candidate', 'inferred', 'unverified')
    ),
    constraint protocol_scoped_clusters_confidence check (
        confidence >= 0 and confidence <= 1
    ),
    constraint protocol_scoped_clusters_status check (
        status in ('active', 'dismissed', 'superseded')
    )
);

create index if not exists idx_protocol_scoped_clusters_rank
    on protocol_scoped_clusters (chain_id, domain_id, status, confidence desc, last_seen_at desc);

create table if not exists protocol_scoped_cluster_members (
    cluster_id uuid not null,
    domain_id uuid not null,
    chain_id bigint not null,
    address text not null,
    member_role text not null,
    evidence_state text not null,
    confidence double precision not null,
    interaction_count bigint not null default 0,
    include_in_behavior_scope boolean not null default true,
    include_in_economic_scope boolean not null default false,
    supporting_tx_hashes text[] not null default array[]::text[],
    evidence jsonb not null default '{}'::jsonb,
    first_seen_at timestamptz,
    last_seen_at timestamptz,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (cluster_id, address),
    foreign key (cluster_id, domain_id, chain_id)
        references protocol_scoped_clusters(cluster_id, domain_id, chain_id) on delete cascade,
    constraint protocol_scoped_cluster_members_address check (
        address = lower(address) and address ~ '^0x[0-9a-f]{40}$'
    ),
    constraint protocol_scoped_cluster_members_role check (
        member_role in (
            'creator', 'deployer', 'launch_buyer', 'repeat_buyer', 'funder',
            'settlement_recipient', 'worker', 'executor', 'liquidator',
            'router', 'pool', 'protocol_contract', 'factory', 'unresolved'
        )
    ),
    constraint protocol_scoped_cluster_members_evidence_state check (
        evidence_state in ('observed', 'candidate', 'inferred', 'unverified')
    ),
    constraint protocol_scoped_cluster_members_confidence check (
        confidence >= 0 and confidence <= 1
    ),
    constraint protocol_scoped_cluster_members_count check (interaction_count >= 0),
    constraint protocol_scoped_cluster_members_economic_scope check (
        not include_in_economic_scope or member_role not in
            ('router', 'pool', 'protocol_contract', 'factory')
    )
);

create index if not exists idx_protocol_scoped_cluster_members_address
    on protocol_scoped_cluster_members (chain_id, domain_id, address);

create table if not exists protocol_scoped_cluster_edges (
    edge_id uuid primary key default gen_random_uuid(),
    cluster_id uuid not null,
    domain_id uuid not null,
    chain_id bigint not null,
    source_address text not null,
    target_address text not null,
    relationship_type text not null,
    evidence_state text not null,
    confidence double precision not null,
    interaction_count bigint not null,
    value_transferred jsonb not null default '[]'::jsonb,
    supporting_tx_hashes text[] not null default array[]::text[],
    evidence_reason text not null,
    evidence jsonb not null default '{}'::jsonb,
    first_seen_at timestamptz,
    last_seen_at timestamptz,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    foreign key (cluster_id, domain_id, chain_id)
        references protocol_scoped_clusters(cluster_id, domain_id, chain_id) on delete cascade,
    unique (cluster_id, source_address, target_address, relationship_type),
    constraint protocol_scoped_cluster_edges_addresses check (
        source_address = lower(source_address)
        and source_address ~ '^0x[0-9a-f]{40}$'
        and target_address = lower(target_address)
        and target_address ~ '^0x[0-9a-f]{40}$'
        and source_address <> target_address
    ),
    constraint protocol_scoped_cluster_edges_relationship check (
        relationship_type in (
            'created_launch', 'bought_launch', 'repeated_launch_buyer',
            'funded_by', 'funded', 'called', 'executed_through',
            'settled_to', 'traded_in_pool', 'co_participated'
        )
    ),
    constraint protocol_scoped_cluster_edges_evidence_state check (
        evidence_state in ('observed', 'candidate', 'inferred', 'unverified')
    ),
    constraint protocol_scoped_cluster_edges_confidence check (
        confidence >= 0 and confidence <= 1
    ),
    constraint protocol_scoped_cluster_edges_count check (interaction_count > 0),
    constraint protocol_scoped_cluster_edges_reason check (length(trim(evidence_reason)) > 0)
);

create table if not exists protocol_token_launches (
    launch_id uuid primary key default gen_random_uuid(),
    domain_id uuid not null,
    chain_id bigint not null,
    token_address text not null,
    creator_address text,
    launch_tx_hash text not null,
    launch_block bigint not null,
    launched_at timestamptz,
    pool_address text,
    quote_token_address text,
    name text,
    symbol text,
    launch_status text not null default 'observed',
    issuer_cluster_id uuid,
    evidence jsonb not null default '{}'::jsonb,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    foreign key (domain_id, chain_id)
        references protocol_intelligence_domains(domain_id, chain_id) on delete cascade,
    foreign key (issuer_cluster_id, domain_id, chain_id)
        references protocol_scoped_clusters(cluster_id, domain_id, chain_id) on delete restrict,
    unique (domain_id, token_address),
    unique (launch_id, domain_id, chain_id),
    constraint protocol_token_launches_token check (
        token_address = lower(token_address) and token_address ~ '^0x[0-9a-f]{40}$'
    ),
    constraint protocol_token_launches_creator check (
        creator_address is null or
        (creator_address = lower(creator_address) and creator_address ~ '^0x[0-9a-f]{40}$')
    ),
    constraint protocol_token_launches_tx check (
        launch_tx_hash = lower(launch_tx_hash) and launch_tx_hash ~ '^0x[0-9a-f]{64}$'
    ),
    constraint protocol_token_launches_status check (
        launch_status in ('observed', 'trading', 'graduated', 'inactive', 'unverified')
    )
);

create index if not exists idx_protocol_token_launches_recent
    on protocol_token_launches (chain_id, domain_id, launched_at desc, launch_block desc);

create table if not exists protocol_launch_metric_snapshots (
    snapshot_id uuid primary key default gen_random_uuid(),
    launch_id uuid not null,
    domain_id uuid not null,
    chain_id bigint not null,
    window_key text not null,
    price_quote numeric,
    price_change_percent numeric,
    net_flow_quote numeric,
    inflow_velocity_quote_per_minute numeric,
    buy_volume_quote numeric,
    sell_volume_quote numeric,
    liquidity_quote numeric,
    unique_buyers bigint,
    unique_sellers bigint,
    issuer_cohort_buyers bigint,
    external_buyers bigint,
    unresolved_buyers bigint,
    verified_winning_participants bigint,
    verified_losing_participants bigint,
    outcome_coverage_ratio double precision,
    evidence_coverage_ratio double precision not null default 0,
    methodology_version text not null,
    limitations text[] not null default array[]::text[],
    evidence jsonb not null default '{}'::jsonb,
    measured_at timestamptz not null,
    created_at timestamptz not null default now(),
    foreign key (launch_id, domain_id, chain_id)
        references protocol_token_launches(launch_id, domain_id, chain_id) on delete cascade,
    unique (launch_id, window_key, measured_at),
    constraint protocol_launch_metric_snapshots_window check (
        window_key in ('5m', '15m', '1h', '6h', '24h', '7d', '30d', 'lifetime')
    ),
    constraint protocol_launch_metric_snapshots_counts check (
        coalesce(unique_buyers, 0) >= 0 and coalesce(unique_sellers, 0) >= 0
        and coalesce(issuer_cohort_buyers, 0) >= 0 and coalesce(external_buyers, 0) >= 0
        and coalesce(unresolved_buyers, 0) >= 0
        and coalesce(verified_winning_participants, 0) >= 0
        and coalesce(verified_losing_participants, 0) >= 0
    ),
    constraint protocol_launch_metric_snapshots_outcome_coverage check (
        outcome_coverage_ratio is null or
        (outcome_coverage_ratio >= 0 and outcome_coverage_ratio <= 1)
    ),
    constraint protocol_launch_metric_snapshots_evidence_coverage check (
        evidence_coverage_ratio >= 0 and evidence_coverage_ratio <= 1
    )
);

create index if not exists idx_protocol_launch_metric_snapshots_rank
    on protocol_launch_metric_snapshots
        (chain_id, domain_id, window_key, measured_at desc);

create table if not exists protocol_launch_participant_outcomes (
    outcome_id uuid primary key default gen_random_uuid(),
    launch_id uuid not null,
    domain_id uuid not null,
    chain_id bigint not null,
    address text not null,
    cohort text not null,
    result_status text not null,
    realized_result_quote numeric,
    gas_cost_native numeric,
    buy_count bigint not null default 0,
    sell_count bigint not null default 0,
    included_transaction_count bigint not null default 0,
    excluded_transaction_count bigint not null default 0,
    exclusion_reasons text[] not null default array[]::text[],
    evidence jsonb not null default '{}'::jsonb,
    methodology_version text not null,
    window_key text not null,
    measured_at timestamptz not null,
    created_at timestamptz not null default now(),
    foreign key (launch_id, domain_id, chain_id)
        references protocol_token_launches(launch_id, domain_id, chain_id) on delete cascade,
    unique (launch_id, address, window_key, measured_at),
    constraint protocol_launch_participant_outcomes_address check (
        address = lower(address) and address ~ '^0x[0-9a-f]{40}$'
    ),
    constraint protocol_launch_participant_outcomes_cohort check (
        cohort in ('issuer', 'related', 'external', 'unresolved')
    ),
    constraint protocol_launch_participant_outcomes_status check (
        result_status in ('verified', 'estimated', 'unknown', 'excluded')
    ),
    constraint protocol_launch_participant_outcomes_verified check (
        result_status <> 'verified' or realized_result_quote is not null
    ),
    constraint protocol_launch_participant_outcomes_window check (
        window_key in ('5m', '15m', '1h', '6h', '24h', '7d', '30d', 'lifetime')
    )
);

create index if not exists idx_protocol_launch_participant_outcomes_rank
    on protocol_launch_participant_outcomes
        (chain_id, domain_id, result_status, realized_result_quote desc, measured_at desc);

create table if not exists market_wallet_accounts (
    wallet_address text primary key,
    first_connected_at timestamptz not null default now(),
    last_connected_at timestamptz not null default now(),
    metadata jsonb not null default '{}'::jsonb,
    constraint market_wallet_accounts_address check (
        wallet_address = lower(wallet_address) and wallet_address ~ '^0x[0-9a-f]{40}$'
    )
);

create table if not exists market_wallet_challenges (
    challenge_id uuid primary key default gen_random_uuid(),
    wallet_address text not null,
    chain_id bigint not null,
    nonce text not null unique,
    message text not null,
    expires_at timestamptz not null,
    consumed_at timestamptz,
    created_at timestamptz not null default now(),
    constraint market_wallet_challenges_address check (
        wallet_address = lower(wallet_address) and wallet_address ~ '^0x[0-9a-f]{40}$'
    )
);

create index if not exists idx_market_wallet_challenges_active
    on market_wallet_challenges (wallet_address, expires_at desc)
    where consumed_at is null;

create table if not exists market_wallet_sessions (
    session_id uuid primary key default gen_random_uuid(),
    wallet_address text not null references market_wallet_accounts(wallet_address) on delete cascade,
    token_hash text not null unique,
    expires_at timestamptz not null,
    last_seen_at timestamptz not null default now(),
    revoked_at timestamptz,
    created_at timestamptz not null default now()
);

create index if not exists idx_market_wallet_sessions_active
    on market_wallet_sessions (wallet_address, expires_at desc)
    where revoked_at is null;

comment on table protocol_intelligence_domains is
    'An analytical boundary. Clusters and economic conclusions are valid only inside this chain/protocol domain.';
comment on table protocol_scoped_clusters is
    'Evidence-scored protocol-local relationship groups; never a global ownership or same-team assertion.';
comment on table protocol_launch_participant_outcomes is
    'Launch-scoped outcomes. Funding and asset inflow are not realized profit.';
