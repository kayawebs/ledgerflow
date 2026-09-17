create table if not exists protocol_cluster_projection_checkpoints (
    domain_id uuid not null,
    chain_id bigint not null,
    source_kind text not null,
    last_source_updated_at timestamptz,
    last_run_at timestamptz,
    status text not null default 'idle',
    last_error text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (domain_id, source_kind),
    foreign key (domain_id, chain_id)
        references protocol_intelligence_domains(domain_id, chain_id) on delete cascade,
    constraint protocol_cluster_projection_source check (
        source_kind in ('launch_trades', 'verified_arbitrage')
    ),
    constraint protocol_cluster_projection_status check (
        status in ('idle', 'running', 'ready', 'error')
    )
);

create index if not exists idx_protocol_launches_creator_cluster
    on protocol_token_launches (domain_id, creator_address, launch_id)
    where creator_address is not null;

create index if not exists idx_protocol_launch_trade_cluster_projection
    on protocol_launch_trade_facts
        (domain_id, updated_at, launch_id, trader_address)
    where is_canonical and trade_side = 'buy';

create index if not exists idx_protocol_cluster_members_cluster_rank
    on protocol_scoped_cluster_members
        (cluster_id, member_role, confidence desc, interaction_count desc);

comment on table protocol_cluster_projection_checkpoints is
    'High-water marks for LedgerFlow-owned protocol-scoped cluster projection. No global address-similarity clustering is performed.';
