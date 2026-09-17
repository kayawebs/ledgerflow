insert into intelligence_sources (
    source_key, display_name, source_kind, trust_score, owner, metadata
)
values (
    'mev_seed_classifier',
    'Deterministic MEV seed candidate classifier',
    'model',
    0.78,
    'LedgerFlow',
    '{"claim_scope":"candidate_only","inputs":["arbitrage_candidates","behavior_assertion_reviews"],"method_version":"mev_seed_v1"}'::jsonb
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
    'actor:mev_candidate',
    'MEV actor candidate',
    'behavior',
    'An address selected for targeted analysis from repeated MEV-like execution evidence. This is not a verified MEV identity.',
    '{"requires_review":true,"candidate_only":true,"method_version":"mev_seed_v1"}'::jsonb
)
on conflict (label_key) do update set
    display_name = excluded.display_name,
    dimension = excluded.dimension,
    description = excluded.description,
    status = 'active',
    metadata = label_definitions.metadata || excluded.metadata,
    updated_at = now();

create table if not exists intelligence_seed_addresses (
    seed_id uuid primary key default gen_random_uuid(),
    chain_id bigint not null,
    address text not null,
    seed_kind text not null,
    roles text[] not null default array[]::text[],
    source_key text not null references intelligence_sources(source_key),
    status text not null default 'candidate',
    priority integer not null default 50,
    confidence double precision not null,
    evidence jsonb not null default '{}'::jsonb,
    evidence_digest text not null,
    first_seen_block bigint,
    last_seen_block bigint,
    first_seen_at timestamptz not null default now(),
    last_seen_at timestamptz not null default now(),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    constraint intelligence_seed_addresses_unique
        unique (chain_id, address, seed_kind, source_key),
    constraint intelligence_seed_addresses_address
        check (address = lower(address) and address ~ '^0x[0-9a-f]{40}$'),
    constraint intelligence_seed_addresses_kind check (
        seed_kind in (
            'customer', 'official_contract', 'protocol_contract', 'asset',
            'mev_actor', 'automated_actor', 'relationship'
        )
    ),
    constraint intelligence_seed_addresses_status check (
        status in ('candidate', 'active', 'rejected', 'retired')
    ),
    constraint intelligence_seed_addresses_priority check (
        priority between 0 and 100
    ),
    constraint intelligence_seed_addresses_confidence check (
        confidence >= 0 and confidence <= 1
    ),
    constraint intelligence_seed_addresses_block_order check (
        first_seen_block is null or last_seen_block is null
        or first_seen_block <= last_seen_block
    )
);

create index if not exists idx_intelligence_seed_addresses_queue
    on intelligence_seed_addresses (chain_id, status, priority, updated_at desc);
create index if not exists idx_intelligence_seed_addresses_address
    on intelligence_seed_addresses (chain_id, address, status);
create index if not exists idx_intelligence_seed_addresses_kind
    on intelligence_seed_addresses (chain_id, seed_kind, status, confidence desc);

create table if not exists seed_intelligence_runs (
    run_id uuid primary key default gen_random_uuid(),
    worker_id text not null,
    chain_id bigint not null,
    status text not null default 'running',
    curated_seed_changes bigint not null default 0,
    protocol_seed_changes bigint not null default 0,
    active_pool_seed_changes bigint not null default 0,
    asset_seed_changes bigint not null default 0,
    mev_candidate_changes bigint not null default 0,
    address_jobs_changed bigint not null default 0,
    last_error text,
    metadata jsonb not null default '{}'::jsonb,
    started_at timestamptz not null default now(),
    completed_at timestamptz,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    constraint seed_intelligence_runs_status check (
        status in ('running', 'completed', 'failed')
    )
);

create index if not exists idx_seed_intelligence_runs_worker
    on seed_intelligence_runs (worker_id, chain_id, started_at desc);
create index if not exists idx_seed_intelligence_runs_status
    on seed_intelligence_runs (status, started_at desc);

create or replace view effective_intelligence_seeds as
select
    seeds.chain_id,
    seeds.address,
    min(seeds.priority) as priority,
    max(seeds.confidence) as confidence,
    case
        when bool_or(seeds.status = 'active') then 'active'
        else 'candidate'
    end as status,
    array_agg(distinct seeds.seed_kind order by seeds.seed_kind) as seed_kinds,
    array_agg(distinct seeds.source_key order by seeds.source_key) as source_keys,
    (
        select array_agg(distinct role order by role)
        from intelligence_seed_addresses related
        cross join lateral unnest(related.roles) as role
        where related.chain_id = seeds.chain_id
          and related.address = seeds.address
          and related.status in ('active', 'candidate')
    ) as roles,
    min(seeds.first_seen_block) as first_seen_block,
    max(seeds.last_seen_block) as last_seen_block,
    md5(string_agg(
        seeds.seed_kind || ':' || seeds.source_key || ':' || seeds.evidence_digest,
        '|' order by seeds.seed_kind, seeds.source_key
    )) as seed_digest,
    min(seeds.first_seen_at) as first_seen_at,
    max(seeds.last_seen_at) as last_seen_at,
    max(seeds.updated_at) as updated_at
from intelligence_seed_addresses seeds
where seeds.status in ('active', 'candidate')
group by seeds.chain_id, seeds.address;

comment on table intelligence_seed_addresses is
    'Database-backed cold-start seeds. Candidate status never implies a verified MEV or real-world identity.';
comment on view effective_intelligence_seeds is
    'Materialized-at-read effective seed priority used by the address intelligence scheduler.';

