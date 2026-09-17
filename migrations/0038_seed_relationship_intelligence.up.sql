insert into intelligence_sources (
    source_key, display_name, source_kind, trust_score, owner, metadata
)
values (
    'seed_relationship_extractor',
    'Deterministic seed relationship extractor',
    'onchain',
    0.90,
    'LedgerFlow',
    '{"inputs":["arbitrage_candidates","arbitrage_candidate_pools"],"method_version":"seed_relationship_v1","semantic_boundary":"observed_relationship_only"}'::jsonb
)
on conflict (source_key) do update set
    display_name = excluded.display_name,
    source_kind = excluded.source_kind,
    trust_score = excluded.trust_score,
    owner = excluded.owner,
    status = 'active',
    metadata = intelligence_sources.metadata || excluded.metadata,
    updated_at = now();

create table if not exists intelligence_seed_relationships (
    relationship_id uuid primary key default gen_random_uuid(),
    chain_id bigint not null,
    seed_address text not null,
    related_address text not null,
    relationship_type text not null,
    source_key text not null references intelligence_sources(source_key),
    status text not null default 'observed',
    transaction_count bigint not null default 0,
    sample_transaction_hashes text[] not null default array[]::text[],
    first_seen_block bigint not null,
    last_seen_block bigint not null,
    confidence double precision not null,
    evidence jsonb not null default '{}'::jsonb,
    evidence_digest text not null,
    first_seen_at timestamptz not null default now(),
    last_seen_at timestamptz not null default now(),
    expires_at timestamptz,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    constraint intelligence_seed_relationships_unique unique (
        chain_id, seed_address, related_address, relationship_type, source_key
    ),
    constraint intelligence_seed_relationships_seed_address check (
        seed_address = lower(seed_address)
        and seed_address ~ '^0x[0-9a-f]{40}$'
    ),
    constraint intelligence_seed_relationships_related_address check (
        related_address = lower(related_address)
        and related_address ~ '^0x[0-9a-f]{40}$'
        and related_address <> seed_address
    ),
    constraint intelligence_seed_relationships_type check (
        relationship_type in (
            'top_level_call_target',
            'transaction_pool',
            'confirmed_profit_recipient'
        )
    ),
    constraint intelligence_seed_relationships_status check (
        status in ('observed', 'expired', 'retracted')
    ),
    constraint intelligence_seed_relationships_transaction_count check (
        transaction_count > 0
    ),
    constraint intelligence_seed_relationships_block_order check (
        first_seen_block <= last_seen_block
    ),
    constraint intelligence_seed_relationships_confidence check (
        confidence >= 0 and confidence <= 1
    )
);

create index if not exists idx_intelligence_seed_relationships_seed
    on intelligence_seed_relationships (
        chain_id, seed_address, status, transaction_count desc, last_seen_block desc
    );
create index if not exists idx_intelligence_seed_relationships_related
    on intelligence_seed_relationships (
        chain_id, related_address, status, last_seen_block desc
    );
create index if not exists idx_intelligence_seed_relationships_type
    on intelligence_seed_relationships (
        chain_id, relationship_type, status, last_seen_block desc
    );

alter table seed_intelligence_runs
    add column if not exists relationship_changes bigint not null default 0,
    add column if not exists relationship_address_jobs_changed bigint not null default 0;

comment on table intelligence_seed_relationships is
    'Deterministic, neutral seed relationships. Rows prove an observed transaction relationship, not ownership, actor identity, intent, or profit unless the relationship type explicitly requires confirmed settlement evidence.';
