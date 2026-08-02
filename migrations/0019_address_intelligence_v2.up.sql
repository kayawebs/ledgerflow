-- Address intelligence V2 separates chain identity, source provenance, raw
-- assertions, and effective labels. Existing V1 tables remain available while
-- services migrate, so this migration is additive and safe for rolling deploys.

create table if not exists chain_addresses (
    chain_id bigint not null,
    address text not null,
    address_kind text not null default 'unknown',
    first_seen_block bigint,
    last_seen_block bigint,
    first_seen_at timestamptz not null default now(),
    last_seen_at timestamptz not null default now(),
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (chain_id, address),
    constraint chain_addresses_normalized_address check (address = lower(address)),
    constraint chain_addresses_kind check (
        address_kind in ('unknown', 'eoa', 'contract', 'system', 'precompile')
    ),
    constraint chain_addresses_block_order check (
        first_seen_block is null or last_seen_block is null or first_seen_block <= last_seen_block
    )
);

create index if not exists idx_chain_addresses_kind
    on chain_addresses (chain_id, address_kind);
create index if not exists idx_chain_addresses_last_seen
    on chain_addresses (chain_id, last_seen_at desc);

create table if not exists address_contract_states (
    state_id uuid primary key default gen_random_uuid(),
    chain_id bigint not null,
    address text not null,
    code_hash text,
    bytecode_length bigint,
    observed_block bigint,
    valid_to_block bigint,
    observed_at timestamptz not null default now(),
    source_key text not null default 'address_worker',
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    foreign key (chain_id, address) references chain_addresses (chain_id, address),
    constraint address_contract_states_block_order check (
        observed_block is null or valid_to_block is null or observed_block <= valid_to_block
    )
);

create unique index if not exists idx_address_contract_states_current
    on address_contract_states (chain_id, address, coalesce(observed_block, -1), source_key);
create index if not exists idx_address_contract_states_lookup
    on address_contract_states (chain_id, address, observed_at desc);

create table if not exists intelligence_sources (
    source_key text primary key,
    display_name text not null,
    source_kind text not null,
    trust_score double precision not null default 0.5,
    owner text,
    homepage text,
    status text not null default 'active',
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    constraint intelligence_sources_kind check (
        source_kind in ('curated', 'official', 'manual', 'onchain', 'model', 'import', 'migration')
    ),
    constraint intelligence_sources_trust check (trust_score >= 0 and trust_score <= 1),
    constraint intelligence_sources_status check (status in ('active', 'disabled'))
);

create table if not exists label_definitions (
    label_key text primary key,
    display_name text not null,
    dimension text not null,
    exclusive_group text,
    description text,
    status text not null default 'active',
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    constraint label_definitions_status check (status in ('active', 'deprecated'))
);

create index if not exists idx_label_definitions_dimension
    on label_definitions (dimension, exclusive_group);

create table if not exists intelligence_datasets (
    dataset_key text not null,
    dataset_version text not null,
    source_key text not null references intelligence_sources (source_key),
    chain_id bigint,
    checksum_sha256 text not null,
    status text not null default 'active',
    manifest jsonb not null default '{}'::jsonb,
    imported_at timestamptz not null default now(),
    created_at timestamptz not null default now(),
    primary key (dataset_key, dataset_version),
    constraint intelligence_datasets_status check (status in ('active', 'superseded', 'retracted'))
);

create index if not exists idx_intelligence_datasets_chain
    on intelligence_datasets (chain_id, status, imported_at desc);

create table if not exists address_label_assertions (
    assertion_id uuid primary key default gen_random_uuid(),
    claim_fingerprint text not null unique,
    chain_id bigint not null,
    address text not null,
    label_key text not null references label_definitions (label_key),
    source_key text not null references intelligence_sources (source_key),
    origin_kind text not null,
    method_version text not null,
    dataset_key text,
    dataset_version text,
    assertion_value boolean not null default true,
    confidence double precision not null,
    status text not null default 'active',
    valid_from_block bigint,
    valid_to_block bigint,
    observed_at timestamptz not null default now(),
    expires_at timestamptz,
    supersedes_assertion_id uuid references address_label_assertions (assertion_id),
    evidence_summary jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    foreign key (chain_id, address) references chain_addresses (chain_id, address),
    foreign key (dataset_key, dataset_version)
        references intelligence_datasets (dataset_key, dataset_version),
    constraint address_label_assertions_origin check (
        origin_kind in ('curated_seed', 'official', 'manual', 'onchain', 'model', 'migration')
    ),
    constraint address_label_assertions_confidence check (confidence >= 0 and confidence <= 1),
    constraint address_label_assertions_status check (
        status in ('active', 'retracted', 'superseded')
    ),
    constraint address_label_assertions_block_order check (
        valid_from_block is null or valid_to_block is null or valid_from_block <= valid_to_block
    ),
    constraint address_label_assertions_dataset_pair check (
        (dataset_key is null and dataset_version is null)
        or (dataset_key is not null and dataset_version is not null)
    )
);

create index if not exists idx_address_label_assertions_lookup
    on address_label_assertions (chain_id, address, status, observed_at desc);
create index if not exists idx_address_label_assertions_label
    on address_label_assertions (chain_id, label_key, status, confidence desc);
create index if not exists idx_address_label_assertions_source
    on address_label_assertions (source_key, status, updated_at desc);
create index if not exists idx_address_label_assertions_dataset
    on address_label_assertions (dataset_key, dataset_version, status);

create table if not exists assertion_evidence (
    evidence_id uuid primary key default gen_random_uuid(),
    assertion_id uuid not null references address_label_assertions (assertion_id) on delete cascade,
    evidence_type text not null,
    evidence_ref text,
    evidence_hash text not null,
    payload jsonb not null default '{}'::jsonb,
    observed_at timestamptz not null default now(),
    created_at timestamptz not null default now(),
    unique (assertion_id, evidence_hash)
);

create index if not exists idx_assertion_evidence_assertion
    on assertion_evidence (assertion_id, observed_at desc);

create table if not exists protocols (
    protocol_key text primary key,
    display_name text not null,
    protocol_family text not null,
    website text,
    metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists protocol_deployments (
    deployment_id uuid primary key default gen_random_uuid(),
    chain_id bigint not null,
    address text not null,
    protocol_key text not null references protocols (protocol_key),
    contract_role text not null,
    source_key text not null references intelligence_sources (source_key),
    method_version text not null,
    dataset_key text,
    dataset_version text,
    confidence double precision not null,
    status text not null default 'active',
    valid_from_block bigint,
    valid_to_block bigint,
    metadata jsonb not null default '{}'::jsonb,
    first_seen_at timestamptz not null default now(),
    last_seen_at timestamptz not null default now(),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    foreign key (chain_id, address) references chain_addresses (chain_id, address),
    foreign key (dataset_key, dataset_version)
        references intelligence_datasets (dataset_key, dataset_version),
    constraint protocol_deployments_confidence check (confidence >= 0 and confidence <= 1),
    constraint protocol_deployments_status check (status in ('active', 'retracted', 'superseded')),
    constraint protocol_deployments_dataset_pair check (
        (dataset_key is null and dataset_version is null)
        or (dataset_key is not null and dataset_version is not null)
    ),
    constraint protocol_deployments_block_order check (
        valid_from_block is null or valid_to_block is null or valid_from_block <= valid_to_block
    ),
    unique (chain_id, address, protocol_key, contract_role, source_key, method_version)
);

create index if not exists idx_protocol_deployments_lookup
    on protocol_deployments (chain_id, address, status);
create index if not exists idx_protocol_deployments_protocol
    on protocol_deployments (chain_id, protocol_key, contract_role, status);

-- Register every V1 producer as an explicit source before backfilling claims.
insert into intelligence_sources (source_key, display_name, source_kind, trust_score, metadata)
values
    ('legacy_v1', 'LedgerFlow V1 migration', 'migration', 0.80, '{"migration":"0019"}'::jsonb),
    ('ledgerflow_seed', 'LedgerFlow curated seed registry', 'curated', 0.99, '{}'::jsonb),
    ('builtin', 'LedgerFlow legacy built-ins', 'curated', 0.95, '{"deprecated":true}'::jsonb),
    ('address_worker', 'Address bytecode classifier', 'onchain', 0.90, '{}'::jsonb),
    ('tx_analysis', 'Transaction role analysis', 'model', 0.78, '{}'::jsonb),
    ('protocol_probe', 'Protocol interface probe', 'onchain', 0.92, '{}'::jsonb),
    ('balance_classifier', 'Portfolio balance classifier', 'model', 0.82, '{}'::jsonb)
on conflict (source_key) do nothing;

insert into intelligence_sources (source_key, display_name, source_kind, trust_score, metadata)
select distinct
    labels.source,
    labels.source,
    'migration',
    0.70,
    jsonb_build_object('discovered_from', 'address_labels_v1')
from address_labels labels
on conflict (source_key) do nothing;

-- The identity registry is intentionally populated only by addresses that have
-- already entered the intelligence pipeline, not every transaction participant.
insert into chain_addresses (
    chain_id, address, address_kind, first_seen_at, last_seen_at, metadata, created_at, updated_at
)
select
    profiles.chain_id,
    lower(profiles.address),
    case
        when profiles.address_type = 'eoa' then 'eoa'
        when coalesce(profiles.bytecode_length, 0) > 0 then 'contract'
        else 'unknown'
    end,
    profiles.first_seen_at,
    profiles.last_seen_at,
    profiles.metadata,
    profiles.created_at,
    profiles.updated_at
from address_profiles profiles
on conflict (chain_id, address) do update set
    address_kind = excluded.address_kind,
    first_seen_at = least(chain_addresses.first_seen_at, excluded.first_seen_at),
    last_seen_at = greatest(chain_addresses.last_seen_at, excluded.last_seen_at),
    metadata = chain_addresses.metadata || excluded.metadata,
    updated_at = greatest(chain_addresses.updated_at, excluded.updated_at);

insert into chain_addresses (chain_id, address, metadata)
select distinct labels.chain_id, lower(labels.address), '{"backfilled_from":"address_labels"}'::jsonb
from address_labels labels
on conflict (chain_id, address) do nothing;

insert into chain_addresses (chain_id, address, address_kind, metadata)
select distinct metadata.chain_id, lower(metadata.address), 'contract',
    '{"backfilled_from":"address_protocol_metadata"}'::jsonb
from address_protocol_metadata metadata
on conflict (chain_id, address) do nothing;

insert into chain_addresses (chain_id, address, address_kind, metadata)
select distinct tokens.chain_id, lower(tokens.token_address), 'contract',
    '{"backfilled_from":"erc20_token_metadata"}'::jsonb
from erc20_token_metadata tokens
on conflict (chain_id, address) do nothing;

insert into address_contract_states (
    chain_id, address, code_hash, bytecode_length, observed_at, source_key, metadata, created_at
)
select
    profiles.chain_id,
    lower(profiles.address),
    profiles.code_hash,
    profiles.bytecode_length,
    coalesce(profiles.last_analyzed_at, profiles.updated_at),
    'address_worker',
    jsonb_build_object('backfilled_from', 'address_profiles'),
    profiles.created_at
from address_profiles profiles
where profiles.code_hash is not null or profiles.bytecode_length is not null
on conflict (chain_id, address, (coalesce(observed_block, -1)), source_key) do update set
    code_hash = excluded.code_hash,
    bytecode_length = excluded.bytecode_length,
    observed_at = greatest(address_contract_states.observed_at, excluded.observed_at);

insert into label_definitions (
    label_key, display_name, dimension, exclusive_group, metadata
)
select distinct
    labels.label,
    labels.label,
    case
        when labels.label like 'actor:%' then 'actor_type'
        when labels.label like 'token:%' or labels.label like 'canonical:%'
            or labels.label = 'stablecoin' then 'asset_identity'
        when labels.label like 'protocol:%' then 'protocol'
        when labels.label in ('eoa', 'contract')
            then 'address_kind'
        when labels.label like '%router%' or labels.label like '%pool%'
            or labels.label in ('token', 'protocol', 'router_candidate', 'protocol_candidate',
                                'approval_spender', 'touched_contract') then 'contract_role'
        else 'behavior_or_role'
    end,
    case
        when labels.label in ('eoa', 'contract')
            then 'address_kind'
        else null
    end,
    jsonb_build_object('backfilled_from', 'address_labels_v1')
from address_labels labels
on conflict (label_key) do nothing;

insert into address_label_assertions (
    claim_fingerprint, chain_id, address, label_key, source_key, origin_kind,
    method_version, assertion_value, confidence, status, observed_at,
    evidence_summary, created_at, updated_at
)
select
    encode(digest(concat_ws('|', labels.chain_id::text, lower(labels.address), labels.label,
                            labels.source, 'legacy_v1'), 'sha256'), 'hex'),
    labels.chain_id,
    lower(labels.address),
    labels.label,
    labels.source,
    case
        when labels.source in ('ledgerflow_seed', 'builtin') then 'curated_seed'
        when labels.source in ('address_worker', 'protocol_probe') then 'onchain'
        when labels.source in ('tx_analysis', 'balance_classifier') then 'model'
        else 'migration'
    end,
    'legacy_v1',
    true,
    labels.confidence,
    'active',
    labels.updated_at,
    labels.evidence_json || jsonb_build_object('migrated_from', 'address_labels'),
    labels.created_at,
    labels.updated_at
from address_labels labels
on conflict (claim_fingerprint) do nothing;

insert into protocols (protocol_key, display_name, protocol_family, metadata)
select distinct
    metadata.protocol_name,
    metadata.protocol_name,
    metadata.protocol_family,
    jsonb_build_object('backfilled_from', 'address_protocol_metadata')
from address_protocol_metadata metadata
on conflict (protocol_key) do update set
    protocol_family = excluded.protocol_family,
    updated_at = now();

insert into protocol_deployments (
    chain_id, address, protocol_key, contract_role, source_key, method_version,
    confidence, metadata, first_seen_at, last_seen_at, created_at, updated_at
)
select
    metadata.chain_id,
    lower(metadata.address),
    metadata.protocol_name,
    metadata.contract_role,
    metadata.source,
    metadata.adapter_version,
    metadata.confidence,
    metadata.metadata || jsonb_build_object('protocol_family', metadata.protocol_family),
    metadata.first_seen_at,
    metadata.last_seen_at,
    metadata.first_seen_at,
    metadata.updated_at
from address_protocol_metadata metadata
on conflict (chain_id, address, protocol_key, contract_role, source_key, method_version)
do update set
    confidence = greatest(protocol_deployments.confidence, excluded.confidence),
    metadata = protocol_deployments.metadata || excluded.metadata,
    last_seen_at = greatest(protocol_deployments.last_seen_at, excluded.last_seen_at),
    updated_at = greatest(protocol_deployments.updated_at, excluded.updated_at);

-- Consumers read this projection. Raw assertions remain available for audit,
-- retraction, source comparison, and future rescoring.
create or replace view effective_address_labels as
with ranked as (
    select
        assertions.chain_id,
        assertions.address,
        assertions.label_key as label,
        assertions.source_key as source,
        least(1.0, assertions.confidence * sources.trust_score) as confidence,
        assertions.evidence_summary || jsonb_build_object(
            'assertion_id', assertions.assertion_id,
            'origin_kind', assertions.origin_kind,
            'method_version', assertions.method_version,
            'raw_confidence', assertions.confidence,
            'source_trust', sources.trust_score
        ) as evidence_json,
        assertions.created_at,
        assertions.updated_at,
        row_number() over (
            partition by assertions.chain_id, assertions.address,
                case
                    when definitions.exclusive_group is not null
                        then 'exclusive:' || definitions.exclusive_group
                    else 'label:' || assertions.label_key
                end
            order by assertions.confidence * sources.trust_score desc,
                     assertions.observed_at desc,
                     assertions.assertion_id desc
        ) as precedence_rank
    from address_label_assertions assertions
    join intelligence_sources sources on sources.source_key = assertions.source_key
    join label_definitions definitions on definitions.label_key = assertions.label_key
    where assertions.status = 'active'
      and assertions.assertion_value = true
      and sources.status = 'active'
      and definitions.status = 'active'
      and (assertions.expires_at is null or assertions.expires_at > now())
)
select chain_id, address, label, source, confidence, evidence_json, created_at, updated_at
from ranked
where precedence_rank = 1;
