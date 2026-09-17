insert into intelligence_sources (
    source_key, display_name, source_kind, trust_score, owner, metadata
)
values (
    'asset_role_classifier',
    'Token metadata asset-role classifier',
    'model',
    0.95,
    'LedgerFlow',
    '{"method_version":"token_metadata_roles_v1","address_source":"database"}'::jsonb
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
values
    (
        'stablecoin',
        'Stable quote asset',
        'asset_role',
        'A database-resolved stable quote asset eligible for conservative USD anchoring.',
        '{"runtime_role":"stable_quote"}'::jsonb
    ),
    (
        'wrapped_native',
        'Wrapped native asset',
        'asset_role',
        'The database-resolved wrapped form of the chain native asset.',
        '{"runtime_role":"wrapped_native"}'::jsonb
    )
on conflict (label_key) do update set
    display_name = excluded.display_name,
    dimension = excluded.dimension,
    description = excluded.description,
    status = 'active',
    metadata = label_definitions.metadata || excluded.metadata,
    updated_at = now();

with official_tokens as (
    select distinct
        metadata.chain_id,
        lower(metadata.token_address) as address,
        metadata.symbol,
        metadata.name,
        metadata.decimals
    from erc20_token_metadata metadata
    where metadata.fetch_status = 'ok'
      and exists (
          select 1
          from address_labels labels
          where labels.chain_id = metadata.chain_id
            and lower(labels.address) = lower(metadata.token_address)
            and labels.label = 'canonical_token'
      )
), inferred_roles as (
    select *, 'stablecoin'::text as label_key
    from official_tokens
    where upper(coalesce(symbol, '')) in (
        'USDC', 'USDBC', 'USDT', 'DAI', 'USDS', 'PYUSD', 'FRAX',
        'LUSD', 'GHO', 'USD0', 'USDG', 'USDE'
    )
       or lower(coalesce(name, '')) like '%usd coin%'
       or lower(coalesce(name, '')) like '%stablecoin%'
    union all
    select *, 'wrapped_native'::text as label_key
    from official_tokens
    where upper(coalesce(symbol, '')) in ('WETH', 'WETH.E')
       or lower(coalesce(name, '')) in ('wrapped ether', 'wrapped eth')
)
insert into address_label_assertions (
    claim_fingerprint, chain_id, address, label_key, source_key, origin_kind,
    method_version, assertion_value, confidence, status, observed_at,
    evidence_summary, created_at, updated_at
)
select
    'asset-role-v1:' || roles.chain_id::text || ':' || roles.address || ':' || roles.label_key,
    roles.chain_id,
    roles.address,
    roles.label_key,
    'asset_role_classifier',
    'model',
    'token_metadata_roles_v1',
    true,
    0.95,
    'active',
    now(),
    jsonb_build_object(
        'symbol', roles.symbol,
        'name', roles.name,
        'decimals', roles.decimals,
        'address_source', 'erc20_token_metadata',
        'eligibility_source', 'canonical_token label'
    ),
    now(),
    now()
from inferred_roles roles
on conflict (claim_fingerprint) do update set
    assertion_value = true,
    confidence = excluded.confidence,
    status = 'active',
    observed_at = now(),
    evidence_summary = excluded.evidence_summary,
    updated_at = now();

insert into address_labels (
    chain_id, address, label, source, confidence, evidence_json, created_at, updated_at
)
select
    assertions.chain_id,
    assertions.address,
    assertions.label_key,
    assertions.source_key,
    assertions.confidence,
    assertions.evidence_summary,
    now(),
    now()
from address_label_assertions assertions
where assertions.source_key = 'asset_role_classifier'
  and assertions.method_version = 'token_metadata_roles_v1'
  and assertions.status = 'active'
  and assertions.assertion_value = true
on conflict (chain_id, address, label, source) do update set
    confidence = excluded.confidence,
    evidence_json = excluded.evidence_json,
    updated_at = now();
