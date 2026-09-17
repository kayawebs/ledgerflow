insert into chain_registry (
    chain_id,
    slug,
    display_name,
    native_symbol,
    explorer_base_url,
    api_base_url,
    indexer_api_base_url,
    enabled,
    capabilities,
    sort_order,
    config_version
)
values
    (
        8453,
        'base',
        'Base',
        'ETH',
        'https://basescan.org',
        'http://host.docker.internal:3200',
        'http://host.docker.internal:8191',
        true,
        '{"address_index":true,"cluster_analysis":true,"asset_changes":true,"arbitrage_monitor":true,"trace_analysis":true}'::jsonb,
        10,
        1
    ),
    (
        4663,
        'robinhood',
        'Robinhood Chain',
        'ETH',
        'https://robinhoodchain.blockscout.com',
        'http://app:3000',
        'http://indexer:8090',
        true,
        '{"address_index":true,"cluster_analysis":true,"asset_changes":true,"arbitrage_monitor":true,"trace_analysis":true}'::jsonb,
        20,
        1
    )
on conflict (chain_id) do update
set slug = excluded.slug,
    display_name = excluded.display_name,
    native_symbol = excluded.native_symbol,
    explorer_base_url = excluded.explorer_base_url,
    api_base_url = excluded.api_base_url,
    indexer_api_base_url = excluded.indexer_api_base_url,
    enabled = true,
    capabilities = excluded.capabilities,
    sort_order = excluded.sort_order,
    config_version = greatest(chain_registry.config_version, excluded.config_version),
    updated_at = now();

