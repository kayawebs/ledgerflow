-- The Base production data plane runs in the ledgerflow-base-pilot Compose
-- project. Its host ports are loopback-only, so sibling containers must use
-- the private shared Docker network rather than host.docker.internal.
update chain_registry
set api_base_url = 'http://ledgerflow-base-pilot-app-1:3000',
    indexer_api_base_url = 'http://ledgerflow-base-pilot-indexer-1:8090',
    config_version = greatest(config_version, 2),
    updated_at = now()
where chain_id = 8453;
