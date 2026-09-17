update chain_registry
set api_base_url = 'http://host.docker.internal:3200',
    indexer_api_base_url = 'http://host.docker.internal:8191',
    config_version = 1,
    updated_at = now()
where chain_id = 8453
  and api_base_url = 'http://ledgerflow-base-pilot-app-1:3000'
  and indexer_api_base_url = 'http://ledgerflow-base-pilot-indexer-1:8090';
