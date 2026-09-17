delete from chain_registry
where (chain_id = 8453
       and api_base_url = 'http://host.docker.internal:3200'
       and indexer_api_base_url = 'http://host.docker.internal:8191')
   or (chain_id = 4663
       and api_base_url = 'http://app:3000'
       and indexer_api_base_url = 'http://indexer:8090');

