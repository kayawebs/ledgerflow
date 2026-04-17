drop table if exists cluster_spend_summaries;
drop index if exists idx_cluster_addresses_address;
drop table if exists cluster_addresses;
drop table if exists address_clusters;
drop index if exists idx_transaction_analysis_block_timestamp;
drop index if exists idx_transaction_analysis_to_address;
drop index if exists idx_transaction_analysis_from_address;
alter table transaction_analysis
  drop column if exists tx_status,
  drop column if exists to_address,
  drop column if exists from_address,
  drop column if exists block_timestamp;
