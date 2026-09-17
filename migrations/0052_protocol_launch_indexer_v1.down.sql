drop table if exists protocol_launch_lifecycle_events;

update protocol_seed_addresses set seed_role = 'factory' where seed_role = 'launch_factory';
alter table protocol_seed_addresses drop constraint if exists protocol_seed_addresses_role;
alter table protocol_seed_addresses add constraint protocol_seed_addresses_role check (
    seed_role in (
        'factory', 'router', 'pool_manager', 'position_manager', 'locker',
        'quoter', 'quote_asset', 'fee_recipient', 'oracle', 'lending_pool'
    )
);

drop index if exists idx_protocol_token_launches_lifecycle;
alter table protocol_token_launches drop constraint if exists protocol_token_launches_graduation_tx;
alter table protocol_token_launches drop constraint if exists protocol_token_launches_curve;
alter table protocol_token_launches drop column if exists last_indexed_at;
alter table protocol_token_launches drop column if exists data_source;
alter table protocol_token_launches drop column if exists last_lifecycle_block;
alter table protocol_token_launches drop column if exists pool_id;
alter table protocol_token_launches drop column if exists position_id;
alter table protocol_token_launches drop column if exists graduated_at;
alter table protocol_token_launches drop column if exists graduation_block;
alter table protocol_token_launches drop column if exists graduation_tx_hash;
alter table protocol_token_launches drop column if exists graduation_threshold_raw;
alter table protocol_token_launches drop column if exists launch_config_id;
alter table protocol_token_launches drop column if exists lifecycle_phase;
alter table protocol_token_launches drop column if exists curve_address;
alter table protocol_token_launches drop column if exists protocol_version;
