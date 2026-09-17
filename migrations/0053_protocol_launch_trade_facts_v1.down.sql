drop table if exists protocol_launch_trade_facts;
drop index if exists idx_protocol_token_launches_metadata_pending;
alter table protocol_token_launches drop constraint if exists protocol_token_launches_metadata_status;
alter table protocol_token_launches drop column if exists metadata_error;
alter table protocol_token_launches drop column if exists metadata_attempted_at;
alter table protocol_token_launches drop column if exists metadata_status;
