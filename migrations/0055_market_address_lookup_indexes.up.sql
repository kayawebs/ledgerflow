create index if not exists idx_protocol_launch_trade_facts_trader_lookup
    on protocol_launch_trade_facts (chain_id, trader_address, block_number desc)
    where is_canonical;

create index if not exists idx_protocol_launch_trade_facts_recipient_lookup
    on protocol_launch_trade_facts (chain_id, recipient_address, block_number desc)
    where is_canonical;

create index if not exists idx_economic_verification_results_recipient
    on economic_verification_results (
        chain_id, profit_recipient, result_status, updated_at desc
    )
    where profit_recipient is not null;
