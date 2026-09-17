-- no-transaction

-- Market projectors advance by block number over the live trade-fact table.
-- Build concurrently so the protocol launch indexer keeps writing.
create index concurrently if not exists idx_protocol_launch_trade_facts_chain_block
    on public.protocol_launch_trade_facts (chain_id, block_number);
