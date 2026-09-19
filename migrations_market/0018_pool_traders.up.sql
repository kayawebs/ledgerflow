-- Who actually made a pool trade.
--
-- Wallet profit and loss covered curve trades only, because a pool swap's
-- sender is the router contract that executed it. More than half the chain's
-- trading happens in pools, so the scoreboard was computed from a minority of
-- the market and systematically missed the largest wins, which happen after a
-- token graduates. The transaction sender is the account that acted; it is
-- resolved separately and kept beside the router address rather than
-- overwriting it.
alter table market.pool_swap_facts
    add column if not exists trader_address text;

create index if not exists idx_market_pool_swap_facts_trader
    on market.pool_swap_facts (chain_id, trader_address, occurred_at desc)
    where trader_address is not null;

-- The resolver walks swaps in block order from a cursor.
create index if not exists idx_market_pool_swap_facts_unresolved
    on market.pool_swap_facts (chain_id, block_number)
    where trader_address is null;
