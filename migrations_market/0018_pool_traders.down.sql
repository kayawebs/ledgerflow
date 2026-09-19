drop index if exists market.idx_market_pool_swap_facts_unresolved;
drop index if exists market.idx_market_pool_swap_facts_trader;
alter table market.pool_swap_facts drop column if exists trader_address;
