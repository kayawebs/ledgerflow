-- Whether every token in the execution's scope has a price from pool state or
-- a stablecoin anchor. Cross-token results priced from swap medians stay
-- visible but never enter the ranked profit totals.
alter table market.arbitrage_estimates
    add column if not exists reliable_pricing boolean not null default false;

create index if not exists idx_market_arbitrage_estimates_reliable
    on market.arbitrage_estimates (chain_id, block_time desc)
    where status = 'estimated' and reliable_pricing;
