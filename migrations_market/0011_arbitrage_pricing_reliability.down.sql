drop index if exists market.idx_market_arbitrage_estimates_reliable;
alter table market.arbitrage_estimates drop column if exists reliable_pricing;
