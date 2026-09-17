-- Gross value sent out of the executing scope. A USD estimate is published
-- only when capital visibly leaves the scope and the net result is a plausible
-- margin on it; otherwise funding or proceeds moved through native ETH or
-- another address and the receipt cannot support a profit figure.
alter table market.arbitrage_estimate_deltas
    add column if not exists outflow_raw numeric not null default 0;
alter table market.arbitrage_estimates
    add column if not exists outflow_usd numeric;
