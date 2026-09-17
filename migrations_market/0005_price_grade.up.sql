-- 'reliable': quote asset is native/wrapped native (pool-snapshot priced) or a
-- registry-labelled stablecoin. 'derived': USD comes from banded swap medians.
alter table market.token_stats
    add column if not exists price_grade text not null default 'derived'
        check (price_grade in ('reliable', 'derived'));
