-- Quote-asset USD prices are stored as outlier-filtered hourly medians:
-- swap-derived observations are too noisy for minute buckets.
alter table if exists market.asset_usd_prices_1m rename to asset_usd_prices;
comment on table market.asset_usd_prices is
  'Hourly outlier-filtered median USD price per quote asset; bucket_start is the hour.';
