-- One-off repair: re-aggregate candles for the launches passed in :launches
-- using the current rules (dust trades no longer set prices).
-- Usage: psql -v chain=4663 -v from_block=0 -v to_block=<candle cursor> \
--   -v lookback=6000 -v price_age=24 -v method=market_curve_v1 \
--   -v native=0x0000000000000000000000000000000000000000 -v dust=1000 -v launches="{uuid,...}"

with target as (
  select unnest(:'launches'::uuid[]) as launch_id
), touched_facts as (
  select fact.launch_id, fact.occurred_at
  from target join public.protocol_launch_trade_facts fact using (launch_id)
  where fact.chain_id = :chain and fact.block_number between :from_block and :to_block and fact.occurred_at is not null
  union all
  select swap.launch_id, swap.occurred_at
  from target join market.pool_swap_facts swap using (launch_id)
  where swap.chain_id = :chain and swap.block_number between :from_block and :to_block and swap.occurred_at is not null
), touched as materialized (
  select launch_id,
         date_trunc('minute', min(occurred_at)) as first_bucket,
         date_trunc('minute', max(occurred_at)) as last_bucket
  from touched_facts
  group by launch_id
), launch_meta as materialized (
  select touched.launch_id, touched.first_bucket, touched.last_bucket,
         launch.domain_id, launch.quote_token_address,
         token_meta.decimals as token_decimals,
         coalesce(quote_meta.decimals,
           case when launch.quote_token_address = :'native' then 18 end) as quote_decimals,
         pool.token_is_currency0,
         case when pool.token_is_currency0 then pool.currency1 else pool.currency0 end
           as pool_quote_currency
  from touched
  join public.protocol_token_launches launch on launch.launch_id = touched.launch_id
  left join public.erc20_token_metadata token_meta
    on token_meta.chain_id = launch.chain_id and token_meta.token_address = launch.token_address
   and token_meta.fetch_status = 'ok'
  left join public.erc20_token_metadata quote_meta
    on quote_meta.chain_id = launch.chain_id
   and quote_meta.token_address = launch.quote_token_address
   and quote_meta.fetch_status = 'ok'
  left join market.token_pools pool
    on pool.launch_id = launch.launch_id and pool.status = 'mapped'
), facts as materialized (
  select fact.launch_id, meta.domain_id, meta.quote_token_address,
         date_trunc('minute', fact.occurred_at) as bucket_start,
         fact.trade_side, fact.trader_address, fact.block_number, fact.tx_index, fact.log_index,
         fact.quote_amount_raw::numeric * power(10::numeric, -(meta.quote_decimals)) as quote_amount,
         fact.token_amount_raw::numeric * power(10::numeric, -(meta.token_decimals)) as token_amount,
         fact.quote_amount_raw::numeric as quote_amount_raw,
         fact.token_amount_raw::numeric as token_amount_raw,
         fact.fee_amount_raw::numeric * power(10::numeric, -(meta.quote_decimals)) as fee_amount,
         fact.tax_amount_raw::numeric * power(10::numeric, -(meta.quote_decimals)) as tax_amount,
         true as is_curve
  from launch_meta meta
  join public.protocol_launch_trade_facts fact
    on fact.launch_id = meta.launch_id and fact.is_canonical
   and fact.block_number between :from_block - :lookback and :to_block
   and fact.occurred_at >= meta.first_bucket
   and fact.occurred_at < meta.last_bucket + interval '1 minute'
  where meta.token_decimals is not null and meta.quote_decimals is not null
  union all
  -- Post-graduation V4 swaps against the launch's own quote currency.
  select swap.launch_id, meta.domain_id, meta.quote_token_address,
         date_trunc('minute', swap.occurred_at),
         swap.trade_side, swap.sender_address, swap.block_number, 0, swap.log_index,
         swap.quote_amount_raw * power(10::numeric, -(meta.quote_decimals)),
         swap.token_amount_raw * power(10::numeric, -(meta.token_decimals)),
         swap.quote_amount_raw, swap.token_amount_raw,
         0, 0, false
  from launch_meta meta
  join market.pool_swap_facts swap
    on swap.launch_id = meta.launch_id
   and swap.block_number between :from_block - :lookback and :to_block
   and swap.occurred_at >= meta.first_bucket
   and swap.occurred_at < meta.last_bucket + interval '1 minute'
  where meta.token_decimals is not null and meta.quote_decimals is not null
    and meta.pool_quote_currency = meta.quote_token_address
), priced as (
  select facts.*,
         -- Dust trades (a few raw units on either leg) carry no price
         -- information: a 1-wei-in/1-wei-out swap implies a price of 1.
         case when token_amount > 0 and quote_amount_raw >= :dust and token_amount_raw >= :dust
           then quote_amount / token_amount end as price
  from facts
), aggregated as (
  select launch_id, bucket_start, domain_id, quote_token_address,
         (array_agg(price order by block_number, tx_index, log_index)
            filter (where price is not null))[1] as open_price,
         max(price) as high_price,
         min(price) as low_price,
         (array_agg(price order by block_number desc, tx_index desc, log_index desc)
            filter (where price is not null))[1] as close_price,
         coalesce(sum(quote_amount) filter (where trade_side = 'buy'), 0) as buy_quote,
         coalesce(sum(quote_amount) filter (where trade_side = 'sell'), 0) as sell_quote,
         coalesce(sum(token_amount) filter (where trade_side = 'buy'), 0) as buy_token,
         coalesce(sum(token_amount) filter (where trade_side = 'sell'), 0) as sell_token,
         coalesce(sum(fee_amount), 0) as fee_quote,
         coalesce(sum(case when not is_curve then 0
                    when trade_side = 'buy' then quote_amount - fee_amount - tax_amount
                    else -(quote_amount + fee_amount + tax_amount) end), 0) as reserve_delta,
         count(*)::int as trade_count,
         (count(*) filter (where trade_side = 'buy'))::int as buy_count,
         (count(*) filter (where trade_side = 'sell'))::int as sell_count,
         count(distinct trader_address)::int as unique_traders,
         min(block_number) as first_block,
         max(block_number) as last_block
  from priced
  group by launch_id, bucket_start, domain_id, quote_token_address
), upserted as (
  insert into market.token_candles_1m (
    launch_id, bucket_start, chain_id, domain_id,
    open_price_quote, high_price_quote, low_price_quote, close_price_quote, quote_usd_price,
    buy_quote, sell_quote, buy_token, sell_token, fee_quote, curve_reserve_delta_quote,
    trade_count, buy_count, sell_count, unique_traders, first_block, last_block,
    methodology_version, updated_at
  )
  select aggregated.launch_id, aggregated.bucket_start, :chain, aggregated.domain_id,
         open_price, high_price, low_price, close_price,
         (select price.price_usd from market.asset_usd_prices price
          where price.chain_id = :chain and price.asset_address = aggregated.quote_token_address
            and price.bucket_start <= aggregated.bucket_start
            and price.bucket_start > aggregated.bucket_start - make_interval(hours => :price_age::int)
          order by price.bucket_start desc limit 1),
         buy_quote, sell_quote, buy_token, sell_token, fee_quote, reserve_delta,
         trade_count, buy_count, sell_count, unique_traders, first_block, last_block,
         'market_curve_v1', clock_timestamp()
  from aggregated
  on conflict (launch_id, bucket_start) do update set
    open_price_quote = excluded.open_price_quote,
    high_price_quote = excluded.high_price_quote,
    low_price_quote = excluded.low_price_quote,
    close_price_quote = excluded.close_price_quote,
    quote_usd_price = coalesce(excluded.quote_usd_price, market.token_candles_1m.quote_usd_price),
    buy_quote = excluded.buy_quote,
    sell_quote = excluded.sell_quote,
    buy_token = excluded.buy_token,
    sell_token = excluded.sell_token,
    fee_quote = excluded.fee_quote,
    curve_reserve_delta_quote = excluded.curve_reserve_delta_quote,
    trade_count = excluded.trade_count,
    buy_count = excluded.buy_count,
    sell_count = excluded.sell_count,
    unique_traders = excluded.unique_traders,
    first_block = excluded.first_block,
    last_block = excluded.last_block,
    methodology_version = excluded.methodology_version,
    updated_at = excluded.updated_at
  where (market.token_candles_1m.trade_count, market.token_candles_1m.buy_quote,
         market.token_candles_1m.sell_quote, market.token_candles_1m.close_price_quote,
         market.token_candles_1m.quote_usd_price)
        is distinct from
        (excluded.trade_count, excluded.buy_quote, excluded.sell_quote,
         excluded.close_price_quote,
         coalesce(excluded.quote_usd_price, market.token_candles_1m.quote_usd_price))
  returning 1
), deleted as (
  delete from market.token_candles_1m candle
  using touched
  where candle.launch_id = touched.launch_id
    and candle.bucket_start between touched.first_bucket and touched.last_bucket
    and not exists (
      select 1 from aggregated
      where aggregated.launch_id = candle.launch_id
        and aggregated.bucket_start = candle.bucket_start
    )
  returning 1
)
select (select count(*) from upserted) as upserted,
       (select count(*) from deleted) as deleted,
       (select count(*) from touched) as touched

