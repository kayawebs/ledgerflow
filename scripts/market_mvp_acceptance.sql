-- Market MVP acceptance checks. Run with:
--   sudo docker exec -i ledgerflow-chainactors-v2-db-1 psql -U postgres -d ledgerflow \
--     -v ON_ERROR_STOP=1 < scripts/market_mvp_acceptance.sql
\pset pager off
\timing off

\echo '== 1.4 candle reconciliation: 20 random launches, completed window now-6h .. now-30m =='
with sample as (
  select launch_id from market.token_stats
  where chain_id = 4663 and trades_24h > 0
  order by md5(launch_id::text) limit 20
), candle_totals as (
  select c.launch_id, sum(c.trade_count) as trades, sum(c.buy_quote) as buy_quote,
         sum(c.sell_quote) as sell_quote
  from market.token_candles_1m c
  join sample using (launch_id)
  where c.bucket_start >= date_trunc('minute', now() - interval '6 hours')
    and c.bucket_start < date_trunc('minute', now() - interval '30 minutes')
  group by c.launch_id
), fact_totals as (
  select f.launch_id, count(*) as trades,
         coalesce(sum(f.quote_amount_raw::numeric) filter (where f.trade_side = 'buy'), 0)
           / power(10::numeric, max(coalesce(q.decimals, 18))) as buy_quote,
         coalesce(sum(f.quote_amount_raw::numeric) filter (where f.trade_side = 'sell'), 0)
           / power(10::numeric, max(coalesce(q.decimals, 18))) as sell_quote
  from public.protocol_launch_trade_facts f
  join sample using (launch_id)
  join public.protocol_token_launches l on l.launch_id = f.launch_id
  left join public.erc20_token_metadata q
    on q.chain_id = l.chain_id and q.token_address = l.quote_token_address
  where f.is_canonical
    and f.occurred_at >= date_trunc('minute', now() - interval '6 hours')
    and f.occurred_at < date_trunc('minute', now() - interval '30 minutes')
  group by f.launch_id
)
select count(*) as launches_with_trades_in_window,
       count(*) filter (where c.trades = f.trades) as trade_count_match,
       count(*) filter (where c.buy_quote = f.buy_quote) as buy_quote_match,
       count(*) filter (where c.sell_quote = f.sell_quote) as sell_quote_match
from fact_totals f left join candle_totals c using (launch_id);

\echo '== 1.5 freshness: candle lag behind newest trade fact =='
select (select max(block_number) from public.protocol_launch_trade_facts where chain_id = 4663)
     - (select last_position from market.projector_checkpoints where checkpoint_key = 'candles:4663')
       as candle_lag_blocks,
       extract(epoch from now() - (select max(bucket_start) from market.token_candles_1m
                                   where chain_id = 4663))::int as newest_candle_age_seconds;

\echo '== 1.6 coverage: 24h active tokens with priced stats =='
select count(*) as active_24h,
       count(*) filter (where price_usd is not null) as with_price,
       count(*) filter (where volume_24h_usd > 0) as with_volume,
       count(*) filter (where market_cap_usd is not null) as with_market_cap,
       count(*) filter (where change_24h_pct is not null) as with_change,
       count(*) filter (where bonding_progress_pct is not null) as with_progress,
       count(*) filter (where price_grade = 'reliable') as reliable_price
from market.token_stats where chain_id = 4663 and trades_24h > 0;

\echo '== 1.7 decay: tokens whose last trade is older than 24h but still report 24h volume =='
select count(*) as stale_windows
from market.token_stats
where chain_id = 4663 and last_trade_at < now() - interval '25 hours' and volume_24h_usd > 0;

\echo '== 3.1 position reconciliation: 20 random positions below the position cursor =='
with cursor_block as (
  select last_position as block from market.projector_checkpoints
  where checkpoint_key = 'positions:4663'
), sample as (
  select p.launch_id, p.address, p.bought_token, p.sold_token, p.buy_count, p.sell_count
  from market.wallet_positions p, cursor_block
  where p.chain_id = 4663 and p.last_block <= cursor_block.block
    and p.last_trade_at > now() - interval '3 days'
  order by md5(p.launch_id::text || p.address) limit 20
), recomputed as (
  select s.launch_id, s.address,
         coalesce(sum(f.token_amount_raw::numeric) filter (where f.trade_side = 'buy'), 0)
           / 1e18 as bought_token,
         coalesce(sum(f.token_amount_raw::numeric) filter (where f.trade_side = 'sell'), 0)
           / 1e18 as sold_token,
         count(*) filter (where f.trade_side = 'buy') as buys,
         count(*) filter (where f.trade_side = 'sell') as sells
  from sample s, cursor_block
  join public.protocol_launch_trade_facts f on true
  where f.launch_id = s.launch_id and f.is_canonical and f.block_number <= cursor_block.block
    and case when f.trade_side = 'buy' then f.recipient_address else f.trader_address end = s.address
  group by s.launch_id, s.address
)
select count(*) as positions,
       count(*) filter (where s.buy_count = r.buys and s.sell_count = r.sells) as count_match,
       count(*) filter (where s.bought_token = r.bought_token and s.sold_token = r.sold_token)
         as amount_match
from sample s join recomputed r using (launch_id, address);

\echo '== 3.3 leaderboards =='
select window_key, count(*) as wallets, max(pnl_usd)::numeric(18, 2) as top_pnl,
       max(computed_at) as computed_at
from market.wallet_leaderboard where chain_id = 4663 group by 1 order by 1;
select window_key, count(*) as clusters, count(*) filter (where pnl_usd > 0) as profitable,
       max(computed_at) as computed_at
from market.cluster_leaderboard where chain_id = 4663 group by 1 order by 1;

\echo '== 5.1 arbitrage estimate vs deterministic verification (same profit token) =='
select count(*) as overlapping_verified,
       count(*) filter (where d.amount_raw = v.profit_amount_raw::numeric) as exact_match,
       count(*) filter (where d.amount_raw is null) as token_not_in_scope,
       count(*) filter (where d.amount_raw is not null and d.amount_raw <> v.profit_amount_raw::numeric)
         as differing
from public.economic_verification_results v
join market.arbitrage_estimates e on e.chain_id = v.chain_id and e.tx_hash = v.tx_hash
left join market.arbitrage_estimate_deltas d
  on d.chain_id = v.chain_id and d.tx_hash = v.tx_hash and d.token_address = v.profit_token_address
where v.chain_id = 4663 and v.result_status = 'verified';

\echo '== 5.2 arbitrage coverage in the last 24h =='
select (select count(*) from public.arbitrage_candidates
        where chain_id = 4663 and block_timestamp > now() - interval '24 hours') as candidates_24h,
       count(*) as estimated_24h,
       count(*) filter (where status = 'estimated') as usd_priced,
       extract(epoch from now() - max(block_time))::int as newest_estimate_age_seconds
from market.arbitrage_estimates
where chain_id = 4663 and block_time > now() - interval '24 hours';

\echo '== 6.x liquidations =='
select (select last_position from market.projector_checkpoints
        where checkpoint_key = 'liquidations:4663') as scanned_to_block,
       count(*) as liquidations, count(distinct venue_address) as venues,
       count(*) filter (where status = 'estimated') as usd_priced,
       min(block_time), max(block_time)
from market.lending_liquidations where chain_id = 4663;
