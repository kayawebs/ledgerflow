//! Wallet positions and earnings leaderboards for the market web.
//!
//! Positions are exact aggregates of Pons curve trade facts per (launch,
//! owner). The owner of a buy is its token recipient and the owner of a sell is
//! its trader, so inventory always follows the tokens. Earnings use
//! average-cost accounting against the latest curve price and are published
//! as estimates: gas, transfers and off-curve venues are out of scope, and
//! positions that sold more than they bought on the curve are excluded.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use serde::Serialize;
use sqlx::{PgPool, Row};
use tokio::sync::Mutex;

use crate::{
    core::errors::{LedgerError, LedgerResult},
    services::market_data::NATIVE_ASSET_ADDRESS,
};

pub const PNL_METHODOLOGY_VERSION: &str = "market_wallet_pnl_v1";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum EarningsWindow {
    Day,
    Week,
    Month,
}

impl EarningsWindow {
    pub const ALL: [EarningsWindow; 3] = [Self::Day, Self::Week, Self::Month];

    pub fn key(self) -> &'static str {
        match self {
            Self::Day => "24h",
            Self::Week => "7d",
            Self::Month => "30d",
        }
    }

    pub fn parse(value: Option<&str>) -> Self {
        match value {
            Some("7d") => Self::Week,
            Some("30d") => Self::Month,
            _ => Self::Day,
        }
    }

    fn hours(self) -> i32 {
        match self {
            Self::Day => 24,
            Self::Week => 24 * 7,
            Self::Month => 24 * 30,
        }
    }

    /// How stale a leaderboard may become before it is rebuilt.
    fn refresh_every(self) -> Duration {
        match self {
            Self::Day => Duration::from_secs(60),
            Self::Week => Duration::from_secs(300),
            Self::Month => Duration::from_secs(900),
        }
    }
}

#[derive(Clone, Debug)]
pub struct MarketPnlConfig {
    pub chain_id: u64,
    pub position_block_batch: i64,
    pub position_reprocess_blocks: i64,
    pub leaderboard_size: i64,
    pub new_cluster_rows: i64,
    pub min_wallet_cost_usd: f64,
}

impl Default for MarketPnlConfig {
    fn default() -> Self {
        Self {
            chain_id: 4663,
            position_block_batch: 100_000,
            position_reprocess_blocks: 1_500,
            leaderboard_size: 500,
            new_cluster_rows: 100,
            min_wallet_cost_usd: 50.0,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct MarketPnlSummary {
    pub position_from_block: i64,
    pub position_to_block: i64,
    pub position_limit_block: i64,
    pub positions_upserted: i64,
    pub positions_deleted: i64,
    pub wallet_rows: u64,
    pub cluster_rows: u64,
    pub position_ms: u64,
    pub leaderboard_ms: u64,
}

impl MarketPnlSummary {
    pub fn has_backlog(&self) -> bool {
        self.position_to_block < self.position_limit_block
    }
}

#[derive(Clone)]
pub struct MarketPnlProjector {
    pool: PgPool,
    config: MarketPnlConfig,
    last_rebuilt: Arc<Mutex<[Option<Instant>; 3]>>,
}

impl MarketPnlProjector {
    pub fn new(pool: PgPool, config: MarketPnlConfig) -> Self {
        Self {
            pool,
            config,
            last_rebuilt: Arc::new(Mutex::new([None; 3])),
        }
    }

    pub async fn project_once(&self) -> LedgerResult<MarketPnlSummary> {
        let mut summary = MarketPnlSummary::default();
        let started = Instant::now();
        self.project_positions(&mut summary).await?;
        summary.position_ms = started.elapsed().as_millis() as u64;
        if summary.has_backlog() {
            return Ok(summary);
        }
        let started = Instant::now();
        let mut last_rebuilt = self.last_rebuilt.lock().await;
        for (index, window) in EarningsWindow::ALL.into_iter().enumerate() {
            if last_rebuilt[index].is_some_and(|at| at.elapsed() < window.refresh_every()) {
                continue;
            }
            summary.wallet_rows += self.rebuild_wallet_leaderboard(window).await?;
            summary.cluster_rows += self.rebuild_cluster_leaderboard(window).await?;
            last_rebuilt[index] = Some(Instant::now());
        }
        summary.leaderboard_ms = started.elapsed().as_millis() as u64;
        Ok(summary)
    }

    async fn project_positions(&self, summary: &mut MarketPnlSummary) -> LedgerResult<()> {
        let chain_id = self.config.chain_id as i64;
        let key = format!("positions:{}", self.config.chain_id);
        // Positions price trades from candles, so they never pass the candle cursor.
        let limit: Option<i64> = sqlx::query_scalar(
            "select last_position from market.projector_checkpoints where checkpoint_key = $1",
        )
        .bind(format!("candles:{}", self.config.chain_id))
        .fetch_optional(&self.pool)
        .await
        .map_err(db_error)?;
        let Some(limit) = limit else {
            return Ok(());
        };
        summary.position_limit_block = limit;
        let cursor: i64 = match sqlx::query_scalar::<_, i64>(
            "select last_position from market.projector_checkpoints where checkpoint_key = $1",
        )
        .bind(&key)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_error)?
        {
            Some(position) => position,
            None => {
                let first: Option<i64> = sqlx::query_scalar(
                    "select min(block_number) from public.protocol_launch_trade_facts where chain_id = $1",
                )
                .bind(chain_id)
                .fetch_one(&self.pool)
                .await
                .map_err(db_error)?;
                first.unwrap_or(limit) - 1
            }
        };
        let end = (cursor + self.config.position_block_batch).min(limit);
        let start = if end >= limit {
            (cursor + 1 - self.config.position_reprocess_blocks).max(0)
        } else {
            cursor + 1
        };
        summary.position_from_block = start;
        summary.position_to_block = end;
        if end < start {
            return Ok(());
        }
        let row = sqlx::query(POSITION_PROJECTION_SQL)
            .bind(chain_id)
            .bind(start)
            .bind(end)
            .bind(NATIVE_ASSET_ADDRESS)
            .fetch_one(&self.pool)
            .await
            .map_err(db_error)?;
        summary.positions_upserted = row.get("upserted");
        summary.positions_deleted = row.get("deleted");
        sqlx::query(
            r#"insert into market.projector_checkpoints (
                 checkpoint_key, last_position, last_run_at, last_success_at, updated_at
               ) values ($1, $2, now(), now(), now())
               on conflict (checkpoint_key) do update set
                 last_position = excluded.last_position, last_run_at = now(),
                 last_success_at = now(), last_error = null, updated_at = now()"#,
        )
        .bind(&key)
        .bind(end.max(cursor))
        .execute(&self.pool)
        .await
        .map_err(db_error)?;
        Ok(())
    }

    async fn rebuild_wallet_leaderboard(&self, window: EarningsWindow) -> LedgerResult<u64> {
        let mut tx = self.pool.begin().await.map_err(db_error)?;
        sqlx::query(
            "delete from market.wallet_leaderboard where chain_id = $1 and window_key = $2",
        )
        .bind(self.config.chain_id as i64)
        .bind(window.key())
        .execute(&mut *tx)
        .await
        .map_err(db_error)?;
        let result = sqlx::query(WALLET_LEADERBOARD_SQL.as_str())
            .bind(self.config.chain_id as i64)
            .bind(window.key())
            .bind(window.hours())
            .bind(self.config.min_wallet_cost_usd)
            .bind(self.config.leaderboard_size)
            .bind(PNL_METHODOLOGY_VERSION)
            .execute(&mut *tx)
            .await
            .map_err(db_error)?;
        tx.commit().await.map_err(db_error)?;
        Ok(result.rows_affected())
    }

    async fn rebuild_cluster_leaderboard(&self, window: EarningsWindow) -> LedgerResult<u64> {
        let mut tx = self.pool.begin().await.map_err(db_error)?;
        sqlx::query(
            "delete from market.cluster_leaderboard where chain_id = $1 and window_key = $2",
        )
        .bind(self.config.chain_id as i64)
        .bind(window.key())
        .execute(&mut *tx)
        .await
        .map_err(db_error)?;
        let result = sqlx::query(CLUSTER_LEADERBOARD_SQL.as_str())
            .bind(self.config.chain_id as i64)
            .bind(window.key())
            .bind(window.hours())
            .bind(self.config.leaderboard_size)
            .bind(self.config.new_cluster_rows)
            .bind(PNL_METHODOLOGY_VERSION)
            .execute(&mut *tx)
            .await
            .map_err(db_error)?;
        tx.commit().await.map_err(db_error)?;
        Ok(result.rows_affected())
    }
}

/// Rebuilds positions for every (launch, owner) pair with a trade in blocks
/// `$2..=$3`, using each pair's complete canonical history up to `$3`.
const POSITION_PROJECTION_SQL: &str = r#"
with touched as materialized (
  select distinct launch_id,
         case when trade_side = 'buy' then recipient_address else trader_address end as owner
  from public.protocol_launch_trade_facts
  where chain_id = $1 and block_number between $2 and $3
), touched_launches as materialized (
  select distinct touched.launch_id, launch.domain_id, launch.creator_address,
         token_meta.decimals as token_decimals,
         coalesce(quote_meta.decimals,
           case when launch.quote_token_address = $4 then 18 end) as quote_decimals
  from touched
  join public.protocol_token_launches launch on launch.launch_id = touched.launch_id
  left join public.erc20_token_metadata token_meta
    on token_meta.chain_id = launch.chain_id and token_meta.token_address = launch.token_address
   and token_meta.fetch_status = 'ok'
  left join public.erc20_token_metadata quote_meta
    on quote_meta.chain_id = launch.chain_id
   and quote_meta.token_address = launch.quote_token_address
   and quote_meta.fetch_status = 'ok'
), facts as materialized (
  select fact.launch_id, launch.domain_id, launch.creator_address,
         case when fact.trade_side = 'buy' then fact.recipient_address else fact.trader_address end
           as owner,
         fact.trade_side, fact.block_number, fact.occurred_at,
         fact.quote_amount_raw::numeric / power(10::numeric, launch.quote_decimals) as quote_amount,
         fact.token_amount_raw::numeric / power(10::numeric, launch.token_decimals) as token_amount,
         candle.quote_usd_price
  from touched_launches launch
  join public.protocol_launch_trade_facts fact
    on fact.launch_id = launch.launch_id and fact.is_canonical and fact.block_number <= $3
  left join market.token_candles_1m candle
    on candle.launch_id = fact.launch_id
   and candle.bucket_start = date_trunc('minute', fact.occurred_at)
  where launch.token_decimals is not null and launch.quote_decimals is not null
), scoped as materialized (
  select facts.* from facts
  join touched on touched.launch_id = facts.launch_id and touched.owner = facts.owner
), aggregated as (
  select launch_id, owner, domain_id, creator_address,
         coalesce(sum(token_amount) filter (where trade_side = 'buy'), 0) as bought_token,
         coalesce(sum(token_amount) filter (where trade_side = 'sell'), 0) as sold_token,
         coalesce(sum(quote_amount) filter (where trade_side = 'buy'), 0) as cost_quote,
         coalesce(sum(quote_amount) filter (where trade_side = 'sell'), 0) as proceeds_quote,
         case when bool_and(quote_usd_price is not null) filter (where trade_side = 'buy')
                   is not false
           then coalesce(sum(quote_amount * quote_usd_price) filter (where trade_side = 'buy'), 0)
         end as cost_usd,
         case when bool_and(quote_usd_price is not null) filter (where trade_side = 'sell')
                   is not false
           then coalesce(sum(quote_amount * quote_usd_price) filter (where trade_side = 'sell'), 0)
         end as proceeds_usd,
         (count(*) filter (where trade_side = 'buy'))::int as buy_count,
         (count(*) filter (where trade_side = 'sell'))::int as sell_count,
         min(occurred_at) as first_trade_at,
         max(occurred_at) as last_trade_at,
         max(block_number) as last_block
  from scoped
  group by launch_id, owner, domain_id, creator_address
), upserted as (
  insert into market.wallet_positions (
    launch_id, address, chain_id, domain_id, creator_address, bought_token, sold_token,
    cost_quote, proceeds_quote, cost_usd, proceeds_usd, buy_count, sell_count,
    first_trade_at, last_trade_at, last_block, updated_at
  )
  select launch_id, owner, $1, domain_id, creator_address, bought_token, sold_token,
         cost_quote, proceeds_quote, cost_usd, proceeds_usd, buy_count, sell_count,
         first_trade_at, last_trade_at, last_block, now()
  from aggregated
  on conflict (launch_id, address) do update set
    bought_token = excluded.bought_token, sold_token = excluded.sold_token,
    cost_quote = excluded.cost_quote, proceeds_quote = excluded.proceeds_quote,
    cost_usd = excluded.cost_usd, proceeds_usd = excluded.proceeds_usd,
    buy_count = excluded.buy_count, sell_count = excluded.sell_count,
    first_trade_at = excluded.first_trade_at, last_trade_at = excluded.last_trade_at,
    last_block = excluded.last_block, updated_at = excluded.updated_at
  where (market.wallet_positions.buy_count, market.wallet_positions.sell_count,
         market.wallet_positions.last_block, market.wallet_positions.cost_usd,
         market.wallet_positions.proceeds_usd)
        is distinct from
        (excluded.buy_count, excluded.sell_count, excluded.last_block, excluded.cost_usd,
         excluded.proceeds_usd)
  returning 1
), deleted as (
  delete from market.wallet_positions position
  using touched
  where position.launch_id = touched.launch_id and position.address = touched.owner
    and not exists (
      select 1 from aggregated
      where aggregated.launch_id = touched.launch_id and aggregated.owner = touched.owner
    )
  returning 1
)
select (select count(*) from upserted) as upserted, (select count(*) from deleted) as deleted
"#;

/// Average-cost PnL for positions traded inside the window, valued at the
/// latest curve price. Shared by the wallet and cluster rankings.
const POSITION_PNL_CTE: &str = r#"
positions as materialized (
  select position.launch_id, position.address, position.creator_address,
         position.cost_usd, position.proceeds_usd,
         position.buy_count + position.sell_count as trades,
         position.last_trade_at,
         stats.symbol, stats.price_usd,
         least(position.sold_token / position.bought_token, 1) as sold_ratio,
         greatest(position.bought_token - position.sold_token, 0) as remaining
  from market.wallet_positions position
  join market.token_stats stats on stats.launch_id = position.launch_id
  where position.chain_id = $1
    and position.last_trade_at >= now() - make_interval(hours => $3::int)
    and position.bought_token > 0
    and position.sold_token <= position.bought_token * 1.000001
    and position.cost_usd is not null and position.proceeds_usd is not null
    and stats.price_usd is not null
    and stats.price_grade = 'reliable'
), valued as materialized (
  select positions.*,
         proceeds_usd - cost_usd * sold_ratio as realized,
         remaining * price_usd - cost_usd * (1 - sold_ratio) as unrealized,
         proceeds_usd + remaining * price_usd - cost_usd as pnl,
         case when cost_usd > 0 then (proceeds_usd + remaining * price_usd) / cost_usd end
           as multiple
  from positions
)
"#;

const WALLET_LEADERBOARD_SQL_BODY: &str = r#"
, wallets as (
  select address,
         sum(pnl) as pnl, sum(realized) as realized, sum(unrealized) as unrealized,
         sum(cost_usd) as cost, count(*)::int as tokens,
         (count(*) filter (where pnl > 0))::int as wins,
         sum(trades)::int as trades, max(last_trade_at) as last_trade_at,
         (array_agg(launch_id order by pnl desc))[1] as best_launch_id,
         (array_agg(symbol order by pnl desc))[1] as best_symbol,
         (array_agg(pnl order by pnl desc))[1] as best_pnl,
         (array_agg(cost_usd order by pnl desc))[1] as best_cost,
         (array_agg(multiple order by pnl desc))[1] as best_multiple
  from valued
  group by address
  having sum(cost_usd) >= $4
), ranked as (
  select wallets.*, row_number() over (order by pnl desc, address) as rank
  from wallets
  where not exists (
    select 1 from public.protocol_deployments deployment
    where deployment.chain_id = $1 and deployment.address = wallets.address
      and deployment.status = 'active'
  )
  order by pnl desc
  limit $5
)
insert into market.wallet_leaderboard (
  chain_id, window_key, address, rank, address_kind, pnl_usd, realized_usd, unrealized_usd,
  cost_usd, tokens_traded, winning_tokens, trades, best_launch_id, best_symbol, best_pnl_usd,
  best_cost_usd, best_multiple, last_trade_at, cluster_count, methodology_version, computed_at
)
select $1, $2, ranked.address, ranked.rank, kind.address_kind, pnl, realized, unrealized, cost,
       tokens, wins, trades, best_launch_id, best_symbol, best_pnl, best_cost, best_multiple,
       last_trade_at,
       (select count(*) from public.protocol_scoped_cluster_members member
        join public.protocol_scoped_clusters cluster on cluster.cluster_id = member.cluster_id
        where member.chain_id = $1 and member.address = ranked.address
          and cluster.status = 'active')::int,
       $6, now()
from ranked
left join public.chain_addresses kind on kind.chain_id = $1 and kind.address = ranked.address
"#;

const CLUSTER_LEADERBOARD_SQL_BODY: &str = r#"
, scoped as materialized (
  select cluster.cluster_id, cluster.first_seen_at, cluster.subject_address, domain.domain_key,
         cluster.cluster_kind, valued.*
  from valued
  join public.protocol_scoped_clusters cluster
    on cluster.chain_id = $1 and cluster.status = 'active'
   and cluster.cluster_kind = 'issuer_cohort'
   and cluster.subject_address = valued.creator_address
  join public.protocol_scoped_cluster_members member
    on member.cluster_id = cluster.cluster_id and member.address = valued.address
  join public.protocol_intelligence_domains domain on domain.domain_id = cluster.domain_id
), member_totals as (
  select cluster_id, address, sum(pnl) as pnl from scoped group by cluster_id, address
), clusters as (
  select scoped.cluster_id, min(domain_key) as domain_key, min(cluster_kind) as cluster_kind,
         min(subject_address) as subject_address, min(first_seen_at) as first_seen_at,
         sum(pnl) as pnl, sum(realized) as realized, sum(unrealized) as unrealized,
         sum(cost_usd) as cost, count(distinct launch_id)::int as tokens,
         count(distinct address)::int as active_members,
         max(last_trade_at) as last_trade_at,
         (array_agg(launch_id order by pnl desc))[1] as best_launch_id,
         (array_agg(symbol order by pnl desc))[1] as best_symbol,
         (array_agg(pnl order by pnl desc))[1] as best_pnl
  from scoped
  group by scoped.cluster_id
), ranked as (
  select clusters.*,
         row_number() over (order by pnl desc, cluster_id) as rank,
         row_number() over (order by first_seen_at desc nulls last, cluster_id) as newest
  from clusters
)
insert into market.cluster_leaderboard (
  chain_id, window_key, cluster_id, rank, domain_key, cluster_kind, subject_address,
  member_count, active_members, pnl_usd, realized_usd, unrealized_usd, cost_usd, tokens_traded,
  winning_members, best_launch_id, best_symbol, best_pnl_usd, last_trade_at, first_seen_at,
  methodology_version, computed_at
)
select $1, $2, ranked.cluster_id, ranked.rank, domain_key, cluster_kind, subject_address,
       (select count(*) from public.protocol_scoped_cluster_members member
        where member.cluster_id = ranked.cluster_id)::int,
       active_members, pnl, realized, unrealized, cost, tokens,
       (select count(*) from member_totals
        where member_totals.cluster_id = ranked.cluster_id and member_totals.pnl > 0)::int,
       best_launch_id, best_symbol, best_pnl, last_trade_at, first_seen_at, $6, now()
from ranked
where rank <= $4 or newest <= $5
"#;

static WALLET_LEADERBOARD_SQL: std::sync::LazyLock<String> =
    std::sync::LazyLock::new(|| format!("with {POSITION_PNL_CTE}{WALLET_LEADERBOARD_SQL_BODY}"));
static CLUSTER_LEADERBOARD_SQL: std::sync::LazyLock<String> =
    std::sync::LazyLock::new(|| format!("with {POSITION_PNL_CTE}{CLUSTER_LEADERBOARD_SQL_BODY}"));

fn db_error(error: impl std::fmt::Display) -> LedgerError {
    LedgerError::Persistence(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn windows_round_trip_and_refresh_by_size() {
        for window in EarningsWindow::ALL {
            assert_eq!(EarningsWindow::parse(Some(window.key())), window);
        }
        assert_eq!(EarningsWindow::parse(Some("bogus")), EarningsWindow::Day);
        assert!(EarningsWindow::Day.refresh_every() < EarningsWindow::Month.refresh_every());
        assert_eq!(EarningsWindow::Week.hours(), 168);
    }

    #[test]
    fn leaderboard_statements_share_the_pnl_definition() {
        assert!(WALLET_LEADERBOARD_SQL.contains("proceeds_usd + remaining * price_usd - cost_usd"));
        assert!(
            CLUSTER_LEADERBOARD_SQL.contains("proceeds_usd + remaining * price_usd - cost_usd")
        );
        // Incomplete inventories (sold more than bought on the curve) never rank.
        assert!(WALLET_LEADERBOARD_SQL.contains("position.sold_token <= position.bought_token"));
    }
}
