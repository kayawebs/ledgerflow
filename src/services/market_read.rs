//! Read models over the `market` schema for the market web. Every query is
//! bounded and index-backed; none scans the full trade-fact table.

use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::{FromRow, PgPool, Row};
use uuid::Uuid;

use crate::core::errors::{LedgerError, LedgerResult};
use crate::services::market_data::NATIVE_ASSET_ADDRESS;

#[derive(Clone, Debug, Default, Serialize)]
pub struct MarketTicker {
    pub native_usd: Option<f64>,
    pub volume_24h_usd: f64,
    pub trades_24h: i64,
    pub active_tokens_24h: i64,
    pub launches_1h: i64,
    pub launches_24h: i64,
    pub graduations_24h: i64,
    pub top_gainer_symbol: Option<String>,
    pub top_gainer_launch_id: Option<Uuid>,
    pub top_gainer_change: Option<f64>,
    pub refreshed_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Serialize, FromRow)]
pub struct TokenRow {
    pub launch_id: Uuid,
    pub token_address: String,
    pub symbol: Option<String>,
    pub name: Option<String>,
    pub creator_address: Option<String>,
    pub quote_symbol: Option<String>,
    pub protocol_version: Option<String>,
    pub lifecycle_phase: String,
    pub launched_at: Option<DateTime<Utc>>,
    pub graduated_at: Option<DateTime<Utc>>,
    pub price_quote: Option<f64>,
    pub price_usd: Option<f64>,
    pub market_cap_usd: Option<f64>,
    pub change_5m_pct: Option<f64>,
    pub change_1h_pct: Option<f64>,
    pub change_6h_pct: Option<f64>,
    pub change_24h_pct: Option<f64>,
    pub volume_5m_usd: Option<f64>,
    pub volume_1h_usd: Option<f64>,
    pub volume_24h_usd: Option<f64>,
    pub net_flow_1h_usd: Option<f64>,
    pub net_flow_24h_usd: Option<f64>,
    pub trades_1h: Option<i32>,
    pub trades_24h: Option<i32>,
    pub buys_24h: Option<i32>,
    pub sells_24h: Option<i32>,
    pub buyers_24h: Option<i32>,
    pub sellers_24h: Option<i32>,
    pub bonding_progress_pct: Option<f64>,
    pub ath_market_cap_usd: Option<f64>,
    pub sparkline_24h: Option<Vec<Option<f64>>>,
    pub last_trade_at: Option<DateTime<Utc>>,
    pub hot_score: Option<f64>,
    pub usd_status: Option<String>,
    pub price_grade: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum TokenBoard {
    Hot,
    Gainers,
    Losers,
    Volume,
    New,
    Graduating,
    Graduated,
    MarketCap,
}

impl TokenBoard {
    pub fn parse(value: Option<&str>) -> Self {
        match value.unwrap_or("hot") {
            "gainers" => Self::Gainers,
            "losers" => Self::Losers,
            "volume" => Self::Volume,
            "new" => Self::New,
            "graduating" => Self::Graduating,
            "graduated" => Self::Graduated,
            "mcap" => Self::MarketCap,
            _ => Self::Hot,
        }
    }

    pub fn key(self) -> &'static str {
        match self {
            Self::Hot => "hot",
            Self::Gainers => "gainers",
            Self::Losers => "losers",
            Self::Volume => "volume",
            Self::New => "new",
            Self::Graduating => "graduating",
            Self::Graduated => "graduated",
            Self::MarketCap => "mcap",
        }
    }

    pub const ALL: [TokenBoard; 8] = [
        Self::Hot,
        Self::Gainers,
        Self::Losers,
        Self::Volume,
        Self::New,
        Self::Graduating,
        Self::Graduated,
        Self::MarketCap,
    ];
}

#[derive(Clone, Debug, Serialize)]
pub struct TokenPage {
    pub rows: Vec<TokenRow>,
    pub total: i64,
    pub page: i64,
    pub page_size: i64,
}

#[derive(Clone, Debug, Serialize, FromRow)]
pub struct CandleRow {
    pub bucket_start: DateTime<Utc>,
    pub open: Option<f64>,
    pub high: Option<f64>,
    pub low: Option<f64>,
    pub close: Option<f64>,
    pub volume_usd: Option<f64>,
    pub trades: i64,
}

#[derive(Clone, Debug, Serialize, FromRow)]
pub struct TradeRow {
    pub launch_id: Uuid,
    pub symbol: Option<String>,
    pub token_address: String,
    pub trader_address: String,
    pub trade_side: String,
    pub quote_amount: Option<f64>,
    pub token_amount: Option<f64>,
    pub usd_value: Option<f64>,
    pub quote_symbol: Option<String>,
    pub tx_hash: String,
    pub occurred_at: Option<DateTime<Utc>>,
}

/// Sort columns accepted by the token table.
pub const TOKEN_SORTS: [&str; 9] = [
    "hot",
    "price",
    "change_5m",
    "change_1h",
    "change_24h",
    "volume",
    "mcap",
    "buyers",
    "age",
];

/// Minimum 24h USD volume for a token to enter gainer/loser boards, so a
/// single dust trade cannot top the ranking.
pub const MIN_RANKED_VOLUME_USD: f64 = 250.0;

const TOKEN_COLUMNS: &str = r#"
  s.launch_id, s.token_address, s.symbol, s.name, s.creator_address, s.quote_symbol,
  s.protocol_version, s.lifecycle_phase, s.launched_at, s.graduated_at,
  s.price_quote::float8 as price_quote, s.price_usd::float8 as price_usd,
  s.market_cap_usd::float8 as market_cap_usd,
  s.change_5m_pct::float8 as change_5m_pct, s.change_1h_pct::float8 as change_1h_pct,
  s.change_6h_pct::float8 as change_6h_pct, s.change_24h_pct::float8 as change_24h_pct,
  s.volume_5m_usd::float8 as volume_5m_usd, s.volume_1h_usd::float8 as volume_1h_usd,
  s.volume_24h_usd::float8 as volume_24h_usd,
  s.net_flow_1h_usd::float8 as net_flow_1h_usd, s.net_flow_24h_usd::float8 as net_flow_24h_usd,
  s.trades_1h, s.trades_24h, s.buys_24h, s.sells_24h, s.buyers_24h, s.sellers_24h,
  s.bonding_progress_pct::float8 as bonding_progress_pct,
  s.ath_market_cap_usd::float8 as ath_market_cap_usd,
  s.sparkline_24h::float8[] as sparkline_24h,
  s.last_trade_at, s.hot_score, s.usd_status, s.price_grade
"#;

#[derive(Clone)]
pub struct MarketReadService {
    pool: PgPool,
}

impl MarketReadService {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn ticker(&self, chain_id: u64) -> LedgerResult<MarketTicker> {
        let row = sqlx::query(
            r#"select
                 (select price_usd::float8 from market.asset_usd_prices
                  where chain_id = $1 and asset_address = $2
                  order by bucket_start desc limit 1) as native_usd,
                 (select coalesce(sum(volume_24h_usd), 0)::float8 from market.token_stats
                  where chain_id = $1 and trades_24h > 0) as volume_24h,
                 (select coalesce(sum(trades_24h), 0)::bigint from market.token_stats
                  where chain_id = $1 and trades_24h > 0) as trades_24h,
                 (select count(*) from market.token_stats
                  where chain_id = $1 and trades_24h > 0) as active_tokens,
                 (select count(*) from public.protocol_token_launches
                  where chain_id = $1 and domain_id = any(
                    select domain_id from public.protocol_intelligence_domains
                    where chain_id = $1 and domain_kind = 'token_launch')
                    and launched_at > now() - interval '1 hour') as launches_1h,
                 (select count(*) from public.protocol_token_launches
                  where chain_id = $1 and domain_id = any(
                    select domain_id from public.protocol_intelligence_domains
                    where chain_id = $1 and domain_kind = 'token_launch')
                    and launched_at > now() - interval '24 hours') as launches_24h,
                 (select count(*) from market.token_stats
                  where chain_id = $1 and graduated_at > now() - interval '24 hours') as graduations,
                 (select max(refreshed_at) from market.token_stats where chain_id = $1) as refreshed_at"#,
        )
        .bind(chain_id as i64)
        .bind(NATIVE_ASSET_ADDRESS)
        .fetch_one(&self.pool)
        .await
        .map_err(db_error)?;
        let gainer = sqlx::query(
            r#"select launch_id, symbol, change_24h_pct::float8 as change
               from market.token_stats
               where chain_id = $1 and trades_24h > 0 and volume_24h_usd >= $2
                 and change_24h_pct is not null
               order by change_24h_pct desc
               limit 1"#,
        )
        .bind(chain_id as i64)
        .bind(MIN_RANKED_VOLUME_USD * 4.0)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_error)?;
        Ok(MarketTicker {
            native_usd: row.get("native_usd"),
            volume_24h_usd: row.get("volume_24h"),
            trades_24h: row.get("trades_24h"),
            active_tokens_24h: row.get("active_tokens"),
            launches_1h: row.get("launches_1h"),
            launches_24h: row.get("launches_24h"),
            graduations_24h: row.get("graduations"),
            top_gainer_symbol: gainer.as_ref().and_then(|row| row.get("symbol")),
            top_gainer_launch_id: gainer.as_ref().map(|row| row.get("launch_id")),
            top_gainer_change: gainer.as_ref().and_then(|row| row.get("change")),
            refreshed_at: row.get("refreshed_at"),
        })
    }

    /// Leaderboard slices for the overview page.
    pub async fn board(
        &self,
        chain_id: u64,
        board: TokenBoard,
        limit: i64,
    ) -> LedgerResult<Vec<TokenRow>> {
        self.token_page(chain_id, board, None, None, true, 1, limit, false)
            .await
            .map(|page| page.rows)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn token_page(
        &self,
        chain_id: u64,
        board: TokenBoard,
        sort: Option<&str>,
        search: Option<&str>,
        descending: bool,
        page: i64,
        page_size: i64,
        with_total: bool,
    ) -> LedgerResult<TokenPage> {
        let page = page.max(1);
        let page_size = page_size.clamp(1, 100);
        let (filter, default_order) = board_clause(board);
        let order = sort
            .and_then(sort_expression)
            .map(|expression| {
                format!(
                    "{expression} {} nulls last, s.launch_id",
                    if descending { "desc" } else { "asc" }
                )
            })
            .unwrap_or_else(|| default_order.to_owned());
        let search = search
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| value.to_ascii_lowercase());
        let search_clause = match &search {
            Some(value) if value.starts_with("0x") && value.len() == 42 => {
                " and s.token_address = $3"
            }
            Some(_) => " and lower(s.symbol) like $3 || '%'",
            None => " and $3::text is null",
        };
        let sql = format!(
            "select {TOKEN_COLUMNS}
             from market.token_stats s
             where s.chain_id = $1 and {filter}{search_clause}
             order by {order}
             limit $2 offset $4"
        );
        let rows = sqlx::query_as::<_, TokenRow>(&sql)
            .bind(chain_id as i64)
            .bind(page_size)
            .bind(search.as_deref())
            .bind((page - 1) * page_size)
            .bind(MIN_RANKED_VOLUME_USD)
            .fetch_all(&self.pool)
            .await
            .map_err(db_error)?;
        // A separate count keeps the page query an index-ordered LIMIT scan.
        let total = if with_total {
            let count_sql = format!(
                "select count(*) from market.token_stats s
                 where s.chain_id = $1 and {filter}{search_clause}
                   and $2::bigint is not null and $4::bigint is not null"
            );
            sqlx::query_scalar::<_, i64>(&count_sql)
                .bind(chain_id as i64)
                .bind(page_size)
                .bind(search.as_deref())
                .bind((page - 1) * page_size)
                .bind(MIN_RANKED_VOLUME_USD)
                .fetch_one(&self.pool)
                .await
                .map_err(db_error)?
        } else {
            rows.len() as i64
        };
        Ok(TokenPage {
            rows,
            total,
            page,
            page_size,
        })
    }

    pub async fn token(&self, chain_id: u64, launch_id: Uuid) -> LedgerResult<Option<TokenRow>> {
        let sql = format!(
            "select {TOKEN_COLUMNS} from market.token_stats s where s.chain_id = $1 and s.launch_id = $2"
        );
        sqlx::query_as::<_, TokenRow>(&sql)
            .bind(chain_id as i64)
            .bind(launch_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(db_error)
    }

    pub async fn launch_id_for_token(
        &self,
        chain_id: u64,
        token_address: &str,
    ) -> LedgerResult<Option<Uuid>> {
        sqlx::query_scalar(
            r#"select launch_id from public.protocol_token_launches
               where chain_id = $1 and token_address = $2
               order by launched_at desc nulls last limit 1"#,
        )
        .bind(chain_id as i64)
        .bind(token_address)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_error)
    }

    /// Candles in USD aggregated to `interval_minutes`, newest `limit` buckets.
    pub async fn candles(
        &self,
        launch_id: Uuid,
        interval_minutes: i32,
        limit: i64,
    ) -> LedgerResult<Vec<CandleRow>> {
        let interval_minutes = interval_minutes.clamp(1, 1_440);
        let mut rows = sqlx::query_as::<_, CandleRow>(
            r#"with scoped as (
                 select c.*, coalesce(c.quote_usd_price, s.quote_usd_price) as usd
                 from market.token_candles_1m c
                 join market.token_stats s on s.launch_id = c.launch_id
                 where c.launch_id = $1
                   and c.bucket_start >= (
                     select max(bucket_start) from market.token_candles_1m where launch_id = $1
                   ) - make_interval(mins => $2 * $3::int)
               )
               select to_timestamp(floor(extract(epoch from bucket_start) / ($2 * 60)) * ($2 * 60))
                        as bucket_start,
                      ((array_agg(open_price_quote * usd order by bucket_start)
                        filter (where open_price_quote is not null))[1])::float8 as open,
                      max(high_price_quote * usd)::float8 as high,
                      min(low_price_quote * usd)::float8 as low,
                      ((array_agg(close_price_quote * usd order by bucket_start desc)
                        filter (where close_price_quote is not null))[1])::float8 as close,
                      sum((buy_quote + sell_quote) * usd)::float8 as volume_usd,
                      sum(trade_count)::bigint as trades
               from scoped
               group by 1
               order by 1 desc
               limit $3"#,
        )
        .bind(launch_id)
        .bind(interval_minutes)
        .bind(limit.clamp(1, 1_000))
        .fetch_all(&self.pool)
        .await
        .map_err(db_error)?;
        rows.reverse();
        Ok(rows)
    }

    /// Latest curve trades and post-graduation pool swaps for one launch.
    pub async fn recent_trades(&self, launch_id: Uuid, limit: i64) -> LedgerResult<Vec<TradeRow>> {
        sqlx::query_as::<_, TradeRow>(
            r#"with scale as (
                 select s.launch_id, s.symbol, s.token_address, s.quote_symbol, s.quote_usd_price,
                        t.decimals as token_decimals,
                        coalesce(
                          (select decimals from public.erc20_token_metadata m
                           where m.chain_id = s.chain_id and m.token_address = s.quote_token_address),
                          case when s.quote_token_address = $3 then 18 end) as quote_decimals
                 from market.token_stats s
                 join public.erc20_token_metadata t
                   on t.chain_id = s.chain_id and t.token_address = s.token_address
                 where s.launch_id = $1
               ), trades as (
                 (select f.trader_address, f.trade_side, f.quote_amount_raw::numeric as quote_raw,
                         f.token_amount_raw::numeric as token_raw, f.tx_hash, f.occurred_at,
                         f.block_number, f.log_index
                  from public.protocol_launch_trade_facts f
                  where f.launch_id = $1 and f.is_canonical
                  order by f.block_number desc, f.tx_index desc, f.log_index desc
                  limit $2)
                 union all
                 (select p.sender_address, p.trade_side, p.quote_amount_raw, p.token_amount_raw,
                         p.tx_hash, p.occurred_at, p.block_number, p.log_index
                  from market.pool_swap_facts p
                  where p.launch_id = $1
                  order by p.block_number desc, p.log_index desc
                  limit $2)
               )
               select scale.launch_id, scale.symbol, scale.token_address, trades.trader_address,
                      trades.trade_side,
                      (trades.quote_raw * power(10::numeric, -(scale.quote_decimals)))::float8
                        as quote_amount,
                      (trades.token_raw * power(10::numeric, -(scale.token_decimals)))::float8
                        as token_amount,
                      (trades.quote_raw * power(10::numeric, -(scale.quote_decimals))
                        * scale.quote_usd_price)::float8 as usd_value,
                      scale.quote_symbol, trades.tx_hash, trades.occurred_at
               from trades cross join scale
               order by trades.block_number desc, trades.log_index desc
               limit $2"#,
        )
        .bind(launch_id)
        .bind(limit.clamp(1, 200))
        .bind(NATIVE_ASSET_ADDRESS)
        .fetch_all(&self.pool)
        .await
        .map_err(db_error)
    }

    /// Recent trades across all tokens whose USD value exceeds `min_usd`.
    pub async fn big_trades(
        &self,
        chain_id: u64,
        min_usd: f64,
        lookback_blocks: i64,
        limit: i64,
    ) -> LedgerResult<Vec<TradeRow>> {
        let head: Option<i64> = sqlx::query_scalar(
            "select max(block_number) from public.protocol_launch_trade_facts where chain_id = $1",
        )
        .bind(chain_id as i64)
        .fetch_one(&self.pool)
        .await
        .map_err(db_error)?;
        let Some(head) = head else {
            return Ok(Vec::new());
        };
        // The lower block bound is bound as a literal so the planner can use
        // the (chain_id, block_number) index instead of a parallel scan.
        sqlx::query_as::<_, TradeRow>(
            r#"with recent as materialized (
                 select f.launch_id, f.token_address, f.trader_address, f.trade_side,
                        f.quote_amount_raw::numeric as quote_amount_raw,
                        f.token_amount_raw::numeric as token_amount_raw,
                        f.tx_hash, f.occurred_at, f.block_number, f.log_index
                 from public.protocol_launch_trade_facts f
                 where f.chain_id = $1 and f.block_number > $3 and f.is_canonical
                 union all
                 select p.launch_id, s.token_address, p.sender_address, p.trade_side,
                        p.quote_amount_raw, p.token_amount_raw, p.tx_hash, p.occurred_at,
                        p.block_number, p.log_index
                 from market.pool_swap_facts p
                 join market.token_stats s on s.launch_id = p.launch_id
                 where p.chain_id = $1 and p.block_number > $3
               ), valued as (
                 select f.launch_id, s.symbol, f.token_address, f.trader_address, f.trade_side,
                        (f.quote_amount_raw * power(10::numeric, -(q.decimals)))::float8
                          as quote_amount,
                        (f.token_amount_raw * power(10::numeric, -(18)))::float8
                          as token_amount,
                        (f.quote_amount_raw * power(10::numeric, -(q.decimals))
                          * s.quote_usd_price)::float8 as usd_value,
                        s.quote_symbol, f.tx_hash, f.occurred_at, f.block_number, f.log_index
                 from recent f
                 join market.token_stats s on s.launch_id = f.launch_id
                 cross join lateral (
                   select coalesce(
                     (select decimals from public.erc20_token_metadata m
                      where m.chain_id = s.chain_id and m.token_address = s.quote_token_address),
                     case when s.quote_token_address = $5 then 18 end) as decimals
                 ) q
               )
               select launch_id, symbol, token_address, trader_address, trade_side, quote_amount,
                      token_amount, usd_value, quote_symbol, tx_hash, occurred_at
               from valued
               where usd_value >= $2
               order by block_number desc, log_index desc
               limit $4"#,
        )
        .bind(chain_id as i64)
        .bind(min_usd)
        .bind(head - lookback_blocks.clamp(100, 200_000))
        .bind(limit.clamp(1, 100))
        .bind(NATIVE_ASSET_ADDRESS)
        .fetch_all(&self.pool)
        .await
        .map_err(db_error)
    }

    /// Newest launches, including ones that have not traded yet.
    pub async fn newest_launches(&self, chain_id: u64, limit: i64) -> LedgerResult<Vec<TokenRow>> {
        let sql = format!(
            "select {TOKEN_COLUMNS}
             from market.token_stats s
             where s.chain_id = $1 and s.launched_at > now() - interval '24 hours'
             order by s.launched_at desc
             limit $2"
        );
        sqlx::query_as::<_, TokenRow>(&sql)
            .bind(chain_id as i64)
            .bind(limit.clamp(1, 100))
            .fetch_all(&self.pool)
            .await
            .map_err(db_error)
    }
}

#[derive(Clone, Debug, Serialize, FromRow)]
pub struct WalletRankRow {
    pub rank: i32,
    pub address: String,
    pub address_kind: Option<String>,
    pub pnl_usd: f64,
    pub realized_usd: f64,
    pub unrealized_usd: f64,
    pub cost_usd: f64,
    pub tokens_traded: i32,
    pub winning_tokens: i32,
    pub trades: i32,
    pub best_launch_id: Option<Uuid>,
    pub best_symbol: Option<String>,
    pub best_pnl_usd: Option<f64>,
    pub best_cost_usd: Option<f64>,
    pub best_multiple: Option<f64>,
    pub last_trade_at: Option<DateTime<Utc>>,
    pub cluster_count: i32,
    pub computed_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, FromRow)]
pub struct ClusterRankRow {
    pub rank: i32,
    pub cluster_id: Uuid,
    pub domain_key: String,
    pub cluster_kind: String,
    pub subject_address: Option<String>,
    pub member_count: i32,
    pub active_members: i32,
    pub pnl_usd: f64,
    pub realized_usd: f64,
    pub unrealized_usd: f64,
    pub cost_usd: f64,
    pub tokens_traded: i32,
    pub winning_members: i32,
    pub best_launch_id: Option<Uuid>,
    pub best_symbol: Option<String>,
    pub best_pnl_usd: Option<f64>,
    pub last_trade_at: Option<DateTime<Utc>>,
    pub first_seen_at: Option<DateTime<Utc>>,
    pub computed_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, FromRow)]
pub struct PositionRow {
    pub launch_id: Uuid,
    pub symbol: Option<String>,
    pub token_address: String,
    pub buy_count: i32,
    pub sell_count: i32,
    pub cost_usd: Option<f64>,
    pub proceeds_usd: Option<f64>,
    pub holding_usd: Option<f64>,
    pub pnl_usd: Option<f64>,
    pub multiple: Option<f64>,
    pub complete_inventory: bool,
    pub first_trade_at: Option<DateTime<Utc>>,
    pub last_trade_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct AddressEarnings {
    pub pnl_usd: f64,
    pub realized_usd: f64,
    pub unrealized_usd: f64,
    pub cost_usd: f64,
    pub tokens: i64,
    pub winning_tokens: i64,
    pub excluded_positions: i64,
    pub launches_created: i64,
    pub first_trade_at: Option<DateTime<Utc>>,
    pub last_trade_at: Option<DateTime<Utc>>,
}

const WALLET_RANK_COLUMNS: &str = r#"
  rank, address, address_kind, pnl_usd::float8 as pnl_usd, realized_usd::float8 as realized_usd,
  unrealized_usd::float8 as unrealized_usd, cost_usd::float8 as cost_usd, tokens_traded,
  winning_tokens, trades, best_launch_id, best_symbol, best_pnl_usd::float8 as best_pnl_usd,
  best_cost_usd::float8 as best_cost_usd, best_multiple::float8 as best_multiple, last_trade_at,
  cluster_count, computed_at
"#;

const CLUSTER_RANK_COLUMNS: &str = r#"
  rank, cluster_id, domain_key, cluster_kind, subject_address, member_count, active_members,
  pnl_usd::float8 as pnl_usd, realized_usd::float8 as realized_usd,
  unrealized_usd::float8 as unrealized_usd, cost_usd::float8 as cost_usd, tokens_traded,
  winning_members, best_launch_id, best_symbol, best_pnl_usd::float8 as best_pnl_usd,
  last_trade_at, first_seen_at, computed_at
"#;

impl MarketReadService {
    pub async fn wallet_leaderboard(
        &self,
        chain_id: u64,
        window: &str,
        limit: i64,
        offset: i64,
    ) -> LedgerResult<Vec<WalletRankRow>> {
        let sql = format!(
            "select {WALLET_RANK_COLUMNS} from market.wallet_leaderboard
             where chain_id = $1 and window_key = $2 order by rank limit $3 offset $4"
        );
        sqlx::query_as::<_, WalletRankRow>(&sql)
            .bind(chain_id as i64)
            .bind(window)
            .bind(limit.clamp(1, 500))
            .bind(offset.max(0))
            .fetch_all(&self.pool)
            .await
            .map_err(db_error)
    }

    pub async fn cluster_leaderboard(
        &self,
        chain_id: u64,
        window: &str,
        limit: i64,
    ) -> LedgerResult<Vec<ClusterRankRow>> {
        let sql = format!(
            "select {CLUSTER_RANK_COLUMNS} from market.cluster_leaderboard
             where chain_id = $1 and window_key = $2 order by rank limit $3"
        );
        sqlx::query_as::<_, ClusterRankRow>(&sql)
            .bind(chain_id as i64)
            .bind(window)
            .bind(limit.clamp(1, 500))
            .fetch_all(&self.pool)
            .await
            .map_err(db_error)
    }

    /// Most recently discovered clusters that already have ranked activity.
    pub async fn newest_clusters(
        &self,
        chain_id: u64,
        window: &str,
        limit: i64,
    ) -> LedgerResult<Vec<ClusterRankRow>> {
        let sql = format!(
            "select {CLUSTER_RANK_COLUMNS} from market.cluster_leaderboard
             where chain_id = $1 and window_key = $2
             order by first_seen_at desc nulls last limit $3"
        );
        sqlx::query_as::<_, ClusterRankRow>(&sql)
            .bind(chain_id as i64)
            .bind(window)
            .bind(limit.clamp(1, 100))
            .fetch_all(&self.pool)
            .await
            .map_err(db_error)
    }

    pub async fn wallet_rank(
        &self,
        chain_id: u64,
        window: &str,
        address: &str,
    ) -> LedgerResult<Option<WalletRankRow>> {
        let sql = format!(
            "select {WALLET_RANK_COLUMNS} from market.wallet_leaderboard
             where chain_id = $1 and window_key = $2 and address = $3"
        );
        sqlx::query_as::<_, WalletRankRow>(&sql)
            .bind(chain_id as i64)
            .bind(window)
            .bind(address)
            .fetch_optional(&self.pool)
            .await
            .map_err(db_error)
    }

    pub async fn cluster_rank(
        &self,
        chain_id: u64,
        window: &str,
        cluster_id: Uuid,
    ) -> LedgerResult<Option<ClusterRankRow>> {
        let sql = format!(
            "select {CLUSTER_RANK_COLUMNS} from market.cluster_leaderboard
             where chain_id = $1 and window_key = $2 and cluster_id = $3"
        );
        sqlx::query_as::<_, ClusterRankRow>(&sql)
            .bind(chain_id as i64)
            .bind(window)
            .bind(cluster_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(db_error)
    }

    /// Positions of one wallet valued at the latest curve price, best first.
    pub async fn address_positions(
        &self,
        chain_id: u64,
        address: &str,
        limit: i64,
    ) -> LedgerResult<Vec<PositionRow>> {
        sqlx::query_as::<_, PositionRow>(
            r#"select p.launch_id, s.symbol, s.token_address, p.buy_count, p.sell_count,
                      p.cost_usd::float8 as cost_usd, p.proceeds_usd::float8 as proceeds_usd,
                      (greatest(p.bought_token - p.sold_token, 0) * s.price_usd)::float8
                        as holding_usd,
                      case when p.sold_token <= p.bought_token * 1.000001 then
                        (p.proceeds_usd + greatest(p.bought_token - p.sold_token, 0) * s.price_usd
                          - p.cost_usd)::float8 end as pnl_usd,
                      case when p.sold_token <= p.bought_token * 1.000001 and p.cost_usd > 0 then
                        ((p.proceeds_usd + greatest(p.bought_token - p.sold_token, 0) * s.price_usd)
                          / p.cost_usd)::float8 end as multiple,
                      p.sold_token <= p.bought_token * 1.000001 as complete_inventory,
                      p.first_trade_at, p.last_trade_at
               from market.wallet_positions p
               join market.token_stats s on s.launch_id = p.launch_id
               where p.chain_id = $1 and p.address = $2
               order by p.last_trade_at desc
               limit $3"#,
        )
        .bind(chain_id as i64)
        .bind(address)
        .bind(limit.clamp(1, 500))
        .fetch_all(&self.pool)
        .await
        .map_err(db_error)
    }

    pub async fn address_earnings(
        &self,
        chain_id: u64,
        address: &str,
    ) -> LedgerResult<AddressEarnings> {
        let row = sqlx::query(
            r#"with valued as (
                 select p.*, s.price_usd,
                        least(p.sold_token / nullif(p.bought_token, 0), 1) as sold_ratio,
                        greatest(p.bought_token - p.sold_token, 0) as remaining,
                        p.bought_token > 0 and p.sold_token <= p.bought_token * 1.000001
                          and p.cost_usd is not null and p.proceeds_usd is not null
                          and s.price_usd is not null and s.price_grade = 'reliable' as eligible
                 from market.wallet_positions p
                 join market.token_stats s on s.launch_id = p.launch_id
                 where p.chain_id = $1 and p.address = $2
               )
               select
                 coalesce(sum(proceeds_usd + remaining * price_usd - cost_usd)
                   filter (where eligible), 0)::float8 as pnl,
                 coalesce(sum(proceeds_usd - cost_usd * sold_ratio) filter (where eligible), 0)::float8
                   as realized,
                 coalesce(sum(remaining * price_usd - cost_usd * (1 - sold_ratio))
                   filter (where eligible), 0)::float8 as unrealized,
                 coalesce(sum(cost_usd) filter (where eligible), 0)::float8 as cost,
                 count(*) filter (where eligible) as tokens,
                 count(*) filter (where eligible
                   and proceeds_usd - cost_usd * sold_ratio > 0) as wins,
                 count(*) filter (where not eligible) as excluded,
                 (select count(*) from public.protocol_token_launches
                  where chain_id = $1 and creator_address = $2) as created,
                 min(first_trade_at) as first_trade_at,
                 max(last_trade_at) as last_trade_at
               from valued"#,
        )
        .bind(chain_id as i64)
        .bind(address)
        .fetch_one(&self.pool)
        .await
        .map_err(db_error)?;
        Ok(AddressEarnings {
            pnl_usd: row.get("pnl"),
            realized_usd: row.get("realized"),
            unrealized_usd: row.get("unrealized"),
            cost_usd: row.get("cost"),
            tokens: row.get("tokens"),
            winning_tokens: row.get("wins"),
            excluded_positions: row.get("excluded"),
            launches_created: row.get("created"),
            first_trade_at: row.get("first_trade_at"),
            last_trade_at: row.get("last_trade_at"),
        })
    }

    /// Cluster members ranked by their earnings on the cluster creator's launches.
    pub async fn cluster_member_earnings(
        &self,
        cluster_id: Uuid,
        limit: i64,
    ) -> LedgerResult<Vec<(String, String, f64, f64, i64)>> {
        let rows = sqlx::query(
            r#"select m.address, m.member_role,
                      coalesce(sum(p.proceeds_usd + greatest(p.bought_token - p.sold_token, 0)
                        * s.price_usd - p.cost_usd), 0)::float8 as pnl,
                      coalesce(sum(p.cost_usd), 0)::float8 as cost,
                      count(p.launch_id) as tokens
               from public.protocol_scoped_cluster_members m
               join public.protocol_scoped_clusters c on c.cluster_id = m.cluster_id
               left join market.wallet_positions p
                 on p.chain_id = c.chain_id and p.address = m.address
                and p.creator_address = c.subject_address
                and p.bought_token > 0 and p.sold_token <= p.bought_token * 1.000001
                and p.cost_usd is not null and p.proceeds_usd is not null
               left join market.token_stats s
                 on s.launch_id = p.launch_id and s.price_grade = 'reliable'
               where m.cluster_id = $1 and (p.launch_id is null or s.launch_id is not null)
               group by m.address, m.member_role
               order by pnl desc
               limit $2"#,
        )
        .bind(cluster_id)
        .bind(limit.clamp(1, 500))
        .fetch_all(&self.pool)
        .await
        .map_err(db_error)?;
        Ok(rows
            .into_iter()
            .map(|row| {
                (
                    row.get("address"),
                    row.get("member_role"),
                    row.get("pnl"),
                    row.get("cost"),
                    row.get("tokens"),
                )
            })
            .collect())
    }
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct ArbitrageOverview {
    pub executions: i64,
    pub estimated: i64,
    pub profitable: i64,
    pub net_usd: f64,
    pub gross_usd: f64,
    pub gas_usd: f64,
    pub bots: i64,
    pub verified: i64,
    pub latest_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Serialize, FromRow)]
pub struct BotRankRow {
    pub rank: i32,
    pub bot_address: String,
    pub address_kind: Option<String>,
    pub net_usd: f64,
    pub gross_usd: f64,
    pub gas_usd: f64,
    pub executions: i32,
    pub profitable_executions: i32,
    pub unknown_executions: i32,
    pub senders: i32,
    pub verified_executions: i32,
    pub best_tx_hash: Option<String>,
    pub best_net_usd: Option<f64>,
    pub last_execution_at: Option<DateTime<Utc>>,
    pub cluster_id: Option<Uuid>,
    pub computed_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, FromRow)]
pub struct ArbitrageRow {
    pub tx_hash: String,
    pub bot_address: String,
    pub sender_address: Option<String>,
    pub block_time: Option<DateTime<Utc>>,
    pub swap_count: i32,
    pub pool_count: i32,
    pub net_usd: Option<f64>,
    pub gross_usd: Option<f64>,
    pub gas_usd: Option<f64>,
    pub status: String,
    pub is_verified: bool,
    pub tokens: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct LiquidationOverview {
    pub liquidations: i64,
    pub estimated: i64,
    pub repaid_usd: f64,
    pub seized_usd: f64,
    pub bonus_usd: f64,
    pub liquidators: i64,
    pub borrowers: i64,
    pub bad_debt_events: i64,
    pub latest_at: Option<DateTime<Utc>>,
    pub scanned_to_block: Option<i64>,
}

#[derive(Clone, Debug, Serialize, FromRow)]
pub struct LiquidatorRankRow {
    pub liquidator: String,
    pub liquidations: i64,
    pub repaid_usd: Option<f64>,
    pub bonus_usd: Option<f64>,
    pub best_bonus_usd: Option<f64>,
    pub markets: i64,
    pub last_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Serialize, FromRow)]
pub struct LiquidationRow {
    pub tx_hash: String,
    pub block_time: Option<DateTime<Utc>>,
    pub liquidator: String,
    pub borrower: String,
    pub loan_symbol: Option<String>,
    pub collateral_symbol: Option<String>,
    pub repaid_amount: Option<f64>,
    pub seized_amount: Option<f64>,
    pub repaid_usd: Option<f64>,
    pub bonus_usd: Option<f64>,
    pub bad_debt: bool,
    pub status: String,
}

const ARBITRAGE_ROW_COLUMNS: &str = r#"
  e.tx_hash, e.bot_address, e.sender_address, e.block_time, e.swap_count, e.pool_count,
  e.net_usd::float8 as net_usd, e.gross_usd::float8 as gross_usd, e.gas_usd::float8 as gas_usd,
  e.status, e.is_verified,
  (select string_agg(coalesce(m.symbol, left(d.token_address, 8)), ' · ')
   from market.arbitrage_estimate_deltas d
   left join public.erc20_token_metadata m
     on m.chain_id = d.chain_id and m.token_address = d.token_address
   where d.chain_id = e.chain_id and d.tx_hash = e.tx_hash) as tokens
"#;

const LIQUIDATION_ROW_COLUMNS: &str = r#"
  l.tx_hash, l.block_time, l.liquidator, l.borrower,
  loan.symbol as loan_symbol, collateral.symbol as collateral_symbol,
  l.repaid_amount::float8 as repaid_amount, l.seized_amount::float8 as seized_amount,
  l.repaid_usd::float8 as repaid_usd, l.bonus_usd::float8 as bonus_usd,
  l.bad_debt_raw > 0 as bad_debt, l.status
"#;

impl MarketReadService {
    pub async fn arbitrage_overview(
        &self,
        chain_id: u64,
        hours: i32,
    ) -> LedgerResult<ArbitrageOverview> {
        let row = sqlx::query(
            r#"select count(*) as executions,
                      count(*) filter (where status = 'estimated') as estimated,
                      count(*) filter (where status = 'estimated' and net_usd > 0) as profitable,
                      coalesce(sum(net_usd) filter (where status = 'estimated'), 0)::float8 as net,
                      coalesce(sum(gross_usd) filter (where status = 'estimated'), 0)::float8 as gross,
                      coalesce(sum(gas_usd) filter (where status = 'estimated'), 0)::float8 as gas,
                      count(distinct bot_address) as bots,
                      count(*) filter (where is_verified) as verified,
                      max(block_time) as latest_at
               from market.arbitrage_estimates
               where chain_id = $1 and block_time >= now() - make_interval(hours => $2)"#,
        )
        .bind(chain_id as i64)
        .bind(hours)
        .fetch_one(&self.pool)
        .await
        .map_err(db_error)?;
        Ok(ArbitrageOverview {
            executions: row.get("executions"),
            estimated: row.get("estimated"),
            profitable: row.get("profitable"),
            net_usd: row.get("net"),
            gross_usd: row.get("gross"),
            gas_usd: row.get("gas"),
            bots: row.get("bots"),
            verified: row.get("verified"),
            latest_at: row.get("latest_at"),
        })
    }

    pub async fn bot_leaderboard(
        &self,
        chain_id: u64,
        window: &str,
        limit: i64,
    ) -> LedgerResult<Vec<BotRankRow>> {
        sqlx::query_as::<_, BotRankRow>(
            r#"select rank, bot_address, address_kind, net_usd::float8 as net_usd,
                      gross_usd::float8 as gross_usd, gas_usd::float8 as gas_usd, executions,
                      profitable_executions, unknown_executions, senders, verified_executions,
                      best_tx_hash, best_net_usd::float8 as best_net_usd, last_execution_at,
                      cluster_id, computed_at
               from market.arbitrage_bot_leaderboard
               where chain_id = $1 and window_key = $2
               order by rank limit $3"#,
        )
        .bind(chain_id as i64)
        .bind(window)
        .bind(limit.clamp(1, 200))
        .fetch_all(&self.pool)
        .await
        .map_err(db_error)
    }

    pub async fn biggest_arbitrages(
        &self,
        chain_id: u64,
        hours: i32,
        limit: i64,
    ) -> LedgerResult<Vec<ArbitrageRow>> {
        let sql = format!(
            "select {ARBITRAGE_ROW_COLUMNS} from market.arbitrage_estimates e
             where e.chain_id = $1 and e.status = 'estimated'
               and e.block_time >= now() - make_interval(hours => $2)
             order by e.net_usd desc nulls last limit $3"
        );
        sqlx::query_as::<_, ArbitrageRow>(&sql)
            .bind(chain_id as i64)
            .bind(hours)
            .bind(limit.clamp(1, 100))
            .fetch_all(&self.pool)
            .await
            .map_err(db_error)
    }

    pub async fn recent_profitable_arbitrages(
        &self,
        chain_id: u64,
        min_net_usd: f64,
        limit: i64,
    ) -> LedgerResult<Vec<ArbitrageRow>> {
        let sql = format!(
            "select {ARBITRAGE_ROW_COLUMNS} from market.arbitrage_estimates e
             where e.chain_id = $1 and e.block_time >= now() - interval '6 hours'
               and e.status = 'estimated' and e.net_usd >= $2
             order by e.block_time desc limit $3"
        );
        sqlx::query_as::<_, ArbitrageRow>(&sql)
            .bind(chain_id as i64)
            .bind(min_net_usd)
            .bind(limit.clamp(1, 100))
            .fetch_all(&self.pool)
            .await
            .map_err(db_error)
    }

    pub async fn bot_executions(
        &self,
        chain_id: u64,
        bot_address: &str,
        limit: i64,
    ) -> LedgerResult<Vec<ArbitrageRow>> {
        let sql = format!(
            "select {ARBITRAGE_ROW_COLUMNS} from market.arbitrage_estimates e
             where e.chain_id = $1 and e.bot_address = $2
             order by e.block_time desc limit $3"
        );
        sqlx::query_as::<_, ArbitrageRow>(&sql)
            .bind(chain_id as i64)
            .bind(bot_address)
            .bind(limit.clamp(1, 200))
            .fetch_all(&self.pool)
            .await
            .map_err(db_error)
    }

    pub async fn liquidation_overview(
        &self,
        chain_id: u64,
        days: i32,
    ) -> LedgerResult<LiquidationOverview> {
        let row = sqlx::query(
            r#"select count(*) as liquidations,
                      count(*) filter (where status = 'estimated') as estimated,
                      coalesce(sum(repaid_usd), 0)::float8 as repaid,
                      coalesce(sum(seized_usd), 0)::float8 as seized,
                      coalesce(sum(bonus_usd), 0)::float8 as bonus,
                      count(distinct liquidator) as liquidators,
                      count(distinct borrower) as borrowers,
                      count(*) filter (where bad_debt_raw > 0) as bad_debt,
                      max(block_time) as latest_at,
                      (select last_position from market.projector_checkpoints
                       where checkpoint_key = 'liquidations:' || $1::text) as scanned_to
               from market.lending_liquidations
               where chain_id = $1 and block_time >= now() - make_interval(days => $2)"#,
        )
        .bind(chain_id as i64)
        .bind(days)
        .fetch_one(&self.pool)
        .await
        .map_err(db_error)?;
        Ok(LiquidationOverview {
            liquidations: row.get("liquidations"),
            estimated: row.get("estimated"),
            repaid_usd: row.get("repaid"),
            seized_usd: row.get("seized"),
            bonus_usd: row.get("bonus"),
            liquidators: row.get("liquidators"),
            borrowers: row.get("borrowers"),
            bad_debt_events: row.get("bad_debt"),
            latest_at: row.get("latest_at"),
            scanned_to_block: row.get("scanned_to"),
        })
    }

    pub async fn liquidator_leaderboard(
        &self,
        chain_id: u64,
        days: i32,
        limit: i64,
    ) -> LedgerResult<Vec<LiquidatorRankRow>> {
        sqlx::query_as::<_, LiquidatorRankRow>(
            r#"select liquidator, count(*) as liquidations,
                      sum(repaid_usd)::float8 as repaid_usd,
                      sum(bonus_usd)::float8 as bonus_usd,
                      max(bonus_usd)::float8 as best_bonus_usd,
                      count(distinct market_id) as markets,
                      max(block_time) as last_at
               from market.lending_liquidations
               where chain_id = $1 and block_time >= now() - make_interval(days => $2)
               group by liquidator
               order by sum(bonus_usd) desc nulls last, count(*) desc
               limit $3"#,
        )
        .bind(chain_id as i64)
        .bind(days)
        .bind(limit.clamp(1, 100))
        .fetch_all(&self.pool)
        .await
        .map_err(db_error)
    }

    pub async fn liquidations(
        &self,
        chain_id: u64,
        days: i32,
        order_by_bonus: bool,
        limit: i64,
    ) -> LedgerResult<Vec<LiquidationRow>> {
        let order = if order_by_bonus {
            "l.bonus_usd desc nulls last"
        } else {
            "l.block_time desc"
        };
        let sql = format!(
            "select {LIQUIDATION_ROW_COLUMNS}
             from market.lending_liquidations l
             left join public.erc20_token_metadata loan
               on loan.chain_id = l.chain_id and loan.token_address = l.loan_token
             left join public.erc20_token_metadata collateral
               on collateral.chain_id = l.chain_id and collateral.token_address = l.collateral_token
             where l.chain_id = $1 and l.block_time >= now() - make_interval(days => $2)
             order by {order} limit $3"
        );
        sqlx::query_as::<_, LiquidationRow>(&sql)
            .bind(chain_id as i64)
            .bind(days)
            .bind(limit.clamp(1, 200))
            .fetch_all(&self.pool)
            .await
            .map_err(db_error)
    }
}

/// Board filter and default ordering. `$5` is the minimum ranked volume.
fn board_clause(board: TokenBoard) -> (&'static str, &'static str) {
    match board {
        TokenBoard::Hot => (
            "s.trades_24h > 0 and $5::float8 is not null",
            "s.hot_score desc, s.launch_id",
        ),
        TokenBoard::Gainers => (
            "s.trades_24h > 0 and s.volume_24h_usd >= $5 and s.change_24h_pct > 0",
            "s.change_24h_pct desc nulls last, s.launch_id",
        ),
        TokenBoard::Losers => (
            "s.trades_24h > 0 and s.volume_24h_usd >= $5 and s.change_24h_pct < 0",
            "s.change_24h_pct asc nulls last, s.launch_id",
        ),
        TokenBoard::Volume => (
            "s.trades_24h > 0 and $5::float8 is not null",
            "s.volume_24h_usd desc, s.launch_id",
        ),
        TokenBoard::New => (
            "s.launched_at > now() - interval '24 hours' and $5::float8 is not null",
            "s.launched_at desc nulls last, s.launch_id",
        ),
        TokenBoard::Graduating => (
            "s.lifecycle_phase = 'curve_trading' and s.trades_24h > 0 and s.bonding_progress_pct < 100 and $5::float8 is not null",
            "s.bonding_progress_pct desc nulls last, s.launch_id",
        ),
        TokenBoard::Graduated => (
            "s.graduated_at is not null and $5::float8 is not null",
            "s.graduated_at desc nulls last, s.launch_id",
        ),
        TokenBoard::MarketCap => (
            "s.trades_24h > 0 and s.market_cap_usd is not null and $5::float8 is not null",
            "s.market_cap_usd desc nulls last, s.launch_id",
        ),
    }
}

fn sort_expression(sort: &str) -> Option<&'static str> {
    Some(match sort {
        "hot" => "s.hot_score",
        "price" => "s.price_usd",
        "change_5m" => "s.change_5m_pct",
        "change_1h" => "s.change_1h_pct",
        "change_24h" => "s.change_24h_pct",
        "volume" => "s.volume_24h_usd",
        "mcap" => "s.market_cap_usd",
        "buyers" => "s.buyers_24h",
        "age" => "s.launched_at",
        _ => return None,
    })
}

fn db_error(error: impl std::fmt::Display) -> LedgerError {
    LedgerError::Persistence(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn boards_round_trip_through_keys() {
        for board in TokenBoard::ALL {
            assert_eq!(TokenBoard::parse(Some(board.key())), board);
        }
        assert_eq!(TokenBoard::parse(Some("unknown")), TokenBoard::Hot);
        assert_eq!(TokenBoard::parse(None), TokenBoard::Hot);
    }

    #[test]
    fn only_whitelisted_sorts_reach_sql() {
        for sort in TOKEN_SORTS {
            assert!(sort_expression(sort).is_some(), "{sort}");
        }
        assert!(sort_expression("s.symbol; drop table x").is_none());
    }

    #[test]
    fn every_board_filter_binds_the_volume_floor() {
        // Postgres rejects statements that do not reference every bound
        // parameter's type, so each filter must mention $5.
        for board in TokenBoard::ALL {
            let (filter, _) = board_clause(board);
            assert!(filter.contains("$5"), "{board:?}");
        }
    }
}
