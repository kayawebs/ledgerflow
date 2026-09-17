//! Market MVP data foundation: quote-asset USD prices, one-minute token candles
//! and current token statistics derived from LedgerFlow's own trade facts.
//!
//! Read models live in the `market` schema and are rebuilt idempotently from
//! `protocol_launch_trade_facts` and `price_observations`. No third-party data
//! source is used; the only network dependency is the local RPC for
//! `totalSupply()` calls.

use std::{
    collections::BTreeSet,
    sync::Arc,
    time::{Duration, Instant},
};

use chrono::{DateTime, TimeZone, Utc};
use num_bigint::BigUint;
use serde::Serialize;
use sqlx::{Connection, PgConnection, PgPool, Row, migrate::Migrator};
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::{
    core::errors::{LedgerError, LedgerResult},
    indexer::rpc::IndexerRpcClient,
};

pub const MARKET_METHODOLOGY_VERSION: &str = "market_curve_v1";
pub const NATIVE_ASSET_ADDRESS: &str = "0x0000000000000000000000000000000000000000";
const TOTAL_SUPPLY_SELECTOR: &str = "0x18160ddd";
const QUOTE_ASSET_CACHE_TTL: Duration = Duration::from_secs(600);
/// Observations further than this factor from the asset's anchor are dropped.
const PRICE_OUTLIER_FACTOR: f64 = 1.6;
/// No quote asset on the market is worth more than this; larger values are
/// decoding or routing artefacts.
const MAX_QUOTE_ASSET_USD: f64 = 10_000_000.0;
/// History given to newly registered assets: 30 days of 20-second buckets.
const PRICE_BACKFILL_BUCKETS: i64 = 30 * 24 * 180;

static MARKET_MIGRATOR: Migrator = sqlx::migrate!("./migrations_market");

/// Applies the `market` schema migrations on a dedicated connection whose
/// search path points at `market`, so their history table never collides with
/// the public migrator used by older binaries.
pub async fn run_market_migrations(database_url: &str) -> LedgerResult<()> {
    let mut connection = PgConnection::connect(database_url)
        .await
        .map_err(db_error)?;
    sqlx::query("create schema if not exists market")
        .execute(&mut connection)
        .await
        .map_err(db_error)?;
    sqlx::query("set search_path to market")
        .execute(&mut connection)
        .await
        .map_err(db_error)?;
    MARKET_MIGRATOR
        .run(&mut connection)
        .await
        .map_err(|error| LedgerError::Persistence(format!("market migration failed: {error}")))?;
    connection.close().await.map_err(db_error)?;
    Ok(())
}

#[derive(Clone, Debug)]
pub struct MarketProjectorConfig {
    pub chain_id: u64,
    /// Asset whose USD price is used for the native (zero-address) quote asset.
    /// When unset it is resolved from the `l2_weth` protocol deployment.
    pub wrapped_native_asset: Option<String>,
    pub price_bucket_batch: i64,
    pub price_bucket_overlap: i64,
    pub min_price_confidence: f32,
    pub max_quote_price_age_hours: i64,
    pub candle_block_batch: i64,
    pub candle_reprocess_blocks: i64,
    pub candle_minute_lookback_blocks: i64,
    pub stats_candle_scan_limit: i64,
    pub stats_batch_size: usize,
    pub stats_decay_seconds: i64,
    pub supply_fetch_limit: i64,
    pub supply_fetch_timeout_ms: u64,
}

impl Default for MarketProjectorConfig {
    fn default() -> Self {
        Self {
            chain_id: 4663,
            wrapped_native_asset: None,
            price_bucket_batch: 2_000,
            price_bucket_overlap: 240,
            min_price_confidence: 0.5,
            max_quote_price_age_hours: 24,
            candle_block_batch: 100_000,
            candle_reprocess_blocks: 1_500,
            candle_minute_lookback_blocks: 6_000,
            stats_candle_scan_limit: 60_000,
            stats_batch_size: 2_000,
            stats_decay_seconds: 60,
            supply_fetch_limit: 200,
            supply_fetch_timeout_ms: 5_000,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct MarketProjectionSummary {
    pub price_buckets_scanned: i64,
    pub price_rows_upserted: u64,
    pub price_cursor: i64,
    pub candle_from_block: i64,
    pub candle_to_block: i64,
    pub candle_head_block: i64,
    pub candles_upserted: i64,
    pub candles_deleted: i64,
    pub launches_touched: i64,
    pub stats_refreshed: u64,
    pub supplies_fetched: u64,
    pub candle_ms: u64,
    pub stats_ms: u64,
    pub stats_backlog: bool,
}

impl MarketProjectionSummary {
    /// True when any stage still has a backlog and the loop should not sleep.
    pub fn has_backlog(&self) -> bool {
        self.candle_to_block < self.candle_head_block
            || self.price_buckets_scanned > 0
            || self.stats_backlog
    }
}

#[derive(Clone)]
pub struct MarketProjector {
    pool: PgPool,
    rpc: Option<IndexerRpcClient>,
    config: MarketProjectorConfig,
    quote_assets: Arc<Mutex<Option<(Instant, Vec<String>)>>>,
}

struct Checkpoint {
    position: i64,
}

impl MarketProjector {
    pub fn new(pool: PgPool, rpc: Option<IndexerRpcClient>, config: MarketProjectorConfig) -> Self {
        Self {
            pool,
            rpc,
            config,
            quote_assets: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn project_once(&self) -> LedgerResult<MarketProjectionSummary> {
        let mut summary = MarketProjectionSummary::default();
        self.project_prices(&mut summary).await?;
        if summary.price_buckets_scanned > 0 {
            // Candles snapshot quote prices; let the price series catch up first.
            return Ok(summary);
        }
        let started = Instant::now();
        self.project_candles(&mut summary).await?;
        summary.candle_ms = started.elapsed().as_millis() as u64;
        if summary.candle_to_block < summary.candle_head_block {
            // Stats are recomputed from complete candle history; refreshing
            // them while candles are still backfilling repeats the same work.
            return Ok(summary);
        }
        let started = Instant::now();
        self.project_stats(&mut summary).await?;
        summary.stats_ms = started.elapsed().as_millis() as u64;
        summary.supplies_fetched = self.fetch_total_supplies().await?;
        Ok(summary)
    }

    /// Fills candles whose quote price was unknown at projection time. Runs on
    /// a slower cadence because it scans recent candles.
    pub async fn repair_candle_prices(&self) -> LedgerResult<u64> {
        let result = sqlx::query(
            r#"update market.token_candles_1m candle
               set quote_usd_price = priced.price_usd, updated_at = clock_timestamp()
               from (
                 select candle.launch_id, candle.bucket_start, price.price_usd
                 from market.token_candles_1m candle
                 join public.protocol_token_launches launch on launch.launch_id = candle.launch_id
                 cross join lateral (
                   select p.price_usd
                   from market.asset_usd_prices p
                   where p.chain_id = candle.chain_id
                     and p.asset_address = launch.quote_token_address
                     and p.bucket_start <= candle.bucket_start
                     and p.bucket_start > candle.bucket_start - make_interval(hours => $2::int)
                   order by p.bucket_start desc
                   limit 1
                 ) price
                 where candle.chain_id = $1
                   and candle.quote_usd_price is null
                   and candle.bucket_start > now() - interval '3 days'
               ) priced
               where candle.launch_id = priced.launch_id
                 and candle.bucket_start = priced.bucket_start"#,
        )
        .bind(self.config.chain_id as i64)
        .bind(self.config.max_quote_price_age_hours as i32)
        .execute(&self.pool)
        .await
        .map_err(db_error)?;
        Ok(result.rows_affected())
    }

    async fn project_prices(&self, summary: &mut MarketProjectionSummary) -> LedgerResult<()> {
        let key = format!("usd_prices:{}", self.config.chain_id);
        let wrapped_native = self.wrapped_native_asset().await?;
        let checkpoint = self.load_checkpoint(&key).await?;
        let assets = self.quote_assets(wrapped_native.as_deref()).await?;
        if assets.is_empty() {
            return Ok(());
        }
        // The wrapped native asset is the densest price series; its newest
        // bucket bounds the scan without an expensive multi-asset max().
        let Some(reference_asset) = wrapped_native.clone() else {
            return Ok(());
        };
        let head = sqlx::query(
            r#"select block_bucket, block_number from public.price_observations
               where chain_id = $1 and token_address = $2 and quote_symbol = 'USD'
                 and block_number is not null
               order by block_bucket desc
               limit 1"#,
        )
        .bind(self.config.chain_id as i64)
        .bind(&reference_asset)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_error)?;
        let Some(head) = head else {
            return Ok(());
        };
        let head_bucket: i64 = head.get("block_bucket");
        let (initial, start) = match checkpoint {
            Some(checkpoint) => (
                false,
                (checkpoint.position + 1 - self.config.price_bucket_overlap).max(0),
            ),
            None => {
                // Start one day of buckets before the first market fact, using
                // the observed blocks-per-bucket ratio of the newest observation.
                let head_block: i64 = head.get("block_number");
                let first_fact_block: Option<i64> = sqlx::query_scalar(
                    "select min(block_number) from public.protocol_launch_trade_facts where chain_id = $1",
                )
                .bind(self.config.chain_id as i64)
                .fetch_one(&self.pool)
                .await
                .map_err(db_error)?;
                let blocks_per_bucket = (head_block / head_bucket.max(1)).max(1);
                let start = first_fact_block
                    .map(|block| block / blocks_per_bucket - 86_400 / 20)
                    .unwrap_or(head_bucket)
                    .max(0);
                (true, start)
            }
        };
        let end = (start + self.config.price_bucket_batch).min(head_bucket);
        if end < start {
            return Ok(());
        }
        let result = self
            .rollup_prices(&assets, start, end, initial, wrapped_native.as_deref())
            .await?;
        summary.price_rows_upserted = result;
        summary.price_buckets_scanned = if end < head_bucket {
            end - start + 1
        } else {
            0
        };
        summary.price_cursor = end;
        self.save_checkpoint(&key, end).await
    }

    async fn project_candles(&self, summary: &mut MarketProjectionSummary) -> LedgerResult<()> {
        let key = format!("candles:{}", self.config.chain_id);
        let chain_id = self.config.chain_id as i64;
        let head: Option<i64> = sqlx::query_scalar(
            "select max(block_number) from public.protocol_launch_trade_facts where chain_id = $1",
        )
        .bind(chain_id)
        .fetch_one(&self.pool)
        .await
        .map_err(db_error)?;
        let Some(head) = head else {
            return Ok(());
        };
        summary.candle_head_block = head;
        let cursor = match self.load_checkpoint(&key).await? {
            Some(checkpoint) => checkpoint.position,
            None => {
                let first: i64 = sqlx::query_scalar(
                    "select min(block_number) from public.protocol_launch_trade_facts where chain_id = $1",
                )
                .bind(chain_id)
                .fetch_one(&self.pool)
                .await
                .map_err(db_error)?;
                first - 1
            }
        };
        // Graduated pools feed the same candles; never pass their swap cursor.
        let pool_cursor = self
            .load_checkpoint(&format!("pool_swaps:{}", self.config.chain_id))
            .await?;
        let head = match pool_cursor {
            Some(pool_cursor) => head.min(pool_cursor.position),
            None => head,
        };
        summary.candle_head_block = head;
        let end = (cursor + self.config.candle_block_batch).min(head);
        let start = if end >= head {
            // Caught up: re-derive a trailing window so late canonical flips and
            // partially filled minutes converge.
            (cursor + 1 - self.config.candle_reprocess_blocks).max(0)
        } else {
            cursor + 1
        };
        summary.candle_from_block = start;
        summary.candle_to_block = end;
        if end < start {
            return Ok(());
        }

        let row = sqlx::query(CANDLE_PROJECTION_SQL)
            .bind(chain_id)
            .bind(start)
            .bind(end)
            .bind(self.config.candle_minute_lookback_blocks)
            .bind(self.config.max_quote_price_age_hours as i32)
            .bind(MARKET_METHODOLOGY_VERSION)
            .bind(NATIVE_ASSET_ADDRESS)
            .fetch_one(&self.pool)
            .await
            .map_err(db_error)?;
        summary.candles_upserted = row.get("upserted");
        summary.candles_deleted = row.get("deleted");
        summary.launches_touched = row.get("touched");
        self.save_checkpoint(&key, end.max(cursor)).await
    }

    async fn project_stats(&self, summary: &mut MarketProjectionSummary) -> LedgerResult<()> {
        let key = format!("stats:{}", self.config.chain_id);
        let since_micros = self
            .load_checkpoint(&key)
            .await?
            .map(|checkpoint| checkpoint.position)
            .unwrap_or(0);
        let since = Utc
            .timestamp_micros(since_micros)
            .single()
            .unwrap_or_else(|| Utc.timestamp_opt(0, 0).unwrap());

        let changed = sqlx::query(
            r#"select launch_id, updated_at from market.token_candles_1m
               where chain_id = $1 and updated_at > $2
               order by updated_at
               limit $3"#,
        )
        .bind(self.config.chain_id as i64)
        .bind(since)
        .bind(self.config.stats_candle_scan_limit)
        .fetch_all(&self.pool)
        .await
        .map_err(db_error)?;
        summary.stats_backlog = changed.len() as i64 >= self.config.stats_candle_scan_limit;
        let mut next_since = since;
        let mut launches = BTreeSet::new();
        for row in &changed {
            launches.insert(row.get::<Uuid, _>("launch_id"));
            next_since = row.get::<DateTime<Utc>, _>("updated_at");
        }

        let decaying = sqlx::query_scalar::<_, Uuid>(
            r#"select launch_id from market.token_stats
               where chain_id = $1 and trades_24h > 0
                 and refreshed_at < now() - make_interval(secs => $2::double precision)
               order by refreshed_at
               limit $3"#,
        )
        .bind(self.config.chain_id as i64)
        .bind(self.config.stats_decay_seconds as f64)
        .bind(self.config.stats_batch_size as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(db_error)?;
        launches.extend(decaying);

        if launches.is_empty() {
            return Ok(());
        }
        let block_24h = self.block_at_or_before_24h().await?;
        let wrapped_native = self.wrapped_native_asset().await?;
        let launches = launches.into_iter().collect::<Vec<_>>();
        for chunk in launches.chunks(self.config.stats_batch_size.max(1)) {
            let result = sqlx::query(STATS_PROJECTION_SQL)
                .bind(self.config.chain_id as i64)
                .bind(chunk)
                .bind(block_24h)
                .bind(self.config.max_quote_price_age_hours as i32)
                .bind(MARKET_METHODOLOGY_VERSION)
                .bind(NATIVE_ASSET_ADDRESS)
                .bind(wrapped_native.as_deref().unwrap_or(NATIVE_ASSET_ADDRESS))
                .execute(&self.pool)
                .await
                .map_err(db_error)?;
            summary.stats_refreshed += result.rows_affected();
        }
        if next_since > since {
            self.save_checkpoint(&key, next_since.timestamp_micros())
                .await?;
        }
        Ok(())
    }

    async fn fetch_total_supplies(&self) -> LedgerResult<u64> {
        let Some(rpc) = self.rpc.clone() else {
            return Ok(0);
        };
        let rows = sqlx::query(
            r#"select stats.launch_id, stats.token_address, meta.decimals
               from market.token_stats stats
               left join public.erc20_token_metadata meta
                 on meta.chain_id = stats.chain_id and meta.token_address = stats.token_address
                and meta.fetch_status = 'ok'
               where stats.chain_id = $1 and stats.total_supply_status = 'pending'
               order by stats.hot_score desc, stats.last_trade_at desc nulls last
               limit $2"#,
        )
        .bind(self.config.chain_id as i64)
        .bind(self.config.supply_fetch_limit)
        .fetch_all(&self.pool)
        .await
        .map_err(db_error)?;
        let targets = rows
            .iter()
            .map(|row| {
                (
                    row.get::<Uuid, _>("launch_id"),
                    row.get::<String, _>("token_address"),
                    row.get::<Option<i32>, _>("decimals"),
                )
            })
            .collect::<Vec<_>>();
        let calls = targets
            .iter()
            .map(|(_, token, _)| (token.clone(), TOTAL_SUPPLY_SELECTOR.to_owned()))
            .collect::<Vec<_>>();
        let results = match tokio::time::timeout(
            Duration::from_millis(self.config.supply_fetch_timeout_ms.max(100) * 4),
            rpc.eth_call_batch(&calls),
        )
        .await
        {
            Ok(Ok(results)) => results,
            // Transient RPC trouble: leave supplies pending for the next pass.
            Ok(Err(_)) | Err(_) => return Ok(0),
        };
        let mut fetched = 0;
        for ((launch_id, _, decimals), result) in targets.into_iter().zip(results) {
            let supply = match (result, decimals) {
                (Ok(hex), Some(decimals)) => scaled_uint_from_hex(&hex, decimals.max(0) as u32),
                _ => None,
            };
            let status = if supply.is_some() { "ready" } else { "error" };
            fetched += u64::from(supply.is_some());
            sqlx::query(
                r#"update market.token_stats set
                     total_supply = $2::numeric,
                     total_supply_status = $3,
                     market_cap_usd = case when $2::numeric is not null
                       then price_usd * $2::numeric else market_cap_usd end
                   where launch_id = $1"#,
            )
            .bind(launch_id)
            .bind(supply)
            .bind(status)
            .execute(&self.pool)
            .await
            .map_err(db_error)?;
        }
        Ok(fetched)
    }

    async fn block_at_or_before_24h(&self) -> LedgerResult<i64> {
        let block: Option<i64> = sqlx::query_scalar(
            r#"select min(first_block) from market.token_candles_1m
               where chain_id = $1
                 and bucket_start >= now() - interval '24 hours'
                 and bucket_start < now() - interval '23 hours 50 minutes'"#,
        )
        .bind(self.config.chain_id as i64)
        .fetch_one(&self.pool)
        .await
        .map_err(db_error)?;
        Ok(block.unwrap_or(0))
    }

    async fn wrapped_native_asset(&self) -> LedgerResult<Option<String>> {
        if let Some(asset) = &self.config.wrapped_native_asset {
            return Ok(Some(asset.to_ascii_lowercase()));
        }
        sqlx::query_scalar(
            r#"select address from public.protocol_deployments
               where chain_id = $1 and contract_role = 'l2_weth' and status = 'active'
               order by confidence desc, address
               limit 1"#,
        )
        .bind(self.config.chain_id as i64)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_error)
    }

    async fn rollup_prices(
        &self,
        assets: &[String],
        start: i64,
        end: i64,
        initial: bool,
        wrapped_native: Option<&str>,
    ) -> LedgerResult<u64> {
        let result = sqlx::query(PRICE_ROLLUP_SQL)
            .bind(self.config.chain_id as i64)
            .bind(assets)
            .bind(start)
            .bind(end)
            .bind(self.config.min_price_confidence)
            .bind(NATIVE_ASSET_ADDRESS)
            .bind(wrapped_native.unwrap_or(NATIVE_ASSET_ADDRESS))
            .bind(initial)
            .bind(PRICE_OUTLIER_FACTOR)
            .bind(MAX_QUOTE_ASSET_USD)
            .execute(&self.pool)
            .await
            .map_err(db_error)?;
        Ok(result.rows_affected())
    }

    /// Gives newly registered assets (for example tokens first seen in an
    /// arbitrage delta) a bounded price history before they join the
    /// incremental rollup.
    pub async fn backfill_registered_assets(&self) -> LedgerResult<u64> {
        let pending: Vec<String> = sqlx::query_scalar(
            r#"select asset_address from market.price_assets
               where chain_id = $1 and not backfilled
               order by registered_at
               limit 25"#,
        )
        .bind(self.config.chain_id as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(db_error)?;
        if pending.is_empty() {
            return Ok(0);
        }
        let Some(head) = self
            .load_checkpoint(&format!("usd_prices:{}", self.config.chain_id))
            .await?
        else {
            return Ok(0);
        };
        let wrapped_native = self.wrapped_native_asset().await?;
        let rows = self
            .rollup_prices(
                &pending,
                (head.position - PRICE_BACKFILL_BUCKETS).max(0),
                head.position,
                true,
                wrapped_native.as_deref(),
            )
            .await?;
        sqlx::query(
            r#"update market.price_assets set backfilled = true, backfilled_at = now()
               where chain_id = $1 and asset_address = any($2)"#,
        )
        .bind(self.config.chain_id as i64)
        .bind(&pending)
        .execute(&self.pool)
        .await
        .map_err(db_error)?;
        // The quote-asset cache must include the new assets on the next pass.
        *self.quote_assets.lock().await = None;
        Ok(rows)
    }

    /// Every quote asset used by any launch plus the wrapped native asset.
    /// Cached because the source scan covers the full launch table.
    async fn quote_assets(&self, wrapped_native: Option<&str>) -> LedgerResult<Vec<String>> {
        let mut cache = self.quote_assets.lock().await;
        if let Some((loaded_at, assets)) = cache.as_ref()
            && loaded_at.elapsed() < QUOTE_ASSET_CACHE_TTL
        {
            return Ok(assets.clone());
        }
        // Launch quote assets are part of the main rollup from its first batch.
        sqlx::query(
            r#"insert into market.price_assets (chain_id, asset_address, source, backfilled, backfilled_at)
               select $1, quote_token_address, 'launch_quote', true, now()
               from public.protocol_token_launches
               where chain_id = $1 and quote_token_address is not null and quote_token_address <> $2
               group by quote_token_address
               on conflict (chain_id, asset_address) do nothing"#,
        )
        .bind(self.config.chain_id as i64)
        .bind(NATIVE_ASSET_ADDRESS)
        .execute(&self.pool)
        .await
        .map_err(db_error)?;
        let mut assets: Vec<String> = sqlx::query_scalar(
            "select asset_address from market.price_assets where chain_id = $1 and backfilled",
        )
        .bind(self.config.chain_id as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(db_error)?;
        if let Some(wrapped) = wrapped_native {
            assets.push(wrapped.to_owned());
        }
        assets.retain(|asset| asset != NATIVE_ASSET_ADDRESS);
        assets.sort();
        assets.dedup();
        *cache = Some((Instant::now(), assets.clone()));
        Ok(assets)
    }

    async fn load_checkpoint(&self, key: &str) -> LedgerResult<Option<Checkpoint>> {
        let position: Option<i64> = sqlx::query_scalar(
            "select last_position from market.projector_checkpoints where checkpoint_key = $1",
        )
        .bind(key)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_error)?;
        Ok(position.map(|position| Checkpoint { position }))
    }

    async fn save_checkpoint(&self, key: &str, position: i64) -> LedgerResult<()> {
        sqlx::query(
            r#"insert into market.projector_checkpoints (
                 checkpoint_key, last_position, last_run_at, last_success_at, last_error, updated_at
               ) values ($1, $2, now(), now(), null, now())
               on conflict (checkpoint_key) do update set
                 last_position = excluded.last_position, last_run_at = now(),
                 last_success_at = now(), last_error = null, updated_at = now()"#,
        )
        .bind(key)
        .bind(position)
        .execute(&self.pool)
        .await
        .map_err(db_error)?;
        Ok(())
    }

    pub async fn record_error(&self, error: &str) -> LedgerResult<()> {
        sqlx::query(
            r#"insert into market.projector_checkpoints (checkpoint_key, last_run_at, last_error)
               values ($1, now(), $2)
               on conflict (checkpoint_key) do update set
                 last_run_at = now(), last_error = excluded.last_error, updated_at = now()"#,
        )
        .bind(format!("errors:{}", self.config.chain_id))
        .bind(error.chars().take(1_000).collect::<String>())
        .execute(&self.pool)
        .await
        .map_err(db_error)?;
        Ok(())
    }
}

/// Hourly outlier-filtered USD prices for `$2` over price buckets `$3..=$4`.
const PRICE_ROLLUP_SQL: &str = r#"with observations as materialized (
                 select token_address as asset_address,
                        date_trunc('hour', block_timestamp) as bucket_start,
                        price_decimal, confidence, source
                 from public.price_observations
                 where chain_id = $1 and quote_symbol = 'USD' and token_address = any($2)
                   and block_bucket between $3 and $4
                   and block_timestamp is not null and price_decimal > 0 and confidence >= $5
                 union all
                 -- The native asset is priced by the wrapped native asset's observations.
                 select $6::text, date_trunc('hour', block_timestamp), price_decimal, confidence,
                        source
                 from public.price_observations
                 where chain_id = $1 and quote_symbol = 'USD' and token_address = $7
                   and $7 <> $6
                   and block_bucket between $3 and $4
                   and block_timestamp is not null and price_decimal > 0 and confidence >= $5
               ), references_ as materialized (
                 -- Robust anchor per asset, in order of preference: the median of the
                 -- asset's stored hourly prices over the previous 7 days, the batch
                 -- median when the batch is well observed, otherwise the batch median
                 -- of pool snapshots only. Swap-derived observations are noisy (odd
                 -- routes, dust), so anything beyond the outlier factor is dropped.
                 select batch.asset_address,
                        coalesce(history.median_price,
                          case when batch.observation_count >= 5 then batch.median_price end,
                          batch.snapshot_median) as anchor
                 from (
                   select asset_address, min(bucket_start) as first_bucket,
                          count(*) as observation_count,
                          percentile_disc(0.5) within group (order by price_decimal) as median_price,
                          percentile_disc(0.5) within group (order by price_decimal)
                            filter (where source = 'dex_pool_snapshot') as snapshot_median
                   from observations group by asset_address
                 ) batch
                 left join lateral (
                   select percentile_disc(0.5) within group (order by price.price_usd) as median_price
                   from market.asset_usd_prices price
                   where price.chain_id = $1 and price.asset_address = batch.asset_address
                     and price.bucket_start < batch.first_bucket
                     and price.bucket_start > batch.first_bucket - interval '7 days'
                   having count(*) >= 6
                 ) history on true
               ), accepted as (
                 select observations.*
                 from observations
                 join references_ using (asset_address)
                 where observations.price_decimal between references_.anchor / $9
                                                      and references_.anchor * $9
                   and observations.price_decimal <= $10
               ), scoped as (
                 select * from accepted
                 where $8 or bucket_start > (select min(bucket_start) from observations)
               ), minutes as (
                 -- Pool-state snapshots are exact; prefer them over swap-derived prices.
                 select asset_address, bucket_start,
                        coalesce(
                          percentile_disc(0.5) within group (order by price_decimal)
                            filter (where source = 'dex_pool_snapshot'),
                          percentile_disc(0.5) within group (order by price_decimal)
                        ) as price_usd,
                        count(*) as observation_count, min(confidence) as min_confidence,
                        case when bool_or(source = 'dex_pool_snapshot')
                          then 'pool_snapshot_median' else 'swap_median_banded' end as source
                 from scoped
                 group by asset_address, bucket_start
                 -- A single swap-derived observation is not an hourly price.
                 having bool_or(source = 'dex_pool_snapshot') or count(*) >= 2
               )
               insert into market.asset_usd_prices (
                 chain_id, asset_address, bucket_start, price_usd, observation_count,
                 min_confidence, source, updated_at
               )
               select $1, asset_address, bucket_start, price_usd, observation_count,
                      min_confidence, source, now()
               from minutes
               on conflict (chain_id, asset_address, bucket_start) do update set
                 price_usd = excluded.price_usd,
                 observation_count = excluded.observation_count,
                 min_confidence = excluded.min_confidence,
                 source = excluded.source,
                 updated_at = excluded.updated_at"#;

/// Recomputes every minute bucket touched by trade facts in blocks `$2..=$3`.
///
/// Each touched launch is re-aggregated from the start of the first touched
/// minute (looking back at most `$4` blocks) so a bucket split across batches is
/// always rebuilt from all of its facts. Buckets whose facts all became
/// non-canonical are deleted.
const CANDLE_PROJECTION_SQL: &str = r#"
with touched_facts as (
  select launch_id, occurred_at
  from public.protocol_launch_trade_facts
  where chain_id = $1 and block_number between $2 and $3 and occurred_at is not null
  union all
  select launch_id, occurred_at
  from market.pool_swap_facts
  where chain_id = $1 and block_number between $2 and $3 and occurred_at is not null
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
           case when launch.quote_token_address = $7 then 18 end) as quote_decimals,
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
         fact.fee_amount_raw::numeric * power(10::numeric, -(meta.quote_decimals)) as fee_amount,
         fact.tax_amount_raw::numeric * power(10::numeric, -(meta.quote_decimals)) as tax_amount,
         true as is_curve
  from launch_meta meta
  join public.protocol_launch_trade_facts fact
    on fact.launch_id = meta.launch_id and fact.is_canonical
   and fact.block_number between $2 - $4 and $3
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
         0, 0, false
  from launch_meta meta
  join market.pool_swap_facts swap
    on swap.launch_id = meta.launch_id
   and swap.block_number between $2 - $4 and $3
   and swap.occurred_at >= meta.first_bucket
   and swap.occurred_at < meta.last_bucket + interval '1 minute'
  where meta.token_decimals is not null and meta.quote_decimals is not null
    and meta.pool_quote_currency = meta.quote_token_address
), priced as (
  select facts.*,
         case when token_amount > 0 then quote_amount / token_amount end as price
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
  select aggregated.launch_id, aggregated.bucket_start, $1, aggregated.domain_id,
         open_price, high_price, low_price, close_price,
         (select price.price_usd from market.asset_usd_prices price
          where price.chain_id = $1 and price.asset_address = aggregated.quote_token_address
            and price.bucket_start <= aggregated.bucket_start
            and price.bucket_start > aggregated.bucket_start - make_interval(hours => $5::int)
          order by price.bucket_start desc limit 1),
         buy_quote, sell_quote, buy_token, sell_token, fee_quote, reserve_delta,
         trade_count, buy_count, sell_count, unique_traders, first_block, last_block,
         $6, clock_timestamp()
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
"#;

/// Rebuilds current statistics for the launches in `$2` from their candles.
///
/// Window changes compare the latest close with the last close before the
/// window start (or the first open for launches younger than the window), in
/// quote-asset units. USD values use each candle's quote price, falling back to
/// the quote asset's latest price. Supply fields are preserved.
const STATS_PROJECTION_SQL: &str = r#"
with base as materialized (
  select launch.launch_id, launch.chain_id, launch.domain_id, launch.token_address,
         launch.symbol, launch.name, launch.creator_address, launch.quote_token_address,
         quote_meta.symbol as quote_symbol, launch.protocol_version, launch.lifecycle_phase,
         launch.launched_at, launch.graduated_at,
         launch.graduation_threshold_raw::numeric /
           power(10::numeric, quote_decimals.value) as graduation_threshold,
         existing.total_supply,
         case
           when launch.quote_token_address in ($6, $7) then 'reliable'
           when exists (
             select 1 from public.address_label_assertions assertion
             join public.intelligence_sources source
               on source.source_key = assertion.source_key and source.status = 'active'
             where assertion.chain_id = launch.chain_id
               and assertion.address = launch.quote_token_address
               and assertion.label_key = 'stablecoin' and assertion.status = 'active'
               and assertion.assertion_value
               and (assertion.expires_at is null or assertion.expires_at > now())
           ) then 'reliable'
           else 'derived'
         end as price_grade
  from unnest($2::uuid[]) as target(launch_id)
  join public.protocol_token_launches launch on launch.launch_id = target.launch_id
  left join public.erc20_token_metadata quote_meta
    on quote_meta.chain_id = launch.chain_id
   and quote_meta.token_address = launch.quote_token_address
   and quote_meta.fetch_status = 'ok'
  left join market.token_stats existing on existing.launch_id = launch.launch_id
  cross join lateral (
    select coalesce(quote_meta.decimals,
             case when launch.quote_token_address = $6 then 18 end) as value
  ) quote_decimals
  where launch.chain_id = $1
), quote_price as materialized (
  select base.launch_id, price.price_usd, price.bucket_start as priced_at
  from base
  left join lateral (
    select p.price_usd, p.bucket_start
    from market.asset_usd_prices p
    where p.chain_id = $1 and p.asset_address = base.quote_token_address
      and p.bucket_start > now() - make_interval(hours => $4::int)
    order by p.bucket_start desc
    limit 1
  ) price on true
), latest as materialized (
  select base.launch_id, candle.close_price_quote, candle.bucket_start
  from base
  left join lateral (
    select close_price_quote, bucket_start from market.token_candles_1m c
    where c.launch_id = base.launch_id and c.close_price_quote is not null
    order by bucket_start desc limit 1
  ) candle on true
), lifetime as materialized (
  select c.launch_id,
         sum(c.curve_reserve_delta_quote) as reserve,
         max(c.high_price_quote) as ath_price,
         min(c.bucket_start) as first_trade_at,
         max(c.bucket_start) as last_trade_at,
         (array_agg(c.open_price_quote order by c.bucket_start)
            filter (where c.open_price_quote is not null))[1] as first_open
  from market.token_candles_1m c
  join base on base.launch_id = c.launch_id
  group by c.launch_id
), windows as materialized (
  select c.launch_id,
         sum((c.buy_quote + c.sell_quote) * coalesce(c.quote_usd_price, qp.price_usd))
           filter (where c.bucket_start >= now() - interval '5 minutes') as volume_5m,
         sum((c.buy_quote + c.sell_quote) * coalesce(c.quote_usd_price, qp.price_usd))
           filter (where c.bucket_start >= now() - interval '1 hour') as volume_1h,
         sum((c.buy_quote + c.sell_quote) * coalesce(c.quote_usd_price, qp.price_usd))
           filter (where c.bucket_start >= now() - interval '6 hours') as volume_6h,
         sum((c.buy_quote + c.sell_quote) * coalesce(c.quote_usd_price, qp.price_usd))
           as volume_24h,
         sum(c.buy_quote * coalesce(c.quote_usd_price, qp.price_usd)) as buy_24h,
         sum(c.sell_quote * coalesce(c.quote_usd_price, qp.price_usd)) as sell_24h,
         sum((c.buy_quote - c.sell_quote) * coalesce(c.quote_usd_price, qp.price_usd))
           filter (where c.bucket_start >= now() - interval '1 hour') as net_1h,
         sum((c.buy_quote - c.sell_quote) * coalesce(c.quote_usd_price, qp.price_usd))
           as net_24h,
         sum(c.trade_count) filter (where c.bucket_start >= now() - interval '5 minutes') as trades_5m,
         sum(c.trade_count) filter (where c.bucket_start >= now() - interval '1 hour') as trades_1h,
         sum(c.trade_count) as trades_24h,
         sum(c.buy_count) as buys_24h,
         sum(c.sell_count) as sells_24h
  from market.token_candles_1m c
  join base on base.launch_id = c.launch_id
  join quote_price qp on qp.launch_id = c.launch_id
  where c.bucket_start >= now() - interval '24 hours'
  group by c.launch_id
), references_ as materialized (
  select base.launch_id,
         (select close_price_quote from market.token_candles_1m c
          where c.launch_id = base.launch_id and c.bucket_start < now() - interval '5 minutes'
            and c.close_price_quote is not null
          order by bucket_start desc limit 1) as ref_5m,
         (select close_price_quote from market.token_candles_1m c
          where c.launch_id = base.launch_id and c.bucket_start < now() - interval '1 hour'
            and c.close_price_quote is not null
          order by bucket_start desc limit 1) as ref_1h,
         (select close_price_quote from market.token_candles_1m c
          where c.launch_id = base.launch_id and c.bucket_start < now() - interval '6 hours'
            and c.close_price_quote is not null
          order by bucket_start desc limit 1) as ref_6h,
         (select close_price_quote from market.token_candles_1m c
          where c.launch_id = base.launch_id and c.bucket_start < now() - interval '24 hours'
            and c.close_price_quote is not null
          order by bucket_start desc limit 1) as ref_24h
  from base
), traders as materialized (
  select fact.launch_id,
         count(distinct fact.trader_address) filter (where fact.trade_side = 'buy') as buyers,
         count(distinct fact.trader_address) filter (where fact.trade_side = 'sell') as sellers
  from base
  join public.protocol_launch_trade_facts fact
    on fact.launch_id = base.launch_id and fact.is_canonical
   and fact.block_number >= $3
   and fact.occurred_at >= now() - interval '24 hours'
  group by fact.launch_id
), sparkline as materialized (
  select base.launch_id,
         array_agg(point.close_price_quote order by hours.offset_hours desc) as points
  from base
  cross join generate_series(0, 23) as hours(offset_hours)
  left join lateral (
    select close_price_quote from market.token_candles_1m c
    where c.launch_id = base.launch_id
      and c.bucket_start < now() - make_interval(hours => hours.offset_hours)
      and c.close_price_quote is not null
    order by bucket_start desc limit 1
  ) point on true
  group by base.launch_id
), computed as (
  select base.*,
         latest.close_price_quote as price_quote,
         qp.price_usd as quote_usd_price,
         latest.close_price_quote * qp.price_usd as price_usd,
         case
           when qp.priced_at is null then 'unpriced'
           when qp.priced_at >= now() - interval '1 hour' then 'priced'
           else 'stale'
         end as usd_status,
         lifetime.reserve, lifetime.ath_price, lifetime.first_trade_at, lifetime.last_trade_at,
         coalesce(windows.volume_5m, 0) as volume_5m,
         coalesce(windows.volume_1h, 0) as volume_1h,
         coalesce(windows.volume_6h, 0) as volume_6h,
         coalesce(windows.volume_24h, 0) as volume_24h,
         coalesce(windows.buy_24h, 0) as buy_24h,
         coalesce(windows.sell_24h, 0) as sell_24h,
         coalesce(windows.net_1h, 0) as net_1h,
         coalesce(windows.net_24h, 0) as net_24h,
         coalesce(windows.trades_5m, 0)::int as trades_5m,
         coalesce(windows.trades_1h, 0)::int as trades_1h,
         coalesce(windows.trades_24h, 0)::int as trades_24h,
         coalesce(windows.buys_24h, 0)::int as buys_24h,
         coalesce(windows.sells_24h, 0)::int as sells_24h,
         traders.buyers, traders.sellers,
         case when coalesce(refs.ref_5m, lifetime.first_open) > 0
           then (latest.close_price_quote / coalesce(refs.ref_5m, lifetime.first_open) - 1) * 100 end
           as change_5m,
         case when coalesce(refs.ref_1h, lifetime.first_open) > 0
           then (latest.close_price_quote / coalesce(refs.ref_1h, lifetime.first_open) - 1) * 100 end
           as change_1h,
         case when coalesce(refs.ref_6h, lifetime.first_open) > 0
           then (latest.close_price_quote / coalesce(refs.ref_6h, lifetime.first_open) - 1) * 100 end
           as change_6h,
         case when coalesce(refs.ref_24h, lifetime.first_open) > 0
           then (latest.close_price_quote / coalesce(refs.ref_24h, lifetime.first_open) - 1) * 100 end
           as change_24h,
         sparkline.points
  from base
  join quote_price qp on qp.launch_id = base.launch_id
  join latest on latest.launch_id = base.launch_id
  left join lifetime on lifetime.launch_id = base.launch_id
  left join windows on windows.launch_id = base.launch_id
  left join references_ refs on refs.launch_id = base.launch_id
  left join traders on traders.launch_id = base.launch_id
  left join sparkline on sparkline.launch_id = base.launch_id
)
insert into market.token_stats (
  launch_id, chain_id, domain_id, token_address, symbol, name, creator_address,
  quote_token_address, quote_symbol, protocol_version, lifecycle_phase, launched_at, graduated_at,
  price_quote, quote_usd_price, price_usd, market_cap_usd,
  change_5m_pct, change_1h_pct, change_6h_pct, change_24h_pct,
  volume_5m_usd, volume_1h_usd, volume_6h_usd, volume_24h_usd,
  buy_volume_24h_usd, sell_volume_24h_usd, net_flow_1h_usd, net_flow_24h_usd,
  trades_5m, trades_1h, trades_24h, buys_24h, sells_24h, buyers_24h, sellers_24h,
  curve_reserve_quote, graduation_threshold_quote, bonding_progress_pct,
  ath_price_quote, ath_market_cap_usd, sparkline_24h, first_trade_at, last_trade_at,
  hot_score, usd_status, price_grade, methodology_version, refreshed_at
)
select launch_id, chain_id, domain_id, token_address, symbol, name, creator_address,
       quote_token_address, quote_symbol, protocol_version, lifecycle_phase, launched_at, graduated_at,
       price_quote, quote_usd_price, price_usd, price_usd * total_supply,
       change_5m, change_1h, change_6h, change_24h,
       volume_5m, volume_1h, volume_6h, volume_24h,
       buy_24h, sell_24h, net_1h, net_24h,
       trades_5m, trades_1h, trades_24h, buys_24h, sells_24h, buyers, sellers,
       reserve, graduation_threshold,
       case
         when lifecycle_phase <> 'curve_trading' or graduated_at is not null then 100
         when graduation_threshold > 0 and reserve is not null
           then least(greatest(reserve / graduation_threshold * 100, 0), 100)
       end,
       ath_price, ath_price * quote_usd_price * total_supply,
       coalesce(
         (select array_agg(point * quote_usd_price order by ordinality)
          from unnest(points) with ordinality as p(point, ordinality)),
         array[]::numeric[]
       ),
       first_trade_at, last_trade_at,
       case when trades_24h = 0 then 0 else
         2.0 * ln(1 + volume_1h::double precision)
         + ln(1 + trades_1h::double precision)
         + 0.5 * ln(1 + volume_24h::double precision)
         + case
             when change_1h is null then 0
             else greatest(least(
               ln(greatest(1 + change_1h::double precision / 100, 0.001)), 5), -3)
           end
       end,
       usd_status, price_grade, $5, now()
from computed
where price_quote is not null
on conflict (launch_id) do update set
  symbol = excluded.symbol, name = excluded.name, creator_address = excluded.creator_address,
  quote_token_address = excluded.quote_token_address, quote_symbol = excluded.quote_symbol,
  protocol_version = excluded.protocol_version, lifecycle_phase = excluded.lifecycle_phase,
  launched_at = excluded.launched_at, graduated_at = excluded.graduated_at,
  price_quote = excluded.price_quote, quote_usd_price = excluded.quote_usd_price,
  price_usd = excluded.price_usd,
  market_cap_usd = excluded.price_usd * market.token_stats.total_supply,
  change_5m_pct = excluded.change_5m_pct, change_1h_pct = excluded.change_1h_pct,
  change_6h_pct = excluded.change_6h_pct, change_24h_pct = excluded.change_24h_pct,
  volume_5m_usd = excluded.volume_5m_usd, volume_1h_usd = excluded.volume_1h_usd,
  volume_6h_usd = excluded.volume_6h_usd, volume_24h_usd = excluded.volume_24h_usd,
  buy_volume_24h_usd = excluded.buy_volume_24h_usd, sell_volume_24h_usd = excluded.sell_volume_24h_usd,
  net_flow_1h_usd = excluded.net_flow_1h_usd, net_flow_24h_usd = excluded.net_flow_24h_usd,
  trades_5m = excluded.trades_5m, trades_1h = excluded.trades_1h, trades_24h = excluded.trades_24h,
  buys_24h = excluded.buys_24h, sells_24h = excluded.sells_24h,
  buyers_24h = excluded.buyers_24h, sellers_24h = excluded.sellers_24h,
  curve_reserve_quote = excluded.curve_reserve_quote,
  graduation_threshold_quote = excluded.graduation_threshold_quote,
  bonding_progress_pct = excluded.bonding_progress_pct,
  ath_price_quote = excluded.ath_price_quote,
  ath_market_cap_usd = excluded.ath_price_quote * excluded.quote_usd_price
    * market.token_stats.total_supply,
  sparkline_24h = excluded.sparkline_24h,
  first_trade_at = excluded.first_trade_at, last_trade_at = excluded.last_trade_at,
  hot_score = excluded.hot_score, usd_status = excluded.usd_status,
  price_grade = excluded.price_grade,
  methodology_version = excluded.methodology_version, refreshed_at = excluded.refreshed_at
"#;

/// Parses an ABI-encoded uint256 and scales it by `decimals`, returning a
/// decimal string suitable for a PostgreSQL numeric bind.
pub fn scaled_uint_from_hex(hex: &str, decimals: u32) -> Option<String> {
    let digits = hex.trim_start_matches("0x");
    if digits.is_empty() || digits.len() > 64 {
        return None;
    }
    let value = BigUint::parse_bytes(digits.as_bytes(), 16)?;
    let raw = value.to_str_radix(10);
    if decimals == 0 {
        return Some(raw);
    }
    let decimals = decimals as usize;
    let padded = format!("{raw:0>width$}", width = decimals + 1);
    let (whole, fraction) = padded.split_at(padded.len() - decimals);
    let fraction = fraction.trim_end_matches('0');
    Some(if fraction.is_empty() {
        whole.to_owned()
    } else {
        format!("{whole}.{fraction}")
    })
}

fn db_error(error: impl std::fmt::Display) -> LedgerError {
    LedgerError::Persistence(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scales_total_supply_by_decimals() {
        let one_billion = "0x0000000000000000000000000000000000000000033b2e3c9fd0803ce8000000";
        assert_eq!(
            scaled_uint_from_hex(one_billion, 18).as_deref(),
            Some("1000000000")
        );
        assert_eq!(scaled_uint_from_hex("0x0f", 1).as_deref(), Some("1.5"));
        assert_eq!(scaled_uint_from_hex("0x01", 3).as_deref(), Some("0.001"));
        assert_eq!(scaled_uint_from_hex("0x2a", 0).as_deref(), Some("42"));
    }

    #[test]
    fn rejects_malformed_uint_words() {
        assert_eq!(scaled_uint_from_hex("0x", 18), None);
        assert_eq!(scaled_uint_from_hex("0xzz", 18), None);
        assert_eq!(
            scaled_uint_from_hex(&format!("0x{}", "1".repeat(65)), 18),
            None
        );
    }

    #[test]
    fn backlog_detection_tracks_both_cursors() {
        let mut summary = MarketProjectionSummary {
            candle_to_block: 10,
            candle_head_block: 10,
            ..MarketProjectionSummary::default()
        };
        assert!(!summary.has_backlog());
        summary.candle_head_block = 11;
        assert!(summary.has_backlog());
        summary.candle_head_block = 10;
        summary.price_buckets_scanned = 5;
        assert!(summary.has_backlog());
    }
}
