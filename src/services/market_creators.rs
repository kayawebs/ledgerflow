//! Creator attribution and track records.
//!
//! Before buying a new launch, the most useful fact available is what the same
//! person's previous launches did. Two things stand in the way: the protocol
//! event reports `msg.sender` as the creator, which is a contract whenever the
//! caller went through a router or multicall, and nothing aggregates a
//! creator's history. This module resolves the transaction sender behind each
//! launch and rebuilds a per-creator track record from it.

use std::time::Duration;

use sqlx::{PgPool, Row};

use crate::{
    core::errors::{LedgerError, LedgerResult},
    indexer::{clickhouse::ClickHouseClient, rpc::IndexerRpcClient},
};

#[derive(Clone, Debug)]
pub struct CreatorIntelligenceConfig {
    pub chain_id: u64,
    /// Launches resolved per pass. Each pass is one ClickHouse query bounded to
    /// the batch's block range.
    pub resolve_batch: i64,
    /// Blocks to step back when resuming, so a launch indexed after the cursor
    /// passed its block is still picked up.
    pub resume_backstep: i64,
    /// Launches per pass that fall back to the node. The transaction store has
    /// gaps in the chain's early history, and those launches would otherwise
    /// never be attributed.
    pub rpc_fallback_batch: i64,
    /// Transactions per JSON-RPC batch on the fallback path.
    pub rpc_chunk: usize,
    pub profile_interval: Duration,
}

impl Default for CreatorIntelligenceConfig {
    fn default() -> Self {
        Self {
            chain_id: 4663,
            resolve_batch: 4_000,
            resume_backstep: 500,
            rpc_fallback_batch: 500,
            rpc_chunk: 50,
            profile_interval: Duration::from_secs(900),
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct OriginSummary {
    pub launches: u64,
    pub resolved: u64,
    pub from_block: i64,
    pub to_block: i64,
    pub backlog: bool,
}

pub struct CreatorIntelligence {
    pool: PgPool,
    clickhouse: ClickHouseClient,
    rpc: Option<IndexerRpcClient>,
    config: CreatorIntelligenceConfig,
}

impl CreatorIntelligence {
    pub fn new(
        pool: PgPool,
        clickhouse: ClickHouseClient,
        rpc: Option<IndexerRpcClient>,
        config: CreatorIntelligenceConfig,
    ) -> Self {
        Self {
            pool,
            clickhouse,
            rpc,
            config,
        }
    }

    fn checkpoint_key(&self) -> String {
        format!("launch_origins:{}", self.config.chain_id)
    }

    /// Resolves the transaction sender for the next batch of launches.
    pub async fn resolve_origins_once(&self) -> LedgerResult<OriginSummary> {
        let cursor: i64 = sqlx::query_scalar(
            "select last_position from market.projector_checkpoints where checkpoint_key = $1",
        )
        .bind(self.checkpoint_key())
        .fetch_optional(&self.pool)
        .await
        .map_err(db_error)?
        .unwrap_or(0);

        let pending = sqlx::query(
            r#"select launch.launch_id, launch.launch_block, launch.launch_tx_hash,
                      launch.creator_address
               from public.protocol_token_launches launch
               left join market.launch_origins origin on origin.launch_id = launch.launch_id
               where launch.chain_id = $1 and launch.launch_block >= $2
                 and origin.launch_id is null
               order by launch.launch_block
               limit $3"#,
        )
        .bind(self.config.chain_id as i64)
        .bind(cursor)
        .bind(self.config.resolve_batch)
        .fetch_all(&self.pool)
        .await
        .map_err(db_error)?;

        if pending.is_empty() {
            return Ok(OriginSummary::default());
        }

        let mut launch_ids = Vec::with_capacity(pending.len());
        let mut blocks = Vec::with_capacity(pending.len());
        let mut hashes = Vec::with_capacity(pending.len());
        let mut creators = Vec::with_capacity(pending.len());
        for row in &pending {
            launch_ids.push(row.get::<uuid::Uuid, _>("launch_id"));
            blocks.push(row.get::<i64, _>("launch_block"));
            hashes.push(
                row.get::<Option<String>, _>("launch_tx_hash")
                    .unwrap_or_default()
                    .to_ascii_lowercase(),
            );
            creators.push(row.get::<Option<String>, _>("creator_address"));
        }
        let from_block = blocks.iter().copied().min().unwrap_or(0);
        let to_block = blocks.iter().copied().max().unwrap_or(0);

        let lookups: Vec<String> = hashes
            .iter()
            .filter(|hash| hash.len() == 66)
            .cloned()
            .collect();
        let senders = self
            .clickhouse
            .transaction_senders(
                self.config.chain_id as u32,
                from_block.max(0) as u64,
                to_block.max(0) as u64,
                &lookups,
            )
            .await?;
        let senders: std::collections::HashMap<String, String> = senders.into_iter().collect();

        let origins: Vec<Option<String>> = hashes
            .iter()
            .map(|hash| senders.get(hash).cloned())
            .collect();
        let resolved = origins.iter().filter(|origin| origin.is_some()).count() as u64;

        let mut tx = self.pool.begin().await.map_err(db_error)?;
        sqlx::query(
            r#"insert into market.launch_origins
                 (launch_id, chain_id, launch_block, event_creator, origin_address)
               select * from unnest($1::uuid[], $2::bigint[], $3::bigint[], $4::text[], $5::text[])
               on conflict (launch_id) do update set
                 origin_address = coalesce(excluded.origin_address, market.launch_origins.origin_address),
                 event_creator = excluded.event_creator,
                 resolved_at = now()"#,
        )
        .bind(&launch_ids)
        .bind(&vec![self.config.chain_id as i64; launch_ids.len()])
        .bind(&blocks)
        .bind(&creators)
        .bind(&origins)
        .execute(&mut *tx)
        .await
        .map_err(db_error)?;

        // Resume a little before the last block seen: launches are indexed in
        // block order, but a straggler written after the cursor passed would
        // otherwise never be visited.
        let next_cursor = (to_block - self.config.resume_backstep).max(0);
        sqlx::query(
            r#"insert into market.projector_checkpoints (checkpoint_key, last_position, updated_at)
               values ($1, $2, now())
               on conflict (checkpoint_key) do update set
                 last_position = greatest(market.projector_checkpoints.last_position, excluded.last_position),
                 updated_at = now()"#,
        )
        .bind(self.checkpoint_key())
        .bind(next_cursor)
        .execute(&mut *tx)
        .await
        .map_err(db_error)?;
        tx.commit().await.map_err(db_error)?;

        Ok(OriginSummary {
            launches: pending.len() as u64,
            resolved,
            from_block,
            to_block,
            backlog: pending.len() as i64 >= self.config.resolve_batch,
        })
    }

    /// Fills in launches the transaction store could not answer for, by asking
    /// the node directly. Runs a bounded number per pass because it competes
    /// with the rest of the pipeline for the same rate-limited gateway.
    pub async fn backfill_missing_origins(&self) -> LedgerResult<u64> {
        let Some(rpc) = self.rpc.as_ref() else {
            return Ok(0);
        };
        let pending = sqlx::query(
            r#"select origin.launch_id, launch.launch_tx_hash
               from market.launch_origins origin
               join public.protocol_token_launches launch on launch.launch_id = origin.launch_id
               where origin.chain_id = $1 and origin.origin_address is null
                 and launch.launch_tx_hash is not null
               order by origin.launch_block desc
               limit $2"#,
        )
        .bind(self.config.chain_id as i64)
        .bind(self.config.rpc_fallback_batch)
        .fetch_all(&self.pool)
        .await
        .map_err(db_error)?;
        if pending.is_empty() {
            return Ok(0);
        }
        let launch_ids: Vec<uuid::Uuid> = pending
            .iter()
            .map(|row| row.get::<uuid::Uuid, _>("launch_id"))
            .collect();
        let hashes: Vec<String> = pending
            .iter()
            .map(|row| {
                row.get::<Option<String>, _>("launch_tx_hash")
                    .unwrap_or_default()
            })
            .collect();

        let mut senders: Vec<Option<String>> = Vec::with_capacity(hashes.len());
        for chunk in hashes.chunks(self.config.rpc_chunk) {
            senders.extend(rpc.get_transaction_senders(chunk).await?);
        }
        let resolved = senders.iter().filter(|sender| sender.is_some()).count() as u64;
        if resolved == 0 {
            return Ok(0);
        }
        sqlx::query(
            r#"update market.launch_origins origin
               set origin_address = incoming.origin_address, resolved_at = now()
               from (select * from unnest($1::uuid[], $2::text[])
                       as t(launch_id, origin_address)) as incoming
               where origin.launch_id = incoming.launch_id
                 and incoming.origin_address is not null"#,
        )
        .bind(&launch_ids)
        .bind(&senders)
        .execute(&self.pool)
        .await
        .map_err(db_error)?;
        Ok(resolved)
    }

    /// Rebuilds every creator's track record. The aggregates span a creator's
    /// whole history, so this is a full rebuild rather than an incremental one.
    pub async fn rebuild_profiles(&self) -> LedgerResult<u64> {
        let mut tx = self.pool.begin().await.map_err(db_error)?;
        let started: chrono::DateTime<chrono::Utc> = sqlx::query_scalar("select now()")
            .fetch_one(&mut *tx)
            .await
            .map_err(db_error)?;
        let result = sqlx::query(PROFILE_SQL)
            .bind(self.config.chain_id as i64)
            .execute(&mut *tx)
            .await
            .map_err(db_error)?;
        // A launch moves from its event creator to the resolved sender as
        // origins fill in, which retires the profile it used to belong to.
        // Anything this pass did not rewrite is such a leftover.
        sqlx::query("delete from market.creator_profiles where chain_id = $1 and computed_at < $2")
            .bind(self.config.chain_id as i64)
            .bind(started)
            .execute(&mut *tx)
            .await
            .map_err(db_error)?;
        tx.commit().await.map_err(db_error)?;
        Ok(result.rows_affected())
    }
}

const PROFILE_SQL: &str = r#"
with launches as (
  select launch.launch_id,
         coalesce(origin.origin_address, launch.creator_address) as creator_address,
         origin.origin_address is not null as by_origin,
         launch.launched_at,
         (launch.graduated_at is not null or launch.lifecycle_phase = 'pool_created')
           as graduated,
         stats.ath_market_cap_usd,
         stats.last_trade_at
  from public.protocol_token_launches launch
  left join market.launch_origins origin on origin.launch_id = launch.launch_id
  left join market.token_stats stats on stats.launch_id = launch.launch_id
  where launch.chain_id = $1
    and coalesce(origin.origin_address, launch.creator_address) is not null
), traders as (
  select coalesce(origin.origin_address, position.creator_address) as creator_address,
         count(distinct position.address) as buyers,
         count(distinct position.address) filter (
           where position.sold_token >= position.bought_token
             and position.proceeds_usd < position.cost_usd
         ) as losing_buyers,
         sum(position.cost_usd) as cost_usd,
         sum(position.proceeds_usd - position.cost_usd) filter (
           where position.sold_token >= position.bought_token
         ) as realized_usd
  from market.wallet_positions position
  left join market.launch_origins origin on origin.launch_id = position.launch_id
  where position.chain_id = $1
    and coalesce(origin.origin_address, position.creator_address) is not null
  group by 1
), profiles as (
  select creator_address,
         case when bool_or(by_origin) then 'origin' else 'event' end as attribution,
         count(*)::int as launches,
         count(*) filter (where graduated)::int as graduated,
         count(*) filter (where launched_at > now() - interval '7 days')::int as launches_7d,
         count(*) filter (where launched_at > now() - interval '24 hours')::int as launches_24h,
         min(launched_at) as first_launch_at,
         max(launched_at) as last_launch_at,
         max(ath_market_cap_usd) as best_ath_mcap_usd,
         percentile_cont(0.5) within group (order by ath_market_cap_usd)
           as median_ath_mcap_usd,
         count(*) filter (where last_trade_at is not null)::int as traded_launches,
         -- Traded, then quiet for a day, and never graduated: the shape of a
         -- launch that went nowhere.
         count(*) filter (
           where last_trade_at is not null
             and last_trade_at < now() - interval '24 hours'
             and not graduated
         )::int as dead_launches
  from launches
  group by creator_address
)
insert into market.creator_profiles (
  chain_id, creator_address, attribution, launches, graduated, graduation_rate,
  launches_7d, launches_24h, first_launch_at, last_launch_at,
  best_ath_mcap_usd, median_ath_mcap_usd, traded_launches, dead_launches,
  buyers, losing_buyers, buyer_cost_usd, buyer_realized_usd, computed_at
)
select $1, profiles.creator_address, profiles.attribution, profiles.launches,
       profiles.graduated,
       case when profiles.launches > 0
         then profiles.graduated::numeric / profiles.launches * 100 end,
       profiles.launches_7d, profiles.launches_24h,
       profiles.first_launch_at, profiles.last_launch_at,
       profiles.best_ath_mcap_usd, profiles.median_ath_mcap_usd,
       profiles.traded_launches, profiles.dead_launches,
       coalesce(traders.buyers, 0)::int, coalesce(traders.losing_buyers, 0)::int,
       traders.cost_usd, traders.realized_usd, now()
from profiles
left join traders on traders.creator_address = profiles.creator_address
on conflict (chain_id, creator_address) do update set
  attribution = excluded.attribution,
  launches = excluded.launches,
  graduated = excluded.graduated,
  graduation_rate = excluded.graduation_rate,
  launches_7d = excluded.launches_7d,
  launches_24h = excluded.launches_24h,
  first_launch_at = excluded.first_launch_at,
  last_launch_at = excluded.last_launch_at,
  best_ath_mcap_usd = excluded.best_ath_mcap_usd,
  median_ath_mcap_usd = excluded.median_ath_mcap_usd,
  traded_launches = excluded.traded_launches,
  dead_launches = excluded.dead_launches,
  buyers = excluded.buyers,
  losing_buyers = excluded.losing_buyers,
  buyer_cost_usd = excluded.buyer_cost_usd,
  buyer_realized_usd = excluded.buyer_realized_usd,
  computed_at = excluded.computed_at
"#;

fn db_error(error: impl std::fmt::Display) -> LedgerError {
    LedgerError::Persistence(error.to_string())
}
