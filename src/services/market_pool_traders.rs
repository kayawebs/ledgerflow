//! Who made a pool trade.
//!
//! A Uniswap V4 swap records the router that called the PoolManager, not the
//! person behind it, so wallet profit and loss covered curve trades only —
//! a minority of this chain's trading, and the half that excludes every win
//! made after a token graduates. The transaction sender is the account that
//! acted, and the transaction store already holds it.
//!
//! The attribution is honest but not absolute: for someone trading through a
//! front end the sender is the trader, while for a bundler or a relayer it is
//! whoever paid for the transaction. It is recorded beside the router address
//! rather than replacing it, so either reading stays available.

use std::time::Duration;

use sqlx::{PgPool, Row};

use crate::{
    core::errors::{LedgerError, LedgerResult},
    indexer::clickhouse::ClickHouseClient,
};

#[derive(Clone, Debug)]
pub struct PoolTraderConfig {
    pub chain_id: u64,
    /// Swaps resolved per pass.
    pub batch_size: i64,
    /// Blocks held back from the newest swap, so a block still being written is
    /// not resolved half-indexed.
    pub safety_lag_blocks: i64,
    /// How far back from the head live attribution reaches. Swaps older than
    /// this are history, and history is only walked when backfill is on: the
    /// two jobs have completely different urgency and cost.
    pub live_window_blocks: i64,
    /// Whether to walk the whole table rather than just the live window.
    pub backfill: bool,
    pub idle_delay: Duration,
}

impl Default for PoolTraderConfig {
    fn default() -> Self {
        Self {
            chain_id: 4663,
            batch_size: 4_000,
            safety_lag_blocks: 30,
            live_window_blocks: 200_000,
            backfill: false,
            idle_delay: Duration::from_secs(3),
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct PoolTraderSummary {
    pub swaps: u64,
    pub resolved: u64,
    pub from_block: i64,
    pub to_block: i64,
    pub backlog: bool,
}

pub struct PoolTraderResolver {
    pool: PgPool,
    clickhouse: ClickHouseClient,
    config: PoolTraderConfig,
}

impl PoolTraderResolver {
    pub fn new(pool: PgPool, clickhouse: ClickHouseClient, config: PoolTraderConfig) -> Self {
        Self {
            pool,
            clickhouse,
            config,
        }
    }

    pub async fn resolve_once(&self) -> LedgerResult<PoolTraderSummary> {
        // Newest first. The position projection walks forward from a cursor near
        // the chain head, so those are the swaps it is about to read; a history
        // of thirty-six million older swaps would otherwise be in front of them
        // in the queue and the live scoreboard would wait half a day.
        // In live mode the floor keeps this off the thirty-seven million rows of
        // history; with backfill on it is lifted and the whole table is walked,
        // newest first.
        let floor = if self.config.backfill {
            i64::MIN
        } else {
            self.config.live_window_blocks
        };
        let pending = sqlx::query(
            r#"select tx_hash, block_number
               from market.pool_swap_facts
               where chain_id = $1 and trader_address is null
                 and block_number <= (
                   select max(block_number) - $3 from market.pool_swap_facts where chain_id = $1
                 )
                 and ($4 = -9223372036854775808 or block_number >= (
                   select max(block_number) - $4 from market.pool_swap_facts where chain_id = $1
                 ))
               order by block_number desc
               limit $2"#,
        )
        .bind(self.config.chain_id as i64)
        .bind(self.config.batch_size)
        .bind(self.config.safety_lag_blocks)
        .bind(floor)
        .fetch_all(&self.pool)
        .await
        .map_err(db_error)?;
        if pending.is_empty() {
            return Ok(PoolTraderSummary::default());
        }

        let mut hashes: Vec<String> = Vec::with_capacity(pending.len());
        let mut from_block = i64::MAX;
        let mut to_block = 0_i64;
        for row in &pending {
            let hash: String = row.get("tx_hash");
            let block: i64 = row.get("block_number");
            from_block = from_block.min(block);
            to_block = to_block.max(block);
            hashes.push(hash.to_ascii_lowercase());
        }
        hashes.sort();
        hashes.dedup();

        let senders = self
            .clickhouse
            .transaction_senders(
                self.config.chain_id as u32,
                from_block.max(0) as u64,
                to_block.max(0) as u64,
                &hashes,
            )
            .await?;
        if senders.is_empty() {
            // Nothing to write, but the rows must not be retried forever: the
            // next pass will see them again only once the store catches up.
            return Ok(PoolTraderSummary {
                swaps: pending.len() as u64,
                from_block,
                to_block,
                backlog: pending.len() as i64 >= self.config.batch_size,
                ..Default::default()
            });
        }

        let (tx_hashes, traders): (Vec<String>, Vec<String>) = senders
            .into_iter()
            .map(|(hash, sender, _entry)| (hash, sender))
            .unzip();
        let result = sqlx::query(
            r#"update market.pool_swap_facts swap
               set trader_address = incoming.trader
               from (select * from unnest($2::text[], $3::text[]) as t(tx_hash, trader)) as incoming
               where swap.chain_id = $1 and swap.tx_hash = incoming.tx_hash
                 and swap.trader_address is null"#,
        )
        .bind(self.config.chain_id as i64)
        .bind(&tx_hashes)
        .bind(&traders)
        .execute(&self.pool)
        .await
        .map_err(db_error)?;

        Ok(PoolTraderSummary {
            swaps: pending.len() as u64,
            resolved: result.rows_affected(),
            from_block,
            to_block,
            backlog: pending.len() as i64 >= self.config.batch_size,
        })
    }
}

fn db_error(error: impl std::fmt::Display) -> LedgerError {
    LedgerError::Persistence(error.to_string())
}
