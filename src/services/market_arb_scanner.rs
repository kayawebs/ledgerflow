//! Arbitrage candidate detection over every block.
//!
//! The trace monitor detects candidates too, but it deliberately stays at the
//! chain head: when it falls behind it jumps forward and drops the blocks in
//! between. On a chain producing ten blocks a second that leaves roughly a
//! tenth of them examined, which makes every arbitrage figure a sample of
//! unknown size rather than a count. It also reads Uniswap V2 and V3 only,
//! while V4 is now the busiest venue on this chain.
//!
//! This scanner covers every block from its own cursor, reads the log store
//! rather than the node, and recognises V4 as well.

use serde_json::{Value, json};
use sqlx::{PgPool, Row};

use crate::{
    core::errors::{LedgerError, LedgerResult},
    indexer::clickhouse::{ArbitrageTopics, ClickHouseClient},
};

pub const UNISWAP_V2_SWAP_TOPIC: &str =
    "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822";
pub const UNISWAP_V3_SWAP_TOPIC: &str =
    "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67";
pub const UNISWAP_V4_SWAP_TOPIC: &str =
    "0x40e9cecb9f5f1f1c5b9c97dec2917b7ee92e57ba5563708daca94dd84ad7112f";

/// Marks the rows this scanner wrote, so its coverage can be told apart from
/// the trace monitor's sampled ones.
const DETECTION_SOURCE: &str = "clickhouse_swap_topics_v4";

#[derive(Clone, Debug)]
pub struct ArbScannerConfig {
    pub chain_id: u64,
    /// Blocks per pass. Large enough to outrun block production so the cursor
    /// catches up rather than falling behind for good.
    pub block_batch: u64,
    /// Blocks held back from the log store's head, so a block being written is
    /// never read half-indexed.
    pub safety_lag_blocks: u64,
    pub min_swaps: u32,
    /// Where to start when there is no cursor yet. History is not backfilled,
    /// so this is a short look-back, not the chain's beginning.
    pub initial_lookback_blocks: u64,
}

impl Default for ArbScannerConfig {
    fn default() -> Self {
        Self {
            chain_id: 4663,
            block_batch: 2_000,
            safety_lag_blocks: 30,
            min_swaps: 2,
            initial_lookback_blocks: 20_000,
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct ArbScanSummary {
    pub from_block: i64,
    pub to_block: i64,
    pub head_block: i64,
    pub candidates: u64,
    pub rows: u64,
    pub backlog: bool,
}

pub struct ArbCandidateScanner {
    pool: PgPool,
    clickhouse: ClickHouseClient,
    config: ArbScannerConfig,
}

impl ArbCandidateScanner {
    pub fn new(pool: PgPool, clickhouse: ClickHouseClient, config: ArbScannerConfig) -> Self {
        Self {
            pool,
            clickhouse,
            config,
        }
    }

    fn checkpoint_key(&self) -> String {
        format!("arb_candidates:{}", self.config.chain_id)
    }

    pub async fn scan_once(&self) -> LedgerResult<ArbScanSummary> {
        let cursor: Option<i64> = sqlx::query_scalar(
            "select last_position from market.projector_checkpoints where checkpoint_key = $1",
        )
        .bind(self.checkpoint_key())
        .fetch_optional(&self.pool)
        .await
        .map_err(db_error)?;

        let head = self
            .clickhouse
            .max_log_block(
                self.config.chain_id as u32,
                cursor.unwrap_or(0).max(0) as u64,
            )
            .await?
            .unwrap_or(0)
            .saturating_sub(self.config.safety_lag_blocks);
        if head == 0 {
            return Ok(ArbScanSummary::default());
        }

        let from = match cursor {
            Some(cursor) => (cursor as u64).saturating_add(1),
            None => head.saturating_sub(self.config.initial_lookback_blocks),
        };
        if from > head {
            return Ok(ArbScanSummary {
                from_block: from as i64,
                head_block: head as i64,
                ..Default::default()
            });
        }
        let to = head.min(
            from.saturating_add(self.config.block_batch)
                .saturating_sub(1),
        );

        let topics = ArbitrageTopics {
            v2: UNISWAP_V2_SWAP_TOPIC,
            v3: UNISWAP_V3_SWAP_TOPIC,
            v4: UNISWAP_V4_SWAP_TOPIC,
        };
        let rows = self
            .clickhouse
            .arbitrage_candidate_txs(
                self.config.chain_id as u32,
                from,
                to,
                self.config.min_swaps,
                &topics,
            )
            .await?;

        let mut written = 0;
        if !rows.is_empty() {
            written = self.persist(&rows, from, to).await?;
        }
        self.advance(to as i64).await?;

        Ok(ArbScanSummary {
            from_block: from as i64,
            to_block: to as i64,
            head_block: head as i64,
            candidates: rows.len() as u64,
            rows: written,
            backlog: to < head,
        })
    }

    async fn persist(&self, rows: &[Value], from: u64, to: u64) -> LedgerResult<u64> {
        let hashes: Vec<String> = rows
            .iter()
            .filter_map(|row| row.get("tx_hash")?.as_str().map(str::to_owned))
            .collect();
        // The sender and the contract it entered come from the transaction, not
        // the logs, and the estimator scopes its profit accounting to both.
        let parties = self
            .clickhouse
            .transaction_senders(self.config.chain_id as u32, from, to, &hashes)
            .await?;
        let parties: std::collections::HashMap<String, (String, Option<String>)> = parties
            .into_iter()
            .map(|(hash, sender, entry)| (hash, (sender, entry)))
            .collect();

        let mut tx_hashes = Vec::with_capacity(rows.len());
        let mut blocks = Vec::with_capacity(rows.len());
        let mut timestamps = Vec::with_capacity(rows.len());
        let mut senders = Vec::with_capacity(rows.len());
        let mut entries = Vec::with_capacity(rows.len());
        let mut tx_indexes = Vec::with_capacity(rows.len());
        let mut swap_counts = Vec::with_capacity(rows.len());
        let mut v2_counts = Vec::with_capacity(rows.len());
        let mut v3_counts = Vec::with_capacity(rows.len());
        let mut pool_counts = Vec::with_capacity(rows.len());
        let mut metadata = Vec::with_capacity(rows.len());

        for row in rows {
            let Some(hash) = row.get("tx_hash").and_then(Value::as_str) else {
                continue;
            };
            let number = number_of(row, "block_number");
            let timestamp = number_of(row, "block_timestamp");
            let v4 = number_of(row, "v4_count");
            tx_hashes.push(hash.to_owned());
            blocks.push(number);
            timestamps.push(
                chrono::DateTime::from_timestamp(timestamp, 0).unwrap_or_else(chrono::Utc::now),
            );
            let (sender, entry) = parties
                .get(hash)
                .cloned()
                .map(|(sender, entry)| (Some(sender), entry))
                .unwrap_or((None, None));
            senders.push(sender);
            entries.push(entry);
            tx_indexes.push(number_of(row, "tx_index"));
            swap_counts.push(number_of(row, "swap_count") as i32);
            v2_counts.push(number_of(row, "v2_count") as i32);
            v3_counts.push(number_of(row, "v3_count") as i32);
            pool_counts.push(number_of(row, "pool_count") as i32);
            // The table has no column for V4, so the count rides along in the
            // metadata rather than being folded into the V3 figure.
            metadata.push(json!({ "v4_swap_count": v4, "scanner": DETECTION_SOURCE }));
        }

        let result = sqlx::query(
            r#"insert into public.arbitrage_candidates (
                 chain_id, tx_hash, block_number, block_timestamp, detection_source,
                 sender_address, entry_contract, tx_index, score, swap_log_count,
                 v2_swap_count, v3_swap_count, distinct_pool_count, metadata
               )
               select $1, hash, block, stamp, $2, sender, entry, idx, swaps, swaps, v2, v3, pools, meta
               from unnest($3::text[], $4::bigint[], $5::timestamptz[], $6::text[], $7::text[],
                           $8::bigint[], $9::int[], $10::int[], $11::int[], $12::int[], $13::jsonb[])
                 as t(hash, block, stamp, sender, entry, idx, swaps, v2, v3, pools, meta)
               on conflict (chain_id, tx_hash) do update set
                 block_number = excluded.block_number,
                 block_timestamp = excluded.block_timestamp,
                 detection_source = excluded.detection_source,
                 sender_address = coalesce(excluded.sender_address,
                                           public.arbitrage_candidates.sender_address),
                 entry_contract = coalesce(excluded.entry_contract,
                                           public.arbitrage_candidates.entry_contract),
                 tx_index = excluded.tx_index,
                 score = greatest(public.arbitrage_candidates.score, excluded.score),
                 swap_log_count = excluded.swap_log_count,
                 v2_swap_count = excluded.v2_swap_count,
                 v3_swap_count = excluded.v3_swap_count,
                 distinct_pool_count = excluded.distinct_pool_count,
                 metadata = public.arbitrage_candidates.metadata || excluded.metadata,
                 updated_at = now()"#,
        )
        .bind(self.config.chain_id as i64)
        .bind(DETECTION_SOURCE)
        .bind(&tx_hashes)
        .bind(&blocks)
        .bind(&timestamps)
        .bind(&senders)
        .bind(&entries)
        .bind(&tx_indexes)
        .bind(&swap_counts)
        .bind(&v2_counts)
        .bind(&v3_counts)
        .bind(&pool_counts)
        .bind(&metadata)
        .execute(&self.pool)
        .await
        .map_err(db_error)?;
        Ok(result.rows_affected())
    }

    async fn advance(&self, to: i64) -> LedgerResult<()> {
        sqlx::query(
            r#"insert into market.projector_checkpoints (checkpoint_key, last_position, updated_at)
               values ($1, $2, now())
               on conflict (checkpoint_key) do update set
                 last_position = greatest(market.projector_checkpoints.last_position,
                                          excluded.last_position),
                 updated_at = now()"#,
        )
        .bind(self.checkpoint_key())
        .bind(to)
        .execute(&self.pool)
        .await
        .map_err(db_error)?;
        Ok(())
    }
}

/// ClickHouse returns integers as JSON strings for 64-bit columns.
fn number_of(row: &Value, key: &str) -> i64 {
    match row.get(key) {
        Some(Value::Number(number)) => number.as_i64().unwrap_or_default(),
        Some(Value::String(text)) => text.parse().unwrap_or_default(),
        _ => 0,
    }
}

fn db_error(error: impl std::fmt::Display) -> LedgerError {
    LedgerError::Persistence(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_clickhouse_numbers_in_either_shape() {
        let row = json!({ "a": 42, "b": "1789", "c": null });
        assert_eq!(number_of(&row, "a"), 42);
        assert_eq!(number_of(&row, "b"), 1789);
        assert_eq!(number_of(&row, "c"), 0);
        assert_eq!(number_of(&row, "missing"), 0);
    }

    #[test]
    fn the_batch_outruns_block_production() {
        // The chain produces about ten blocks a second; a pass must cover more
        // than a poll interval's worth or the cursor never catches up.
        let config = ArbScannerConfig::default();
        assert!(config.block_batch >= 600, "{}", config.block_batch);
    }
}
