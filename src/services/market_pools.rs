//! Post-graduation pool trading for Pons V2 launches.
//!
//! Graduation moves a launch's liquidity into a Uniswap V4 pool. The
//! graduation transaction emits the PoolManager `Initialize` event, whose
//! currencies identify the launch token, so the pool id is read from evidence
//! instead of being derived. Swaps are then fetched from local-RPC logs
//! filtered by those pool ids and stored as facts that feed the same candles
//! as curve trades.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use num_bigint::BigInt;
use serde::Serialize;
use serde_json::{Value, json};
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::{
    core::errors::{LedgerError, LedgerResult},
    indexer::{clickhouse::ClickHouseClient, rpc::IndexerRpcClient},
};

pub const V4_INITIALIZE_TOPIC: &str =
    "0xdd466e674ea557f56295e2d0218a125ea4b4f0f6f3307b95f85e6110838d6438";
pub const V4_SWAP_TOPIC: &str =
    "0x40e9cecb9f5f1f1c5b9c97dec2917b7ee92e57ba5563708daca94dd84ad7112f";
const CLICKHOUSE_POOL_IDS_PER_QUERY: usize = 2_000;
const SWAP_INSERT_CHUNK: usize = 10_000;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PoolInitialization {
    pub pool_manager: String,
    pub pool_id: String,
    pub currency0: String,
    pub currency1: String,
    pub fee: u32,
    pub tick_spacing: i32,
    pub hooks: String,
    pub block_number: u64,
}

/// Finds the V4 `Initialize` event in `receipt` that created a pool for `token`.
pub fn find_pool_initialization(receipt: &Value, token: &str) -> Option<PoolInitialization> {
    let token = token.to_ascii_lowercase();
    receipt.get("logs")?.as_array()?.iter().find_map(|log| {
        let topics = log
            .get("topics")?
            .as_array()?
            .iter()
            .filter_map(Value::as_str)
            .collect::<Vec<_>>();
        if topics.len() != 4 || !topics[0].eq_ignore_ascii_case(V4_INITIALIZE_TOPIC) {
            return None;
        }
        let currency0 = word_address(topics[2]);
        let currency1 = word_address(topics[3]);
        if currency0 != token && currency1 != token {
            return None;
        }
        let data = log.get("data")?.as_str()?.trim_start_matches("0x");
        if data.len() < 64 * 3 {
            return None;
        }
        Some(PoolInitialization {
            pool_manager: log.get("address")?.as_str()?.to_ascii_lowercase(),
            pool_id: topics[1].to_ascii_lowercase(),
            currency0,
            currency1,
            fee: u32::from_str_radix(&data[64 - 8..64], 16).ok()?,
            tick_spacing: signed_word(&data[64..128])?.try_into().ok()?,
            hooks: word_address(&data[128..192]),
            block_number: hex_u64(log.get("blockNumber")?)?,
        })
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PoolSwap {
    pub pool_id: String,
    pub sender: String,
    pub amount0: BigInt,
    pub amount1: BigInt,
    pub sqrt_price_x96: BigInt,
    pub tx_hash: String,
    pub log_index: u64,
    pub block_number: u64,
    pub block_time: Option<DateTime<Utc>>,
}

/// Decodes `Swap(PoolId indexed id, address indexed sender, int128 amount0,
/// int128 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick,
/// uint24 fee)`. Amounts are from the swapper's side: positive means the
/// swapper received the currency, negative means it paid.
pub fn decode_swap(log: &Value) -> Option<PoolSwap> {
    let topics = log
        .get("topics")?
        .as_array()?
        .iter()
        .filter_map(Value::as_str)
        .collect::<Vec<_>>();
    if topics.len() != 3 || !topics[0].eq_ignore_ascii_case(V4_SWAP_TOPIC) {
        return None;
    }
    let data = log.get("data")?.as_str()?.trim_start_matches("0x");
    if data.len() != 64 * 6 {
        return None;
    }
    Some(PoolSwap {
        pool_id: topics[1].to_ascii_lowercase(),
        sender: word_address(topics[2]),
        amount0: signed_word(&data[0..64])?,
        amount1: signed_word(&data[64..128])?,
        sqrt_price_x96: signed_word(&data[128..192])?,
        tx_hash: log.get("transactionHash")?.as_str()?.to_ascii_lowercase(),
        log_index: hex_u64(log.get("logIndex")?)?,
        block_number: hex_u64(log.get("blockNumber")?)?,
        block_time: log
            .get("blockTimestamp")
            .and_then(hex_u64)
            .and_then(|seconds| DateTime::from_timestamp(seconds as i64, 0)),
    })
}

/// Converts a swap into a launch-token trade: `buy` when the swapper received
/// the token. Returns (side, token amount, quote amount) as raw integers, or
/// `None` when both legs move in the same direction.
pub fn swap_trade(
    swap: &PoolSwap,
    token_is_currency0: bool,
) -> Option<(&'static str, BigInt, BigInt)> {
    let (token, quote) = if token_is_currency0 {
        (&swap.amount0, &swap.amount1)
    } else {
        (&swap.amount1, &swap.amount0)
    };
    let zero = BigInt::from(0);
    if *token > zero && *quote < zero {
        Some(("buy", token.clone(), -quote.clone()))
    } else if *token < zero && *quote > zero {
        Some(("sell", -token.clone(), quote.clone()))
    } else {
        None
    }
}

fn signed_word(word: &str) -> Option<BigInt> {
    let bytes = hex::decode(word).ok()?;
    Some(BigInt::from_signed_bytes_be(&bytes))
}

fn word_address(word: &str) -> String {
    let digits = word.trim_start_matches("0x");
    format!("0x{}", &digits[digits.len().saturating_sub(40)..]).to_ascii_lowercase()
}

fn hex_u64(value: &Value) -> Option<u64> {
    u64::from_str_radix(value.as_str()?.trim_start_matches("0x"), 16).ok()
}

#[derive(Clone, Debug)]
pub struct PoolIndexerConfig {
    pub chain_id: u64,
    pub head_lag_blocks: u64,
    pub map_batch: i64,
    pub initial_range: u64,
    pub max_range: u64,
    pub min_range: u64,
    pub pool_ids_per_request: usize,
}

impl Default for PoolIndexerConfig {
    fn default() -> Self {
        Self {
            chain_id: 4663,
            head_lag_blocks: 4,
            map_batch: 200,
            initial_range: 20_000,
            max_range: 500_000,
            min_range: 1_000,
            pool_ids_per_request: 200,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct PoolIteration {
    pub mapped: u64,
    pub unmapped: u64,
    pub from_block: u64,
    pub to_block: u64,
    pub head_block: u64,
    pub swaps: u64,
    pub range: u64,
}

impl PoolIteration {
    pub fn has_backlog(&self) -> bool {
        self.to_block < self.head_block || self.mapped + self.unmapped > 0
    }
}

pub struct PoolIndexer {
    pool: PgPool,
    rpc: IndexerRpcClient,
    /// LedgerFlow's canonical log store, ingested from the same local node.
    /// Preferred for swap history: node `eth_getLogs` over the busy PoolManager
    /// times out on large ranges.
    logs: Option<ClickHouseClient>,
    config: PoolIndexerConfig,
    range: u64,
}

#[derive(Default)]
struct SwapColumns {
    tx_hashes: Vec<String>,
    log_indexes: Vec<i64>,
    launch_ids: Vec<Uuid>,
    pool_ids: Vec<String>,
    blocks: Vec<i64>,
    times: Vec<Option<DateTime<Utc>>>,
    sides: Vec<String>,
    senders: Vec<String>,
    token_amounts: Vec<String>,
    quote_amounts: Vec<String>,
    sqrt_prices: Vec<String>,
}

struct MappedPool {
    launch_id: Uuid,
    token_is_currency0: bool,
}

impl PoolIndexer {
    pub fn new(
        pool: PgPool,
        rpc: IndexerRpcClient,
        logs: Option<ClickHouseClient>,
        config: PoolIndexerConfig,
    ) -> Self {
        Self {
            range: if logs.is_some() {
                config.max_range
            } else {
                config.initial_range
            },
            pool,
            rpc,
            logs,
            config,
        }
    }

    fn cursor_key(&self) -> String {
        format!("pool_swaps:{}", self.config.chain_id)
    }

    pub async fn process_once(&mut self) -> LedgerResult<PoolIteration> {
        let mut iteration = PoolIteration::default();
        self.map_graduated_launches(&mut iteration).await?;
        if iteration.mapped + iteration.unmapped >= self.config.map_batch as u64 {
            // Map every graduated launch before scanning swaps; otherwise each
            // pool mapped behind the cursor needs its own historical catch-up.
            return Ok(iteration);
        }
        self.index_swaps(&mut iteration).await?;
        Ok(iteration)
    }

    async fn map_graduated_launches(&self, iteration: &mut PoolIteration) -> LedgerResult<()> {
        let chain_id = self.config.chain_id as i64;
        let rows = sqlx::query(
            r#"select launch.launch_id, launch.token_address, launch.graduation_tx_hash
               from public.protocol_token_launches launch
               where launch.chain_id = $1 and launch.graduation_tx_hash is not null
                 and not exists (
                   select 1 from market.token_pools pool where pool.launch_id = launch.launch_id
                 )
               order by launch.graduated_at desc nulls last
               limit $2"#,
        )
        .bind(chain_id)
        .bind(self.config.map_batch)
        .fetch_all(&self.pool)
        .await
        .map_err(db_error)?;
        if rows.is_empty() {
            return Ok(());
        }
        let hashes = rows
            .iter()
            .map(|row| row.get::<String, _>("graduation_tx_hash"))
            .collect::<Vec<_>>();
        let receipts = self.rpc.get_transaction_receipts(&hashes).await?;
        let cursor = self.load_cursor().await?;
        for (row, receipt) in rows.iter().zip(receipts.iter()) {
            let launch_id: Uuid = row.get("launch_id");
            let token: String = row.get("token_address");
            match find_pool_initialization(receipt, &token) {
                Some(init) => {
                    let token_is_currency0 = init.currency0 == token.to_ascii_lowercase();
                    sqlx::query(
                        r#"insert into market.token_pools (
                             launch_id, chain_id, status, pool_manager, pool_id, currency0, currency1,
                             token_is_currency0, fee, tick_spacing, hooks, init_block
                           ) values ($1, $2, 'mapped', $3, $4, $5, $6, $7, $8, $9, $10, $11)
                           on conflict (launch_id) do nothing"#,
                    )
                    .bind(launch_id)
                    .bind(chain_id)
                    .bind(&init.pool_manager)
                    .bind(&init.pool_id)
                    .bind(&init.currency0)
                    .bind(&init.currency1)
                    .bind(token_is_currency0)
                    .bind(init.fee as i32)
                    .bind(init.tick_spacing)
                    .bind(&init.hooks)
                    .bind(init.block_number as i64)
                    .execute(&self.pool)
                    .await
                    .map_err(db_error)?;
                    iteration.mapped += 1;
                    // A pool mapped behind the swap cursor gets its history now.
                    if let Some(cursor) = cursor
                        && init.block_number <= cursor
                    {
                        let mut pools = BTreeMap::new();
                        pools.insert(
                            init.pool_id.clone(),
                            MappedPool {
                                launch_id,
                                token_is_currency0,
                            },
                        );
                        self.fetch_and_store(&pools, &init.pool_manager, init.block_number, cursor)
                            .await?;
                    }
                }
                None => {
                    sqlx::query(
                        r#"insert into market.token_pools (launch_id, chain_id, status, reason)
                           values ($1, $2, 'unmapped', $3)
                           on conflict (launch_id) do nothing"#,
                    )
                    .bind(launch_id)
                    .bind(chain_id)
                    .bind(if receipt.is_null() {
                        "graduation receipt unavailable"
                    } else {
                        "no V4 Initialize event for the launch token in the graduation receipt"
                    })
                    .execute(&self.pool)
                    .await
                    .map_err(db_error)?;
                    iteration.unmapped += 1;
                }
            }
        }
        Ok(())
    }

    async fn index_swaps(&mut self, iteration: &mut PoolIteration) -> LedgerResult<()> {
        let chain_id = self.config.chain_id as i64;
        let mut head = self
            .rpc
            .get_latest_block_number()
            .await?
            .saturating_sub(self.config.head_lag_blocks);
        if let Some(logs) = &self.logs {
            // Only scan what the log store has ingested.
            let floor = self.load_cursor().await?.unwrap_or_default();
            match logs
                .max_log_block(self.config.chain_id as u32, floor)
                .await?
            {
                Some(ingested) => {
                    head = head.min(ingested.saturating_sub(self.config.head_lag_blocks))
                }
                None => head = 0,
            }
        }
        iteration.head_block = head;
        let first_init: Option<i64> = sqlx::query_scalar(
            "select min(init_block) from market.token_pools where chain_id = $1 and status = 'mapped'",
        )
        .bind(chain_id)
        .fetch_one(&self.pool)
        .await
        .map_err(db_error)?;
        let Some(first_init) = first_init else {
            iteration.to_block = head;
            return Ok(());
        };
        let from = self
            .load_cursor()
            .await?
            .map(|cursor| cursor + 1)
            .unwrap_or(first_init as u64);
        iteration.from_block = from;
        iteration.range = self.range;
        if from > head {
            iteration.to_block = head;
            return Ok(());
        }
        let to = (from + self.range - 1).min(head);
        let rows = sqlx::query(
            r#"select launch_id, pool_id, pool_manager, token_is_currency0
               from market.token_pools
               where chain_id = $1 and status = 'mapped' and init_block <= $2"#,
        )
        .bind(chain_id)
        .bind(to as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(db_error)?;
        let mut by_manager = BTreeMap::<String, BTreeMap<String, MappedPool>>::new();
        for row in rows {
            by_manager
                .entry(row.get("pool_manager"))
                .or_default()
                .insert(
                    row.get("pool_id"),
                    MappedPool {
                        launch_id: row.get("launch_id"),
                        token_is_currency0: row.get("token_is_currency0"),
                    },
                );
        }
        let mut stored = 0;
        for (manager, pools) in &by_manager {
            match self.fetch_and_store(pools, manager, from, to).await {
                Ok(count) => stored += count,
                Err(error) if self.range > self.config.min_range => {
                    self.range = (self.range / 4).max(self.config.min_range);
                    tracing::warn!(error = %error, range = self.range, "pool swap log range reduced");
                    iteration.to_block = from.saturating_sub(1);
                    return Ok(());
                }
                Err(error) => return Err(error),
            }
        }
        if to - from + 1 >= self.range {
            self.range = (self.range * 2).min(self.config.max_range);
        }
        sqlx::query(
            r#"insert into market.projector_checkpoints (
                 checkpoint_key, last_position, last_run_at, last_success_at, updated_at
               ) values ($1, $2, now(), now(), now())
               on conflict (checkpoint_key) do update set
                 last_position = excluded.last_position, last_run_at = now(),
                 last_success_at = now(), last_error = null, updated_at = now()"#,
        )
        .bind(self.cursor_key())
        .bind(to as i64)
        .execute(&self.pool)
        .await
        .map_err(db_error)?;
        iteration.to_block = to;
        iteration.swaps = stored;
        Ok(())
    }

    async fn fetch_and_store(
        &self,
        pools: &BTreeMap<String, MappedPool>,
        manager: &str,
        from: u64,
        to: u64,
    ) -> LedgerResult<u64> {
        let ids = pools.keys().cloned().collect::<Vec<_>>();
        let mut logs = Vec::new();
        if let Some(store) = &self.logs {
            for chunk in ids.chunks(CLICKHOUSE_POOL_IDS_PER_QUERY) {
                logs.extend(
                    store
                        .canonical_logs_by_topic1(
                            self.config.chain_id as u32,
                            from,
                            to,
                            manager,
                            V4_SWAP_TOPIC,
                            chunk,
                        )
                        .await?,
                );
            }
            return self.store_swaps(pools, &logs).await;
        }
        for chunk in ids.chunks(self.config.pool_ids_per_request.max(1)) {
            let mut start = from;
            while start <= to {
                let end = (start + self.config.max_range - 1).min(to);
                logs.extend(
                    self.rpc
                        .get_logs(json!({
                            "fromBlock": format!("0x{start:x}"),
                            "toBlock": format!("0x{end:x}"),
                            "address": manager,
                            "topics": [V4_SWAP_TOPIC, chunk],
                        }))
                        .await?,
                );
                start = end + 1;
            }
        }
        self.store_swaps(pools, &logs).await
    }

    async fn store_swaps(
        &self,
        pools: &BTreeMap<String, MappedPool>,
        logs: &[Value],
    ) -> LedgerResult<u64> {
        let mut seen = BTreeSet::new();
        let mut rows = SwapColumns::default();
        for log in logs {
            let Some(swap) = decode_swap(log) else {
                continue;
            };
            let Some(mapped) = pools.get(&swap.pool_id) else {
                continue;
            };
            let Some((side, token_amount, quote_amount)) =
                swap_trade(&swap, mapped.token_is_currency0)
            else {
                continue;
            };
            if !seen.insert((swap.tx_hash.clone(), swap.log_index)) {
                continue;
            }
            rows.tx_hashes.push(swap.tx_hash);
            rows.log_indexes.push(swap.log_index as i64);
            rows.launch_ids.push(mapped.launch_id);
            rows.pool_ids.push(swap.pool_id);
            rows.blocks.push(swap.block_number as i64);
            rows.times.push(swap.block_time);
            rows.sides.push(side.to_owned());
            rows.senders.push(swap.sender);
            rows.token_amounts.push(token_amount.to_string());
            rows.quote_amounts.push(quote_amount.to_string());
            rows.sqrt_prices.push(swap.sqrt_price_x96.to_string());
        }
        let mut tx = self.pool.begin().await.map_err(db_error)?;
        let mut stored = 0;
        for start in (0..rows.tx_hashes.len()).step_by(SWAP_INSERT_CHUNK) {
            let end = (start + SWAP_INSERT_CHUNK).min(rows.tx_hashes.len());
            let result = sqlx::query(
                r#"insert into market.pool_swap_facts (
                     chain_id, tx_hash, log_index, launch_id, pool_id, block_number, occurred_at,
                     trade_side, sender_address, token_amount_raw, quote_amount_raw, sqrt_price_x96
                   )
                   select $1, tx_hash, log_index, launch_id, pool_id, block_number, occurred_at,
                          trade_side, sender_address, token_amount::numeric, quote_amount::numeric,
                          sqrt_price::numeric
                   from unnest($2::text[], $3::bigint[], $4::uuid[], $5::text[], $6::bigint[],
                               $7::timestamptz[], $8::text[], $9::text[], $10::text[], $11::text[],
                               $12::text[])
                     as swap(tx_hash, log_index, launch_id, pool_id, block_number, occurred_at,
                             trade_side, sender_address, token_amount, quote_amount, sqrt_price)
                   on conflict (chain_id, tx_hash, log_index) do nothing"#,
            )
            .bind(self.config.chain_id as i64)
            .bind(&rows.tx_hashes[start..end])
            .bind(&rows.log_indexes[start..end])
            .bind(&rows.launch_ids[start..end])
            .bind(&rows.pool_ids[start..end])
            .bind(&rows.blocks[start..end])
            .bind(&rows.times[start..end])
            .bind(&rows.sides[start..end])
            .bind(&rows.senders[start..end])
            .bind(&rows.token_amounts[start..end])
            .bind(&rows.quote_amounts[start..end])
            .bind(&rows.sqrt_prices[start..end])
            .execute(&mut *tx)
            .await
            .map_err(db_error)?;
            stored += result.rows_affected();
        }
        tx.commit().await.map_err(db_error)?;
        Ok(stored)
    }

    async fn load_cursor(&self) -> LedgerResult<Option<u64>> {
        let cursor: Option<i64> = sqlx::query_scalar(
            "select last_position from market.projector_checkpoints where checkpoint_key = $1",
        )
        .bind(self.cursor_key())
        .fetch_optional(&self.pool)
        .await
        .map_err(db_error)?;
        Ok(cursor.map(|value| value as u64))
    }
}

fn db_error(error: impl std::fmt::Display) -> LedgerError {
    LedgerError::Persistence(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    const TOKEN: &str = "0xc11d78a097472424fb4278dda7d8a79a7c945c27";
    const POOL_ID: &str = "0x5fcd6cd140c70e19000000000000000000000000000000000000000000000000";

    fn signed(value: i128) -> String {
        let bytes = BigInt::from(value).to_signed_bytes_be();
        let fill = if value < 0 { "ff" } else { "00" };
        format!("{}{}", fill.repeat(32 - bytes.len()), hex::encode(bytes))
    }

    #[test]
    fn signed_words_round_trip() {
        assert_eq!(
            signed_word(&signed(-13_832_396_805_626_049)),
            Some(BigInt::from(-13_832_396_805_626_049_i64))
        );
        assert_eq!(signed_word(&signed(42)), Some(BigInt::from(42)));
    }

    #[test]
    fn finds_the_initialize_event_for_the_launch_token() {
        let receipt = json!({"logs": [
            {"address": "0x8366A39cC670b4001A1121b8f6a443a643E40951",
             "topics": [V4_INITIALIZE_TOPIC, POOL_ID,
                        "0x0000000000000000000000000000000000000000000000000000000000000000",
                        format!("0x000000000000000000000000{}", &TOKEN[2..])],
             "data": format!("0x{}{}{}{}{}", signed(10_000), signed(200),
                             "000000000000000000000000c7819b64a1daecd7ec19856d026cb14efbd89046",
                             signed(1), signed(-5)),
             "blockNumber": "0x10"}
        ]});
        let init = find_pool_initialization(&receipt, TOKEN).expect("initialize");
        assert_eq!(
            init.pool_manager,
            "0x8366a39cc670b4001a1121b8f6a443a643e40951"
        );
        assert_eq!(init.pool_id, POOL_ID);
        assert_eq!(init.currency0, "0x0000000000000000000000000000000000000000");
        assert_eq!(init.currency1, TOKEN);
        assert_eq!(init.fee, 10_000);
        assert_eq!(init.tick_spacing, 200);
        assert_eq!(init.hooks, "0xc7819b64a1daecd7ec19856d026cb14efbd89046");
        assert!(
            find_pool_initialization(&receipt, "0x1111111111111111111111111111111111111111")
                .is_none()
        );
    }

    #[test]
    fn decodes_a_real_swap_as_a_token_buy() {
        // Observed on Robinhood Chain: the swapper paid native ETH (amount0 < 0)
        // and received the launch token, which left the PoolManager in a Transfer.
        let log = json!({
            "topics": [V4_SWAP_TOPIC, POOL_ID,
                       "0x00000000000000000000000066a9893cc07d91d95644aedd05d03f95e1dba8af"],
            "data": format!("0x{}{}{}{}{}{}", signed(-13_832_396_805_626_049),
                            signed(6_351_111_571_021_346_346_654_742),
                            signed(1), signed(1), signed(-10), signed(10_000)),
            "transactionHash": "0xabc",
            "logIndex": "0x3",
            "blockNumber": "0x20",
            "blockTimestamp": "0x6aa99efe"
        });
        let swap = decode_swap(&log).expect("swap");
        let (side, token, quote) = swap_trade(&swap, false).expect("trade");
        assert_eq!(side, "buy");
        assert_eq!(token, BigInt::from(6_351_111_571_021_346_346_654_742_i128));
        assert_eq!(quote, BigInt::from(13_832_396_805_626_049_i64));
        let (side, _, _) = swap_trade(&swap, true).expect("mirrored");
        assert_eq!(side, "sell");
    }
}
