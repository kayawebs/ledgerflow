//! Estimated arbitrage earnings from local-RPC receipts.
//!
//! The deterministic economic verifier only accounts for the transaction
//! sender, while most arbitrage bots settle inside their executor contract, so
//! nearly every candidate ends as `insufficient_evidence`. This estimator nets
//! ERC-20 `Transfer` logs over the executing scope (sender + entry contract),
//! values each token delta at the hourly USD price and subtracts exact gas.
//! Results are labelled as estimates; unpriced deltas make the result unknown.

use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};

use chrono::{DateTime, Utc};
use num_bigint::{BigInt, BigUint, Sign};
use serde::Serialize;
use serde_json::Value;
use sqlx::{PgPool, Row};
use tokio::sync::Mutex;

use crate::{
    core::errors::{LedgerError, LedgerResult},
    indexer::rpc::IndexerRpcClient,
    services::market_data::NATIVE_ASSET_ADDRESS,
};

pub const ARBITRAGE_METHODOLOGY_VERSION: &str = "market_arbitrage_receipt_scope_v1";
const TRANSFER_TOPIC: &str = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReceiptAccounting {
    pub tx_status: &'static str,
    pub deltas: BTreeMap<String, BigInt>,
    pub gas_wei: BigUint,
}

/// Nets ERC-20 transfers into and out of `scope`. Transfers between two scope
/// addresses cancel; zero deltas are dropped.
pub fn account_receipt(receipt: &Value, scope: &[String]) -> ReceiptAccounting {
    let tx_status = match receipt.get("status").and_then(Value::as_str) {
        Some("0x1") => "success",
        Some(_) => "reverted",
        None if receipt.is_null() => "missing",
        None => "success",
    };
    let gas_used = hex_uint(receipt.get("gasUsed").and_then(Value::as_str));
    let gas_price = hex_uint(receipt.get("effectiveGasPrice").and_then(Value::as_str));
    let mut deltas = BTreeMap::<String, BigInt>::new();
    if tx_status == "success" {
        for log in receipt
            .get("logs")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let topics = log
                .get("topics")
                .and_then(Value::as_array)
                .map(|topics| topics.iter().filter_map(Value::as_str).collect::<Vec<_>>())
                .unwrap_or_default();
            // ERC-721 transfers carry the token id as a fourth topic.
            if topics.len() != 3 || !topics[0].eq_ignore_ascii_case(TRANSFER_TOPIC) {
                continue;
            }
            let Some(token) = log.get("address").and_then(Value::as_str) else {
                continue;
            };
            let data = log.get("data").and_then(Value::as_str).unwrap_or("0x");
            let digits = data.trim_start_matches("0x");
            if digits.len() != 64 {
                continue;
            }
            let Some(amount) = BigUint::parse_bytes(digits.as_bytes(), 16) else {
                continue;
            };
            let from = topic_address(topics[1]);
            let to = topic_address(topics[2]);
            let from_scope = scope.contains(&from);
            let to_scope = scope.contains(&to);
            if from_scope == to_scope {
                continue;
            }
            let entry = deltas.entry(token.to_ascii_lowercase()).or_default();
            let signed = BigInt::from_biguint(Sign::Plus, amount);
            if to_scope {
                *entry += signed;
            } else {
                *entry -= signed;
            }
        }
    }
    deltas.retain(|_, amount| amount.sign() != Sign::NoSign);
    ReceiptAccounting {
        tx_status,
        deltas,
        gas_wei: gas_used * gas_price,
    }
}

fn hex_uint(value: Option<&str>) -> BigUint {
    value
        .and_then(|value| BigUint::parse_bytes(value.trim_start_matches("0x").as_bytes(), 16))
        .unwrap_or_default()
}

fn topic_address(topic: &str) -> String {
    let digits = topic.trim_start_matches("0x");
    format!("0x{}", &digits[digits.len().saturating_sub(40)..]).to_ascii_lowercase()
}

#[derive(Clone, Debug)]
pub struct MarketArbitrageConfig {
    pub chain_id: u64,
    pub backfill_days: i64,
    pub batch_size: i64,
    pub receipt_batch_size: usize,
    pub receipt_concurrency: usize,
    pub leaderboard_size: i64,
}

impl Default for MarketArbitrageConfig {
    fn default() -> Self {
        Self {
            chain_id: 4663,
            backfill_days: 2,
            batch_size: 1_000,
            receipt_batch_size: 50,
            receipt_concurrency: 4,
            leaderboard_size: 200,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct MarketArbitrageSummary {
    pub candidates: u64,
    pub estimated: u64,
    pub cursor_time: Option<DateTime<Utc>>,
    pub backlog: bool,
    pub leaderboard_rows: u64,
    pub receipt_ms: u64,
    pub persist_ms: u64,
}

#[derive(Clone)]
pub struct MarketArbitrageEstimator {
    pool: PgPool,
    rpc: IndexerRpcClient,
    config: MarketArbitrageConfig,
    last_rebuilt: Arc<Mutex<[Option<Instant>; 3]>>,
}

struct Candidate {
    tx_hash: String,
    block_number: i64,
    block_time: Option<DateTime<Utc>>,
    sender: Option<String>,
    entry_contract: Option<String>,
    swap_count: i32,
    pool_count: i32,
}

impl MarketArbitrageEstimator {
    pub fn new(pool: PgPool, rpc: IndexerRpcClient, config: MarketArbitrageConfig) -> Self {
        Self {
            pool,
            rpc,
            config,
            last_rebuilt: Arc::new(Mutex::new([None; 3])),
        }
    }

    pub async fn process_once(&self) -> LedgerResult<MarketArbitrageSummary> {
        let mut summary = MarketArbitrageSummary::default();
        let chain_id = self.config.chain_id as i64;
        let key = format!("arbitrage_estimates:{}", self.config.chain_id);
        let cursor_micros: Option<i64> = sqlx::query_scalar(
            "select last_position from market.projector_checkpoints where checkpoint_key = $1",
        )
        .bind(&key)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_error)?;
        let cursor = cursor_micros
            .and_then(|micros| DateTime::<Utc>::from_timestamp_micros(micros))
            .unwrap_or_else(|| Utc::now() - chrono::Duration::days(self.config.backfill_days));
        // Re-read one second behind the cursor: many candidates share a timestamp.
        let rows = sqlx::query(
            r#"select candidate.tx_hash, candidate.block_number, candidate.block_timestamp,
                      candidate.sender_address, candidate.entry_contract,
                      candidate.swap_log_count, candidate.distinct_pool_count
               from public.arbitrage_candidates candidate
               where candidate.chain_id = $1
                 and candidate.block_timestamp >= $2 - interval '1 second'
                 and not exists (
                   select 1 from market.arbitrage_estimates estimate
                   where estimate.chain_id = candidate.chain_id
                     and estimate.tx_hash = candidate.tx_hash
                 )
               order by candidate.block_timestamp
               limit $3"#,
        )
        .bind(chain_id)
        .bind(cursor)
        .bind(self.config.batch_size)
        .fetch_all(&self.pool)
        .await
        .map_err(db_error)?;
        let mut rows = rows;
        if (rows.len() as i64) < self.config.batch_size {
            // Once caught up, also estimate older deterministically verified
            // executions so estimates can be checked against them.
            let verified = sqlx::query(
                r#"select candidate.tx_hash, candidate.block_number, candidate.block_timestamp,
                          candidate.sender_address, candidate.entry_contract,
                          candidate.swap_log_count, candidate.distinct_pool_count
                   from public.economic_verification_results verification
                   join public.arbitrage_candidates candidate
                     on candidate.chain_id = verification.chain_id
                    and candidate.tx_hash = verification.tx_hash
                   where verification.chain_id = $1 and verification.result_status = 'verified'
                     and not exists (
                       select 1 from market.arbitrage_estimates estimate
                       where estimate.chain_id = candidate.chain_id
                         and estimate.tx_hash = candidate.tx_hash
                     )
                   limit $2"#,
            )
            .bind(chain_id)
            .bind(self.config.batch_size - rows.len() as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(db_error)?;
            rows.extend(verified);
        }
        let candidates = rows
            .iter()
            .map(|row| Candidate {
                tx_hash: row.get("tx_hash"),
                block_number: row.get("block_number"),
                block_time: row.get("block_timestamp"),
                sender: row.get("sender_address"),
                entry_contract: row.get("entry_contract"),
                swap_count: row.get("swap_log_count"),
                pool_count: row.get("distinct_pool_count"),
            })
            .collect::<Vec<_>>();
        summary.candidates = candidates.len() as u64;
        summary.backlog = candidates.len() as i64 >= self.config.batch_size;
        if candidates.is_empty() {
            summary.leaderboard_rows = self.rebuild_leaderboards().await?;
            return Ok(summary);
        }

        let started = Instant::now();
        let receipts = self.fetch_receipts(&candidates).await?;
        summary.receipt_ms = started.elapsed().as_millis() as u64;

        let started = Instant::now();
        let mut tx = self.pool.begin().await.map_err(db_error)?;
        for (candidate, receipt) in candidates.iter().zip(receipts.iter()) {
            let scope = [candidate.sender.clone(), candidate.entry_contract.clone()]
                .into_iter()
                .flatten()
                .map(|address| address.to_ascii_lowercase())
                .collect::<Vec<_>>();
            let accounting = account_receipt(receipt, &scope);
            let bot = candidate
                .entry_contract
                .clone()
                .or_else(|| candidate.sender.clone())
                .unwrap_or_default()
                .to_ascii_lowercase();
            sqlx::query(
                r#"insert into market.arbitrage_estimates (
                     chain_id, tx_hash, block_number, block_time, sender_address, entry_contract,
                     bot_address, swap_count, pool_count, tx_status, token_count, gas_native,
                     status, reason, methodology_version, computed_at
                   ) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
                             $12::numeric / 1e18, 'unknown', 'pending valuation', $13, now())
                   on conflict (chain_id, tx_hash) do update set
                     tx_status = excluded.tx_status, token_count = excluded.token_count,
                     gas_native = excluded.gas_native, computed_at = now()"#,
            )
            .bind(chain_id)
            .bind(&candidate.tx_hash)
            .bind(candidate.block_number)
            .bind(candidate.block_time)
            .bind(&candidate.sender)
            .bind(&candidate.entry_contract)
            .bind(&bot)
            .bind(candidate.swap_count)
            .bind(candidate.pool_count)
            .bind(accounting.tx_status)
            .bind(accounting.deltas.len() as i32)
            .bind(accounting.gas_wei.to_string())
            .bind(ARBITRAGE_METHODOLOGY_VERSION)
            .execute(&mut *tx)
            .await
            .map_err(db_error)?;
            for (token, amount) in &accounting.deltas {
                sqlx::query(
                    r#"insert into market.arbitrage_estimate_deltas (chain_id, tx_hash, token_address, amount_raw)
                       values ($1, $2, $3, $4::numeric)
                       on conflict (chain_id, tx_hash, token_address) do update set
                         amount_raw = excluded.amount_raw"#,
                )
                .bind(chain_id)
                .bind(&candidate.tx_hash)
                .bind(token)
                .bind(amount.to_string())
                .execute(&mut *tx)
                .await
                .map_err(db_error)?;
            }
        }
        let hashes = candidates
            .iter()
            .map(|candidate| candidate.tx_hash.clone())
            .collect::<Vec<_>>();
        // Tokens without a price series get one backfilled by the market projector.
        sqlx::query(
            r#"insert into market.price_assets (chain_id, asset_address, source)
               select distinct chain_id, token_address, 'arbitrage_delta'
               from market.arbitrage_estimate_deltas
               where chain_id = $1 and tx_hash = any($2)
               on conflict (chain_id, asset_address) do nothing"#,
        )
        .bind(chain_id)
        .bind(&hashes)
        .execute(&mut *tx)
        .await
        .map_err(db_error)?;
        let valued = sqlx::query(VALUATION_SQL)
            .bind(chain_id)
            .bind(&hashes)
            .bind(NATIVE_ASSET_ADDRESS)
            .execute(&mut *tx)
            .await
            .map_err(db_error)?;
        summary.estimated = valued.rows_affected();
        let newest = candidates
            .iter()
            .filter_map(|candidate| candidate.block_time)
            .max()
            .unwrap_or(cursor);
        sqlx::query(
            r#"insert into market.projector_checkpoints (
                 checkpoint_key, last_position, last_run_at, last_success_at, updated_at
               ) values ($1, $2, now(), now(), now())
               on conflict (checkpoint_key) do update set
                 last_position = greatest(market.projector_checkpoints.last_position,
                                          excluded.last_position),
                 last_run_at = now(), last_success_at = now(), last_error = null,
                 updated_at = now()"#,
        )
        .bind(&key)
        .bind(newest.timestamp_micros())
        .execute(&mut *tx)
        .await
        .map_err(db_error)?;
        tx.commit().await.map_err(db_error)?;
        summary.persist_ms = started.elapsed().as_millis() as u64;
        summary.cursor_time = Some(newest);
        if !summary.backlog {
            summary.leaderboard_rows = self.rebuild_leaderboards().await?;
        }
        Ok(summary)
    }

    /// Re-values recent estimates that were unknown because a token had no
    /// USD price yet.
    pub async fn revalue_unknown(&self) -> LedgerResult<u64> {
        let hashes: Vec<String> = sqlx::query_scalar(
            r#"select tx_hash from market.arbitrage_estimates
               where chain_id = $1 and status = 'unknown'
                 and block_time > now() - interval '30 days'
               order by block_time desc
               limit 50000"#,
        )
        .bind(self.config.chain_id as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(db_error)?;
        if hashes.is_empty() {
            return Ok(0);
        }
        let result = sqlx::query(VALUATION_SQL)
            .bind(self.config.chain_id as i64)
            .bind(&hashes)
            .bind(NATIVE_ASSET_ADDRESS)
            .execute(&self.pool)
            .await
            .map_err(db_error)?;
        Ok(result.rows_affected())
    }

    async fn fetch_receipts(&self, candidates: &[Candidate]) -> LedgerResult<Vec<Value>> {
        let chunks = candidates
            .chunks(self.config.receipt_batch_size.max(1))
            .map(|chunk| {
                chunk
                    .iter()
                    .map(|candidate| candidate.tx_hash.clone())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let mut results = vec![Vec::new(); chunks.len()];
        for window in chunks
            .iter()
            .enumerate()
            .collect::<Vec<_>>()
            .chunks(self.config.receipt_concurrency.max(1))
        {
            let mut tasks = tokio::task::JoinSet::new();
            for (index, hashes) in window {
                let rpc = self.rpc.clone();
                let hashes = (*hashes).clone();
                let index = *index;
                tasks.spawn(async move { (index, rpc.get_transaction_receipts(&hashes).await) });
            }
            while let Some(joined) = tasks.join_next().await {
                let (index, receipts) = joined.map_err(|error| {
                    LedgerError::TransientIngestion(format!("receipt task failed: {error}"))
                })?;
                results[index] = receipts?;
            }
        }
        Ok(results.into_iter().flatten().collect())
    }

    async fn rebuild_leaderboards(&self) -> LedgerResult<u64> {
        let windows = [("24h", 24, 60_u64), ("7d", 168, 300), ("30d", 720, 900)];
        let mut last_rebuilt = self.last_rebuilt.lock().await;
        let mut rows = 0;
        for (index, (key, hours, every)) in windows.into_iter().enumerate() {
            if last_rebuilt[index].is_some_and(|at| at.elapsed() < Duration::from_secs(every)) {
                continue;
            }
            let mut tx = self.pool.begin().await.map_err(db_error)?;
            sqlx::query(
                "delete from market.arbitrage_bot_leaderboard where chain_id = $1 and window_key = $2",
            )
            .bind(self.config.chain_id as i64)
            .bind(key)
            .execute(&mut *tx)
            .await
            .map_err(db_error)?;
            let result = sqlx::query(BOT_LEADERBOARD_SQL)
                .bind(self.config.chain_id as i64)
                .bind(key)
                .bind(hours)
                .bind(self.config.leaderboard_size)
                .bind(ARBITRAGE_METHODOLOGY_VERSION)
                .execute(&mut *tx)
                .await
                .map_err(db_error)?;
            tx.commit().await.map_err(db_error)?;
            rows += result.rows_affected();
            last_rebuilt[index] = Some(Instant::now());
        }
        Ok(rows)
    }
}

/// Values the deltas of estimates `$2` at the hourly USD price in effect at the
/// block, then derives gross, gas and net USD for each estimate.
const VALUATION_SQL: &str = r#"
with priced as (
  select delta.tx_hash, delta.token_address,
         delta.amount_raw * power(10::numeric, -(meta.decimals)) as amount,
         delta.amount_raw * power(10::numeric, -(meta.decimals)) * price.price_usd as usd
  from market.arbitrage_estimate_deltas delta
  join market.arbitrage_estimates estimate
    on estimate.chain_id = delta.chain_id and estimate.tx_hash = delta.tx_hash
  left join public.erc20_token_metadata meta
    on meta.chain_id = delta.chain_id and meta.token_address = delta.token_address
   and meta.fetch_status = 'ok'
  left join lateral (
    select p.price_usd from market.asset_usd_prices p
    where p.chain_id = delta.chain_id and p.asset_address = delta.token_address
      and p.bucket_start <= estimate.block_time
      and p.bucket_start > estimate.block_time - interval '48 hours'
    order by p.bucket_start desc limit 1
  ) price on true
  where delta.chain_id = $1 and delta.tx_hash = any($2)
), updated_deltas as (
  update market.arbitrage_estimate_deltas delta
  set amount = priced.amount, usd = priced.usd
  from priced
  where delta.chain_id = $1 and delta.tx_hash = priced.tx_hash
    and delta.token_address = priced.token_address
  returning 1
), totals as (
  select tx_hash, sum(usd) as gross, bool_and(usd is not null) as all_priced,
         (array_agg(token_address order by usd is null desc))[1] as unpriced_token
  from priced group by tx_hash
), verified as (
  -- Deterministic verification (archive balances + traces) supersedes the
  -- receipt estimate for the profit it covers.
  select verification.tx_hash,
         verification.profit_amount_raw::numeric
           * power(10::numeric, -(meta.decimals)) * price.price_usd as usd
  from public.economic_verification_results verification
  join market.arbitrage_estimates estimate
    on estimate.chain_id = verification.chain_id and estimate.tx_hash = verification.tx_hash
  left join public.erc20_token_metadata meta
    on meta.chain_id = verification.chain_id
   and meta.token_address = verification.profit_token_address
  left join lateral (
    select p.price_usd from market.asset_usd_prices p
    where p.chain_id = verification.chain_id
      and p.asset_address = verification.profit_token_address
      and p.bucket_start <= estimate.block_time
      and p.bucket_start > estimate.block_time - interval '48 hours'
    order by p.bucket_start desc limit 1
  ) price on true
  where verification.chain_id = $1 and verification.tx_hash = any($2)
    and verification.result_status = 'verified'
), native as (
  select estimate.tx_hash,
         (select p.price_usd from market.asset_usd_prices p
          where p.chain_id = estimate.chain_id and p.asset_address = $3
            and p.bucket_start <= estimate.block_time
            and p.bucket_start > estimate.block_time - interval '48 hours'
          order by p.bucket_start desc limit 1) as native_usd
  from market.arbitrage_estimates estimate
  where estimate.chain_id = $1 and estimate.tx_hash = any($2)
)
update market.arbitrage_estimates estimate
set gas_usd = estimate.gas_native * native.native_usd,
    gross_usd = case
      when verified.usd is not null then verified.usd
      when coalesce(totals.all_priced, true) then coalesce(totals.gross, 0) end,
    net_usd = case
      when native.native_usd is null then null
      when verified.usd is not null then verified.usd - estimate.gas_native * native.native_usd
      when coalesce(totals.all_priced, true)
        then coalesce(totals.gross, 0) - estimate.gas_native * native.native_usd end,
    status = case
      when native.native_usd is not null
       and (verified.usd is not null or coalesce(totals.all_priced, true))
        then 'estimated' else 'unknown' end,
    reason = case
      when native.native_usd is null then 'native USD price unavailable'
      when verified.usd is not null then 'profit from deterministic verification'
      when not coalesce(totals.all_priced, true) then 'unpriced token ' || totals.unpriced_token
      when estimate.tx_status = 'reverted' then 'reverted transaction; gas only'
      when estimate.token_count = 0 then 'no ERC-20 delta in sender/executor scope'
    end,
    is_verified = exists (
      select 1 from public.economic_verification_results verified
      where verified.chain_id = estimate.chain_id and verified.tx_hash = estimate.tx_hash
        and verified.result_status = 'verified'
    )
from native
left join totals on totals.tx_hash = native.tx_hash
left join verified on verified.tx_hash = native.tx_hash
where estimate.chain_id = $1 and estimate.tx_hash = native.tx_hash
"#;

const BOT_LEADERBOARD_SQL: &str = r#"
with scoped as (
  select * from market.arbitrage_estimates
  where chain_id = $1 and block_time >= now() - make_interval(hours => $3::int)
), bots as (
  select bot_address,
         coalesce(sum(net_usd) filter (where status = 'estimated'), 0) as net,
         coalesce(sum(gross_usd) filter (where status = 'estimated'), 0) as gross,
         coalesce(sum(gas_usd) filter (where status = 'estimated'), 0) as gas,
         count(*)::int as executions,
         (count(*) filter (where status = 'estimated' and net_usd > 0))::int as profitable,
         (count(*) filter (where status = 'unknown'))::int as unknown,
         count(distinct sender_address)::int as senders,
         (count(*) filter (where is_verified))::int as verified,
         (array_agg(tx_hash order by net_usd desc nulls last))[1] as best_tx,
         max(net_usd) as best_net,
         max(block_time) as last_at
  from scoped
  group by bot_address
), ranked as (
  select bots.*, row_number() over (order by net desc, bot_address) as rank
  from bots
  order by net desc
  limit $4
)
insert into market.arbitrage_bot_leaderboard (
  chain_id, window_key, bot_address, rank, address_kind, net_usd, gross_usd, gas_usd,
  executions, profitable_executions, unknown_executions, senders, verified_executions,
  best_tx_hash, best_net_usd, last_execution_at, cluster_id, methodology_version, computed_at
)
select $1, $2, ranked.bot_address, rank, kind.address_kind, net, gross, gas, executions,
       profitable, unknown, senders, verified, best_tx, best_net, last_at,
       (select member.cluster_id from public.protocol_scoped_cluster_members member
        join public.protocol_scoped_clusters cluster on cluster.cluster_id = member.cluster_id
        where member.chain_id = $1 and member.address = ranked.bot_address
          and cluster.cluster_kind = 'searcher' and cluster.status = 'active'
        limit 1),
       $5, now()
from ranked
left join public.chain_addresses kind on kind.chain_id = $1 and kind.address = ranked.bot_address
"#;

fn db_error(error: impl std::fmt::Display) -> LedgerError {
    LedgerError::Persistence(error.to_string())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    const SENDER: &str = "0x1111111111111111111111111111111111111111";
    const EXECUTOR: &str = "0x2222222222222222222222222222222222222222";
    const POOL_A: &str = "0x3333333333333333333333333333333333333333";
    const POOL_B: &str = "0x4444444444444444444444444444444444444444";
    const WETH: &str = "0x0bd7d308f8e1639fab988df18a8011f41eacad73";
    const USDG: &str = "0x5fc5360d0400a0fd4f2af552add042d716f1d168";

    fn topic(address: &str) -> String {
        format!("0x{:0>64}", address.trim_start_matches("0x"))
    }

    fn transfer(token: &str, from: &str, to: &str, amount: u128) -> Value {
        json!({
            "address": token,
            "topics": [TRANSFER_TOPIC, topic(from), topic(to)],
            "data": format!("0x{amount:064x}"),
        })
    }

    fn scope() -> Vec<String> {
        vec![SENDER.to_owned(), EXECUTOR.to_owned()]
    }

    #[test]
    fn cyclic_arbitrage_nets_to_the_profit_token() {
        let receipt = json!({
            "status": "0x1",
            "gasUsed": "0x5208",
            "effectiveGasPrice": "0x3b9aca00",
            "logs": [
                transfer(WETH, EXECUTOR, POOL_A, 1_000),
                transfer(USDG, POOL_A, EXECUTOR, 2_500),
                transfer(USDG, EXECUTOR, POOL_B, 2_500),
                transfer(WETH, POOL_B, EXECUTOR, 1_012),
                // Internal movement between scope members cancels.
                transfer(WETH, EXECUTOR, SENDER, 12),
            ]
        });
        let accounting = account_receipt(&receipt, &scope());
        assert_eq!(accounting.tx_status, "success");
        assert_eq!(accounting.deltas.len(), 1);
        assert_eq!(accounting.deltas[WETH], BigInt::from(12));
        assert_eq!(
            accounting.gas_wei,
            BigUint::from(21_000_u64 * 1_000_000_000)
        );
    }

    #[test]
    fn reverted_transactions_only_cost_gas() {
        let receipt = json!({
            "status": "0x0",
            "gasUsed": "0x10",
            "effectiveGasPrice": "0x2",
            "logs": [transfer(WETH, POOL_A, EXECUTOR, 5)]
        });
        let accounting = account_receipt(&receipt, &scope());
        assert_eq!(accounting.tx_status, "reverted");
        assert!(accounting.deltas.is_empty());
        assert_eq!(accounting.gas_wei, BigUint::from(32_u32));
    }

    #[test]
    fn nft_transfers_and_malformed_logs_are_ignored() {
        let receipt = json!({
            "status": "0x1",
            "gasUsed": "0x1",
            "effectiveGasPrice": "0x1",
            "logs": [
                {"address": WETH, "topics": [TRANSFER_TOPIC, topic(POOL_A), topic(EXECUTOR), topic(POOL_B)], "data": "0x"},
                {"address": WETH, "topics": [TRANSFER_TOPIC, topic(POOL_A), topic(EXECUTOR)], "data": "0x01"},
                transfer(USDG, POOL_A, POOL_B, 9),
            ]
        });
        assert!(account_receipt(&receipt, &scope()).deltas.is_empty());
    }

    #[test]
    fn losses_are_negative_deltas() {
        let receipt = json!({
            "status": "0x1",
            "gasUsed": "0x0",
            "effectiveGasPrice": "0x0",
            "logs": [
                transfer(WETH, SENDER, POOL_A, 100),
                transfer(WETH, POOL_B, SENDER, 90),
            ]
        });
        assert_eq!(
            account_receipt(&receipt, &scope()).deltas[WETH],
            BigInt::from(-10)
        );
    }

    #[test]
    fn missing_receipts_are_flagged() {
        assert_eq!(account_receipt(&Value::Null, &scope()).tx_status, "missing");
    }
}
