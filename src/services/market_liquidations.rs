//! Lending liquidation indexer over local-RPC logs.
//!
//! Scans for the Morpho Blue `Liquidate` event across all emitters, accepts an
//! emitter only after `idToMarketParams(id)` returns a market with non-zero
//! loan and collateral tokens, and values repaid debt and seized collateral at
//! the hourly USD price. The liquidator's bonus (seized − repaid, before gas)
//! is published as an estimate.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use num_bigint::BigUint;
use serde::Serialize;
use serde_json::{Value, json};
use sha3::{Digest, Keccak256};
use sqlx::PgPool;

use crate::{
    core::errors::{LedgerError, LedgerResult},
    indexer::rpc::IndexerRpcClient,
    services::market_data::NATIVE_ASSET_ADDRESS,
};

pub const LIQUIDATION_METHODOLOGY_VERSION: &str = "market_lending_liquidation_v1";
pub const MORPHO_LIQUIDATE_TOPIC: &str =
    "0xa4946ede45d0c6f06a0f5ce92c9ad3b4751452d2fe0e25010783bcab57a67e41";
const PROTOCOL: &str = "morpho_blue_compatible";
const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LiquidateLog {
    pub venue: String,
    pub market_id: String,
    pub liquidator: String,
    pub borrower: String,
    pub repaid_assets: BigUint,
    pub seized_assets: BigUint,
    pub bad_debt_assets: BigUint,
    pub tx_hash: String,
    pub log_index: u64,
    pub block_number: u64,
    pub block_time: Option<DateTime<Utc>>,
}

/// Decodes `Liquidate(Id indexed id, address indexed caller, address indexed
/// borrower, uint256 repaidAssets, uint256 repaidShares, uint256 seizedAssets,
/// uint256 badDebtAssets, uint256 badDebtShares)`.
pub fn decode_liquidate_log(log: &Value) -> Option<LiquidateLog> {
    let topics = log
        .get("topics")?
        .as_array()?
        .iter()
        .filter_map(Value::as_str)
        .collect::<Vec<_>>();
    if topics.len() != 4 || !topics[0].eq_ignore_ascii_case(MORPHO_LIQUIDATE_TOPIC) {
        return None;
    }
    let data = log.get("data")?.as_str()?.trim_start_matches("0x");
    if data.len() != 64 * 5 {
        return None;
    }
    let word =
        |index: usize| BigUint::parse_bytes(data[index * 64..(index + 1) * 64].as_bytes(), 16);
    Some(LiquidateLog {
        venue: log.get("address")?.as_str()?.to_ascii_lowercase(),
        market_id: topics[1].to_ascii_lowercase(),
        liquidator: word_address(topics[2]),
        borrower: word_address(topics[3]),
        repaid_assets: word(0)?,
        seized_assets: word(2)?,
        bad_debt_assets: word(3)?,
        tx_hash: log.get("transactionHash")?.as_str()?.to_ascii_lowercase(),
        log_index: hex_u64(log.get("logIndex")?)?,
        block_number: hex_u64(log.get("blockNumber")?)?,
        block_time: log
            .get("blockTimestamp")
            .and_then(hex_u64)
            .and_then(|seconds| DateTime::from_timestamp(seconds as i64, 0)),
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MarketParams {
    pub loan_token: String,
    pub collateral_token: String,
    pub oracle: String,
    pub irm: String,
    pub lltv: BigUint,
}

/// Decodes the static `MarketParams` tuple returned by `idToMarketParams`.
pub fn decode_market_params(hex: &str) -> Option<MarketParams> {
    let data = hex.trim_start_matches("0x");
    if data.len() < 64 * 5 {
        return None;
    }
    let address = |index: usize| word_address(&data[index * 64..(index + 1) * 64]);
    let params = MarketParams {
        loan_token: address(0),
        collateral_token: address(1),
        oracle: address(2),
        irm: address(3),
        lltv: BigUint::parse_bytes(data[4 * 64..5 * 64].as_bytes(), 16)?,
    };
    (params.loan_token != ZERO_ADDRESS && params.collateral_token != ZERO_ADDRESS).then_some(params)
}

pub fn id_to_market_params_selector() -> String {
    let digest = Keccak256::digest(b"idToMarketParams(bytes32)");
    format!("0x{}", hex::encode(&digest[..4]))
}

fn word_address(word: &str) -> String {
    let digits = word.trim_start_matches("0x");
    format!("0x{}", &digits[digits.len().saturating_sub(40)..]).to_ascii_lowercase()
}

fn hex_u64(value: &Value) -> Option<u64> {
    u64::from_str_radix(value.as_str()?.trim_start_matches("0x"), 16).ok()
}

#[derive(Clone, Debug)]
pub struct LiquidationIndexerConfig {
    pub chain_id: u64,
    pub start_block: u64,
    pub head_lag_blocks: u64,
    pub initial_range: u64,
    pub max_range: u64,
    pub min_range: u64,
}

impl Default for LiquidationIndexerConfig {
    fn default() -> Self {
        Self {
            chain_id: 4663,
            start_block: 0,
            head_lag_blocks: 4,
            initial_range: 250_000,
            max_range: 2_000_000,
            min_range: 2_000,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct LiquidationIteration {
    pub from_block: u64,
    pub to_block: u64,
    pub head_block: u64,
    pub logs: u64,
    pub accepted: u64,
    pub rejected: u64,
    pub range: u64,
}

impl LiquidationIteration {
    pub fn has_backlog(&self) -> bool {
        self.to_block < self.head_block
    }
}

pub struct LiquidationIndexer {
    pool: PgPool,
    rpc: IndexerRpcClient,
    config: LiquidationIndexerConfig,
    range: u64,
    selector: String,
    market_cache: BTreeMap<(String, String), Option<MarketParams>>,
}

impl LiquidationIndexer {
    pub fn new(pool: PgPool, rpc: IndexerRpcClient, config: LiquidationIndexerConfig) -> Self {
        Self {
            range: config.initial_range,
            pool,
            rpc,
            config,
            selector: id_to_market_params_selector(),
            market_cache: BTreeMap::new(),
        }
    }

    pub async fn process_once(&mut self) -> LedgerResult<LiquidationIteration> {
        let chain_id = self.config.chain_id as i64;
        let key = format!("liquidations:{}", self.config.chain_id);
        let head = self
            .rpc
            .get_latest_block_number()
            .await?
            .saturating_sub(self.config.head_lag_blocks);
        let cursor: Option<i64> = sqlx::query_scalar(
            "select last_position from market.projector_checkpoints where checkpoint_key = $1",
        )
        .bind(&key)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_error)?;
        let from = cursor
            .map(|block| block as u64 + 1)
            .unwrap_or(self.config.start_block);
        let mut iteration = LiquidationIteration {
            head_block: head,
            from_block: from,
            to_block: from.saturating_sub(1),
            range: self.range,
            ..LiquidationIteration::default()
        };
        if from > head {
            return Ok(iteration);
        }
        let to = (from + self.range - 1).min(head);
        let logs = match self
            .rpc
            .get_logs(json!({
                "fromBlock": format!("0x{from:x}"),
                "toBlock": format!("0x{to:x}"),
                "topics": [MORPHO_LIQUIDATE_TOPIC],
            }))
            .await
        {
            Ok(logs) => {
                if to - from + 1 >= self.range {
                    self.range = (self.range * 2).min(self.config.max_range);
                }
                logs
            }
            Err(error) if self.range > self.config.min_range => {
                // Large ranges time out on a busy node; retry smaller.
                self.range = (self.range / 4).max(self.config.min_range);
                tracing::warn!(error = %error, range = self.range, "liquidation log range reduced");
                return Ok(iteration);
            }
            Err(error) => return Err(error),
        };
        iteration.logs = logs.len() as u64;
        let mut tx = self.pool.begin().await.map_err(db_error)?;
        for log in &logs {
            let Some(liquidation) = decode_liquidate_log(log) else {
                iteration.rejected += 1;
                continue;
            };
            let Some(params) = self
                .market_params(&liquidation.venue, &liquidation.market_id)
                .await?
            else {
                iteration.rejected += 1;
                continue;
            };
            sqlx::query(
                r#"insert into market.lending_markets (
                     chain_id, venue_address, market_id, protocol, loan_token, collateral_token,
                     oracle, irm, lltv_raw
                   ) values ($1, $2, $3, $4, $5, $6, $7, $8, $9::numeric)
                   on conflict (chain_id, venue_address, market_id) do nothing"#,
            )
            .bind(chain_id)
            .bind(&liquidation.venue)
            .bind(&liquidation.market_id)
            .bind(PROTOCOL)
            .bind(&params.loan_token)
            .bind(&params.collateral_token)
            .bind(&params.oracle)
            .bind(&params.irm)
            .bind(params.lltv.to_string())
            .execute(&mut *tx)
            .await
            .map_err(db_error)?;
            sqlx::query(
                r#"insert into market.lending_liquidations (
                     chain_id, tx_hash, log_index, block_number, block_time, protocol, venue_address,
                     market_id, liquidator, borrower, loan_token, collateral_token, repaid_raw,
                     seized_raw, bad_debt_raw, status, reason, methodology_version
                   ) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13::numeric,
                             $14::numeric, $15::numeric, 'unknown', 'pending valuation', $16)
                   on conflict (chain_id, tx_hash, log_index) do nothing"#,
            )
            .bind(chain_id)
            .bind(&liquidation.tx_hash)
            .bind(liquidation.log_index as i64)
            .bind(liquidation.block_number as i64)
            .bind(liquidation.block_time)
            .bind(PROTOCOL)
            .bind(&liquidation.venue)
            .bind(&liquidation.market_id)
            .bind(&liquidation.liquidator)
            .bind(&liquidation.borrower)
            .bind(&params.loan_token)
            .bind(&params.collateral_token)
            .bind(liquidation.repaid_assets.to_string())
            .bind(liquidation.seized_assets.to_string())
            .bind(liquidation.bad_debt_assets.to_string())
            .bind(LIQUIDATION_METHODOLOGY_VERSION)
            .execute(&mut *tx)
            .await
            .map_err(db_error)?;
            for token in [&params.loan_token, &params.collateral_token] {
                sqlx::query(
                    r#"insert into market.price_assets (chain_id, asset_address, source)
                       values ($1, $2, 'lending_market')
                       on conflict (chain_id, asset_address) do nothing"#,
                )
                .bind(chain_id)
                .bind(token)
                .execute(&mut *tx)
                .await
                .map_err(db_error)?;
            }
            iteration.accepted += 1;
        }
        sqlx::query(
            r#"insert into market.projector_checkpoints (
                 checkpoint_key, last_position, last_run_at, last_success_at, updated_at
               ) values ($1, $2, now(), now(), now())
               on conflict (checkpoint_key) do update set
                 last_position = excluded.last_position, last_run_at = now(),
                 last_success_at = now(), last_error = null, updated_at = now()"#,
        )
        .bind(&key)
        .bind(to as i64)
        .execute(&mut *tx)
        .await
        .map_err(db_error)?;
        tx.commit().await.map_err(db_error)?;
        iteration.to_block = to;
        Ok(iteration)
    }

    /// Values liquidations whose USD amounts are still unknown.
    pub async fn value_pending(&self) -> LedgerResult<u64> {
        let result = sqlx::query(
            r#"with priced as (
                 select liquidation.chain_id, liquidation.tx_hash, liquidation.log_index,
                        liquidation.repaid_raw / power(10::numeric, loan_meta.decimals) as repaid,
                        liquidation.seized_raw / power(10::numeric, collateral_meta.decimals) as seized,
                        loan_price.price_usd as loan_usd,
                        collateral_price.price_usd as collateral_usd
                 from market.lending_liquidations liquidation
                 left join public.erc20_token_metadata loan_meta
                   on loan_meta.chain_id = liquidation.chain_id
                  and loan_meta.token_address = liquidation.loan_token
                 left join public.erc20_token_metadata collateral_meta
                   on collateral_meta.chain_id = liquidation.chain_id
                  and collateral_meta.token_address = liquidation.collateral_token
                 left join lateral (
                   select price_usd from market.asset_usd_prices price
                   where price.chain_id = liquidation.chain_id
                     and price.asset_address = liquidation.loan_token
                     and price.bucket_start <= liquidation.block_time
                     and price.bucket_start > liquidation.block_time - interval '48 hours'
                   order by price.bucket_start desc limit 1
                 ) loan_price on true
                 left join lateral (
                   select price_usd from market.asset_usd_prices price
                   where price.chain_id = liquidation.chain_id
                     and price.asset_address = liquidation.collateral_token
                     and price.bucket_start <= liquidation.block_time
                     and price.bucket_start > liquidation.block_time - interval '48 hours'
                   order by price.bucket_start desc limit 1
                 ) collateral_price on true
                 where liquidation.chain_id = $1 and liquidation.status = 'unknown'
                   and $2::text is not null
               )
               update market.lending_liquidations liquidation
               set repaid_amount = priced.repaid,
                   seized_amount = priced.seized,
                   repaid_usd = priced.repaid * priced.loan_usd,
                   seized_usd = priced.seized * priced.collateral_usd,
                   bonus_usd = priced.seized * priced.collateral_usd - priced.repaid * priced.loan_usd,
                   status = case when priced.loan_usd is not null and priced.collateral_usd is not null
                     and priced.repaid is not null and priced.seized is not null
                     then 'estimated' else 'unknown' end,
                   reason = case
                     when priced.repaid is null or priced.seized is null then 'token decimals unavailable'
                     when priced.loan_usd is null then 'loan token USD price unavailable'
                     when priced.collateral_usd is null then 'collateral USD price unavailable'
                   end,
                   computed_at = now()
               from priced
               where liquidation.chain_id = priced.chain_id and liquidation.tx_hash = priced.tx_hash
                 and liquidation.log_index = priced.log_index"#,
        )
        .bind(self.config.chain_id as i64)
        .bind(NATIVE_ASSET_ADDRESS)
        .execute(&self.pool)
        .await
        .map_err(db_error)?;
        Ok(result.rows_affected())
    }

    async fn market_params(
        &mut self,
        venue: &str,
        market_id: &str,
    ) -> LedgerResult<Option<MarketParams>> {
        let key = (venue.to_owned(), market_id.to_owned());
        if let Some(cached) = self.market_cache.get(&key) {
            return Ok(cached.clone());
        }
        let data = format!("{}{}", self.selector, market_id.trim_start_matches("0x"));
        let params = match self.rpc.eth_call(venue, &data).await {
            Ok(hex) => decode_market_params(&hex),
            Err(LedgerError::TransientIngestion(message)) if message.contains("revert") => None,
            Err(error) => return Err(error),
        };
        self.market_cache.insert(key, params.clone());
        Ok(params)
    }
}

fn db_error(error: impl std::fmt::Display) -> LedgerError {
    LedgerError::Persistence(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn real_log() -> Value {
        json!({
            "address": "0xF0a0A33729270586cdD66010b1cEDe649745C3a5",
            "topics": [
                MORPHO_LIQUIDATE_TOPIC,
                "0x7b0a7e894164df14f0422c18c218f6104c5ca5a626c33e2f0e9f77189bbd42f2",
                "0x000000000000000000000000f68fa710d2072d0f85f78b415be77dff15423dab",
                "0x000000000000000000000000a2ad64b2d890a59ce18c0ddd51a7de9ef69eb3da"
            ],
            "data": "0x00000000000000000000000000000000000000000000000000000000015393e50000000000000000000000000000000000000000000000000000143d7ad9748100000000000000000000000000000000000000000000000000fb6c4af841d93c00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
            "blockNumber": "0x3cf2b4e",
            "transactionHash": "0x048095ad0e614bc34b6308929659118c76a059a290365ceb1cb6a5163fc1bc9a",
            "logIndex": "0x2e",
            "blockTimestamp": "0x6aa99efe"
        })
    }

    #[test]
    fn decodes_a_robinhood_chain_liquidation() {
        let decoded = decode_liquidate_log(&real_log()).expect("valid log");
        assert_eq!(decoded.venue, "0xf0a0a33729270586cdd66010b1cede649745c3a5");
        assert_eq!(
            decoded.liquidator,
            "0xf68fa710d2072d0f85f78b415be77dff15423dab"
        );
        assert_eq!(
            decoded.borrower,
            "0xa2ad64b2d890a59ce18c0ddd51a7de9ef69eb3da"
        );
        assert_eq!(decoded.repaid_assets, BigUint::from(0x015393e5_u64));
        assert_eq!(decoded.seized_assets, BigUint::from(0x00fb6c4af841d93c_u64));
        assert_eq!(decoded.bad_debt_assets, BigUint::from(0_u8));
        assert_eq!(decoded.block_number, 0x3cf2b4e);
        assert_eq!(decoded.log_index, 0x2e);
        assert!(decoded.block_time.is_some());
    }

    #[test]
    fn rejects_foreign_topics_and_truncated_data() {
        let mut log = real_log();
        log["topics"][0] = json!("0x00");
        assert!(decode_liquidate_log(&log).is_none());
        let mut log = real_log();
        log["data"] = json!("0x01");
        assert!(decode_liquidate_log(&log).is_none());
    }

    #[test]
    fn decodes_market_params_and_rejects_empty_markets() {
        let hex = "0x0000000000000000000000005fc5360d0400a0fd4f2af552add042d716f1d168000000000000000000000000322f0929c4625ed5bad873c95208d54e1c003b2d000000000000000000000000b2df94538ba8c6273a528617a9d16e48ee082dbc0000000000000000000000009316e7edab60ef0f489b1e4d11ddcdaa560acbfd00000000000000000000000000000000000000000000000008ac7230489e8000";
        let params = decode_market_params(hex).expect("market");
        assert_eq!(
            params.loan_token,
            "0x5fc5360d0400a0fd4f2af552add042d716f1d168"
        );
        assert_eq!(
            params.collateral_token,
            "0x322f0929c4625ed5bad873c95208d54e1c003b2d"
        );
        assert_eq!(params.lltv, BigUint::from(625_000_000_000_000_000_u64));
        assert!(decode_market_params(&format!("0x{}", "0".repeat(320))).is_none());
    }

    #[test]
    fn selector_matches_morpho_blue() {
        assert_eq!(id_to_market_params_selector(), "0x2c3c9157");
    }
}
