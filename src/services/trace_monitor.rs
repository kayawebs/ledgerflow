use std::collections::{BTreeMap, BTreeSet};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicI64, Ordering},
};

use chrono::{DateTime, Duration, NaiveDateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::{Row, types::Json};
use tracing::{debug, warn};
use uuid::Uuid;

use crate::{
    core::errors::{LedgerError, LedgerResult},
    domain::{enums::AssetType, interfaces::AnalysisRepository, value_objects::AssetFlow},
    flows::derivation::derive_basic_flows,
    indexer::rpc::IndexerRpcClient,
    ingestion::rpc::{HttpRpcClient, RpcClient},
    ingestion::{fetchers::TransactionBundleFetcher, models::RawTransactionBundle},
    repositories::sqlx::SqlxAnalysisRepository,
};

const CLEANUP_INTERVAL_SECS: i64 = 15 * 60;
const CLEANUP_BATCH_ROWS: i64 = 10_000;
const UNISWAP_V2_SWAP_TOPIC: &str =
    "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822";
const UNISWAP_V3_SWAP_TOPIC: &str =
    "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67";
const DEX_SWAP_TOPICS: [&str; 2] = [UNISWAP_V2_SWAP_TOPIC, UNISWAP_V3_SWAP_TOPIC];
const ERC20_TRANSFER_TOPIC_HEX: &str =
    "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
const BASE_USDC: &str = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913";
const BASE_USDBC: &str = "0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca";
const FAST_ARBITRAGE_SOURCE_RPC_LOGS: &str = "rpc_logs";
const FAST_ARBITRAGE_SOURCE_CRYPTOHOUSE: &str = "cryptohouse";
const DEFAULT_CRYPTOHOUSE_URL: &str = "https://crypto-clickhouse.clickhouse.com/?user=crypto";
const FAST_ARBITRAGE_RPC_ENRICHMENT_MAX_PER_RANGE: usize = 16;

#[derive(Clone, Debug)]
pub struct TraceMonitorConfig {
    pub chain_id: u64,
    pub worker_id: String,
    pub start_block: Option<u64>,
    pub confirmations: u64,
    pub block_batch_size: u64,
    pub retention_days: i64,
    pub fast_arbitrage_enabled: bool,
    pub fast_arbitrage_source: String,
    pub fast_arbitrage_min_swaps: u32,
    pub fast_arbitrage_source_lag_blocks: u64,
    pub cryptohouse_url: String,
    pub light_deltas_enabled: bool,
}

#[derive(Clone, Debug, Default)]
pub struct TraceMonitorIteration {
    pub latest_block: u64,
    pub target_block: u64,
    pub start_block: Option<u64>,
    pub end_block: Option<u64>,
    pub processed_blocks: u64,
    pub processed_txs: u64,
    pub failed_txs: u64,
    pub address_delta_rows: u64,
    pub cluster_delta_rows: u64,
    pub candidate_rows: u64,
    pub skipped_blocks: u64,
}

#[derive(Clone, Debug, Default)]
struct DeltaAccumulator {
    amount_in: i128,
    amount_out: i128,
    flow_count: u32,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct AddressDeltaKey {
    address: String,
    asset_type: String,
    token_address: String,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ClusterDeltaKey {
    cluster_id: Uuid,
    asset_type: String,
    token_address: String,
}

#[derive(Clone, Debug)]
struct LightMonitorCandidate {
    kind: &'static str,
    score: i32,
    dex_swap_count: u32,
    liquidation_event_count: u32,
    metadata: Value,
}

#[derive(Clone, Debug, Default)]
struct FastArbitrageScan {
    swap_logs: u64,
    candidate_txs: u64,
    candidate_rows: u64,
}

#[derive(Clone, Debug)]
struct FastSwapAccumulator {
    tx_hash: String,
    block_number: u64,
    tx_index: Option<u64>,
    sender_address: Option<String>,
    entry_contract: Option<String>,
    swap_log_count: u32,
    v2_swap_count: u32,
    v3_swap_count: u32,
    pools: BTreeSet<String>,
    pool_stats: BTreeMap<String, FastPoolStat>,
    log_indexes: Vec<u64>,
    confirmation: FastArbitrageConfirmation,
}

#[derive(Clone, Debug, Default)]
struct FastPoolStat {
    pool_address: String,
    first_log_index: u64,
    swap_log_count: u32,
    v2_swap_count: u32,
    v3_swap_count: u32,
}

#[derive(Clone, Debug)]
struct FastArbitrageConfirmation {
    status: String,
    profit_recipient: Option<String>,
    profit_token_address: Option<String>,
    profit_amount_raw: Option<String>,
    metadata: Value,
}

#[derive(Clone, Debug)]
struct Erc20TransferLite {
    tx_hash: String,
    token_address: String,
    from_address: String,
    to_address: String,
    amount_raw: i128,
}

impl Default for FastArbitrageConfirmation {
    fn default() -> Self {
        Self {
            status: "candidate".to_owned(),
            profit_recipient: None,
            profit_token_address: None,
            profit_amount_raw: None,
            metadata: json!({
                "method": "not_confirmed",
                "reason": "candidate has not met conservative stable-surplus confirmation"
            }),
        }
    }
}

impl FastSwapAccumulator {
    fn record_swap_log(&mut self, pool: String, topic0: &str, log_index: u64) {
        self.swap_log_count = self.swap_log_count.saturating_add(1);
        self.pools.insert(pool.clone());
        self.log_indexes.push(log_index);
        let pool_stat = self
            .pool_stats
            .entry(pool.clone())
            .or_insert_with(|| FastPoolStat {
                pool_address: pool,
                first_log_index: log_index,
                ..Default::default()
            });
        pool_stat.first_log_index = pool_stat.first_log_index.min(log_index);
        pool_stat.swap_log_count = pool_stat.swap_log_count.saturating_add(1);
        if topic0 == UNISWAP_V2_SWAP_TOPIC {
            self.v2_swap_count = self.v2_swap_count.saturating_add(1);
            pool_stat.v2_swap_count = pool_stat.v2_swap_count.saturating_add(1);
        } else if topic0 == UNISWAP_V3_SWAP_TOPIC {
            self.v3_swap_count = self.v3_swap_count.saturating_add(1);
            pool_stat.v3_swap_count = pool_stat.v3_swap_count.saturating_add(1);
        }
    }

    fn pool_addresses(&self) -> Vec<String> {
        self.route_pool_stats()
            .into_iter()
            .map(|pool| pool.pool_address)
            .collect()
    }

    fn route_pool_stats(&self) -> Vec<FastPoolStat> {
        let mut pools = self.pool_stats.values().cloned().collect::<Vec<_>>();
        pools.sort_by(|left, right| {
            left.first_log_index
                .cmp(&right.first_log_index)
                .then_with(|| left.pool_address.cmp(&right.pool_address))
        });
        pools
    }
}

#[derive(Clone)]
pub struct TraceMonitorService {
    repository: SqlxAnalysisRepository,
    rpc_client: Arc<IndexerRpcClient>,
    block_receipt_rpc: Arc<HttpRpcClient>,
    cryptohouse_client: reqwest::Client,
    light_fetcher: TransactionBundleFetcher,
    config: TraceMonitorConfig,
    startup_anchor_initialized: Arc<AtomicBool>,
    next_cleanup_at_unix: Arc<AtomicI64>,
}

#[derive(Clone, Debug)]
struct BlockTxRef {
    tx_hash: String,
    tx_payload: Option<Value>,
    receipt_payload: Option<Value>,
    block_timestamp: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct TopAddressDeltasQuery {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub top_n: Option<u32>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct RecentArbitrageQuery {
    pub chain_id: Option<u64>,
    pub minutes: Option<i64>,
    pub limit: Option<u32>,
    pub min_swaps: Option<u32>,
    pub confirmed_only: Option<bool>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct HotArbitragePoolsQuery {
    pub chain_id: Option<u64>,
    pub minutes: Option<i64>,
    pub limit: Option<u32>,
    pub min_txs: Option<u32>,
}

#[derive(Clone, Debug, Serialize)]
pub struct RecentArbitrageCandidate {
    pub tx_hash: String,
    pub block_number: u64,
    pub block_timestamp: Option<DateTime<Utc>>,
    pub detection_source: String,
    pub sender_address: Option<String>,
    pub entry_contract: Option<String>,
    pub score: i32,
    pub swap_log_count: u32,
    pub distinct_pool_count: u32,
    pub route: String,
    pub route_fingerprint: String,
    pub pool_addresses: Vec<String>,
    pub confirmation_status: String,
    pub profit_recipient: Option<String>,
    pub profit_token_address: Option<String>,
    pub profit_amount_raw: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct HotArbitragePool {
    pub pool_address: String,
    pub tx_count: u64,
    pub swap_log_count: u64,
    pub unique_senders: u64,
    pub unique_routes: u64,
    pub first_seen: Option<DateTime<Utc>>,
    pub last_seen: Option<DateTime<Utc>>,
    pub sample_tx_hashes: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct HotArbitrageRoute {
    pub route_fingerprint: String,
    pub route: String,
    pub tx_count: u64,
    pub swap_log_count: u64,
    pub unique_senders: u64,
    pub first_seen: Option<DateTime<Utc>>,
    pub last_seen: Option<DateTime<Utc>>,
    pub sample_tx_hashes: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct TopAddressDelta {
    pub address: String,
    pub asset_type: String,
    pub token_address: String,
    pub net_raw: String,
    pub amount_in_raw: String,
    pub amount_out_raw: String,
    pub tx_count: u64,
    pub tx_hashes: Vec<String>,
}

impl TraceMonitorService {
    pub fn new(
        repository: SqlxAnalysisRepository,
        rpc_client: Arc<IndexerRpcClient>,
        block_receipt_rpc: Arc<HttpRpcClient>,
        light_fetcher: TransactionBundleFetcher,
        config: TraceMonitorConfig,
    ) -> Self {
        Self {
            repository,
            rpc_client,
            block_receipt_rpc,
            cryptohouse_client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(15))
                .build()
                .unwrap_or_else(|_| reqwest::Client::new()),
            light_fetcher,
            config,
            startup_anchor_initialized: Arc::new(AtomicBool::new(false)),
            next_cleanup_at_unix: Arc::new(AtomicI64::new(
                Utc::now().timestamp().saturating_add(CLEANUP_INTERVAL_SECS),
            )),
        }
    }

    pub async fn process_once(&self) -> LedgerResult<TraceMonitorIteration> {
        let latest_block = self.rpc_client.get_latest_block_number().await?;
        let target_block = latest_block
            .saturating_sub(self.config.confirmations)
            .saturating_sub(self.fast_arbitrage_source_lag_blocks());
        let skip_history_on_startup = !self.startup_anchor_initialized.swap(true, Ordering::SeqCst);
        let last_processed = if skip_history_on_startup {
            None
        } else {
            self.load_last_processed_block().await?
        };
        let (start_block, skipped_blocks) = next_start_block(
            skip_history_on_startup,
            last_processed,
            self.config.start_block,
            target_block,
            self.config.block_batch_size.max(1),
        );

        let mut iteration = TraceMonitorIteration {
            latest_block,
            target_block,
            start_block: Some(start_block),
            skipped_blocks,
            ..Default::default()
        };

        if start_block > target_block {
            iteration.start_block = None;
            self.cleanup_expired_rows_if_due().await?;
            return Ok(iteration);
        }

        let end_block = target_block.min(
            start_block
                .saturating_add(self.config.block_batch_size.max(1))
                .saturating_sub(1),
        );
        iteration.end_block = Some(end_block);

        if self.config.fast_arbitrage_enabled {
            let fast_scan = self
                .process_fast_arbitrage_range(start_block, end_block)
                .await?;
            debug!(
                start_block,
                end_block,
                swap_logs = fast_scan.swap_logs,
                candidate_txs = fast_scan.candidate_txs,
                candidate_rows = fast_scan.candidate_rows,
                "trace monitor fast arbitrage scan completed"
            );
            iteration.processed_txs = iteration
                .processed_txs
                .saturating_add(fast_scan.candidate_txs);
            iteration.candidate_rows = iteration
                .candidate_rows
                .saturating_add(fast_scan.candidate_rows);
        }

        if !self.config.light_deltas_enabled {
            self.upsert_progress(Some(end_block)).await?;
            iteration.processed_blocks = end_block.saturating_sub(start_block).saturating_add(1);
            self.cleanup_expired_rows_if_due().await?;
            return Ok(iteration);
        }

        let clusters = self.load_cluster_scopes().await?;
        for block_number in start_block..=end_block {
            let tx_refs = self.block_tx_refs(block_number).await?;
            for tx_ref in tx_refs {
                let tx_hash = tx_ref.tx_hash.clone();
                match self.process_light_tx(tx_ref, &clusters).await {
                    Ok((address_rows, cluster_rows, candidate_rows)) => {
                        iteration.processed_txs += 1;
                        iteration.address_delta_rows += address_rows;
                        iteration.cluster_delta_rows += cluster_rows;
                        iteration.candidate_rows += candidate_rows;
                    }
                    Err(err) => {
                        iteration.failed_txs += 1;
                        warn!(
                            tx_hash,
                            error = %err,
                            "trace monitor light tx failed"
                        );
                    }
                }
            }
            self.upsert_progress(Some(block_number)).await?;
            iteration.processed_blocks += 1;
        }

        self.cleanup_expired_rows_if_due().await?;
        Ok(iteration)
    }

    pub async fn anchor_to_live_start(&self) -> LedgerResult<(u64, u64)> {
        let latest_block = self.rpc_client.get_latest_block_number().await?;
        let target_block = latest_block
            .saturating_sub(self.config.confirmations)
            .saturating_sub(self.fast_arbitrage_source_lag_blocks());
        self.upsert_progress(Some(target_block)).await?;
        self.startup_anchor_initialized
            .store(true, Ordering::SeqCst);
        Ok((latest_block, target_block))
    }

    async fn process_light_tx(
        &self,
        tx_ref: BlockTxRef,
        clusters: &[ClusterScope],
    ) -> LedgerResult<(u64, u64, u64)> {
        let bundle = match (tx_ref.tx_payload, tx_ref.receipt_payload) {
            (Some(tx_payload), Some(receipt_payload)) => {
                self.light_fetcher
                    .fetch_without_code_from_payloads_with_timestamp(
                        self.config.chain_id,
                        tx_payload,
                        receipt_payload,
                        tx_ref.block_timestamp,
                    )
                    .await?
            }
            (Some(tx_payload), None) => {
                self.light_fetcher
                    .fetch_without_code_from_tx_payload_with_timestamp(
                        self.config.chain_id,
                        tx_payload,
                        tx_ref.block_timestamp,
                    )
                    .await?
            }
            (None, _) => {
                self.light_fetcher
                    .fetch_without_code_with_timestamp(
                        self.config.chain_id,
                        &tx_ref.tx_hash,
                        tx_ref.block_timestamp,
                    )
                    .await?
            }
        };
        let flows = derive_basic_flows(&bundle);
        let candidates = detect_light_candidates(&bundle, &flows);
        let address_deltas = aggregate_address_deltas(&flows);
        let address_rows = self
            .upsert_address_deltas(
                &bundle.tx.tx_hash,
                bundle.tx.block_number.unwrap_or_default(),
                bundle.tx.block_timestamp,
                &bundle.tx.from_address,
                bundle.tx.to_address.as_deref(),
                bundle.trace.is_some(),
                &address_deltas,
            )
            .await?;

        let cluster_deltas = aggregate_cluster_deltas(&flows, clusters);
        let cluster_rows = self
            .upsert_cluster_deltas(
                &bundle.tx.tx_hash,
                bundle.tx.block_number.unwrap_or_default(),
                bundle.tx.block_timestamp,
                bundle.trace.is_some(),
                &cluster_deltas,
            )
            .await?;
        let candidate_rows = self.upsert_candidates(&bundle, &candidates).await?;
        Ok((address_rows, cluster_rows, candidate_rows))
    }

    async fn process_fast_arbitrage_range(
        &self,
        start_block: u64,
        end_block: u64,
    ) -> LedgerResult<FastArbitrageScan> {
        match self.fast_arbitrage_source().as_str() {
            FAST_ARBITRAGE_SOURCE_CRYPTOHOUSE => {
                self.process_cryptohouse_arbitrage_range(start_block, end_block)
                    .await
            }
            _ => {
                self.process_rpc_log_arbitrage_range(start_block, end_block)
                    .await
            }
        }
    }

    async fn process_rpc_log_arbitrage_range(
        &self,
        start_block: u64,
        end_block: u64,
    ) -> LedgerResult<FastArbitrageScan> {
        let logs = self
            .block_receipt_rpc
            .get_logs_by_topics(start_block, end_block, &DEX_SWAP_TOPICS)
            .await?;
        let min_swaps = self.config.fast_arbitrage_min_swaps.max(2);
        let (swap_logs, mut candidates) = fast_arbitrage_candidates_from_logs(logs, min_swaps);
        self.enrich_rpc_log_arbitrage_endpoints(&mut candidates)
            .await;
        let mut block_timestamps = BTreeMap::<u64, Option<DateTime<Utc>>>::new();
        let mut candidate_rows = 0_u64;
        let mut candidate_txs = 0_u64;
        for candidate in &candidates {
            let block_timestamp =
                if let Some(timestamp) = block_timestamps.get(&candidate.block_number) {
                    *timestamp
                } else {
                    let timestamp = self.block_timestamp(candidate.block_number).await?;
                    block_timestamps.insert(candidate.block_number, timestamp);
                    timestamp
                };
            candidate_rows = candidate_rows.saturating_add(
                self.upsert_fast_arbitrage_candidate(
                    candidate,
                    block_timestamp,
                    "rpc_logs_swap_topics",
                )
                .await?,
            );
            candidate_txs = candidate_txs.saturating_add(1);
        }

        Ok(FastArbitrageScan {
            swap_logs,
            candidate_txs,
            candidate_rows,
        })
    }

    async fn enrich_rpc_log_arbitrage_endpoints(&self, candidates: &mut [FastSwapAccumulator]) {
        for candidate in candidates
            .iter_mut()
            .take(FAST_ARBITRAGE_RPC_ENRICHMENT_MAX_PER_RANGE)
        {
            match self
                .rpc_client
                .get_transaction_by_hash(&candidate.tx_hash)
                .await
            {
                Ok(transaction) => {
                    candidate.sender_address = transaction
                        .get("from")
                        .and_then(Value::as_str)
                        .map(|value| value.to_ascii_lowercase());
                    candidate.entry_contract = transaction
                        .get("to")
                        .and_then(Value::as_str)
                        .map(|value| value.to_ascii_lowercase());
                    candidate.tx_index = candidate.tx_index.or_else(|| {
                        transaction
                            .get("transactionIndex")
                            .and_then(Value::as_str)
                            .and_then(parse_hex_u64)
                    });
                }
                Err(error) => {
                    warn!(
                        tx_hash = %candidate.tx_hash,
                        error = %error,
                        "could not enrich rpc-log arbitrage candidate endpoints"
                    );
                }
            }
        }
    }

    async fn process_cryptohouse_arbitrage_range(
        &self,
        start_block: u64,
        end_block: u64,
    ) -> LedgerResult<FastArbitrageScan> {
        let min_swaps = self.config.fast_arbitrage_min_swaps.max(2);
        let query = cryptohouse_fast_arbitrage_query(start_block, end_block, min_swaps);
        let response = self
            .cryptohouse_client
            .post(self.cryptohouse_url())
            .body(query)
            .send()
            .await
            .map_err(|err| LedgerError::TransientIngestion(err.to_string()))?;
        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|err| LedgerError::TransientIngestion(err.to_string()))?;
        if !status.is_success() {
            return Err(LedgerError::TransientIngestion(format!(
                "cryptohouse query failed with status {status}: {}",
                truncate_for_error(&body, 500)
            )));
        }

        let (swap_logs, mut candidates) = fast_arbitrage_candidates_from_cryptohouse_rows(&body)?;
        self.enrich_cryptohouse_arbitrage_confirmations(start_block, end_block, &mut candidates)
            .await?;
        let mut candidate_rows = 0_u64;
        let mut candidate_txs = 0_u64;
        for (candidate, block_timestamp) in &candidates {
            candidate_rows = candidate_rows.saturating_add(
                self.upsert_fast_arbitrage_candidate(
                    candidate,
                    *block_timestamp,
                    "cryptohouse_swap_topics",
                )
                .await?,
            );
            candidate_txs = candidate_txs.saturating_add(1);
        }

        Ok(FastArbitrageScan {
            swap_logs,
            candidate_txs,
            candidate_rows,
        })
    }

    async fn enrich_cryptohouse_arbitrage_confirmations(
        &self,
        start_block: u64,
        end_block: u64,
        candidates: &mut [(FastSwapAccumulator, Option<DateTime<Utc>>)],
    ) -> LedgerResult<()> {
        if candidates.is_empty() {
            return Ok(());
        }
        let tx_hashes = candidates
            .iter()
            .map(|(candidate, _)| sql_string(&candidate.tx_hash))
            .collect::<Vec<_>>()
            .join(",");
        let query = cryptohouse_transfer_logs_query(start_block, end_block, &tx_hashes);
        let response = self
            .cryptohouse_client
            .post(self.cryptohouse_url())
            .body(query)
            .send()
            .await
            .map_err(|err| LedgerError::TransientIngestion(err.to_string()))?;
        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|err| LedgerError::TransientIngestion(err.to_string()))?;
        if !status.is_success() {
            return Err(LedgerError::TransientIngestion(format!(
                "cryptohouse transfer query failed with status {status}: {}",
                truncate_for_error(&body, 500)
            )));
        }

        let transfers = erc20_transfers_from_cryptohouse_rows(&body)?;
        let mut by_tx = BTreeMap::<String, Vec<Erc20TransferLite>>::new();
        for transfer in transfers {
            by_tx
                .entry(transfer.tx_hash.clone())
                .or_default()
                .push(transfer);
        }
        for (candidate, _) in candidates {
            candidate.confirmation =
                conservative_arbitrage_confirmation(candidate, by_tx.get(&candidate.tx_hash));
        }
        Ok(())
    }

    fn fast_arbitrage_source(&self) -> String {
        match self
            .config
            .fast_arbitrage_source
            .to_ascii_lowercase()
            .as_str()
        {
            FAST_ARBITRAGE_SOURCE_CRYPTOHOUSE => FAST_ARBITRAGE_SOURCE_CRYPTOHOUSE.to_owned(),
            FAST_ARBITRAGE_SOURCE_RPC_LOGS | "" => FAST_ARBITRAGE_SOURCE_RPC_LOGS.to_owned(),
            other => {
                warn!(
                    configured_source = other,
                    fallback_source = FAST_ARBITRAGE_SOURCE_RPC_LOGS,
                    "unknown trace monitor fast arbitrage source"
                );
                FAST_ARBITRAGE_SOURCE_RPC_LOGS.to_owned()
            }
        }
    }

    fn fast_arbitrage_source_lag_blocks(&self) -> u64 {
        if self.config.fast_arbitrage_enabled
            && self.fast_arbitrage_source() == FAST_ARBITRAGE_SOURCE_CRYPTOHOUSE
        {
            self.config.fast_arbitrage_source_lag_blocks
        } else {
            0
        }
    }

    fn cryptohouse_url(&self) -> &str {
        let url = self.config.cryptohouse_url.trim();
        if url.is_empty() {
            DEFAULT_CRYPTOHOUSE_URL
        } else {
            url
        }
    }

    async fn block_timestamp(&self, block_number: u64) -> LedgerResult<Option<DateTime<Utc>>> {
        let block = self
            .block_receipt_rpc
            .get_block(self.config.chain_id, block_number)
            .await?;
        Ok(block
            .get("timestamp")
            .and_then(Value::as_str)
            .and_then(parse_hex_u64)
            .and_then(|value| Utc.timestamp_opt(value as i64, 0).single()))
    }

    pub async fn top_address_deltas(
        &self,
        query: &TopAddressDeltasQuery,
    ) -> LedgerResult<Vec<TopAddressDelta>> {
        let top_n = query.top_n.unwrap_or(25).clamp(1, 200);
        let rows = sqlx::query(
            r#"
            with top_addresses as (
              select
                address,
                asset_type,
                token_address,
                sum(net_raw)::text as net_raw,
                sum(amount_in_raw)::text as amount_in_raw,
                sum(amount_out_raw)::text as amount_out_raw,
                count(distinct tx_hash)::bigint as tx_count
              from trace_address_asset_deltas
              where chain_id = $1
                and block_timestamp >= $2
                and block_timestamp <= $3
                and net_raw > 0
              group by address, asset_type, token_address
              order by sum(net_raw) desc
              limit $4
            )
            select
              top_addresses.address,
              top_addresses.asset_type,
              top_addresses.token_address,
              top_addresses.net_raw,
              top_addresses.amount_in_raw,
              top_addresses.amount_out_raw,
              top_addresses.tx_count,
              array(
                select d.tx_hash
                from trace_address_asset_deltas d
                where d.chain_id = $1
                  and d.block_timestamp >= $2
                  and d.block_timestamp <= $3
                  and d.address = top_addresses.address
                  and d.asset_type = top_addresses.asset_type
                  and d.token_address = top_addresses.token_address
                  and d.net_raw > 0
                order by d.net_raw desc, d.block_number desc
                limit 50
              ) as tx_hashes
            from top_addresses
            "#,
        )
        .bind(self.config.chain_id as i64)
        .bind(query.start)
        .bind(query.end)
        .bind(top_n as i64)
        .fetch_all(self.repository.pool())
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;

        rows.into_iter()
            .map(|row| {
                Ok(TopAddressDelta {
                    address: row
                        .try_get("address")
                        .map_err(|err| LedgerError::Persistence(err.to_string()))?,
                    asset_type: row
                        .try_get("asset_type")
                        .map_err(|err| LedgerError::Persistence(err.to_string()))?,
                    token_address: row
                        .try_get("token_address")
                        .map_err(|err| LedgerError::Persistence(err.to_string()))?,
                    net_raw: row
                        .try_get("net_raw")
                        .map_err(|err| LedgerError::Persistence(err.to_string()))?,
                    amount_in_raw: row
                        .try_get("amount_in_raw")
                        .map_err(|err| LedgerError::Persistence(err.to_string()))?,
                    amount_out_raw: row
                        .try_get("amount_out_raw")
                        .map_err(|err| LedgerError::Persistence(err.to_string()))?,
                    tx_count: row
                        .try_get::<i64, _>("tx_count")
                        .map(|value| value as u64)
                        .map_err(|err| LedgerError::Persistence(err.to_string()))?,
                    tx_hashes: row
                        .try_get::<Vec<String>, _>("tx_hashes")
                        .map_err(|err| LedgerError::Persistence(err.to_string()))?,
                })
            })
            .collect()
    }

    async fn block_tx_refs(&self, block_number: u64) -> LedgerResult<Vec<BlockTxRef>> {
        let block = self
            .rpc_client
            .get_block_with_transactions(block_number)
            .await?;
        let block_timestamp = block
            .get("timestamp")
            .and_then(Value::as_str)
            .and_then(|value| u64::from_str_radix(value.trim_start_matches("0x"), 16).ok())
            .and_then(|value| Utc.timestamp_opt(value as i64, 0).single());
        let receipts_by_hash = match self
            .block_receipt_rpc
            .get_block_receipts(block_number)
            .await
        {
            Ok(receipts) => receipts
                .into_iter()
                .filter_map(|receipt| {
                    let tx_hash = receipt
                        .get("transactionHash")
                        .and_then(Value::as_str)?
                        .to_ascii_lowercase();
                    Some((tx_hash, receipt))
                })
                .collect::<BTreeMap<_, _>>(),
            Err(err) => {
                debug!(
                    block_number,
                    error = %err,
                    "block receipts unavailable; falling back to per-transaction receipts"
                );
                BTreeMap::new()
            }
        };
        Ok(block
            .get("transactions")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(|item| {
                        let tx_hash = item.as_str().map(ToOwned::to_owned).or_else(|| {
                            item.get("hash")
                                .and_then(Value::as_str)
                                .map(ToOwned::to_owned)
                        })?;
                        let tx_payload = item.as_object().map(|_| item.clone());
                        let receipt_payload =
                            receipts_by_hash.get(&tx_hash.to_ascii_lowercase()).cloned();
                        Some(BlockTxRef {
                            tx_hash,
                            tx_payload,
                            receipt_payload,
                            block_timestamp,
                        })
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default())
    }

    async fn load_last_processed_block(&self) -> LedgerResult<Option<u64>> {
        let row = sqlx::query(
            "select last_processed_block from trace_monitor_progress where worker_id = $1",
        )
        .bind(&self.config.worker_id)
        .fetch_optional(self.repository.pool())
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        row.map(|row| {
            row.try_get::<Option<i64>, _>("last_processed_block")
                .map(|value| value.map(|block| block as u64))
                .map_err(|err| LedgerError::Persistence(err.to_string()))
        })
        .transpose()
        .map(Option::flatten)
    }

    async fn upsert_progress(&self, block_number: Option<u64>) -> LedgerResult<()> {
        sqlx::query(
            r#"
            insert into trace_monitor_progress (
              worker_id, chain_id, last_processed_block, last_processed_at, metadata, created_at, updated_at
            )
            values ($1, $2, $3, now(), $4, now(), now())
            on conflict (worker_id) do update set
              chain_id = excluded.chain_id,
              last_processed_block = excluded.last_processed_block,
              last_processed_at = excluded.last_processed_at,
              metadata = excluded.metadata,
              updated_at = now()
            "#,
        )
        .bind(&self.config.worker_id)
        .bind(self.config.chain_id as i64)
        .bind(block_number.map(|value| value as i64))
        .bind(Json(json!({
            "confirmations": self.config.confirmations,
            "block_batch_size": self.config.block_batch_size,
            "retention_days": self.config.retention_days,
        })))
        .execute(self.repository.pool())
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        Ok(())
    }

    async fn load_cluster_scopes(&self) -> LedgerResult<Vec<ClusterScope>> {
        let clusters = self.repository.list_clusters().await?;
        Ok(clusters
            .into_iter()
            .filter(|cluster| cluster.chain_id == self.config.chain_id)
            .map(|cluster| ClusterScope {
                cluster_id: cluster.cluster_id,
                addresses: cluster
                    .addresses
                    .into_iter()
                    .map(|address| address.address.to_ascii_lowercase())
                    .collect(),
            })
            .collect())
    }

    async fn upsert_address_deltas(
        &self,
        tx_hash: &str,
        block_number: u64,
        block_timestamp: Option<chrono::DateTime<Utc>>,
        tx_from: &str,
        tx_to: Option<&str>,
        trace_available: bool,
        deltas: &BTreeMap<AddressDeltaKey, DeltaAccumulator>,
    ) -> LedgerResult<u64> {
        let mut rows = 0;
        for (key, delta) in deltas {
            let net = delta.amount_in.saturating_sub(delta.amount_out);
            sqlx::query(
                r#"
                insert into trace_address_asset_deltas (
                  chain_id, tx_hash, block_number, block_timestamp, address, asset_type, token_address,
                  amount_in_raw, amount_out_raw, net_raw, flow_count, metadata, created_at, updated_at
                )
                values ($1,$2,$3,$4,$5,$6,$7,$8::numeric,$9::numeric,$10::numeric,$11,$12,now(),now())
                on conflict (chain_id, tx_hash, address, asset_type, token_address) do update set
                  block_number = excluded.block_number,
                  block_timestamp = excluded.block_timestamp,
                  amount_in_raw = excluded.amount_in_raw,
                  amount_out_raw = excluded.amount_out_raw,
                  net_raw = excluded.net_raw,
                  flow_count = excluded.flow_count,
                  metadata = excluded.metadata,
                  updated_at = now()
                "#,
            )
            .bind(self.config.chain_id as i64)
            .bind(tx_hash)
            .bind(block_number as i64)
            .bind(block_timestamp)
            .bind(&key.address)
            .bind(&key.asset_type)
            .bind(&key.token_address)
            .bind(delta.amount_in.to_string())
            .bind(delta.amount_out.to_string())
            .bind(net.to_string())
            .bind(delta.flow_count as i32)
            .bind(Json(json!({
                "tx_from": tx_from.to_ascii_lowercase(),
                "tx_to": tx_to.map(str::to_ascii_lowercase),
                "trace_available": trace_available,
            })))
            .execute(self.repository.pool())
            .await
            .map_err(|err| LedgerError::Persistence(err.to_string()))?;
            rows += 1;
        }
        Ok(rows)
    }

    pub async fn recent_arbitrage(
        &self,
        query: &RecentArbitrageQuery,
    ) -> LedgerResult<Vec<RecentArbitrageCandidate>> {
        let chain_id = query.chain_id.unwrap_or(self.config.chain_id) as i64;
        let minutes = query.minutes.unwrap_or(60).clamp(1, 24 * 60);
        let limit = query.limit.unwrap_or(50).clamp(1, 500) as i64;
        let min_swaps = query.min_swaps.unwrap_or(2).max(2) as i32;
        let confirmed_only = query.confirmed_only.unwrap_or(false);
        let rows = sqlx::query(
            r#"
            select
              tx_hash, block_number, block_timestamp, detection_source,
              sender_address, entry_contract, score, swap_log_count,
              distinct_pool_count, route, route_fingerprint,
              metadata -> 'pool_addresses' as pool_addresses,
              confirmation_status, profit_recipient, profit_token_address,
              profit_amount_raw
            from arbitrage_candidates
            where chain_id = $1
              and block_timestamp >= now() - ($2::text || ' minutes')::interval
              and swap_log_count >= $3
              and ($5 = false or confirmation_status <> 'candidate')
            order by block_timestamp desc nulls last, block_number desc, score desc
            limit $4
            "#,
        )
        .bind(chain_id)
        .bind(minutes)
        .bind(min_swaps)
        .bind(limit)
        .bind(confirmed_only)
        .fetch_all(self.repository.pool())
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;

        Ok(rows
            .into_iter()
            .map(|row| RecentArbitrageCandidate {
                tx_hash: row.get::<String, _>("tx_hash"),
                block_number: row.get::<i64, _>("block_number").max(0) as u64,
                block_timestamp: row.get("block_timestamp"),
                detection_source: row.get("detection_source"),
                sender_address: row.get("sender_address"),
                entry_contract: row.get("entry_contract"),
                score: row.get("score"),
                swap_log_count: row.get::<i32, _>("swap_log_count").max(0) as u32,
                distinct_pool_count: row.get::<i32, _>("distinct_pool_count").max(0) as u32,
                route: row.get("route"),
                route_fingerprint: row.get("route_fingerprint"),
                pool_addresses: row
                    .get::<Option<Value>, _>("pool_addresses")
                    .and_then(|value| {
                        value.as_array().map(|items| {
                            items
                                .iter()
                                .filter_map(Value::as_str)
                                .map(ToOwned::to_owned)
                                .collect()
                        })
                    })
                    .unwrap_or_default(),
                confirmation_status: row.get("confirmation_status"),
                profit_recipient: row.get("profit_recipient"),
                profit_token_address: row.get("profit_token_address"),
                profit_amount_raw: row.get("profit_amount_raw"),
            })
            .collect())
    }

    pub async fn hot_arbitrage_pools(
        &self,
        query: &HotArbitragePoolsQuery,
    ) -> LedgerResult<Vec<HotArbitragePool>> {
        let chain_id = query.chain_id.unwrap_or(self.config.chain_id) as i64;
        let minutes = query.minutes.unwrap_or(60).clamp(1, 24 * 60);
        let limit = query.limit.unwrap_or(25).clamp(1, 200) as i64;
        let min_txs = query.min_txs.unwrap_or(1).max(1) as i64;
        let rows = sqlx::query(
            r#"
            select
              p.pool_address,
              count(distinct p.tx_hash)::bigint as tx_count,
              coalesce(sum(p.swap_log_count), 0)::bigint as swap_log_count,
              count(distinct c.sender_address)::bigint as unique_senders,
              count(distinct c.route_fingerprint)::bigint as unique_routes,
              min(p.block_timestamp) as first_seen,
              max(p.block_timestamp) as last_seen,
              coalesce(jsonb_agg(distinct p.tx_hash) filter (where p.tx_hash is not null), '[]'::jsonb) as sample_tx_hashes
            from arbitrage_candidate_pools p
            join arbitrage_candidates c
              on c.chain_id = p.chain_id
             and c.tx_hash = p.tx_hash
            where p.chain_id = $1
              and p.block_timestamp >= now() - ($2::text || ' minutes')::interval
            group by p.pool_address
            having count(distinct p.tx_hash) >= $3
            order by tx_count desc, swap_log_count desc, last_seen desc
            limit $4
            "#,
        )
        .bind(chain_id)
        .bind(minutes)
        .bind(min_txs)
        .bind(limit)
        .fetch_all(self.repository.pool())
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;

        Ok(rows
            .into_iter()
            .map(|row| HotArbitragePool {
                pool_address: row.get("pool_address"),
                tx_count: row.get::<i64, _>("tx_count").max(0) as u64,
                swap_log_count: row.get::<i64, _>("swap_log_count").max(0) as u64,
                unique_senders: row.get::<i64, _>("unique_senders").max(0) as u64,
                unique_routes: row.get::<i64, _>("unique_routes").max(0) as u64,
                first_seen: row.get("first_seen"),
                last_seen: row.get("last_seen"),
                sample_tx_hashes: json_string_array(
                    row.get::<Option<Value>, _>("sample_tx_hashes").as_ref(),
                )
                .into_iter()
                .take(5)
                .collect(),
            })
            .collect())
    }

    pub async fn hot_arbitrage_routes(
        &self,
        query: &HotArbitragePoolsQuery,
    ) -> LedgerResult<Vec<HotArbitrageRoute>> {
        let chain_id = query.chain_id.unwrap_or(self.config.chain_id) as i64;
        let minutes = query.minutes.unwrap_or(60).clamp(1, 24 * 60);
        let limit = query.limit.unwrap_or(25).clamp(1, 200) as i64;
        let min_txs = query.min_txs.unwrap_or(1).max(1) as i64;
        let rows = sqlx::query(
            r#"
            select
              route_fingerprint,
              route,
              count(*)::bigint as tx_count,
              coalesce(sum(swap_log_count), 0)::bigint as swap_log_count,
              count(distinct sender_address)::bigint as unique_senders,
              min(block_timestamp) as first_seen,
              max(block_timestamp) as last_seen,
              coalesce(jsonb_agg(tx_hash order by block_timestamp desc) filter (where tx_hash is not null), '[]'::jsonb) as sample_tx_hashes
            from arbitrage_candidates
            where chain_id = $1
              and block_timestamp >= now() - ($2::text || ' minutes')::interval
              and route_fingerprint <> ''
            group by route_fingerprint, route
            having count(*) >= $3
            order by tx_count desc, swap_log_count desc, last_seen desc
            limit $4
            "#,
        )
        .bind(chain_id)
        .bind(minutes)
        .bind(min_txs)
        .bind(limit)
        .fetch_all(self.repository.pool())
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;

        Ok(rows
            .into_iter()
            .map(|row| HotArbitrageRoute {
                route_fingerprint: row.get("route_fingerprint"),
                route: row.get("route"),
                tx_count: row.get::<i64, _>("tx_count").max(0) as u64,
                swap_log_count: row.get::<i64, _>("swap_log_count").max(0) as u64,
                unique_senders: row.get::<i64, _>("unique_senders").max(0) as u64,
                first_seen: row.get("first_seen"),
                last_seen: row.get("last_seen"),
                sample_tx_hashes: json_string_array(
                    row.get::<Option<Value>, _>("sample_tx_hashes").as_ref(),
                )
                .into_iter()
                .take(5)
                .collect(),
            })
            .collect())
    }

    async fn upsert_cluster_deltas(
        &self,
        tx_hash: &str,
        block_number: u64,
        block_timestamp: Option<chrono::DateTime<Utc>>,
        trace_available: bool,
        deltas: &BTreeMap<ClusterDeltaKey, ClusterDeltaAccumulator>,
    ) -> LedgerResult<u64> {
        let mut rows = 0;
        for (key, delta) in deltas {
            let net = delta.amount_in.saturating_sub(delta.amount_out);
            if delta.amount_in == 0 && delta.amount_out == 0 {
                continue;
            }
            sqlx::query(
                r#"
                insert into trace_cluster_asset_deltas (
                  chain_id, cluster_id, tx_hash, block_number, block_timestamp, asset_type, token_address,
                  amount_in_raw, amount_out_raw, net_raw, contributing_address_count, metadata, created_at, updated_at
                )
                values ($1,$2,$3,$4,$5,$6,$7,$8::numeric,$9::numeric,$10::numeric,$11,$12,now(),now())
                on conflict (chain_id, cluster_id, tx_hash, asset_type, token_address) do update set
                  block_number = excluded.block_number,
                  block_timestamp = excluded.block_timestamp,
                  amount_in_raw = excluded.amount_in_raw,
                  amount_out_raw = excluded.amount_out_raw,
                  net_raw = excluded.net_raw,
                  contributing_address_count = excluded.contributing_address_count,
                  metadata = excluded.metadata,
                  updated_at = now()
                "#,
            )
            .bind(self.config.chain_id as i64)
            .bind(key.cluster_id)
            .bind(tx_hash)
            .bind(block_number as i64)
            .bind(block_timestamp)
            .bind(&key.asset_type)
            .bind(&key.token_address)
            .bind(delta.amount_in.to_string())
            .bind(delta.amount_out.to_string())
            .bind(net.to_string())
            .bind(delta.addresses.len() as i32)
            .bind(Json(json!({
                "trace_available": trace_available,
                "addresses": delta.addresses.iter().cloned().collect::<Vec<_>>(),
            })))
            .execute(self.repository.pool())
            .await
            .map_err(|err| LedgerError::Persistence(err.to_string()))?;
            rows += 1;
        }
        Ok(rows)
    }

    async fn upsert_candidates(
        &self,
        bundle: &RawTransactionBundle,
        candidates: &[LightMonitorCandidate],
    ) -> LedgerResult<u64> {
        let mut rows = 0;
        for candidate in candidates {
            sqlx::query(
                r#"
                insert into trace_monitor_candidates (
                  chain_id, tx_hash, candidate_kind, block_number, block_timestamp,
                  score, dex_swap_count, liquidation_event_count, metadata, created_at, updated_at
                )
                values ($1,$2,$3,$4,$5,$6,$7,$8,$9,now(),now())
                on conflict (chain_id, tx_hash, candidate_kind) do update set
                  block_number = excluded.block_number,
                  block_timestamp = excluded.block_timestamp,
                  score = greatest(trace_monitor_candidates.score, excluded.score),
                  dex_swap_count = excluded.dex_swap_count,
                  liquidation_event_count = excluded.liquidation_event_count,
                  metadata = excluded.metadata,
                  updated_at = now()
                "#,
            )
            .bind(self.config.chain_id as i64)
            .bind(&bundle.tx.tx_hash)
            .bind(candidate.kind)
            .bind(bundle.tx.block_number.unwrap_or_default() as i64)
            .bind(bundle.tx.block_timestamp)
            .bind(candidate.score)
            .bind(candidate.dex_swap_count as i32)
            .bind(candidate.liquidation_event_count as i32)
            .bind(Json(candidate.metadata.clone()))
            .execute(self.repository.pool())
            .await
            .map_err(|err| LedgerError::Persistence(err.to_string()))?;
            rows += 1;
        }
        Ok(rows)
    }

    async fn upsert_fast_arbitrage_candidate(
        &self,
        candidate: &FastSwapAccumulator,
        block_timestamp: Option<DateTime<Utc>>,
        detection: &str,
    ) -> LedgerResult<u64> {
        let score = 80_i32
            .saturating_add(i32::try_from(candidate.swap_log_count.min(20)).unwrap_or_default())
            .saturating_add(i32::try_from(candidate.pools.len().min(20)).unwrap_or_default());
        let route = candidate_route(candidate);
        let route_fingerprint = route_fingerprint(&route);
        let pool_addresses = candidate.pool_addresses();
        sqlx::query(
            r#"
            insert into trace_monitor_candidates (
              chain_id, tx_hash, candidate_kind, block_number, block_timestamp,
              score, dex_swap_count, liquidation_event_count, metadata, created_at, updated_at
            )
            values ($1,$2,$3,$4,$5,$6,$7,0,$8,now(),now())
            on conflict (chain_id, tx_hash, candidate_kind) do update set
              block_number = excluded.block_number,
              block_timestamp = excluded.block_timestamp,
              score = greatest(trace_monitor_candidates.score, excluded.score),
              dex_swap_count = excluded.dex_swap_count,
              metadata = excluded.metadata,
              updated_at = now()
            "#,
        )
        .bind(self.config.chain_id as i64)
        .bind(&candidate.tx_hash)
        .bind("arbitrage_candidate")
        .bind(candidate.block_number as i64)
        .bind(block_timestamp)
        .bind(score)
        .bind(candidate.swap_log_count as i32)
        .bind(Json(json!({
            "detection": detection,
            "candidate_reason": "multiple dex swap events across distinct pools in one transaction",
            "swap_log_count": candidate.swap_log_count,
            "v2_swap_count": candidate.v2_swap_count,
            "v3_swap_count": candidate.v3_swap_count,
            "distinct_pool_count": candidate.pools.len(),
            "pool_addresses": pool_addresses,
            "log_indexes": candidate.log_indexes,
            "tx_index": candidate.tx_index,
            "sender_address": candidate.sender_address,
            "entry_contract": candidate.entry_contract,
            "route": route,
            "route_fingerprint": route_fingerprint,
            "coverage": {
                "uses_receipt": false,
                "uses_trace": false,
                "uses_full_block": false,
                "label": "candidate_not_confirmed_pnl"
            }
        })))
        .execute(self.repository.pool())
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        self.upsert_structured_arbitrage_candidate(
            candidate,
            block_timestamp,
            detection,
            score,
            &route,
            &route_fingerprint,
        )
        .await?;
        Ok(1)
    }

    async fn upsert_structured_arbitrage_candidate(
        &self,
        candidate: &FastSwapAccumulator,
        block_timestamp: Option<DateTime<Utc>>,
        detection: &str,
        score: i32,
        route: &str,
        route_fingerprint: &str,
    ) -> LedgerResult<()> {
        sqlx::query(
            r#"
            insert into arbitrage_candidates (
              chain_id, tx_hash, block_number, block_timestamp, detection_source,
              sender_address, entry_contract, tx_index, score, swap_log_count,
              v2_swap_count, v3_swap_count, distinct_pool_count, route,
              route_fingerprint, confirmation_status, profit_recipient,
              profit_token_address, profit_amount_raw, confirmation_metadata,
              metadata, created_at, updated_at
            )
            values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,now(),now())
            on conflict (chain_id, tx_hash) do update set
              block_number = excluded.block_number,
              block_timestamp = excluded.block_timestamp,
              detection_source = excluded.detection_source,
              sender_address = excluded.sender_address,
              entry_contract = excluded.entry_contract,
              tx_index = excluded.tx_index,
              score = greatest(arbitrage_candidates.score, excluded.score),
              swap_log_count = excluded.swap_log_count,
              v2_swap_count = excluded.v2_swap_count,
              v3_swap_count = excluded.v3_swap_count,
              distinct_pool_count = excluded.distinct_pool_count,
              route = excluded.route,
              route_fingerprint = excluded.route_fingerprint,
              confirmation_status = excluded.confirmation_status,
              profit_recipient = excluded.profit_recipient,
              profit_token_address = excluded.profit_token_address,
              profit_amount_raw = excluded.profit_amount_raw,
              confirmation_metadata = excluded.confirmation_metadata,
              metadata = excluded.metadata,
              updated_at = now()
            "#,
        )
        .bind(self.config.chain_id as i64)
        .bind(&candidate.tx_hash)
        .bind(candidate.block_number as i64)
        .bind(block_timestamp)
        .bind(detection)
        .bind(candidate.sender_address.as_deref())
        .bind(candidate.entry_contract.as_deref())
        .bind(candidate.tx_index.map(|value| value as i64))
        .bind(score)
        .bind(candidate.swap_log_count as i32)
        .bind(candidate.v2_swap_count as i32)
        .bind(candidate.v3_swap_count as i32)
        .bind(candidate.pools.len() as i32)
        .bind(route)
        .bind(route_fingerprint)
        .bind(candidate.confirmation.status.as_str())
        .bind(candidate.confirmation.profit_recipient.as_deref())
        .bind(candidate.confirmation.profit_token_address.as_deref())
        .bind(candidate.confirmation.profit_amount_raw.as_deref())
        .bind(Json(candidate.confirmation.metadata.clone()))
        .bind(Json(json!({
            "pool_addresses": candidate.pool_addresses(),
            "log_indexes": candidate.log_indexes,
            "candidate_reason": "multiple dex swap events across distinct pools in one transaction",
            "confirmed_pnl": false,
            "confirmation_status": candidate.confirmation.status.as_str(),
        })))
        .execute(self.repository.pool())
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;

        sqlx::query(
            r#"
            delete from arbitrage_candidate_pools
            where chain_id = $1 and tx_hash = $2
            "#,
        )
        .bind(self.config.chain_id as i64)
        .bind(&candidate.tx_hash)
        .execute(self.repository.pool())
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;

        for (position, pool) in candidate.route_pool_stats().iter().enumerate() {
            sqlx::query(
                r#"
                insert into arbitrage_candidate_pools (
                  chain_id, tx_hash, pool_address, block_number, block_timestamp,
                  first_log_index, swap_log_count, v2_swap_count, v3_swap_count,
                  route_position, metadata, created_at, updated_at
                )
                values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,now(),now())
                on conflict (chain_id, tx_hash, pool_address) do update set
                  block_number = excluded.block_number,
                  block_timestamp = excluded.block_timestamp,
                  first_log_index = excluded.first_log_index,
                  swap_log_count = excluded.swap_log_count,
                  v2_swap_count = excluded.v2_swap_count,
                  v3_swap_count = excluded.v3_swap_count,
                  route_position = excluded.route_position,
                  metadata = excluded.metadata,
                  updated_at = now()
                "#,
            )
            .bind(self.config.chain_id as i64)
            .bind(&candidate.tx_hash)
            .bind(&pool.pool_address)
            .bind(candidate.block_number as i64)
            .bind(block_timestamp)
            .bind(pool.first_log_index as i64)
            .bind(pool.swap_log_count as i32)
            .bind(pool.v2_swap_count as i32)
            .bind(pool.v3_swap_count as i32)
            .bind(position as i32)
            .bind(Json(json!({
                "tx_index": candidate.tx_index,
                "sender_address": candidate.sender_address,
                "entry_contract": candidate.entry_contract,
            })))
            .execute(self.repository.pool())
            .await
            .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        }
        Ok(())
    }

    async fn cleanup_expired_rows_if_due(&self) -> LedgerResult<()> {
        let now = Utc::now().timestamp();
        let next_cleanup_at = self.next_cleanup_at_unix.load(Ordering::SeqCst);
        if now < next_cleanup_at {
            return Ok(());
        }
        let claimed = self
            .next_cleanup_at_unix
            .compare_exchange(
                next_cleanup_at,
                now.saturating_add(CLEANUP_INTERVAL_SECS),
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_ok();
        if !claimed {
            return Ok(());
        }

        let cutoff = Utc::now() - Duration::days(self.config.retention_days.max(1));
        for table in [
            "trace_address_asset_deltas",
            "trace_cluster_asset_deltas",
            "trace_monitor_candidates",
        ] {
            let deleted = sqlx::query(&format!(
                r#"
                delete from {table}
                where ctid in (
                  select ctid
                  from {table}
                  where block_timestamp is not null and block_timestamp < $1
                  limit $2
                )
                "#
            ))
            .bind(cutoff)
            .bind(CLEANUP_BATCH_ROWS)
            .execute(self.repository.pool())
            .await
            .map_err(|err| LedgerError::Persistence(err.to_string()))?
            .rows_affected();
            if deleted > 0 {
                debug!(table, deleted, "trace monitor cleanup removed old rows");
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct ClusterScope {
    cluster_id: Uuid,
    addresses: BTreeSet<String>,
}

#[derive(Clone, Debug, Default)]
struct ClusterDeltaAccumulator {
    amount_in: i128,
    amount_out: i128,
    addresses: BTreeSet<String>,
}

fn aggregate_address_deltas(flows: &[AssetFlow]) -> BTreeMap<AddressDeltaKey, DeltaAccumulator> {
    let mut deltas = BTreeMap::new();
    for flow in flows {
        let amount = parse_amount_i128(&flow.amount_raw);
        if amount == 0 {
            continue;
        }
        let asset_type = asset_type_key(&flow.asset_type);
        let token_address = flow
            .token_address
            .clone()
            .unwrap_or_else(|| "native".to_owned())
            .to_ascii_lowercase();
        let from_key = AddressDeltaKey {
            address: flow.from_address.to_ascii_lowercase(),
            asset_type: asset_type.clone(),
            token_address: token_address.clone(),
        };
        let to_key = AddressDeltaKey {
            address: flow.to_address.to_ascii_lowercase(),
            asset_type,
            token_address,
        };
        let from_delta = deltas
            .entry(from_key)
            .or_insert_with(DeltaAccumulator::default);
        from_delta.amount_out = from_delta.amount_out.saturating_add(amount);
        from_delta.flow_count += 1;
        let to_delta = deltas
            .entry(to_key)
            .or_insert_with(DeltaAccumulator::default);
        to_delta.amount_in = to_delta.amount_in.saturating_add(amount);
        to_delta.flow_count += 1;
    }
    deltas
}

fn aggregate_cluster_deltas(
    flows: &[AssetFlow],
    clusters: &[ClusterScope],
) -> BTreeMap<ClusterDeltaKey, ClusterDeltaAccumulator> {
    let mut deltas = BTreeMap::new();
    for flow in flows {
        let amount = parse_amount_i128(&flow.amount_raw);
        if amount == 0 {
            continue;
        }
        let from = flow.from_address.to_ascii_lowercase();
        let to = flow.to_address.to_ascii_lowercase();
        for cluster in clusters {
            let from_in_scope = cluster.addresses.contains(&from);
            let to_in_scope = cluster.addresses.contains(&to);
            if from_in_scope == to_in_scope {
                continue;
            }
            let key = ClusterDeltaKey {
                cluster_id: cluster.cluster_id,
                asset_type: asset_type_key(&flow.asset_type),
                token_address: flow
                    .token_address
                    .clone()
                    .unwrap_or_else(|| "native".to_owned())
                    .to_ascii_lowercase(),
            };
            let delta = deltas
                .entry(key)
                .or_insert_with(ClusterDeltaAccumulator::default);
            if to_in_scope {
                delta.amount_in = delta.amount_in.saturating_add(amount);
                delta.addresses.insert(to.clone());
            }
            if from_in_scope {
                delta.amount_out = delta.amount_out.saturating_add(amount);
                delta.addresses.insert(from.clone());
            }
        }
    }
    deltas
}

fn asset_type_key(value: &AssetType) -> String {
    match value {
        AssetType::Native => "native",
        AssetType::Erc20 => "erc20",
    }
    .to_owned()
}

fn parse_amount_i128(value: &str) -> i128 {
    value.parse::<i128>().unwrap_or_default()
}

fn parse_hex_u64(value: &str) -> Option<u64> {
    u64::from_str_radix(value.trim_start_matches("0x"), 16).ok()
}

fn fast_arbitrage_candidates_from_logs(
    logs: Vec<Value>,
    min_swaps: u32,
) -> (u64, Vec<FastSwapAccumulator>) {
    let mut grouped = BTreeMap::<String, FastSwapAccumulator>::new();
    let mut swap_logs = 0_u64;

    for log in logs {
        if log.get("removed").and_then(Value::as_bool).unwrap_or(false) {
            continue;
        }
        let Some(tx_hash) = log
            .get("transactionHash")
            .and_then(Value::as_str)
            .map(str::to_ascii_lowercase)
        else {
            continue;
        };
        let Some(block_number) = log
            .get("blockNumber")
            .and_then(Value::as_str)
            .and_then(parse_hex_u64)
        else {
            continue;
        };
        let topic0 = log
            .get("topics")
            .and_then(Value::as_array)
            .and_then(|topics| topics.first())
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_ascii_lowercase();
        if !is_dex_swap_topic(&topic0) {
            continue;
        }
        let pool = log
            .get("address")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_ascii_lowercase();
        let tx_index = log
            .get("transactionIndex")
            .and_then(Value::as_str)
            .and_then(parse_hex_u64);
        let log_index = log
            .get("logIndex")
            .and_then(Value::as_str)
            .and_then(parse_hex_u64)
            .unwrap_or_default();

        let entry = grouped
            .entry(tx_hash.clone())
            .or_insert_with(|| FastSwapAccumulator {
                tx_hash,
                block_number,
                tx_index,
                sender_address: None,
                entry_contract: None,
                swap_log_count: 0,
                v2_swap_count: 0,
                v3_swap_count: 0,
                pools: BTreeSet::new(),
                pool_stats: BTreeMap::new(),
                log_indexes: Vec::new(),
                confirmation: FastArbitrageConfirmation::default(),
            });
        entry.block_number = entry.block_number.min(block_number);
        entry.tx_index = entry.tx_index.or(tx_index);
        entry.record_swap_log(pool, &topic0, log_index);
        swap_logs = swap_logs.saturating_add(1);
    }

    let candidates = grouped
        .into_values()
        .filter(|candidate| candidate.swap_log_count >= min_swaps && candidate.pools.len() >= 2)
        .collect::<Vec<_>>();
    (swap_logs, candidates)
}

fn cryptohouse_fast_arbitrage_query(start_block: u64, end_block: u64, min_swaps: u32) -> String {
    let v2_topic = UNISWAP_V2_SWAP_TOPIC.trim_start_matches("0x");
    let v3_topic = UNISWAP_V3_SWAP_TOPIC.trim_start_matches("0x");
    format!(
        r#"
SELECT
  l.block_number,
  l.transaction_hash,
  count() AS swap_log_count,
  countIf(position(l.topics, '{v2_topic}') > 0) AS v2_swap_count,
  countIf(position(l.topics, '{v3_topic}') > 0) AS v3_swap_count,
  uniqExact(l.address) AS distinct_pool_count,
  groupArray(128)(l.address) AS pool_addresses,
  groupArray(128)(l.log_index) AS log_indexes,
  groupArray(128)(arrayElement(splitByChar(',', l.topics), 1)) AS topic0s,
  min(l.transaction_index) AS tx_index,
  toString(max(l.block_timestamp)) AS block_timestamp,
  any(t.from_address) AS sender_address,
  any(t.to_address) AS entry_contract
FROM base.logs AS l
LEFT JOIN base.transactions AS t
  ON t.block_number = l.block_number
 AND t.hash = l.transaction_hash
WHERE l.block_number BETWEEN {start_block} AND {end_block}
  AND (
    position(l.topics, '{v2_topic}') > 0
    OR position(l.topics, '{v3_topic}') > 0
  )
GROUP BY l.block_number, l.transaction_hash
HAVING swap_log_count >= {min_swaps}
   AND distinct_pool_count >= 2
ORDER BY l.block_number, l.transaction_hash
LIMIT 10000
FORMAT JSONEachRow
"#
    )
}

fn cryptohouse_transfer_logs_query(start_block: u64, end_block: u64, tx_hashes: &str) -> String {
    format!(
        r#"
SELECT
  transaction_hash,
  address AS token_address,
  concat('0x', right(arrayElement(splitByChar(',', topics), 2), 40)) AS from_address,
  concat('0x', right(arrayElement(splitByChar(',', topics), 3), 40)) AS to_address,
  data
FROM base.logs
WHERE block_number BETWEEN {start_block} AND {end_block}
  AND transaction_hash IN ({tx_hashes})
  AND position(topics, '{ERC20_TRANSFER_TOPIC_HEX}') > 0
  AND length(arrayElement(splitByChar(',', topics), 2)) >= 40
  AND length(arrayElement(splitByChar(',', topics), 3)) >= 40
FORMAT JSONEachRow
"#
    )
}

fn fast_arbitrage_candidates_from_cryptohouse_rows(
    body: &str,
) -> LedgerResult<(u64, Vec<(FastSwapAccumulator, Option<DateTime<Utc>>)>)> {
    let mut swap_logs = 0_u64;
    let mut candidates = Vec::new();

    for line in body.lines().map(str::trim).filter(|line| !line.is_empty()) {
        let row: Value = serde_json::from_str(line).map_err(|err| {
            LedgerError::MalformedEvidence(format!(
                "cryptohouse JSONEachRow parse failed: {err}; row={}",
                truncate_for_error(line, 300)
            ))
        })?;
        let tx_hash = row
            .get("transaction_hash")
            .and_then(Value::as_str)
            .map(normalize_hash)
            .ok_or_else(|| {
                LedgerError::MalformedEvidence(
                    "cryptohouse row missing transaction_hash".to_owned(),
                )
            })?;
        let block_number = json_u64(row.get("block_number")).ok_or_else(|| {
            LedgerError::MalformedEvidence("cryptohouse row missing block_number".to_owned())
        })?;
        let swap_log_count = json_u64(row.get("swap_log_count")).unwrap_or_default() as u32;
        let v2_swap_count = json_u64(row.get("v2_swap_count")).unwrap_or_default() as u32;
        let v3_swap_count = json_u64(row.get("v3_swap_count")).unwrap_or_default() as u32;
        let tx_index = json_u64(row.get("tx_index"));
        let sender_address = row
            .get("sender_address")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(normalize_hash);
        let entry_contract = row
            .get("entry_contract")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(normalize_hash);
        let pool_logs = json_string_array(row.get("pool_addresses"));
        let log_indexes = json_u64_array(row.get("log_indexes"));
        let topic0s = json_string_array(row.get("topic0s"));
        let block_timestamp = row
            .get("block_timestamp")
            .and_then(Value::as_str)
            .and_then(parse_clickhouse_timestamp);

        let mut candidate = FastSwapAccumulator {
            tx_hash,
            block_number,
            tx_index,
            sender_address,
            entry_contract,
            swap_log_count: 0,
            v2_swap_count: 0,
            v3_swap_count: 0,
            pools: BTreeSet::new(),
            pool_stats: BTreeMap::new(),
            log_indexes: Vec::new(),
            confirmation: FastArbitrageConfirmation::default(),
        };
        for (index, pool) in pool_logs.iter().enumerate() {
            let topic0 = topic0s.get(index).map(String::as_str).unwrap_or_default();
            let log_index = log_indexes.get(index).copied().unwrap_or_default();
            candidate.record_swap_log(normalize_hash(pool), &normalize_hash(topic0), log_index);
        }
        if candidate.swap_log_count == 0 {
            candidate.swap_log_count = swap_log_count;
            candidate.v2_swap_count = v2_swap_count;
            candidate.v3_swap_count = v3_swap_count;
            candidate.log_indexes = log_indexes;
        }
        swap_logs = swap_logs.saturating_add(u64::from(swap_log_count));
        candidates.push((candidate, block_timestamp));
    }

    Ok((swap_logs, candidates))
}

fn erc20_transfers_from_cryptohouse_rows(body: &str) -> LedgerResult<Vec<Erc20TransferLite>> {
    let mut transfers = Vec::new();
    for line in body.lines().map(str::trim).filter(|line| !line.is_empty()) {
        let row: Value = serde_json::from_str(line).map_err(|err| {
            LedgerError::MalformedEvidence(format!(
                "cryptohouse transfer JSONEachRow parse failed: {err}; row={}",
                truncate_for_error(line, 300)
            ))
        })?;
        let Some(amount_raw) = row
            .get("data")
            .and_then(Value::as_str)
            .and_then(parse_erc20_amount_i128)
        else {
            continue;
        };
        transfers.push(Erc20TransferLite {
            tx_hash: row
                .get("transaction_hash")
                .and_then(Value::as_str)
                .map(normalize_hash)
                .ok_or_else(|| {
                    LedgerError::MalformedEvidence(
                        "cryptohouse transfer row missing transaction_hash".to_owned(),
                    )
                })?,
            token_address: row
                .get("token_address")
                .and_then(Value::as_str)
                .map(normalize_hash)
                .ok_or_else(|| {
                    LedgerError::MalformedEvidence(
                        "cryptohouse transfer row missing token_address".to_owned(),
                    )
                })?,
            from_address: row
                .get("from_address")
                .and_then(Value::as_str)
                .map(normalize_hash)
                .ok_or_else(|| {
                    LedgerError::MalformedEvidence(
                        "cryptohouse transfer row missing from_address".to_owned(),
                    )
                })?,
            to_address: row
                .get("to_address")
                .and_then(Value::as_str)
                .map(normalize_hash)
                .ok_or_else(|| {
                    LedgerError::MalformedEvidence(
                        "cryptohouse transfer row missing to_address".to_owned(),
                    )
                })?,
            amount_raw,
        });
    }
    Ok(transfers)
}

fn conservative_arbitrage_confirmation(
    candidate: &FastSwapAccumulator,
    transfers: Option<&Vec<Erc20TransferLite>>,
) -> FastArbitrageConfirmation {
    let Some(sender) = candidate.sender_address.as_deref() else {
        return FastArbitrageConfirmation {
            metadata: json!({
                "method": "erc20_transfer_net_to_sender",
                "reason": "transaction sender is unavailable"
            }),
            ..Default::default()
        };
    };
    let mut stable_token_nets = BTreeMap::<String, i128>::new();
    for transfer in transfers.into_iter().flatten() {
        if !is_conservative_profit_token(&transfer.token_address) {
            continue;
        }
        let entry = stable_token_nets
            .entry(transfer.token_address.clone())
            .or_default();
        if transfer.to_address == sender {
            *entry = entry.saturating_add(transfer.amount_raw);
        }
        if transfer.from_address == sender {
            *entry = entry.saturating_sub(transfer.amount_raw);
        }
    }

    let stable_token_nets_json = stable_token_nets
        .iter()
        .map(|(token, net)| (token.clone(), net.to_string()))
        .collect::<BTreeMap<_, _>>();
    let has_negative_stable_net = stable_token_nets.values().any(|net| *net < 0);
    let best_profit = stable_token_nets
        .iter()
        .filter(|(_, net)| **net > 0)
        .max_by_key(|(_, net)| **net);

    if !has_negative_stable_net {
        if let Some((token, net)) = best_profit {
            return FastArbitrageConfirmation {
                status: "confirmed_stable_surplus_to_sender".to_owned(),
                profit_recipient: Some(sender.to_owned()),
                profit_token_address: Some(token.clone()),
                profit_amount_raw: Some(net.to_string()),
                metadata: json!({
                    "method": "erc20_transfer_net_to_sender",
                    "scope": "usdc_and_usdbc_only",
                    "reason": "sender has positive stable-token net transfer and no stable-token net outflow",
                    "stable_token_nets": stable_token_nets_json
                }),
            };
        }
    }

    FastArbitrageConfirmation {
        metadata: json!({
            "method": "erc20_transfer_net_to_sender",
            "scope": "usdc_and_usdbc_only",
            "reason": if has_negative_stable_net {
                "sender has stable-token net outflow"
            } else {
                "no positive stable-token net transfer to sender"
            },
            "stable_token_nets": stable_token_nets_json
        }),
        ..Default::default()
    }
}

fn is_conservative_profit_token(token_address: &str) -> bool {
    matches!(token_address, BASE_USDC | BASE_USDBC)
}

fn parse_erc20_amount_i128(value: &str) -> Option<i128> {
    let value = value.trim().trim_start_matches("0x");
    if value.is_empty() {
        return None;
    }
    let value = if value.len() > 64 {
        &value[value.len().saturating_sub(64)..]
    } else {
        value
    };
    u128::from_str_radix(value, 16)
        .ok()
        .and_then(|amount| i128::try_from(amount).ok())
}

fn json_u64(value: Option<&Value>) -> Option<u64> {
    match value {
        Some(Value::Number(number)) => number.as_u64(),
        Some(Value::String(value)) => value.parse::<u64>().ok(),
        _ => None,
    }
}

fn json_string_array(value: Option<&Value>) -> Vec<String> {
    value
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(ToOwned::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

fn json_u64_array(value: Option<&Value>) -> Vec<u64> {
    value
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| json_u64(Some(item)))
                .collect()
        })
        .unwrap_or_default()
}

fn normalize_hash(value: &str) -> String {
    let value = value.trim().to_ascii_lowercase();
    if value.starts_with("0x") {
        value
    } else {
        format!("0x{value}")
    }
}

fn sql_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn candidate_route(candidate: &FastSwapAccumulator) -> String {
    candidate.pool_addresses().join(">")
}

fn route_fingerprint(route: &str) -> String {
    if route.is_empty() {
        return String::new();
    }
    let mut hasher = Sha256::new();
    hasher.update(route.as_bytes());
    format!("sha256:{}", hex::encode(hasher.finalize()))
}

fn parse_clickhouse_timestamp(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .map(|timestamp| timestamp.with_timezone(&Utc))
        .ok()
        .or_else(|| {
            NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S")
                .ok()
                .map(|timestamp| Utc.from_utc_datetime(&timestamp))
        })
}

fn truncate_for_error(value: &str, max_chars: usize) -> String {
    value.chars().take(max_chars).collect()
}

fn detect_light_candidates(
    bundle: &RawTransactionBundle,
    flows: &[AssetFlow],
) -> Vec<LightMonitorCandidate> {
    let dex_swap_count = bundle
        .receipt
        .logs
        .iter()
        .filter(|log| {
            log.topics
                .first()
                .map(|topic| is_dex_swap_topic(topic))
                .unwrap_or(false)
        })
        .count() as u32;
    let liquidation_event_count = bundle
        .receipt
        .logs
        .iter()
        .filter(|log| {
            log.topics
                .first()
                .map(|topic| is_liquidation_topic(topic))
                .unwrap_or(false)
        })
        .count() as u32;
    let transfer_token_count = flows
        .iter()
        .filter_map(|flow| flow.token_address.as_deref())
        .map(str::to_ascii_lowercase)
        .collect::<BTreeSet<_>>()
        .len();
    let mut candidates = Vec::new();
    if liquidation_event_count > 0 {
        candidates.push(LightMonitorCandidate {
            kind: "liquidation_event",
            score: 100,
            dex_swap_count,
            liquidation_event_count,
            metadata: json!({
                "detection": "receipt_topic",
                "transfer_token_count": transfer_token_count,
            }),
        });
    }
    if dex_swap_count >= 2 {
        candidates.push(LightMonitorCandidate {
            kind: "dex_multi_swap",
            score: 70 + i32::try_from(dex_swap_count.min(10)).unwrap_or_default(),
            dex_swap_count,
            liquidation_event_count,
            metadata: json!({
                "detection": "receipt_topic",
                "transfer_token_count": transfer_token_count,
                "candidate_reason": "multiple dex swap events in one transaction",
            }),
        });
    }
    candidates
}

fn is_dex_swap_topic(topic: &str) -> bool {
    matches!(
        topic.to_ascii_lowercase().as_str(),
        UNISWAP_V2_SWAP_TOPIC | UNISWAP_V3_SWAP_TOPIC
    )
}

fn is_liquidation_topic(topic: &str) -> bool {
    matches!(
        topic.to_ascii_lowercase().as_str(),
        "0xa4946ede45d0c6f06a0f5ce92c9ad3b4751452d2fe0e25010783bcab57a67e41"
            | "0xe413a321e8681d83188be0d2364605a8f594ac97918daee13129e6b4f89b8f3"
    )
}

fn next_start_block(
    skip_history_on_startup: bool,
    last_processed: Option<u64>,
    configured_start_block: Option<u64>,
    target_block: u64,
    block_batch_size: u64,
) -> (u64, u64) {
    if skip_history_on_startup {
        return (target_block, 0);
    }
    let requested_start = last_processed
        .map(|block| block.saturating_add(1))
        .or(configured_start_block)
        .unwrap_or(target_block);
    let live_window_start = target_block
        .saturating_sub(block_batch_size.max(1))
        .saturating_add(1);
    if requested_start < live_window_start {
        (
            live_window_start,
            live_window_start.saturating_sub(requested_start),
        )
    } else {
        (requested_start, 0)
    }
}

#[cfg(test)]
mod tests {
    use crate::domain::value_objects::EvidenceRef;
    use crate::ingestion::models::{RawReceipt, RawTransaction};

    use super::*;

    fn flow(from: &str, to: &str, token: Option<&str>, amount: &str) -> AssetFlow {
        AssetFlow {
            flow_id: "flow".to_owned(),
            asset_type: if token.is_some() {
                AssetType::Erc20
            } else {
                AssetType::Native
            },
            token_address: token.map(ToOwned::to_owned),
            from_address: from.to_owned(),
            to_address: to.to_owned(),
            amount_raw: amount.to_owned(),
            direction_hint: None,
            evidence_refs: Vec::<EvidenceRef>::new(),
            derivation_method: "test".to_owned(),
            confidence: 1.0,
            metadata: json!({}),
        }
    }

    #[test]
    fn cluster_delta_ignores_internal_transfers_and_keeps_boundary_flows() {
        let cluster_id = Uuid::new_v4();
        let cluster = ClusterScope {
            cluster_id,
            addresses: ["0xaaa".to_owned(), "0xbbb".to_owned()]
                .into_iter()
                .collect(),
        };
        let flows = vec![
            flow("0xaaa", "0xbbb", Some("0xtoken"), "10"),
            flow("0xccc", "0xaaa", Some("0xtoken"), "25"),
            flow("0xbbb", "0xddd", Some("0xtoken"), "7"),
        ];

        let deltas = aggregate_cluster_deltas(&flows, &[cluster]);
        let delta = deltas.values().next().expect("cluster delta");
        assert_eq!(delta.amount_in, 25);
        assert_eq!(delta.amount_out, 7);
        assert_eq!(delta.addresses.len(), 2);
    }

    #[test]
    fn startup_start_block_ignores_persisted_history_once() {
        assert_eq!(
            next_start_block(true, Some(100), Some(200), 500, 30),
            (500, 0)
        );
        assert_eq!(
            next_start_block(false, Some(100), Some(200), 500, 30),
            (471, 370)
        );
        assert_eq!(next_start_block(false, None, Some(490), 500, 30), (490, 0));
        assert_eq!(next_start_block(false, None, None, 500, 30), (500, 0));
    }

    #[test]
    fn receipt_detector_keeps_liquidations_and_multi_swap_routes_only() {
        let bundle = RawTransactionBundle {
            tx: RawTransaction {
                chain_id: 8453,
                tx_hash: "0xcandidate".to_owned(),
                block_number: Some(10),
                block_timestamp: None,
                from_address: "0xaaa".to_owned(),
                to_address: Some("0xbbb".to_owned()),
                value: "0x0".to_owned(),
                input_data: "0x".to_owned(),
                nonce: None,
                tx_index: None,
            },
            receipt: RawReceipt {
                tx_hash: "0xcandidate".to_owned(),
                status: 1,
                gas_used: None,
                effective_gas_price: None,
                logs: vec![
                    crate::ingestion::models::RawLog {
                        log_index: 1,
                        address: "0xpool1".to_owned(),
                        topics: vec![
                            "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"
                                .to_owned(),
                        ],
                        data: "0x".to_owned(),
                    },
                    crate::ingestion::models::RawLog {
                        log_index: 2,
                        address: "0xpool2".to_owned(),
                        topics: vec![
                            "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"
                                .to_owned(),
                        ],
                        data: "0x".to_owned(),
                    },
                    crate::ingestion::models::RawLog {
                        log_index: 3,
                        address: "0xprotocol".to_owned(),
                        topics: vec![
                            "0xa4946ede45d0c6f06a0f5ce92c9ad3b4751452d2fe0e25010783bcab57a67e41"
                                .to_owned(),
                        ],
                        data: "0x".to_owned(),
                    },
                ],
            },
            trace: None,
            target_code: None,
            snapshots: vec![],
            metadata: json!({}),
        };

        let candidates = detect_light_candidates(&bundle, &[]);
        assert!(
            candidates
                .iter()
                .any(|item| item.kind == "liquidation_event")
        );
        assert!(candidates.iter().any(|item| item.kind == "dex_multi_swap"));
    }

    #[test]
    fn fast_arbitrage_detector_requires_multiple_distinct_pools() {
        let tx_hash = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let log = |pool: &str, topic: &str, log_index: &str| {
            json!({
                "transactionHash": tx_hash,
                "blockNumber": "0x64",
                "transactionIndex": "0x2",
                "logIndex": log_index,
                "address": pool,
                "topics": [topic],
                "removed": false,
            })
        };
        let logs = vec![
            log(
                "0x1111111111111111111111111111111111111111",
                UNISWAP_V3_SWAP_TOPIC,
                "0x1",
            ),
            log(
                "0x1111111111111111111111111111111111111111",
                UNISWAP_V3_SWAP_TOPIC,
                "0x2",
            ),
            log(
                "0x2222222222222222222222222222222222222222",
                UNISWAP_V2_SWAP_TOPIC,
                "0x3",
            ),
        ];

        let (swap_logs, candidates) = fast_arbitrage_candidates_from_logs(logs, 2);
        assert_eq!(swap_logs, 3);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].swap_log_count, 3);
        assert_eq!(candidates[0].pools.len(), 2);
        assert_eq!(candidates[0].v2_swap_count, 1);
        assert_eq!(candidates[0].v3_swap_count, 2);

        let (_, single_pool_candidates) = fast_arbitrage_candidates_from_logs(
            vec![
                log(
                    "0x3333333333333333333333333333333333333333",
                    UNISWAP_V3_SWAP_TOPIC,
                    "0x4",
                ),
                log(
                    "0x3333333333333333333333333333333333333333",
                    UNISWAP_V3_SWAP_TOPIC,
                    "0x5",
                ),
            ],
            2,
        );
        assert!(single_pool_candidates.is_empty());
    }

    #[test]
    fn cryptohouse_fast_arbitrage_parser_keeps_candidate_shape() {
        let body = r#"{"block_number":48428766,"transaction_hash":"da749791a102579224bbfce9bd95b49c8276dd31ac5833a3bfcc46bb2d7d5cfa","swap_log_count":3,"v2_swap_count":1,"v3_swap_count":2,"distinct_pool_count":2,"pool_addresses":["0x1111111111111111111111111111111111111111","2222222222222222222222222222222222222222","0x1111111111111111111111111111111111111111"],"log_indexes":[1,"2",3],"topic0s":["0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822","0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67","0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"],"tx_index":12,"block_timestamp":"2026-07-10 01:02:03","sender_address":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","entry_contract":"0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}"#;
        let (swap_logs, candidates) =
            fast_arbitrage_candidates_from_cryptohouse_rows(body).expect("parser should work");

        assert_eq!(swap_logs, 3);
        assert_eq!(candidates.len(), 1);
        let (candidate, block_timestamp) = &candidates[0];
        assert_eq!(
            candidate.tx_hash,
            "0xda749791a102579224bbfce9bd95b49c8276dd31ac5833a3bfcc46bb2d7d5cfa"
        );
        assert_eq!(candidate.block_number, 48_428_766);
        assert_eq!(candidate.swap_log_count, 3);
        assert_eq!(candidate.v2_swap_count, 1);
        assert_eq!(candidate.v3_swap_count, 2);
        assert_eq!(candidate.pools.len(), 2);
        assert_eq!(candidate.log_indexes, vec![1, 2, 3]);
        assert_eq!(candidate.tx_index, Some(12));
        assert_eq!(
            candidate.sender_address.as_deref(),
            Some("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        );
        assert_eq!(
            candidate.entry_contract.as_deref(),
            Some("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
        );
        assert_eq!(
            candidate_route(candidate),
            "0x1111111111111111111111111111111111111111>0x2222222222222222222222222222222222222222"
        );
        assert!(route_fingerprint(&candidate_route(candidate)).starts_with("sha256:"));
        assert!(block_timestamp.is_some());
    }
}
