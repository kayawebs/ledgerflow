use std::{collections::BTreeMap, sync::Arc};

use tokio::task::JoinSet;
use tokio::time::{Duration, sleep};
use tracing::{error, info, warn};

use crate::core::errors::{LedgerError, LedgerResult};
use crate::indexer::clickhouse::ClickHouseClient;
use crate::indexer::config::IndexerConfig;
use crate::indexer::models::{
    AddressTxDirection, AddressTxHashesResponse, AddressTxQueryResponse, extract_block_refs,
    normalize_address,
};
use crate::indexer::rpc::IndexerRpcClient;

#[derive(Clone)]
pub struct IndexerService {
    pub config: IndexerConfig,
    rpc: Arc<IndexerRpcClient>,
    clickhouse: Arc<ClickHouseClient>,
}

impl IndexerService {
    pub fn new(config: IndexerConfig) -> Self {
        let rpc = Arc::new(IndexerRpcClient::new(config.rpc_url.clone()));
        let clickhouse = Arc::new(ClickHouseClient::new(
            config.clickhouse_url.clone(),
            config.clickhouse_database.clone(),
            config.clickhouse_user.clone(),
            config.clickhouse_password.clone(),
        ));
        Self {
            config,
            rpc,
            clickhouse,
        }
    }

    pub async fn bootstrap(&self) -> LedgerResult<()> {
        self.clickhouse.bootstrap().await
    }

    pub async fn run_worker_forever(&self) {
        loop {
            if let Err(err) = self.run_worker_once().await {
                error!(error = %err, "indexer worker loop failed");
                sleep(Duration::from_millis(self.config.poll_interval_ms)).await;
            } else {
                return;
            }
        }
    }

    pub async fn run_endpoint_backfill_until_caught_up(&self) {
        if !self.config.endpoint_backfill_enabled {
            info!("indexer endpoint backfill disabled");
            return;
        }

        loop {
            match self.backfill_missing_endpoints_batch().await {
                Ok(0) => {
                    info!("indexer endpoint backfill complete");
                    return;
                }
                Ok(processed) => {
                    info!(processed_rows = processed, "indexer endpoint backfill batch");
                    if self.config.rpc_request_delay_ms > 0 {
                        sleep(Duration::from_millis(self.config.rpc_request_delay_ms)).await;
                    }
                }
                Err(err) => {
                    error!(error = %err, "indexer endpoint backfill failed");
                    sleep(Duration::from_millis(self.config.poll_interval_ms)).await;
                }
            }
        }
    }

    pub async fn get_progress(&self) -> LedgerResult<Option<u64>> {
        self.clickhouse
            .get_progress(self.config.chain_id, &self.config.worker_id)
            .await
    }

    pub async fn get_endpoint_backfill_pending_rows(&self) -> LedgerResult<u64> {
        self.clickhouse
            .count_refs_missing_endpoints(self.config.chain_id)
            .await
    }

    pub async fn query_address(
        &self,
        address: &str,
        limit: Option<u32>,
        from_block: Option<u32>,
        to_block: Option<u32>,
        direction: Option<AddressTxDirection>,
    ) -> LedgerResult<AddressTxQueryResponse> {
        let address = normalize_address(address)?;
        let limit = limit
            .unwrap_or(self.config.query_limit_default)
            .min(self.config.query_limit_max);
        let refs = self
            .clickhouse
            .query_address_refs(
                self.config.chain_id,
                &address,
                limit,
                from_block,
                to_block,
                direction,
            )
            .await?;
        Ok(AddressTxQueryResponse { address, refs })
    }

    pub async fn query_address_hashes(
        &self,
        address: &str,
        limit: Option<u32>,
        from_block: Option<u32>,
        to_block: Option<u32>,
        direction: Option<AddressTxDirection>,
    ) -> LedgerResult<AddressTxHashesResponse> {
        let address = normalize_address(address)?;
        let limit = limit
            .unwrap_or(self.config.query_limit_default)
            .min(self.config.query_limit_max);
        let tx_hashes = self
            .clickhouse
            .query_address_hashes(
                self.config.chain_id,
                &address,
                limit,
                from_block,
                to_block,
                direction,
            )
            .await?;
        Ok(AddressTxHashesResponse { address, tx_hashes })
    }

    async fn run_worker_once(&self) -> LedgerResult<()> {
        self.bootstrap().await?;
        let mut next_block = self
            .clickhouse
            .get_progress(self.config.chain_id, &self.config.worker_id)
            .await?
            .map(|block| block + 1)
            .unwrap_or(self.config.start_block);

        loop {
            let latest_block = self.get_latest_block_number_with_retry().await?;
            let target_block = self
                .config
                .end_block
                .map(|end_block| end_block.min(latest_block))
                .unwrap_or(latest_block);

            if next_block > target_block {
                if self.config.live_follow && self.config.end_block.is_none() {
                    sleep(Duration::from_millis(self.config.poll_interval_ms)).await;
                    continue;
                }
                info!(last_block = target_block, "indexer caught up");
                break;
            }

            let batch_end = target_block.min(next_block + self.config.batch_size.saturating_sub(1));
            info!(
                start_block = next_block,
                end_block = batch_end,
                concurrency = self.config.rpc_concurrency,
                "index batch"
            );
            let refs = self.fetch_batch_refs(next_block, batch_end).await?;
            self.clickhouse.insert_refs(&refs).await?;
            self.clickhouse
                .set_progress(self.config.chain_id, &self.config.worker_id, batch_end)
                .await?;
            next_block = batch_end + 1;
        }

        Ok(())
    }

    async fn backfill_missing_endpoints_batch(&self) -> LedgerResult<usize> {
        let refs = self
            .clickhouse
            .query_refs_missing_endpoints(
                self.config.chain_id,
                self.config.endpoint_backfill_batch_size.max(1),
            )
            .await?;
        if refs.is_empty() {
            return Ok(0);
        }

        let mut endpoint_cache = BTreeMap::<String, TransactionEndpoints>::new();
        let mut updated_refs = Vec::with_capacity(refs.len());
        for mut tx_ref in refs {
            let endpoints = if let Some(cached) = endpoint_cache.get(&tx_ref.tx_hash) {
                cached.clone()
            } else {
                let payload = self.rpc.get_transaction_by_hash(&tx_ref.tx_hash).await?;
                if payload.is_null() {
                    continue;
                }
                let endpoints = TransactionEndpoints::from_rpc_payload(&payload)?;
                endpoint_cache.insert(tx_ref.tx_hash.clone(), endpoints.clone());
                endpoints
            };
            tx_ref.from_address = Some(endpoints.from_address.clone());
            tx_ref.to_address = endpoints.to_address.clone();
            updated_refs.push(tx_ref);
        }

        if updated_refs.is_empty() {
            return Ok(0);
        }
        self.clickhouse.insert_refs(&updated_refs).await?;
        Ok(updated_refs.len())
    }

    async fn fetch_batch_refs(&self, start_block: u64, end_block: u64) -> LedgerResult<Vec<crate::indexer::models::AddressTxRef>> {
        let concurrency = self.config.rpc_concurrency.max(1);
        let mut join_set = JoinSet::new();
        let mut refs = Vec::new();
        let mut next_to_spawn = start_block;

        while next_to_spawn <= end_block || !join_set.is_empty() {
            while next_to_spawn <= end_block && join_set.len() < concurrency {
                let rpc = self.rpc.clone();
                let config = self.config.clone();
                let chain_id = self.config.chain_id;
                let block_number = next_to_spawn;
                join_set.spawn(async move {
                    fetch_block_refs_with_retry(rpc, config, chain_id, block_number).await
                });
                next_to_spawn += 1;
            }

            match join_set.join_next().await {
                Some(Ok(Ok(block_refs))) => refs.extend(block_refs),
                Some(Ok(Err(err))) => return Err(err),
                Some(Err(err)) => {
                    return Err(LedgerError::TransientIngestion(format!(
                        "indexer fetch task join error: {err}"
                    )));
                }
                None => break,
            }
        }

        Ok(refs)
    }

    async fn get_latest_block_number_with_retry(&self) -> LedgerResult<u64> {
        let mut attempt = 0_u32;
        loop {
            match self.rpc.get_latest_block_number().await {
                Ok(block_number) => return Ok(block_number),
                Err(err) if is_rate_limit_error(&err) && attempt < self.config.rpc_max_retries => {
                    let delay_ms = retry_delay_ms(
                        self.config.rpc_retry_base_ms,
                        self.config.rpc_retry_max_ms,
                        attempt,
                    );
                    warn!(
                        attempt = attempt + 1,
                        delay_ms,
                        error = %err,
                        "latest block request hit rate limit, retrying"
                    );
                    sleep(Duration::from_millis(delay_ms)).await;
                    attempt += 1;
                }
                Err(err) => return Err(err),
            }
        }
    }

}

async fn fetch_block_refs_with_retry(
    rpc: Arc<IndexerRpcClient>,
    config: IndexerConfig,
    chain_id: u32,
    block_number: u64,
) -> LedgerResult<Vec<crate::indexer::models::AddressTxRef>> {
    let mut attempt = 0_u32;
    loop {
        if config.rpc_request_delay_ms > 0 {
            sleep(Duration::from_millis(config.rpc_request_delay_ms)).await;
        }
        match rpc.get_block_with_transactions(block_number).await {
            Ok(block_payload) => {
                if block_payload.is_null() {
                    warn!(block_number, "block payload missing, retry later");
                    return Err(LedgerError::TransientIngestion(format!(
                        "block {block_number} returned null payload"
                    )));
                }
                return extract_block_refs(chain_id, &block_payload);
            }
            Err(err) if is_rate_limit_error(&err) && attempt < config.rpc_max_retries => {
                let delay_ms =
                    retry_delay_ms(config.rpc_retry_base_ms, config.rpc_retry_max_ms, attempt);
                warn!(
                    block_number,
                    attempt = attempt + 1,
                    delay_ms,
                    error = %err,
                    "block fetch hit rate limit, retrying"
                );
                sleep(Duration::from_millis(delay_ms)).await;
                attempt += 1;
            }
            Err(err) => return Err(err),
        }
    }
}

fn is_rate_limit_error(error: &LedgerError) -> bool {
    match error {
        LedgerError::TransientIngestion(message) => {
            let message = message.to_ascii_lowercase();
            message.contains("rate limit")
                || message.contains("over rate limit")
                || message.contains("\"code\":-32016")
        }
        _ => false,
    }
}

fn retry_delay_ms(base_ms: u64, max_ms: u64, attempt: u32) -> u64 {
    let multiplier = 1_u64.checked_shl(attempt.min(16)).unwrap_or(u64::MAX);
    base_ms.saturating_mul(multiplier).min(max_ms)
}

#[derive(Clone)]
struct TransactionEndpoints {
    from_address: String,
    to_address: Option<String>,
}

impl TransactionEndpoints {
    fn from_rpc_payload(payload: &serde_json::Value) -> LedgerResult<Self> {
        let from_address = normalize_address(
            payload
                .get("from")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| {
                    LedgerError::MalformedEvidence(
                        "eth_getTransactionByHash missing from".to_owned(),
                    )
                })?,
        )?;
        let to_address = payload
            .get("to")
            .and_then(serde_json::Value::as_str)
            .map(normalize_address)
            .transpose()?;
        Ok(Self {
            from_address,
            to_address,
        })
    }
}
