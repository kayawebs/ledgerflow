use std::sync::Arc;

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

    pub async fn get_progress(&self) -> LedgerResult<Option<u64>> {
        self.clickhouse
            .get_progress(self.config.chain_id, &self.config.worker_id)
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
            let latest_block = self.rpc.get_latest_block_number().await?;
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
                "index batch"
            );
            let mut refs = Vec::new();
            let mut last_successful_block = next_block.saturating_sub(1);
            for block_number in next_block..=batch_end {
                let block_payload = self.rpc.get_block_with_transactions(block_number).await?;
                if block_payload.is_null() {
                    warn!(block_number, "block payload missing, retry later");
                    return Err(LedgerError::TransientIngestion(format!(
                        "block {block_number} returned null payload"
                    )));
                }
                refs.extend(extract_block_refs(self.config.chain_id, &block_payload)?);
                last_successful_block = block_number;
            }
            self.clickhouse.insert_refs(&refs).await?;
            if last_successful_block >= next_block {
                self.clickhouse
                    .set_progress(
                        self.config.chain_id,
                        &self.config.worker_id,
                        last_successful_block,
                    )
                    .await?;
                next_block = last_successful_block + 1;
            }
        }

        Ok(())
    }
}
