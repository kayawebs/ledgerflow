use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use chrono::Utc;
use sha2::{Digest, Sha256};
use tokio::task::JoinSet;
use tokio::time::{Duration, sleep};
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::core::errors::{LedgerError, LedgerResult};
use crate::indexer::clickhouse::ClickHouseClient;
use crate::indexer::config::{IndexerConfig, IndexerHistoryReceiptMode, IndexerWorkerMode};
use crate::indexer::models::{
    AddressTxDirection, AddressTxHashesResponse, AddressTxQueryResponse, ChainReorganization,
    CreateIndexerApiKeyRequest, CreateIndexerApiKeyResponse, IndexedBlock, IndexedBlockFacts,
    IndexerApiKeyRecord, IndexerApiKeyStatus, IndexerStorageStats, IndexerWorkerProgress,
    LazyIndexCoverage, LazyIndexingRequest, RevokeIndexerApiKeyRequest, extract_block_facts,
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
        let rpc = Arc::new(IndexerRpcClient::with_timeout(
            config.rpc_url.clone(),
            Duration::from_millis(config.rpc_request_timeout_ms),
        ));
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
        let mut consecutive_failures = 0_u32;
        loop {
            if let Err(err) = self.run_worker_once().await {
                consecutive_failures = consecutive_failures.saturating_add(1);
                if let Err(progress_error) = self
                    .record_worker_failure(consecutive_failures, &err.to_string())
                    .await
                {
                    warn!(error = %progress_error, "failed to persist indexer worker error");
                }
                error!(error = %err, "indexer worker loop failed");
                sleep(Duration::from_millis(self.config.poll_interval_ms)).await;
            } else {
                return;
            }
        }
    }

    pub async fn run_lazy_worker_forever(&self) {
        loop {
            if let Err(err) = self.run_lazy_worker().await {
                error!(error = %err, "indexer lazy worker loop failed");
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
                    info!(
                        processed_rows = processed,
                        "indexer endpoint backfill batch"
                    );
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

    pub async fn get_worker_progresses(&self) -> LedgerResult<Vec<IndexerWorkerProgress>> {
        self.clickhouse
            .list_worker_progress(self.config.chain_id)
            .await
    }

    pub async fn get_storage_stats(&self) -> LedgerResult<IndexerStorageStats> {
        self.clickhouse.storage_stats(self.config.chain_id).await
    }

    pub async fn get_rpc_head(&self) -> LedgerResult<u64> {
        self.get_latest_block_number_with_retry().await
    }

    pub fn lazy_enabled(&self) -> bool {
        self.config.lazy_enabled
    }

    pub async fn get_query_target_block(&self) -> LedgerResult<u64> {
        let latest_block = self.get_latest_block_number_with_retry().await?;
        Ok(self
            .config
            .end_block
            .map(|end_block| end_block.min(latest_block))
            .unwrap_or(latest_block))
    }

    pub async fn get_max_indexed_block(&self) -> LedgerResult<Option<u64>> {
        let legacy = self.get_progress().await?;
        let lazy = self
            .clickhouse
            .max_indexed_block(self.config.chain_id)
            .await?;
        Ok(match (legacy, lazy) {
            (Some(left), Some(right)) => Some(left.max(right)),
            (Some(value), None) | (None, Some(value)) => Some(value),
            (None, None) => None,
        })
    }

    pub async fn get_pending_indexing_request_count(&self) -> LedgerResult<u64> {
        self.clickhouse
            .count_pending_indexing_requests(self.config.chain_id)
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
        known_only: bool,
    ) -> LedgerResult<AddressTxQueryResponse> {
        let address = normalize_address(address)?;
        let limit = limit
            .unwrap_or(self.config.query_limit_default)
            .min(self.config.query_limit_max);
        let coverage = if known_only {
            match (from_block, to_block) {
                (Some(from_block), Some(to_block)) => {
                    Some(self.lazy_coverage(from_block, to_block).await?)
                }
                _ => None,
            }
        } else {
            self.prepare_lazy_query(&address, from_block, to_block)
                .await?
        };
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
        Ok(AddressTxQueryResponse {
            address,
            refs,
            coverage,
        })
    }

    pub async fn query_address_hashes(
        &self,
        address: &str,
        limit: Option<u32>,
        from_block: Option<u32>,
        to_block: Option<u32>,
        direction: Option<AddressTxDirection>,
        known_only: bool,
    ) -> LedgerResult<AddressTxHashesResponse> {
        let address = normalize_address(address)?;
        let limit = limit
            .unwrap_or(self.config.query_limit_default)
            .min(self.config.query_limit_max);
        let coverage = if known_only {
            match (from_block, to_block) {
                (Some(from_block), Some(to_block)) => {
                    Some(self.lazy_coverage(from_block, to_block).await?)
                }
                _ => None,
            }
        } else {
            self.prepare_lazy_query(&address, from_block, to_block)
                .await?
        };
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
        Ok(AddressTxHashesResponse {
            address,
            tx_hashes,
            coverage,
        })
    }

    async fn prepare_lazy_query(
        &self,
        address: &str,
        from_block: Option<u32>,
        to_block: Option<u32>,
    ) -> LedgerResult<Option<LazyIndexCoverage>> {
        if !self.config.lazy_enabled {
            return Ok(None);
        }
        let configured_start = u32::try_from(self.config.start_block).map_err(|_| {
            LedgerError::AnalysisInvariant(format!(
                "indexer start block exceeds UInt32: {}",
                self.config.start_block
            ))
        })?;
        let from_block = from_block.unwrap_or(configured_start);
        let to_block = match to_block {
            Some(to_block) => match self.config.end_block {
                Some(configured_end) => {
                    to_block.min(u32::try_from(configured_end).map_err(|_| {
                        LedgerError::AnalysisInvariant(
                            "configured indexer end block exceeds UInt32".to_owned(),
                        )
                    })?)
                }
                None => to_block,
            },
            None => u32::try_from(self.get_query_target_block().await?).map_err(|_| {
                LedgerError::AnalysisInvariant("indexer target block exceeds UInt32".to_owned())
            })?,
        };
        if from_block > to_block {
            return Err(LedgerError::MalformedEvidence(format!(
                "invalid lazy index range: from_block {from_block} > to_block {to_block}"
            )));
        }
        validate_lazy_range(from_block, to_block, self.config.lazy_max_range_blocks)?;

        let request_id = lazy_request_id(self.config.chain_id, from_block, to_block);
        if let Some(existing) = self
            .clickhouse
            .get_indexing_request(self.config.chain_id, &request_id)
            .await?
            && matches!(existing.status.as_str(), "queued" | "running" | "completed")
        {
            let requested_block_count = u64::from(to_block) - u64::from(from_block) + 1;
            let complete = existing.status == "completed";
            let indexed_block_count = if complete {
                requested_block_count
            } else {
                u64::from(existing.next_block.saturating_sub(from_block)).min(requested_block_count)
            };
            return Ok(Some(LazyIndexCoverage {
                from_block,
                to_block,
                requested_block_count,
                indexed_block_count,
                complete,
                request_id: (!complete).then_some(request_id),
            }));
        }

        let mut coverage = self.lazy_coverage(from_block, to_block).await?;
        if coverage.complete {
            return Ok(Some(coverage));
        }

        self.clickhouse
            .upsert_indexing_request(&LazyIndexingRequest {
                request_id: request_id.clone(),
                chain_id: self.config.chain_id,
                requested_address: address.to_owned(),
                from_block,
                to_block,
                next_block: from_block,
                status: "queued".to_owned(),
                last_error: String::new(),
            })
            .await?;
        coverage.request_id = Some(request_id);
        Ok(Some(coverage))
    }

    async fn lazy_coverage(
        &self,
        from_block: u32,
        to_block: u32,
    ) -> LedgerResult<LazyIndexCoverage> {
        let legacy_progress = self
            .clickhouse
            .get_legacy_progress(self.config.chain_id, &self.config.worker_id)
            .await?;
        let legacy_range = legacy_coverage_range(
            u32::try_from(self.config.start_block).ok(),
            legacy_progress.and_then(|value| u32::try_from(value).ok()),
            from_block,
            to_block,
        );
        let legacy_count = legacy_range
            .map(|(start, end)| u64::from(end) - u64::from(start) + 1)
            .unwrap_or(0);
        let lazy_count = self
            .clickhouse
            .count_indexed_blocks(self.config.chain_id, from_block, to_block, legacy_range)
            .await?;
        let requested_block_count = u64::from(to_block) - u64::from(from_block) + 1;
        let indexed_block_count = legacy_count
            .saturating_add(lazy_count)
            .min(requested_block_count);
        Ok(LazyIndexCoverage {
            from_block,
            to_block,
            requested_block_count,
            indexed_block_count,
            complete: indexed_block_count == requested_block_count,
            request_id: None,
        })
    }

    pub fn authorize_internal_token(&self, token: Option<&str>) -> bool {
        match self.config.internal_api_token.as_deref() {
            Some(expected) if !expected.is_empty() => token == Some(expected),
            _ => true,
        }
    }

    pub async fn authorize_public_token(&self, token: Option<&str>) -> LedgerResult<()> {
        let token = match token {
            Some(token) if !token.trim().is_empty() => token.trim(),
            _ => {
                if self.config.internal_api_token.is_some() {
                    return Err(LedgerError::NotFound("missing api token".to_owned()));
                }
                return Ok(());
            }
        };
        if self.authorize_internal_token(Some(token)) {
            return Ok(());
        }
        let token_hash = hash_api_key(token);
        let Some(record) = self
            .clickhouse
            .get_active_api_key_by_hash(&token_hash)
            .await?
        else {
            return Err(LedgerError::NotFound("invalid api token".to_owned()));
        };
        self.clickhouse.touch_api_key_last_used(&record).await?;
        Ok(())
    }

    pub async fn create_api_key(
        &self,
        request: CreateIndexerApiKeyRequest,
    ) -> LedgerResult<CreateIndexerApiKeyResponse> {
        let api_key = format!(
            "lf_idx_{}_{}",
            Uuid::new_v4().simple(),
            Uuid::new_v4().simple()
        );
        let now = Utc::now();
        let record = IndexerApiKeyRecord {
            key_id: Uuid::new_v4().to_string(),
            token_hash: Some(hash_api_key(&api_key)),
            owner_user_id: request.owner_user_id,
            owner_email: request.owner_email,
            label: request.label.trim().to_owned(),
            token_prefix: api_key.chars().take(18).collect(),
            status: IndexerApiKeyStatus::Active,
            created_at: now,
            updated_at: now,
            last_used_at: None,
        };
        self.clickhouse
            .insert_api_key(&record, record.token_hash.as_deref().unwrap_or_default())
            .await?;
        Ok(CreateIndexerApiKeyResponse { record, api_key })
    }

    pub async fn list_api_keys(
        &self,
        owner_user_id: &str,
    ) -> LedgerResult<Vec<IndexerApiKeyRecord>> {
        self.clickhouse.list_api_keys(owner_user_id).await
    }

    pub async fn revoke_api_key(
        &self,
        key_id: &str,
        request: RevokeIndexerApiKeyRequest,
    ) -> LedgerResult<IndexerApiKeyRecord> {
        let Some(mut record) = self.clickhouse.get_api_key_by_id(key_id).await? else {
            return Err(LedgerError::NotFound("api key not found".to_owned()));
        };
        if record.owner_user_id != request.owner_user_id {
            return Err(LedgerError::NotFound("api key not found".to_owned()));
        }
        record.status = IndexerApiKeyStatus::Revoked;
        record.updated_at = Utc::now();
        self.clickhouse.insert_api_key(&record, "").await?;
        Ok(record)
    }

    async fn run_worker_once(&self) -> LedgerResult<()> {
        self.bootstrap().await?;
        match self.config.worker_mode {
            IndexerWorkerMode::Lazy => self.run_lazy_worker().await,
            IndexerWorkerMode::Live | IndexerWorkerMode::History => {
                self.run_continuous_worker().await
            }
        }
    }

    async fn run_continuous_worker(&self) -> LedgerResult<()> {
        let mut latest_block = self.get_latest_block_number_with_retry().await?;
        let mut progress = self
            .clickhouse
            .get_worker_progress(self.config.chain_id, &self.config.worker_id)
            .await?;
        let range_end = self.resolve_continuous_range_end(progress.as_ref()).await?;
        let mut target_block = self.target_block(latest_block, range_end);

        if progress.is_none() {
            let range_start = self.initial_range_start(target_block);
            if range_start > 0 {
                let initial = self.progress_record(
                    range_start - 1,
                    String::new(),
                    range_start,
                    latest_block,
                    0,
                    String::new(),
                    None,
                    range_end,
                )?;
                self.clickhouse.upsert_worker_progress(&initial).await?;
                progress = Some(initial);
            }
        }

        let mut next_block = progress
            .as_ref()
            .map(|item| u64::from(item.last_indexed_block).saturating_add(1))
            .unwrap_or_else(|| self.initial_range_start(target_block));

        loop {
            latest_block = self.get_latest_block_number_with_retry().await?;
            target_block = self.target_block(latest_block, range_end);

            if self.config.worker_mode == IndexerWorkerMode::Live
                && let Some(current) = progress.as_ref()
                && !current.last_indexed_hash.is_empty()
                && let Some(rewound) = self.reconcile_live_tip(current, latest_block).await?
            {
                next_block = u64::from(rewound.last_indexed_block).saturating_add(1);
                progress = Some(rewound);
                continue;
            }

            if next_block > target_block {
                if self.config.worker_mode == IndexerWorkerMode::Live
                    && self.config.live_follow
                    && range_end.is_none()
                {
                    if let Some(current) = progress.as_ref() {
                        let refreshed = self.progress_record(
                            u64::from(current.last_indexed_block),
                            current.last_indexed_hash.clone(),
                            u64::from(current.range_start),
                            latest_block,
                            0,
                            String::new(),
                            current.last_success_at.clone(),
                            range_end,
                        )?;
                        self.clickhouse.upsert_worker_progress(&refreshed).await?;
                        progress = Some(refreshed);
                    }
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
                rpc_batch_request_size = self.config.rpc_batch_request_size.max(1),
                include_receipts = self.config.history_include_receipts,
                history_receipt_mode = self.config.history_receipt_mode.as_str(),
                "index batch"
            );
            let blocks = self
                .fetch_block_range(next_block, batch_end, self.config.history_include_receipts)
                .await?;
            let expected_parent = if next_block > 0 {
                self.clickhouse
                    .get_canonical_block(
                        self.config.chain_id,
                        u32::try_from(next_block - 1).map_err(|_| {
                            LedgerError::AnalysisInvariant(
                                "indexer parent block exceeds UInt32".to_owned(),
                            )
                        })?,
                    )
                    .await?
                    .map(|block| block.block_hash)
            } else {
                None
            };
            validate_block_chain(&blocks, expected_parent.as_deref())?;
            self.persist_full_block_facts(&blocks).await?;

            let last = blocks.last().ok_or_else(|| {
                LedgerError::AnalysisInvariant("index batch returned no blocks".to_owned())
            })?;
            let range_start = progress
                .as_ref()
                .map(|item| u64::from(item.range_start))
                .unwrap_or(next_block);
            let updated = self.progress_record(
                batch_end,
                last.block.block_hash.clone(),
                range_start,
                latest_block,
                0,
                String::new(),
                Some(now_clickhouse()),
                range_end,
            )?;
            self.clickhouse.upsert_worker_progress(&updated).await?;
            progress = Some(updated);
            next_block = batch_end + 1;
            if self.config.inter_batch_delay_ms > 0 {
                sleep(Duration::from_millis(self.config.inter_batch_delay_ms)).await;
            }
        }

        Ok(())
    }

    async fn resolve_continuous_range_end(
        &self,
        progress: Option<&IndexerWorkerProgress>,
    ) -> LedgerResult<Option<u64>> {
        if let Some(end_block) = self.config.end_block {
            return Ok(Some(end_block));
        }
        if self.config.worker_mode != IndexerWorkerMode::History {
            return Ok(None);
        }
        if let Some(range_end) = progress.and_then(|progress| progress.range_end) {
            return Ok(Some(u64::from(range_end)));
        }
        let boundary = self
            .clickhouse
            .get_worker_progress(
                self.config.chain_id,
                &self.config.history_boundary_worker_id,
            )
            .await?
            .ok_or_else(|| {
                LedgerError::AnalysisInvariant(format!(
                    "history boundary worker {} has no persisted progress",
                    self.config.history_boundary_worker_id
                ))
            })?;
        Ok(Some(u64::from(boundary.range_start).saturating_sub(1)))
    }

    fn target_block(&self, latest_block: u64, range_end: Option<u64>) -> u64 {
        let lag = match self.config.worker_mode {
            IndexerWorkerMode::Live => self.config.live_head_lag_blocks,
            IndexerWorkerMode::History => self.config.history_head_lag_blocks,
            IndexerWorkerMode::Lazy => 0,
        };
        let safe_head = latest_block.saturating_sub(lag);
        range_end
            .map(|end_block| end_block.min(safe_head))
            .unwrap_or(safe_head)
    }

    fn initial_range_start(&self, target_block: u64) -> u64 {
        match self.config.worker_mode {
            IndexerWorkerMode::Live => target_block
                .saturating_sub(self.config.live_bootstrap_blocks.saturating_sub(1))
                .max(self.config.start_block),
            IndexerWorkerMode::History | IndexerWorkerMode::Lazy => self.config.start_block,
        }
    }

    fn progress_record(
        &self,
        last_indexed_block: u64,
        last_indexed_hash: String,
        range_start: u64,
        head_block: u64,
        retry_count: u32,
        last_error: String,
        last_success_at: Option<String>,
        range_end: Option<u64>,
    ) -> LedgerResult<IndexerWorkerProgress> {
        let last_indexed_block = u32::try_from(last_indexed_block).map_err(|_| {
            LedgerError::AnalysisInvariant("indexer cursor exceeds UInt32".to_owned())
        })?;
        let range_start = u32::try_from(range_start).map_err(|_| {
            LedgerError::AnalysisInvariant("indexer range start exceeds UInt32".to_owned())
        })?;
        let head_block = u32::try_from(head_block).map_err(|_| {
            LedgerError::AnalysisInvariant("indexer head exceeds UInt32".to_owned())
        })?;
        let range_end = range_end
            .map(|value| {
                u32::try_from(value).map_err(|_| {
                    LedgerError::AnalysisInvariant("indexer range end exceeds UInt32".to_owned())
                })
            })
            .transpose()?;
        Ok(IndexerWorkerProgress {
            chain_id: self.config.chain_id,
            worker_id: self.config.worker_id.clone(),
            worker_kind: self.config.worker_mode.as_str().to_owned(),
            last_indexed_block,
            last_indexed_hash,
            range_start,
            range_end,
            rpc_url: sanitized_rpc_url(&self.config.rpc_url),
            retry_count,
            last_error,
            last_success_at,
            head_block: Some(head_block),
            lag_blocks: Some(head_block.saturating_sub(last_indexed_block)),
        })
    }

    async fn record_worker_failure(&self, retry_count: u32, message: &str) -> LedgerResult<()> {
        let Some(mut progress) = self
            .clickhouse
            .get_worker_progress(self.config.chain_id, &self.config.worker_id)
            .await?
        else {
            return Ok(());
        };
        progress.retry_count = retry_count;
        progress.last_error = message.chars().take(1_000).collect();
        self.clickhouse.upsert_worker_progress(&progress).await
    }

    async fn persist_full_block_facts(&self, blocks: &[IndexedBlockFacts]) -> LedgerResult<()> {
        let refs = blocks
            .iter()
            .flat_map(|block| block.refs.iter().cloned())
            .collect::<Vec<_>>();
        let transactions = blocks
            .iter()
            .flat_map(|block| block.transactions.iter().cloned())
            .collect::<Vec<_>>();
        let logs = blocks
            .iter()
            .flat_map(|block| block.logs.iter().cloned())
            .collect::<Vec<_>>();
        let indexed_blocks = blocks
            .iter()
            .map(|block| IndexedBlock {
                chain_id: block.block.chain_id,
                block_number: block.block.block_number,
                block_hash: block.block.block_hash.clone(),
            })
            .collect::<Vec<_>>();
        let canonical_blocks = blocks
            .iter()
            .map(|block| block.block.clone())
            .collect::<Vec<_>>();

        self.clickhouse.insert_refs(&refs).await?;
        self.clickhouse
            .insert_raw_transactions(&transactions)
            .await?;
        self.clickhouse.insert_raw_logs(&logs).await?;
        self.clickhouse
            .insert_indexed_blocks(&indexed_blocks)
            .await?;
        self.clickhouse
            .insert_canonical_blocks(&canonical_blocks)
            .await
    }

    async fn persist_sparse_block_facts(&self, blocks: &[IndexedBlockFacts]) -> LedgerResult<()> {
        let refs = blocks
            .iter()
            .flat_map(|block| block.refs.iter().cloned())
            .collect::<Vec<_>>();
        let indexed_blocks = blocks
            .iter()
            .map(|block| IndexedBlock {
                chain_id: block.block.chain_id,
                block_number: block.block.block_number,
                block_hash: block.block.block_hash.clone(),
            })
            .collect::<Vec<_>>();
        let canonical_blocks = blocks
            .iter()
            .map(|block| block.block.clone())
            .collect::<Vec<_>>();
        self.clickhouse.insert_refs(&refs).await?;
        self.clickhouse
            .insert_indexed_blocks(&indexed_blocks)
            .await?;
        self.clickhouse
            .insert_canonical_blocks(&canonical_blocks)
            .await
    }

    async fn reconcile_live_tip(
        &self,
        progress: &IndexerWorkerProgress,
        latest_block: u64,
    ) -> LedgerResult<Option<IndexerWorkerProgress>> {
        let old_tip = u64::from(progress.last_indexed_block);
        if old_tip <= latest_block {
            let remote_tip = self.get_block_header_with_retry(old_tip).await?;
            let remote_hash = block_hash_from_payload(&remote_tip, old_tip)?;
            if remote_hash == progress.last_indexed_hash {
                return Ok(None);
            }
        }

        let search_tip = old_tip.min(latest_block);
        let search_floor = search_tip.saturating_sub(self.config.reorg_lookback_blocks);
        let mut common_ancestor = None;
        for block_number in (search_floor..=search_tip).rev() {
            let block_number_u32 = u32::try_from(block_number).map_err(|_| {
                LedgerError::AnalysisInvariant("reorg search block exceeds UInt32".to_owned())
            })?;
            let Some(stored) = self
                .clickhouse
                .get_canonical_block(self.config.chain_id, block_number_u32)
                .await?
            else {
                continue;
            };
            let remote = self.get_block_header_with_retry(block_number).await?;
            if block_hash_from_payload(&remote, block_number)? == stored.block_hash {
                common_ancestor = Some(stored);
                break;
            }
        }

        let ancestor = common_ancestor.ok_or_else(|| {
            LedgerError::AnalysisInvariant(format!(
                "no common canonical ancestor found within {} blocks of {search_tip}",
                self.config.reorg_lookback_blocks
            ))
        })?;
        let orphan_from = ancestor.block_number.saturating_add(1);
        let old_tip_u32 = u32::try_from(old_tip)
            .map_err(|_| LedgerError::AnalysisInvariant("reorg tip exceeds UInt32".to_owned()))?;
        let orphaned = self
            .clickhouse
            .mark_canonical_range_orphaned(self.config.chain_id, orphan_from, old_tip_u32)
            .await?;
        let latest_u32 = u32::try_from(latest_block)
            .map_err(|_| LedgerError::AnalysisInvariant("reorg head exceeds UInt32".to_owned()))?;
        let reorganization = ChainReorganization {
            event_id: Uuid::new_v4().to_string(),
            chain_id: self.config.chain_id,
            worker_id: self.config.worker_id.clone(),
            previous_tip: old_tip_u32,
            common_ancestor: ancestor.block_number,
            new_head: latest_u32,
            depth: old_tip_u32.saturating_sub(ancestor.block_number),
        };
        self.clickhouse
            .insert_reorganization(&reorganization)
            .await?;
        let rewound = self.progress_record(
            u64::from(ancestor.block_number),
            ancestor.block_hash.clone(),
            u64::from(progress.range_start),
            latest_block,
            0,
            String::new(),
            Some(now_clickhouse()),
            self.config.end_block,
        )?;
        self.clickhouse.upsert_worker_progress(&rewound).await?;
        warn!(
            previous_tip = old_tip_u32,
            common_ancestor = ancestor.block_number,
            depth = reorganization.depth,
            orphaned_blocks = orphaned,
            "chain reorganization detected; live cursor rewound"
        );
        Ok(Some(rewound))
    }

    async fn run_lazy_worker(&self) -> LedgerResult<()> {
        info!(
            batch_size = self.config.batch_size,
            "indexer lazy worker waiting for requested ranges"
        );
        loop {
            let Some(mut request) = self
                .clickhouse
                .next_indexing_request(self.config.chain_id)
                .await?
            else {
                sleep(Duration::from_millis(self.config.poll_interval_ms)).await;
                continue;
            };

            if let Err(error) = validate_lazy_range(
                request.from_block,
                request.to_block,
                self.config.lazy_max_range_blocks,
            ) {
                request.status = "failed".to_owned();
                request.last_error = error.to_string();
                self.clickhouse.upsert_indexing_request(&request).await?;
                warn!(
                    request_id = request.request_id,
                    from_block = request.from_block,
                    to_block = request.to_block,
                    max_range_blocks = self.config.lazy_max_range_blocks,
                    "rejected oversized lazy indexing request"
                );
                continue;
            }

            if request.next_block > request.to_block {
                request.status = "completed".to_owned();
                request.last_error.clear();
                self.clickhouse.upsert_indexing_request(&request).await?;
                continue;
            }

            request.status = "running".to_owned();
            request.last_error.clear();
            self.clickhouse.upsert_indexing_request(&request).await?;

            let batch_size = u32::try_from(self.config.batch_size.max(1)).unwrap_or(u32::MAX);
            let batch_end = request.to_block.min(
                request
                    .next_block
                    .saturating_add(batch_size.saturating_sub(1)),
            );
            let indexed = self
                .clickhouse
                .indexed_block_numbers(self.config.chain_id, request.next_block, batch_end)
                .await?;
            let incomplete_receipt_blocks = if self.config.history_include_receipts {
                self.clickhouse
                    .block_numbers_missing_receipts(
                        self.config.chain_id,
                        request.next_block,
                        batch_end,
                    )
                    .await?
            } else {
                Vec::new()
            };
            let legacy_progress = self
                .clickhouse
                .get_legacy_progress(self.config.chain_id, &self.config.worker_id)
                .await?;
            let mut missing_blocks = missing_block_numbers(
                request.next_block,
                batch_end,
                u32::try_from(self.config.start_block).ok(),
                legacy_progress.and_then(|value| u32::try_from(value).ok()),
                &indexed.into_iter().collect(),
            );
            missing_blocks.extend(incomplete_receipt_blocks);
            missing_blocks.sort_unstable();
            missing_blocks.dedup();

            info!(
                request_id = request.request_id,
                start_block = request.next_block,
                end_block = batch_end,
                missing_block_count = missing_blocks.len(),
                "lazy index batch"
            );

            if !missing_blocks.is_empty() {
                match self
                    .fetch_block_numbers(&missing_blocks, self.config.history_include_receipts)
                    .await
                {
                    Ok(blocks) => {
                        if self.config.history_include_receipts {
                            self.persist_full_block_facts(&blocks).await?;
                        } else {
                            self.persist_sparse_block_facts(&blocks).await?;
                        }
                    }
                    Err(error) => {
                        request.last_error = error.to_string();
                        self.clickhouse.upsert_indexing_request(&request).await?;
                        return Err(error);
                    }
                }
            }

            let completed = batch_end >= request.to_block;
            request.next_block = if completed {
                request.to_block
            } else {
                batch_end + 1
            };
            request.status = if completed { "completed" } else { "running" }.to_owned();
            request.last_error.clear();
            self.clickhouse.upsert_indexing_request(&request).await?;
        }
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

    async fn fetch_block_range(
        &self,
        start_block: u64,
        end_block: u64,
        include_receipts: bool,
    ) -> LedgerResult<Vec<IndexedBlockFacts>> {
        let block_numbers = (start_block..=end_block)
            .map(|block_number| {
                u32::try_from(block_number).map_err(|_| {
                    LedgerError::AnalysisInvariant(format!(
                        "block number exceeds UInt32: {block_number}"
                    ))
                })
            })
            .collect::<LedgerResult<Vec<_>>>()?;
        self.fetch_block_numbers(&block_numbers, include_receipts)
            .await
    }

    async fn fetch_block_numbers(
        &self,
        block_numbers: &[u32],
        include_receipts: bool,
    ) -> LedgerResult<Vec<IndexedBlockFacts>> {
        if block_numbers.is_empty() {
            return Ok(Vec::new());
        }
        let block_concurrency = self.config.rpc_concurrency.max(1);
        let request_size = self
            .config
            .rpc_batch_request_size
            .max(1)
            .min(block_concurrency);
        let request_concurrency = (block_concurrency / request_size).max(1);
        let mut join_set = JoinSet::new();
        let groups = block_numbers
            .chunks(request_size)
            .map(|chunk| chunk.iter().copied().map(u64::from).collect::<Vec<_>>())
            .collect::<Vec<_>>();
        let mut blocks = Vec::new();
        let mut next_group = 0usize;

        while next_group < groups.len() || !join_set.is_empty() {
            while next_group < groups.len() && join_set.len() < request_concurrency {
                let rpc = self.rpc.clone();
                let config = self.config.clone();
                let chain_id = self.config.chain_id;
                let block_numbers = groups[next_group].clone();
                join_set.spawn(async move {
                    fetch_block_group_with_retry(
                        rpc,
                        config,
                        chain_id,
                        block_numbers,
                        include_receipts,
                    )
                    .await
                });
                next_group += 1;
            }

            match join_set.join_next().await {
                Some(Ok(Ok(block_results))) => blocks.extend(block_results),
                Some(Ok(Err(err))) => return Err(err),
                Some(Err(err)) => {
                    return Err(LedgerError::TransientIngestion(format!(
                        "indexer fetch task join error: {err}"
                    )));
                }
                None => break,
            }
        }

        blocks.sort_by_key(|block| block.block.block_number);
        Ok(blocks)
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

    async fn get_block_header_with_retry(
        &self,
        block_number: u64,
    ) -> LedgerResult<serde_json::Value> {
        let mut attempt = 0_u32;
        loop {
            match self.rpc.get_block_header(block_number).await {
                Ok(payload) if payload.is_null() => {
                    return Err(LedgerError::TransientIngestion(format!(
                        "block {block_number} returned null header"
                    )));
                }
                Ok(payload) => return Ok(payload),
                Err(err) if is_rate_limit_error(&err) && attempt < self.config.rpc_max_retries => {
                    let delay_ms = retry_delay_ms(
                        self.config.rpc_retry_base_ms,
                        self.config.rpc_retry_max_ms,
                        attempt,
                    );
                    sleep(Duration::from_millis(delay_ms)).await;
                    attempt += 1;
                }
                Err(err) => return Err(err),
            }
        }
    }
}

fn validate_block_chain(
    blocks: &[IndexedBlockFacts],
    expected_parent_hash: Option<&str>,
) -> LedgerResult<()> {
    let Some(first) = blocks.first() else {
        return Err(LedgerError::AnalysisInvariant(
            "cannot validate an empty block batch".to_owned(),
        ));
    };
    if let Some(expected_parent_hash) = expected_parent_hash
        && first.block.parent_hash != expected_parent_hash
    {
        return Err(LedgerError::TransientIngestion(format!(
            "block {} parent {} does not match stored canonical parent {expected_parent_hash}",
            first.block.block_number, first.block.parent_hash
        )));
    }
    for pair in blocks.windows(2) {
        let parent = &pair[0].block;
        let child = &pair[1].block;
        if child.block_number != parent.block_number.saturating_add(1) {
            return Err(LedgerError::TransientIngestion(format!(
                "non-contiguous block batch: {} followed by {}",
                parent.block_number, child.block_number
            )));
        }
        if child.parent_hash != parent.block_hash {
            return Err(LedgerError::TransientIngestion(format!(
                "block {} parent {} does not match block {} hash {}",
                child.block_number, child.parent_hash, parent.block_number, parent.block_hash
            )));
        }
    }
    Ok(())
}

fn block_hash_from_payload(payload: &serde_json::Value, block_number: u64) -> LedgerResult<String> {
    payload
        .get("hash")
        .and_then(serde_json::Value::as_str)
        .map(str::to_ascii_lowercase)
        .ok_or_else(|| {
            LedgerError::MalformedEvidence(format!(
                "block {block_number} header payload missing hash"
            ))
        })
}

fn sanitized_rpc_url(value: &str) -> String {
    let Ok(mut url) = reqwest::Url::parse(value) else {
        return value.split('?').next().unwrap_or(value).to_owned();
    };
    let _ = url.set_username("");
    let _ = url.set_password(None);
    url.set_query(None);
    url.set_fragment(None);
    url.to_string()
}

fn now_clickhouse() -> String {
    Utc::now().format("%Y-%m-%d %H:%M:%S%.3f").to_string()
}

fn hash_api_key(token: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(token.as_bytes());
    hex::encode(hasher.finalize())
}

async fn fetch_block_group_with_retry(
    rpc: Arc<IndexerRpcClient>,
    config: IndexerConfig,
    chain_id: u32,
    block_numbers: Vec<u64>,
    include_receipts: bool,
) -> LedgerResult<Vec<IndexedBlockFacts>> {
    let mut attempt = 0_u32;
    loop {
        if config.rpc_request_delay_ms > 0 {
            sleep(Duration::from_millis(config.rpc_request_delay_ms)).await;
        }
        let fetch_result = if include_receipts {
            let result = if config.worker_mode == IndexerWorkerMode::History
                && config.history_receipt_mode == IndexerHistoryReceiptMode::Transaction
            {
                rpc.get_blocks_with_transactions_and_transaction_receipts(
                    &block_numbers,
                    config.rpc_request_delay_ms,
                )
                .await
            } else {
                rpc.get_blocks_with_transactions_and_receipts(&block_numbers)
                    .await
            };
            result.map(|items| {
                items
                    .into_iter()
                    .map(|(block_number, block, receipts)| (block_number, block, Some(receipts)))
                    .collect::<Vec<_>>()
            })
        } else {
            rpc.get_blocks_with_transactions(&block_numbers)
                .await
                .map(|items| {
                    items
                        .into_iter()
                        .map(|(block_number, block)| (block_number, block, None))
                        .collect::<Vec<_>>()
                })
        };
        match fetch_result {
            Ok(block_payloads) => {
                let mut blocks = Vec::new();
                for (block_number, block_payload, receipts_payload) in block_payloads {
                    if block_payload.is_null() {
                        warn!(block_number, "block payload missing, retry later");
                        return Err(LedgerError::TransientIngestion(format!(
                            "block {block_number} returned null payload"
                        )));
                    }
                    let expected_block_number = u32::try_from(block_number).map_err(|_| {
                        LedgerError::AnalysisInvariant("indexed block exceeds UInt32".to_owned())
                    })?;
                    if receipts_payload
                        .as_ref()
                        .is_some_and(serde_json::Value::is_null)
                    {
                        return Err(LedgerError::TransientIngestion(format!(
                            "block {block_number} returned null receipts"
                        )));
                    }
                    let facts = extract_block_facts(
                        chain_id,
                        &config.worker_id,
                        &block_payload,
                        receipts_payload.as_ref(),
                    )?;
                    if facts.block.block_number != expected_block_number {
                        return Err(LedgerError::MalformedEvidence(format!(
                            "requested block {expected_block_number} but RPC returned {}",
                            facts.block.block_number
                        )));
                    }
                    blocks.push(facts);
                }
                return Ok(blocks);
            }
            Err(err) if is_rate_limit_error(&err) && attempt < config.rpc_max_retries => {
                let delay_ms =
                    retry_delay_ms(config.rpc_retry_base_ms, config.rpc_retry_max_ms, attempt);
                warn!(
                    start_block = block_numbers.first().copied().unwrap_or_default(),
                    end_block = block_numbers.last().copied().unwrap_or_default(),
                    attempt = attempt + 1,
                    delay_ms,
                    error = %err,
                    "block group fetch hit rate limit, retrying"
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

fn lazy_request_id(chain_id: u32, from_block: u32, to_block: u32) -> String {
    let mut hasher = Sha256::new();
    hasher.update(format!("{chain_id}:{from_block}:{to_block}").as_bytes());
    format!("lazy_{}", hex::encode(hasher.finalize()))
}

fn validate_lazy_range(from_block: u32, to_block: u32, max_range_blocks: u64) -> LedgerResult<u64> {
    let requested_blocks = u64::from(to_block)
        .saturating_sub(u64::from(from_block))
        .saturating_add(1);
    if requested_blocks > max_range_blocks.max(1) {
        return Err(LedgerError::MalformedEvidence(format!(
            "lazy index range contains {requested_blocks} blocks; maximum is {}. Provide an explicit bounded from_block/to_block range or use the opt-in history worker",
            max_range_blocks.max(1)
        )));
    }
    Ok(requested_blocks)
}

fn legacy_coverage_range(
    configured_start: Option<u32>,
    legacy_progress: Option<u32>,
    from_block: u32,
    to_block: u32,
) -> Option<(u32, u32)> {
    let start = configured_start?.max(from_block);
    let end = legacy_progress?.min(to_block);
    (start <= end).then_some((start, end))
}

fn missing_block_numbers(
    from_block: u32,
    to_block: u32,
    configured_start: Option<u32>,
    legacy_progress: Option<u32>,
    indexed_blocks: &BTreeSet<u32>,
) -> Vec<u32> {
    let legacy_range =
        legacy_coverage_range(configured_start, legacy_progress, from_block, to_block);
    (from_block..=to_block)
        .filter(|block_number| {
            let covered_by_legacy = legacy_range
                .map(|(start, end)| *block_number >= start && *block_number <= end)
                .unwrap_or(false);
            !covered_by_legacy && !indexed_blocks.contains(block_number)
        })
        .collect()
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

#[cfg(test)]
mod tests {
    use super::{
        lazy_request_id, legacy_coverage_range, missing_block_numbers, sanitized_rpc_url,
        validate_block_chain, validate_lazy_range,
    };
    use crate::indexer::models::{CanonicalBlock, IndexedBlockFacts};
    use std::collections::BTreeSet;

    #[test]
    fn lazy_request_id_is_shared_by_addresses_for_the_same_range() {
        assert_eq!(
            lazy_request_id(8453, 100, 200),
            lazy_request_id(8453, 100, 200)
        );
        assert_ne!(
            lazy_request_id(8453, 100, 200),
            lazy_request_id(8453, 100, 201)
        );
    }

    #[test]
    fn lazy_ranges_are_bounded_and_never_become_implicit_full_history() {
        assert_eq!(validate_lazy_range(100, 199, 100).unwrap(), 100);
        let error = validate_lazy_range(0, 250_000, 250_000).unwrap_err();
        assert!(error.to_string().contains("maximum is 250000"));
    }

    #[test]
    fn legacy_progress_contributes_only_its_contiguous_interval() {
        assert_eq!(
            legacy_coverage_range(Some(50), Some(120), 0, 200),
            Some((50, 120))
        );
        assert_eq!(legacy_coverage_range(Some(50), Some(120), 121, 200), None);
    }

    #[test]
    fn missing_blocks_exclude_legacy_and_lazy_coverage() {
        let indexed = BTreeSet::from([6, 8]);
        assert_eq!(
            missing_block_numbers(1, 10, Some(1), Some(5), &indexed),
            vec![7, 9, 10]
        );
    }

    #[test]
    fn block_chain_validation_rejects_a_changed_parent_hash() {
        let blocks = vec![
            block_facts(10, "0x10", "0x09"),
            block_facts(11, "0x11", "0xdead"),
        ];
        let error = validate_block_chain(&blocks, Some("0x09")).unwrap_err();
        assert!(error.to_string().contains("does not match block 10 hash"));
    }

    #[test]
    fn block_chain_validation_accepts_contiguous_canonical_blocks() {
        let blocks = vec![
            block_facts(10, "0x10", "0x09"),
            block_facts(11, "0x11", "0x10"),
        ];
        validate_block_chain(&blocks, Some("0x09")).unwrap();
    }

    #[test]
    fn progress_rpc_url_drops_credentials_and_query_parameters() {
        assert_eq!(
            sanitized_rpc_url("https://user:secret@example.com/rpc?token=hidden"),
            "https://example.com/rpc"
        );
    }

    fn block_facts(block_number: u32, block_hash: &str, parent_hash: &str) -> IndexedBlockFacts {
        IndexedBlockFacts {
            block: CanonicalBlock {
                chain_id: 4663,
                block_number,
                block_hash: block_hash.to_owned(),
                parent_hash: parent_hash.to_owned(),
                timestamp: 1,
                l1_block_number: Some(1),
                finality_state: "soft_confirmed".to_owned(),
                is_canonical: 1,
                worker_id: "robinhood_live".to_owned(),
            },
            transactions: Vec::new(),
            logs: Vec::new(),
            refs: Vec::new(),
        }
    }
}
