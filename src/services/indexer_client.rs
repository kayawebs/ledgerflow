use async_trait::async_trait;
use chrono::{DateTime, NaiveDateTime, Utc};
use reqwest::Client;
use serde::Deserialize;
use tokio::time::{Duration, sleep};

use crate::{
    core::errors::{LedgerError, LedgerResult},
    domain::{
        interfaces::ClusterTxSource,
        value_objects::{AddressTxDiscovery, AddressTxRef, ChainDataSourceStatus},
    },
};

#[derive(Clone)]
pub struct IndexerHttpClient {
    base_url: String,
    bearer_token: Option<String>,
    client: Client,
    known_only: bool,
}

impl IndexerHttpClient {
    pub fn new(base_url: String, bearer_token: Option<String>) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_owned(),
            bearer_token,
            client: Client::new(),
            known_only: false,
        }
    }

    /// Address-intelligence discovery must not turn a bounded object lookup
    /// into a full block-range backfill. This client mode reads existing live
    /// and lazy coverage without enqueueing missing blocks.
    pub fn known_only(mut self) -> Self {
        self.known_only = true;
        self
    }

    fn apply_auth(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match self.bearer_token.as_deref() {
            Some(token) if !token.is_empty() => request.bearer_auth(token),
            _ => request,
        }
    }
}

#[derive(Debug, Deserialize)]
struct IndexerProgressResponse {
    chain_id: u64,
    last_indexed_block: u64,
    rpc_head_block: Option<u64>,
    live_lag_blocks: Option<u64>,
    coverage: Option<IndexerProgressCoverage>,
    workers: Option<Vec<IndexerProgressWorker>>,
}

#[derive(Debug, Deserialize)]
struct IndexerProgressCoverage {
    configured_start_block: Option<u64>,
    history_backfill_complete: Option<bool>,
    live_indexed_from_block: Option<u64>,
    live_indexed_through_block: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct IndexerProgressWorker {
    worker_kind: Option<String>,
    last_success_at: Option<String>,
}

#[derive(Debug, Deserialize)]
struct IndexerAddressRefsResponse {
    #[allow(dead_code)]
    address: String,
    refs: Vec<AddressTxRef>,
    coverage: Option<IndexerCoverageResponse>,
}

#[derive(Debug, Deserialize)]
struct IndexerCoverageResponse {
    from_block: u64,
    to_block: u64,
    indexed_block_count: u64,
    requested_block_count: u64,
    complete: bool,
    request_id: Option<String>,
}

#[async_trait]
impl ClusterTxSource for IndexerHttpClient {
    async fn get_last_indexed_block(&self, chain_id: u64) -> LedgerResult<u64> {
        Ok(self
            .get_data_source_status(chain_id)
            .await?
            .indexed_to_block
            .unwrap_or(0))
    }

    async fn get_data_source_status(&self, chain_id: u64) -> LedgerResult<ChainDataSourceStatus> {
        let response = self
            .apply_auth(self.client.get(format!("{}/progress", self.base_url)))
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
                "indexer http status {status}: {body}"
            )));
        }
        let payload: IndexerProgressResponse = serde_json::from_str(&body)
            .map_err(|err| LedgerError::TransientIngestion(err.to_string()))?;
        if payload.chain_id != chain_id {
            return Err(LedgerError::TransientIngestion(format!(
                "indexer data source chain mismatch: requested {chain_id}, received {}",
                payload.chain_id
            )));
        }
        let coverage = payload.coverage;
        let full_history_available = coverage
            .as_ref()
            .and_then(|value| value.history_backfill_complete)
            .unwrap_or(false);
        let indexed_from_block = coverage.as_ref().and_then(|value| {
            if full_history_available {
                value.configured_start_block
            } else {
                value.live_indexed_from_block
            }
        });
        let indexed_to_block = coverage
            .as_ref()
            .and_then(|value| value.live_indexed_through_block)
            .or(Some(payload.last_indexed_block));
        let last_updated_at = payload.workers.as_deref().and_then(|workers| {
            workers
                .iter()
                .filter(|worker| worker.worker_kind.as_deref() == Some("live"))
                .filter_map(|worker| {
                    worker
                        .last_success_at
                        .as_deref()
                        .and_then(parse_indexer_time)
                })
                .max()
        });
        let ingestion_lag = payload.live_lag_blocks.or_else(|| {
            payload
                .rpc_head_block
                .zip(indexed_to_block)
                .map(|(head, indexed)| head.saturating_sub(indexed))
        });
        let available = ingestion_lag.is_none_or(|lag| lag <= 64);
        Ok(ChainDataSourceStatus {
            chain_id,
            data_source: "live_canonical_index_with_address_lazy_history".to_owned(),
            indexed_from_block,
            indexed_to_block,
            chain_head: payload.rpc_head_block,
            ingestion_lag,
            last_updated_at,
            history_mode: if full_history_available {
                "contiguous_full_history_plus_live".to_owned()
            } else {
                "live_index_plus_bounded_address_lazy_history".to_owned()
            },
            full_history_available,
            available,
            limitation: if !available {
                ingestion_lag.map(|lag| {
                    format!("Live canonical ingestion is {lag} blocks behind the chain head.")
                })
            } else if !full_history_available {
                Some(
                    "Global full history is not contiguous; historical coverage is resolved per observed address."
                        .to_owned(),
                )
            } else {
                None
            },
        })
    }

    async fn query_address_refs(
        &self,
        chain_id: u64,
        address: &str,
        limit: u32,
        from_block: Option<u64>,
        to_block: Option<u64>,
    ) -> LedgerResult<Vec<AddressTxRef>> {
        Ok(self
            .query_address_refs_with_coverage(chain_id, address, limit, from_block, to_block)
            .await?
            .refs)
    }

    async fn query_address_refs_with_coverage(
        &self,
        _chain_id: u64,
        address: &str,
        limit: u32,
        from_block: Option<u64>,
        to_block: Option<u64>,
    ) -> LedgerResult<AddressTxDiscovery> {
        let mut fixed_from_block = from_block;
        let mut fixed_to_block = to_block;
        loop {
            let mut request = self.apply_auth(
                self.client
                    .get(format!("{}/addresses/{}/txs", self.base_url, address)),
            );
            request = request.query(&[("limit", limit)]);
            if self.known_only {
                request = request.query(&[("known_only", true)]);
            }
            if let Some(from_block) = fixed_from_block {
                request = request.query(&[("from_block", from_block)]);
            }
            if let Some(to_block) = fixed_to_block {
                request = request.query(&[("to_block", to_block)]);
            }

            let response = request
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
                    "indexer http status {status}: {body}"
                )));
            }
            let payload: IndexerAddressRefsResponse = serde_json::from_str(&body)
                .map_err(|err| LedgerError::TransientIngestion(err.to_string()))?;
            let pending = !self.known_only
                && (status == reqwest::StatusCode::ACCEPTED
                    || payload
                        .coverage
                        .as_ref()
                        .map(|coverage| !coverage.complete)
                        .unwrap_or(false));
            if !pending {
                return Ok(AddressTxDiscovery {
                    refs: payload.refs,
                    coverage_complete: payload.coverage.as_ref().map(|value| value.complete),
                    indexed_block_count: payload
                        .coverage
                        .as_ref()
                        .map(|value| value.indexed_block_count),
                    requested_block_count: payload
                        .coverage
                        .as_ref()
                        .map(|value| value.requested_block_count),
                });
            }
            if let Some(coverage) = payload.coverage {
                fixed_from_block = Some(coverage.from_block);
                fixed_to_block = Some(coverage.to_block);
                tracing::debug!(
                    address,
                    indexed_block_count = coverage.indexed_block_count,
                    requested_block_count = coverage.requested_block_count,
                    request_id = coverage.request_id.unwrap_or_else(|| "unknown".to_owned()),
                    "waiting for lazy index coverage"
                );
            }
            sleep(Duration::from_millis(2_000)).await;
        }
    }
}

fn parse_indexer_time(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|value| value.with_timezone(&Utc))
        .or_else(|| {
            NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f")
                .ok()
                .map(|value| value.and_utc())
        })
}
