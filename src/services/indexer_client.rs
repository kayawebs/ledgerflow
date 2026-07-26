use async_trait::async_trait;
use reqwest::Client;
use serde::Deserialize;
use tokio::time::{Duration, sleep};

use crate::{
    core::errors::{LedgerError, LedgerResult},
    domain::{interfaces::ClusterTxSource, value_objects::AddressTxRef},
};

#[derive(Clone)]
pub struct IndexerHttpClient {
    base_url: String,
    bearer_token: Option<String>,
    client: Client,
}

impl IndexerHttpClient {
    pub fn new(base_url: String, bearer_token: Option<String>) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_owned(),
            bearer_token,
            client: Client::new(),
        }
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
    #[allow(dead_code)]
    chain_id: u64,
    last_indexed_block: u64,
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
    async fn get_last_indexed_block(&self, _chain_id: u64) -> LedgerResult<u64> {
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
        Ok(payload.last_indexed_block)
    }

    async fn query_address_refs(
        &self,
        _chain_id: u64,
        address: &str,
        limit: u32,
        from_block: Option<u64>,
        to_block: Option<u64>,
    ) -> LedgerResult<Vec<AddressTxRef>> {
        let mut fixed_from_block = from_block;
        let mut fixed_to_block = to_block;
        loop {
            let mut request = self.apply_auth(
                self.client
                    .get(format!("{}/addresses/{}/txs", self.base_url, address)),
            );
            request = request.query(&[("limit", limit)]);
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
            let pending = status == reqwest::StatusCode::ACCEPTED
                || payload
                    .coverage
                    .as_ref()
                    .map(|coverage| !coverage.complete)
                    .unwrap_or(false);
            if !pending {
                return Ok(payload.refs);
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
