use async_trait::async_trait;
use reqwest::Client;
use serde::Deserialize;

use crate::{
    core::errors::{LedgerError, LedgerResult},
    domain::{interfaces::ClusterTxSource, value_objects::AddressTxRef},
};

#[derive(Clone)]
pub struct IndexerHttpClient {
    base_url: String,
    client: Client,
}

impl IndexerHttpClient {
    pub fn new(base_url: String) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_owned(),
            client: Client::new(),
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
}

#[async_trait]
impl ClusterTxSource for IndexerHttpClient {
    async fn get_last_indexed_block(&self, _chain_id: u64) -> LedgerResult<u64> {
        let response = self
            .client
            .get(format!("{}/progress", self.base_url))
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
        let mut request = self
            .client
            .get(format!("{}/addresses/{}/txs", self.base_url, address));
        request = request.query(&[("limit", limit)]);
        if let Some(from_block) = from_block {
            request = request.query(&[("from_block", from_block)]);
        }
        if let Some(to_block) = to_block {
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
        Ok(payload.refs)
    }
}
