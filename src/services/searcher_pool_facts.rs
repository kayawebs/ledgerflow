use std::{collections::BTreeSet, time::Duration};

use reqwest::Client;
use serde::{Deserialize, Serialize};

use crate::core::errors::{LedgerError, LedgerResult};

/// This optional contract covers only swaps for pools LedgerFlow already knows. Launch events,
/// curve trades, token metadata, prices, and market interpretation remain LedgerFlow-owned.
pub const SEARCHER_POOL_FACTS_SCHEMA: &str = "searcher.pool-facts.v1";

#[derive(Clone)]
pub struct SearcherPoolFactsClient {
    base_url: String,
    bearer_token: Option<String>,
    client: Client,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PoolFactsCapabilities {
    pub schema_version: String,
    pub service_version: String,
    pub chain_ids: Vec<u64>,
    pub datasets: Vec<String>,
    pub retention_from_block: Option<u64>,
    pub max_page_size: u32,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct PoolSwapFactsQuery {
    pub chain_id: u64,
    pub from_block: u64,
    pub to_block: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pool_addresses: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pool_ids: Vec<String>,
    pub after: Option<PoolFactCursor>,
    pub limit: u32,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct PoolFactCursor {
    pub block_number: u64,
    pub tx_index: u32,
    pub log_index: u32,
    pub fact_id: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PoolSwapFactsPage {
    pub schema_version: String,
    pub chain_id: u64,
    pub source_head: u64,
    pub safe_head: u64,
    pub facts: Vec<SearcherPoolSwapFact>,
    pub next_cursor: Option<PoolFactCursor>,
    pub has_more: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SearcherPoolSwapFact {
    pub fact_id: String,
    pub chain_id: u64,
    pub block_number: u64,
    pub block_hash: String,
    pub block_timestamp: String,
    pub tx_hash: String,
    pub tx_index: u32,
    pub log_index: u32,
    pub protocol_key: Option<String>,
    pub pool_address: Option<String>,
    pub pool_id: Option<String>,
    pub token0_address: String,
    pub token1_address: String,
    pub amount0_raw: String,
    pub amount1_raw: String,
    pub sqrt_price_x96: Option<String>,
    pub tick: Option<i32>,
    pub liquidity_raw: Option<String>,
    pub fee_pips: Option<u32>,
    pub canonical: bool,
    pub finalized: bool,
    pub decoder_version: String,
}

impl SearcherPoolFactsClient {
    pub fn new(
        base_url: impl Into<String>,
        bearer_token: Option<String>,
        timeout: Duration,
    ) -> LedgerResult<Self> {
        let client = Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|error| LedgerError::TransientIngestion(error.to_string()))?;
        Ok(Self {
            base_url: base_url.into().trim_end_matches('/').to_owned(),
            bearer_token,
            client,
        })
    }

    pub async fn capabilities(&self) -> LedgerResult<PoolFactsCapabilities> {
        let request = self
            .client
            .get(format!("{}/v1/pool-facts/capabilities", self.base_url));
        decode_response(
            self.authorize(request)
                .send()
                .await
                .map_err(network_error)?,
        )
        .await
    }

    pub async fn query_swaps(&self, query: &PoolSwapFactsQuery) -> LedgerResult<PoolSwapFactsPage> {
        validate_query(query)?;
        let request = self
            .client
            .post(format!("{}/v1/pool-facts/swaps/query", self.base_url))
            .json(query);
        let page: PoolSwapFactsPage = decode_response(
            self.authorize(request)
                .send()
                .await
                .map_err(network_error)?,
        )
        .await?;
        validate_page(query, &page)?;
        Ok(page)
    }

    fn authorize(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.bearer_token {
            Some(token) => request.bearer_auth(token),
            None => request,
        }
    }
}

fn validate_query(query: &PoolSwapFactsQuery) -> LedgerResult<()> {
    if query.chain_id == 0 || query.from_block > query.to_block {
        return Err(LedgerError::MalformedEvidence(
            "invalid Searcher pool-fact block range".to_owned(),
        ));
    }
    if query.limit == 0 || query.limit > 10_000 {
        return Err(LedgerError::MalformedEvidence(
            "Searcher pool-fact limit must be within 1..=10000".to_owned(),
        ));
    }
    if query.pool_addresses.is_empty() && query.pool_ids.is_empty() {
        return Err(LedgerError::MalformedEvidence(
            "Searcher pool-fact query requires a pool address or PoolId".to_owned(),
        ));
    }
    Ok(())
}

fn validate_page(query: &PoolSwapFactsQuery, page: &PoolSwapFactsPage) -> LedgerResult<()> {
    if page.schema_version != SEARCHER_POOL_FACTS_SCHEMA || page.chain_id != query.chain_id {
        return Err(LedgerError::MalformedEvidence(
            "Searcher pool-fact response schema or chain mismatch".to_owned(),
        ));
    }
    let mut seen = BTreeSet::new();
    let mut previous: Option<PoolFactCursor> = None;
    for fact in &page.facts {
        if fact.chain_id != query.chain_id
            || fact.block_number < query.from_block
            || fact.block_number > query.to_block
            || !fact.canonical
            || !fact.finalized
            || !seen.insert(&fact.fact_id)
        {
            return Err(LedgerError::MalformedEvidence(format!(
                "invalid Searcher pool fact {}",
                fact.fact_id
            )));
        }
        let cursor = PoolFactCursor {
            block_number: fact.block_number,
            tx_index: fact.tx_index,
            log_index: fact.log_index,
            fact_id: fact.fact_id.clone(),
        };
        if previous.as_ref().is_some_and(|value| value >= &cursor) {
            return Err(LedgerError::MalformedEvidence(
                "Searcher pool facts are not strictly ordered".to_owned(),
            ));
        }
        previous = Some(cursor);
    }
    Ok(())
}

async fn decode_response<T: for<'de> Deserialize<'de>>(
    response: reqwest::Response,
) -> LedgerResult<T> {
    let status = response.status();
    let body = response.text().await.map_err(network_error)?;
    if !status.is_success() {
        return Err(LedgerError::TransientIngestion(format!(
            "Searcher pool facts returned HTTP {status}"
        )));
    }
    serde_json::from_str(&body).map_err(|error| {
        LedgerError::MalformedEvidence(format!("invalid Searcher pool facts response: {error}"))
    })
}

fn network_error(error: reqwest::Error) -> LedgerError {
    LedgerError::TransientIngestion(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requires_a_known_pool_scope() {
        let query = PoolSwapFactsQuery {
            chain_id: 4663,
            from_block: 100,
            to_block: 200,
            limit: 100,
            ..PoolSwapFactsQuery::default()
        };
        assert!(validate_query(&query).is_err());
    }

    #[test]
    fn accepts_pool_address_scope() {
        let query = PoolSwapFactsQuery {
            chain_id: 4663,
            from_block: 100,
            to_block: 200,
            pool_addresses: vec!["0x1111111111111111111111111111111111111111".to_owned()],
            limit: 100,
            ..PoolSwapFactsQuery::default()
        };
        validate_query(&query).expect("known pool scope");
    }
}
