use std::collections::BTreeMap;
use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::indexer::legacy_api::{ApiError, parse_direction};
use crate::indexer::models::AddressTxRef;
use crate::indexer::service::IndexerService;

#[derive(Clone)]
pub struct PublicApiState {
    pub service: Arc<IndexerService>,
}

#[derive(Debug, Deserialize)]
pub struct AddressTxQuery {
    pub limit: Option<u32>,
    pub from_block: Option<u32>,
    pub to_block: Option<u32>,
    pub direction: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct BatchAddressTxRequest {
    pub addresses: Vec<String>,
    pub limit: Option<u32>,
    pub from_block: Option<u32>,
    pub to_block: Option<u32>,
    pub direction: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct PublicRefsResponse {
    pub chain: &'static str,
    pub chain_id: u32,
    pub address: String,
    pub items: Vec<AddressTxRef>,
}

#[derive(Debug, Serialize)]
pub struct PublicHashesResponse {
    pub chain: &'static str,
    pub chain_id: u32,
    pub address: String,
    pub tx_hashes: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct BatchRefsResponse {
    pub chain: &'static str,
    pub chain_id: u32,
    pub items: Vec<AddressTxRef>,
    pub per_address_counts: BTreeMap<String, usize>,
}

pub fn router(service: Arc<IndexerService>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/v1/chains/base/progress", get(progress))
        .route("/v1/base/address/{address}/txs", get(query_address_txs))
        .route(
            "/v1/base/address/{address}/tx-hashes",
            get(query_address_tx_hashes),
        )
        .route("/v1/base/addresses/txs", post(batch_query_address_txs))
        .with_state(PublicApiState { service })
}

async fn health() -> Json<serde_json::Value> {
    Json(json!({ "status": "ok", "service": "ledgerflow-indexer" }))
}

async fn progress(
    State(state): State<PublicApiState>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let progress = state.service.get_progress().await.map_err(ApiError::from)?;
    Ok(Json(json!({
        "chain": "base",
        "chain_id": state.service.config.chain_id,
        "worker_id": state.service.config.worker_id,
        "last_indexed_block": progress,
    })))
}

async fn query_address_txs(
    State(state): State<PublicApiState>,
    Path(address): Path<String>,
    Query(query): Query<AddressTxQuery>,
) -> Result<Json<PublicRefsResponse>, ApiError> {
    let direction = parse_direction(query.direction.as_deref())?;
    let response = state
        .service
        .query_address(
            &address,
            query.limit,
            query.from_block,
            query.to_block,
            direction,
        )
        .await
        .map_err(ApiError::from)?;
    Ok(Json(PublicRefsResponse {
        chain: "base",
        chain_id: state.service.config.chain_id,
        address: response.address,
        items: response.refs,
    }))
}

async fn query_address_tx_hashes(
    State(state): State<PublicApiState>,
    Path(address): Path<String>,
    Query(query): Query<AddressTxQuery>,
) -> Result<Json<PublicHashesResponse>, ApiError> {
    let direction = parse_direction(query.direction.as_deref())?;
    let response = state
        .service
        .query_address_hashes(
            &address,
            query.limit,
            query.from_block,
            query.to_block,
            direction,
        )
        .await
        .map_err(ApiError::from)?;
    Ok(Json(PublicHashesResponse {
        chain: "base",
        chain_id: state.service.config.chain_id,
        address: response.address,
        tx_hashes: response.tx_hashes,
    }))
}

async fn batch_query_address_txs(
    State(state): State<PublicApiState>,
    Json(request): Json<BatchAddressTxRequest>,
) -> Result<Json<BatchRefsResponse>, ApiError> {
    let direction = parse_direction(request.direction.as_deref())?;
    let mut items = Vec::new();
    let mut per_address_counts = BTreeMap::new();

    for address in request.addresses {
        let response = state
            .service
            .query_address(
                &address,
                request.limit,
                request.from_block,
                request.to_block,
                direction.clone(),
            )
            .await
            .map_err(ApiError::from)?;
        per_address_counts.insert(response.address, response.refs.len());
        items.extend(response.refs);
    }

    items.sort_by(|left, right| {
        right
            .block_number
            .cmp(&left.block_number)
            .then(right.tx_index.cmp(&left.tx_index))
            .then(left.address.cmp(&right.address))
    });

    Ok(Json(BatchRefsResponse {
        chain: "base",
        chain_id: state.service.config.chain_id,
        items,
        per_address_counts,
    }))
}
