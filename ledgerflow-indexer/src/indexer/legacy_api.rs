use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    routing::get,
};
use serde::Deserialize;
use serde_json::json;

use crate::core::errors::LedgerError;
use crate::indexer::models::{AddressTxDirection, AddressTxHashesResponse, AddressTxQueryResponse};
use crate::indexer::service::IndexerService;

#[derive(Clone)]
pub struct LegacyApiState {
    pub service: Arc<IndexerService>,
}

#[derive(Debug, Deserialize)]
pub struct AddressTxQuery {
    pub limit: Option<u32>,
    pub from_block: Option<u32>,
    pub to_block: Option<u32>,
    pub direction: Option<String>,
}

pub fn router(service: Arc<IndexerService>) -> Router {
    Router::new()
        .route("/progress", get(progress))
        .route("/addresses/{address}/txs", get(query_address_txs))
        .route(
            "/addresses/{address}/tx-hashes",
            get(query_address_tx_hashes),
        )
        .with_state(LegacyApiState { service })
}

async fn progress(
    State(state): State<LegacyApiState>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let progress = state.service.get_progress().await.map_err(ApiError::from)?;
    Ok(Json(json!({
        "chain_id": state.service.config.chain_id,
        "worker_id": state.service.config.worker_id,
        "last_indexed_block": progress,
    })))
}

async fn query_address_txs(
    State(state): State<LegacyApiState>,
    Path(address): Path<String>,
    Query(query): Query<AddressTxQuery>,
) -> Result<Json<AddressTxQueryResponse>, ApiError> {
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
    Ok(Json(response))
}

async fn query_address_tx_hashes(
    State(state): State<LegacyApiState>,
    Path(address): Path<String>,
    Query(query): Query<AddressTxQuery>,
) -> Result<Json<AddressTxHashesResponse>, ApiError> {
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
    Ok(Json(response))
}

pub fn parse_direction(value: Option<&str>) -> Result<Option<AddressTxDirection>, ApiError> {
    match value {
        None | Some("any") => Ok(None),
        Some("from") => Ok(Some(AddressTxDirection::From)),
        Some("to") => Ok(Some(AddressTxDirection::To)),
        Some(value) => Err(ApiError::conflict(format!(
            "unsupported direction: {value}"
        ))),
    }
}

pub struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    pub fn conflict(message: String) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            message,
        }
    }
}

impl From<LedgerError> for ApiError {
    fn from(value: LedgerError) -> Self {
        match value {
            LedgerError::MalformedEvidence(message) => Self {
                status: StatusCode::BAD_REQUEST,
                message,
            },
            LedgerError::Conflict(message) => Self {
                status: StatusCode::CONFLICT,
                message,
            },
            LedgerError::TransientIngestion(message) | LedgerError::Persistence(message) => Self {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                message,
            },
        }
    }
}

impl axum::response::IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        (
            self.status,
            Json(json!({
                "error": self.message,
            })),
        )
            .into_response()
    }
}
