use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    routing::{get, post},
};
use serde::Deserialize;
use serde_json::json;

use crate::core::errors::LedgerError;
use crate::indexer::models::{
    AddressTxDirection, AddressTxHashesResponse, AddressTxQueryResponse,
    CreateIndexerApiKeyRequest, CreateIndexerApiKeyResponse, IndexerApiKeyRecord,
    RevokeIndexerApiKeyRequest,
};
use crate::indexer::service::IndexerService;

#[derive(Clone)]
pub struct IndexerApiState {
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
pub struct ApiKeyListQuery {
    pub owner_user_id: String,
}

pub fn router(service: Arc<IndexerService>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/progress", get(progress))
        .route("/addresses/{address}/txs", get(query_address_txs))
        .route(
            "/addresses/{address}/tx-hashes",
            get(query_address_tx_hashes),
        )
        .route("/admin/api-keys", get(list_api_keys).post(create_api_key))
        .route("/admin/api-keys/{key_id}/revoke", post(revoke_api_key))
        .with_state(IndexerApiState { service })
}

async fn health() -> Json<serde_json::Value> {
    Json(json!({ "status": "ok" }))
}

async fn progress(
    State(state): State<IndexerApiState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, ApiError> {
    state
        .service
        .authorize_public_token(extract_api_token(&headers))
        .await
        .map_err(ApiError::from)?;
    let progress = state.service.get_progress().await.map_err(ApiError::from)?;
    let endpoint_backfill_pending_rows = state
        .service
        .get_endpoint_backfill_pending_rows()
        .await
        .map_err(ApiError::from)?;
    Ok(Json(json!({
        "chain_id": state.service.config.chain_id,
        "worker_id": state.service.config.worker_id,
        "last_indexed_block": progress,
        "endpoint_backfill_pending_rows": endpoint_backfill_pending_rows,
    })))
}

async fn query_address_txs(
    State(state): State<IndexerApiState>,
    headers: HeaderMap,
    Path(address): Path<String>,
    Query(query): Query<AddressTxQuery>,
) -> Result<Json<AddressTxQueryResponse>, ApiError> {
    state
        .service
        .authorize_public_token(extract_api_token(&headers))
        .await
        .map_err(ApiError::from)?;
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
    State(state): State<IndexerApiState>,
    headers: HeaderMap,
    Path(address): Path<String>,
    Query(query): Query<AddressTxQuery>,
) -> Result<Json<AddressTxHashesResponse>, ApiError> {
    state
        .service
        .authorize_public_token(extract_api_token(&headers))
        .await
        .map_err(ApiError::from)?;
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

async fn list_api_keys(
    State(state): State<IndexerApiState>,
    headers: HeaderMap,
    Query(query): Query<ApiKeyListQuery>,
) -> Result<Json<Vec<IndexerApiKeyRecord>>, ApiError> {
    require_internal_access(&state, &headers)?;
    let keys = state
        .service
        .list_api_keys(&query.owner_user_id)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(keys))
}

async fn create_api_key(
    State(state): State<IndexerApiState>,
    headers: HeaderMap,
    Json(request): Json<CreateIndexerApiKeyRequest>,
) -> Result<Json<CreateIndexerApiKeyResponse>, ApiError> {
    require_internal_access(&state, &headers)?;
    let response = state
        .service
        .create_api_key(request)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(response))
}

async fn revoke_api_key(
    State(state): State<IndexerApiState>,
    headers: HeaderMap,
    Path(key_id): Path<String>,
    Json(request): Json<RevokeIndexerApiKeyRequest>,
) -> Result<Json<IndexerApiKeyRecord>, ApiError> {
    require_internal_access(&state, &headers)?;
    let response = state
        .service
        .revoke_api_key(&key_id, request)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(response))
}

fn parse_direction(value: Option<&str>) -> Result<Option<AddressTxDirection>, ApiError> {
    match value {
        None => Ok(None),
        Some("from") => Ok(Some(AddressTxDirection::From)),
        Some("to") => Ok(Some(AddressTxDirection::To)),
        Some(value) => Err(ApiError::conflict(format!(
            "unsupported direction: {value}"
        ))),
    }
}

fn extract_api_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .or_else(|| headers.get("x-api-key").and_then(|value| value.to_str().ok()))
}

fn require_internal_access(state: &IndexerApiState, headers: &HeaderMap) -> Result<(), ApiError> {
    if state
        .service
        .authorize_internal_token(extract_api_token(headers))
    {
        Ok(())
    } else {
        Err(ApiError {
            status: StatusCode::UNAUTHORIZED,
            message: "missing or invalid internal api token".to_owned(),
        })
    }
}

pub struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn conflict(message: String) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            message,
        }
    }
}

impl From<LedgerError> for ApiError {
    fn from(value: LedgerError) -> Self {
        match value {
            LedgerError::NotFound(message) => Self {
                status: StatusCode::UNAUTHORIZED,
                message,
            },
            LedgerError::MalformedEvidence(message) => Self {
                status: StatusCode::BAD_REQUEST,
                message,
            },
            LedgerError::UnsupportedTrace(message)
            | LedgerError::TransientIngestion(message)
            | LedgerError::AnalysisInvariant(message)
            | LedgerError::RecognizerExecution(message)
            | LedgerError::Persistence(message) => Self {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                message,
            },
        }
    }
}

impl axum::response::IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        (self.status, Json(json!({ "error": self.message }))).into_response()
    }
}
