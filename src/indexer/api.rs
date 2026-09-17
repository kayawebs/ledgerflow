use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::core::errors::LedgerError;
use crate::indexer::models::{
    AddressTxDirection, AddressTxQueryResponse, CreateIndexerApiKeyRequest,
    CreateIndexerApiKeyResponse, IndexerApiKeyRecord, IndexerWorkerProgress,
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
    /// Return only refs already present in ClickHouse. Observation workers use
    /// this mode so an address lookup never schedules a whole-range lazy block
    /// backfill.
    pub known_only: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct ApiKeyListQuery {
    pub owner_user_id: String,
}

#[derive(Debug, Default, Deserialize)]
pub struct ProgressQuery {
    pub include_exact_storage_stats: Option<bool>,
}

#[derive(Debug, PartialEq, Serialize)]
struct FullHistoryCoverage {
    configured_start_block: Option<u32>,
    target_block: Option<u32>,
    contiguous_indexed_through_block: Option<u32>,
    remaining_blocks: Option<u64>,
    completion_ratio: Option<f64>,
    history_backfill_complete: bool,
    live_indexed_from_block: Option<u32>,
    live_indexed_through_block: Option<u32>,
    unindexed_gap_to_live_blocks: Option<u64>,
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
    Query(query): Query<ProgressQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    state
        .service
        .authorize_public_token(extract_api_token(&headers))
        .await
        .map_err(ApiError::from)?;
    let (rpc_head_block, pending_indexing_requests, workers) = tokio::try_join!(
        state.service.get_rpc_head(),
        state.service.get_pending_indexing_request_count(),
        state.service.get_worker_progresses(),
    )
    .map_err(ApiError::from)?;
    let query_target_block = state
        .service
        .config
        .end_block
        .map(|end_block| u64::from(end_block).min(rpc_head_block))
        .unwrap_or(rpc_head_block);
    let coverage = full_history_coverage(&workers);
    let last_indexed_block = workers
        .iter()
        .map(|worker| u64::from(worker.last_indexed_block))
        .max()
        .unwrap_or(0);
    let live_lag_blocks = workers
        .iter()
        .find(|worker| worker.worker_kind == "live")
        .map(|worker| rpc_head_block.saturating_sub(u64::from(worker.last_indexed_block)));
    let include_exact_storage_stats = query.include_exact_storage_stats.unwrap_or(false);
    let (max_stored_indexed_block, endpoint_backfill_pending_rows, storage) =
        if include_exact_storage_stats {
            let (max_stored_indexed_block, endpoint_backfill_pending_rows, storage) =
                tokio::try_join!(
                    state.service.get_max_indexed_block(),
                    state.service.get_endpoint_backfill_pending_rows(),
                    state.service.get_storage_stats(),
                )
                .map_err(ApiError::from)?;
            (
                max_stored_indexed_block,
                Some(endpoint_backfill_pending_rows),
                Some(storage),
            )
        } else {
            (None, None, None)
        };
    Ok(Json(json!({
        "chain_id": state.service.config.chain_id,
        "worker_id": state.service.config.worker_id,
        "last_indexed_block": last_indexed_block,
        "query_target_block": query_target_block,
        "rpc_head_block": rpc_head_block,
        "live_lag_blocks": live_lag_blocks,
        "contiguous_indexed_through_block": coverage.contiguous_indexed_through_block,
        "coverage": coverage,
        "max_stored_indexed_block": max_stored_indexed_block,
        "lazy_enabled": state.service.lazy_enabled(),
        "pending_indexing_requests": pending_indexing_requests,
        "endpoint_backfill_pending_rows": endpoint_backfill_pending_rows,
        "exact_storage_stats_included": include_exact_storage_stats,
        "canonical_block_count": storage.as_ref().map(|value| value.canonical_block_count),
        "raw_transaction_count": storage.as_ref().map(|value| value.raw_transaction_count),
        "raw_log_count": storage.as_ref().map(|value| value.raw_log_count),
        "raw_trace_count": storage.as_ref().map(|value| value.raw_trace_count),
        "orphaned_block_count": storage.as_ref().map(|value| value.orphaned_block_count),
        "reorg_count": storage.as_ref().map(|value| value.reorg_count),
        "workers": workers,
    })))
}

fn full_history_coverage(workers: &[IndexerWorkerProgress]) -> FullHistoryCoverage {
    let mut history_intervals = workers
        .iter()
        .filter(|worker| worker.worker_kind == "history")
        .map(|worker| {
            (
                worker.range_start,
                worker.last_indexed_block.max(worker.range_start),
                worker.range_end.unwrap_or(worker.last_indexed_block),
            )
        })
        .collect::<Vec<_>>();
    history_intervals.sort_unstable_by_key(|(start, _, _)| *start);

    let configured_start_block = history_intervals.first().map(|(start, _, _)| *start);
    let target_block = history_intervals.iter().map(|(_, _, target)| *target).max();
    let contiguous_indexed_through_block = configured_start_block.map(|start| {
        let mut next_required = start;
        for (interval_start, interval_end, _) in &history_intervals {
            if *interval_start > next_required {
                break;
            }
            if *interval_end >= next_required {
                next_required = interval_end.saturating_add(1);
            }
        }
        next_required.saturating_sub(1)
    });
    let remaining_blocks = match (contiguous_indexed_through_block, target_block) {
        (Some(contiguous), Some(target)) => Some(u64::from(target.saturating_sub(contiguous))),
        _ => None,
    };
    let completion_ratio = match (
        configured_start_block,
        contiguous_indexed_through_block,
        target_block,
    ) {
        (Some(start), Some(contiguous), Some(target)) if target >= start => {
            let completed = u64::from(contiguous.saturating_sub(start)) + 1;
            let total = u64::from(target.saturating_sub(start)) + 1;
            Some(completed as f64 / total as f64)
        }
        _ => None,
    };
    let history_backfill_complete = matches!(remaining_blocks, Some(0));

    let live = workers
        .iter()
        .filter(|worker| worker.worker_kind == "live")
        .max_by_key(|worker| worker.last_indexed_block);
    let live_indexed_from_block = live.map(|worker| worker.range_start);
    let live_indexed_through_block = live.map(|worker| worker.last_indexed_block);
    let unindexed_gap_to_live_blocks =
        match (contiguous_indexed_through_block, live_indexed_from_block) {
            (Some(contiguous), Some(live_start)) => Some(u64::from(
                live_start.saturating_sub(contiguous.saturating_add(1)),
            )),
            _ => None,
        };

    FullHistoryCoverage {
        configured_start_block,
        target_block,
        contiguous_indexed_through_block,
        remaining_blocks,
        completion_ratio,
        history_backfill_complete,
        live_indexed_from_block,
        live_indexed_through_block,
        unindexed_gap_to_live_blocks,
    }
}

async fn query_address_txs(
    State(state): State<IndexerApiState>,
    headers: HeaderMap,
    Path(address): Path<String>,
    Query(query): Query<AddressTxQuery>,
) -> Result<Response, ApiError> {
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
            query.known_only.unwrap_or(false),
        )
        .await
        .map_err(ApiError::from)?;
    Ok(address_query_response(response))
}

async fn query_address_tx_hashes(
    State(state): State<IndexerApiState>,
    headers: HeaderMap,
    Path(address): Path<String>,
    Query(query): Query<AddressTxQuery>,
) -> Result<Response, ApiError> {
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
            query.known_only.unwrap_or(false),
        )
        .await
        .map_err(ApiError::from)?;
    let status = if response
        .coverage
        .as_ref()
        .map(|coverage| !coverage.complete)
        .unwrap_or(false)
    {
        StatusCode::ACCEPTED
    } else {
        StatusCode::OK
    };
    Ok((status, Json(response)).into_response())
}

fn address_query_response(response: AddressTxQueryResponse) -> Response {
    let status = if response
        .coverage
        .as_ref()
        .map(|coverage| !coverage.complete)
        .unwrap_or(false)
    {
        StatusCode::ACCEPTED
    } else {
        StatusCode::OK
    };
    (status, Json(response)).into_response()
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
        .or_else(|| {
            headers
                .get("x-api-key")
                .and_then(|value| value.to_str().ok())
        })
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_history_coverage_does_not_treat_live_tip_as_contiguous_history() {
        let workers = vec![
            worker("history", 0, 794_789, Some(27_251_940)),
            worker("live", 27_251_941, 36_711_605, None),
        ];
        let coverage = full_history_coverage(&workers);
        assert_eq!(coverage.configured_start_block, Some(0));
        assert_eq!(coverage.target_block, Some(27_251_940));
        assert_eq!(coverage.contiguous_indexed_through_block, Some(794_789));
        assert_eq!(coverage.remaining_blocks, Some(26_457_151));
        assert_eq!(coverage.unindexed_gap_to_live_blocks, Some(26_457_151));
        assert!(!coverage.history_backfill_complete);
    }

    #[test]
    fn adjacent_history_partitions_merge_into_complete_coverage() {
        let workers = vec![
            worker("history", 0, 99, Some(99)),
            worker("history", 100, 199, Some(199)),
            worker("live", 200, 250, None),
        ];
        let coverage = full_history_coverage(&workers);
        assert_eq!(coverage.contiguous_indexed_through_block, Some(199));
        assert_eq!(coverage.remaining_blocks, Some(0));
        assert_eq!(coverage.unindexed_gap_to_live_blocks, Some(0));
        assert!(coverage.history_backfill_complete);
        assert_eq!(coverage.completion_ratio, Some(1.0));
    }

    fn worker(
        worker_kind: &str,
        range_start: u32,
        last_indexed_block: u32,
        range_end: Option<u32>,
    ) -> IndexerWorkerProgress {
        IndexerWorkerProgress {
            chain_id: 4663,
            worker_id: format!("{worker_kind}_{range_start}"),
            worker_kind: worker_kind.to_owned(),
            last_indexed_block,
            last_indexed_hash: "0xhash".to_owned(),
            range_start,
            range_end,
            rpc_url: "http://rpc".to_owned(),
            retry_count: 0,
            last_error: String::new(),
            last_success_at: Some("2026-08-15 00:00:00.000".to_owned()),
            head_block: Some(250),
            lag_blocks: Some(0),
        }
    }
}
