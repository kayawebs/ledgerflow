use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

use crate::core::errors::LedgerError;
use crate::domain::entities::{AddressCluster, ClusterAddress, ClusterSpendSummary};
use crate::plugins::loader::PluginManifest;
use crate::services::analysis::AnalysisService;
use crate::services::cluster::ClusterService;
use crate::services::cluster_backfill::{
    ClusterBackfillRequest as ServiceClusterBackfillRequest, ClusterBackfillResult,
    ClusterBackfillService,
};

#[derive(Clone)]
pub struct ApiState {
    pub service: Arc<AnalysisService>,
    pub cluster_service: Arc<ClusterService>,
    pub backfill_service: Arc<ClusterBackfillService>,
}

#[derive(Debug, Deserialize)]
pub struct AnalyzeTxRequest {
    pub chain_id: u64,
    pub tx_hash: String,
}

#[derive(Debug, Deserialize)]
pub struct ReplayTxRequest {
    pub chain_id: u64,
    pub tx_hash: String,
    pub ruleset_id: Option<String>,
    pub recognizer_ids: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
pub struct AnalysisQuery {
    pub chain_id: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct CreateClusterRequest {
    pub chain_id: u64,
    pub owner_label: String,
    pub name: String,
    pub description: Option<String>,
    pub addresses: Vec<ClusterAddressInput>,
}

#[derive(Debug, Deserialize)]
pub struct ClusterAddressInput {
    pub address: String,
    pub label: Option<String>,
    pub role: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ClusterSummaryQuery {
    pub start: Option<String>,
    pub end: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ClusterBackfillRequest {
    pub start_block: Option<u64>,
    pub end_block: Option<u64>,
    pub force_reanalyze: Option<bool>,
    pub query_limit: Option<u32>,
}

#[derive(Debug, Serialize)]
pub struct AnalysisEnvelope {
    pub analysis: crate::domain::entities::TransactionAnalysis,
    pub evidence_pack: crate::domain::entities::EvidencePack,
}

#[derive(Debug, Serialize)]
pub struct RecognizersResponse {
    pub recognizers: Vec<crate::recognition::registry::RecognizerMetadata>,
    pub enabled: Vec<String>,
    pub plugins: Vec<PluginManifest>,
}

pub fn router(
    service: Arc<AnalysisService>,
    backfill_service: Arc<ClusterBackfillService>,
) -> Router {
    let cluster_service = Arc::new(ClusterService::new(service.repository.clone()));
    Router::new()
        .route("/health", get(health))
        .route("/analyses/tx", post(analyze_tx))
        .route("/analyses/{tx_hash}", get(get_analysis))
        .route("/evidence-packs/{pack_id}", get(get_evidence_pack))
        .route("/recognizers", get(list_recognizers))
        .route("/replay/tx", post(replay_tx))
        .route("/clusters", post(create_cluster))
        .route(
            "/clusters/{cluster_id}/spend-summary",
            get(get_cluster_spend_summary),
        )
        .route("/clusters/{cluster_id}/backfill", post(backfill_cluster))
        .route("/clusters/{cluster_id}/coverage", get(get_cluster_coverage))
        .route(
            "/clusters/{cluster_id}/backfill-runs",
            get(list_cluster_backfill_runs),
        )
        .with_state(ApiState {
            service,
            cluster_service,
            backfill_service,
        })
}

async fn health() -> Json<serde_json::Value> {
    Json(json!({ "status": "ok" }))
}

async fn analyze_tx(
    State(state): State<ApiState>,
    Json(request): Json<AnalyzeTxRequest>,
) -> Result<Json<AnalysisEnvelope>, ApiError> {
    let (analysis, evidence_pack) = state
        .service
        .analyze_transaction(request.chain_id, &request.tx_hash)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(AnalysisEnvelope {
        analysis,
        evidence_pack,
    }))
}

async fn get_analysis(
    State(state): State<ApiState>,
    Path(tx_hash): Path<String>,
    Query(query): Query<AnalysisQuery>,
) -> Result<Json<AnalysisEnvelope>, ApiError> {
    let chain_id = query.chain_id.unwrap_or(8453);
    let analysis = state
        .service
        .get_analysis(chain_id, &tx_hash)
        .await
        .map_err(ApiError::from)?;
    let evidence_pack = state
        .service
        .get_evidence_pack_by_tx(chain_id, &tx_hash)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(AnalysisEnvelope {
        analysis,
        evidence_pack,
    }))
}

async fn get_evidence_pack(
    State(state): State<ApiState>,
    Path(pack_id): Path<String>,
) -> Result<Json<crate::domain::entities::EvidencePack>, ApiError> {
    let pack = state
        .service
        .get_evidence_pack(&pack_id)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(pack))
}

async fn list_recognizers(State(state): State<ApiState>) -> Json<RecognizersResponse> {
    Json(RecognizersResponse {
        recognizers: state.service.recognizer_registry.list_metadata(),
        enabled: state.service.enabled_recognizers.clone(),
        plugins: state.service.plugin_context.manifests.clone(),
    })
}

async fn replay_tx(
    State(state): State<ApiState>,
    Json(request): Json<ReplayTxRequest>,
) -> Result<Json<AnalysisEnvelope>, ApiError> {
    if let Some(ruleset_id) = request.ruleset_id.as_deref() {
        state
            .service
            .ensure_ruleset_available(ruleset_id)
            .map_err(|err| ApiError::conflict(err.to_string()))?;
    }
    if let Some(recognizer_ids) = request.recognizer_ids.as_ref() {
        let installed = state
            .service
            .recognizer_registry
            .list_metadata()
            .into_iter()
            .map(|item| item.recognizer_id)
            .collect::<Vec<_>>();
        let missing = recognizer_ids
            .iter()
            .filter(|item| !installed.iter().any(|installed_id| installed_id == *item))
            .cloned()
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            return Err(ApiError::conflict(format!(
                "recognizers not installed: {}",
                missing.join(",")
            )));
        }
    }
    let (analysis, evidence_pack) = state
        .service
        .analyze_transaction_with_recognizers(
            request.chain_id,
            &request.tx_hash,
            request.recognizer_ids,
        )
        .await
        .map_err(ApiError::from)?;
    Ok(Json(AnalysisEnvelope {
        analysis,
        evidence_pack,
    }))
}

async fn create_cluster(
    State(state): State<ApiState>,
    Json(request): Json<CreateClusterRequest>,
) -> Result<Json<AddressCluster>, ApiError> {
    let cluster = state
        .cluster_service
        .create_cluster(
            request.chain_id,
            &request.owner_label,
            &request.name,
            request.description,
            request
                .addresses
                .into_iter()
                .map(|item| ClusterAddress {
                    address: item.address,
                    label: item.label,
                    role: item.role,
                    metadata: json!({}),
                })
                .collect(),
        )
        .await
        .map_err(ApiError::from)?;
    Ok(Json(cluster))
}

async fn get_cluster_spend_summary(
    State(state): State<ApiState>,
    Path(cluster_id): Path<String>,
    Query(query): Query<ClusterSummaryQuery>,
) -> Result<Json<ClusterSpendSummary>, ApiError> {
    let cluster_id = Uuid::parse_str(&cluster_id)
        .map_err(|err| ApiError::conflict(format!("invalid cluster id: {err}")))?;
    let start = parse_time(query.start.as_deref())?;
    let end = parse_time(query.end.as_deref())?;
    let summary = state
        .cluster_service
        .get_cluster_summary(cluster_id, start, end)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(summary))
}

async fn backfill_cluster(
    State(state): State<ApiState>,
    Path(cluster_id): Path<String>,
    Json(request): Json<ClusterBackfillRequest>,
) -> Result<Json<ClusterBackfillResult>, ApiError> {
    let cluster_id = Uuid::parse_str(&cluster_id)
        .map_err(|err| ApiError::conflict(format!("invalid cluster id: {err}")))?;
    let result = state
        .backfill_service
        .backfill_cluster(
            cluster_id,
            ServiceClusterBackfillRequest {
                start_block: request.start_block,
                end_block: request.end_block,
                force_reanalyze: request.force_reanalyze.unwrap_or(false),
                query_limit: request.query_limit,
            },
        )
        .await
        .map_err(ApiError::from)?;
    Ok(Json(result))
}

async fn get_cluster_coverage(
    State(state): State<ApiState>,
    Path(cluster_id): Path<String>,
) -> Result<Json<crate::domain::entities::ClusterCoverage>, ApiError> {
    let cluster_id = Uuid::parse_str(&cluster_id)
        .map_err(|err| ApiError::conflict(format!("invalid cluster id: {err}")))?;
    let coverage = state
        .backfill_service
        .get_cluster_coverage(cluster_id)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(coverage))
}

async fn list_cluster_backfill_runs(
    State(state): State<ApiState>,
    Path(cluster_id): Path<String>,
) -> Result<Json<Vec<crate::domain::entities::ClusterBackfillRun>>, ApiError> {
    let cluster_id = Uuid::parse_str(&cluster_id)
        .map_err(|err| ApiError::conflict(format!("invalid cluster id: {err}")))?;
    let runs = state
        .backfill_service
        .list_cluster_backfill_runs(cluster_id)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(runs))
}

fn parse_time(value: Option<&str>) -> Result<Option<chrono::DateTime<chrono::Utc>>, ApiError> {
    match value {
        None => Ok(None),
        Some(value) => chrono::DateTime::parse_from_rfc3339(value)
            .map(|value| Some(value.with_timezone(&chrono::Utc)))
            .map_err(|err| ApiError::conflict(format!("invalid timestamp: {err}"))),
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
                status: StatusCode::NOT_FOUND,
                message,
            },
            LedgerError::UnsupportedTrace(message) => Self::conflict(message),
            LedgerError::TransientIngestion(message)
            | LedgerError::MalformedEvidence(message)
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
