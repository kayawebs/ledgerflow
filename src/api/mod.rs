use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::{PgPool, Row};
use uuid::Uuid;

const ADDRESS_HISTORY_REFRESH_INTERVAL_MINUTES: i64 = 15;

use crate::core::errors::LedgerError;
use crate::domain::entities::{
    AddressAnalysisCandidate, AddressCluster, AddressLabel, AddressProfile, ClassifiedTransaction,
    ClusterAddress, ClusterAlert, ClusterAssetChangeReport, ClusterMetricsSnapshot,
    ClusterSpendSummary, ClusterStrategyReportSnapshot, ClusterTrackingState, TransactionAnalysis,
};
use crate::ingestion::rpc::RpcClient;
use crate::plugins::loader::PluginManifest;
use crate::services::alerts::ClusterAlertService;
use crate::services::analysis::AnalysisService;
use crate::services::behavior_assertions::{BehaviorReviewItem, BehaviorReviewService};
use crate::services::cluster::ClusterService;
use crate::services::cluster_backfill::{
    ClusterBackfillRequest as ServiceClusterBackfillRequest, ClusterBackfillResult,
    ClusterBackfillService,
};
use crate::services::cluster_tracking::ClusterTrackingService;
use crate::services::metrics::ClusterMetricsService;
use crate::services::observation_intelligence::ObservationIntelligenceService;
use crate::services::report_agent::{
    ReportAgentFilters, ReportAgentQuery, ReportAgentResponse, ReportAgentSection,
    ReportAgentService,
};
use crate::services::searcher_cluster_intelligence::{
    ClusterMemberOverride, SearcherClusterIntelligenceService,
};
use crate::services::strategy_analysis::{ClusterStrategyAnalysis, ClusterStrategyAnalysisService};

#[derive(Clone)]
pub struct ApiState {
    pub default_chain_id: u64,
    pub service: Arc<AnalysisService>,
    pub cluster_service: Arc<ClusterService>,
    pub backfill_service: Arc<ClusterBackfillService>,
    pub rpc_client: Arc<dyn RpcClient>,
    pub tracking_service: Arc<ClusterTrackingService>,
    pub metrics_service: Arc<ClusterMetricsService>,
    pub strategy_analysis_service: Arc<ClusterStrategyAnalysisService>,
    pub report_agent_service: Arc<ReportAgentService>,
    pub alert_service: Arc<ClusterAlertService>,
    pub behavior_review_service: Arc<BehaviorReviewService>,
    pub auto_backfill_on_cluster_create: bool,
    pub pilot_pool: PgPool,
    pub observation_service: Option<Arc<ObservationIntelligenceService>>,
    pub searcher_cluster_service: Option<Arc<SearcherClusterIntelligenceService>>,
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
pub struct UpdateClusterRequest {
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

#[derive(Debug, Deserialize)]
pub struct ReportAgentQueryRequest {
    pub window_start: Option<String>,
    pub window_end: Option<String>,
    pub all_time: Option<bool>,
    pub sections: Option<Vec<ReportAgentSection>>,
    pub filters: Option<ReportAgentFilters>,
    pub row_limit: Option<u32>,
    pub cost_budget: Option<u32>,
}

#[derive(Debug, Serialize)]
pub struct AnalysisEnvelope {
    pub analysis: crate::domain::entities::TransactionAnalysis,
    pub evidence_pack: crate::domain::entities::EvidencePack,
    pub address_intelligence: AddressIntelligenceEnvelope,
}

#[derive(Debug, Serialize)]
pub struct AddressIntelligenceEnvelope {
    pub profiles: Vec<AddressProfile>,
    pub labels: Vec<AddressLabel>,
}

#[derive(Debug, Deserialize)]
pub struct AddressSummaryQuery {
    pub chain_id: Option<u64>,
    pub address: String,
    pub lite: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct EnqueueAddressIntelligenceRequest {
    pub chain_id: u64,
    pub addresses: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct AddressSummaryEnvelope {
    pub chain_id: u64,
    pub address: String,
    pub native_balance_raw: Option<String>,
    pub profile: Option<AddressProfile>,
    pub labels: Vec<AddressLabel>,
    pub analyzed_tx_count: u64,
    pub gas_spent_raw: String,
    pub behavior_rows: Vec<AddressBehaviorSummaryRow>,
    pub recent_txs: Vec<AddressRecentTxRow>,
    pub queued: bool,
}

#[derive(Debug, Serialize)]
pub struct AddressBehaviorSummaryRow {
    pub behavior: String,
    pub tx_count: u64,
    pub gas_spent_raw: String,
}

#[derive(Debug, Serialize)]
pub struct AddressRecentTxRow {
    pub tx_hash: String,
    pub block_timestamp: Option<String>,
    pub behavior: String,
    pub gas_raw: String,
}

#[derive(Debug, Serialize)]
pub struct RecognizersResponse {
    pub recognizers: Vec<crate::recognition::registry::RecognizerMetadata>,
    pub enabled: Vec<String>,
    pub plugins: Vec<PluginManifest>,
}

#[derive(Debug, Deserialize)]
pub struct ClusterAlertsQuery {
    pub include_read: Option<bool>,
    pub limit: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub struct AddressIntelligenceQuery {
    pub chain_id: Option<u64>,
    pub addresses: String,
}

#[derive(Debug, Deserialize)]
pub struct BehaviorReviewQuery {
    pub chain_id: Option<u64>,
    pub status: Option<String>,
    pub limit: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub struct BehaviorReviewDecisionRequest {
    pub decision: String,
    pub reviewer: String,
    pub rationale: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct PilotEventsQuery {
    pub limit: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub struct SeedIntelligenceQuery {
    pub chain_id: Option<u64>,
    pub address: Option<String>,
    pub seed_kind: Option<String>,
    pub relationship_type: Option<String>,
    pub status: Option<String>,
    pub limit: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub struct CreateObservationRequest {
    pub chain_id: u64,
    pub address: String,
    pub source: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ObservationSnapshotQuery {
    pub chain_id: Option<u64>,
    pub timeline_limit: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub struct ClusterSnapshotQuery {
    pub chain_id: Option<u64>,
    pub execution_limit: Option<u32>,
    pub execution_offset: Option<u64>,
}

pub fn router(
    service: Arc<AnalysisService>,
    backfill_service: Arc<ClusterBackfillService>,
    rpc_client: Arc<dyn RpcClient>,
    behavior_review_service: Arc<BehaviorReviewService>,
    auto_backfill_on_cluster_create: bool,
    default_chain_id: u64,
    pilot_pool: PgPool,
    observation_service: Option<Arc<ObservationIntelligenceService>>,
    searcher_cluster_service: Option<Arc<SearcherClusterIntelligenceService>>,
) -> Router {
    let cluster_service = Arc::new(ClusterService::new(service.repository.clone()));
    let tracking_service = Arc::new(ClusterTrackingService::new(service.repository.clone()));
    let metrics_service = Arc::new(ClusterMetricsService::new(service.repository.clone()));
    let alert_service = Arc::new(ClusterAlertService::new(service.repository.clone()));
    let strategy_analysis_service = Arc::new(ClusterStrategyAnalysisService::new(
        service.repository.clone(),
    ));
    let report_agent_service = Arc::new(ReportAgentService::new(strategy_analysis_service.clone()));
    Router::new()
        .route("/health", get(health))
        .route("/ready", get(readiness))
        .route("/metrics", get(metrics))
        .route("/intelligence/seeds", get(list_intelligence_seeds))
        .route(
            "/intelligence/seeds/relationships",
            get(list_intelligence_seed_relationships),
        )
        .route(
            "/intelligence/seeds/coverage",
            get(get_intelligence_seed_coverage),
        )
        .route(
            "/intelligence/seeds/summary",
            get(get_intelligence_seed_summary),
        )
        .route(
            "/pilot/evidence-packs/{event_id}",
            get(get_pilot_evidence_pack),
        )
        .route(
            "/pilot/projects/{project_id}/events",
            get(list_pilot_events),
        )
        .route(
            "/pilot/projects/{project_id}/deliveries",
            get(list_pilot_deliveries),
        )
        .route(
            "/pilot/projects/{project_id}/weekly-reports/latest",
            get(get_latest_pilot_report),
        )
        .route("/analyses/tx", post(analyze_tx))
        .route("/analyses/{tx_hash}", get(get_analysis))
        .route("/evidence-packs/{pack_id}", get(get_evidence_pack))
        .route("/recognizers", get(list_recognizers))
        .route("/addresses/intelligence", get(get_address_intelligence))
        .route(
            "/addresses/intelligence/enqueue",
            post(enqueue_address_intelligence),
        )
        .route("/addresses/summary", get(get_address_summary))
        .route("/observations", post(create_observation))
        .route("/observations/{address}", get(get_observation))
        .route(
            "/observations/{address}/cluster",
            get(get_observation_cluster),
        )
        .route(
            "/observations/{address}/cluster/members/{member_address}",
            post(override_observation_cluster_member),
        )
        .route(
            "/observations/{address}/briefs/{brief_id}",
            get(get_observation_brief),
        )
        .route("/observations/{address}/retry", post(retry_observation))
        .route("/behavior-reviews", get(list_behavior_reviews))
        .route(
            "/behavior-reviews/{review_id}/decision",
            post(decide_behavior_review),
        )
        .route("/replay/tx", post(replay_tx))
        .route("/clusters", post(create_cluster))
        .route("/clusters/{cluster_id}", get(get_cluster))
        .route("/clusters/{cluster_id}/update", post(update_cluster))
        .route("/clusters/{cluster_id}/delete", post(delete_cluster))
        .route(
            "/clusters/{cluster_id}/spend-summary",
            get(get_cluster_spend_summary),
        )
        .route("/clusters/{cluster_id}/backfill", post(backfill_cluster))
        .route("/clusters/{cluster_id}/coverage", get(get_cluster_coverage))
        .route(
            "/clusters/{cluster_id}/asset-changes",
            get(get_cluster_asset_changes),
        )
        .route(
            "/clusters/{cluster_id}/backfill-runs",
            get(list_cluster_backfill_runs),
        )
        .route("/clusters/{cluster_id}/tracking", get(get_cluster_tracking))
        .route(
            "/clusters/{cluster_id}/tracking/enable",
            post(enable_cluster_tracking),
        )
        .route(
            "/clusters/{cluster_id}/tracking/pause",
            post(pause_cluster_tracking),
        )
        .route(
            "/clusters/{cluster_id}/classified-transactions",
            get(list_classified_transactions),
        )
        .route("/clusters/{cluster_id}/metrics", get(list_cluster_metrics))
        .route(
            "/clusters/{cluster_id}/strategy-analysis",
            get(get_cluster_strategy_analysis),
        )
        .route(
            "/clusters/{cluster_id}/strategy-analysis/snapshot",
            get(get_cluster_strategy_analysis_snapshot),
        )
        .route(
            "/clusters/{cluster_id}/strategy-analysis/refresh",
            post(refresh_cluster_strategy_analysis_snapshot),
        )
        .route(
            "/clusters/{cluster_id}/report-agent/query",
            post(query_cluster_report_agent),
        )
        .route("/clusters/{cluster_id}/alerts", get(list_cluster_alerts))
        .route(
            "/clusters/{cluster_id}/alerts/{alert_id}/read",
            post(mark_cluster_alert_read),
        )
        .with_state(ApiState {
            default_chain_id,
            service,
            cluster_service,
            backfill_service,
            rpc_client,
            tracking_service,
            metrics_service,
            strategy_analysis_service,
            report_agent_service,
            alert_service,
            behavior_review_service,
            auto_backfill_on_cluster_create,
            pilot_pool,
            observation_service,
            searcher_cluster_service,
        })
}

async fn create_observation(
    State(state): State<ApiState>,
    Json(request): Json<CreateObservationRequest>,
) -> Result<(StatusCode, Json<serde_json::Value>), ApiError> {
    let service = state.observation_service.as_ref().ok_or_else(|| {
        ApiError::conflict("observation intelligence is unavailable on this data plane".to_owned())
    })?;
    service
        .enqueue(
            request.chain_id,
            &request.address,
            request.source.as_deref().unwrap_or("api"),
        )
        .await
        .map_err(ApiError::from)?;
    let candidate_cluster_id =
        if let Some(cluster_service) = state.searcher_cluster_service.as_ref() {
            Some(
                cluster_service
                    .enqueue(request.chain_id, &request.address)
                    .await
                    .map_err(ApiError::from)?,
            )
        } else {
            None
        };
    Ok((
        StatusCode::ACCEPTED,
        Json(json!({
            "chain_id": request.chain_id,
            "address": request.address.to_ascii_lowercase(),
            "status": "queued",
            "monitoring_enabled": true,
            "candidate_cluster_id":candidate_cluster_id,
            "cluster_status":candidate_cluster_id.map(|_| "queued")
        })),
    ))
}

async fn get_observation_cluster(
    State(state): State<ApiState>,
    Path(address): Path<String>,
    Query(query): Query<ClusterSnapshotQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let service = state.searcher_cluster_service.as_ref().ok_or_else(|| {
        ApiError::conflict(
            "searcher cluster intelligence is unavailable on this data plane".to_owned(),
        )
    })?;
    let chain_id = query.chain_id.unwrap_or(state.default_chain_id);
    let snapshot = service
        .snapshot(
            chain_id,
            &address,
            query.execution_limit.unwrap_or(20),
            query.execution_offset.unwrap_or(0),
        )
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| {
            ApiError::from(LedgerError::NotFound(
                "candidate cluster not found".to_owned(),
            ))
        })?;
    Ok(Json(snapshot))
}

async fn override_observation_cluster_member(
    State(state): State<ApiState>,
    Path((address, member_address)): Path<(String, String)>,
    Query(query): Query<ClusterSnapshotQuery>,
    Json(request): Json<ClusterMemberOverride>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let service = state.searcher_cluster_service.as_ref().ok_or_else(|| {
        ApiError::conflict(
            "searcher cluster intelligence is unavailable on this data plane".to_owned(),
        )
    })?;
    let chain_id = query.chain_id.unwrap_or(state.default_chain_id);
    service
        .override_member(chain_id, &address, &member_address, request)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(json!({
        "chain_id":chain_id,
        "seed_address":address.to_ascii_lowercase(),
        "member_address":member_address.to_ascii_lowercase(),
        "status":"accepted"
    })))
}

async fn get_observation(
    State(state): State<ApiState>,
    Path(address): Path<String>,
    Query(query): Query<ObservationSnapshotQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let service = state.observation_service.as_ref().ok_or_else(|| {
        ApiError::conflict("observation intelligence is unavailable on this data plane".to_owned())
    })?;
    let chain_id = query.chain_id.unwrap_or(state.default_chain_id);
    let snapshot = service
        .snapshot(chain_id, &address, query.timeline_limit.unwrap_or(100))
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::from(LedgerError::NotFound("observation not found".to_owned())))?;
    Ok(Json(snapshot))
}

async fn retry_observation(
    State(state): State<ApiState>,
    Path(address): Path<String>,
    Query(query): Query<ObservationSnapshotQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let service = state.observation_service.as_ref().ok_or_else(|| {
        ApiError::conflict("observation intelligence is unavailable on this data plane".to_owned())
    })?;
    let chain_id = query.chain_id.unwrap_or(state.default_chain_id);
    if !service
        .retry(chain_id, &address)
        .await
        .map_err(ApiError::from)?
    {
        return Err(ApiError::from(LedgerError::NotFound(
            "observation not found".to_owned(),
        )));
    }
    if let Some(cluster_service) = state.searcher_cluster_service.as_ref() {
        let _ = cluster_service.retry(chain_id, &address).await;
    }
    Ok(Json(
        json!({"chain_id":chain_id,"address":address.to_ascii_lowercase(),"status":"queued"}),
    ))
}

async fn get_observation_brief(
    State(state): State<ApiState>,
    Path((address, brief_id)): Path<(String, Uuid)>,
    Query(query): Query<ObservationSnapshotQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let service = state.observation_service.as_ref().ok_or_else(|| {
        ApiError::conflict("observation intelligence is unavailable on this data plane".to_owned())
    })?;
    let chain_id = query.chain_id.unwrap_or(state.default_chain_id);
    let brief = service
        .brief(chain_id, &address, brief_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::from(LedgerError::NotFound("brief not found".to_owned())))?;
    Ok(Json(brief))
}

async fn health() -> Json<serde_json::Value> {
    Json(json!({ "status": "ok" }))
}

async fn readiness(State(state): State<ApiState>) -> (StatusCode, Json<serde_json::Value>) {
    let expected_chain_id = state.default_chain_id;
    match state.rpc_client.get_chain_id().await {
        Ok(actual) if actual == expected_chain_id => {}
        Ok(actual) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({
                    "status": "not_ready",
                    "expected_chain_id": expected_chain_id,
                    "rpc_chain_id": actual
                })),
            );
        }
        Err(error) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({
                    "status": "not_ready",
                    "expected_chain_id": expected_chain_id,
                    "rpc": "error",
                    "detail": error.to_string()
                })),
            );
        }
    }
    match sqlx::query_scalar::<_, i32>("SELECT 1")
        .fetch_one(&state.pilot_pool)
        .await
    {
        Ok(1) => (
            StatusCode::OK,
            Json(json!({
                "status": "ready",
                "chain_id": expected_chain_id,
                "rpc": "ok",
                "postgres": "ok"
            })),
        ),
        Ok(_) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"status":"not_ready","postgres":"unexpected response"})),
        ),
        Err(error) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"status":"not_ready","postgres":"error","detail":error.to_string()})),
        ),
    }
}

async fn metrics(State(state): State<ApiState>) -> (StatusCode, String) {
    let counts = sqlx::query(
        r#"
        select
          (select count(*) from pilot_projects where enabled) projects,
          (select count(*) from pilot_events) events,
          (select count(*) from pilot_analysis_runs where status = 'failed') analysis_failures,
          (select count(*) from pilot_alert_deliveries where status = 'queued') queued,
          (select count(*) from pilot_alert_deliveries where status = 'retry') retries,
          (select count(*) from pilot_alert_deliveries where status = 'dead_letter') dlq,
          (select count(*) from cluster_tracking_state s
             join address_clusters c using(cluster_id)
           where c.metadata ? 'pilot_project_id' and s.tracking_status = 'active') active_monitors,
          (select count(*) from cluster_tracking_state s
             join address_clusters c using(cluster_id)
           where c.metadata ? 'pilot_project_id' and s.tracking_status = 'error') monitor_errors,
          (select count(distinct address) from effective_intelligence_seeds
           where chain_id = $1) seed_addresses,
          (select count(distinct address) from intelligence_seed_addresses
           where chain_id = $1 and seed_kind = 'mev_actor'
             and status = 'candidate') mev_candidates,
          coalesce((select (metadata #>> '{summary,relationship_count}')::bigint
                    from seed_intelligence_runs
                    where chain_id = $1 and status = 'completed'
                    order by completed_at desc limit 1), 0) seed_relationships,
          coalesce((select (metadata #>> '{summary,seeds_with_relationships}')::bigint
                    from seed_intelligence_runs
                    where chain_id = $1 and status = 'completed'
                    order by completed_at desc limit 1), 0) seeds_with_relationships,
          coalesce((select (metadata #>> '{summary,evidence_transaction_count}')::bigint
                    from seed_intelligence_runs
                    where chain_id = $1 and status = 'completed'
                    order by completed_at desc limit 1), 0) seed_evidence_transactions,
          coalesce((select (metadata #>> '{summary,analyzed_evidence_transaction_count}')::bigint
                    from seed_intelligence_runs
                    where chain_id = $1 and status = 'completed'
                    order by completed_at desc limit 1), 0) analyzed_seed_evidence_transactions,
          coalesce((select (metadata #>> '{summary,queued_seed_jobs}')::bigint
                    from seed_intelligence_runs
                    where chain_id = $1 and status = 'completed'
                    order by completed_at desc limit 1), 0) queued_seed_jobs,
          coalesce((select (metadata #>> '{summary,deferred_generic_jobs}')::bigint
                    from seed_intelligence_runs
                    where chain_id = $1 and status = 'completed'
                    order by completed_at desc limit 1), 0) deferred_generic_jobs,
          (select count(*) from economic_verification_jobs
           where chain_id = $1 and status in ('queued', 'running')) economic_verification_pending,
          (select count(*) from economic_verification_jobs
           where chain_id = $1 and status = 'failed') economic_verification_failed,
          (select count(*) from economic_verification_results
           where chain_id = $1 and result_status = 'verified') economic_verified,
          (select count(*) from economic_verification_results
           where chain_id = $1 and result_status = 'insufficient_evidence') economic_insufficient,
          (select count(*) from economic_verification_results
           where chain_id = $1 and result_status = 'dismissed') economic_dismissed,
          (select extract(epoch from now() - max(updated_at))::double precision
           from economic_verification_results
           where chain_id = $1) economic_verification_age_seconds,
          (select extract(epoch from now() - max(completed_at))::double precision
           from seed_intelligence_runs
           where chain_id = $1 and status = 'completed') seed_run_age_seconds,
          (select count(*) from observed_address_objects
           where chain_id = $1) observation_objects,
          (select count(*) from observed_address_objects
           where chain_id = $1 and last_incremental_update is not null) observation_ready,
          (select count(*) from observed_address_objects
           where chain_id = $1 and current_status = 'failed') observation_failed,
          (select count(*) from object_activity_transactions
           where chain_id = $1 and processing_status = 'analyzed') observation_activity_analyzed,
          (select extract(epoch from now() - max(last_incremental_update))::double precision
           from observed_address_objects
           where chain_id = $1 and last_incremental_update is not null) observation_update_age_seconds
        "#,
    )
    .bind(state.default_chain_id as i64)
    .fetch_one(&state.pilot_pool)
    .await;
    let Ok(row) = counts else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "ledgerflow_api_up 1\nledgerflow_postgres_up 0\n".to_owned(),
        );
    };
    let body = format!(
        "# HELP ledgerflow_api_up API process availability\n# TYPE ledgerflow_api_up gauge\nledgerflow_api_up 1\nledgerflow_postgres_up 1\nledgerflow_pilot_projects {}\nledgerflow_pilot_active_monitors {}\nledgerflow_pilot_monitor_errors {}\nledgerflow_pilot_events_total {}\nledgerflow_pilot_analysis_failures {}\nledgerflow_pilot_alert_queued {}\nledgerflow_pilot_alert_retries {}\nledgerflow_pilot_alert_dead_letters {}\nledgerflow_seed_addresses {}\nledgerflow_seed_mev_candidates {}\nledgerflow_seed_relationships {}\nledgerflow_seeds_with_relationships {}\nledgerflow_seed_evidence_transactions {}\nledgerflow_seed_evidence_transactions_analyzed {}\nledgerflow_seed_analysis_jobs_queued {}\nledgerflow_address_jobs_deferred_generic {}\nledgerflow_economic_verification_pending {}\nledgerflow_economic_verification_failed {}\nledgerflow_economic_verified_total {}\nledgerflow_economic_insufficient_evidence_total {}\nledgerflow_economic_dismissed_total {}\nledgerflow_economic_last_result_age_seconds {}\nledgerflow_seed_last_completed_run_age_seconds {}\nledgerflow_observation_objects {}\nledgerflow_observation_ready {}\nledgerflow_observation_failed {}\nledgerflow_observation_activity_analyzed_total {}\nledgerflow_observation_last_update_age_seconds {}\nledgerflow_configured_chain_id {}\n",
        row.get::<i64, _>("projects"),
        row.get::<i64, _>("active_monitors"),
        row.get::<i64, _>("monitor_errors"),
        row.get::<i64, _>("events"),
        row.get::<i64, _>("analysis_failures"),
        row.get::<i64, _>("queued"),
        row.get::<i64, _>("retries"),
        row.get::<i64, _>("dlq"),
        row.get::<i64, _>("seed_addresses"),
        row.get::<i64, _>("mev_candidates"),
        row.get::<i64, _>("seed_relationships"),
        row.get::<i64, _>("seeds_with_relationships"),
        row.get::<i64, _>("seed_evidence_transactions"),
        row.get::<i64, _>("analyzed_seed_evidence_transactions"),
        row.get::<i64, _>("queued_seed_jobs"),
        row.get::<i64, _>("deferred_generic_jobs"),
        row.get::<i64, _>("economic_verification_pending"),
        row.get::<i64, _>("economic_verification_failed"),
        row.get::<i64, _>("economic_verified"),
        row.get::<i64, _>("economic_insufficient"),
        row.get::<i64, _>("economic_dismissed"),
        row.get::<Option<f64>, _>("economic_verification_age_seconds")
            .map(|value| value.to_string())
            .unwrap_or_else(|| "NaN".to_owned()),
        row.get::<Option<f64>, _>("seed_run_age_seconds")
            .map(|value| value.to_string())
            .unwrap_or_else(|| "NaN".to_owned()),
        row.get::<i64, _>("observation_objects"),
        row.get::<i64, _>("observation_ready"),
        row.get::<i64, _>("observation_failed"),
        row.get::<i64, _>("observation_activity_analyzed"),
        row.get::<Option<f64>, _>("observation_update_age_seconds")
            .map(|value| value.to_string())
            .unwrap_or_else(|| "NaN".to_owned()),
        state.default_chain_id,
    );
    (StatusCode::OK, body)
}

async fn get_intelligence_seed_summary(
    State(state): State<ApiState>,
    Query(query): Query<SeedIntelligenceQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let chain_id = query.chain_id.unwrap_or(state.default_chain_id);
    let row = sqlx::query(
        r#"
        select
          (select count(distinct address) from effective_intelligence_seeds
           where chain_id = $1) as total_seed_addresses,
          (select count(distinct address) from effective_intelligence_seeds
           where chain_id = $1 and status = 'active') as active_seed_addresses,
          (select count(distinct address) from effective_intelligence_seeds
           where chain_id = $1 and status = 'candidate') as candidate_seed_addresses,
          (select count(distinct address) from intelligence_seed_addresses
           where chain_id = $1 and status = 'active'
             and seed_kind = 'official_contract') as official_seed_addresses,
          (select count(distinct address) from intelligence_seed_addresses
           where chain_id = $1 and status = 'candidate'
             and seed_kind = 'mev_actor') as mev_candidate_addresses,
          coalesce((select (metadata #>> '{summary,relationship_count}')::bigint
                    from seed_intelligence_runs
                    where chain_id = $1 and status = 'completed'
                    order by completed_at desc limit 1), 0) as relationship_count,
          coalesce((select (metadata #>> '{summary,seeds_with_relationships}')::bigint
                    from seed_intelligence_runs
                    where chain_id = $1 and status = 'completed'
                    order by completed_at desc limit 1), 0) as seeds_with_relationships,
          coalesce((select (metadata #>> '{summary,evidence_transaction_count}')::bigint
                    from seed_intelligence_runs
                    where chain_id = $1 and status = 'completed'
                    order by completed_at desc limit 1), 0) as evidence_transaction_count,
          coalesce((select (metadata #>> '{summary,analyzed_evidence_transaction_count}')::bigint
                    from seed_intelligence_runs
                    where chain_id = $1 and status = 'completed'
                    order by completed_at desc limit 1), 0) as analyzed_evidence_transaction_count,
          coalesce((select (metadata #>> '{summary,queued_seed_jobs}')::bigint
                    from seed_intelligence_runs
                    where chain_id = $1 and status = 'completed'
                    order by completed_at desc limit 1), 0) as queued_seed_jobs,
          coalesce((select (metadata #>> '{summary,deferred_generic_jobs}')::bigint
                    from seed_intelligence_runs
                    where chain_id = $1 and status = 'completed'
                    order by completed_at desc limit 1), 0) as deferred_generic_jobs,
          (select count(*) from economic_verification_jobs
           where chain_id = $1 and status in ('queued', 'running'))
             as economic_verification_pending,
          (select count(*) from economic_verification_jobs
           where chain_id = $1 and status = 'failed')
             as economic_verification_failed,
          (select count(*) from economic_verification_results
           where chain_id = $1 and result_status = 'verified')
             as economic_verified,
          (select count(*) from economic_verification_results
           where chain_id = $1 and result_status = 'insufficient_evidence')
             as economic_insufficient_evidence,
          (select count(*) from economic_verification_results
           where chain_id = $1 and result_status = 'dismissed')
             as economic_dismissed,
          (select max(completed_at) from seed_intelligence_runs
           where chain_id = $1 and status = 'completed') as latest_completed_run_at
        "#,
    )
    .bind(chain_id as i64)
    .fetch_one(&state.pilot_pool)
    .await
    .map_err(|error| ApiError::from(LedgerError::Persistence(error.to_string())))?;
    Ok(Json(json!({
        "chain_id": chain_id,
        "total_seed_addresses": row.get::<i64, _>("total_seed_addresses"),
        "active_seed_addresses": row.get::<i64, _>("active_seed_addresses"),
        "candidate_seed_addresses": row.get::<i64, _>("candidate_seed_addresses"),
        "official_seed_addresses": row.get::<i64, _>("official_seed_addresses"),
        "mev_candidate_addresses": row.get::<i64, _>("mev_candidate_addresses"),
        "relationship_count": row.get::<i64, _>("relationship_count"),
        "seeds_with_relationships": row.get::<i64, _>("seeds_with_relationships"),
        "evidence_transaction_count": row.get::<i64, _>("evidence_transaction_count"),
        "analyzed_evidence_transaction_count": row.get::<i64, _>("analyzed_evidence_transaction_count"),
        "queued_seed_jobs": row.get::<i64, _>("queued_seed_jobs"),
        "deferred_generic_jobs": row.get::<i64, _>("deferred_generic_jobs"),
        "economic_verification_pending": row.get::<i64, _>("economic_verification_pending"),
        "economic_verification_failed": row.get::<i64, _>("economic_verification_failed"),
        "economic_verified": row.get::<i64, _>("economic_verified"),
        "economic_insufficient_evidence": row.get::<i64, _>("economic_insufficient_evidence"),
        "economic_dismissed": row.get::<i64, _>("economic_dismissed"),
        "latest_completed_run_at": row.get::<Option<chrono::DateTime<chrono::Utc>>, _>("latest_completed_run_at"),
        "queue_counts_are_snapshot": true,
        "limitations": [
            "Candidate MEV seeds are selected for analysis and are not verified identities.",
            "Observed call targets and pools do not prove ownership, executor roles, intent, or profit.",
            "Only explicit candidate evidence transactions are scheduled; this is not full-history coverage.",
            "Generic jobs above priority 39 remain indexed but are intentionally deferred."
        ]
    })))
}

async fn list_intelligence_seeds(
    State(state): State<ApiState>,
    Query(query): Query<SeedIntelligenceQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let chain_id = query.chain_id.unwrap_or(state.default_chain_id);
    let limit = query.limit.unwrap_or(100).clamp(1, 500) as i64;
    let rows = sqlx::query(
        r#"
        select seed_id, chain_id, address, seed_kind, roles, source_key, status,
               priority, confidence, evidence, first_seen_block, last_seen_block,
               first_seen_at, last_seen_at, updated_at
        from intelligence_seed_addresses
        where chain_id = $1
          and ($2::text is null or seed_kind = $2)
          and ($3::text is null or status = $3)
        order by priority asc, confidence desc, last_seen_block desc nulls last,
                 address, source_key
        limit $4
        "#,
    )
    .bind(chain_id as i64)
    .bind(query.seed_kind.as_deref())
    .bind(query.status.as_deref())
    .bind(limit)
    .fetch_all(&state.pilot_pool)
    .await
    .map_err(|error| ApiError::from(LedgerError::Persistence(error.to_string())))?;
    let seeds = rows
        .into_iter()
        .map(|row| {
            json!({
                "seed_id": row.get::<Uuid, _>("seed_id"),
                "chain_id": row.get::<i64, _>("chain_id"),
                "address": row.get::<String, _>("address"),
                "seed_kind": row.get::<String, _>("seed_kind"),
                "roles": row.get::<Vec<String>, _>("roles"),
                "source_key": row.get::<String, _>("source_key"),
                "status": row.get::<String, _>("status"),
                "priority": row.get::<i32, _>("priority"),
                "confidence": row.get::<f64, _>("confidence"),
                "evidence": row.get::<sqlx::types::Json<serde_json::Value>, _>("evidence").0,
                "first_seen_block": row.get::<Option<i64>, _>("first_seen_block"),
                "last_seen_block": row.get::<Option<i64>, _>("last_seen_block"),
                "first_seen_at": row.get::<chrono::DateTime<chrono::Utc>, _>("first_seen_at"),
                "last_seen_at": row.get::<chrono::DateTime<chrono::Utc>, _>("last_seen_at"),
                "updated_at": row.get::<chrono::DateTime<chrono::Utc>, _>("updated_at")
            })
        })
        .collect::<Vec<_>>();
    Ok(Json(json!({"chain_id": chain_id, "seeds": seeds})))
}

async fn list_intelligence_seed_relationships(
    State(state): State<ApiState>,
    Query(query): Query<SeedIntelligenceQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let chain_id = query.chain_id.unwrap_or(state.default_chain_id);
    let address = query.address.as_deref().map(str::to_ascii_lowercase);
    let status = query.status.as_deref().unwrap_or("observed");
    let limit = query.limit.unwrap_or(100).clamp(1, 500) as i64;
    let rows = sqlx::query(
        r#"
        select relationship_id, chain_id, seed_address, related_address,
               relationship_type, source_key, status, transaction_count,
               sample_transaction_hashes, first_seen_block, last_seen_block,
               confidence, evidence, first_seen_at, last_seen_at, expires_at,
               updated_at
        from intelligence_seed_relationships
        where chain_id = $1
          and ($2::text is null or seed_address = $2 or related_address = $2)
          and ($3::text is null or relationship_type = $3)
          and status = $4
          and (expires_at is null or expires_at > now() or status <> 'observed')
        order by transaction_count desc, last_seen_block desc,
                 seed_address, related_address, relationship_type
        limit $5
        "#,
    )
    .bind(chain_id as i64)
    .bind(address.as_deref())
    .bind(query.relationship_type.as_deref())
    .bind(status)
    .bind(limit)
    .fetch_all(&state.pilot_pool)
    .await
    .map_err(|error| ApiError::from(LedgerError::Persistence(error.to_string())))?;
    let relationships = rows
        .into_iter()
        .map(|row| {
            json!({
                "relationship_id": row.get::<Uuid, _>("relationship_id"),
                "chain_id": row.get::<i64, _>("chain_id"),
                "seed_address": row.get::<String, _>("seed_address"),
                "related_address": row.get::<String, _>("related_address"),
                "relationship_type": row.get::<String, _>("relationship_type"),
                "source_key": row.get::<String, _>("source_key"),
                "status": row.get::<String, _>("status"),
                "transaction_count": row.get::<i64, _>("transaction_count"),
                "sample_transaction_hashes": row.get::<Vec<String>, _>("sample_transaction_hashes"),
                "first_seen_block": row.get::<i64, _>("first_seen_block"),
                "last_seen_block": row.get::<i64, _>("last_seen_block"),
                "confidence": row.get::<f64, _>("confidence"),
                "evidence": row.get::<sqlx::types::Json<serde_json::Value>, _>("evidence").0,
                "first_seen_at": row.get::<chrono::DateTime<chrono::Utc>, _>("first_seen_at"),
                "last_seen_at": row.get::<chrono::DateTime<chrono::Utc>, _>("last_seen_at"),
                "expires_at": row.get::<Option<chrono::DateTime<chrono::Utc>>, _>("expires_at"),
                "updated_at": row.get::<chrono::DateTime<chrono::Utc>, _>("updated_at")
            })
        })
        .collect::<Vec<_>>();
    Ok(Json(json!({
        "chain_id": chain_id,
        "semantic_boundary": "observed_relationship_only",
        "relationships": relationships
    })))
}

async fn get_intelligence_seed_coverage(
    State(state): State<ApiState>,
    Query(query): Query<SeedIntelligenceQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let chain_id = query.chain_id.unwrap_or(state.default_chain_id);
    let address = query.address.as_deref().map(str::to_ascii_lowercase);
    let limit = query.limit.unwrap_or(100).clamp(1, 500) as i64;
    let rows = sqlx::query(
        r#"
        select
          seeds.address,
          seeds.status,
          seeds.seed_kinds,
          coalesce(relationships.relationship_count, 0) as relationship_count,
          coalesce(relationships.relationship_type_count, 0) as relationship_type_count,
          coalesce(evidence.evidence_transaction_count, 0) as evidence_transaction_count,
          coalesce(evidence.analyzed_evidence_transaction_count, 0)
            as analyzed_evidence_transaction_count,
          profiles.last_analyzed_at as address_last_analyzed_at
        from effective_intelligence_seeds seeds
        left join address_profiles profiles
          on profiles.chain_id = seeds.chain_id
         and profiles.address = seeds.address
        left join lateral (
          select
            count(*)::bigint as relationship_count,
            count(distinct relationship_type)::bigint as relationship_type_count
          from intelligence_seed_relationships observed
          where observed.chain_id = seeds.chain_id
            and observed.seed_address = seeds.address
            and observed.status = 'observed'
            and (observed.expires_at is null or observed.expires_at > now())
        ) relationships on true
        left join lateral (
          select
            count(*)::bigint as evidence_transaction_count,
            count(analyses.tx_hash)::bigint as analyzed_evidence_transaction_count
          from (
            select candidates.tx_hash
            from arbitrage_candidates candidates
            where candidates.chain_id = seeds.chain_id
              and candidates.sender_address = seeds.address
              and candidates.block_timestamp >= now() - interval '30 days'
            order by candidates.block_number desc, candidates.tx_hash
            limit 8
          ) selected
          left join transaction_analysis analyses
            on analyses.chain_id = seeds.chain_id
           and analyses.tx_hash = selected.tx_hash
        ) evidence on true
        where seeds.chain_id = $1
          and ($2::text is null or seeds.address = $2)
          and ($3::text is null or $3 = any(seeds.seed_kinds))
          and ($4::text is null or seeds.status = $4)
        order by seeds.priority, seeds.confidence desc, seeds.address
        limit $5
        "#,
    )
    .bind(chain_id as i64)
    .bind(address.as_deref())
    .bind(query.seed_kind.as_deref())
    .bind(query.status.as_deref())
    .bind(limit)
    .fetch_all(&state.pilot_pool)
    .await
    .map_err(|error| ApiError::from(LedgerError::Persistence(error.to_string())))?;
    let coverage = rows
        .into_iter()
        .map(|row| {
            let evidence_count = row.get::<i64, _>("evidence_transaction_count");
            let analyzed_count = row.get::<i64, _>("analyzed_evidence_transaction_count");
            json!({
                "address": row.get::<String, _>("address"),
                "status": row.get::<String, _>("status"),
                "seed_kinds": row.get::<Vec<String>, _>("seed_kinds"),
                "relationship_count": row.get::<i64, _>("relationship_count"),
                "relationship_type_count": row.get::<i64, _>("relationship_type_count"),
                "evidence_transaction_count": evidence_count,
                "analyzed_evidence_transaction_count": analyzed_count,
                "evidence_analysis_coverage": if evidence_count > 0 {
                    Some(analyzed_count as f64 / evidence_count as f64)
                } else {
                    None
                },
                "address_last_analyzed_at": row.get::<Option<chrono::DateTime<chrono::Utc>>, _>("address_last_analyzed_at")
            })
        })
        .collect::<Vec<_>>();
    Ok(Json(json!({
        "chain_id": chain_id,
        "coverage_mode": "latest_8_explicit_candidate_evidence_transactions",
        "full_history_coverage": false,
        "seeds": coverage
    })))
}

async fn get_pilot_evidence_pack(
    State(state): State<ApiState>,
    Path(event_id): Path<Uuid>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let row = sqlx::query("SELECT evidence_pack FROM pilot_events WHERE event_id=$1")
        .bind(event_id)
        .fetch_optional(&state.pilot_pool)
        .await
        .map_err(|e| ApiError::from(LedgerError::Persistence(e.to_string())))?
        .ok_or_else(|| ApiError::from(LedgerError::NotFound(format!("pilot event {event_id}"))))?;
    let sqlx::types::Json(value): sqlx::types::Json<serde_json::Value> = row
        .try_get("evidence_pack")
        .map_err(|e| ApiError::from(LedgerError::Persistence(e.to_string())))?;
    Ok(Json(value))
}

async fn list_pilot_events(
    State(state): State<ApiState>,
    Path(project_id): Path<String>,
    Query(query): Query<PilotEventsQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let rows=sqlx::query("SELECT evidence_pack FROM pilot_events WHERE project_id=$1 ORDER BY created_at DESC,event_id DESC LIMIT $2").bind(&project_id).bind(query.limit.unwrap_or(100).clamp(1,500) as i64).fetch_all(&state.pilot_pool).await.map_err(|e|ApiError::from(LedgerError::Persistence(e.to_string())))?;
    let events = rows
        .into_iter()
        .filter_map(|row| {
            row.try_get::<sqlx::types::Json<serde_json::Value>, _>("evidence_pack")
                .ok()
                .map(|v| v.0)
        })
        .collect::<Vec<_>>();
    Ok(Json(json!({"project_id":project_id,"events":events})))
}

async fn list_pilot_deliveries(
    State(state): State<ApiState>,
    Path(project_id): Path<String>,
    Query(query): Query<PilotEventsQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let rows=sqlx::query("SELECT d.delivery_id,d.event_id,d.destination_id,d.status,d.attempt_count,d.next_attempt_at,d.last_error,d.telegram_message_id,d.sent_at,d.created_at,d.updated_at,e.event_type,e.tx_hash FROM pilot_alert_deliveries d JOIN pilot_events e ON e.event_id=d.event_id WHERE e.project_id=$1 ORDER BY d.created_at DESC,d.delivery_id DESC LIMIT $2").bind(&project_id).bind(query.limit.unwrap_or(100).clamp(1,500) as i64).fetch_all(&state.pilot_pool).await.map_err(|e|ApiError::from(LedgerError::Persistence(e.to_string())))?;
    let deliveries = rows
        .into_iter()
        .map(|row| {
            json!({
                "delivery_id": row.get::<Uuid, _>("delivery_id"),
                "event_id": row.get::<Uuid, _>("event_id"),
                "destination_id": row.get::<Uuid, _>("destination_id"),
                "event_type": row.get::<String, _>("event_type"),
                "tx_hash": row.get::<String, _>("tx_hash"),
                "status": row.get::<String, _>("status"),
                "attempt_count": row.get::<i32, _>("attempt_count"),
                "next_attempt_at": row.get::<chrono::DateTime<chrono::Utc>, _>("next_attempt_at"),
                "last_error": row.get::<Option<String>, _>("last_error"),
                "telegram_message_id": row.get::<Option<String>, _>("telegram_message_id"),
                "sent_at": row.get::<Option<chrono::DateTime<chrono::Utc>>, _>("sent_at"),
                "created_at": row.get::<chrono::DateTime<chrono::Utc>, _>("created_at"),
                "updated_at": row.get::<chrono::DateTime<chrono::Utc>, _>("updated_at")
            })
        })
        .collect::<Vec<_>>();
    Ok(Json(
        json!({"project_id":project_id,"deliveries":deliveries}),
    ))
}

async fn get_latest_pilot_report(
    State(state): State<ApiState>,
    Path(project_id): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let row=sqlx::query("SELECT report_id,week_start,week_end,markdown,html,inputs_digest,created_at FROM pilot_weekly_reports WHERE project_id=$1 ORDER BY week_start DESC LIMIT 1").bind(&project_id).fetch_optional(&state.pilot_pool).await.map_err(|e|ApiError::from(LedgerError::Persistence(e.to_string())))?.ok_or_else(||ApiError::from(LedgerError::NotFound(format!("weekly report for {project_id}"))))?;
    Ok(Json(
        json!({"report_id":row.get::<Uuid,_>("report_id"),"project_id":project_id,"week_start":row.get::<chrono::NaiveDate,_>("week_start"),"week_end":row.get::<chrono::NaiveDate,_>("week_end"),"markdown":row.get::<String,_>("markdown"),"html":row.get::<String,_>("html"),"inputs_digest":row.get::<String,_>("inputs_digest"),"created_at":row.get::<chrono::DateTime<chrono::Utc>,_>("created_at")}),
    ))
}

async fn list_behavior_reviews(
    State(state): State<ApiState>,
    Query(query): Query<BehaviorReviewQuery>,
) -> Result<Json<Vec<BehaviorReviewItem>>, ApiError> {
    let reviews = state
        .behavior_review_service
        .list(
            query.chain_id,
            query.status.as_deref(),
            query.limit.unwrap_or(50),
        )
        .await
        .map_err(behavior_review_api_error)?;
    Ok(Json(reviews))
}

async fn decide_behavior_review(
    State(state): State<ApiState>,
    Path(review_id): Path<String>,
    Json(request): Json<BehaviorReviewDecisionRequest>,
) -> Result<Json<BehaviorReviewItem>, ApiError> {
    let review_id = Uuid::parse_str(&review_id)
        .map_err(|error| ApiError::bad_request(format!("invalid review id: {error}")))?;
    let review = state
        .behavior_review_service
        .decide(
            review_id,
            &request.decision,
            &request.reviewer,
            request.rationale.as_deref(),
        )
        .await
        .map_err(behavior_review_api_error)?;
    Ok(Json(review))
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
    Ok(Json(
        build_analysis_envelope(&state, analysis, evidence_pack).await?,
    ))
}

async fn get_analysis(
    State(state): State<ApiState>,
    Path(tx_hash): Path<String>,
    Query(query): Query<AnalysisQuery>,
) -> Result<Json<AnalysisEnvelope>, ApiError> {
    let chain_id = query.chain_id.unwrap_or(state.default_chain_id);
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
    Ok(Json(
        build_analysis_envelope(&state, analysis, evidence_pack).await?,
    ))
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

async fn get_address_intelligence(
    State(state): State<ApiState>,
    Query(query): Query<AddressIntelligenceQuery>,
) -> Result<Json<AddressIntelligenceEnvelope>, ApiError> {
    let chain_id = query.chain_id.unwrap_or(state.default_chain_id);
    let addresses = query
        .addresses
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_ascii_lowercase)
        .collect::<Vec<_>>();
    let profiles = state
        .service
        .repository
        .list_address_profiles(chain_id, &addresses)
        .await
        .map_err(ApiError::from)?;
    let labels = state
        .service
        .repository
        .list_address_labels(chain_id, &addresses)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(AddressIntelligenceEnvelope { profiles, labels }))
}

async fn enqueue_address_intelligence(
    State(state): State<ApiState>,
    Json(request): Json<EnqueueAddressIntelligenceRequest>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let candidates = request
        .addresses
        .into_iter()
        .filter_map(|address| normalize_address(&address))
        .map(|address| AddressAnalysisCandidate {
            address,
            priority: 30,
            metadata: json!({ "source": "address_analysis_page", "roles": ["manual_lookup"] }),
        })
        .collect::<Vec<_>>();
    let queued = state
        .service
        .repository
        .enqueue_address_analysis_candidates(request.chain_id, &candidates)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(json!({ "queued": queued })))
}

async fn get_address_summary(
    State(state): State<ApiState>,
    Query(query): Query<AddressSummaryQuery>,
) -> Result<Json<AddressSummaryEnvelope>, ApiError> {
    let chain_id = query.chain_id.unwrap_or(state.default_chain_id);
    let address = normalize_address(&query.address)
        .ok_or_else(|| ApiError::bad_request("invalid address".to_owned()))?;
    let addresses = vec![address.clone()];
    let profiles = state
        .service
        .repository
        .list_address_profiles(chain_id, &addresses)
        .await
        .map_err(ApiError::from)?;
    let labels = state
        .service
        .repository
        .list_address_labels(chain_id, &addresses)
        .await
        .map_err(ApiError::from)?;
    let lite = query.lite.unwrap_or(false);
    let native_balance_raw = state
        .rpc_client
        .get_balance(chain_id, &address, None)
        .await
        .ok()
        .and_then(|value| hex_quantity_to_decimal(&value));
    let should_enqueue_history = profiles
        .first()
        .map(|profile| !profile_history_is_fresh(profile))
        .unwrap_or(true);
    let queued = if should_enqueue_history {
        state
            .service
            .repository
            .enqueue_address_analysis_candidates(
                chain_id,
                &[AddressAnalysisCandidate {
                    address: address.clone(),
                    priority: 5,
                    metadata: json!({
                        "source": "address_summary",
                        "roles": ["manual_lookup", "observed_object"],
                        "history_scope": "bounded_recent_activity"
                    }),
                }],
            )
            .await
            .map_err(ApiError::from)?
            > 0
    } else {
        false
    };
    let analyses = if lite {
        Vec::new()
    } else {
        state
            .service
            .repository
            .list_analyses_for_observed_addresses(chain_id, &addresses, None, None)
            .await
            .map_err(ApiError::from)?
    };
    let mut behavior = std::collections::BTreeMap::<String, (u64, i128)>::new();
    let mut gas_total = 0i128;
    let mut recent = analyses.clone();
    recent.sort_by(|left, right| right.block_timestamp.cmp(&left.block_timestamp));
    for analysis in &analyses {
        let gas = analysis_gas_raw(analysis);
        gas_total += gas;
        let kind = analysis_kind(analysis);
        let entry = behavior.entry(kind).or_insert((0, 0));
        entry.0 += 1;
        entry.1 += gas;
    }
    let behavior_rows = behavior
        .into_iter()
        .map(|(behavior, (tx_count, gas))| AddressBehaviorSummaryRow {
            behavior,
            tx_count,
            gas_spent_raw: gas.to_string(),
        })
        .collect::<Vec<_>>();
    let recent_txs = recent
        .into_iter()
        .take(25)
        .map(|analysis| AddressRecentTxRow {
            tx_hash: analysis.tx_hash.clone(),
            block_timestamp: analysis.block_timestamp.map(|value| value.to_rfc3339()),
            behavior: analysis_kind(&analysis),
            gas_raw: analysis_gas_raw(&analysis).to_string(),
        })
        .collect::<Vec<_>>();
    Ok(Json(AddressSummaryEnvelope {
        chain_id,
        address,
        native_balance_raw,
        profile: profiles.into_iter().next(),
        labels,
        analyzed_tx_count: analyses.len() as u64,
        gas_spent_raw: gas_total.to_string(),
        behavior_rows,
        recent_txs,
        queued,
    }))
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
    Ok(Json(
        build_analysis_envelope(&state, analysis, evidence_pack).await?,
    ))
}

async fn build_analysis_envelope(
    state: &ApiState,
    analysis: TransactionAnalysis,
    evidence_pack: crate::domain::entities::EvidencePack,
) -> Result<AnalysisEnvelope, ApiError> {
    let addresses = collect_analysis_addresses(&analysis);
    let profiles = state
        .service
        .repository
        .list_address_profiles(analysis.chain_id, &addresses)
        .await
        .map_err(ApiError::from)?;
    let labels = state
        .service
        .repository
        .list_address_labels(analysis.chain_id, &addresses)
        .await
        .map_err(ApiError::from)?;
    Ok(AnalysisEnvelope {
        analysis,
        evidence_pack,
        address_intelligence: AddressIntelligenceEnvelope { profiles, labels },
    })
}

fn collect_analysis_addresses(analysis: &TransactionAnalysis) -> Vec<String> {
    let mut addresses = std::collections::BTreeSet::new();
    insert_analysis_address(&mut addresses, &analysis.from_address);
    if let Some(address) = analysis.to_address.as_deref() {
        insert_analysis_address(&mut addresses, address);
    }
    for address in &analysis.normalized_execution_facts.participants {
        insert_analysis_address(&mut addresses, address);
    }
    for log in &analysis.normalized_execution_facts.log_summaries {
        if let Some(address) = log.get("address").and_then(|value| value.as_str()) {
            insert_analysis_address(&mut addresses, address);
        }
    }
    for flow in &analysis.derived_flows {
        insert_analysis_address(&mut addresses, &flow.from_address);
        insert_analysis_address(&mut addresses, &flow.to_address);
        if let Some(address) = flow.token_address.as_deref() {
            insert_analysis_address(&mut addresses, address);
        }
    }
    addresses.into_iter().collect()
}

fn insert_analysis_address(addresses: &mut std::collections::BTreeSet<String>, address: &str) {
    let address = address.trim().to_ascii_lowercase();
    if address.len() == 42 && address.starts_with("0x") {
        addresses.insert(address);
    }
}

fn normalize_address(address: &str) -> Option<String> {
    let address = address.trim().to_ascii_lowercase();
    if address.len() == 42 && address.starts_with("0x") {
        Some(address)
    } else {
        None
    }
}

fn hex_quantity_to_decimal(value: &str) -> Option<String> {
    let hex = value.trim().strip_prefix("0x").unwrap_or(value.trim());
    u128::from_str_radix(hex, 16)
        .ok()
        .map(|value| value.to_string())
}

fn analysis_kind(analysis: &TransactionAnalysis) -> String {
    analysis
        .scenario_outputs
        .get("storyline")
        .and_then(|value| value.get("kind"))
        .and_then(|value| value.as_str())
        .map(str::to_owned)
        .or_else(|| {
            analysis
                .recognizer_results
                .first()
                .map(|value| value.label.clone())
        })
        .unwrap_or_else(|| "unknown".to_owned())
}

fn analysis_gas_raw(analysis: &TransactionAnalysis) -> i128 {
    analysis
        .scenario_outputs
        .get("storyline")
        .and_then(|value| value.get("net_effect"))
        .and_then(|value| value.get("gas"))
        .and_then(|value| value.get("amount_raw"))
        .and_then(|value| value.as_str())
        .and_then(|value| value.parse::<i128>().ok())
        .unwrap_or(0)
}

fn profile_history_is_fresh(profile: &AddressProfile) -> bool {
    profile
        .metadata
        .get("address_history_backfilled_at")
        .and_then(serde_json::Value::as_str)
        .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
        .map(|value| {
            value.with_timezone(&chrono::Utc)
                >= chrono::Utc::now()
                    - chrono::Duration::minutes(ADDRESS_HISTORY_REFRESH_INTERVAL_MINUTES)
        })
        .unwrap_or(false)
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
                    role: item.role.or_else(|| Some("seed".to_owned())),
                    metadata: json!({
                        "source": "user_seed",
                        "auto_discovery_seed": true,
                    }),
                })
                .collect(),
        )
        .await
        .map_err(ApiError::from)?;
    if state.auto_backfill_on_cluster_create && state.backfill_service.is_configured() {
        state
            .backfill_service
            .enqueue_cluster(
                cluster.cluster_id,
                ServiceClusterBackfillRequest {
                    start_block: Some(0),
                    end_block: None,
                    force_reanalyze: false,
                    query_limit: None,
                },
            )
            .await
            .map_err(ApiError::from)?;
    }
    Ok(Json(cluster))
}

async fn get_cluster(
    State(state): State<ApiState>,
    Path(cluster_id): Path<String>,
) -> Result<Json<AddressCluster>, ApiError> {
    let cluster_id = Uuid::parse_str(&cluster_id)
        .map_err(|err| ApiError::conflict(format!("invalid cluster id: {err}")))?;
    let cluster = state
        .cluster_service
        .get_cluster(cluster_id)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(cluster))
}

async fn update_cluster(
    State(state): State<ApiState>,
    Path(cluster_id): Path<String>,
    Json(request): Json<UpdateClusterRequest>,
) -> Result<Json<AddressCluster>, ApiError> {
    let cluster_id = Uuid::parse_str(&cluster_id)
        .map_err(|err| ApiError::conflict(format!("invalid cluster id: {err}")))?;
    let existing = state
        .cluster_service
        .get_cluster(cluster_id)
        .await
        .map_err(ApiError::from)?;
    let existing_by_address = existing
        .addresses
        .into_iter()
        .map(|address| (address.address.to_ascii_lowercase(), address))
        .collect::<std::collections::BTreeMap<_, _>>();
    let addresses = request
        .addresses
        .into_iter()
        .map(|entry| {
            let existing = existing_by_address.get(&entry.address.to_ascii_lowercase());
            let metadata = if existing
                .and_then(|address| address.metadata.get("source"))
                .and_then(serde_json::Value::as_str)
                == Some("auto_seed_discovery")
            {
                existing
                    .map(|address| address.metadata.clone())
                    .unwrap_or_else(|| json!({}))
            } else {
                json!({
                    "source": "user_seed",
                    "auto_discovery_seed": true,
                })
            };
            ClusterAddress {
                address: entry.address,
                label: entry
                    .label
                    .or_else(|| existing.and_then(|address| address.label.clone())),
                role: entry
                    .role
                    .or_else(|| existing.and_then(|address| address.role.clone()))
                    .or_else(|| Some("seed".to_owned())),
                metadata,
            }
        })
        .collect();
    let cluster = state
        .cluster_service
        .update_cluster(
            cluster_id,
            request.chain_id,
            &request.owner_label,
            &request.name,
            request.description,
            addresses,
        )
        .await
        .map_err(ApiError::from)?;
    Ok(Json(cluster))
}

async fn delete_cluster(
    State(state): State<ApiState>,
    Path(cluster_id): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let cluster_id = Uuid::parse_str(&cluster_id)
        .map_err(|err| ApiError::conflict(format!("invalid cluster id: {err}")))?;
    state
        .cluster_service
        .delete_cluster(cluster_id)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(json!({ "status": "deleted" })))
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

async fn get_cluster_asset_changes(
    State(state): State<ApiState>,
    Path(cluster_id): Path<String>,
    Query(query): Query<ClusterSummaryQuery>,
) -> Result<Json<ClusterAssetChangeReport>, ApiError> {
    let cluster_id = Uuid::parse_str(&cluster_id)
        .map_err(|err| ApiError::conflict(format!("invalid cluster id: {err}")))?;
    let start = parse_time(query.start.as_deref())?;
    let end = parse_time(query.end.as_deref())?;
    let report = state
        .cluster_service
        .get_cluster_asset_changes(cluster_id, start, end)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(report))
}

async fn backfill_cluster(
    State(state): State<ApiState>,
    Path(cluster_id): Path<String>,
    Json(request): Json<ClusterBackfillRequest>,
) -> Result<Json<ClusterBackfillResult>, ApiError> {
    let cluster_id = Uuid::parse_str(&cluster_id)
        .map_err(|err| ApiError::conflict(format!("invalid cluster id: {err}")))?;
    let run = state
        .backfill_service
        .enqueue_cluster(
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
    let coverage = state
        .backfill_service
        .get_cluster_coverage(cluster_id)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(ClusterBackfillResult { run, coverage }))
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

#[derive(Debug, Deserialize)]
pub struct ClassifiedTransactionsQuery {
    pub limit: Option<u32>,
    pub page: Option<u32>,
    pub page_size: Option<u32>,
    pub behavior_tag: Option<String>,
    pub protocol: Option<String>,
    pub sort_by: Option<String>,
    pub sort_dir: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ClassifiedTransactionsPage {
    pub items: Vec<ClassifiedTransaction>,
    pub total: u64,
    pub page: u32,
    pub page_size: u32,
}

async fn get_cluster_tracking(
    State(state): State<ApiState>,
    Path(cluster_id): Path<String>,
) -> Result<Json<ClusterTrackingState>, ApiError> {
    let cluster_id = Uuid::parse_str(&cluster_id)
        .map_err(|err| ApiError::conflict(format!("invalid cluster id: {err}")))?;
    let tracking = state
        .tracking_service
        .get_tracking_state(cluster_id)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(tracking))
}

async fn enable_cluster_tracking(
    State(state): State<ApiState>,
    Path(cluster_id): Path<String>,
) -> Result<Json<ClusterTrackingState>, ApiError> {
    let cluster_id = Uuid::parse_str(&cluster_id)
        .map_err(|err| ApiError::conflict(format!("invalid cluster id: {err}")))?;
    let tracking = state
        .tracking_service
        .enable_tracking(cluster_id)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(tracking))
}

async fn pause_cluster_tracking(
    State(state): State<ApiState>,
    Path(cluster_id): Path<String>,
) -> Result<Json<ClusterTrackingState>, ApiError> {
    let cluster_id = Uuid::parse_str(&cluster_id)
        .map_err(|err| ApiError::conflict(format!("invalid cluster id: {err}")))?;
    let tracking = state
        .tracking_service
        .pause_tracking(cluster_id)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(tracking))
}

async fn list_classified_transactions(
    State(state): State<ApiState>,
    Path(cluster_id): Path<String>,
    Query(query): Query<ClassifiedTransactionsQuery>,
) -> Result<Json<ClassifiedTransactionsPage>, ApiError> {
    let cluster_id = Uuid::parse_str(&cluster_id)
        .map_err(|err| ApiError::conflict(format!("invalid cluster id: {err}")))?;
    let behavior_tag = parse_behavior_tag_query(query.behavior_tag.as_deref())?;
    let descending = !matches!(query.sort_dir.as_deref(), Some("asc"));
    let page = state
        .tracking_service
        .list_classified_transactions_page(
            cluster_id,
            query.page.unwrap_or(1),
            query.page_size.or(query.limit).unwrap_or(20),
            behavior_tag,
            query.protocol.clone(),
            query.sort_by.unwrap_or_else(|| "timestamp".to_owned()),
            descending,
        )
        .await
        .map_err(ApiError::from)?;
    Ok(Json(ClassifiedTransactionsPage {
        items: page.items,
        total: page.total,
        page: page.page,
        page_size: page.page_size,
    }))
}

async fn list_cluster_metrics(
    State(state): State<ApiState>,
    Path(cluster_id): Path<String>,
) -> Result<Json<Vec<ClusterMetricsSnapshot>>, ApiError> {
    let cluster_id = Uuid::parse_str(&cluster_id)
        .map_err(|err| ApiError::conflict(format!("invalid cluster id: {err}")))?;
    let snapshots = state
        .metrics_service
        .list_cluster_metrics(cluster_id)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(snapshots))
}

async fn get_cluster_strategy_analysis(
    State(state): State<ApiState>,
    Path(cluster_id): Path<String>,
    Query(query): Query<ClusterSummaryQuery>,
) -> Result<Json<ClusterStrategyAnalysis>, ApiError> {
    let cluster_id = Uuid::parse_str(&cluster_id)
        .map_err(|err| ApiError::conflict(format!("invalid cluster id: {err}")))?;
    let start = parse_time(query.start.as_deref())?;
    let end = parse_time(query.end.as_deref())?;
    let analysis = state
        .strategy_analysis_service
        .analyze_cluster(cluster_id, start, end)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(analysis))
}

async fn get_cluster_strategy_analysis_snapshot(
    State(state): State<ApiState>,
    Path(cluster_id): Path<String>,
    Query(query): Query<ClusterSummaryQuery>,
) -> Result<Json<ClusterStrategyReportSnapshot>, ApiError> {
    let cluster_id = Uuid::parse_str(&cluster_id)
        .map_err(|err| ApiError::conflict(format!("invalid cluster id: {err}")))?;
    let start = parse_time(query.start.as_deref())?;
    let end = parse_time(query.end.as_deref())?;
    let snapshot = state
        .strategy_analysis_service
        .get_cached_cluster_report_snapshot(cluster_id, start, end)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| {
            ApiError::from(LedgerError::NotFound(format!(
                "cluster strategy report snapshot for {cluster_id} not found"
            )))
        })?;
    Ok(Json(snapshot))
}

async fn refresh_cluster_strategy_analysis_snapshot(
    State(state): State<ApiState>,
    Path(cluster_id): Path<String>,
    Query(query): Query<ClusterSummaryQuery>,
) -> Result<Json<ClusterStrategyReportSnapshot>, ApiError> {
    let cluster_id = Uuid::parse_str(&cluster_id)
        .map_err(|err| ApiError::conflict(format!("invalid cluster id: {err}")))?;
    let start = parse_time(query.start.as_deref())?;
    let end = parse_time(query.end.as_deref())?;
    let snapshot = state
        .strategy_analysis_service
        .refresh_cluster_report_snapshot(cluster_id, start, end)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(snapshot))
}

async fn query_cluster_report_agent(
    State(state): State<ApiState>,
    Path(cluster_id): Path<String>,
    Json(request): Json<ReportAgentQueryRequest>,
) -> Result<Json<ReportAgentResponse>, ApiError> {
    let cluster_id = Uuid::parse_str(&cluster_id)
        .map_err(|err| ApiError::conflict(format!("invalid cluster id: {err}")))?;
    let cost_budget = request
        .cost_budget
        .ok_or_else(|| ApiError::bad_request("cost_budget is required".to_owned()))?;
    let query = ReportAgentQuery {
        cluster_id,
        window_start: parse_time(request.window_start.as_deref())?,
        window_end: parse_time(request.window_end.as_deref())?,
        all_time: request.all_time.unwrap_or(false),
        sections: request.sections.unwrap_or_default(),
        filters: request.filters.unwrap_or_default(),
        row_limit: request.row_limit.unwrap_or(10),
        cost_budget,
    };
    let response = state
        .report_agent_service
        .query_cluster_report(query)
        .await
        .map_err(report_agent_api_error)?;
    Ok(Json(response))
}

async fn list_cluster_alerts(
    State(state): State<ApiState>,
    Path(cluster_id): Path<String>,
    Query(query): Query<ClusterAlertsQuery>,
) -> Result<Json<Vec<ClusterAlert>>, ApiError> {
    let cluster_id = Uuid::parse_str(&cluster_id)
        .map_err(|err| ApiError::conflict(format!("invalid cluster id: {err}")))?;
    let alerts = state
        .alert_service
        .list_cluster_alerts(
            cluster_id,
            query.include_read.unwrap_or(false),
            query.limit.unwrap_or(20),
        )
        .await
        .map_err(ApiError::from)?;
    Ok(Json(alerts))
}

async fn mark_cluster_alert_read(
    State(state): State<ApiState>,
    Path((cluster_id, alert_id)): Path<(String, String)>,
) -> Result<Json<ClusterAlert>, ApiError> {
    let cluster_id = Uuid::parse_str(&cluster_id)
        .map_err(|err| ApiError::conflict(format!("invalid cluster id: {err}")))?;
    let alert_id = Uuid::parse_str(&alert_id)
        .map_err(|err| ApiError::conflict(format!("invalid alert id: {err}")))?;
    let alert = state
        .alert_service
        .mark_alert_read(cluster_id, alert_id)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(alert))
}

fn parse_time(value: Option<&str>) -> Result<Option<chrono::DateTime<chrono::Utc>>, ApiError> {
    match value {
        None => Ok(None),
        Some(value) => chrono::DateTime::parse_from_rfc3339(value)
            .map(|value| Some(value.with_timezone(&chrono::Utc)))
            .map_err(|err| ApiError::conflict(format!("invalid timestamp: {err}"))),
    }
}

fn parse_behavior_tag_query(
    value: Option<&str>,
) -> Result<Option<crate::domain::enums::BehaviorTag>, ApiError> {
    match value.map(str::trim).filter(|value| !value.is_empty()) {
        None => Ok(None),
        Some("arbitrage") => Ok(Some(crate::domain::enums::BehaviorTag::Arbitrage)),
        Some("position") => Ok(Some(crate::domain::enums::BehaviorTag::Position)),
        Some("lp") => Ok(Some(crate::domain::enums::BehaviorTag::Lp)),
        Some("liquidation") => Ok(Some(crate::domain::enums::BehaviorTag::Liquidation)),
        Some("unknown") => Ok(Some(crate::domain::enums::BehaviorTag::Unknown)),
        Some(other) => Err(ApiError::conflict(format!(
            "invalid behavior_tag filter: {other}"
        ))),
    }
}

pub struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn bad_request(message: String) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message,
        }
    }

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
                if message.contains("live intelligence is not available") =>
            {
                Self {
                    status: StatusCode::SERVICE_UNAVAILABLE,
                    message,
                }
            }
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

fn report_agent_api_error(value: LedgerError) -> ApiError {
    match value {
        LedgerError::AnalysisInvariant(message) => ApiError::conflict(message),
        other => ApiError::from(other),
    }
}

fn behavior_review_api_error(value: LedgerError) -> ApiError {
    match value {
        LedgerError::AnalysisInvariant(message) => ApiError::bad_request(message),
        other => ApiError::from(other),
    }
}

impl axum::response::IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        (self.status, Json(json!({ "error": self.message }))).into_response()
    }
}
