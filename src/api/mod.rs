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
use crate::domain::entities::{
    AddressAnalysisCandidate, AddressCluster, AddressLabel, AddressProfile, ClassifiedTransaction,
    ClusterAddress, ClusterAlert, ClusterMetricsSnapshot, ClusterSpendSummary,
    ClusterStrategyReportSnapshot, ClusterTrackingState, TransactionAnalysis,
};
use crate::ingestion::rpc::RpcClient;
use crate::plugins::loader::PluginManifest;
use crate::services::alerts::ClusterAlertService;
use crate::services::analysis::AnalysisService;
use crate::services::cluster::ClusterService;
use crate::services::cluster_backfill::{
    ClusterBackfillRequest as ServiceClusterBackfillRequest, ClusterBackfillResult,
    ClusterBackfillService,
};
use crate::services::cluster_tracking::ClusterTrackingService;
use crate::services::metrics::ClusterMetricsService;
use crate::services::report_agent::{
    ReportAgentFilters, ReportAgentQuery, ReportAgentResponse, ReportAgentSection,
    ReportAgentService,
};
use crate::services::strategy_analysis::{ClusterStrategyAnalysis, ClusterStrategyAnalysisService};

#[derive(Clone)]
pub struct ApiState {
    pub service: Arc<AnalysisService>,
    pub cluster_service: Arc<ClusterService>,
    pub backfill_service: Arc<ClusterBackfillService>,
    pub rpc_client: Arc<dyn RpcClient>,
    pub tracking_service: Arc<ClusterTrackingService>,
    pub metrics_service: Arc<ClusterMetricsService>,
    pub strategy_analysis_service: Arc<ClusterStrategyAnalysisService>,
    pub report_agent_service: Arc<ReportAgentService>,
    pub alert_service: Arc<ClusterAlertService>,
    pub auto_backfill_on_cluster_create: bool,
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

pub fn router(
    service: Arc<AnalysisService>,
    backfill_service: Arc<ClusterBackfillService>,
    rpc_client: Arc<dyn RpcClient>,
    auto_backfill_on_cluster_create: bool,
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
            service,
            cluster_service,
            backfill_service,
            rpc_client,
            tracking_service,
            metrics_service,
            strategy_analysis_service,
            report_agent_service,
            alert_service,
            auto_backfill_on_cluster_create,
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
    Ok(Json(
        build_analysis_envelope(&state, analysis, evidence_pack).await?,
    ))
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
    let chain_id = query.chain_id.unwrap_or(8453);
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
    let chain_id = query.chain_id.unwrap_or(8453);
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
        .map(|profile| !profile_history_backfilled(profile))
        .unwrap_or(true);
    let queued = if should_enqueue_history {
        state
            .service
            .repository
            .enqueue_address_analysis_candidates(
                chain_id,
                &[AddressAnalysisCandidate {
                    address: address.clone(),
                    priority: 25,
                    metadata: json!({ "source": "address_summary", "roles": ["manual_lookup"] }),
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
            .list_analyses_for_cluster(chain_id, &addresses, None, None)
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

fn profile_history_backfilled(profile: &AddressProfile) -> bool {
    profile
        .metadata
        .get("address_history_backfilled_at")
        .is_some()
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

impl axum::response::IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        (self.status, Json(json!({ "error": self.message }))).into_response()
    }
}
