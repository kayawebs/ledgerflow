use std::sync::Arc;

use axum::{
    Form, Json, Router,
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{Html, IntoResponse, Redirect, Response},
    routing::{get, post},
};
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::json;
use sqlx::{PgPool, Row, postgres::PgPoolOptions, types::Json as SqlJson};
use tracing::info;
use uuid::Uuid;

use crate::{
    core::errors::{LedgerError, LedgerResult},
    domain::{
        entities::{
            AddressCluster, AddressLabel, AddressProfile, AppUser, ClassifiedTransaction,
            ClusterAlert, ClusterAssetChangeReport, ClusterCoverage, ClusterMetricsSnapshot,
            ClusterSpendSummary, ClusterTrackingState, EvidencePack, TransactionAnalysis,
        },
        enums::{AddressHitDirection, ClusterBackfillStatus, VerificationPurpose},
    },
    services::{
        auth::{
            AuthService, LogVerificationSender, PostmarkVerificationSender,
            ResendVerificationSender, VerificationSender, verification_purpose_from_str,
            verification_purpose_to_str,
        },
        portal::{DashboardClusterRecord, PortalService},
    },
};

const DEFAULT_PRICE_ETH: &str = "0.5";
const SWAP_BLOCK_RANGE_MAX_BLOCKS: u64 = 2_000;
const UNISWAP_V2_SWAP_TOPIC_HEX: &str =
    "d78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822";
const UNISWAP_V3_SWAP_TOPIC_HEX: &str =
    "c42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67";

#[derive(Clone)]
pub struct WebState {
    pub api_base_url: String,
    pub indexer_api_base_url: Option<String>,
    pub indexer_api_bearer_token: Option<String>,
    pub bind_addr: String,
    pub default_chain_id: u64,
    pub web_base_url: String,
    pub session_cookie_name: String,
    pub pool: PgPool,
    pub client: Client,
    pub cryptohouse_url: String,
    pub auth_service: Arc<AuthService>,
    pub portal_service: Arc<PortalService>,
    pub email_delivery_mode: &'static str,
    pub email_delivery_ready: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmailDeliveryMode {
    Auto,
    Log,
    Postmark,
    Resend,
}

#[derive(Debug, Clone)]
pub struct WebConfig {
    pub bind_addr: String,
    pub api_base_url: String,
    pub indexer_api_base_url: Option<String>,
    pub indexer_api_bearer_token: Option<String>,
    pub database_url: String,
    pub default_chain_id: u64,
    pub web_base_url: String,
    pub session_cookie_name: String,
    pub resend_api_key: Option<String>,
    pub postmark_server_token: Option<String>,
    pub postmark_message_stream: String,
    pub email_from: Option<String>,
    pub email_delivery_mode: EmailDeliveryMode,
    pub verification_ttl_minutes: i64,
    pub session_ttl_days: i64,
    pub cryptohouse_url: String,
}

impl WebConfig {
    pub fn from_env() -> Self {
        let resend_api_key = first_env(&["LEDGERFLOW_RESEND_API_KEY", "RESEND_API_KEY"]);
        let postmark_server_token =
            first_env(&["LEDGERFLOW_POSTMARK_SERVER_TOKEN", "POSTMARK_SERVER_TOKEN"]);
        let email_from = first_env(&["LEDGERFLOW_EMAIL_FROM", "EMAIL_FROM"]);
        Self {
            bind_addr: std::env::var("LEDGERFLOW_WEB_BIND_ADDR")
                .unwrap_or_else(|_| "127.0.0.1:3001".to_owned()),
            api_base_url: std::env::var("LEDGERFLOW_API_BASE_URL")
                .unwrap_or_else(|_| "http://127.0.0.1:3000".to_owned()),
            indexer_api_base_url: std::env::var("LEDGERFLOW_INDEXER_API_BASE_URL").ok(),
            indexer_api_bearer_token: std::env::var("LEDGERFLOW_INDEXER_API_BEARER_TOKEN").ok(),
            database_url: std::env::var("LEDGERFLOW_DATABASE_URL").unwrap_or_else(|_| {
                "postgres://postgres:postgres@127.0.0.1:5532/ledgerflow".to_owned()
            }),
            default_chain_id: std::env::var("LEDGERFLOW_DEFAULT_CHAIN_ID")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(8453),
            web_base_url: public_base_url_from_env()
                .unwrap_or_else(|| "http://127.0.0.1:3001".to_owned()),
            session_cookie_name: std::env::var("LEDGERFLOW_SESSION_COOKIE_NAME")
                .unwrap_or_else(|_| "ledgerflow_session".to_owned()),
            resend_api_key,
            postmark_server_token,
            postmark_message_stream: std::env::var("LEDGERFLOW_POSTMARK_MESSAGE_STREAM")
                .unwrap_or_else(|_| "outbound".to_owned()),
            email_from,
            email_delivery_mode: EmailDeliveryMode::from_env(),
            verification_ttl_minutes: std::env::var("LEDGERFLOW_VERIFICATION_TTL_MINUTES")
                .ok()
                .and_then(|value| value.parse::<i64>().ok())
                .unwrap_or(15),
            session_ttl_days: std::env::var("LEDGERFLOW_SESSION_TTL_DAYS")
                .ok()
                .and_then(|value| value.parse::<i64>().ok())
                .unwrap_or(30),
            cryptohouse_url: std::env::var("LEDGERFLOW_TRACE_MONITOR_CRYPTOHOUSE_URL")
                .unwrap_or_else(|_| {
                    "https://crypto-clickhouse.clickhouse.com/?user=crypto".to_owned()
                }),
        }
    }
}

impl EmailDeliveryMode {
    fn from_env() -> Self {
        match std::env::var("LEDGERFLOW_EMAIL_DELIVERY_MODE")
            .unwrap_or_else(|_| "auto".to_owned())
            .trim()
            .to_ascii_lowercase()
            .as_str()
        {
            "log" => Self::Log,
            "postmark" => Self::Postmark,
            "resend" => Self::Resend,
            _ => Self::Auto,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Log => "log",
            Self::Postmark => "postmark",
            Self::Resend => "resend",
        }
    }
}

fn first_env(keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| {
        std::env::var(key)
            .ok()
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty())
    })
}

fn public_base_url_from_env() -> Option<String> {
    first_env(&["LEDGERFLOW_TRACE_DOMAIN", "LEDGERFLOW_WEB_BASE_URL"]).map(|value| {
        let value = value.trim().trim_end_matches('/').to_owned();
        if value.starts_with("http://") || value.starts_with("https://") {
            value
        } else {
            format!("https://{value}")
        }
    })
}

#[derive(Debug, Deserialize)]
struct AuthPageQuery {
    notice: Option<String>,
    mode: Option<String>,
}

#[derive(Debug, Deserialize)]
struct VerifyPageQuery {
    email: Option<String>,
    purpose: Option<String>,
    notice: Option<String>,
}

#[derive(Debug, Deserialize)]
struct DashboardQuery {
    tab: Option<String>,
    cluster_id: Option<String>,
    start: Option<String>,
    end: Option<String>,
    cluster_tab: Option<String>,
    tx_page: Option<String>,
    tx_page_size: Option<String>,
    tx_behavior: Option<String>,
    tx_protocol: Option<String>,
    tx_sort_by: Option<String>,
    tx_sort_dir: Option<String>,
    notice: Option<String>,
    created_key: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
struct IndexerLookupQuery {
    address: Option<String>,
    limit: Option<u32>,
    from_block: Option<u64>,
    to_block: Option<u64>,
    direction: Option<String>,
    notice: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ArbitragePageQuery {
    minutes: Option<i64>,
    limit: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct SwapBlockRangeQuery {
    from_block: u64,
    to_block: u64,
    min_swaps: Option<u32>,
    tx_limit: Option<u32>,
    pool_limit: Option<u32>,
}

#[derive(Debug, Clone)]
struct ArbitrageRecentRow {
    tx_hash: String,
    block_number: i64,
    block_timestamp: Option<DateTime<Utc>>,
    sender_address: Option<String>,
    entry_contract: Option<String>,
    swap_log_count: i32,
    distinct_pool_count: i32,
    route: String,
    confirmation_status: String,
    profit_token_address: Option<String>,
    profit_amount_raw: Option<String>,
}

#[derive(Debug, Clone)]
struct ArbitrageHotPoolRow {
    pool_address: String,
    tx_count: i64,
    swap_log_count: i64,
    unique_senders: i64,
    unique_routes: i64,
    last_seen: Option<DateTime<Utc>>,
    sample_tx_hashes: Vec<String>,
}

#[derive(Debug, Clone)]
struct ArbitrageHotRouteRow {
    route_fingerprint: String,
    route: String,
    tx_count: i64,
    swap_log_count: i64,
    unique_senders: i64,
    last_seen: Option<DateTime<Utc>>,
    sample_tx_hashes: Vec<String>,
}

#[derive(Debug, Deserialize, Clone)]
struct AddressAnalysisQuery {
    address: Option<String>,
    chain_id: Option<u64>,
}

#[derive(Debug, Deserialize, Clone)]
struct PageNoticeQuery {
    notice: Option<String>,
    created_key: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
struct ClustersPageQuery {
    notice: Option<String>,
    cluster_id: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
struct ClusterDetailQuery {
    start: Option<String>,
    end: Option<String>,
    cluster_tab: Option<String>,
    tx_page: Option<u32>,
    tx_page_size: Option<u32>,
    tx_behavior: Option<String>,
    tx_protocol: Option<String>,
    tx_sort_by: Option<String>,
    tx_sort_dir: Option<String>,
    notice: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RequestCodeForm {
    email: String,
    purpose: String,
}

#[derive(Debug, Deserialize)]
struct VerifyCodeForm {
    email: String,
    purpose: String,
    code: String,
}

#[derive(Debug, Deserialize)]
struct CreateClusterForm {
    chain_id: u64,
    owner_label: String,
    name: String,
    description: Option<String>,
    addresses: String,
}

#[derive(Debug, Deserialize)]
struct UpdateClusterForm {
    chain_id: u64,
    owner_label: String,
    name: String,
    description: Option<String>,
    addresses: String,
}

#[derive(Debug, Deserialize)]
struct RunTxForm {
    chain_id: u64,
    tx_hash: String,
    mode: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct ClusterAddressInput {
    address: String,
    label: Option<String>,
    role: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct CreateClusterRequest {
    chain_id: u64,
    owner_label: String,
    name: String,
    description: Option<String>,
    addresses: Vec<ClusterAddressInput>,
}

#[derive(Debug, Deserialize, Serialize)]
struct ClusterBackfillRequest {
    start_block: Option<u64>,
    end_block: Option<u64>,
    force_reanalyze: Option<bool>,
    query_limit: Option<u32>,
}

#[derive(Debug, Deserialize, Serialize)]
struct AnalyzeTxRequest {
    chain_id: u64,
    tx_hash: String,
}

#[derive(Debug, Deserialize)]
struct ShareTxForm {
    chain_id: u64,
    tx_hash: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct AnalysisEnvelope {
    analysis: TransactionAnalysis,
    evidence_pack: EvidencePack,
    #[serde(default)]
    address_intelligence: AddressIntelligenceEnvelope,
}

#[derive(Debug, Deserialize, Serialize, Default)]
struct AddressSummaryEnvelope {
    chain_id: u64,
    address: String,
    native_balance_raw: Option<String>,
    profile: Option<AddressProfile>,
    labels: Vec<AddressLabel>,
    analyzed_tx_count: u64,
    gas_spent_raw: String,
    behavior_rows: Vec<AddressBehaviorSummaryRow>,
    recent_txs: Vec<AddressRecentTxRow>,
    queued: bool,
}

#[derive(Debug, Deserialize, Serialize, Default)]
struct AddressBehaviorSummaryRow {
    behavior: String,
    tx_count: u64,
    gas_spent_raw: String,
}

#[derive(Debug, Deserialize, Serialize, Default)]
struct AddressRecentTxRow {
    tx_hash: String,
    block_timestamp: Option<String>,
    behavior: String,
    gas_raw: String,
}

#[derive(Debug, Deserialize, Serialize, Default)]
struct AddressIntelligenceEnvelope {
    profiles: Vec<AddressProfile>,
    labels: Vec<AddressLabel>,
}

#[derive(Debug)]
struct TxAnalysisShareRecord {
    share_id: Uuid,
    snapshot: serde_json::Value,
}

#[derive(Debug, Deserialize)]
struct IndexerProgressPayload {
    chain_id: u64,
    worker_id: String,
    last_indexed_block: u64,
    #[serde(default)]
    endpoint_backfill_pending_rows: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct IndexerAddressRefsPayload {
    address: String,
    refs: Vec<IndexerAddressRef>,
}

#[derive(Debug, Deserialize)]
struct IndexerAddressRef {
    chain_id: u64,
    tx_hash: String,
    block_number: u64,
    tx_index: u32,
    direction: AddressHitDirection,
}

#[derive(Debug, Deserialize, Clone)]
struct IndexerApiKeyPayload {
    key_id: String,
    label: String,
    token_prefix: String,
    status: String,
    created_at: String,
    last_used_at: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CreatedIndexerApiKeyPayload {
    api_key: String,
}

#[derive(Debug, Serialize)]
struct CreateIndexerApiKeyPayload {
    owner_user_id: String,
    owner_email: String,
    label: String,
}

#[derive(Debug, Serialize)]
struct RevokeIndexerApiKeyPayload {
    owner_user_id: String,
}

#[derive(Debug, Deserialize)]
struct CreateIndexerApiKeyForm {
    label: String,
}

#[derive(Debug, Deserialize)]
struct ClassifiedTransactionsPagePayload {
    items: Vec<ClassifiedTransaction>,
    total: u64,
    page: u32,
    page_size: u32,
}

#[derive(Debug, Deserialize, Serialize)]
struct ClusterStrategyAnalysisPayload {
    cluster_id: String,
    chain_id: u64,
    generated_at: String,
    window_start: Option<String>,
    window_end: Option<String>,
    tx_count: u32,
    arbitrage_tx_count: u32,
    successful_arbitrage_tx_count: u32,
    failed_arbitrage_tx_count: u32,
    arbitrage_success_rate: Option<f64>,
    total_estimated_pnl_usd: Option<f64>,
    total_gas_cost_usd: Option<f64>,
    top_routes: Vec<ClusterRouteStatPayload>,
    top_protocols: Vec<ClusterProtocolStatPayload>,
    address_roles: Vec<ClusterAddressRoleStatPayload>,
    strategy_breakdown: Vec<ClusterStrategyBreakdownPayload>,
    behavior_profile: ClusterBehaviorProfilePayload,
    performance: BotPerformanceSummaryPayload,
    arbitrage_report: Option<StrategyDetailReportPayload>,
    liquidation_report: Option<StrategyDetailReportPayload>,
    representative_transactions: Vec<RepresentativeTransactionPayload>,
}

#[derive(Debug, Deserialize, Serialize)]
struct ClusterBehaviorProfilePayload {
    primary_behavior: String,
    primary_behavior_tx_count: u32,
    primary_behavior_tx_share: Option<f64>,
    active_addresses_count: u32,
    behavior_breakdown: Vec<ClusterStrategyBreakdownPayload>,
}

#[derive(Debug, Deserialize, Serialize)]
struct BotPerformanceSummaryPayload {
    tx_count: u32,
    success_count: u32,
    failed_count: u32,
    success_rate: Option<f64>,
    estimated_pnl_usd: Option<f64>,
    gas_cost_usd: Option<f64>,
    avg_estimated_pnl_usd: Option<f64>,
    avg_gas_cost_usd: Option<f64>,
    pnl_to_gas_ratio: Option<f64>,
}

#[derive(Debug, Deserialize, Serialize)]
struct StrategyDetailReportPayload {
    strategy_type: String,
    tx_count: u32,
    success_count: u32,
    failed_count: u32,
    success_rate: Option<f64>,
    estimated_pnl_usd: Option<f64>,
    gas_cost_usd: Option<f64>,
    avg_estimated_pnl_usd: Option<f64>,
    avg_gas_cost_usd: Option<f64>,
    pnl_to_gas_ratio: Option<f64>,
    top_routes: Vec<ClusterRouteStatPayload>,
    top_protocols: Vec<StrategyProtocolStatPayload>,
    representative_transactions: Vec<RepresentativeTransactionPayload>,
}

#[derive(Debug, Deserialize, Serialize)]
struct StrategyProtocolStatPayload {
    protocol: String,
    tx_count: u32,
    success_count: u32,
    success_rate: Option<f64>,
    estimated_pnl_usd: Option<f64>,
    gas_cost_usd: Option<f64>,
    sample_tx_hashes: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct RepresentativeTransactionPayload {
    tx_hash: String,
    behavior_tag: String,
    strategy_subtype: String,
    protocol: Option<String>,
    protocol_family: Option<String>,
    address: String,
    block_number: u64,
    timestamp: Option<String>,
    estimated_pnl_usd: Option<f64>,
    gas_cost_usd: Option<f64>,
    route: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct ClusterRouteStatPayload {
    route: String,
    tx_count: u32,
    success_count: u32,
    success_rate: Option<f64>,
    estimated_pnl_usd: Option<f64>,
    gas_cost_usd: Option<f64>,
    sample_tx_hashes: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct ClusterProtocolStatPayload {
    protocol: String,
    tx_count: u32,
    arbitrage_tx_count: u32,
    success_count: u32,
    success_rate: Option<f64>,
    estimated_pnl_usd: Option<f64>,
    gas_cost_usd: Option<f64>,
    sample_tx_hashes: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct ClusterAddressRoleStatPayload {
    address: String,
    configured_role: Option<String>,
    inferred_role: String,
    tx_count: u32,
    arbitrage_tx_count: u32,
    success_count: u32,
    estimated_pnl_usd: Option<f64>,
    sample_tx_hashes: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct ClusterStrategyBreakdownPayload {
    strategy_type: String,
    tx_count: u32,
    success_count: u32,
    estimated_pnl_usd: Option<f64>,
}

#[derive(Debug, Serialize)]
struct ClusterLiveResponse {
    status_label: String,
    status_tone: String,
    tracking: Option<ClusterTrackingState>,
    coverage: Option<ClusterCoverage>,
    spend_summary: Option<ClusterSpendSummary>,
    asset_changes: Option<ClusterAssetChangeReport>,
    metrics: Vec<ClusterMetricsSnapshot>,
    alerts: Vec<ClusterAlert>,
    strategy_analysis: Option<ClusterStrategyAnalysisPayload>,
}

#[derive(Debug, Clone)]
struct ClusterTxTableState {
    page: u32,
    page_size: u32,
    behavior_tag: Option<String>,
    protocol: Option<String>,
    sort_by: String,
    sort_dir: String,
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
enum ClusterAnalysisTab {
    Overview,
    Metrics,
    Strategy,
    Summary,
    Activity,
}

impl ClusterAnalysisTab {
    fn from_query(value: Option<&str>) -> Self {
        match value {
            Some("metrics") => Self::Metrics,
            Some("strategy") => Self::Strategy,
            Some("summary") => Self::Summary,
            Some("activity") => Self::Activity,
            _ => Self::Overview,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Overview => "overview",
            Self::Metrics => "metrics",
            Self::Strategy => "strategy",
            Self::Summary => "summary",
            Self::Activity => "activity",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DashboardTab {
    Clusters,
    ClusterAnalysis,
    AddressAnalysis,
    TxAnalysis,
    ClusterGraph,
    IndexerProgress,
    IndexerLookup,
    IndexerApiKeys,
}

fn section_title(tab: &DashboardTab) -> &'static str {
    match tab {
        DashboardTab::Clusters => "Cluster management",
        DashboardTab::ClusterAnalysis => "Cluster analysis",
        DashboardTab::AddressAnalysis => "Address analysis",
        DashboardTab::TxAnalysis => "Single tx analysis",
        DashboardTab::ClusterGraph => "Cluster graph",
        DashboardTab::IndexerProgress => "Indexer progress",
        DashboardTab::IndexerLookup => "Indexer address lookup",
        DashboardTab::IndexerApiKeys => "Indexer API keys",
    }
}

impl DashboardTab {
    fn from_query(value: Option<&str>) -> Self {
        match value {
            Some("cluster-analysis") => Self::ClusterAnalysis,
            Some("address-analysis") => Self::AddressAnalysis,
            Some("tx-analysis") => Self::TxAnalysis,
            Some("cluster-graph") => Self::ClusterGraph,
            Some("indexer-progress") => Self::IndexerProgress,
            Some("indexer-lookup") => Self::IndexerLookup,
            Some("indexer-api-keys") => Self::IndexerApiKeys,
            _ => Self::Clusters,
        }
    }
}

pub async fn build_router(config: WebConfig) -> LedgerResult<Router> {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&config.database_url)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;

    let (sender, email_delivery_mode, email_delivery_ready): (
        Arc<dyn VerificationSender>,
        &'static str,
        bool,
    ) = match config.email_delivery_mode {
        EmailDeliveryMode::Log => (Arc::new(LogVerificationSender), "log", false),
        EmailDeliveryMode::Postmark => {
            let Some(server_token) = config.postmark_server_token.clone() else {
                return Err(LedgerError::AnalysisInvariant(
                    "email delivery mode is postmark but POSTMARK server token is missing"
                        .to_owned(),
                ));
            };
            let Some(from_email) = config.email_from.clone() else {
                return Err(LedgerError::AnalysisInvariant(
                    "email delivery mode is postmark but sender email is missing".to_owned(),
                ));
            };
            (
                Arc::new(PostmarkVerificationSender::new(
                    server_token,
                    from_email,
                    config.postmark_message_stream.clone(),
                )),
                "postmark",
                true,
            )
        }
        EmailDeliveryMode::Resend => {
            let Some(api_key) = config.resend_api_key.clone() else {
                return Err(LedgerError::AnalysisInvariant(
                    "email delivery mode is resend but RESEND API key is missing".to_owned(),
                ));
            };
            let Some(from_email) = config.email_from.clone() else {
                return Err(LedgerError::AnalysisInvariant(
                    "email delivery mode is resend but sender email is missing".to_owned(),
                ));
            };
            (
                Arc::new(ResendVerificationSender::new(api_key, from_email)),
                "resend",
                true,
            )
        }
        EmailDeliveryMode::Auto => {
            if let (Some(server_token), Some(from_email)) = (
                config.postmark_server_token.clone(),
                config.email_from.clone(),
            ) {
                (
                    Arc::new(PostmarkVerificationSender::new(
                        server_token,
                        from_email,
                        config.postmark_message_stream.clone(),
                    )),
                    "postmark",
                    true,
                )
            } else if let (Some(api_key), Some(from_email)) =
                (config.resend_api_key.clone(), config.email_from.clone())
            {
                (
                    Arc::new(ResendVerificationSender::new(api_key, from_email)),
                    "resend",
                    true,
                )
            } else {
                (Arc::new(LogVerificationSender), "log", false)
            }
        }
    };
    info!(
        requested_mode = %config.email_delivery_mode.label(),
        active_mode = %email_delivery_mode,
        ready = email_delivery_ready,
        "web email delivery configured"
    );

    let state = WebState {
        api_base_url: config.api_base_url.clone(),
        indexer_api_base_url: config.indexer_api_base_url.clone(),
        indexer_api_bearer_token: config.indexer_api_bearer_token.clone(),
        bind_addr: config.bind_addr.clone(),
        default_chain_id: config.default_chain_id,
        web_base_url: config.web_base_url.clone(),
        session_cookie_name: config.session_cookie_name.clone(),
        pool: pool.clone(),
        client: Client::new(),
        cryptohouse_url: config.cryptohouse_url.clone(),
        auth_service: Arc::new(AuthService::new(
            pool.clone(),
            sender,
            config.web_base_url,
            config.session_ttl_days,
            config.verification_ttl_minutes,
        )),
        portal_service: Arc::new(PortalService::new(pool)),
        email_delivery_mode,
        email_delivery_ready,
    };

    Ok(Router::new()
        .route("/assets/ledgerflow-mark.svg", get(brand_mark_svg))
        .route("/", get(homepage))
        .route("/pricing", get(pricing))
        .route("/auth", get(auth_page))
        .route("/auth/request-code", post(request_code))
        .route("/verify", get(verify_page))
        .route("/auth/verify", post(verify_code))
        .route("/auth/logout", post(logout))
        .route("/app", get(app_home))
        .route("/app/clusters", get(clusters_page))
        .route("/app/clusters/status", get(cluster_status))
        .route("/app/clusters/{cluster_id}", get(cluster_analysis_page))
        .route("/app/address-analysis", get(address_analysis_page))
        .route("/app/clusters/create", post(create_cluster))
        .route("/app/clusters/{cluster_id}/update", post(update_cluster))
        .route("/app/clusters/{cluster_id}/delete", post(delete_cluster))
        .route(
            "/app/clusters/{cluster_id}/backfill",
            post(queue_cluster_backfill),
        )
        .route(
            "/app/clusters/{cluster_id}/tracking/enable",
            post(enable_cluster_tracking),
        )
        .route(
            "/app/clusters/{cluster_id}/tracking/pause",
            post(pause_cluster_tracking),
        )
        .route(
            "/app/clusters/{cluster_id}/alerts/{alert_id}/read",
            post(mark_cluster_alert_read),
        )
        .route("/app/tx-analysis", get(tx_analysis_page))
        .route("/app/tx-analysis/run", post(run_tx_analysis))
        .route("/app/tx-analysis/share", post(create_tx_analysis_share))
        .route("/share/tx/{share_id}", get(public_tx_analysis_share))
        .route("/app/arbitrage", get(arbitrage_page))
        .route("/api/swaps/block-range", get(swaps_block_range_api))
        .route("/app/cluster-graph", get(cluster_graph_page))
        .route("/app/indexer/progress", get(indexer_progress_page))
        .route("/app/indexer/address-lookup", get(indexer_lookup_page))
        .route("/app/indexer/api-keys", get(indexer_api_keys_page))
        .route("/app/indexer/api-keys/create", post(create_indexer_api_key))
        .route(
            "/app/indexer/api-keys/{key_id}/revoke",
            post(revoke_indexer_api_key),
        )
        .route("/dashboard", get(dashboard))
        .route("/dashboard/cluster-status", get(cluster_status))
        .route("/dashboard/clusters/create", post(create_cluster))
        .route("/dashboard/tx/run", post(run_tx_analysis))
        .route("/health", get(health))
        .with_state(state))
}

async fn health(State(state): State<WebState>) -> Html<String> {
    Html(format!(
        "{{\"status\":\"ok\",\"web_bind_addr\":\"{}\",\"api_base_url\":\"{}\",\"email_delivery_mode\":\"{}\",\"email_delivery_ready\":{}}}",
        state.bind_addr, state.api_base_url, state.email_delivery_mode, state.email_delivery_ready
    ))
}

async fn brand_mark_svg() -> Response {
    (
        [(header::CONTENT_TYPE, "image/svg+xml; charset=utf-8")],
        brand_mark_svg_source(),
    )
        .into_response()
}

async fn homepage(
    State(state): State<WebState>,
    headers: HeaderMap,
) -> Result<Html<String>, WebError> {
    let user = current_user(&state, &headers).await?;
    Ok(Html(render_marketing_page(
        "LedgerFlow",
        user.as_ref(),
        render_homepage_content(user.as_ref()),
    )))
}

async fn pricing(
    State(state): State<WebState>,
    headers: HeaderMap,
) -> Result<Html<String>, WebError> {
    let user = current_user(&state, &headers).await?;
    Ok(Html(render_marketing_page(
        "Pricing",
        user.as_ref(),
        render_pricing_content(),
    )))
}

async fn auth_page(
    State(state): State<WebState>,
    headers: HeaderMap,
    Query(query): Query<AuthPageQuery>,
) -> Result<Response, WebError> {
    if current_user(&state, &headers).await?.is_some() {
        return Ok(Redirect::to("/app/clusters").into_response());
    }
    let notice = query.notice.as_deref().and_then(render_notice_message);
    let mode = query.mode.as_deref().unwrap_or("signup");
    Ok(Html(render_marketing_page(
        "Sign in",
        None,
        render_auth_content(
            notice,
            None,
            mode,
            state.email_delivery_mode,
            state.email_delivery_ready,
        ),
    ))
    .into_response())
}

async fn request_code(
    State(state): State<WebState>,
    headers: HeaderMap,
    Form(form): Form<RequestCodeForm>,
) -> Result<Response, WebError> {
    let purpose = parse_purpose(&form.purpose)?;
    let user_agent = user_agent(&headers);
    match state
        .auth_service
        .request_code(&form.email, purpose.clone(), None, user_agent.as_deref())
        .await
    {
        Ok(email) => Ok(redirect_response(&format!(
            "/verify?email={}&purpose={}&notice=code-sent",
            urlencoding::encode(&email),
            verification_purpose_to_str(&purpose)
        ))),
        Err(err) => Ok(Html(render_marketing_page(
            "Sign in",
            None,
            render_auth_content(
                None,
                Some(err.to_string()),
                verification_purpose_to_str(&purpose),
                state.email_delivery_mode,
                state.email_delivery_ready,
            ),
        ))
        .into_response()),
    }
}

async fn verify_page(
    State(state): State<WebState>,
    headers: HeaderMap,
    Query(query): Query<VerifyPageQuery>,
) -> Result<Response, WebError> {
    if current_user(&state, &headers).await?.is_some() {
        return Ok(Redirect::to("/app/clusters").into_response());
    }
    let notice = query.notice.as_deref().and_then(render_notice_message);
    Ok(Html(render_marketing_page(
        "Verify email",
        None,
        render_verify_content(
            query.email.as_deref().unwrap_or_default(),
            query.purpose.as_deref().unwrap_or("signup"),
            notice,
            None,
        ),
    ))
    .into_response())
}

async fn verify_code(
    State(state): State<WebState>,
    headers: HeaderMap,
    Form(form): Form<VerifyCodeForm>,
) -> Result<Response, WebError> {
    let purpose = parse_purpose(&form.purpose)?;
    let user_agent = user_agent(&headers);
    match state
        .auth_service
        .verify_code(&form.email, purpose, &form.code, user_agent.as_deref())
        .await
    {
        Ok((_user, session_token)) => {
            let mut response = Redirect::to("/app/clusters?notice=signed-in").into_response();
            response.headers_mut().insert(
                header::SET_COOKIE,
                HeaderValue::from_str(&session_cookie(
                    &state.session_cookie_name,
                    &session_token,
                    60 * 60 * 24 * 30,
                ))
                .map_err(|err| WebError::internal(err.to_string()))?,
            );
            Ok(response)
        }
        Err(err) => Ok(Html(render_marketing_page(
            "Verify email",
            None,
            render_verify_content(&form.email, &form.purpose, None, Some(err.to_string())),
        ))
        .into_response()),
    }
}

async fn logout(State(state): State<WebState>, headers: HeaderMap) -> Result<Response, WebError> {
    if let Some(token) = session_token_from_headers(&headers, &state.session_cookie_name) {
        let _ = state.auth_service.revoke_session(&token).await;
    }
    let mut response = Redirect::to("/auth?notice=logged-out").into_response();
    response.headers_mut().insert(
        header::SET_COOKIE,
        HeaderValue::from_str(&clear_session_cookie(&state.session_cookie_name))
            .map_err(|err| WebError::internal(err.to_string()))?,
    );
    Ok(response)
}

async fn dashboard(Query(query): Query<DashboardQuery>) -> Result<Response, WebError> {
    let location = match DashboardTab::from_query(query.tab.as_deref()) {
        DashboardTab::Clusters => "/app/clusters".to_owned(),
        DashboardTab::ClusterAnalysis => {
            if let Some(cluster_id) = query.cluster_id.as_deref() {
                let mut params = Vec::new();
                if let Some(start) = query.start.as_deref() {
                    params.push(format!("start={}", urlencoding::encode(start)));
                }
                if let Some(end) = query.end.as_deref() {
                    params.push(format!("end={}", urlencoding::encode(end)));
                }
                if let Some(notice) = query.notice.as_deref() {
                    params.push(format!("notice={}", urlencoding::encode(notice)));
                }
                if params.is_empty() {
                    format!("/app/clusters/{cluster_id}")
                } else {
                    format!("/app/clusters/{cluster_id}?{}", params.join("&"))
                }
            } else {
                "/app/clusters".to_owned()
            }
        }
        DashboardTab::AddressAnalysis => "/app/address-analysis".to_owned(),
        DashboardTab::TxAnalysis => "/app/tx-analysis".to_owned(),
        DashboardTab::ClusterGraph => "/app/cluster-graph".to_owned(),
        DashboardTab::IndexerProgress => "/app/indexer/progress".to_owned(),
        DashboardTab::IndexerLookup => "/app/indexer/address-lookup".to_owned(),
        DashboardTab::IndexerApiKeys => "/app/indexer/api-keys".to_owned(),
    };
    Ok(Redirect::to(&location).into_response())
}

async fn app_home() -> Response {
    Redirect::to("/app/clusters").into_response()
}

async fn clusters_page(
    State(state): State<WebState>,
    headers: HeaderMap,
    Query(query): Query<ClustersPageQuery>,
) -> Result<Response, WebError> {
    let Some(user) = current_user(&state, &headers).await? else {
        return Ok(Redirect::to("/auth?notice=auth-required").into_response());
    };
    if let Some(cluster_id) = query.cluster_id.as_deref() {
        ensure_user_owns_cluster(&state, user.user_id, cluster_id).await?;
    }
    let editable_cluster = load_editable_cluster(&state, query.cluster_id.as_deref()).await?;
    render_dashboard_response(
        &state,
        user,
        DashboardQuery {
            tab: Some("clusters".to_owned()),
            cluster_id: query.cluster_id.clone(),
            start: None,
            end: None,
            cluster_tab: None,
            tx_page: None,
            tx_page_size: None,
            tx_behavior: None,
            tx_protocol: None,
            tx_sort_by: None,
            tx_sort_dir: None,
            notice: query.notice,
            created_key: None,
        },
        editable_cluster.as_ref(),
        None,
        None,
        None,
        None,
        None,
    )
    .await
}

async fn cluster_analysis_page(
    State(state): State<WebState>,
    headers: HeaderMap,
    Path(cluster_id): Path<String>,
    Query(query): Query<ClusterDetailQuery>,
) -> Result<Response, WebError> {
    let Some(user) = current_user(&state, &headers).await? else {
        return Ok(Redirect::to("/auth?notice=auth-required").into_response());
    };
    render_dashboard_response(
        &state,
        user,
        DashboardQuery {
            tab: Some("cluster-analysis".to_owned()),
            cluster_id: Some(cluster_id),
            start: query.start,
            end: query.end,
            cluster_tab: query.cluster_tab,
            tx_page: query.tx_page.map(|value| value.to_string()),
            tx_page_size: query.tx_page_size.map(|value| value.to_string()),
            tx_behavior: query.tx_behavior,
            tx_protocol: query.tx_protocol,
            tx_sort_by: query.tx_sort_by,
            tx_sort_dir: query.tx_sort_dir,
            notice: query.notice,
            created_key: None,
        },
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .await
}

async fn address_analysis_page(
    State(state): State<WebState>,
    headers: HeaderMap,
    Query(query): Query<AddressAnalysisQuery>,
) -> Result<Response, WebError> {
    let Some(user) = current_user(&state, &headers).await? else {
        return Ok(Redirect::to("/auth?notice=auth-required").into_response());
    };
    let (payload, error) = if let Some(address) = query
        .address
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        let chain_id = query.chain_id.unwrap_or(state.default_chain_id);
        let path = format!(
            "/addresses/summary?chain_id={}&address={}&lite=true",
            chain_id,
            urlencoding::encode(address)
        );
        match api_get_json::<AddressSummaryEnvelope>(&state, &path).await {
            Ok(payload) => (Some(payload), None),
            Err(err) => (None, Some(err.to_string())),
        }
    } else {
        (None, None)
    };
    let dashboard_query = DashboardQuery {
        tab: Some("address-analysis".to_owned()),
        cluster_id: None,
        start: None,
        end: None,
        cluster_tab: None,
        tx_page: None,
        tx_page_size: None,
        tx_behavior: None,
        tx_protocol: None,
        tx_sort_by: None,
        tx_sort_dir: None,
        notice: None,
        created_key: None,
    };
    render_dashboard_response(
        &state,
        user,
        dashboard_query,
        None,
        None,
        error,
        None,
        None,
        payload,
    )
    .await
}

async fn tx_analysis_page(
    State(state): State<WebState>,
    headers: HeaderMap,
    Query(query): Query<PageNoticeQuery>,
) -> Result<Response, WebError> {
    let Some(user) = current_user(&state, &headers).await? else {
        return Ok(Redirect::to("/auth?notice=auth-required").into_response());
    };
    render_dashboard_response(
        &state,
        user,
        DashboardQuery {
            tab: Some("tx-analysis".to_owned()),
            cluster_id: None,
            start: None,
            end: None,
            cluster_tab: None,
            tx_page: None,
            tx_page_size: None,
            tx_behavior: None,
            tx_protocol: None,
            tx_sort_by: None,
            tx_sort_dir: None,
            notice: query.notice,
            created_key: query.created_key,
        },
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .await
}

async fn arbitrage_page(
    State(state): State<WebState>,
    headers: HeaderMap,
    Query(query): Query<ArbitragePageQuery>,
) -> Result<Response, WebError> {
    let Some(user) = current_user(&state, &headers).await? else {
        return Ok(Redirect::to("/auth?notice=auth-required").into_response());
    };
    let minutes = query.minutes.unwrap_or(60).clamp(1, 24 * 60);
    let limit = query.limit.unwrap_or(50).clamp(10, 200);
    let recent = load_arbitrage_recent(&state, minutes, limit).await?;
    let hot_pools = load_arbitrage_hot_pools(&state, minutes, 25).await?;
    let hot_routes = load_arbitrage_hot_routes(&state, minutes, 25).await?;
    Ok(Html(render_arbitrage_page(
        &state,
        &user,
        minutes,
        limit,
        &recent,
        &hot_pools,
        &hot_routes,
    ))
    .into_response())
}

async fn swaps_block_range_api(
    State(state): State<WebState>,
    headers: HeaderMap,
    Query(query): Query<SwapBlockRangeQuery>,
) -> Result<Response, WebError> {
    let Some(_user) = current_user(&state, &headers).await? else {
        return Ok((
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "auth required"})),
        )
            .into_response());
    };
    if query.from_block > query.to_block {
        return Ok((
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "from_block must be <= to_block"})),
        )
            .into_response());
    }
    let block_count = query
        .to_block
        .saturating_sub(query.from_block)
        .saturating_add(1);
    if block_count > SWAP_BLOCK_RANGE_MAX_BLOCKS {
        return Ok((
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": format!("block range too large; max {SWAP_BLOCK_RANGE_MAX_BLOCKS} blocks")
            })),
        )
            .into_response());
    }
    let min_swaps = query.min_swaps.unwrap_or(1).clamp(1, 100);
    let tx_limit = query.tx_limit.unwrap_or(100).clamp(1, 500);
    let pool_limit = query.pool_limit.unwrap_or(50).clamp(1, 200);
    let summary_query = cryptohouse_swaps_summary_query(query.from_block, query.to_block);
    let txs_query =
        cryptohouse_swap_txs_query(query.from_block, query.to_block, min_swaps, tx_limit);
    let pools_query =
        cryptohouse_hot_swap_pools_query(query.from_block, query.to_block, pool_limit);
    let summary = cryptohouse_json_each_row(&state, &summary_query).await?;
    let swap_txs = cryptohouse_json_each_row(&state, &txs_query).await?;
    let hot_pools = cryptohouse_json_each_row(&state, &pools_query).await?;
    Ok(Json(json!({
        "from_block": query.from_block,
        "to_block": query.to_block,
        "block_count": block_count,
        "min_swaps": min_swaps,
        "tx_limit": tx_limit,
        "pool_limit": pool_limit,
        "summary": summary.first().cloned().unwrap_or_else(|| json!({})),
        "swap_txs": swap_txs,
        "hot_pools": hot_pools,
    }))
    .into_response())
}

async fn cryptohouse_json_each_row(
    state: &WebState,
    sql: &str,
) -> Result<Vec<serde_json::Value>, WebError> {
    let response = state
        .client
        .post(&state.cryptohouse_url)
        .body(sql.to_owned())
        .send()
        .await
        .map_err(WebError::upstream)?;
    let status = response.status();
    let body = response.text().await.map_err(WebError::upstream)?;
    if !status.is_success() {
        return Err(WebError::bad_gateway(format!(
            "cryptohouse query failed: {status} {}",
            truncate_for_display(&body, 300)
        )));
    }
    body.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_str(line).map_err(|err| WebError::internal(err.to_string())))
        .collect()
}

fn cryptohouse_swaps_summary_query(from_block: u64, to_block: u64) -> String {
    format!(
        r#"
SELECT
  count() AS swap_log_count,
  uniqExact(transaction_hash) AS swap_tx_count,
  uniqExact(address) AS pool_count,
  min(block_timestamp) AS first_seen,
  max(block_timestamp) AS last_seen
FROM base.logs
WHERE block_number BETWEEN {from_block} AND {to_block}
  AND (
    position(topics, '{UNISWAP_V2_SWAP_TOPIC_HEX}') > 0
    OR position(topics, '{UNISWAP_V3_SWAP_TOPIC_HEX}') > 0
  )
FORMAT JSONEachRow
"#
    )
}

fn cryptohouse_swap_txs_query(
    from_block: u64,
    to_block: u64,
    min_swaps: u32,
    limit: u32,
) -> String {
    format!(
        r#"
SELECT
  l.block_number,
  l.transaction_hash,
  count() AS swap_log_count,
  countIf(position(l.topics, '{UNISWAP_V2_SWAP_TOPIC_HEX}') > 0) AS v2_swap_count,
  countIf(position(l.topics, '{UNISWAP_V3_SWAP_TOPIC_HEX}') > 0) AS v3_swap_count,
  uniqExact(l.address) AS distinct_pool_count,
  groupUniqArray(64)(l.address) AS pool_addresses,
  groupArray(128)(l.log_index) AS log_indexes,
  any(t.from_address) AS sender_address,
  any(t.to_address) AS entry_contract,
  toString(max(l.block_timestamp)) AS block_timestamp
FROM base.logs AS l
LEFT JOIN base.transactions AS t
  ON t.block_number = l.block_number
 AND t.hash = l.transaction_hash
WHERE l.block_number BETWEEN {from_block} AND {to_block}
  AND (
    position(l.topics, '{UNISWAP_V2_SWAP_TOPIC_HEX}') > 0
    OR position(l.topics, '{UNISWAP_V3_SWAP_TOPIC_HEX}') > 0
  )
GROUP BY l.block_number, l.transaction_hash
HAVING swap_log_count >= {min_swaps}
ORDER BY swap_log_count DESC, l.block_number DESC
LIMIT {limit}
FORMAT JSONEachRow
"#
    )
}

fn cryptohouse_hot_swap_pools_query(from_block: u64, to_block: u64, limit: u32) -> String {
    format!(
        r#"
SELECT
  address AS pool_address,
  count() AS swap_log_count,
  uniqExact(transaction_hash) AS tx_count,
  uniqExact(block_number) AS block_count,
  min(block_number) AS first_block,
  max(block_number) AS last_block,
  min(block_timestamp) AS first_seen,
  max(block_timestamp) AS last_seen,
  groupUniqArray(8)(transaction_hash) AS sample_tx_hashes
FROM base.logs
WHERE block_number BETWEEN {from_block} AND {to_block}
  AND (
    position(topics, '{UNISWAP_V2_SWAP_TOPIC_HEX}') > 0
    OR position(topics, '{UNISWAP_V3_SWAP_TOPIC_HEX}') > 0
  )
GROUP BY address
ORDER BY tx_count DESC, swap_log_count DESC, last_block DESC
LIMIT {limit}
FORMAT JSONEachRow
"#
    )
}

async fn load_arbitrage_recent(
    state: &WebState,
    minutes: i64,
    limit: i64,
) -> Result<Vec<ArbitrageRecentRow>, WebError> {
    let rows = sqlx::query(
        r#"
        select tx_hash, block_number, block_timestamp, sender_address, entry_contract,
               swap_log_count, distinct_pool_count, route,
               confirmation_status, profit_token_address, profit_amount_raw
        from arbitrage_candidates
        where chain_id = $1
          and block_timestamp >= now() - ($2::text || ' minutes')::interval
        order by block_timestamp desc nulls last, block_number desc, score desc
        limit $3
        "#,
    )
    .bind(state.default_chain_id as i64)
    .bind(minutes)
    .bind(limit)
    .fetch_all(&state.pool)
    .await
    .map_err(|err| WebError::internal(err.to_string()))?;
    Ok(rows
        .into_iter()
        .map(|row| ArbitrageRecentRow {
            tx_hash: row.get("tx_hash"),
            block_number: row.get("block_number"),
            block_timestamp: row.get("block_timestamp"),
            sender_address: row.get("sender_address"),
            entry_contract: row.get("entry_contract"),
            swap_log_count: row.get("swap_log_count"),
            distinct_pool_count: row.get("distinct_pool_count"),
            route: row.get("route"),
            confirmation_status: row.get("confirmation_status"),
            profit_token_address: row.get("profit_token_address"),
            profit_amount_raw: row.get("profit_amount_raw"),
        })
        .collect())
}

async fn load_arbitrage_hot_pools(
    state: &WebState,
    minutes: i64,
    limit: i64,
) -> Result<Vec<ArbitrageHotPoolRow>, WebError> {
    let rows = sqlx::query(
        r#"
        select
          p.pool_address,
          count(distinct p.tx_hash)::bigint as tx_count,
          coalesce(sum(p.swap_log_count), 0)::bigint as swap_log_count,
          count(distinct c.sender_address)::bigint as unique_senders,
          count(distinct c.route_fingerprint)::bigint as unique_routes,
          max(p.block_timestamp) as last_seen,
          coalesce(jsonb_agg(distinct p.tx_hash) filter (where p.tx_hash is not null), '[]'::jsonb) as sample_tx_hashes
        from arbitrage_candidate_pools p
        join arbitrage_candidates c on c.chain_id = p.chain_id and c.tx_hash = p.tx_hash
        where p.chain_id = $1
          and p.block_timestamp >= now() - ($2::text || ' minutes')::interval
        group by p.pool_address
        order by tx_count desc, swap_log_count desc, last_seen desc
        limit $3
        "#,
    )
    .bind(state.default_chain_id as i64)
    .bind(minutes)
    .bind(limit)
    .fetch_all(&state.pool)
    .await
    .map_err(|err| WebError::internal(err.to_string()))?;
    Ok(rows
        .into_iter()
        .map(|row| ArbitrageHotPoolRow {
            pool_address: row.get("pool_address"),
            tx_count: row.get("tx_count"),
            swap_log_count: row.get("swap_log_count"),
            unique_senders: row.get("unique_senders"),
            unique_routes: row.get("unique_routes"),
            last_seen: row.get("last_seen"),
            sample_tx_hashes: json_value_strings(
                row.get::<Option<serde_json::Value>, _>("sample_tx_hashes"),
            ),
        })
        .collect())
}

async fn load_arbitrage_hot_routes(
    state: &WebState,
    minutes: i64,
    limit: i64,
) -> Result<Vec<ArbitrageHotRouteRow>, WebError> {
    let rows = sqlx::query(
        r#"
        select route_fingerprint, route,
               count(*)::bigint as tx_count,
               coalesce(sum(swap_log_count), 0)::bigint as swap_log_count,
               count(distinct sender_address)::bigint as unique_senders,
               max(block_timestamp) as last_seen,
               coalesce(jsonb_agg(tx_hash order by block_timestamp desc) filter (where tx_hash is not null), '[]'::jsonb) as sample_tx_hashes
        from arbitrage_candidates
        where chain_id = $1
          and block_timestamp >= now() - ($2::text || ' minutes')::interval
          and route_fingerprint <> ''
        group by route_fingerprint, route
        order by tx_count desc, swap_log_count desc, last_seen desc
        limit $3
        "#,
    )
    .bind(state.default_chain_id as i64)
    .bind(minutes)
    .bind(limit)
    .fetch_all(&state.pool)
    .await
    .map_err(|err| WebError::internal(err.to_string()))?;
    Ok(rows
        .into_iter()
        .map(|row| ArbitrageHotRouteRow {
            route_fingerprint: row.get("route_fingerprint"),
            route: row.get("route"),
            tx_count: row.get("tx_count"),
            swap_log_count: row.get("swap_log_count"),
            unique_senders: row.get("unique_senders"),
            last_seen: row.get("last_seen"),
            sample_tx_hashes: json_value_strings(
                row.get::<Option<serde_json::Value>, _>("sample_tx_hashes"),
            ),
        })
        .collect())
}

async fn cluster_graph_page(
    State(state): State<WebState>,
    headers: HeaderMap,
    Query(query): Query<PageNoticeQuery>,
) -> Result<Response, WebError> {
    let Some(user) = current_user(&state, &headers).await? else {
        return Ok(Redirect::to("/auth?notice=auth-required").into_response());
    };
    render_dashboard_response(
        &state,
        user,
        DashboardQuery {
            tab: Some("cluster-graph".to_owned()),
            cluster_id: None,
            start: None,
            end: None,
            cluster_tab: None,
            tx_page: None,
            tx_page_size: None,
            tx_behavior: None,
            tx_protocol: None,
            tx_sort_by: None,
            tx_sort_dir: None,
            notice: query.notice,
            created_key: query.created_key,
        },
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .await
}

async fn indexer_progress_page(
    State(state): State<WebState>,
    headers: HeaderMap,
    Query(query): Query<PageNoticeQuery>,
) -> Result<Response, WebError> {
    let Some(user) = current_user(&state, &headers).await? else {
        return Ok(Redirect::to("/auth?notice=auth-required").into_response());
    };
    render_dashboard_response(
        &state,
        user,
        DashboardQuery {
            tab: Some("indexer-progress".to_owned()),
            cluster_id: None,
            start: None,
            end: None,
            cluster_tab: None,
            tx_page: None,
            tx_page_size: None,
            tx_behavior: None,
            tx_protocol: None,
            tx_sort_by: None,
            tx_sort_dir: None,
            notice: query.notice,
            created_key: query.created_key,
        },
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .await
}

async fn indexer_lookup_page(
    State(state): State<WebState>,
    headers: HeaderMap,
    Query(query): Query<IndexerLookupQuery>,
) -> Result<Response, WebError> {
    let Some(user) = current_user(&state, &headers).await? else {
        return Ok(Redirect::to("/auth?notice=auth-required").into_response());
    };
    let path = build_indexer_lookup_path(&query)?;
    let (payload, error) = if let Some(path) = path.as_deref() {
        match indexer_get_json::<IndexerAddressRefsPayload>(&state, path).await {
            Ok(payload) => (Some(payload), None),
            Err(err) => (None, Some(err.to_string())),
        }
    } else {
        (None, None)
    };
    render_dashboard_response(
        &state,
        user,
        DashboardQuery {
            tab: Some("indexer-lookup".to_owned()),
            cluster_id: None,
            start: None,
            end: None,
            cluster_tab: None,
            tx_page: None,
            tx_page_size: None,
            tx_behavior: None,
            tx_protocol: None,
            tx_sort_by: None,
            tx_sort_dir: None,
            notice: query.notice.clone(),
            created_key: None,
        },
        None,
        None,
        error,
        payload,
        Some(query),
        None,
    )
    .await
}

async fn indexer_api_keys_page(
    State(state): State<WebState>,
    headers: HeaderMap,
    Query(query): Query<PageNoticeQuery>,
) -> Result<Response, WebError> {
    let Some(user) = current_user(&state, &headers).await? else {
        return Ok(Redirect::to("/auth?notice=auth-required").into_response());
    };
    render_dashboard_response(
        &state,
        user,
        DashboardQuery {
            tab: Some("indexer-api-keys".to_owned()),
            cluster_id: None,
            start: None,
            end: None,
            cluster_tab: None,
            tx_page: None,
            tx_page_size: None,
            tx_behavior: None,
            tx_protocol: None,
            tx_sort_by: None,
            tx_sort_dir: None,
            notice: query.notice,
            created_key: query.created_key,
        },
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .await
}

async fn create_indexer_api_key(
    State(state): State<WebState>,
    headers: HeaderMap,
    Form(form): Form<CreateIndexerApiKeyForm>,
) -> Result<Response, WebError> {
    let Some(user) = current_user(&state, &headers).await? else {
        return Ok(Redirect::to("/auth?notice=auth-required").into_response());
    };
    let label = form.label.trim();
    if label.is_empty() {
        return Ok(
            Redirect::to("/app/indexer/api-keys?notice=indexer-key-label-required").into_response(),
        );
    }
    let created: CreatedIndexerApiKeyPayload = indexer_post_json(
        &state,
        "/admin/api-keys",
        &CreateIndexerApiKeyPayload {
            owner_user_id: user.user_id.to_string(),
            owner_email: user.email.clone(),
            label: label.to_owned(),
        },
    )
    .await?;
    Ok(Redirect::to(&format!(
        "/app/indexer/api-keys?notice=indexer-key-created&created_key={}",
        urlencoding::encode(&created.api_key)
    ))
    .into_response())
}

async fn revoke_indexer_api_key(
    State(state): State<WebState>,
    headers: HeaderMap,
    Path(key_id): Path<String>,
) -> Result<Response, WebError> {
    let Some(user) = current_user(&state, &headers).await? else {
        return Ok(Redirect::to("/auth?notice=auth-required").into_response());
    };
    let _: IndexerApiKeyPayload = indexer_post_json(
        &state,
        &format!("/admin/api-keys/{}/revoke", urlencoding::encode(&key_id)),
        &RevokeIndexerApiKeyPayload {
            owner_user_id: user.user_id.to_string(),
        },
    )
    .await?;
    Ok(Redirect::to("/app/indexer/api-keys?notice=indexer-key-revoked").into_response())
}

async fn cluster_status(
    State(state): State<WebState>,
    headers: HeaderMap,
    Query(query): Query<DashboardQuery>,
) -> Result<Response, WebError> {
    let Some(user) = current_user(&state, &headers).await? else {
        return Ok(Redirect::to("/auth?notice=auth-required").into_response());
    };
    let clusters = state
        .portal_service
        .list_clusters_for_user(user.user_id)
        .await?;
    let Some(cluster_id_raw) = query.cluster_id.as_deref() else {
        return Err(WebError::bad_request("cluster_id is required".to_owned()));
    };
    let cluster_id = Uuid::parse_str(cluster_id_raw)
        .map_err(|err| WebError::bad_request(format!("invalid cluster id: {err}")))?;
    if !clusters
        .iter()
        .any(|cluster| cluster.cluster_id == cluster_id)
    {
        return Err(WebError::bad_request(
            "cluster is not available for the current user".to_owned(),
        ));
    }
    let payload = load_cluster_live_response(
        &state,
        cluster_id,
        query.start.as_deref(),
        query.end.as_deref(),
        &ClusterAnalysisTab::from_query(query.cluster_tab.as_deref()),
    )
    .await?;
    Ok(Json(payload).into_response())
}

async fn create_cluster(
    State(state): State<WebState>,
    headers: HeaderMap,
    Form(form): Form<CreateClusterForm>,
) -> Result<Response, WebError> {
    let Some(user) = current_user(&state, &headers).await? else {
        return Ok(Redirect::to("/auth?notice=auth-required").into_response());
    };
    let addresses = parse_cluster_addresses(&form.addresses)?;
    let request = CreateClusterRequest {
        chain_id: form.chain_id,
        owner_label: form.owner_label,
        name: form.name,
        description: form.description.filter(|value| !value.trim().is_empty()),
        addresses,
    };
    match api_post_json::<_, AddressCluster>(&state, "/clusters", &request).await {
        Ok(cluster) => {
            state
                .portal_service
                .attach_cluster_to_user(user.user_id, cluster.cluster_id)
                .await?;
            Ok(redirect_response(&format!(
                "/app/clusters/{}?notice=cluster-created",
                cluster.cluster_id
            )))
        }
        Err(err) => {
            let query = DashboardQuery {
                tab: Some("clusters".to_owned()),
                cluster_id: None,
                start: None,
                end: None,
                cluster_tab: None,
                tx_page: None,
                tx_page_size: None,
                tx_behavior: None,
                tx_protocol: None,
                tx_sort_by: None,
                tx_sort_dir: None,
                notice: None,
                created_key: None,
            };
            render_dashboard_response(
                &state,
                user,
                query,
                None,
                None,
                Some(err.to_string()),
                None,
                None,
                None,
            )
            .await
        }
    }
}

async fn update_cluster(
    State(state): State<WebState>,
    headers: HeaderMap,
    Path(cluster_id): Path<String>,
    Form(form): Form<UpdateClusterForm>,
) -> Result<Response, WebError> {
    let Some(user) = current_user(&state, &headers).await? else {
        return Ok(Redirect::to("/auth?notice=auth-required").into_response());
    };
    ensure_user_owns_cluster(&state, user.user_id, &cluster_id).await?;
    let addresses = parse_cluster_addresses(&form.addresses)?;
    let request = CreateClusterRequest {
        chain_id: form.chain_id,
        owner_label: form.owner_label,
        name: form.name,
        description: form.description.filter(|value| !value.trim().is_empty()),
        addresses,
    };
    match api_post_json::<_, AddressCluster>(
        &state,
        &format!("/clusters/{cluster_id}/update"),
        &request,
    )
    .await
    {
        Ok(cluster) => Ok(redirect_response(&format!(
            "/app/clusters?cluster_id={}&notice=cluster-updated-needs-backfill",
            cluster.cluster_id
        ))),
        Err(err) => {
            let editable_cluster = load_editable_cluster(&state, Some(&cluster_id)).await?;
            render_dashboard_response(
                &state,
                user,
                DashboardQuery {
                    tab: Some("clusters".to_owned()),
                    cluster_id: Some(cluster_id),
                    start: None,
                    end: None,
                    cluster_tab: None,
                    tx_page: None,
                    tx_page_size: None,
                    tx_behavior: None,
                    tx_protocol: None,
                    tx_sort_by: None,
                    tx_sort_dir: None,
                    notice: None,
                    created_key: None,
                },
                editable_cluster.as_ref(),
                None,
                Some(err.to_string()),
                None,
                None,
                None,
            )
            .await
        }
    }
}

async fn delete_cluster(
    State(state): State<WebState>,
    headers: HeaderMap,
    Path(cluster_id): Path<String>,
) -> Result<Response, WebError> {
    let Some(user) = current_user(&state, &headers).await? else {
        return Ok(Redirect::to("/auth?notice=auth-required").into_response());
    };
    ensure_user_owns_cluster(&state, user.user_id, &cluster_id).await?;
    match api_post_json::<_, serde_json::Value>(
        &state,
        &format!("/clusters/{cluster_id}/delete"),
        &json!({}),
    )
    .await
    {
        Ok(_) => Ok(redirect_response("/app/clusters?notice=cluster-deleted")),
        Err(err) => Ok(redirect_response(&format!(
            "/app/clusters?cluster_id={cluster_id}&notice=cluster-delete-error&error={}",
            urlencoding::encode(&err.to_string())
        ))),
    }
}

async fn queue_cluster_backfill(
    State(state): State<WebState>,
    headers: HeaderMap,
    Path(cluster_id): Path<String>,
) -> Result<Response, WebError> {
    let Some(_user) = current_user(&state, &headers).await? else {
        return Ok(Redirect::to("/auth?notice=auth-required").into_response());
    };
    ensure_user_owns_cluster(&state, _user.user_id, &cluster_id).await?;
    match api_post_json::<_, serde_json::Value>(
        &state,
        &format!("/clusters/{cluster_id}/backfill"),
        &ClusterBackfillRequest {
            start_block: None,
            end_block: None,
            force_reanalyze: Some(false),
            query_limit: None,
        },
    )
    .await
    {
        Ok(_) => Ok(redirect_response(&format!(
            "/app/clusters/{cluster_id}?notice=cluster-backfill-queued"
        ))),
        Err(err) => Ok(redirect_response(&format!(
            "/app/clusters/{cluster_id}?notice=cluster-backfill-error&error={}",
            urlencoding::encode(&err.to_string())
        ))),
    }
}

async fn enable_cluster_tracking(
    State(state): State<WebState>,
    headers: HeaderMap,
    Path(cluster_id): Path<String>,
) -> Result<Response, WebError> {
    let Some(_user) = current_user(&state, &headers).await? else {
        return Ok(Redirect::to("/auth?notice=auth-required").into_response());
    };
    match api_post_json::<_, ClusterTrackingState>(
        &state,
        &format!("/clusters/{cluster_id}/tracking/enable"),
        &json!({}),
    )
    .await
    {
        Ok(_) => Ok(redirect_response(&format!(
            "/app/clusters/{cluster_id}?notice=tracking-enabled"
        ))),
        Err(err) => Ok(redirect_response(&format!(
            "/app/clusters/{cluster_id}?notice=tracking-error&error={}",
            urlencoding::encode(&err.to_string())
        ))),
    }
}

async fn pause_cluster_tracking(
    State(state): State<WebState>,
    headers: HeaderMap,
    Path(cluster_id): Path<String>,
) -> Result<Response, WebError> {
    let Some(_user) = current_user(&state, &headers).await? else {
        return Ok(Redirect::to("/auth?notice=auth-required").into_response());
    };
    match api_post_json::<_, ClusterTrackingState>(
        &state,
        &format!("/clusters/{cluster_id}/tracking/pause"),
        &json!({}),
    )
    .await
    {
        Ok(_) => Ok(redirect_response(&format!(
            "/app/clusters/{cluster_id}?notice=tracking-paused"
        ))),
        Err(err) => Ok(redirect_response(&format!(
            "/app/clusters/{cluster_id}?notice=tracking-error&error={}",
            urlencoding::encode(&err.to_string())
        ))),
    }
}

async fn mark_cluster_alert_read(
    State(state): State<WebState>,
    headers: HeaderMap,
    Path((cluster_id, alert_id)): Path<(String, String)>,
) -> Result<Response, WebError> {
    let Some(_user) = current_user(&state, &headers).await? else {
        return Ok(Redirect::to("/auth?notice=auth-required").into_response());
    };
    match api_post_json::<_, ClusterAlert>(
        &state,
        &format!("/clusters/{cluster_id}/alerts/{alert_id}/read"),
        &json!({}),
    )
    .await
    {
        Ok(_) => Ok(redirect_response(&format!(
            "/app/clusters/{cluster_id}?notice=alert-read"
        ))),
        Err(err) => Ok(redirect_response(&format!(
            "/app/clusters/{cluster_id}?notice=alert-error&error={}",
            urlencoding::encode(&err.to_string())
        ))),
    }
}

async fn run_tx_analysis(
    State(state): State<WebState>,
    headers: HeaderMap,
    Form(form): Form<RunTxForm>,
) -> Result<Response, WebError> {
    let Some(user) = current_user(&state, &headers).await? else {
        return Ok(Redirect::to("/auth?notice=auth-required").into_response());
    };
    let mode = form.mode.as_deref().unwrap_or("analyze");
    let load_path = format!(
        "/analyses/{}?chain_id={}",
        form.tx_hash.trim(),
        form.chain_id
    );
    let result = if mode == "reanalyze" {
        api_post_json::<_, AnalysisEnvelope>(
            &state,
            "/analyses/tx",
            &AnalyzeTxRequest {
                chain_id: form.chain_id,
                tx_hash: form.tx_hash.clone(),
            },
        )
        .await
        .map(|payload| serde_json::to_value(payload).unwrap_or_default())
    } else {
        match api_get_json::<AnalysisEnvelope>(&state, &load_path).await {
            Ok(payload) => Ok(serde_json::to_value(payload).unwrap_or_default()),
            Err(_) => api_post_json::<_, AnalysisEnvelope>(
                &state,
                "/analyses/tx",
                &AnalyzeTxRequest {
                    chain_id: form.chain_id,
                    tx_hash: form.tx_hash.clone(),
                },
            )
            .await
            .map(|payload| serde_json::to_value(payload).unwrap_or_default()),
        }
    };

    let query = DashboardQuery {
        tab: Some("tx-analysis".to_owned()),
        cluster_id: None,
        start: None,
        end: None,
        cluster_tab: None,
        tx_page: None,
        tx_page_size: None,
        tx_behavior: None,
        tx_protocol: None,
        tx_sort_by: None,
        tx_sort_dir: None,
        notice: None,
        created_key: None,
    };
    match result {
        Ok(payload) => {
            render_dashboard_response(
                &state,
                user,
                query,
                None,
                Some(payload),
                None,
                None,
                None,
                None,
            )
            .await
        }
        Err(err) => {
            render_dashboard_response(
                &state,
                user,
                query,
                None,
                None,
                Some(err.to_string()),
                None,
                None,
                None,
            )
            .await
        }
    }
}

async fn create_tx_analysis_share(
    State(state): State<WebState>,
    headers: HeaderMap,
    Form(form): Form<ShareTxForm>,
) -> Result<Response, WebError> {
    let Some(user) = current_user(&state, &headers).await? else {
        return Ok(Redirect::to("/auth?notice=auth-required").into_response());
    };
    let tx_hash = form.tx_hash.trim().to_ascii_lowercase();
    if !tx_hash.starts_with("0x") {
        return Err(WebError::bad_request("invalid tx hash".to_owned()));
    }
    let payload = api_get_json::<AnalysisEnvelope>(
        &state,
        &format!("/analyses/{}?chain_id={}", tx_hash, form.chain_id),
    )
    .await?;
    let snapshot = serde_json::to_value(&payload)
        .map_err(|err| WebError::internal(format!("share snapshot encode failed: {err}")))?;
    let (activity, reason, confidence) = summarize_primary_activity(&payload.analysis);
    let title = format!("{} transaction {}", activity, truncate_middle(&tx_hash, 10));
    let summary = format!("{reason} Confidence: {confidence}.");
    let share_id = Uuid::new_v4();
    sqlx::query(
        r#"
        insert into tx_analysis_shares (
          share_id, chain_id, tx_hash, title, summary, analysis_snapshot, created_by
        )
        values ($1, $2, $3, $4, $5, $6, $7)
        "#,
    )
    .bind(share_id)
    .bind(form.chain_id as i64)
    .bind(&tx_hash)
    .bind(&title)
    .bind(&summary)
    .bind(SqlJson(snapshot))
    .bind(user.user_id)
    .execute(&state.pool)
    .await
    .map_err(|err| WebError::internal(format!("create share failed: {err}")))?;

    Ok(Redirect::to(&format!("/share/tx/{share_id}")).into_response())
}

async fn public_tx_analysis_share(
    State(state): State<WebState>,
    headers: HeaderMap,
    Path(share_id): Path<Uuid>,
) -> Result<Html<String>, WebError> {
    let user = current_user(&state, &headers).await?;
    let share = load_tx_analysis_share(&state, share_id).await?;
    let share_url = format!(
        "{}/share/tx/{}",
        state.web_base_url.trim_end_matches('/'),
        share.share_id
    );
    let content = format!(
        r#"<section class="tx-public-report">
  {}
</section>"#,
        render_tx_analysis_result_with_share_url(&share.snapshot, false, Some(&share_url))
    );
    Ok(Html(render_marketing_page(
        &format!("LedgerFlow share {}", share.share_id),
        user.as_ref(),
        content,
    )))
}

async fn render_dashboard_response(
    state: &WebState,
    user: AppUser,
    query: DashboardQuery,
    editable_cluster: Option<&AddressCluster>,
    tx_result: Option<serde_json::Value>,
    error: Option<String>,
    indexer_lookup: Option<IndexerAddressRefsPayload>,
    indexer_query: Option<IndexerLookupQuery>,
    address_summary: Option<AddressSummaryEnvelope>,
) -> Result<Response, WebError> {
    let tab = DashboardTab::from_query(query.tab.as_deref());
    let cluster_analysis_tab = ClusterAnalysisTab::from_query(query.cluster_tab.as_deref());
    let classified_query = cluster_tx_table_state_from_query(&query);
    let clusters = state
        .portal_service
        .list_clusters_for_user(user.user_id)
        .await?;
    let selected_cluster = query
        .cluster_id
        .as_deref()
        .and_then(|value| Uuid::parse_str(value).ok())
        .and_then(|cluster_id| {
            clusters
                .iter()
                .find(|item| item.cluster_id == cluster_id)
                .cloned()
        })
        .or_else(|| clusters.first().cloned());

    let (
        tracking,
        coverage,
        spend_summary,
        asset_changes,
        metrics,
        alerts,
        strategy_analysis,
        cluster_status,
        cluster_error,
    ) = if tab == DashboardTab::ClusterAnalysis {
        if let Some(cluster) = selected_cluster.as_ref() {
            match load_cluster_live_response(
                state,
                cluster.cluster_id,
                query.start.as_deref(),
                query.end.as_deref(),
                &cluster_analysis_tab,
            )
            .await
            {
                Ok(payload) => (
                    payload.tracking,
                    payload.coverage,
                    payload.spend_summary,
                    payload.asset_changes,
                    payload.metrics,
                    payload.alerts,
                    payload.strategy_analysis,
                    Some((payload.status_label, payload.status_tone)),
                    None::<String>,
                ),
                Err(err) => (
                    None,
                    None,
                    None,
                    None,
                    Vec::new(),
                    Vec::new(),
                    None,
                    None,
                    Some(err.to_string()),
                ),
            }
        } else {
            (
                None,
                None,
                None,
                None,
                Vec::new(),
                Vec::new(),
                None,
                None,
                None,
            )
        }
    } else {
        (
            None,
            None,
            None,
            None,
            Vec::new(),
            Vec::new(),
            None,
            None,
            None,
        )
    };

    let classified_page = if tab == DashboardTab::ClusterAnalysis
        && cluster_analysis_tab == ClusterAnalysisTab::Activity
    {
        if let Some(cluster) = selected_cluster.as_ref() {
            let mut params = vec![
                format!("page={}", classified_query.page),
                format!("page_size={}", classified_query.page_size),
                format!("sort_by={}", urlencoding::encode(&classified_query.sort_by)),
                format!(
                    "sort_dir={}",
                    urlencoding::encode(&classified_query.sort_dir)
                ),
            ];
            if let Some(behavior_tag) = classified_query.behavior_tag.as_deref() {
                params.push(format!(
                    "behavior_tag={}",
                    urlencoding::encode(behavior_tag)
                ));
            }
            if let Some(protocol) = classified_query.protocol.as_deref() {
                params.push(format!("protocol={}", urlencoding::encode(protocol)));
            }
            api_get_json::<ClassifiedTransactionsPagePayload>(
                state,
                &format!(
                    "/clusters/{}/classified-transactions?{}",
                    cluster.cluster_id,
                    params.join("&")
                ),
            )
            .await
            .ok()
        } else {
            None
        }
    } else {
        None
    };

    let (indexer_progress, indexer_error) = if tab == DashboardTab::IndexerProgress {
        match indexer_get_json::<IndexerProgressPayload>(state, "/progress").await {
            Ok(payload) => (Some(payload), None),
            Err(err) => (None, Some(err.to_string())),
        }
    } else {
        (None, None)
    };
    let (indexer_api_keys, indexer_api_keys_error) = if tab == DashboardTab::IndexerApiKeys {
        match indexer_get_json::<Vec<IndexerApiKeyPayload>>(
            state,
            &format!(
                "/admin/api-keys?owner_user_id={}",
                urlencoding::encode(&user.user_id.to_string())
            ),
        )
        .await
        {
            Ok(payload) => (payload, None),
            Err(err) => (Vec::new(), Some(err.to_string())),
        }
    } else {
        (Vec::new(), None)
    };

    let notice = query.notice.as_deref().and_then(render_notice_message);
    Ok(Html(render_dashboard_page(
        state,
        &user,
        &tab,
        notice,
        query.created_key.as_deref(),
        &clusters,
        editable_cluster,
        selected_cluster.as_ref(),
        tracking.as_ref(),
        coverage.as_ref(),
        spend_summary.as_ref(),
        asset_changes.as_ref(),
        &metrics,
        &alerts,
        strategy_analysis.as_ref(),
        classified_page.as_ref(),
        &classified_query,
        cluster_status.as_ref(),
        &cluster_analysis_tab,
        tx_result.as_ref(),
        indexer_progress.as_ref(),
        indexer_lookup.as_ref(),
        &indexer_api_keys,
        indexer_query.as_ref(),
        query.start.as_deref(),
        query.end.as_deref(),
        error
            .as_deref()
            .or(cluster_error.as_deref())
            .or(indexer_error.as_deref())
            .or(indexer_api_keys_error.as_deref()),
        address_summary.as_ref(),
    ))
    .into_response())
}

async fn load_cluster_live_response(
    state: &WebState,
    cluster_id: Uuid,
    start: Option<&str>,
    end: Option<&str>,
    selected_tab: &ClusterAnalysisTab,
) -> Result<ClusterLiveResponse, WebError> {
    let coverage_path = format!("/clusters/{}/coverage", cluster_id);
    let summary_path = match (start, end) {
        (None, None) => format!("/clusters/{}/spend-summary", cluster_id),
        _ => {
            let mut params = Vec::new();
            if let Some(start) = start {
                params.push(format!("start={}", urlencoding::encode(start)));
            }
            if let Some(end) = end {
                params.push(format!("end={}", urlencoding::encode(end)));
            }
            format!(
                "/clusters/{}/spend-summary?{}",
                cluster_id,
                params.join("&")
            )
        }
    };
    let asset_changes_path = match (start, end) {
        (None, None) => format!("/clusters/{}/asset-changes", cluster_id),
        _ => {
            let mut params = Vec::new();
            if let Some(start) = start {
                params.push(format!("start={}", urlencoding::encode(start)));
            }
            if let Some(end) = end {
                params.push(format!("end={}", urlencoding::encode(end)));
            }
            format!(
                "/clusters/{}/asset-changes?{}",
                cluster_id,
                params.join("&")
            )
        }
    };
    let (coverage, tracking, spend_summary, asset_changes, metrics, alerts, strategy_analysis) =
        match selected_tab {
            ClusterAnalysisTab::Overview => {
                let coverage_fut = api_get_json::<ClusterCoverage>(state, &coverage_path);
                let tracking_path = format!("/clusters/{}/tracking", cluster_id);
                let alerts_path = format!(
                    "/clusters/{}/alerts?include_read=false&limit=12",
                    cluster_id
                );
                let tracking_fut = api_get_json::<ClusterTrackingState>(state, &tracking_path);
                let alerts_fut = api_get_json::<Vec<ClusterAlert>>(state, &alerts_path);
                let (coverage, tracking, alerts) =
                    tokio::join!(coverage_fut, tracking_fut, alerts_fut);
                (
                    coverage.ok(),
                    tracking.ok(),
                    None,
                    None,
                    Vec::new(),
                    alerts.unwrap_or_default(),
                    None,
                )
            }
            ClusterAnalysisTab::Metrics => {
                let coverage_fut = api_get_json::<ClusterCoverage>(state, &coverage_path);
                let metrics_path = format!("/clusters/{}/metrics", cluster_id);
                let metrics_fut = api_get_json::<Vec<ClusterMetricsSnapshot>>(state, &metrics_path);
                let (coverage, metrics) = tokio::join!(coverage_fut, metrics_fut);
                (
                    coverage.ok(),
                    None,
                    None,
                    None,
                    metrics.unwrap_or_default(),
                    Vec::new(),
                    None,
                )
            }
            ClusterAnalysisTab::Summary => {
                let coverage_fut = api_get_json::<ClusterCoverage>(state, &coverage_path);
                let summary_fut = api_get_json::<ClusterSpendSummary>(state, &summary_path);
                let asset_changes_fut =
                    api_get_json::<ClusterAssetChangeReport>(state, &asset_changes_path);
                let (coverage, spend_summary, asset_changes) =
                    tokio::join!(coverage_fut, summary_fut, asset_changes_fut);
                (
                    coverage.ok(),
                    None,
                    spend_summary.ok(),
                    asset_changes.ok(),
                    Vec::new(),
                    Vec::new(),
                    None,
                )
            }
            ClusterAnalysisTab::Strategy => {
                let coverage_fut = api_get_json::<ClusterCoverage>(state, &coverage_path);
                let strategy_path = match (start, end) {
                    (None, None) => format!("/clusters/{}/strategy-analysis", cluster_id),
                    _ => {
                        let mut params = Vec::new();
                        if let Some(start) = start {
                            params.push(format!("start={}", urlencoding::encode(start)));
                        }
                        if let Some(end) = end {
                            params.push(format!("end={}", urlencoding::encode(end)));
                        }
                        format!(
                            "/clusters/{}/strategy-analysis?{}",
                            cluster_id,
                            params.join("&")
                        )
                    }
                };
                let strategy_fut =
                    api_get_json::<ClusterStrategyAnalysisPayload>(state, &strategy_path);
                let (coverage, strategy_analysis) = tokio::join!(coverage_fut, strategy_fut);
                (
                    coverage.ok(),
                    None,
                    None,
                    None,
                    Vec::new(),
                    Vec::new(),
                    strategy_analysis.ok(),
                )
            }
            ClusterAnalysisTab::Activity => (
                api_get_json::<ClusterCoverage>(state, &coverage_path)
                    .await
                    .ok(),
                None,
                None,
                None,
                Vec::new(),
                Vec::new(),
                None,
            ),
        };
    let (status_label, status_tone) = derive_cluster_status(coverage.as_ref());
    Ok(ClusterLiveResponse {
        status_label,
        status_tone,
        tracking,
        coverage,
        spend_summary,
        asset_changes,
        metrics,
        alerts,
        strategy_analysis,
    })
}

async fn load_editable_cluster(
    state: &WebState,
    cluster_id: Option<&str>,
) -> Result<Option<AddressCluster>, WebError> {
    let Some(cluster_id) = cluster_id.filter(|value| !value.trim().is_empty()) else {
        return Ok(None);
    };
    let cluster_id = Uuid::parse_str(cluster_id)
        .map_err(|err| WebError::bad_request(format!("invalid cluster id: {err}")))?;
    api_get_json::<AddressCluster>(state, &format!("/clusters/{cluster_id}"))
        .await
        .map(Some)
}

async fn ensure_user_owns_cluster(
    state: &WebState,
    user_id: Uuid,
    cluster_id: &str,
) -> Result<(), WebError> {
    let cluster_id = Uuid::parse_str(cluster_id)
        .map_err(|err| WebError::bad_request(format!("invalid cluster id: {err}")))?;
    let allowed = state
        .portal_service
        .list_clusters_for_user(user_id)
        .await?
        .into_iter()
        .any(|cluster| cluster.cluster_id == cluster_id);
    if allowed {
        Ok(())
    } else {
        Err(WebError::bad_request(
            "cluster is not available for the current user".to_owned(),
        ))
    }
}

async fn current_user(state: &WebState, headers: &HeaderMap) -> Result<Option<AppUser>, WebError> {
    let Some(token) = session_token_from_headers(headers, &state.session_cookie_name) else {
        return Ok(None);
    };
    Ok(state.auth_service.get_user_by_session_token(&token).await?)
}

async fn load_tx_analysis_share(
    state: &WebState,
    share_id: Uuid,
) -> Result<TxAnalysisShareRecord, WebError> {
    let row = sqlx::query(
        r#"
        select share_id, analysis_snapshot
        from tx_analysis_shares
        where share_id = $1
          and revoked_at is null
          and (expires_at is null or expires_at > now())
        "#,
    )
    .bind(share_id)
    .fetch_optional(&state.pool)
    .await
    .map_err(|err| WebError::internal(format!("load share failed: {err}")))?;

    let Some(row) = row else {
        return Err(WebError {
            status: StatusCode::NOT_FOUND,
            message: "share link not found".to_owned(),
        });
    };
    Ok(TxAnalysisShareRecord {
        share_id: row
            .try_get("share_id")
            .map_err(|err| WebError::internal(format!("decode share id failed: {err}")))?,
        snapshot: row
            .try_get::<SqlJson<serde_json::Value>, _>("analysis_snapshot")
            .map_err(|err| WebError::internal(format!("decode share snapshot failed: {err}")))?
            .0,
    })
}

fn parse_cluster_addresses(raw: &str) -> LedgerResult<Vec<ClusterAddressInput>> {
    let addresses = raw
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(|line| {
            let mut parts = line.split(',').map(|item| item.trim());
            let address = parts.next().unwrap_or_default().to_owned();
            if !address.starts_with("0x") {
                return Err(LedgerError::AnalysisInvariant(format!(
                    "address must start with 0x: {address}"
                )));
            }
            Ok(ClusterAddressInput {
                address,
                label: parts
                    .next()
                    .filter(|value| !value.is_empty())
                    .map(str::to_owned),
                role: parts
                    .next()
                    .filter(|value| !value.is_empty())
                    .map(str::to_owned),
            })
        })
        .collect::<LedgerResult<Vec<_>>>()?;
    if addresses.is_empty() {
        return Err(LedgerError::AnalysisInvariant(
            "at least one address is required".to_owned(),
        ));
    }
    Ok(addresses)
}

fn format_cluster_addresses(cluster: &AddressCluster) -> String {
    cluster
        .addresses
        .iter()
        .map(|address| {
            let mut parts = vec![address.address.clone()];
            if address.label.is_some() || address.role.is_some() {
                parts.push(address.label.clone().unwrap_or_default());
            }
            if address.role.is_some() {
                parts.push(address.role.clone().unwrap_or_default());
            }
            parts.join(",")
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn cluster_tx_table_state_from_query(query: &DashboardQuery) -> ClusterTxTableState {
    let page = query
        .tx_page
        .as_deref()
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(1)
        .max(1);
    let page_size = query
        .tx_page_size
        .as_deref()
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(25)
        .clamp(10, 50);
    let sort_by = query
        .tx_sort_by
        .as_deref()
        .filter(|value| {
            matches!(
                *value,
                "timestamp" | "block_number" | "value_usd" | "gas_cost_usd" | "estimated_pnl_usd"
            )
        })
        .unwrap_or("timestamp")
        .to_owned();
    let sort_dir = match query.tx_sort_dir.as_deref() {
        Some("asc") => "asc".to_owned(),
        _ => "desc".to_owned(),
    };
    ClusterTxTableState {
        page,
        page_size,
        behavior_tag: query
            .tx_behavior
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned),
        protocol: query
            .tx_protocol
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned),
        sort_by,
        sort_dir,
    }
}

fn parse_purpose(value: &str) -> Result<VerificationPurpose, WebError> {
    verification_purpose_from_str(value)
        .ok_or_else(|| WebError::bad_request("invalid verification purpose".to_owned()))
}

async fn api_get_json<T: DeserializeOwned>(state: &WebState, path: &str) -> Result<T, WebError> {
    let response = state
        .client
        .get(format!(
            "{}{}",
            state.api_base_url.trim_end_matches('/'),
            path
        ))
        .send()
        .await
        .map_err(WebError::upstream)?;
    parse_api_response(response).await
}

async fn indexer_get_json<T: DeserializeOwned>(
    state: &WebState,
    path: &str,
) -> Result<T, WebError> {
    let Some(base_url) = state.indexer_api_base_url.as_deref() else {
        return Err(WebError::bad_gateway(
            "indexer api is not configured for the web portal".to_owned(),
        ));
    };
    let mut request = state
        .client
        .get(format!("{}{}", base_url.trim_end_matches('/'), path));
    if let Some(token) = state.indexer_api_bearer_token.as_deref() {
        request = request.bearer_auth(token);
    }
    let response = request.send().await.map_err(WebError::upstream)?;
    parse_indexer_response(response).await
}

async fn indexer_post_json<Req: Serialize, Res: DeserializeOwned>(
    state: &WebState,
    path: &str,
    payload: &Req,
) -> Result<Res, WebError> {
    let Some(base_url) = state.indexer_api_base_url.as_deref() else {
        return Err(WebError::bad_gateway(
            "indexer api is not configured for the web portal".to_owned(),
        ));
    };
    let mut request = state
        .client
        .post(format!("{}{}", base_url.trim_end_matches('/'), path))
        .json(payload);
    if let Some(token) = state.indexer_api_bearer_token.as_deref() {
        request = request.bearer_auth(token);
    }
    let response = request.send().await.map_err(WebError::upstream)?;
    parse_indexer_response(response).await
}

async fn api_post_json<Req: Serialize, Res: DeserializeOwned>(
    state: &WebState,
    path: &str,
    payload: &Req,
) -> Result<Res, WebError> {
    let response = state
        .client
        .post(format!(
            "{}{}",
            state.api_base_url.trim_end_matches('/'),
            path
        ))
        .json(payload)
        .send()
        .await
        .map_err(WebError::upstream)?;
    parse_api_response(response).await
}

async fn parse_api_response<T: DeserializeOwned>(
    response: reqwest::Response,
) -> Result<T, WebError> {
    let status = response.status();
    let body = response.text().await.map_err(WebError::upstream)?;
    if !status.is_success() {
        return Err(WebError::bad_gateway(format!(
            "internal analysis api returned {}: {}",
            status, body
        )));
    }
    serde_json::from_str(&body).map_err(|err| WebError::internal(err.to_string()))
}

async fn parse_indexer_response<T: DeserializeOwned>(
    response: reqwest::Response,
) -> Result<T, WebError> {
    let status = response.status();
    let body = response.text().await.map_err(WebError::upstream)?;
    if !status.is_success() {
        return Err(WebError::bad_gateway(format!(
            "indexer api returned {}: {}",
            status, body
        )));
    }
    serde_json::from_str(&body).map_err(|err| WebError::internal(err.to_string()))
}

fn build_indexer_lookup_path(query: &IndexerLookupQuery) -> Result<Option<String>, WebError> {
    let Some(address) = query
        .address
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Ok(None);
    };
    let mut params = Vec::new();
    if let Some(limit) = query.limit {
        params.push(format!("limit={limit}"));
    }
    if let Some(from_block) = query.from_block {
        params.push(format!("from_block={from_block}"));
    }
    if let Some(to_block) = query.to_block {
        params.push(format!("to_block={to_block}"));
    }
    if let Some(direction) = query
        .direction
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        match direction {
            "from" | "to" => params.push(format!("direction={direction}")),
            _ => {
                return Err(WebError::bad_request(
                    "direction must be either from or to".to_owned(),
                ));
            }
        }
    }
    let base = format!("/addresses/{}/txs", urlencoding::encode(address));
    if params.is_empty() {
        Ok(Some(base))
    } else {
        Ok(Some(format!("{base}?{}", params.join("&"))))
    }
}

fn user_agent(headers: &HeaderMap) -> Option<String> {
    headers
        .get(header::USER_AGENT)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}

fn session_token_from_headers(headers: &HeaderMap, cookie_name: &str) -> Option<String> {
    headers
        .get(header::COOKIE)
        .and_then(|value| value.to_str().ok())
        .and_then(|cookie_header| {
            cookie_header.split(';').find_map(|item| {
                let (name, value) = item.trim().split_once('=')?;
                if name == cookie_name {
                    Some(value.to_owned())
                } else {
                    None
                }
            })
        })
}

fn session_cookie(cookie_name: &str, session_token: &str, max_age_seconds: i64) -> String {
    format!(
        "{}={}; Path=/; HttpOnly; SameSite=Lax; Max-Age={}",
        cookie_name, session_token, max_age_seconds
    )
}

fn clear_session_cookie(cookie_name: &str) -> String {
    format!(
        "{}=deleted; Path=/; HttpOnly; SameSite=Lax; Max-Age=0",
        cookie_name
    )
}

fn redirect_response(location: &str) -> Response {
    Redirect::to(location).into_response()
}

fn render_notice_message(code: &str) -> Option<&'static str> {
    match code {
        "code-sent" => Some("Verification code sent. Check your inbox and enter the code below."),
        "signed-in" => Some("Signed in."),
        "logged-out" => Some("Signed out."),
        "cluster-created" => Some("Cluster created and queued for historical backfill."),
        "cluster-updated" => Some("Cluster definition updated."),
        "cluster-updated-needs-backfill" => Some(
            "Cluster definition updated. Review the address set, then manually run a backfill or incremental analysis for the new cluster scope.",
        ),
        "cluster-backfill-queued" => {
            Some("Cluster analysis backfill queued. The worker will process it in the background.")
        }
        "cluster-backfill-error" => Some("Cluster analysis backfill could not be queued."),
        "cluster-deleted" => Some("Cluster deleted."),
        "cluster-delete-error" => {
            Some("Cluster delete failed. Inspect the latest server response or logs.")
        }
        "tracking-enabled" => Some("Cluster tracking enabled."),
        "tracking-paused" => Some("Cluster tracking paused."),
        "tracking-error" => {
            Some("Tracking action failed. Inspect the latest cluster state or server logs.")
        }
        "alert-read" => Some("Alert marked as read."),
        "alert-error" => {
            Some("Alert action failed. Refresh the cluster view and inspect server logs.")
        }
        "indexer-key-created" => {
            Some("Indexer API key created. Copy the full token now; the portal only shows it once.")
        }
        "indexer-key-revoked" => Some("Indexer API key revoked."),
        "indexer-key-label-required" => Some("Provide a label before creating an indexer API key."),
        "auth-required" => Some("Sign in to continue."),
        _ => None,
    }
}

fn render_marketing_page(title: &str, user: Option<&AppUser>, content: String) -> String {
    format!(
        r#"<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>{}</title>
    <link rel="icon" type="image/svg+xml" href="/assets/ledgerflow-mark.svg">
    <style>{}</style>
  </head>
  <body class="marketing">
    <div class="shell">
      <header class="topbar">
        <a class="brand" href="/">{}</a>
        <nav class="navlinks">
          <a href="/">Home</a>
          <a href="/pricing">Pricing</a>
          {}
        </nav>
      </header>
      {}
    </div>
  </body>
</html>"#,
        escape_html(title),
        shared_styles(),
        brand_wordmark_html(),
        if user.is_some() {
            "<a class=\"cta ghost\" href=\"/app/clusters\">Dashboard</a>".to_owned()
        } else {
            "<a class=\"cta ghost\" href=\"/auth\">Login / Register</a>".to_owned()
        },
        content
    )
}

fn render_homepage_content(user: Option<&AppUser>) -> String {
    format!(
        r#"
<section class="hero">
  <div class="hero-copy">
    <span class="eyebrow">Evidence-first onchain ops intelligence</span>
    <h1>Track what your bots and EOAs actually spent, where it went, and which txs explain it.</h1>
    <p>LedgerFlow combines indexer-fed transaction discovery with replayable transaction analysis, then turns that into operator-facing cluster summaries. It is built for address clusters, not explorer vanity metrics.</p>
    <div class="hero-actions">
      <a class="cta primary" href="{}">{}</a>
      <a class="cta secondary" href="/pricing">See pricing</a>
    </div>
  </div>
  <div class="hero-card">
    <div class="metric-card"><span>Cluster coverage</span><strong>discovered / analyzed / failed / pending</strong></div>
    <div class="metric-card"><span>Spend attribution</span><strong>gas, external outflows, internal transfers, incoming</strong></div>
    <div class="metric-card"><span>Debug layer</span><strong>single tx evidence pack and recognizer outputs</strong></div>
  </div>
</section>
<section class="feature-grid">
  <article class="feature-card">
    <h2>Homepage</h2>
    <p>Operator-facing positioning, pricing, and auth entry instead of raw API exposure.</p>
  </article>
  <article class="feature-card">
    <h2>Dashboard</h2>
    <p>Cluster management, cluster analysis, and single-transaction analysis live in separated tabs so the product flow stays clear.</p>
  </article>
  <article class="feature-card">
    <h2>Email code auth</h2>
    <p>Passwordless login and registration by email verification code. Suitable for private beta operations without committing to a full identity stack too early.</p>
  </article>
</section>
<section class="pricing-banner">
  <div>
    <span class="eyebrow">Pricing</span>
    <h2>0.5 ETH / month</h2>
    <p>Private beta is currently free for all invited users.</p>
  </div>
  <a class="cta secondary" href="/pricing">Open pricing</a>
</section>
"#,
        if user.is_some() {
            "/app/clusters"
        } else {
            "/auth"
        },
        if user.is_some() {
            "Open dashboard"
        } else {
            "Request access"
        }
    )
}

fn render_pricing_content() -> String {
    format!(
        r#"
<section class="pricing-grid">
  <article class="pricing-card featured">
    <span class="eyebrow">Private beta plan</span>
    <h1>{} ETH <small>/ month</small></h1>
    <p>Built for a single operator or small desk that needs one place to manage address clusters, backfill historical transactions, and debug single txs.</p>
    <ul class="flat-list">
      <li>Cluster management dashboard</li>
      <li>Historical backfill queue and coverage tracking</li>
      <li>Single-tx evidence analysis</li>
      <li>Email-based auth</li>
    </ul>
    <div class="price-note">Current beta status: <strong>free to use during internal testing</strong>.</div>
    <a class="cta primary" href="/auth">Register for beta</a>
  </article>
  <article class="pricing-card">
    <span class="eyebrow">Billing status</span>
    <h2>Beta access is free right now</h2>
    <p>Formal subscription pricing is still <strong>{} ETH / month</strong>, but enforcement is disabled during the current internal beta while workflow and recognizers continue to harden.</p>
  </article>
</section>
"#,
        DEFAULT_PRICE_ETH, DEFAULT_PRICE_ETH
    )
}

fn render_auth_content(
    notice: Option<&str>,
    error: Option<String>,
    mode: &str,
    email_delivery_mode: &str,
    email_delivery_ready: bool,
) -> String {
    format!(
        r#"
<section class="auth-shell">
  <div class="auth-copy">
    <span class="eyebrow">Access</span>
    <h1>Login or register with a one-time email code.</h1>
    <p>No password setup yet. Request a code, verify it, and you land in the dashboard.</p>
    {}
    {}
    {}
  </div>
  <div class="auth-grid">
    <form class="auth-card" method="post" action="/auth/request-code">
      <h2>Create account</h2>
      <input type="hidden" name="purpose" value="signup">
      <label>Email
        <input type="email" name="email" placeholder="ops@yourdesk.xyz" required>
      </label>
      <button class="cta primary" type="submit">Send signup code</button>
    </form>
    <form class="auth-card" method="post" action="/auth/request-code">
      <h2>Sign in</h2>
      <input type="hidden" name="purpose" value="login">
      <label>Email
        <input type="email" name="email" placeholder="ops@yourdesk.xyz" required>
      </label>
      <button class="cta secondary" type="submit">Send login code</button>
    </form>
  </div>
  <div class="mode-hint">Current mode: <strong>{}</strong></div>
</section>
"#,
        render_email_delivery_notice(email_delivery_mode, email_delivery_ready),
        render_notice_block(notice),
        render_error_block(error.as_deref()),
        escape_html(mode)
    )
}

fn render_verify_content(
    email: &str,
    purpose: &str,
    notice: Option<&str>,
    error: Option<String>,
) -> String {
    format!(
        r#"
<section class="verify-shell">
  <div class="auth-copy">
    <span class="eyebrow">Verify</span>
    <h1>Enter the 6-digit code sent to {}</h1>
    <p>This code flow is used for both signup and login. Codes expire quickly and are single-use.</p>
    {}
    {}
  </div>
  <form class="auth-card narrow" method="post" action="/auth/verify">
    <input type="hidden" name="email" value="{}">
    <input type="hidden" name="purpose" value="{}">
    <label>Verification code
      <input type="text" name="code" placeholder="123456" inputmode="numeric" required>
    </label>
    <button class="cta primary" type="submit">Verify and continue</button>
  </form>
</section>
"#,
        escape_html(email),
        render_notice_block(notice),
        render_error_block(error.as_deref()),
        escape_html(email),
        escape_html(purpose)
    )
}

fn render_email_delivery_notice(email_delivery_mode: &str, email_delivery_ready: bool) -> String {
    if email_delivery_ready {
        return format!(
            "<div class=\"notice success\">Verification emails are being delivered through <strong>{}</strong>.</div>",
            escape_html(email_delivery_mode)
        );
    }
    format!(
        "<div class=\"notice warning\">Verification email delivery is in <strong>{}</strong> mode. Codes will be written to server logs unless you configure `LEDGERFLOW_EMAIL_FROM` plus either `LEDGERFLOW_POSTMARK_SERVER_TOKEN` or `LEDGERFLOW_RESEND_API_KEY`, or force a real delivery mode explicitly.</div>",
        escape_html(email_delivery_mode)
    )
}

#[allow(clippy::too_many_arguments)]
fn render_dashboard_page(
    state: &WebState,
    user: &AppUser,
    tab: &DashboardTab,
    notice: Option<&str>,
    created_key: Option<&str>,
    clusters: &[DashboardClusterRecord],
    editable_cluster: Option<&AddressCluster>,
    selected_cluster: Option<&DashboardClusterRecord>,
    tracking: Option<&ClusterTrackingState>,
    coverage: Option<&ClusterCoverage>,
    spend_summary: Option<&ClusterSpendSummary>,
    asset_changes: Option<&ClusterAssetChangeReport>,
    metrics: &[ClusterMetricsSnapshot],
    alerts: &[ClusterAlert],
    strategy_analysis: Option<&ClusterStrategyAnalysisPayload>,
    classified_page: Option<&ClassifiedTransactionsPagePayload>,
    classified_query: &ClusterTxTableState,
    cluster_status: Option<&(String, String)>,
    cluster_analysis_tab: &ClusterAnalysisTab,
    tx_result: Option<&serde_json::Value>,
    indexer_progress: Option<&IndexerProgressPayload>,
    indexer_lookup: Option<&IndexerAddressRefsPayload>,
    indexer_api_keys: &[IndexerApiKeyPayload],
    indexer_query: Option<&IndexerLookupQuery>,
    start: Option<&str>,
    end: Option<&str>,
    error: Option<&str>,
    address_summary: Option<&AddressSummaryEnvelope>,
) -> String {
    let cluster_cards = if clusters.is_empty() {
        "<div class=\"empty-state\">No clusters yet. Create one in the Cluster Management tab.</div>".to_owned()
    } else {
        clusters
            .iter()
            .map(|cluster| {
                format!(
                    r#"<article class="cluster-card">
<a class="cluster-card-link" href="/app/clusters/{}">
  <strong>{}</strong>
  <span>{}</span>
  <small>{} addresses · chain {}</small>
</a>
<div class="cluster-card-actions">
  <a class="cta ghost tiny" href="/app/clusters?cluster_id={}">Edit</a>
  <form method="post" action="/app/clusters/{}/delete" onsubmit="return confirm('Delete this cluster and its tracking history?');">
    <button class="cta ghost tiny danger" type="submit">Delete</button>
  </form>
</div>
</article>"#,
                    cluster.cluster_id,
                    escape_html(&cluster.name),
                    escape_html(cluster.description.as_deref().unwrap_or("No description")),
                    cluster.address_count,
                    cluster.chain_id,
                    cluster.cluster_id,
                    cluster.cluster_id
                )
            })
            .collect::<Vec<_>>()
            .join("")
    };

    let cluster_analysis_panel = if let Some(cluster) = selected_cluster {
        let status_label = cluster_status
            .map(|item| item.0.as_str())
            .unwrap_or("Waiting for backfill");
        let status_tone = cluster_status.map(|item| item.1.as_str()).unwrap_or("info");
        let overview_active = if *cluster_analysis_tab == ClusterAnalysisTab::Overview {
            "active"
        } else {
            ""
        };
        let metrics_active = if *cluster_analysis_tab == ClusterAnalysisTab::Metrics {
            "active"
        } else {
            ""
        };
        let strategy_active = if *cluster_analysis_tab == ClusterAnalysisTab::Strategy {
            "active"
        } else {
            ""
        };
        let summary_active = if *cluster_analysis_tab == ClusterAnalysisTab::Summary {
            "active"
        } else {
            ""
        };
        let activity_active = if *cluster_analysis_tab == ClusterAnalysisTab::Activity {
            "active"
        } else {
            ""
        };
        let overview_href =
            cluster_analysis_tab_href(cluster.cluster_id, start, end, ClusterAnalysisTab::Overview);
        let metrics_href =
            cluster_analysis_tab_href(cluster.cluster_id, start, end, ClusterAnalysisTab::Metrics);
        let strategy_href =
            cluster_analysis_tab_href(cluster.cluster_id, start, end, ClusterAnalysisTab::Strategy);
        let summary_href =
            cluster_analysis_tab_href(cluster.cluster_id, start, end, ClusterAnalysisTab::Summary);
        let activity_href =
            cluster_analysis_tab_href(cluster.cluster_id, start, end, ClusterAnalysisTab::Activity);
        let tab_panel = match cluster_analysis_tab {
            ClusterAnalysisTab::Overview => format!(
                r#"<section class="cluster-tab-panel active" data-cluster-tab-panel="overview">
      <h3>Auto discovery</h3>
      {}
      <h3>Tracking</h3>
      <div id="cluster-live-tracking">{}</div>
      <h3>Coverage</h3>
      <div id="cluster-live-coverage">{}</div>
      <h3>Alerts</h3>
      <div id="cluster-live-alerts">{}</div>
    </section>"#,
                render_cluster_auto_discovery_panel(cluster),
                render_tracking_status_panel(cluster, tracking),
                render_cluster_coverage_cards(coverage),
                render_cluster_alerts_panel(cluster, alerts),
            ),
            ClusterAnalysisTab::Metrics => format!(
                r#"<section class="cluster-tab-panel active" data-cluster-tab-panel="metrics">
      <h3>Rolling metrics</h3>
      <div id="cluster-live-metrics">{}</div>
      <h3>Behavior breakdown</h3>
      <div id="cluster-live-behavior">{}</div>
    </section>"#,
                render_cluster_metrics_panel(metrics),
                render_behavior_breakdown_panel(metrics),
            ),
            ClusterAnalysisTab::Strategy => format!(
                r#"<section class="cluster-tab-panel active" data-cluster-tab-panel="strategy">
      <h3>Strategy analysis</h3>
      <div id="cluster-live-strategy">{}</div>
    </section>"#,
                render_cluster_strategy_analysis_panel(strategy_analysis),
            ),
            ClusterAnalysisTab::Summary => format!(
                r#"<section class="cluster-tab-panel active" data-cluster-tab-panel="summary">
      <h3>Daily asset changes</h3>
      <div id="cluster-live-asset-changes">{}</div>
      <h3>Spend summary</h3>
      <div id="cluster-live-summary">{}</div>
    </section>"#,
                render_cluster_asset_changes_panel(asset_changes),
                render_cluster_summary_panel(spend_summary),
            ),
            ClusterAnalysisTab::Activity => format!(
                r#"<section class="cluster-tab-panel active" data-cluster-tab-panel="activity">
      <h3>Recent classified transactions</h3>
      <div id="cluster-live-classified">{}</div>
    </section>"#,
                render_recent_classified_transactions_panel(
                    cluster,
                    classified_page,
                    classified_query
                ),
            ),
        };
        format!(
            r#"
<section class="panel">
  <div class="panel-head"><h2>{}</h2><span class="badge">cluster analysis</span></div>
  <div class="panel-body">
    <form class="inline-form" method="get" action="/app/clusters/{}">
      <div class="stat"><span>Cluster</span><strong>{}</strong></div>
      <label>Start<input type="text" name="start" value="{}" placeholder="2026-04-01T00:00:00Z"></label>
      <label>End<input type="text" name="end" value="{}" placeholder="2026-04-30T23:59:59Z"></label>
      <input type="hidden" name="cluster_tab" value="{}">
      <button class="cta secondary" type="submit">Refresh analysis</button>
    </form>
    <div id="cluster-live-root" data-cluster-id="{}" data-start="{}" data-end="{}"></div>
    <div id="cluster-live-status" class="notice {}">{}</div>
    <form class="button-row" method="post" action="/app/clusters/{}/backfill">
      <button class="cta secondary" type="submit">Queue manual analysis</button>
    </form>
    <div class="cluster-tabbar" role="tablist" aria-label="Cluster analysis sections">
      <a class="cluster-tab {}" href="{}">Overview</a>
      <a class="cluster-tab {}" href="{}">Metrics</a>
      <a class="cluster-tab {}" href="{}">Strategy</a>
      <a class="cluster-tab {}" href="{}">Summary</a>
      <a class="cluster-tab {}" href="{}">Activity</a>
    </div>
    {}
  </div>
</section>
"#,
            escape_html(&cluster.name),
            cluster.cluster_id,
            escape_html(&format!("{} · {}", cluster.name, cluster.owner_label)),
            escape_html(start.unwrap_or_default()),
            escape_html(end.unwrap_or_default()),
            cluster_analysis_tab.as_str(),
            cluster.cluster_id,
            escape_html(start.unwrap_or_default()),
            escape_html(end.unwrap_or_default()),
            escape_html(status_tone),
            escape_html(status_label),
            cluster.cluster_id,
            overview_active,
            overview_href,
            metrics_active,
            metrics_href,
            strategy_active,
            strategy_href,
            summary_active,
            summary_href,
            activity_active,
            activity_href,
            tab_panel,
        )
    } else {
        "<section class=\"panel\"><div class=\"panel-body\"><div class=\"empty-state\">Select or create a cluster to view analysis.</div></div></section>".to_owned()
    };

    let (tx_hash_prefill, chain_id_prefill) = tx_form_prefill(tx_result, state.default_chain_id);
    let tx_status_badge = if tx_result.is_some() {
        "Analysis complete"
    } else {
        "Waiting for input"
    };
    let tx_panel = format!(
        r#"
<section class="panel tx-workbench">
  <div class="tx-workbench-head">
    <div>
      <h1>Tx Analysis</h1>
      <p>Enter a transaction hash. LedgerFlow loads a saved result when available; use Reanalyze only when you need fresh evidence.</p>
    </div>
    <span class="readonly-pill">{}</span>
  </div>
  <div class="panel-body">
    <form class="tx-search-form" method="post" action="/app/tx-analysis/run">
      <input type="hidden" name="chain_id" value="{}">
      <label class="sr-only">Transaction hash</label>
      <input type="text" name="tx_hash" value="{}" placeholder="0x transaction hash" required>
      <button class="cta secondary" type="submit" name="mode" value="analyze">Analyze</button>
      <button class="cta ghost" type="submit" name="mode" value="reanalyze">Reanalyze</button>
    </form>
    {}
  </div>
</section>
"#,
        tx_status_badge,
        chain_id_prefill,
        escape_html(&tx_hash_prefill),
        tx_result
            .map(render_tx_analysis_result)
            .unwrap_or_else(|| {
                "<div class=\"empty-state\">Enter one tx hash to load the saved result if it already exists. Use Reanalyze when you want LedgerFlow to fetch evidence again and rerun the recognizers.</div>".to_owned()
            })
    );
    let graph_panel = render_cluster_graph_panel(clusters);
    let address_analysis_panel =
        render_address_analysis_panel(address_summary, state.default_chain_id);
    let indexer_progress_panel = render_indexer_progress_panel(indexer_progress);
    let indexer_lookup_panel = render_indexer_lookup_panel(indexer_lookup, indexer_query);
    let indexer_api_keys_panel =
        render_indexer_api_keys_panel(user, indexer_api_keys, notice, created_key);
    let side_column = format!(
        r#"<section class="panel">
              <div class="panel-head"><h2>Your clusters</h2><span class="badge">{}</span></div>
              <div class="panel-body cluster-list">{}</div>
            </section>"#,
        clusters.len(),
        cluster_cards
    );
    let content_layout = if *tab == DashboardTab::ClusterAnalysis {
        cluster_analysis_panel
    } else if *tab == DashboardTab::IndexerLookup {
        indexer_lookup_panel.clone()
    } else if *tab == DashboardTab::IndexerProgress {
        indexer_progress_panel.clone()
    } else if *tab == DashboardTab::IndexerApiKeys {
        indexer_api_keys_panel.clone()
    } else if *tab == DashboardTab::TxAnalysis {
        tx_panel.clone()
    } else if *tab == DashboardTab::ClusterGraph {
        graph_panel.clone()
    } else if *tab == DashboardTab::AddressAnalysis {
        address_analysis_panel.clone()
    } else if *tab == DashboardTab::Clusters {
        format!(
            r#"<section class="dashboard-grid">
          <div class="main-column">
            {}
          </div>
          <div class="side-column">
            {}
          </div>
        </section>"#,
            match tab {
                DashboardTab::Clusters =>
                    render_cluster_management_panel(state.default_chain_id, editable_cluster),
                _ => String::new(),
            },
            side_column
        )
    } else {
        String::new()
    };

    format!(
        r#"<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>LedgerFlow Dashboard</title>
    <link rel="icon" type="image/svg+xml" href="/assets/ledgerflow-mark.svg">
    <style>{}</style>
  </head>
  <body class="dashboard-body">
    <div class="dashboard-shell">
      <aside class="sidebar">
        <a class="brand" href="/">{}</a>
        <div class="user-card">
          <span class="eyebrow">Signed in</span>
          <strong>{}</strong>
          <small>Private beta pricing: {} ETH / month, currently free.</small>
        </div>
        <nav class="side-nav">
          <a class="{}" href="/app/clusters">Cluster management</a>
          <a class="{}" href="{}">Cluster analysis</a>
          <a class="{}" href="/app/cluster-graph">Cluster graph</a>
          <a class="{}" href="/app/address-analysis">Address analysis</a>
          <a class="{}" href="/app/tx-analysis">Single tx analysis</a>
          <a class="" href="/app/arbitrage">Arbitrage</a>
          <a class="{}" href="/app/indexer/progress">Indexer progress</a>
          <a class="{}" href="/app/indexer/address-lookup">Indexer address lookup</a>
          <a class="{}" href="/app/indexer/api-keys">Indexer API keys</a>
          <a class="" href="/pricing">Pricing</a>
        </nav>
        <form method="post" action="/auth/logout">
          <button class="cta ghost" type="submit">Sign out</button>
        </form>
      </aside>
      <main class="dashboard-main">
        <section class="dashboard-hero">
          <span class="eyebrow">Application</span>
          <h1>{}</h1>
          <p>Use one signed-in portal for seed-based cluster onboarding, worker-driven address expansion, backfill analysis, bot reports, and dedicated single-tx debugging.</p>
          {}
          {}
        </section>
        {}
      </main>
    </div>
    {}
  </body>
</html>"#,
        shared_styles(),
        brand_wordmark_html(),
        escape_html(&user.email),
        DEFAULT_PRICE_ETH,
        active_class(tab, DashboardTab::Clusters),
        active_class(tab, DashboardTab::ClusterAnalysis),
        selected_cluster
            .map(|cluster| format!("/app/clusters/{}", cluster.cluster_id))
            .unwrap_or_else(|| "/app/clusters".to_owned()),
        active_class(tab, DashboardTab::ClusterGraph),
        active_class(tab, DashboardTab::AddressAnalysis),
        active_class(tab, DashboardTab::TxAnalysis),
        active_class(tab, DashboardTab::IndexerProgress),
        active_class(tab, DashboardTab::IndexerLookup),
        active_class(tab, DashboardTab::IndexerApiKeys),
        escape_html(section_title(tab)),
        render_notice_block(notice),
        render_error_block(error),
        content_layout,
        render_cluster_autorefresh_script(selected_cluster, start, end, cluster_analysis_tab)
    )
}

fn render_arbitrage_page(
    state: &WebState,
    user: &AppUser,
    minutes: i64,
    limit: i64,
    recent: &[ArbitrageRecentRow],
    hot_pools: &[ArbitrageHotPoolRow],
    hot_routes: &[ArbitrageHotRouteRow],
) -> String {
    let recent_rows = recent
        .iter()
        .map(|item| {
            format!(
                r#"<tr>
  <td>{}</td>
  <td>{}</td>
  <td>{}</td>
  <td>{}</td>
  <td>{}</td>
  <td>{}</td>
  <td>{}/{}</td>
  <td>{}</td>
  <td>{}</td>
</tr>"#,
                render_copyable_code(&item.tx_hash, 8),
                item.block_number,
                item.block_timestamp
                    .map(|value| value.format("%H:%M:%S").to_string())
                    .unwrap_or_else(|| "n/a".to_owned()),
                render_arbitrage_status(&item.confirmation_status),
                item.sender_address
                    .as_deref()
                    .map(|value| render_copyable_code(value, 6))
                    .unwrap_or_else(|| "n/a".to_owned()),
                item.entry_contract
                    .as_deref()
                    .map(|value| render_copyable_code(value, 6))
                    .unwrap_or_else(|| "n/a".to_owned()),
                item.swap_log_count,
                item.distinct_pool_count,
                render_arbitrage_profit(item),
                render_route_compact(&item.route, 4),
            )
        })
        .collect::<Vec<_>>()
        .join("");
    let pool_rows = hot_pools
        .iter()
        .map(|item| {
            format!(
                r#"<tr>
  <td>{}</td>
  <td>{}</td>
  <td>{}</td>
  <td>{}</td>
  <td>{}</td>
  <td>{}</td>
  <td>{}</td>
</tr>"#,
                render_copyable_code(&item.pool_address, 6),
                item.tx_count,
                item.swap_log_count,
                item.unique_senders,
                item.unique_routes,
                item.last_seen
                    .map(|value| value.format("%H:%M:%S").to_string())
                    .unwrap_or_else(|| "n/a".to_owned()),
                render_sample_hashes(&item.sample_tx_hashes),
            )
        })
        .collect::<Vec<_>>()
        .join("");
    let route_rows = hot_routes
        .iter()
        .map(|item| {
            format!(
                r#"<tr>
  <td>{}</td>
  <td>{}</td>
  <td>{}</td>
  <td>{}</td>
  <td>{}</td>
  <td>{}</td>
  <td>{}</td>
</tr>"#,
                render_copyable_code(&item.route_fingerprint, 10),
                render_route_compact(&item.route, 6),
                item.tx_count,
                item.swap_log_count,
                item.unique_senders,
                item.last_seen
                    .map(|value| value.format("%H:%M:%S").to_string())
                    .unwrap_or_else(|| "n/a".to_owned()),
                render_sample_hashes(&item.sample_tx_hashes),
            )
        })
        .collect::<Vec<_>>()
        .join("");
    format!(
        r#"<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>LedgerFlow Arbitrage</title>
    <link rel="icon" type="image/svg+xml" href="/assets/ledgerflow-mark.svg">
    <style>{}</style>
  </head>
  <body class="dashboard-body">
    <div class="dashboard-shell">
      <aside class="sidebar">
        <a class="brand" href="/">{}</a>
        <div class="user-card">
          <span class="eyebrow">Signed in</span>
          <strong>{}</strong>
          <small>Chain {}</small>
        </div>
        <nav class="side-nav">
          <a href="/app/clusters">Cluster management</a>
          <a href="/app/address-analysis">Address analysis</a>
          <a href="/app/tx-analysis">Single tx analysis</a>
          <a class="active" href="/app/arbitrage">Arbitrage</a>
          <a href="/app/indexer/progress">Indexer progress</a>
        </nav>
        <form method="post" action="/auth/logout">
          <button class="cta ghost" type="submit">Sign out</button>
        </form>
      </aside>
      <main class="dashboard-main">
        <section class="dashboard-hero">
          <span class="eyebrow">Live monitor</span>
          <h1>Arbitrage</h1>
          <form class="inline-form" method="get" action="/app/arbitrage">
            <label>Window minutes<input type="number" min="1" max="1440" name="minutes" value="{}"></label>
            <label>Recent limit<input type="number" min="10" max="200" name="limit" value="{}"></label>
            <button class="cta secondary" type="submit">Refresh</button>
          </form>
        </section>
        <section class="dashboard-grid">
          <div class="main-column">
            <section class="panel">
              <div class="panel-head"><h2>Recent candidates</h2><span class="badge">{}</span></div>
              <div class="panel-body">{}</div>
            </section>
            <section class="panel">
              <div class="panel-head"><h2>Hot routes</h2><span class="badge">{}</span></div>
              <div class="panel-body">{}</div>
            </section>
          </div>
          <div class="side-column">
            <section class="panel">
              <div class="panel-head"><h2>Hot pools</h2><span class="badge">{}</span></div>
              <div class="panel-body">{}</div>
            </section>
          </div>
        </section>
      </main>
    </div>
  </body>
</html>"#,
        shared_styles(),
        brand_wordmark_html(),
        escape_html(&user.email),
        state.default_chain_id,
        minutes,
        limit,
        recent.len(),
        render_table_or_empty(
            &recent_rows,
            "No arbitrage candidates in this window.",
            "<tr><th>Tx</th><th>Block</th><th>Time</th><th>Status</th><th>Sender</th><th>Entry</th><th>Swaps/Pools</th><th>Profit</th><th>Route</th></tr>",
        ),
        hot_routes.len(),
        render_table_or_empty(
            &route_rows,
            "No repeated routes in this window.",
            "<tr><th>Fingerprint</th><th>Route</th><th>Txs</th><th>Swaps</th><th>Senders</th><th>Last</th><th>Samples</th></tr>",
        ),
        hot_pools.len(),
        render_table_or_empty(
            &pool_rows,
            "No pool activity in this window.",
            "<tr><th>Pool</th><th>Txs</th><th>Swaps</th><th>Senders</th><th>Routes</th><th>Last</th><th>Samples</th></tr>",
        ),
    )
}

fn render_route_compact(route: &str, limit: usize) -> String {
    let pools = route
        .split('>')
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    if pools.is_empty() {
        return "n/a".to_owned();
    }
    let mut rendered = pools
        .iter()
        .take(limit)
        .map(|pool| render_copyable_code(pool, 5))
        .collect::<Vec<_>>();
    if pools.len() > limit {
        rendered.push(format!(
            "<span class=\"badge\">+{}</span>",
            pools.len().saturating_sub(limit)
        ));
    }
    rendered.join("<span class=\"route-arrow\">→</span>")
}

fn render_arbitrage_status(status: &str) -> String {
    let label = match status {
        "confirmed_stable_surplus_to_sender" => "confirmed stable",
        "candidate" => "candidate",
        other => other,
    };
    format!("<span class=\"badge\">{}</span>", escape_html(label))
}

fn render_arbitrage_profit(item: &ArbitrageRecentRow) -> String {
    match (
        item.profit_amount_raw.as_deref(),
        item.profit_token_address.as_deref(),
    ) {
        (Some(amount), Some(token)) => format!(
            "<span>{}</span> {}",
            escape_html(amount),
            render_copyable_code(token, 5)
        ),
        _ => "n/a".to_owned(),
    }
}

fn render_cluster_management_panel(
    default_chain_id: u64,
    editable_cluster: Option<&AddressCluster>,
) -> String {
    let is_editing = editable_cluster.is_some();
    let cluster_id = editable_cluster.map(|cluster| cluster.cluster_id.to_string());
    let chain_id = editable_cluster
        .map(|cluster| cluster.chain_id)
        .unwrap_or(default_chain_id);
    let owner_label = editable_cluster
        .map(|cluster| cluster.owner_label.as_str())
        .unwrap_or_default();
    let name = editable_cluster
        .map(|cluster| cluster.name.as_str())
        .unwrap_or_default();
    let description = editable_cluster
        .and_then(|cluster| cluster.description.as_deref())
        .unwrap_or_default();
    let addresses = editable_cluster
        .map(format_cluster_addresses)
        .unwrap_or_default();
    let action = cluster_id
        .as_ref()
        .map(|cluster_id| format!("/app/clusters/{cluster_id}/update"))
        .unwrap_or_else(|| "/app/clusters/create".to_owned());
    format!(
        r#"
<section class="panel">
  <div class="panel-head"><h2>{}</h2><span class="badge">management</span></div>
  <div class="panel-body">
    <p class="panel-copy">{}</p>
    <form class="stack-form" method="post" action="{}">
      <label>Chain ID<input type="number" name="chain_id" value="{}" required></label>
      <label>Owner label<input type="text" name="owner_label" value="{}" placeholder="ops_team" required></label>
      <label>Cluster name<input type="text" name="name" value="{}" placeholder="liquidation_operator" required></label>
      <label><span class="label-row">Description<span class="optional-icon" title="Optional field" aria-label="Optional field">i</span></span><input type="text" name="description" value="{}" placeholder="seed collector wallets; worker expands related addresses automatically"></label>
      <label>Seed addresses<textarea name="addresses" placeholder="0x111...,collector,collector&#10;0x222...,seed-2,collector" required>{}</textarea></label>
      <div class="field-hint">One seed per line. Format: <code>address,label,role</code>. For collector wallets you can paste only <code>0xabc...</code>; LedgerFlow marks it as a seed, the worker finds related addresses, stores suggestions, auto-adds high-confidence candidates, then runs analysis.</div>
      <div class="button-row">
        <button class="cta primary" type="submit">{}</button>
        {}
      </div>
    </form>
  </div>
</section>
"#,
        if is_editing {
            "Edit cluster"
        } else {
            "Create cluster"
        },
        if is_editing {
            "Update the cluster definition in place. Existing analyses stay in the database, while future tracking and backfill runs use the edited address set."
        } else {
            "Create a cluster from one or more seed addresses. As soon as creation succeeds, LedgerFlow queues worker onboarding: related address discovery, candidate classification, high-confidence auto-add, historical backfill, and report refresh."
        },
        action,
        chain_id,
        escape_html(owner_label),
        escape_html(name),
        escape_html(description),
        escape_html(&addresses),
        if is_editing {
            "Save cluster changes"
        } else {
            "Create cluster"
        },
        if is_editing {
            "<a class=\"cta ghost\" href=\"/app/clusters\">Cancel edit</a>".to_owned()
        } else {
            String::new()
        }
    )
}

fn render_address_analysis_panel(
    summary: Option<&AddressSummaryEnvelope>,
    default_chain_id: u64,
) -> String {
    let address = summary
        .map(|value| value.address.as_str())
        .unwrap_or_default();
    let chain_id = summary
        .map(|value| value.chain_id)
        .unwrap_or(default_chain_id);
    let body = summary
        .map(|summary| {
            let profile = summary
                .profile
                .as_ref()
                .map(|profile| {
                    let portfolio = profile
                        .metadata
                        .pointer("/portfolio_estimate/estimated_usd")
                        .and_then(serde_json::Value::as_f64)
                        .map(|estimated_usd| {
                            let classification = if profile
                                .metadata
                                .pointer("/portfolio_estimate/is_whale")
                                .and_then(serde_json::Value::as_bool)
                                .unwrap_or(false)
                            {
                                "whale"
                            } else {
                                "below threshold / partial"
                            };
                            format!(
                                r#"<div class="stat"><span>Observed portfolio value</span><strong>${:.2}</strong><small>{}</small></div>"#,
                                estimated_usd,
                                escape_html(classification)
                            )
                        })
                        .unwrap_or_default();
                    format!(
                        r#"<div class="stat"><span>Type</span><strong>{}</strong></div>
<div class="stat"><span>Code bytes</span><strong>{}</strong></div>{}"#,
                        escape_html(&profile.address_type),
                        profile.bytecode_length.unwrap_or_default(),
                        portfolio
                    )
                })
                .unwrap_or_else(|| {
                    "<div class=\"stat\"><span>Type</span><strong>pending</strong></div><div class=\"stat\"><span>Code bytes</span><strong>n/a</strong></div>".to_owned()
                });
            let labels = if summary.labels.is_empty() {
                "<span class=\"muted\">No labels yet. Worker has been queued if this address was not analyzed.</span>".to_owned()
            } else {
                summary
                    .labels
                    .iter()
                    .map(|label| {
                        format!(
                            r#"<span class="address-label-chip">{} · {}</span>"#,
                            escape_html(&label.label),
                            escape_html(&label.source)
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("")
            };
            let behavior_rows = if summary.behavior_rows.is_empty() {
                "<tr><td colspan=\"3\">No analyzed transactions for this address yet.</td></tr>".to_owned()
            } else {
                summary
                    .behavior_rows
                    .iter()
                    .map(|row| {
                        format!(
                            "<tr><td>{}</td><td>{}</td><td>{}</td></tr>",
                            escape_html(&row.behavior),
                            row.tx_count,
                            render_amount_stack(&format_wei_eth_raw(&row.gas_spent_raw), &row.gas_spent_raw)
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("")
            };
            let recent_rows = if summary.recent_txs.is_empty() {
                "<tr><td colspan=\"4\">No recent analyzed txs found.</td></tr>".to_owned()
            } else {
                summary
                    .recent_txs
                    .iter()
                    .map(|tx| {
                        format!(
                            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
                            render_copyable_code(&tx.tx_hash, 8),
                            escape_html(tx.block_timestamp.as_deref().unwrap_or("n/a")),
                            escape_html(&tx.behavior),
                            render_amount_stack(&format_wei_eth_raw(&tx.gas_raw), &tx.gas_raw)
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("")
            };
            let queued = if summary.queued {
                "<div class=\"notice info\">Address profile job is queued. Keep the address worker running and refresh later.</div>"
            } else {
                ""
            };
            format!(
                r#"{}
<div class="stats-grid">
  <div class="stat"><span>Address</span><strong>{}</strong></div>
  <div class="stat"><span>Native balance</span><strong>{}</strong></div>
  <div class="stat"><span>Analyzed txs</span><strong>{}</strong></div>
  <div class="stat"><span>Gas spent</span><strong>{}</strong></div>
  {}
</div>
<section class="mini-card"><strong>Known labels</strong><div>{}</div></section>
<div class="split-panels">
  <div class="table-shell"><table><thead><tr><th>Behavior</th><th>Tx count</th><th>Gas spent</th></tr></thead><tbody>{}</tbody></table></div>
  <div class="table-shell"><table><thead><tr><th>Tx</th><th>Time</th><th>Behavior</th><th>Gas</th></tr></thead><tbody>{}</tbody></table></div>
</div>"#,
                queued,
                render_copyable_code(&summary.address, 10),
                summary.native_balance_raw.as_deref().map(format_wei_eth_raw).unwrap_or_else(|| "n/a".to_owned()),
                summary.analyzed_tx_count,
                format_wei_eth_raw(&summary.gas_spent_raw),
                profile,
                labels,
                behavior_rows,
                recent_rows
            )
        })
        .unwrap_or_else(|| {
            "<div class=\"empty-state\">Enter an address to inspect known labels, native balance, previously analyzed txs, behavior breakdown, and gas spend.</div>".to_owned()
        });
    format!(
        r#"<section class="panel">
  <div class="panel-head"><h2>Address analysis</h2><span class="badge">address intelligence</span></div>
  <div class="panel-body">
    <p class="panel-copy">This page reads the accumulated address intelligence layer. If the address is new, it is queued for the background address worker.</p>
    <form class="stack-form" method="get" action="/app/address-analysis">
      <div class="split-fields">
        <label>Address<input type="text" name="address" value="{}" placeholder="0x..." required></label>
        <label>Chain ID<input type="number" name="chain_id" value="{}" min="1"></label>
      </div>
      <div class="button-row">
        <button class="cta secondary" type="submit">Analyze address</button>
      </div>
    </form>
    {}
  </div>
</section>"#,
        escape_html(address),
        chain_id,
        body
    )
}

fn summarize_primary_activity(analysis: &TransactionAnalysis) -> (String, String, String) {
    let mut best: Option<(&str, f32, &str)> = None;
    for result in &analysis.recognizer_results {
        let mapped = match result.label.as_str() {
            "liquidation_like_activity" => Some((
                "liquidation",
                "A liquidation-like recognizer fired on this tx.",
            )),
            "swap_like_activity" | "approval_then_swap_like" => Some((
                "swap",
                "The tx shows token-in / token-out swap-like behavior.",
            )),
            "erc20_approval_activity" => Some((
                "approve",
                "The tx granted ERC20 allowance to a spender contract.",
            )),
            "native_transfer_activity" => Some((
                "native transfer",
                "The tx primarily looks like native asset movement.",
            )),
            "erc20_transfer_activity" => Some((
                "token transfer",
                "The tx mainly looks like ERC20 transfer activity.",
            )),
            "batch_router_exec" => Some((
                "batch router",
                "The tx looks like a multi-step router or aggregator execution.",
            )),
            "arbitrage_like_activity" => Some((
                "arbitrage",
                "The tx looks like multi-leg token cycling or arbitrage-like activity.",
            )),
            "deposit_like_activity" => Some((
                "deposit",
                "The tx resembles depositing assets into a contract or position.",
            )),
            "withdraw_like_activity" => Some((
                "withdraw",
                "The tx resembles withdrawing assets from a contract or position.",
            )),
            "bridge_like_asset_exit" => Some((
                "bridge-like transfer",
                "The tx looks like an asset exit into a bridge or escrow-style destination.",
            )),
            "failed_execution" => Some((
                "failed execution",
                "The top-level transaction reverted or failed.",
            )),
            _ => None,
        };
        let Some((activity, reason)) = mapped else {
            continue;
        };
        let priority = activity_priority(activity);
        if best
            .as_ref()
            .map(|(best_activity, confidence, _)| {
                priority > activity_priority(best_activity)
                    || (priority == activity_priority(best_activity)
                        && result.confidence > *confidence)
            })
            .unwrap_or(true)
        {
            best = Some((activity, result.confidence, reason));
        }
    }

    if let Some((activity, confidence, reason)) = best {
        return (
            activity.to_owned(),
            reason.to_owned(),
            format!("{confidence:.2}"),
        );
    }

    if analysis
        .normalized_execution_facts
        .selector
        .as_deref()
        .is_some()
    {
        (
            "generic contract call".to_owned(),
            "The system found a selector, but no higher-level recognizer produced a narrow activity label.".to_owned(),
            "n/a".to_owned(),
        )
    } else {
        (
            "unknown".to_owned(),
            "The current recognizers could not confidently classify this transaction yet."
                .to_owned(),
            "n/a".to_owned(),
        )
    }
}

fn activity_priority(activity: &str) -> u8 {
    match activity {
        "failed execution" => 100,
        "liquidation" => 90,
        "arbitrage" => 80,
        "swap" => 70,
        "batch router" => 60,
        "deposit" | "withdraw" | "bridge-like transfer" => 50,
        "approve" => 30,
        "token transfer" | "native transfer" => 10,
        _ => 0,
    }
}

fn render_tx_analysis_result(payload: &serde_json::Value) -> String {
    render_tx_analysis_result_with_share_url(payload, true, None)
}

fn render_tx_analysis_result_with_share_url(
    payload: &serde_json::Value,
    show_share_action: bool,
    share_url: Option<&str>,
) -> String {
    let raw_json = serde_json::to_string_pretty(payload).unwrap_or_default();
    let Ok(envelope) = serde_json::from_value::<AnalysisEnvelope>(payload.clone()) else {
        return format!(
            "<details class=\"raw-json-shell\" open><summary>Raw analysis JSON</summary><pre class=\"json-shell\">{}</pre></details>",
            escape_html(&raw_json)
        );
    };
    let analysis = &envelope.analysis;
    let address_book = AddressDisplayBook::new(&envelope.address_intelligence);
    let share_action = if show_share_action {
        format!(
            r#"<form class="inline-action-form" method="post" action="/app/tx-analysis/share">
  <input type="hidden" name="chain_id" value="{}">
  <input type="hidden" name="tx_hash" value="{}">
  <button class="cta ghost tiny" type="submit">Create public share link</button>
</form>"#,
            analysis.chain_id,
            escape_html(&analysis.tx_hash)
        )
    } else {
        String::new()
    };
    let (primary_activity, primary_activity_reason, primary_confidence) =
        summarize_primary_activity(analysis);
    let storyline_panel = render_tx_storyline_panel(analysis, &address_book);
    let report_hero = render_tx_report_hero(
        analysis,
        &primary_activity,
        &primary_activity_reason,
        &primary_confidence,
        &share_action,
    );
    let meta_strip =
        render_tx_report_meta_strip(analysis, share_url.is_some(), &share_action, share_url);
    let gas_profile = render_gas_profile_panel(analysis);

    format!(
        r#"<div class="tx-analysis-shell">
  {}
  {}
  {}
  {}
  {}
</div>"#,
        meta_strip,
        report_hero,
        storyline_panel,
        gas_profile,
        render_storyline_gap_explanation(&analysis.scenario_outputs)
            .map(|message| format!(
                r#"<div class="notice info">{}</div>"#,
                escape_html(&message)
            ))
            .unwrap_or_default(),
    )
}

fn render_storyline_gap_explanation(scenario_outputs: &serde_json::Value) -> Option<String> {
    let storyline = scenario_outputs.get("storyline")?;
    let has_visible_lane = [
        "funding_sources",
        "repay_legs",
        "collateral_receipts",
        "disposition_legs",
        "cycle_legs",
        "exit_legs",
    ]
    .iter()
    .any(|key| {
        storyline
            .get(*key)
            .and_then(|value| value.as_array())
            .map(|items| !items.is_empty())
            .unwrap_or(false)
    });
    if has_visible_lane {
        return None;
    }
    Some(
        "LedgerFlow matched the protocol or method pattern, but the current flow reconstruction did not confidently attach concrete funding, repay, reward, or settlement legs yet. In practice this means the protocol match is ahead of the asset-leg attribution."
            .to_owned(),
    )
}

fn render_tx_report_meta_strip(
    analysis: &TransactionAnalysis,
    public_share: bool,
    share_action: &str,
    share_url: Option<&str>,
) -> String {
    let network = match analysis.chain_id {
        1 => "Ethereum",
        8453 => "Base",
        10 => "Optimism",
        42161 => "Arbitrum",
        _ => "EVM chain",
    };
    let block = analysis
        .block_number
        .map(|value| value.to_string())
        .unwrap_or_else(|| "n/a".to_owned());
    let timestamp = analysis
        .block_timestamp
        .map(|value| value.to_rfc3339().replace('T', " ").replace('Z', " UTC"))
        .unwrap_or_else(|| "n/a".to_owned());
    let facts = &analysis.normalized_execution_facts.derived_facts;
    let nonce = facts
        .get("nonce")
        .and_then(|value| value.as_u64())
        .map(|value| value.to_string());
    let tx_index = facts
        .get("tx_index")
        .and_then(|value| value.as_u64())
        .map(|value| value.to_string());
    let value_raw = facts
        .get("tx_value_raw")
        .and_then(|value| value.as_str())
        .unwrap_or(&analysis.value);
    let gas_used = facts
        .get("gas_used_raw")
        .and_then(|value| value.as_str())
        .filter(|value| !value.is_empty() && *value != "0");
    let gas_price = facts
        .get("effective_gas_price_raw")
        .and_then(|value| value.as_str())
        .filter(|value| !value.is_empty() && *value != "0")
        .map(format_gwei_from_wei);
    let tx_fee = facts
        .get("gas_cost_raw")
        .and_then(|value| value.as_str())
        .filter(|value| !value.is_empty() && *value != "0")
        .map(format_wei_eth_raw);
    let mut basic_fields = vec![
        render_basic_info_item(
            "Status",
            &format!(
                r#"<strong class="status-chip status-{}">{}</strong>"#,
                if analysis.status == crate::domain::enums::TransactionStatus::Success {
                    "success"
                } else {
                    "failure"
                },
                escape_html(&enum_label(&analysis.status))
            ),
        ),
        render_basic_info_item(
            "Network",
            &format!("<strong>{}</strong>", escape_html(network)),
        ),
        render_basic_info_item(
            "Block",
            &format!("<strong>{}</strong>", escape_html(&block)),
        ),
        render_basic_info_item(
            "Timestamp",
            &format!("<strong>{}</strong>", escape_html(&timestamp)),
        ),
        render_basic_info_item(
            "Value",
            &format!(
                "<strong>{}</strong>",
                escape_html(&format_wei_compact_decimal(value_raw))
            ),
        ),
        render_basic_info_item(
            "Sender",
            &format!(
                "<strong>{}</strong>",
                render_copyable_code(&analysis.from_address, 12)
            ),
        ),
        render_basic_info_item(
            "Receiver",
            &format!(
                "<strong>{}</strong>",
                analysis
                    .to_address
                    .as_deref()
                    .map(|value| render_copyable_code(value, 12))
                    .unwrap_or_else(|| "contract creation".to_owned())
            ),
        ),
    ];
    if let Some(nonce) = nonce {
        basic_fields.insert(
            4,
            render_basic_info_item(
                "Nonce",
                &format!("<strong>{}</strong>", escape_html(&nonce)),
            ),
        );
    }
    if let Some(tx_index) = tx_index {
        basic_fields.insert(
            4,
            render_basic_info_item(
                "Position in Block",
                &format!("<strong>{}</strong>", escape_html(&tx_index)),
            ),
        );
    }
    let mut other_fields = Vec::new();
    if let Some(gas_used) = gas_used {
        other_fields.push(render_basic_info_item(
            "Gas Used",
            &format!("<strong>{}</strong>", escape_html(gas_used)),
        ));
    }
    if let Some(gas_price) = gas_price.as_deref() {
        other_fields.push(render_basic_info_item(
            "Gas Price",
            &format!("<strong>{}</strong>", escape_html(gas_price)),
        ));
    }
    if let Some(tx_fee) = tx_fee.as_deref() {
        other_fields.push(render_basic_info_item(
            "Transaction Fee",
            &format!("<strong>{}</strong>", escape_html(tx_fee)),
        ));
    }
    other_fields.push(render_basic_info_item(
        "Event Count",
        &format!(
            "<strong>{}</strong>",
            analysis.normalized_execution_facts.event_count
        ),
    ));
    other_fields.push(render_basic_info_item(
        "Internal Call Count",
        &format!(
            "<strong>{}</strong>",
            analysis.normalized_execution_facts.internal_call_count
        ),
    ));
    other_fields.push(render_basic_info_item(
        "Input Data",
        &format!(
            "<strong>{}</strong>",
            render_copyable_code(&analysis.input_data, 14)
        ),
    ));
    let share_top_action = if let Some(url) = share_url {
        format!(
            r#"<div class="tx-report-share-action">{}</div>"#,
            render_copyable_code(url, 16)
        )
    } else if share_action.is_empty() {
        String::new()
    } else {
        format!(
            r#"<div class="tx-report-share-action">{}</div>"#,
            share_action
        )
    };
    format!(
        r#"<section class="tx-report-top tx-basic-info-panel">
  <div class="tx-report-heading">
    <span class="brand-lockup mini"><span class="brand-mark">{}</span><span>On-chain Transaction Evidence</span></span>
    <div class="tx-report-actions">
      <span class="readonly-pill">{}</span>
      {}
    </div>
  </div>
  <div class="tx-report-title-row">
    <div>
      <h1>Transaction Analysis Report</h1>
      <p>Tx Analysis Report</p>
      <span>Tx Hash</span>
      {}
    </div>
  </div>
  <div class="basic-info-card">
    <h2>Basic Information</h2>
    <div class="basic-info-grid">
      {}
    </div>
    <details class="basic-info-more">
      <summary>Other Information</summary>
      <div class="basic-info-grid">
        {}
      </div>
    </details>
  </div>
</section>"#,
        brand_mark_svg_source(),
        if public_share {
            "Public share · Read only"
        } else {
            "Analysis result"
        },
        share_top_action,
        render_copyable_code(&analysis.tx_hash, 16),
        basic_fields.join(""),
        other_fields.join(""),
    )
}

fn render_basic_info_item(label: &str, value_html: &str) -> String {
    format!(
        r#"<div><span>{}</span>{}</div>"#,
        escape_html(label),
        value_html,
    )
}

fn render_gas_profile_panel(analysis: &TransactionAnalysis) -> String {
    let facts = &analysis.normalized_execution_facts.derived_facts;
    let gas_used = facts
        .get("gas_used_raw")
        .and_then(|value| value.as_str())
        .unwrap_or("0");
    let gas_price = facts
        .get("effective_gas_price_raw")
        .and_then(|value| value.as_str())
        .map(format_gwei_from_wei)
        .unwrap_or_else(|| "n/a".to_owned());
    let tx_fee = facts
        .get("gas_cost_raw")
        .and_then(|value| value.as_str())
        .map(format_wei_eth_raw)
        .unwrap_or_else(|| "n/a".to_owned());
    let trace_note = if analysis
        .normalized_execution_facts
        .derived_facts
        .get("trace_available")
        .and_then(|value| value.as_bool())
        .unwrap_or(false)
    {
        "Current trace payload does not include per-call gasUsed, so this is receipt-level gas only."
    } else {
        "Trace is unavailable, so this is receipt-level gas only."
    };
    format!(
        r#"<details class="tx-section-card gas-profile-panel" open>
  <summary class="tx-section-title">
    <span class="tx-section-index">5</span>
    <h2>Gas Profile</h2>
    <span class="badge">receipt-level</span>
  </summary>
  <div class="gas-profile-summary">
    <div><span>Total Gas Used</span><strong>{}</strong></div>
    <div><span>Effective Gas Price</span><strong>{}</strong></div>
    <div><span>Transaction Fee</span><strong>{}</strong></div>
    <div><span>Internal Calls</span><strong>{}</strong></div>
  </div>
  <div class="gas-profile-bar">
    <span style="width: 100%">Gas used · {}</span>
  </div>
  <p class="panel-copy">{}</p>
</details>"#,
        escape_html(gas_used),
        escape_html(&gas_price),
        escape_html(&tx_fee),
        analysis.normalized_execution_facts.internal_call_count,
        escape_html(gas_used),
        escape_html(trace_note),
    )
}

fn tx_activity_icon(kind: &str) -> &'static str {
    match kind {
        "liquidation" => "⚡",
        "arbitrage" => "⇄",
        "swap" => "⟲",
        "approve" => "✓",
        "native transfer" | "token transfer" => "→",
        _ => "◇",
    }
}

struct TxReportVerdict {
    tone: &'static str,
    label: String,
    metric_label: String,
    result: String,
    result_caption: String,
}

fn render_tx_report_hero(
    analysis: &TransactionAnalysis,
    primary_activity: &str,
    _primary_activity_reason: &str,
    primary_confidence: &str,
    _share_action: &str,
) -> String {
    let storyline = analysis.scenario_outputs.get("storyline");
    let kind = storyline
        .and_then(|value| value.get("kind"))
        .and_then(|value| value.as_str())
        .unwrap_or(primary_activity);
    let verdict = build_tx_report_verdict(storyline, analysis);

    format!(
        r#"<details class="tx-section-card tx-report-hero tx-report-{}" open>
  <summary class="tx-section-title">
    <span class="tx-section-index">1</span>
    <h2>Tx Conclusion</h2>
    <span class="confidence-pill">{} confidence</span>
  </summary>
  <div class="tx-conclusion-grid">
    <div class="tx-conclusion-icon">{}</div>
    <div class="tx-conclusion-cell">
      <span>Activity</span>
      <strong class="activity-pill">{}</strong>
    </div>
    <div class="tx-conclusion-cell">
      <span>Verdict</span>
      <strong class="verdict-pill verdict-{}">{}</strong>
    </div>
    <div class="tx-conclusion-cell">
      <span>{}</span>
      <strong class="profit-number">{}</strong>
      <small>{}</small>
    </div>
  </div>
</details>"#,
        verdict.tone,
        escape_html(primary_confidence),
        tx_activity_icon(kind),
        escape_html(kind),
        verdict.tone,
        escape_html(&verdict.label),
        escape_html(&verdict.metric_label),
        escape_html(&verdict.result),
        escape_html(&verdict.result_caption),
    )
}

fn build_tx_report_verdict(
    storyline: Option<&serde_json::Value>,
    analysis: &TransactionAnalysis,
) -> TxReportVerdict {
    let Some(net_effect) = storyline
        .and_then(|value| value.get("net_effect"))
        .filter(|value| !value.is_null())
    else {
        return TxReportVerdict {
            tone: "unknown",
            label: "No clear profit view yet".to_owned(),
            metric_label: "PnL view".to_owned(),
            result: "PnL unavailable".to_owned(),
            result_caption: "missing net effect".to_owned(),
        };
    };

    if let Some(verdict) = build_subject_value_verdict(net_effect, analysis) {
        return verdict;
    }

    let mut eth_equivalent_raw = 0i128;
    let mut positive_assets = 0usize;
    let mut negative_assets = 0usize;
    let mut positive_non_eth_assets = Vec::new();
    let mut negative_non_eth_assets = Vec::new();
    if let Some(rows) = net_effect.get("rows").and_then(|value| value.as_array()) {
        for row in rows {
            let net_raw = row
                .get("net_raw")
                .and_then(|value| value.as_str())
                .and_then(|value| value.parse::<i128>().ok())
                .unwrap_or(0);
            let is_eth_equivalent = token_is_eth_equivalent(row.get("token"));
            if net_raw > 0 {
                positive_assets += 1;
                if !is_eth_equivalent {
                    positive_non_eth_assets.push(describe_net_effect_row(row));
                }
            } else if net_raw < 0 {
                negative_assets += 1;
                if !is_eth_equivalent {
                    negative_non_eth_assets.push(describe_net_effect_row(row));
                }
            }
            if is_eth_equivalent {
                eth_equivalent_raw += net_raw;
            }
        }
    }
    if let Some(parsed_gas_raw) = net_effect
        .get("gas")
        .and_then(|value| value.get("amount_raw"))
        .and_then(|value| value.as_str())
        .and_then(|value| value.parse::<i128>().ok())
    {
        eth_equivalent_raw -= parsed_gas_raw;
    }

    if eth_equivalent_raw < 0 && !positive_non_eth_assets.is_empty() {
        return TxReportVerdict {
            tone: "mixed",
            label: "Asset acquired, PnL unresolved".to_owned(),
            metric_label: "Cash-flow cost".to_owned(),
            result: format_wei_eth(eth_equivalent_raw),
            result_caption: "includes gas".to_owned(),
        };
    }

    if eth_equivalent_raw > 0 {
        TxReportVerdict {
            tone: "profit",
            label: "Likely profitable transaction".to_owned(),
            metric_label: "Core PnL".to_owned(),
            result: format_wei_eth(eth_equivalent_raw),
            result_caption: "after gas".to_owned(),
        }
    } else if eth_equivalent_raw < 0 {
        TxReportVerdict {
            tone: "loss",
            label: "Likely loss after gas".to_owned(),
            metric_label: "Core PnL".to_owned(),
            result: format_wei_eth(eth_equivalent_raw),
            result_caption: "after gas".to_owned(),
        }
    } else if positive_assets > 0 && negative_assets > 0 {
        TxReportVerdict {
            tone: "mixed",
            label: "Mixed asset result".to_owned(),
            metric_label: "PnL view".to_owned(),
            result: "Multi-asset net".to_owned(),
            result_caption: "pricing required".to_owned(),
        }
    } else if positive_assets > 0 {
        TxReportVerdict {
            tone: "profit",
            label: "Net assets gained".to_owned(),
            metric_label: "Net asset change".to_owned(),
            result: "Positive token net".to_owned(),
            result_caption: positive_non_eth_assets
                .first()
                .cloned()
                .unwrap_or_else(|| "token net".to_owned()),
        }
    } else if negative_assets > 0 {
        TxReportVerdict {
            tone: "loss",
            label: "Net assets spent".to_owned(),
            metric_label: "Net asset change".to_owned(),
            result: "Negative token net".to_owned(),
            result_caption: negative_non_eth_assets
                .first()
                .cloned()
                .unwrap_or_else(|| "token net".to_owned()),
        }
    } else {
        TxReportVerdict {
            tone: "unknown",
            label: "Break-even or no external asset change".to_owned(),
            metric_label: "PnL view".to_owned(),
            result: "0 net detected".to_owned(),
            result_caption: "no external delta".to_owned(),
        }
    }
}

fn build_subject_value_verdict(
    net_effect: &serde_json::Value,
    analysis: &TransactionAnalysis,
) -> Option<TxReportVerdict> {
    let mut total_usd = 0.0f64;
    let mut priced_rows = 0usize;
    let mut missing_prices = Vec::new();

    if let Some(rows) = net_effect.get("rows").and_then(|value| value.as_array()) {
        for row in rows {
            let net_raw = row
                .get("net_raw")
                .and_then(|value| value.as_str())
                .unwrap_or("0");
            if raw_is_zero(net_raw) {
                continue;
            }
            let token = row.get("token").unwrap_or(&serde_json::Value::Null);
            if let Some(value) = usd_value_for_token_amount(net_raw, token, analysis) {
                total_usd += value;
                priced_rows += 1;
            } else {
                missing_prices.push(
                    token
                        .get("symbol")
                        .and_then(|value| value.as_str())
                        .unwrap_or("asset")
                        .to_owned(),
                );
            }
        }
    }

    if let Some(gas) = net_effect.get("gas").filter(|value| !value.is_null()) {
        let amount_raw = gas
            .get("amount_raw")
            .and_then(|value| value.as_str())
            .unwrap_or("0");
        if !raw_is_zero(amount_raw) {
            let token = gas.get("token").unwrap_or(&serde_json::Value::Null);
            if let Some(value) = usd_value_for_token_amount(amount_raw, token, analysis) {
                total_usd -= value;
                priced_rows += 1;
            } else {
                missing_prices.push("gas".to_owned());
            }
        }
    }

    if priced_rows == 0 {
        return None;
    }

    let complete = missing_prices.is_empty();
    let tone = if total_usd > 0.000001 {
        "profit"
    } else if total_usd < -0.000001 {
        "loss"
    } else if complete {
        "unknown"
    } else {
        "mixed"
    };
    let label = if !complete {
        "Partial subject value change".to_owned()
    } else if total_usd > 0.000001 {
        "Subject gained value".to_owned()
    } else if total_usd < -0.000001 {
        "Subject lost value".to_owned()
    } else {
        "Subject roughly break-even".to_owned()
    };
    let result_caption = if complete {
        "includes gas when priced".to_owned()
    } else {
        format!(
            "missing prices: {}",
            missing_prices
                .into_iter()
                .take(3)
                .collect::<Vec<_>>()
                .join(", ")
        )
    };

    Some(TxReportVerdict {
        tone,
        label,
        metric_label: "Subject net value".to_owned(),
        result: format_signed_usd(total_usd),
        result_caption,
    })
}

fn describe_net_effect_row(row: &serde_json::Value) -> String {
    row.get("net_display")
        .and_then(|value| value.as_str())
        .map(str::to_owned)
        .unwrap_or_else(|| {
            let symbol = row
                .get("token")
                .and_then(|value| value.get("symbol"))
                .and_then(|value| value.as_str())
                .unwrap_or("asset");
            let raw = row
                .get("net_raw")
                .and_then(|value| value.as_str())
                .unwrap_or("0");
            format!("{raw} raw {symbol}")
        })
}

fn raw_is_zero(raw: &str) -> bool {
    raw.parse::<f64>()
        .map(|value| value == 0.0)
        .unwrap_or(false)
}

fn token_is_eth_equivalent(token: Option<&serde_json::Value>) -> bool {
    let Some(token) = token else {
        return false;
    };
    let symbol = token
        .get("symbol")
        .and_then(|value| value.as_str())
        .unwrap_or("");
    let address = token
        .get("address")
        .and_then(|value| value.as_str())
        .unwrap_or("");
    symbol.eq_ignore_ascii_case("ETH")
        || symbol.eq_ignore_ascii_case("WETH")
        || address.eq_ignore_ascii_case("0x4200000000000000000000000000000000000006")
        || address.eq_ignore_ascii_case("native")
}

fn format_wei_eth(raw: i128) -> String {
    let sign = if raw > 0 {
        "+"
    } else if raw < 0 {
        "-"
    } else {
        ""
    };
    let abs = raw.abs();
    let whole = abs / 1_000_000_000_000_000_000i128;
    let fractional = (abs % 1_000_000_000_000_000_000i128) / 1_000_000_000_000i128;
    format!("{sign}{whole}.{fractional:06} ETH")
}

fn format_wei_eth_raw(raw: &str) -> String {
    raw.parse::<i128>()
        .map(format_wei_eth)
        .unwrap_or_else(|_| "n/a".to_owned())
}

fn format_wei_compact_decimal(raw: &str) -> String {
    let value = parse_u128_decimal_or_hex(raw).unwrap_or_default();
    if value == 0 {
        return "0 wei".to_owned();
    }
    format!("{} ETH", format_unsigned_units_compact(value as i128, 18))
}

fn format_gwei_from_wei(raw: &str) -> String {
    let value = parse_u128_decimal_or_hex(raw).unwrap_or_default();
    if value == 0 {
        return "0 Gwei".to_owned();
    }
    format!("{} Gwei", format_unsigned_units_compact(value as i128, 9))
}

fn parse_u128_decimal_or_hex(raw: &str) -> Option<u128> {
    if let Some(hex) = raw.strip_prefix("0x") {
        u128::from_str_radix(hex, 16).ok()
    } else {
        raw.parse::<u128>().ok()
    }
}

fn tx_form_prefill(payload: Option<&serde_json::Value>, default_chain_id: u64) -> (String, String) {
    let Some(payload) = payload else {
        return (String::new(), default_chain_id.to_string());
    };
    let Ok(envelope) = serde_json::from_value::<AnalysisEnvelope>(payload.clone()) else {
        return (String::new(), default_chain_id.to_string());
    };
    (
        envelope.analysis.tx_hash,
        envelope.analysis.chain_id.to_string(),
    )
}

struct AddressDisplayBook {
    profiles: std::collections::BTreeMap<String, AddressProfile>,
    labels: std::collections::BTreeMap<String, Vec<AddressLabel>>,
}

impl AddressDisplayBook {
    fn new(intelligence: &AddressIntelligenceEnvelope) -> Self {
        let profiles = intelligence
            .profiles
            .iter()
            .map(|profile| (profile.address.to_ascii_lowercase(), profile.clone()))
            .collect::<std::collections::BTreeMap<_, _>>();
        let mut labels = std::collections::BTreeMap::<String, Vec<AddressLabel>>::new();
        for label in &intelligence.labels {
            labels
                .entry(label.address.to_ascii_lowercase())
                .or_default()
                .push(label.clone());
        }
        Self { profiles, labels }
    }

    fn best_label(&self, address: &str) -> Option<String> {
        let address = address.to_ascii_lowercase();
        self.labels
            .get(&address)
            .and_then(|labels| {
                labels.iter().max_by(|left, right| {
                    left.confidence
                        .partial_cmp(&right.confidence)
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
            })
            .map(|label| label.label.replace('_', " "))
            .or_else(|| {
                self.profiles
                    .get(&address)
                    .map(|profile| profile.address_type.clone())
            })
    }
}

fn render_known_address(address: &str, keep: usize, book: &AddressDisplayBook) -> String {
    let code = render_copyable_code(address, keep);
    book.best_label(address)
        .map(|label| {
            format!(
                r#"<span class="known-address">{}<small>{}</small></span>"#,
                code,
                escape_html(&label)
            )
        })
        .unwrap_or(code)
}

fn render_tx_storyline_panel(analysis: &TransactionAnalysis, book: &AddressDisplayBook) -> String {
    let scenario_outputs = &analysis.scenario_outputs;
    let Some(storyline) = scenario_outputs.get("storyline") else {
        let executor = analysis.to_address.as_deref().unwrap_or("n/a");
        return format!(
            "{}{}",
            render_execution_timeline_panel(analysis, &analysis.from_address, executor, None, book),
            render_flow_fallback_panel(
                analysis,
                2,
                "No complete transaction intent was recognized yet. Showing confirmed asset flows first.",
                book,
            )
        );
    };
    let kind = storyline
        .get("kind")
        .and_then(|value| value.as_str())
        .unwrap_or("unknown");
    let protocol = storyline
        .get("protocol")
        .and_then(|value| value.as_str())
        .unwrap_or("n/a");
    let protocol_contract = storyline
        .get("protocol_contract")
        .and_then(|value| value.as_str());
    let executor = storyline
        .get("executor_address")
        .and_then(|value| value.as_str())
        .unwrap_or("n/a");
    let initiator = storyline
        .get("initiator_address")
        .and_then(|value| value.as_str())
        .unwrap_or("n/a");
    let subject_scope = render_subject_scope(storyline.get("subject_scope"));
    let subject_scope_label = storyline
        .get("subject_scope")
        .and_then(|value| value.get("label"))
        .and_then(|value| value.as_str())
        .unwrap_or("subject scope");
    let net_effect =
        render_net_effect_panel(storyline.get("net_effect"), subject_scope_label, analysis);
    let execution_timeline =
        render_execution_timeline_panel(analysis, initiator, executor, protocol_contract, book);
    let step_timeline = if !execution_timeline.is_empty() {
        execution_timeline
    } else {
        let storyline_steps = render_storyline_steps(
            storyline.get("steps"),
            analysis,
            initiator,
            executor,
            protocol_contract,
            book,
        );
        if storyline_steps.is_empty() {
            render_generic_timeline_panel(analysis, initiator, executor, protocol_contract, book)
        } else {
            storyline_steps
        }
    };
    let storyline_visual =
        render_storyline_visual(storyline, kind, protocol, initiator, executor, book);
    let flow_fallback = if !analysis.derived_flows.is_empty() {
        render_flow_fallback_panel(
            analysis,
            3,
            "Protocol intent attribution is incomplete. Below are asset flows reconstructed from logs and value transfers.",
            book,
        )
    } else {
        String::new()
    };

    format!(
        r#"{}
{}
{}
{}
{}"#,
        subject_scope, net_effect, step_timeline, flow_fallback, storyline_visual,
    )
}

fn render_flow_fallback_panel(
    analysis: &TransactionAnalysis,
    section_index: u32,
    explanation: &str,
    book: &AddressDisplayBook,
) -> String {
    let rows = render_flow_balance_rows(analysis, book);
    let rows = if rows.is_empty() {
        "<tr><td colspan=\"3\">No asset flow could be reconstructed from the available receipt / execution evidence.</td></tr>"
            .to_owned()
    } else {
        rows
    };
    format!(
        r#"<details class="tx-section-card compact-section flow-fallback-panel" open>
  <summary class="tx-section-title">
    <span class="tx-section-index">{}</span>
    <h2>Confirmed Asset Flows</h2>
    <span class="badge">fallback view</span>
  </summary>
  <p class="panel-copy">{} Tx initiator: {}</p>
  <div class="table-shell compact-table-shell">
    <table class="compact-table">
      <thead>
        <tr>
          <th>Address</th>
          <th>Token</th>
          <th>Net balance change</th>
        </tr>
      </thead>
      <tbody>{}</tbody>
    </table>
  </div>
</details>"#,
        section_index,
        escape_html(explanation),
        render_known_address(&analysis.from_address, 10, book),
        rows
    )
}

fn render_flow_balance_rows(analysis: &TransactionAnalysis, book: &AddressDisplayBook) -> String {
    let mut rows = std::collections::BTreeMap::<(String, String), (serde_json::Value, i128)>::new();
    for flow in &analysis.derived_flows {
        let amount = flow.amount_raw.parse::<i128>().unwrap_or_default();
        let token = flow_token_descriptor(flow);
        let token_key = token
            .get("address")
            .and_then(|value| value.as_str())
            .or(flow.token_address.as_deref())
            .unwrap_or("native")
            .to_ascii_lowercase();
        let from_key = (flow.from_address.to_ascii_lowercase(), token_key.clone());
        let to_key = (flow.to_address.to_ascii_lowercase(), token_key);
        rows.entry(from_key)
            .and_modify(|(_, value)| *value -= amount)
            .or_insert((token.clone(), -amount));
        rows.entry(to_key)
            .and_modify(|(_, value)| *value += amount)
            .or_insert((token, amount));
    }
    if let Some(gas_raw) = analysis
        .normalized_execution_facts
        .derived_facts
        .get("gas_cost_raw")
        .and_then(|value| value.as_str())
        .and_then(|value| value.parse::<i128>().ok())
        .filter(|value| *value > 0)
    {
        let token = serde_json::json!({
            "symbol": "ETH",
            "name": "Ether",
            "address": "native",
            "decimals": 18,
        });
        rows.entry((
            analysis.from_address.to_ascii_lowercase(),
            "native".to_owned(),
        ))
        .and_modify(|(_, value)| *value -= gas_raw)
        .or_insert((token, -gas_raw));
    }

    let mut grouped_rows =
        std::collections::BTreeMap::<String, Vec<(serde_json::Value, i128)>>::new();
    for ((address, _), (token, amount)) in rows.into_iter().filter(|(_, (_, amount))| *amount != 0)
    {
        grouped_rows
            .entry(address)
            .or_default()
            .push((token, amount));
    }

    grouped_rows
        .into_iter()
        .take(80)
        .flat_map(|(address, mut items)| {
            items.sort_by(|(left_token, _), (right_token, _)| {
                token_sort_key(left_token).cmp(&token_sort_key(right_token))
            });
            let rowspan = items.len();
            items
                .into_iter()
                .enumerate()
                .map(move |(index, (token, amount))| {
                    render_flow_balance_row(
                        if index == 0 {
                            Some(address.as_str())
                        } else {
                            None
                        },
                        rowspan,
                        &token,
                        amount,
                        analysis,
                        book,
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>()
        .join("")
}

fn render_flow_balance_row(
    address: Option<&str>,
    rowspan: usize,
    token: &serde_json::Value,
    amount: i128,
    analysis: &TransactionAnalysis,
    book: &AddressDisplayBook,
) -> String {
    let symbol = token
        .get("symbol")
        .and_then(|value| value.as_str())
        .unwrap_or("asset");
    let name = token
        .get("name")
        .and_then(|value| value.as_str())
        .unwrap_or("unlabeled");
    let token_address = token
        .get("address")
        .and_then(|value| value.as_str())
        .unwrap_or("native");
    let amount_display = format_token_amount_display(amount, token);
    let estimated_usd =
        estimated_usd_for_token_amount(&amount.to_string(), token, analysis).unwrap_or_default();
    let amount_class = if amount > 0 { "gained" } else { "spent" };
    let address_cell = address
        .map(|address| {
            format!(
                r#"<td class="flow-address-cell" rowspan="{}">{}</td>"#,
                rowspan,
                render_known_address(address, 10, book)
            )
        })
        .unwrap_or_default();
    format!(
        "<tr>{}<td><strong>{}</strong><small>{} · {}</small></td><td><span class=\"table-badge {}\">{}</span><small>raw: {}</small>{}</td></tr>",
        address_cell,
        escape_html(symbol),
        escape_html(name),
        render_copyable_code(token_address, 8),
        amount_class,
        escape_html(&amount_display),
        escape_html(&amount.to_string()),
        if estimated_usd.is_empty() {
            String::new()
        } else {
            format!(
                "<small class=\"estimated-usd\">{}</small>",
                escape_html(&estimated_usd)
            )
        },
    )
}

fn token_sort_key(token: &serde_json::Value) -> String {
    format!(
        "{}:{}",
        token
            .get("symbol")
            .and_then(|value| value.as_str())
            .unwrap_or("asset"),
        token
            .get("address")
            .and_then(|value| value.as_str())
            .unwrap_or("native")
    )
}

fn flow_token_descriptor(flow: &crate::domain::value_objects::AssetFlow) -> serde_json::Value {
    if let Some(token) = flow.metadata.get("token").filter(|value| value.is_object()) {
        return token.clone();
    }
    match flow.asset_type {
        crate::domain::enums::AssetType::Native => serde_json::json!({
            "symbol": "ETH",
            "name": "Ether",
            "address": "native",
            "decimals": 18,
        }),
        crate::domain::enums::AssetType::Erc20 => {
            let address = flow.token_address.as_deref().unwrap_or_default();
            let (symbol, name, decimals) = match address.to_ascii_lowercase().as_str() {
                "0x4200000000000000000000000000000000000006" => ("WETH", "Wrapped Ether", Some(18)),
                "0x091a5abe6616e26268e5eecff256c2212fce2707" => ("BondETH", "Bond ETH", Some(18)),
                _ => ("ERC20", "Unlabeled token", None),
            };
            serde_json::json!({
                "symbol": symbol,
                "name": name,
                "address": address,
                "decimals": decimals,
            })
        }
    }
}

fn format_flow_amount_display(
    flow: &crate::domain::value_objects::AssetFlow,
    token: &serde_json::Value,
) -> String {
    let raw = flow.amount_raw.parse::<i128>().ok();
    let symbol = token
        .get("symbol")
        .and_then(|value| value.as_str())
        .unwrap_or("asset");
    let decimals = token
        .get("decimals")
        .and_then(|value| value.as_u64())
        .map(|value| value as u32);
    match (raw, decimals) {
        (Some(raw), Some(decimals)) => {
            format!(
                "{} {}",
                format_unsigned_units_compact(raw, decimals),
                symbol
            )
        }
        _ => format!("{} raw units", flow.amount_raw),
    }
}

fn format_token_amount_display(raw: i128, token: &serde_json::Value) -> String {
    let symbol = token
        .get("symbol")
        .and_then(|value| value.as_str())
        .unwrap_or("asset");
    let decimals = token
        .get("decimals")
        .and_then(|value| value.as_u64())
        .map(|value| value as u32);
    let prefix = if raw > 0 { "+" } else { "" };
    match decimals {
        Some(decimals) => format!(
            "{}{} {}",
            prefix,
            format_unsigned_units_compact(raw, decimals),
            symbol
        ),
        None => format!("{}{} raw units", prefix, raw),
    }
}

fn format_unsigned_units_compact(raw: i128, decimals: u32) -> String {
    let sign = if raw < 0 { "-" } else { "" };
    let abs = raw.abs();
    let scale = 10i128.saturating_pow(decimals);
    if scale == 0 {
        return format!("{sign}{abs}");
    }
    let whole = abs / scale;
    let fraction = abs % scale;
    if decimals == 0 || fraction == 0 {
        return format!("{sign}{whole}");
    }
    let mut fraction_text = format!("{fraction:0width$}", width = decimals as usize);
    fraction_text.truncate(6.min(fraction_text.len()));
    while fraction_text.ends_with('0') {
        fraction_text.pop();
    }
    if fraction_text.is_empty() {
        format!("{sign}{whole}")
    } else {
        format!("{sign}{whole}.{fraction_text}")
    }
}

fn render_subject_scope(value: Option<&serde_json::Value>) -> String {
    let Some(scope) = value else {
        return String::new();
    };
    let label = scope
        .get("label")
        .and_then(|value| value.as_str())
        .unwrap_or("subject scope");
    let addresses = scope
        .get("addresses")
        .and_then(|value| value.as_array())
        .map(|items| {
            items
                .iter()
                .filter_map(|value| value.as_str())
                .map(|value| render_copyable_code(value, 10))
                .collect::<Vec<_>>()
                .join("")
        })
        .unwrap_or_default();
    format!(
        r#"<details class="tx-section-card tx-subject-panel" open>
  <summary class="tx-section-title">
    <span class="tx-section-index">2</span>
    <h2>Subject Perspective</h2>
  </summary>
  <div class="subject-scope-line"><span>Subject scope</span><div class="address-pill-row">{}</div><small>{}</small></div>
</details>"#,
        addresses,
        escape_html(label),
    )
}

fn render_net_effect_panel(
    value: Option<&serde_json::Value>,
    subject_scope_label: &str,
    analysis: &TransactionAnalysis,
) -> String {
    let Some(effect) = value else {
        return String::new();
    };
    let rows = render_net_effect_rows(effect, analysis);
    format!(
        r#"<details class="storyline-net-effect tx-section-card compact-section" open>
  <summary class="tx-section-title"><h2>Net Asset Change</h2><span class="badge">subject scope view</span></summary>
  <div class="panel-copy">Calculated across <strong>{}</strong>. <strong>In</strong> means assets entered the subject scope, <strong>Out</strong> means assets left it, and <strong>Net</strong> is <strong>In - Out</strong>.</div>
  <div class="table-shell compact-table-shell">
    <table class="compact-table">
      <thead>
        <tr>
          <th>Symbol</th>
          <th>In</th>
          <th>Out</th>
          <th>Net</th>
          <th>Token</th>
          <th>Address</th>
        </tr>
      </thead>
      <tbody>{}</tbody>
    </table>
  </div>
</details>"#,
        escape_html(subject_scope_label),
        rows
    )
}

fn render_net_effect_rows(effect: &serde_json::Value, analysis: &TransactionAnalysis) -> String {
    let mut rows = effect
        .get("rows")
        .and_then(|value| value.as_array())
        .map(|items| {
            items
                .iter()
                .map(|item| render_net_effect_asset_row(item, analysis))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if let Some(gas) = effect.get("gas").filter(|value| !value.is_null()) {
        rows.push(render_net_effect_gas_row(gas, analysis));
    }
    if rows.is_empty() {
        "<tr><td colspan=\"6\">No scope-crossing asset changes detected yet.</td></tr>".to_owned()
    } else {
        rows.join("")
    }
}

fn render_net_effect_asset_row(item: &serde_json::Value, analysis: &TransactionAnalysis) -> String {
    let token = item.get("token").unwrap_or(&serde_json::Value::Null);
    let symbol = token
        .get("symbol")
        .and_then(|value| value.as_str())
        .unwrap_or("asset");
    let name = token
        .get("name")
        .and_then(|value| value.as_str())
        .unwrap_or("unlabeled");
    let address = token
        .get("address")
        .and_then(|value| value.as_str())
        .unwrap_or("native");
    let inflow_display = item
        .get("inflow_display")
        .and_then(|value| value.as_str())
        .unwrap_or("n/a");
    let inflow_raw = item
        .get("inflow_raw")
        .and_then(|value| value.as_str())
        .unwrap_or("0");
    let outflow_display = item
        .get("outflow_display")
        .and_then(|value| value.as_str())
        .unwrap_or("n/a");
    let outflow_raw = item
        .get("outflow_raw")
        .and_then(|value| value.as_str())
        .unwrap_or("0");
    let net_display = item
        .get("net_display")
        .and_then(|value| value.as_str())
        .unwrap_or("n/a");
    let net_raw = item
        .get("net_raw")
        .and_then(|value| value.as_str())
        .unwrap_or("0");
    format!(
        "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{} · {}</td><td>{}</td></tr>",
        escape_html(symbol),
        render_amount_stack_with_usd(
            inflow_display,
            inflow_raw,
            estimated_usd_for_token_amount(inflow_raw, token, analysis).as_deref(),
        ),
        render_amount_stack_with_usd(
            outflow_display,
            outflow_raw,
            estimated_usd_for_token_amount(outflow_raw, token, analysis).as_deref(),
        ),
        render_amount_stack_with_usd(
            net_display,
            net_raw,
            estimated_usd_for_token_amount(net_raw, token, analysis).as_deref(),
        ),
        escape_html(symbol),
        escape_html(name),
        render_copyable_code(address, 8),
    )
}

fn render_net_effect_gas_row(item: &serde_json::Value, analysis: &TransactionAnalysis) -> String {
    let token = item.get("token").unwrap_or(&serde_json::Value::Null);
    let symbol = token
        .get("symbol")
        .and_then(|value| value.as_str())
        .unwrap_or("ETH");
    let name = token
        .get("name")
        .and_then(|value| value.as_str())
        .unwrap_or("Ether");
    let amount_display = item
        .get("amount_display")
        .and_then(|value| value.as_str())
        .unwrap_or("n/a");
    let amount_raw = item
        .get("amount_raw")
        .and_then(|value| value.as_str())
        .unwrap_or("0");
    format!(
        "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{} · {}</td><td><code>native</code></td></tr>",
        escape_html(symbol),
        render_amount_stack("0", "0"),
        render_amount_stack_with_usd(
            amount_display,
            amount_raw,
            estimated_usd_for_token_amount(amount_raw, token, analysis).as_deref(),
        ),
        render_amount_stack_with_usd(
            &format!("-{}", amount_display),
            &format!("-{}", amount_raw),
            estimated_usd_for_token_amount(&format!("-{}", amount_raw), token, analysis).as_deref(),
        ),
        escape_html(symbol),
        escape_html(name),
    )
}

fn render_execution_timeline_panel(
    analysis: &TransactionAnalysis,
    initiator: &str,
    executor: &str,
    protocol_contract: Option<&str>,
    book: &AddressDisplayBook,
) -> String {
    let Some(items) = analysis
        .normalized_execution_facts
        .derived_facts
        .get("execution_timeline")
        .and_then(|value| value.as_array())
    else {
        return String::new();
    };
    if items.is_empty() {
        return String::new();
    }
    let trace_available = analysis
        .normalized_execution_facts
        .derived_facts
        .get("trace_available")
        .and_then(|value| value.as_bool())
        .unwrap_or(false);
    let mut attached_flow_ids = std::collections::BTreeSet::<String>::new();
    let initial_limit = 60usize;
    let hard_limit = 240usize;
    let visible_items = items.iter().take(initial_limit).collect::<Vec<_>>();
    let rows = render_execution_timeline_tree(
        &visible_items,
        analysis,
        initiator,
        executor,
        protocol_contract,
        book,
        &mut attached_flow_ids,
    );
    let hidden_rows = if items.len() > initial_limit {
        let hidden_items = items
            .iter()
            .skip(initial_limit)
            .take(hard_limit.saturating_sub(initial_limit))
            .collect::<Vec<_>>();
        let rows = hidden_items
            .iter()
            .map(|item| {
                render_execution_timeline_row(
                    item,
                    analysis,
                    initiator,
                    executor,
                    protocol_contract,
                    book,
                    &mut attached_flow_ids,
                )
            })
            .collect::<Vec<_>>()
            .join("");
        let omitted = items.len().saturating_sub(hard_limit);
        format!(
            r#"<details class="trace-more">
  <summary>Show {} more calls{}</summary>
  <div class="trace-timeline-list">{}</div>
</details>"#,
            items
                .len()
                .saturating_sub(initial_limit)
                .min(hard_limit - initial_limit),
            if omitted > 0 {
                format!(" · {omitted} omitted")
            } else {
                String::new()
            },
            rows
        )
    } else {
        String::new()
    };
    let badge = if trace_available {
        "call stack"
    } else {
        "top-level only"
    };
    format!(
        r#"<details class="tx-section-card storyline-steps trace-timeline-panel" open>
  <summary class="tx-section-title">
    <span class="tx-section-index">3</span>
    <h2>Transaction Timeline</h2>
    <span class="badge">{}</span>
  </summary>
  <div class="trace-timeline-list">{}</div>
  {}
</details>"#,
        badge, rows, hidden_rows
    )
}

fn render_execution_timeline_tree(
    items: &[&serde_json::Value],
    analysis: &TransactionAnalysis,
    initiator: &str,
    executor: &str,
    protocol_contract: Option<&str>,
    book: &AddressDisplayBook,
    attached_flow_ids: &mut std::collections::BTreeSet<String>,
) -> String {
    let mut children = std::collections::BTreeMap::<String, Vec<&serde_json::Value>>::new();
    let mut by_label = std::collections::BTreeSet::<String>::new();
    for item in items {
        by_label.insert(trace_item_label(item));
    }
    let mut roots = Vec::new();
    for item in items {
        let parent = trace_item_parent_label(item);
        if parent
            .as_ref()
            .map(|label| by_label.contains(label))
            .unwrap_or(false)
        {
            children.entry(parent.unwrap()).or_default().push(*item);
        } else {
            roots.push(*item);
        }
    }
    roots
        .into_iter()
        .map(|item| {
            render_execution_timeline_node(
                item,
                &children,
                analysis,
                initiator,
                executor,
                protocol_contract,
                book,
                attached_flow_ids,
            )
        })
        .collect::<Vec<_>>()
        .join("")
}

fn render_execution_timeline_node(
    item: &serde_json::Value,
    children: &std::collections::BTreeMap<String, Vec<&serde_json::Value>>,
    analysis: &TransactionAnalysis,
    initiator: &str,
    executor: &str,
    protocol_contract: Option<&str>,
    book: &AddressDisplayBook,
    attached_flow_ids: &mut std::collections::BTreeSet<String>,
) -> String {
    let label = trace_item_label(item);
    let row = render_execution_timeline_row(
        item,
        analysis,
        initiator,
        executor,
        protocol_contract,
        book,
        attached_flow_ids,
    );
    let child_rows = children
        .get(&label)
        .map(|items| {
            items
                .iter()
                .map(|child| {
                    render_execution_timeline_node(
                        child,
                        children,
                        analysis,
                        initiator,
                        executor,
                        protocol_contract,
                        book,
                        attached_flow_ids,
                    )
                })
                .collect::<Vec<_>>()
                .join("")
        })
        .unwrap_or_default();
    if child_rows.is_empty() {
        row
    } else {
        format!(
            r#"<details class="trace-node" open>
  <summary>{}</summary>
  <div class="trace-children">{}</div>
</details>"#,
            row, child_rows,
        )
    }
}

fn trace_item_label(item: &serde_json::Value) -> String {
    item.get("trace_address_label")
        .and_then(|value| value.as_str())
        .unwrap_or("root")
        .to_owned()
}

fn trace_item_parent_label(item: &serde_json::Value) -> Option<String> {
    let values = item.get("trace_address")?.as_array()?;
    if values.is_empty() {
        return None;
    }
    let parent = &values[..values.len().saturating_sub(1)];
    if parent.is_empty() {
        Some("root".to_owned())
    } else {
        Some(
            parent
                .iter()
                .filter_map(|value| value.as_u64())
                .map(|value| value.to_string())
                .collect::<Vec<_>>()
                .join("."),
        )
    }
}

fn render_execution_timeline_row(
    item: &serde_json::Value,
    analysis: &TransactionAnalysis,
    initiator: &str,
    executor: &str,
    protocol_contract: Option<&str>,
    book: &AddressDisplayBook,
    attached_flow_ids: &mut std::collections::BTreeSet<String>,
) -> String {
    let order = item
        .get("order")
        .and_then(|value| value.as_u64())
        .unwrap_or(0);
    let kind = item
        .get("kind")
        .and_then(|value| value.as_str())
        .unwrap_or("call");
    let call_type = item
        .get("call_type")
        .and_then(|value| value.as_str())
        .unwrap_or("call");
    let selector = item
        .get("selector")
        .and_then(|value| value.as_str())
        .unwrap_or("no selector");
    let from_raw = item
        .get("from_address")
        .and_then(|value| value.as_str())
        .unwrap_or("n/a");
    let to_raw = item
        .get("to_address")
        .and_then(|value| value.as_str())
        .unwrap_or("n/a");
    let value_raw = item
        .get("value_raw")
        .and_then(|value| value.as_str())
        .unwrap_or("0");
    let depth = item
        .get("depth")
        .and_then(|value| value.as_u64())
        .unwrap_or(0);
    let trace_label = item
        .get("trace_address_label")
        .and_then(|value| value.as_str())
        .unwrap_or("root");
    let from_roles = render_storyline_roles(from_raw, initiator, executor, protocol_contract);
    let to_roles = render_storyline_roles(to_raw, initiator, executor, protocol_contract);
    let attachments =
        render_call_attached_flows(analysis, from_raw, to_raw, call_type, attached_flow_ids);
    let value_line = value_raw
        .parse::<i128>()
        .ok()
        .filter(|value| *value > 0)
        .map(|value| {
            format!(
                "<span class=\"trace-value-chip\">value {}</span>",
                escape_html(&format_token_amount_display(
                    value,
                    &serde_json::json!({"symbol": "ETH", "name": "Ether", "address": "native", "decimals": 18})
                ))
            )
        })
        .unwrap_or_default();
    let depth = depth.min(10);
    let call_label = if kind == "tx_entry" { "TX" } else { call_type };
    let attachments = if attachments.is_empty() {
        String::new()
    } else {
        format!(r#"<span class="trace-transfer-group">{attachments}</span>"#)
    };
    format!(
        r#"<div class="trace-line" style="--trace-depth:{}">
  <span class="trace-order">{}</span>
  <span class="trace-depth-lines"></span>
  <span class="trace-call-pill">{} → {}</span>
  <span class="trace-method">{}</span>
  <span class="trace-endpoint">{}{}</span>
  <span class="trace-arrow-inline">→</span>
  <span class="trace-endpoint">{}{}</span>
  {}
  {}
  <span class="trace-ref">trace {}</span>
</div>"#,
        depth,
        order,
        depth,
        escape_html(&call_label.to_ascii_uppercase()),
        escape_html(selector),
        render_compact_known_address(from_raw, book),
        from_roles,
        render_compact_known_address(to_raw, book),
        to_roles,
        value_line,
        attachments,
        escape_html(trace_label),
    )
}

fn render_compact_known_address(address: &str, book: &AddressDisplayBook) -> String {
    if address.is_empty() || address.eq_ignore_ascii_case("n/a") {
        return "<code>n/a</code>".to_owned();
    }
    let label = book
        .best_label(address)
        .map(|label| format!(r#"<small>{}</small>"#, escape_html(&label)))
        .unwrap_or_default();
    format!(
        r#"<span class="trace-address-chip">{}{}</span>"#,
        render_copyable_code(address, 8),
        label,
    )
}

fn render_call_attached_flows(
    analysis: &TransactionAnalysis,
    from_raw: &str,
    to_raw: &str,
    call_type: &str,
    attached_flow_ids: &mut std::collections::BTreeSet<String>,
) -> String {
    if call_type.eq_ignore_ascii_case("staticcall") {
        return String::new();
    }
    let mut rows = Vec::new();
    for flow in &analysis.derived_flows {
        if rows.len() >= 6 {
            break;
        }
        if attached_flow_ids.contains(&flow.flow_id) {
            continue;
        }
        if flow.from_address.eq_ignore_ascii_case(from_raw)
            && flow.to_address.eq_ignore_ascii_case(to_raw)
        {
            attached_flow_ids.insert(flow.flow_id.clone());
            let token = flow_token_descriptor(flow);
            let token_address = token
                .get("address")
                .and_then(|value| value.as_str())
                .or(flow.token_address.as_deref())
                .unwrap_or("native");
            let name = token
                .get("name")
                .and_then(|value| value.as_str())
                .unwrap_or("unlabeled");
            rows.push(format!(
                "<span>{}{} · {} · {}</span>",
                escape_html(&format_flow_amount_display(flow, &token)),
                flow_estimated_usd(flow)
                    .map(|value| format!(" ({})", escape_html(&value)))
                    .unwrap_or_default(),
                escape_html(name),
                render_copy_only_button(token_address),
            ));
        }
    }
    rows.join("")
}

fn render_amount_stack(display: &str, raw: &str) -> String {
    render_amount_stack_with_usd(display, raw, None)
}

fn render_amount_stack_with_usd(display: &str, raw: &str, estimated_usd: Option<&str>) -> String {
    let display = normalize_amount_display(display, raw);
    let estimated_usd = estimated_usd
        .filter(|value| !value.is_empty())
        .map(|value| {
            format!(
                "<small class=\"estimated-usd\">{}</small>",
                escape_html(value)
            )
        })
        .unwrap_or_default();
    format!(
        "<div class=\"amount-stack\"><strong>{}</strong><small>raw: {}</small>{}</div>",
        escape_html(&display),
        escape_html(raw),
        estimated_usd,
    )
}

fn normalize_amount_display(display: &str, raw: &str) -> String {
    if display == format!("{raw} raw ERC20")
        || display == format!("{raw} raw Unknown ERC20")
        || display.ends_with(" raw ERC20")
        || display.ends_with(" raw Unknown ERC20")
    {
        format!("{raw} raw units")
    } else {
        display.to_owned()
    }
}

fn flow_estimated_usd(flow: &crate::domain::value_objects::AssetFlow) -> Option<String> {
    flow.metadata
        .pointer("/estimated_usd/value_decimal")
        .and_then(|value| value.as_str())
        .and_then(|value| value.parse::<f64>().ok())
        .map(format_estimated_usd)
}

fn estimated_usd_for_token_amount(
    raw: &str,
    token: &serde_json::Value,
    analysis: &TransactionAnalysis,
) -> Option<String> {
    usd_value_for_token_amount(raw, token, analysis).map(format_estimated_usd)
}

fn usd_value_for_token_amount(
    raw: &str,
    token: &serde_json::Value,
    analysis: &TransactionAnalysis,
) -> Option<f64> {
    let price = price_for_token_usd(token, analysis)?;
    let decimals = token
        .get("decimals")
        .and_then(|value| value.as_u64())
        .unwrap_or(18);
    let raw = raw.parse::<f64>().ok()?;
    let amount = raw / 10f64.powi(decimals.min(38) as i32);
    let value = amount * price;
    if value.is_finite() { Some(value) } else { None }
}

fn price_for_token_usd(token: &serde_json::Value, analysis: &TransactionAnalysis) -> Option<f64> {
    let address = token
        .get("address")
        .and_then(|value| value.as_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    let symbol = token
        .get("symbol")
        .and_then(|value| value.as_str())
        .unwrap_or("");
    if matches_ignore_ascii_case(symbol, &["USDC", "USDT", "USDBC"])
        || matches_ignore_ascii_case(
            &address,
            &[
                "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",
                "0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca",
                "0xfde4c96c8593536e31f229ea8f37b2ada2699bb2",
            ],
        )
    {
        return Some(1.0);
    }
    let pricing_address = if address == "native" || symbol.eq_ignore_ascii_case("ETH") {
        "0x4200000000000000000000000000000000000006"
    } else {
        address.as_str()
    };
    for flow in &analysis.derived_flows {
        let flow_address = flow
            .token_address
            .as_deref()
            .unwrap_or("")
            .to_ascii_lowercase();
        if flow_address != pricing_address {
            continue;
        }
        if let Some(price) = flow
            .metadata
            .pointer("/estimated_usd/price_decimal")
            .and_then(|value| value.as_str())
            .and_then(|value| value.parse::<f64>().ok())
            .filter(|value| value.is_finite() && *value > 0.0)
        {
            return Some(price);
        }
    }
    analysis
        .scenario_outputs
        .pointer("/pricing/flow_quotes")
        .and_then(|value| value.as_array())
        .and_then(|quotes| {
            quotes.iter().find_map(|item| {
                let quote = item.get("quote")?;
                let quote_address = quote
                    .get("token_address")
                    .and_then(|value| value.as_str())?
                    .to_ascii_lowercase();
                if quote_address != pricing_address {
                    return None;
                }
                quote
                    .get("price_decimal")
                    .and_then(|value| value.as_str())
                    .and_then(|value| value.parse::<f64>().ok())
                    .filter(|value| value.is_finite() && *value > 0.0)
            })
        })
}

fn matches_ignore_ascii_case(value: &str, candidates: &[&str]) -> bool {
    candidates
        .iter()
        .any(|candidate| value.eq_ignore_ascii_case(candidate))
}

fn format_estimated_usd(value: f64) -> String {
    let sign = if value < 0.0 { "-" } else { "" };
    let abs = value.abs();
    let amount = if abs >= 1_000.0 {
        format!("{abs:.2}")
    } else if abs >= 1.0 {
        format!("{abs:.4}")
    } else if abs > 0.0 {
        format!("{abs:.6}")
    } else {
        "0".to_owned()
    };
    format!("est. {sign}${amount}")
}

fn format_signed_usd(value: f64) -> String {
    let sign = if value > 0.0 {
        "+"
    } else if value < 0.0 {
        "-"
    } else {
        ""
    };
    let abs = value.abs();
    let amount = if abs >= 1_000.0 {
        format!("{abs:.2}")
    } else if abs >= 1.0 {
        format!("{abs:.4}")
    } else if abs > 0.0 {
        format!("{abs:.6}")
    } else {
        "0".to_owned()
    };
    format!("{sign}${amount}")
}

fn render_storyline_steps(
    value: Option<&serde_json::Value>,
    analysis: &TransactionAnalysis,
    initiator: &str,
    executor: &str,
    protocol_contract: Option<&str>,
    book: &AddressDisplayBook,
) -> String {
    let Some(items) = value.and_then(|value| value.as_array()) else {
        return String::new();
    };
    if items.is_empty() {
        return String::new();
    }
    format!(
        r#"<details class="tx-section-card storyline-steps" open>
  <summary class="tx-section-title">
    <span class="tx-section-index">3</span>
    <h2>Transaction Timeline</h2>
    <span class="badge">ordered events</span>
  </summary>
  <div class="timeline-list">{}</div>
</details>"#,
        items
            .iter()
            .map(|item| {
                let order = item
                    .get("order")
                    .and_then(|value| value.as_u64())
                    .unwrap_or(0);
                let title = item
                    .get("title")
                    .and_then(|value| value.as_str())
                    .unwrap_or("Step");
                let token = item.get("token").unwrap_or(&serde_json::Value::Null);
                let symbol = token
                    .get("symbol")
                    .and_then(|value| value.as_str())
                    .unwrap_or("asset");
                let name = token
                    .get("name")
                    .and_then(|value| value.as_str())
                    .unwrap_or("unlabeled");
                let token_address = token
                    .get("address")
                    .and_then(|value| value.as_str())
                    .unwrap_or("native");
                let amount_display = item
                    .get("amount_display")
                    .and_then(|value| value.as_str())
                    .unwrap_or("n/a");
                let amount_raw = item
                    .get("amount_raw")
                    .and_then(|value| value.as_str())
                    .unwrap_or("n/a");
                let amount_display = normalize_amount_display(amount_display, amount_raw);
                let estimated_usd = estimated_usd_for_token_amount(amount_raw, token, analysis)
                    .map(|value| format!(" · {}", value))
                    .unwrap_or_default();
                let source_hint_label = item
                    .get("source_hint")
                    .and_then(|value| value.get("label"))
                    .and_then(|value| value.as_str());
                let from_raw = item
                    .get("from_address")
                    .and_then(|value| value.as_str())
                    .unwrap_or("n/a");
                let to_raw = item
                    .get("to_address")
                    .and_then(|value| value.as_str())
                    .unwrap_or("n/a");
                let from_roles =
                    render_storyline_roles(from_raw, initiator, executor, protocol_contract);
                let to_roles =
                    render_storyline_roles(to_raw, initiator, executor, protocol_contract);
                format!(
                    r#"<article class="timeline-step">
  <div class="timeline-head">
    <div class="timeline-index">{}</div>
    <div class="timeline-copy">
      <strong>{}</strong>
      <small>{} · {} · raw: {}{}</small>
      {}
    </div>
  </div>
  <div class="timeline-flow compact">
    <div class="timeline-side">
      {}
      <div class="timeline-role-row">{}</div>
    </div>
    <div class="timeline-mid">
      <strong>{}</strong>
      <span>{}</span>
      <span class="timeline-arrow-inline">→</span>
    </div>
    <div class="timeline-side">
      {}
      <div class="timeline-role-row">{}</div>
    </div>
  </div>
</article>"#,
                    order,
                    escape_html(title),
                    escape_html(symbol),
                    escape_html(name),
                    escape_html(amount_raw),
                    escape_html(&estimated_usd),
                    source_hint_label
                        .map(|value| format!(
                            "<span class=\"timeline-hint\">Likely source: {}</span>",
                            escape_html(value)
                        ))
                        .unwrap_or_default(),
                    render_known_address(from_raw, 10, book),
                    from_roles,
                    escape_html(&amount_display),
                    format!(
                        "{} · {}",
                        escape_html(name),
                        render_copy_only_button(token_address)
                    ),
                    render_known_address(to_raw, 10, book),
                    to_roles,
                )
            })
            .collect::<Vec<_>>()
            .join("")
    )
}

fn render_generic_timeline_panel(
    analysis: &TransactionAnalysis,
    initiator: &str,
    executor: &str,
    protocol_contract: Option<&str>,
    book: &AddressDisplayBook,
) -> String {
    if analysis.derived_flows.is_empty() {
        return String::new();
    }
    format!(
        r#"<details class="tx-section-card storyline-steps" open>
  <summary class="tx-section-title">
    <span class="tx-section-index">3</span>
    <h2>Transaction Timeline</h2>
    <span class="badge">derived from logs</span>
  </summary>
  <div class="timeline-list">{}</div>
</details>"#,
        analysis
            .derived_flows
            .iter()
            .take(80)
            .enumerate()
            .map(|(index, flow)| {
                let token = flow_token_descriptor(flow);
                let symbol = token.get("symbol").and_then(|value| value.as_str()).unwrap_or("asset");
                let name = token.get("name").and_then(|value| value.as_str()).unwrap_or("unlabeled");
                let token_address = token.get("address").and_then(|value| value.as_str()).or(flow.token_address.as_deref()).unwrap_or("native");
                let amount_display = format_flow_amount_display(flow, &token);
                let estimated_usd = flow_estimated_usd(flow)
                    .map(|value| format!(" · {}", value))
                    .unwrap_or_default();
                let from_roles = render_storyline_roles(&flow.from_address, initiator, executor, protocol_contract);
                let to_roles = render_storyline_roles(&flow.to_address, initiator, executor, protocol_contract);
                format!(
                    r#"<article class="timeline-step">
  <div class="timeline-head">
    <div class="timeline-index">{}</div>
    <div class="timeline-copy">
      <strong>Confirmed asset flow</strong>
      <small>{} · {} · raw: {}{}</small>
    </div>
  </div>
  <div class="timeline-flow compact">
    <div class="timeline-side">{}<div class="timeline-role-row">{}</div></div>
    <div class="timeline-mid"><strong>{}</strong><span>{} · {}</span><span class="timeline-arrow-inline">→</span></div>
    <div class="timeline-side">{}<div class="timeline-role-row">{}</div></div>
  </div>
</article>"#,
                    index + 1,
                    escape_html(symbol),
                    escape_html(name),
                    escape_html(&flow.amount_raw),
                    escape_html(&estimated_usd),
                    render_known_address(&flow.from_address, 10, book),
                    from_roles,
                    escape_html(&amount_display),
                    escape_html(name),
                    render_copy_only_button(token_address),
                    render_known_address(&flow.to_address, 10, book),
                    to_roles,
                )
            })
            .collect::<Vec<_>>()
            .join("")
    )
}

fn render_storyline_visual(
    storyline: &serde_json::Value,
    kind: &str,
    protocol: &str,
    initiator: &str,
    executor: &str,
    book: &AddressDisplayBook,
) -> String {
    let protocol_contract = storyline
        .get("protocol_contract")
        .and_then(|value| value.as_str());
    if !protocol_path_is_meaningful(protocol, initiator, executor, protocol_contract) {
        return String::new();
    }
    let direct_protocol_call = storyline
        .get("protocol_call_mode")
        .and_then(|value| value.as_str())
        .map(|value| value == "direct")
        .unwrap_or_else(|| {
            protocol_contract
                .map(|value| executor.eq_ignore_ascii_case(value))
                .unwrap_or(false)
        });
    let funding_lane = render_storyline_lane(
        "Funding",
        "Where the executor got assets or approvals before protocol interaction.",
        storyline.get("funding_sources"),
        "funding",
        initiator,
        executor,
        protocol_contract,
        book,
    );
    let repay_lane = render_storyline_lane(
        "Protocol-facing legs",
        if kind == "liquidation" {
            "Debt repayment or direct protocol-facing asset movement."
        } else {
            "Venue-facing execution legs."
        },
        storyline
            .get("repay_legs")
            .or_else(|| storyline.get("cycle_legs")),
        "protocol",
        initiator,
        executor,
        protocol_contract,
        book,
    );
    let receipt_lane = render_storyline_lane(
        if kind == "liquidation" {
            "Collateral / rewards back"
        } else {
            "Assets back to executor"
        },
        "What came back to the executor after the core action.",
        storyline.get("collateral_receipts"),
        "receipt",
        initiator,
        executor,
        protocol_contract,
        book,
    );
    let exit_lane = render_storyline_lane(
        if kind == "liquidation" {
            "Disposition / settlement"
        } else {
            "Exit / settlement"
        },
        "Where assets moved next after the main action.",
        storyline
            .get("disposition_legs")
            .or_else(|| storyline.get("exit_legs")),
        "exit",
        initiator,
        executor,
        protocol_contract,
        book,
    );

    let main_path = if direct_protocol_call {
        format!(
            r#"<div class="storyline-main-path direct">
    <div class="story-node">
      <span class="story-node-label">Initiator</span>
      <strong>{}</strong>
      <small>EOA or top-level caller</small>
    </div>
    <div class="story-arrow">→</div>
    <div class="story-node protocol">
      <span class="story-node-label">Protocol direct call</span>
      <strong>{}</strong>
      <small>{}</small>
    </div>
  </div>"#,
            render_known_address(initiator, 10, book),
            render_known_address(protocol_contract.unwrap_or(executor), 10, book),
            escape_html(protocol),
        )
    } else {
        format!(
            r#"<div class="storyline-main-path">
    <div class="story-node">
      <span class="story-node-label">Initiator</span>
      <strong>{}</strong>
      <small>EOA or top-level caller</small>
    </div>
    <div class="story-arrow">→</div>
    <div class="story-node accent">
      <span class="story-node-label">Executor</span>
      <strong>{}</strong>
      <small>Contract or account that orchestrated the flow</small>
    </div>
    <div class="story-arrow">→</div>
    <div class="story-node protocol">
      <span class="story-node-label">Protocol</span>
      <strong>{}</strong>
      <small>{}</small>
    </div>
  </div>"#,
            render_known_address(initiator, 10, book),
            render_known_address(executor, 10, book),
            escape_html(protocol),
            escape_html(if kind == "liquidation" {
                "Detected liquidation target"
            } else if kind == "arbitrage" {
                "Detected venue set"
            } else {
                "Resolved execution target"
            }),
        )
    };

    format!(
        r#"<details class="tx-section-card storyline-visual-shell" open>
  <summary class="tx-section-title">
    <span class="tx-section-index">4</span>
    <h2>Protocol Path</h2>
  </summary>
  {}
  <div class="storyline-lanes">
    {}
    {}
    {}
    {}
  </div>
</details>"#,
        main_path, funding_lane, repay_lane, receipt_lane, exit_lane
    )
}

fn protocol_path_is_meaningful(
    protocol: &str,
    initiator: &str,
    executor: &str,
    protocol_contract: Option<&str>,
) -> bool {
    let has_known_protocol = !matches!(
        protocol.to_ascii_lowercase().as_str(),
        "" | "n/a" | "unknown" | "unknown protocol"
    );
    let has_initiator = !matches!(initiator.to_ascii_lowercase().as_str(), "" | "n/a");
    let has_executor = !matches!(executor.to_ascii_lowercase().as_str(), "" | "n/a");
    let has_protocol_contract = protocol_contract
        .map(|value| !matches!(value.to_ascii_lowercase().as_str(), "" | "n/a"))
        .unwrap_or(false);

    has_initiator && has_executor && (has_known_protocol || has_protocol_contract)
}

fn render_storyline_lane(
    title: &str,
    subtitle: &str,
    value: Option<&serde_json::Value>,
    lane_kind: &str,
    initiator: &str,
    executor: &str,
    protocol_contract: Option<&str>,
    book: &AddressDisplayBook,
) -> String {
    let items = value
        .and_then(|value| value.as_array())
        .map(|items| {
            items
                .iter()
                .take(4)
                .map(|item| {
                    render_storyline_flow_chip(
                        item,
                        lane_kind,
                        initiator,
                        executor,
                        protocol_contract,
                        book,
                    )
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if items.is_empty() {
        return String::new();
    }
    let lane_body = items.join("");
    format!(
        r#"<section class="story-lane story-lane-{}">
  <div class="story-lane-head">
    <strong>{}</strong>
    <small>{}</small>
  </div>
  <div class="story-chip-row">{}</div>
</section>"#,
        escape_html(lane_kind),
        escape_html(title),
        escape_html(subtitle),
        lane_body
    )
}

fn render_storyline_flow_chip(
    item: &serde_json::Value,
    lane_kind: &str,
    initiator: &str,
    executor: &str,
    protocol_contract: Option<&str>,
    book: &AddressDisplayBook,
) -> String {
    let asset_type = item
        .get("asset_type")
        .and_then(|value| value.as_str())
        .unwrap_or("asset");
    let token = item.get("token").unwrap_or(&serde_json::Value::Null);
    let token_symbol = token
        .get("symbol")
        .and_then(|value| value.as_str())
        .unwrap_or(asset_type);
    let token_name = token
        .get("name")
        .and_then(|value| value.as_str())
        .unwrap_or("unlabeled asset");
    let token_address = token
        .get("address")
        .and_then(|value| value.as_str())
        .unwrap_or("native");
    let amount_raw = item
        .get("amount_raw")
        .and_then(|value| value.as_str())
        .unwrap_or("n/a");
    let amount_display = item
        .get("amount_display")
        .and_then(|value| value.as_str())
        .unwrap_or(amount_raw);
    let amount_display = normalize_amount_display(amount_display, amount_raw);
    let source_hint_label = item
        .get("source_hint")
        .and_then(|value| value.get("label"))
        .and_then(|value| value.as_str());
    let from_raw = item
        .get("from_address")
        .and_then(|value| value.as_str())
        .unwrap_or("n/a");
    let to_raw = item
        .get("to_address")
        .and_then(|value| value.as_str())
        .unwrap_or("n/a");
    let from_roles = render_storyline_roles(from_raw, initiator, executor, protocol_contract);
    let to_roles = render_storyline_roles(to_raw, initiator, executor, protocol_contract);
    format!(
        r#"<article class="story-chip story-chip-{}">
  <div class="story-chip-head">
    <span class="story-chip-badge">{}</span>
    <span class="story-chip-lane-label">{}</span>
  </div>
  <strong>{}</strong>
  <small>{} · {}</small>
  <small>{} {} → {} {}</small>
  <small>{}</small>
  {}
</article>"#,
        escape_html(lane_kind),
        escape_html(asset_type),
        escape_html(lane_kind),
        escape_html(token_symbol),
        escape_html(token_name),
        render_copyable_code(token_address, 8),
        render_known_address(from_raw, 10, book),
        from_roles,
        render_known_address(to_raw, 10, book),
        to_roles,
        escape_html(&format!("{} · raw: {}", amount_display, amount_raw)),
        source_hint_label
            .map(|value| format!(
                "<small class=\"timeline-hint\">Likely source: {}</small>",
                escape_html(value)
            ))
            .unwrap_or_default()
    )
}

fn render_storyline_roles(
    address: &str,
    initiator: &str,
    executor: &str,
    protocol_contract: Option<&str>,
) -> String {
    let mut tags = Vec::new();
    if address.eq_ignore_ascii_case(initiator) {
        tags.push("<span class=\"story-role-tag initiator\">initiator</span>".to_owned());
    }
    if address.eq_ignore_ascii_case(executor) {
        tags.push("<span class=\"story-role-tag executor\">executor</span>".to_owned());
    }
    if protocol_contract
        .map(|value| address.eq_ignore_ascii_case(value))
        .unwrap_or(false)
    {
        tags.push("<span class=\"story-role-tag protocol\">protocol</span>".to_owned());
    }
    if tags.is_empty() && address != "n/a" {
        tags.push("<span class=\"story-role-tag external\">external</span>".to_owned());
    }
    tags.join("")
}

fn render_cluster_graph_panel(clusters: &[DashboardClusterRecord]) -> String {
    let cluster_links = if clusters.is_empty() {
        "<div class=\"empty-state\">No clusters yet. Create a cluster first so graph planning has a subject.</div>"
            .to_owned()
    } else {
        clusters
            .iter()
            .map(|cluster| {
                format!(
                    "<a class=\"cluster-card\" href=\"/app/clusters/{}\"><strong>{}</strong><span>{} addresses</span><small>graph work will begin from historical cluster transactions</small></a>",
                    cluster.cluster_id,
                    escape_html(&cluster.name),
                    cluster.address_count
                )
            })
            .collect::<Vec<_>>()
            .join("")
    };

    format!(
        r#"
<section class="panel">
  <div class="panel-head"><h2>Cluster relationship graph</h2><span class="badge">planned</span></div>
  <div class="panel-body">
    <p class="panel-copy">This page will evolve into the dedicated graph workspace: cluster-level relationship inference, historical flow clustering, and eventually a high-performance 3D view. The first milestone is not rendering. It is building stable graph data extraction and storage.</p>
    <div class="feature-grid">
      <article class="feature-card">
        <h2>Phase 1</h2>
        <p>Build graph-ready exports from analyzed cluster history: counterparties, internal transfers, repeated router/protocol touchpoints, and address co-occurrence motifs.</p>
      </article>
      <article class="feature-card">
        <h2>Phase 2</h2>
        <p>Add server-side graph aggregation, edge scoring, and time-window filtering so graph queries stay fast before any 3D rendering work starts.</p>
      </article>
      <article class="feature-card">
        <h2>Phase 3</h2>
        <p>Ship a 2D operator graph first, then move to a GPU-backed 3D scene only after the graph data model and interaction patterns are stable.</p>
      </article>
    </div>
    <div class="panel-copy">Current cluster entry points for that roadmap:</div>
    <div class="cluster-list">{}</div>
  </div>
</section>
"#,
        cluster_links
    )
}

fn render_tracking_status_panel(
    cluster: &DashboardClusterRecord,
    tracking: Option<&ClusterTrackingState>,
) -> String {
    let Some(tracking) = tracking else {
        return "<div class=\"empty-state\">Tracking state has not been initialized yet.</div>"
            .to_owned();
    };
    let action_path = match tracking.status {
        crate::domain::enums::ClusterTrackingStatus::Active => {
            format!("/app/clusters/{}/tracking/pause", cluster.cluster_id)
        }
        _ => format!("/app/clusters/{}/tracking/enable", cluster.cluster_id),
    };
    let action_label = match tracking.status {
        crate::domain::enums::ClusterTrackingStatus::Active => "Pause tracking",
        _ => "Start tracking",
    };
    format!(
        r#"<div class="stats-grid">
  <div class="stat"><span>Status</span><strong>{:?}</strong></div>
  <div class="stat"><span>Last processed block</span><strong>{}</strong></div>
  <div class="stat"><span>Last updated</span><strong>{}</strong></div>
  <div class="stat"><span>Tracked addresses</span><strong>{}</strong></div>
</div>
{}
<form method="post" action="{}">
  <button class="cta secondary" type="submit">{}</button>
</form>"#,
        tracking.status,
        tracking
            .last_processed_block
            .map(|value| value.to_string())
            .unwrap_or_else(|| "n/a".to_owned()),
        tracking
            .last_updated_at
            .map(|value| value.format("%Y-%m-%d %H:%M:%S UTC").to_string())
            .unwrap_or_else(|| "n/a".to_owned()),
        tracking.tracked_address_count,
        tracking
            .error_message
            .as_deref()
            .map(|message| format!("<div class=\"notice error\">{}</div>", escape_html(message)))
            .unwrap_or_default(),
        action_path,
        action_label
    )
}

fn render_cluster_auto_discovery_panel(cluster: &DashboardClusterRecord) -> String {
    let Some(discovery) = cluster
        .metadata
        .get("auto_discovery")
        .and_then(|value| value.as_object())
    else {
        return "<div class=\"empty-state\">Worker onboarding has not written auto-discovery results yet. New seed clusters queue onboarding on create; after editing addresses, review the cluster and manually run backfill when ready.</div>".to_owned();
    };
    let status = discovery
        .get("status")
        .and_then(|value| value.as_str())
        .unwrap_or("unknown");
    let updated_at = discovery
        .get("updated_at")
        .and_then(|value| value.as_str())
        .unwrap_or("n/a");
    let auto_added = discovery
        .get("auto_added_addresses")
        .and_then(|value| value.as_array())
        .map(|items| items.len())
        .unwrap_or(0);
    let suggestions = discovery
        .get("suggested_addresses")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    let rows = suggestions
        .iter()
        .take(20)
        .map(|item| {
            let address = item
                .get("address")
                .and_then(|value| value.as_str())
                .unwrap_or("n/a");
            let role = item
                .get("role")
                .and_then(|value| value.as_str())
                .unwrap_or("n/a");
            let relation = item
                .get("relation_type")
                .and_then(|value| value.as_str())
                .unwrap_or("n/a");
            let confidence = item
                .get("confidence")
                .and_then(|value| value.as_f64())
                .map(|value| format!("{value:.2}"))
                .unwrap_or_else(|| "n/a".to_owned());
            let auto_add = item
                .get("auto_add")
                .and_then(|value| value.as_bool())
                .unwrap_or(false);
            let reason = item
                .get("reason")
                .and_then(|value| value.as_str())
                .unwrap_or("n/a");
            format!(
                "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
                render_copyable_code(address, 6),
                escape_html(role),
                escape_html(relation),
                confidence,
                if auto_add { "yes" } else { "suggested" },
                escape_html(reason),
            )
        })
        .collect::<Vec<_>>()
        .join("");
    format!(
        r#"<div class="stats-grid">
  <div class="stat"><span>Status</span><strong>{}</strong></div>
  <div class="stat"><span>Auto-added</span><strong>{}</strong></div>
  <div class="stat"><span>Suggestions</span><strong>{}</strong></div>
  <div class="stat"><span>Updated</span><strong>{}</strong></div>
</div>
{}"#,
        escape_html(status),
        auto_added,
        suggestions.len(),
        escape_html(updated_at),
        render_table_or_empty(
            &rows,
            "No related address suggestions were produced for this cluster.",
            "<tr><th>Address</th><th>Role</th><th>Relation</th><th>Confidence</th><th>Action</th><th>Reason</th></tr>",
        )
    )
}

fn render_cluster_metrics_panel(metrics: &[ClusterMetricsSnapshot]) -> String {
    if metrics.is_empty() {
        return "<div class=\"empty-state\">Rolling metrics have not been computed yet. They appear after classified transactions are written for this cluster.</div>".to_owned();
    }
    let one_hour = metrics
        .iter()
        .find(|item| matches!(item.window, crate::domain::enums::MetricsWindow::OneHour));
    let twenty_four_hours = metrics.iter().find(|item| {
        matches!(
            item.window,
            crate::domain::enums::MetricsWindow::TwentyFourHours
        )
    });
    let seven_days = metrics
        .iter()
        .find(|item| matches!(item.window, crate::domain::enums::MetricsWindow::SevenDays));

    format!(
        r#"<div class="field-hint">Rolling metrics are fixed windows for the MVP: <code>1h</code>, <code>24h</code>, and <code>7d</code>. The custom start/end fields above only affect the spend summary table below, not these rolling snapshots.</div>
<div class="stats-grid">
  <div class="stat"><span>1h PnL / ROI</span><strong>{} / {}</strong></div>
  <div class="stat"><span>24h PnL / ROI</span><strong>{} / {}</strong></div>
  <div class="stat"><span>7d PnL / ROI</span><strong>{} / {}</strong></div>
  <div class="stat"><span>24h tx count</span><strong>{}</strong></div>
  <div class="stat"><span>24h volume</span><strong>{}</strong></div>
  <div class="stat"><span>24h gas cost</span><strong>{}</strong></div>
</div>"#,
        format_usd_option(one_hour.and_then(|item| item.realized_pnl_usd)),
        format_roi_option(one_hour.and_then(|item| item.roi)),
        format_usd_option(twenty_four_hours.and_then(|item| item.realized_pnl_usd)),
        format_roi_option(twenty_four_hours.and_then(|item| item.roi)),
        format_usd_option(seven_days.and_then(|item| item.realized_pnl_usd)),
        format_roi_option(seven_days.and_then(|item| item.roi)),
        twenty_four_hours
            .map(|item| item.tx_count.to_string())
            .unwrap_or_else(|| "0".to_owned()),
        format_usd_option(twenty_four_hours.and_then(|item| item.volume_usd)),
        format_usd_option(twenty_four_hours.and_then(|item| item.gas_cost_usd)),
    )
}

fn render_behavior_breakdown_panel(metrics: &[ClusterMetricsSnapshot]) -> String {
    let Some(snapshot) = metrics.iter().find(|item| {
        matches!(
            item.window,
            crate::domain::enums::MetricsWindow::TwentyFourHours
        )
    }) else {
        return "<div class=\"empty-state\">Behavior breakdown will appear after enough classified transactions are available.</div>".to_owned();
    };

    let keys = ["arbitrage", "position", "lp", "liquidation", "unknown"];
    let rows = keys
        .into_iter()
        .map(|key| {
            let entry = snapshot.behavior_breakdown.get(key);
            let count = entry
                .and_then(|value| value.get("count"))
                .and_then(|value| value.as_u64())
                .unwrap_or(0);
            let ratio = entry
                .and_then(|value| value.get("ratio"))
                .and_then(|value| value.as_f64())
                .unwrap_or(0.0);
            format!(
                r#"<tr>
  <td>{}</td>
  <td>{}</td>
  <td>{:.1}%</td>
  <td><div class="meter"><span style="width:{:.1}%"></span></div></td>
</tr>"#,
                escape_html(key),
                count,
                ratio * 100.0,
                ratio * 100.0
            )
        })
        .collect::<Vec<_>>()
        .join("");

    format!(
        r#"<div class="table-shell">
  <table>
    <thead>
      <tr>
        <th>Behavior</th>
        <th>Count</th>
        <th>Ratio</th>
        <th>Visual</th>
      </tr>
    </thead>
    <tbody>{}</tbody>
  </table>
</div>"#,
        rows
    )
}

fn render_cluster_strategy_analysis_panel(
    analysis: Option<&ClusterStrategyAnalysisPayload>,
) -> String {
    let Some(analysis) = analysis else {
        return "<div class=\"empty-state\">Strategy analysis appears after classified transactions are available for this cluster.</div>".to_owned();
    };
    let profile = &analysis.behavior_profile;
    let performance = &analysis.performance;
    let behavior_rows = analysis
        .behavior_profile
        .behavior_breakdown
        .iter()
        .map(|item| {
            let share = if analysis.tx_count == 0 {
                None
            } else {
                Some(item.tx_count as f64 / analysis.tx_count as f64)
            };
            format!(
                "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td><div class=\"meter\"><span style=\"width:{:.1}%\"></span></div></td></tr>",
                escape_html(&item.strategy_type),
                item.tx_count,
                item.success_count,
                format_usd_option(item.estimated_pnl_usd),
                share.unwrap_or_default() * 100.0,
            )
        })
        .collect::<Vec<_>>()
        .join("");
    let role_rows = analysis
        .address_roles
        .iter()
        .take(12)
        .map(|item| {
            format!(
                "<tr><td><code>{}</code></td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
                escape_html(&truncate_middle(&item.address, 6)),
                escape_html(item.configured_role.as_deref().unwrap_or("n/a")),
                escape_html(&item.inferred_role),
                item.tx_count,
                item.arbitrage_tx_count,
                format_usd_option(item.estimated_pnl_usd),
                render_sample_hashes(&item.sample_tx_hashes),
            )
        })
        .collect::<Vec<_>>()
        .join("");
    let representative_rows = render_representative_transaction_rows(
        &analysis.representative_transactions,
        analysis.chain_id,
        12,
    );

    format!(
        r#"<div class="strategy-report-shell">
  <div class="strategy-report-meta">
    <span>Generated {}</span>
    <span>Window {}</span>
    <span>Cluster {}</span>
  </div>
  <div class="stats-grid">
    <div class="stat"><span>Primary behavior</span><strong>{}</strong><small>{} tx, {}</small></div>
    <div class="stat"><span>Success rate</span><strong>{}</strong><small>{} success / {} failed</small></div>
    <div class="stat"><span>Estimated PnL</span><strong>{}</strong><small>avg {}</small></div>
    <div class="stat"><span>Gas cost</span><strong>{}</strong><small>avg {}</small></div>
    <div class="stat"><span>PnL / gas</span><strong>{}</strong><small>{} classified tx</small></div>
    <div class="stat"><span>Active addresses</span><strong>{}</strong><small>{} arbitrage tx</small></div>
  </div>
  <section class="strategy-report-section">
    <div class="section-head">
      <h3>Behavior profile</h3>
      <span class="badge">cluster</span>
    </div>
    {}
  </section>
  <div class="strategy-detail-grid">
    {}
    {}
  </div>
  <section class="strategy-report-section">
    <div class="section-head">
      <h3>Representative transactions</h3>
      <span class="badge">samples</span>
    </div>
    {}
  </section>
  <section class="strategy-report-section">
    <div class="section-head">
      <h3>Global route and protocol drilldowns</h3>
      <span class="badge">all strategies</span>
    </div>
    <h4>Top routes</h4>
    {}
    <h4>Top protocols</h4>
    {}
    <h4>Address roles</h4>
    {}
  </section>
</div>"#,
        escape_html(&analysis.generated_at),
        escape_html(&report_window_label(
            analysis.window_start.as_deref(),
            analysis.window_end.as_deref()
        )),
        render_copyable_code(&analysis.cluster_id, 6),
        escape_html(&profile.primary_behavior),
        profile.primary_behavior_tx_count,
        format_percent_option(profile.primary_behavior_tx_share),
        format_percent_option(performance.success_rate),
        performance.success_count,
        performance.failed_count,
        format_usd_option(performance.estimated_pnl_usd),
        format_usd_option(performance.avg_estimated_pnl_usd),
        format_usd_option(performance.gas_cost_usd),
        format_usd_option(performance.avg_gas_cost_usd),
        format_ratio_option(performance.pnl_to_gas_ratio),
        performance.tx_count,
        profile.active_addresses_count,
        analysis.arbitrage_tx_count,
        render_table_or_empty(
            &behavior_rows,
            "No behavior profile has been classified yet.",
            "<tr><th>Behavior</th><th>Tx</th><th>Success</th><th>PnL</th><th>Share</th></tr>",
        ),
        render_strategy_detail_report(
            "Arbitrage report",
            analysis.arbitrage_report.as_ref(),
            "No arbitrage behavior has been classified for this cluster.",
            analysis.chain_id,
        ),
        render_strategy_detail_report(
            "Liquidation report",
            analysis.liquidation_report.as_ref(),
            "No liquidation behavior has been classified for this cluster.",
            analysis.chain_id,
        ),
        render_table_or_empty(
            &representative_rows,
            "No representative transactions are available yet.",
            "<tr><th>Tx</th><th>Behavior</th><th>Subtype</th><th>Protocol</th><th>Address</th><th>Route</th><th>PnL</th><th>Gas</th><th>Block / time</th></tr>",
        ),
        render_table_or_empty(
            &render_route_rows(&analysis.top_routes, 10),
            "No route statistics have been classified yet.",
            "<tr><th>Route</th><th>Tx</th><th>Success</th><th>Rate</th><th>PnL</th><th>Gas</th><th>Samples</th></tr>",
        ),
        render_table_or_empty(
            &render_cluster_protocol_rows(&analysis.top_protocols, 10),
            "No protocol statistics have been classified yet.",
            "<tr><th>Protocol</th><th>Tx</th><th>Arb tx</th><th>Success</th><th>Rate</th><th>PnL</th><th>Samples</th></tr>",
        ),
        render_table_or_empty(
            &role_rows,
            "No address role statistics have been classified yet.",
            "<tr><th>Address</th><th>Configured</th><th>Inferred</th><th>Tx</th><th>Arb tx</th><th>PnL</th><th>Samples</th></tr>",
        ),
    )
}

fn render_strategy_detail_report(
    title: &str,
    report: Option<&StrategyDetailReportPayload>,
    empty_message: &str,
    chain_id: u64,
) -> String {
    let Some(report) = report else {
        return format!(
            r#"<section class="strategy-detail-report"><div class="section-head"><h3>{}</h3><span class="badge">none</span></div><div class="empty-state">{}</div></section>"#,
            escape_html(title),
            escape_html(empty_message)
        );
    };
    format!(
        r#"<section class="strategy-detail-report">
  <div class="section-head">
    <h3>{}</h3>
    <span class="badge">{}</span>
  </div>
  <div class="stats-grid compact-stats-grid">
    <div class="stat"><span>Tx</span><strong>{}</strong><small>{} success / {} failed</small></div>
    <div class="stat"><span>Success rate</span><strong>{}</strong><small>strategy only</small></div>
    <div class="stat"><span>Estimated PnL</span><strong>{}</strong><small>avg {}</small></div>
    <div class="stat"><span>Gas cost</span><strong>{}</strong><small>avg {}</small></div>
    <div class="stat"><span>PnL / gas</span><strong>{}</strong><small>strategy only</small></div>
  </div>
  <div class="report-table-group">
    <h4>Routes</h4>
    {}
    <h4>Protocols</h4>
    {}
    <h4>Sample transactions</h4>
    {}
  </div>
</section>"#,
        escape_html(title),
        escape_html(&report.strategy_type),
        report.tx_count,
        report.success_count,
        report.failed_count,
        format_percent_option(report.success_rate),
        format_usd_option(report.estimated_pnl_usd),
        format_usd_option(report.avg_estimated_pnl_usd),
        format_usd_option(report.gas_cost_usd),
        format_usd_option(report.avg_gas_cost_usd),
        format_ratio_option(report.pnl_to_gas_ratio),
        render_table_or_empty(
            &render_route_rows(&report.top_routes, 6),
            "No routes have been classified for this strategy.",
            "<tr><th>Route</th><th>Tx</th><th>Success</th><th>Rate</th><th>PnL</th><th>Gas</th><th>Samples</th></tr>",
        ),
        render_table_or_empty(
            &render_strategy_protocol_rows(&report.top_protocols, 6),
            "No protocol statistics have been classified for this strategy.",
            "<tr><th>Protocol</th><th>Tx</th><th>Success</th><th>Rate</th><th>PnL</th><th>Gas</th><th>Samples</th></tr>",
        ),
        render_table_or_empty(
            &render_representative_transaction_rows(
                &report.representative_transactions,
                chain_id,
                6
            ),
            "No representative transactions are available for this strategy.",
            "<tr><th>Tx</th><th>Behavior</th><th>Subtype</th><th>Protocol</th><th>Address</th><th>Route</th><th>PnL</th><th>Gas</th><th>Block / time</th></tr>",
        ),
    )
}

fn render_route_rows(routes: &[ClusterRouteStatPayload], limit: usize) -> String {
    routes
        .iter()
        .take(limit)
        .map(|item| {
            format!(
                "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
                escape_html(&item.route),
                item.tx_count,
                item.success_count,
                format_percent_option(item.success_rate),
                format_usd_option(item.estimated_pnl_usd),
                format_usd_option(item.gas_cost_usd),
                render_sample_hashes(&item.sample_tx_hashes),
            )
        })
        .collect::<Vec<_>>()
        .join("")
}

fn render_cluster_protocol_rows(protocols: &[ClusterProtocolStatPayload], limit: usize) -> String {
    protocols
        .iter()
        .take(limit)
        .map(|item| {
            format!(
                "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
                escape_html(&item.protocol),
                item.tx_count,
                item.arbitrage_tx_count,
                item.success_count,
                format_percent_option(item.success_rate),
                format_usd_option(item.estimated_pnl_usd),
                render_sample_hashes(&item.sample_tx_hashes),
            )
        })
        .collect::<Vec<_>>()
        .join("")
}

fn render_strategy_protocol_rows(
    protocols: &[StrategyProtocolStatPayload],
    limit: usize,
) -> String {
    protocols
        .iter()
        .take(limit)
        .map(|item| {
            format!(
                "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
                escape_html(&item.protocol),
                item.tx_count,
                item.success_count,
                format_percent_option(item.success_rate),
                format_usd_option(item.estimated_pnl_usd),
                format_usd_option(item.gas_cost_usd),
                render_sample_hashes(&item.sample_tx_hashes),
            )
        })
        .collect::<Vec<_>>()
        .join("")
}

fn render_representative_transaction_rows(
    transactions: &[RepresentativeTransactionPayload],
    chain_id: u64,
    limit: usize,
) -> String {
    transactions
        .iter()
        .take(limit)
        .map(|tx| {
            let protocol = tx.protocol.as_deref().unwrap_or("n/a");
            let family = tx.protocol_family.as_deref().unwrap_or("n/a");
            let timestamp = tx.timestamp.as_deref().unwrap_or("n/a");
            format!(
                "<tr><td>{}</td><td>{}</td><td>{}</td><td><strong>{}</strong><small>{}</small></td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td><strong>{}</strong><small>{}</small></td></tr>",
                render_tx_hash_action(&tx.tx_hash, chain_id),
                escape_html(&tx.behavior_tag),
                escape_html(&tx.strategy_subtype),
                escape_html(protocol),
                escape_html(family),
                render_copyable_code(&tx.address, 6),
                escape_html(&tx.route),
                format_usd_option(tx.estimated_pnl_usd),
                format_usd_option(tx.gas_cost_usd),
                tx.block_number,
                escape_html(timestamp),
            )
        })
        .collect::<Vec<_>>()
        .join("")
}

fn render_tx_hash_action(tx_hash: &str, chain_id: u64) -> String {
    format!(
        r#"<div class="tx-hash-action">{}<form class="inline-action-form compact" method="post" action="/app/tx-analysis/run"><input type="hidden" name="chain_id" value="{}"><input type="hidden" name="tx_hash" value="{}"><button class="copy-button" type="submit" name="mode" value="analyze">Open</button></form></div>"#,
        render_copyable_code(tx_hash, 8),
        chain_id,
        escape_html(tx_hash),
    )
}

fn report_window_label(start: Option<&str>, end: Option<&str>) -> String {
    match (start, end) {
        (None, None) => "all time".to_owned(),
        (Some(start), None) => format!("from {start}"),
        (None, Some(end)) => format!("until {end}"),
        (Some(start), Some(end)) => format!("{start} to {end}"),
    }
}

fn render_table_or_empty(rows: &str, empty_message: &str, header: &str) -> String {
    if rows.is_empty() {
        return format!(
            "<div class=\"empty-state\">{}</div>",
            escape_html(empty_message)
        );
    }
    format!(
        r#"<div class="table-shell"><table><thead>{}</thead><tbody>{}</tbody></table></div>"#,
        header, rows
    )
}

fn render_sample_hashes(hashes: &[String]) -> String {
    if hashes.is_empty() {
        return "n/a".to_owned();
    }
    hashes
        .iter()
        .take(3)
        .map(|hash| format!("<code>{}</code>", escape_html(&truncate_middle(hash, 6))))
        .collect::<Vec<_>>()
        .join(" ")
}

fn json_value_strings(value: Option<serde_json::Value>) -> Vec<String> {
    value
        .and_then(|value| {
            value.as_array().map(|items| {
                items
                    .iter()
                    .filter_map(serde_json::Value::as_str)
                    .map(ToOwned::to_owned)
                    .collect()
            })
        })
        .unwrap_or_default()
}

fn render_cluster_alerts_panel(
    cluster: &DashboardClusterRecord,
    alerts: &[ClusterAlert],
) -> String {
    if alerts.is_empty() {
        return "<div class=\"empty-state\">No active alerts right now. Alert rules run after rolling metrics are refreshed.</div>".to_owned();
    }
    format!(
        r#"<div class="alert-list">{}</div>"#,
        alerts
            .iter()
            .map(|alert| {
                format!(
                    r#"<article class="alert-card severity-{}">
  <div class="alert-card-head">
    <strong>{}</strong>
    <span>{}</span>
  </div>
  <p>{}</p>
  <small>{}</small>
  <form method="post" action="/app/clusters/{}/alerts/{}/read">
    <button class="cta ghost" type="submit">Mark as read</button>
  </form>
</article>"#,
                    alert_severity_class(alert),
                    escape_html(&alert.title),
                    escape_html(&format!("{:?}", alert.severity).to_lowercase()),
                    escape_html(&alert.message),
                    escape_html(
                        &alert
                            .triggered_at
                            .format("%Y-%m-%d %H:%M:%S UTC")
                            .to_string()
                    ),
                    cluster.cluster_id,
                    alert.alert_id
                )
            })
            .collect::<Vec<_>>()
            .join("")
    )
}

fn render_recent_classified_transactions_panel(
    cluster: &DashboardClusterRecord,
    page: Option<&ClassifiedTransactionsPagePayload>,
    query: &ClusterTxTableState,
) -> String {
    let Some(page) = page else {
        return "<div class=\"empty-state\">No classified transactions yet. Once tracking or backfill analyzes transactions, they will appear here.</div>".to_owned();
    };
    if page.items.is_empty() {
        return format!(
            r#"<form class="stack-form compact-form" method="get" action="/app/clusters/{}">
  <input type="hidden" name="cluster_tab" value="activity">
  {}
  <div class="button-row">
    <button class="cta secondary" type="submit">Apply filters</button>
    <a class="cta ghost" href="/app/clusters/{}?cluster_tab=activity">Reset</a>
  </div>
</form>
<div class="empty-state">No classified transactions matched the current filters.</div>"#,
            cluster.cluster_id,
            render_classified_transaction_filters(query),
            cluster.cluster_id
        );
    }
    let total_pages = ((page.total + page.page_size as u64 - 1) / page.page_size as u64).max(1);
    let previous_page = page.page.saturating_sub(1).max(1);
    let next_page = (page.page + 1).min(total_pages as u32);
    format!(
        r#"<form class="stack-form compact-form" method="get" action="/app/clusters/{}">
  <input type="hidden" name="cluster_tab" value="activity">
  {}
  <div class="button-row">
    <button class="cta secondary" type="submit">Apply filters</button>
    <a class="cta ghost" href="/app/clusters/{}?cluster_tab=activity">Reset</a>
  </div>
</form>
<div class="field-hint">Showing page {} of {} · {} total classified transactions.</div>
<div class="table-shell">
  <table>
    <thead>
      <tr>
        <th>Tx</th>
        <th>Time</th>
        <th>Address</th>
        <th>Behavior</th>
        <th>Protocol</th>
        <th>Token in / out</th>
        <th>Volume USD</th>
        <th>Gas USD</th>
        <th>Estimated PnL USD</th>
      </tr>
    </thead>
    <tbody>
      {}
    </tbody>
  </table>
</div>
<div class="button-row pagination-row">
  {}
  {}
</div>"#,
        cluster.cluster_id,
        render_classified_transaction_filters(query),
        cluster.cluster_id,
        page.page,
        total_pages,
        page.total,
        page.items
            .iter()
            .map(|tx| {
                format!(
                    "<tr><td><code>{}</code></td><td>{}</td><td><code>{}</code></td><td>{:?}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
                    escape_html(&truncate_middle(&tx.tx_hash, 8)),
                    tx.timestamp
                        .map(|value| value.format("%Y-%m-%d %H:%M:%S UTC").to_string())
                        .unwrap_or_else(|| "n/a".to_owned()),
                    escape_html(&truncate_middle(&tx.address, 6)),
                    tx.behavior_tag,
                    escape_html(tx.protocol.as_deref().unwrap_or("n/a")),
                    escape_html(&format_token_flow(tx)),
                    format_usd_option(tx.value_usd),
                    tx.gas_cost_usd
                        .map(|value| format!("{value:.2}"))
                        .unwrap_or_else(|| "n/a".to_owned()),
                    tx.estimated_pnl_usd
                        .map(|value| format!("{value:.2}"))
                        .unwrap_or_else(|| "n/a".to_owned())
                )
            })
            .collect::<Vec<_>>()
            .join(""),
        if page.page > 1 {
            format!(
                "<a class=\"cta ghost\" href=\"{}\">Previous</a>",
                classified_page_href(cluster.cluster_id, query, previous_page)
            )
        } else {
            "<span></span>".to_owned()
        },
        if page.page < total_pages as u32 {
            format!(
                "<a class=\"cta ghost\" href=\"{}\">Next</a>",
                classified_page_href(cluster.cluster_id, query, next_page)
            )
        } else {
            "<span></span>".to_owned()
        }
    )
}

fn alert_severity_class(alert: &ClusterAlert) -> &'static str {
    match alert.severity {
        crate::domain::enums::AlertSeverity::Low => "low",
        crate::domain::enums::AlertSeverity::Medium => "medium",
        crate::domain::enums::AlertSeverity::High => "high",
    }
}

fn format_usd_option(value: Option<f64>) -> String {
    value
        .map(|value| format!("${value:.2}"))
        .unwrap_or_else(|| "n/a".to_owned())
}

fn format_roi_option(value: Option<f64>) -> String {
    value
        .map(|value| format!("{:.2}%", value * 100.0))
        .unwrap_or_else(|| "n/a".to_owned())
}

fn format_ratio_option(value: Option<f64>) -> String {
    value
        .map(|value| format!("{value:.2}x"))
        .unwrap_or_else(|| "n/a".to_owned())
}

fn format_percent_option(value: Option<f64>) -> String {
    value
        .map(|value| format!("{:.2}%", value * 100.0))
        .unwrap_or_else(|| "n/a".to_owned())
}

fn format_token_flow(tx: &ClassifiedTransaction) -> String {
    let token_in = tx
        .token_in
        .as_deref()
        .map(|value| truncate_middle(value, 6))
        .unwrap_or_else(|| "-".to_owned());
    let token_out = tx
        .token_out
        .as_deref()
        .map(|value| truncate_middle(value, 6))
        .unwrap_or_else(|| "-".to_owned());
    format!("{token_in} / {token_out}")
}

fn render_classified_transaction_filters(query: &ClusterTxTableState) -> String {
    format!(
        r#"<div class="split-fields">
  <label>Behavior
    <select name="tx_behavior">
      <option value="" {}>all</option>
      <option value="arbitrage" {}>arbitrage</option>
      <option value="position" {}>position</option>
      <option value="lp" {}>lp</option>
      <option value="liquidation" {}>liquidation</option>
      <option value="unknown" {}>unknown</option>
    </select>
  </label>
  <label>Protocol contains<input type="text" name="tx_protocol" value="{}" placeholder="aave / morpho / custom router"></label>
</div>
<div class="split-fields">
  <label>Sort by
    <select name="tx_sort_by">
      <option value="timestamp" {}>time</option>
      <option value="block_number" {}>block</option>
      <option value="value_usd" {}>volume usd</option>
      <option value="gas_cost_usd" {}>gas usd</option>
      <option value="estimated_pnl_usd" {}>estimated pnl usd</option>
    </select>
  </label>
  <label>Direction
    <select name="tx_sort_dir">
      <option value="desc" {}>desc</option>
      <option value="asc" {}>asc</option>
    </select>
  </label>
  <label>Page size
    <select name="tx_page_size">
      <option value="10" {}>10</option>
      <option value="25" {}>25</option>
      <option value="50" {}>50</option>
    </select>
  </label>
</div>
<input type="hidden" name="tx_page" value="1">"#,
        if query.behavior_tag.is_none() {
            "selected"
        } else {
            ""
        },
        if query.behavior_tag.as_deref() == Some("arbitrage") {
            "selected"
        } else {
            ""
        },
        if query.behavior_tag.as_deref() == Some("position") {
            "selected"
        } else {
            ""
        },
        if query.behavior_tag.as_deref() == Some("lp") {
            "selected"
        } else {
            ""
        },
        if query.behavior_tag.as_deref() == Some("liquidation") {
            "selected"
        } else {
            ""
        },
        if query.behavior_tag.as_deref() == Some("unknown") {
            "selected"
        } else {
            ""
        },
        escape_html(query.protocol.as_deref().unwrap_or_default()),
        if query.sort_by == "timestamp" {
            "selected"
        } else {
            ""
        },
        if query.sort_by == "block_number" {
            "selected"
        } else {
            ""
        },
        if query.sort_by == "value_usd" {
            "selected"
        } else {
            ""
        },
        if query.sort_by == "gas_cost_usd" {
            "selected"
        } else {
            ""
        },
        if query.sort_by == "estimated_pnl_usd" {
            "selected"
        } else {
            ""
        },
        if query.sort_dir == "desc" {
            "selected"
        } else {
            ""
        },
        if query.sort_dir == "asc" {
            "selected"
        } else {
            ""
        },
        if query.page_size == 10 {
            "selected"
        } else {
            ""
        },
        if query.page_size == 25 {
            "selected"
        } else {
            ""
        },
        if query.page_size == 50 {
            "selected"
        } else {
            ""
        },
    )
}

fn classified_page_href(cluster_id: Uuid, query: &ClusterTxTableState, page: u32) -> String {
    let mut params = vec![
        "cluster_tab=activity".to_owned(),
        format!("tx_page={page}"),
        format!("tx_page_size={}", query.page_size),
        format!("tx_sort_by={}", urlencoding::encode(&query.sort_by)),
        format!("tx_sort_dir={}", urlencoding::encode(&query.sort_dir)),
    ];
    if let Some(behavior_tag) = query.behavior_tag.as_deref() {
        params.push(format!("tx_behavior={}", urlencoding::encode(behavior_tag)));
    }
    if let Some(protocol) = query.protocol.as_deref() {
        params.push(format!("tx_protocol={}", urlencoding::encode(protocol)));
    }
    format!("/app/clusters/{cluster_id}?{}", params.join("&"))
}

fn cluster_analysis_tab_href(
    cluster_id: Uuid,
    start: Option<&str>,
    end: Option<&str>,
    tab: ClusterAnalysisTab,
) -> String {
    let mut params = vec![format!("cluster_tab={}", tab.as_str())];
    if let Some(start) = start.filter(|value| !value.is_empty()) {
        params.push(format!("start={}", urlencoding::encode(start)));
    }
    if let Some(end) = end.filter(|value| !value.is_empty()) {
        params.push(format!("end={}", urlencoding::encode(end)));
    }
    format!("/app/clusters/{cluster_id}?{}", params.join("&"))
}

fn render_asset_ref(
    asset_type: &str,
    token_address: Option<&str>,
    token_symbol: Option<&str>,
    token_name: Option<&str>,
) -> String {
    if asset_type == "native" {
        return "<code>native ETH</code>".to_owned();
    }
    if token_symbol.is_some() || token_name.is_some() {
        let label = token_symbol
            .filter(|value| !value.trim().is_empty())
            .or(token_name)
            .unwrap_or("ERC20");
        let address = token_address
            .map(|value| render_copyable_code(value, 6))
            .unwrap_or_default();
        return format!(
            "<span>{}</span><small>{}</small>",
            escape_html(label),
            address
        );
    }
    token_address
        .map(|value| render_copyable_code(value, 6))
        .unwrap_or_else(|| format!("<code>{}</code>", escape_html(asset_type)))
}

fn format_asset_amount(asset_type: &str, raw: &str) -> String {
    if asset_type == "native" {
        return format_signed_wei_compact(raw);
    }
    raw.to_owned()
}

fn format_asset_display_amount(
    asset_type: &str,
    raw: &str,
    decimal: Option<&str>,
    token_symbol: Option<&str>,
) -> String {
    if let Some(decimal) = decimal {
        let symbol = if asset_type == "native" {
            "ETH"
        } else {
            token_symbol
                .filter(|value| !value.trim().is_empty())
                .unwrap_or("tokens")
        };
        return format!("{decimal} {symbol}");
    }
    format_asset_amount(asset_type, raw)
}

fn format_usd_cell(value: Option<&str>) -> String {
    value
        .map(|value| format!("${value}"))
        .unwrap_or_else(|| "n/a".to_owned())
}

fn format_signed_wei_compact(raw: &str) -> String {
    let trimmed = raw.trim();
    let negative = trimmed.starts_with('-');
    let unsigned = trimmed.trim_start_matches('-');
    let mut formatted = format_wei_compact(unsigned);
    if negative && formatted != "0 ETH" {
        formatted.insert(0, '-');
    }
    formatted
}

fn format_wei_compact(raw: &str) -> String {
    let Ok(value) = raw.trim().parse::<f64>() else {
        return raw.to_owned();
    };
    let eth = value / 1_000_000_000_000_000_000_f64;
    if eth >= 1000.0 {
        format!("{eth:.2} ETH")
    } else if eth >= 1.0 {
        format!("{eth:.4} ETH")
    } else if eth > 0.0 {
        format!("{eth:.6} ETH")
    } else {
        "0 ETH".to_owned()
    }
}

fn render_indexer_progress_panel(progress: Option<&IndexerProgressPayload>) -> String {
    let body = progress
        .map(|progress| {
            format!(
                r#"<div class="stats-grid">
  <div class="stat"><span>Chain ID</span><strong>{}</strong></div>
  <div class="stat"><span>Worker ID</span><strong>{}</strong></div>
  <div class="stat"><span>Last indexed block</span><strong>{}</strong></div>
  <div class="stat"><span>Pending from/to backfill rows</span><strong>{}</strong></div>
</div>
<div class="panel-copy">This portal keeps indexer and analysis separate. The indexer discovers address-level transaction references; the analysis stack consumes those references for cluster backfill, tx interpretation, and spend attribution.</div>
<div class="button-row">
  <a class="cta secondary" href="/app/indexer/address-lookup">Open address lookup</a>
  <a class="cta ghost" href="/app/clusters">Open clusters</a>
</div>"#,
                progress.chain_id,
                escape_html(&progress.worker_id),
                progress.last_indexed_block,
                progress.endpoint_backfill_pending_rows.unwrap_or(0)
            )
        })
        .unwrap_or_else(|| {
            "<div class=\"empty-state\">Indexer progress is unavailable right now. Check `LEDGERFLOW_INDEXER_API_BASE_URL` and the bearer token configuration.</div>".to_owned()
        });

    format!(
        r#"<section class="panel">
  <div class="panel-head"><h2>Indexer progress</h2><span class="badge">discovery layer</span></div>
  <div class="panel-body">{}</div>
</section>"#,
        body
    )
}

fn render_indexer_api_keys_panel(
    user: &AppUser,
    indexer_api_keys: &[IndexerApiKeyPayload],
    notice: Option<&str>,
    created_key: Option<&str>,
) -> String {
    let one_time_key = created_key
        .filter(|value| !value.trim().is_empty())
        .map(|value| {
            format!(
                r#"<div class="panel-copy success-copy">
  <strong>New indexer API key</strong>
  <p>Copy this token now. It will not be shown again.</p>
  <code class="inline-code-block">{}</code>
</div>"#,
                escape_html(value)
            )
        })
        .unwrap_or_default();

    let list_body = if indexer_api_keys.is_empty() {
        "<div class=\"empty-state\">No external indexer API keys yet. Create one for scripts, dashboards, or client integrations that should call the indexer directly.</div>".to_owned()
    } else {
        format!(
            r#"<div class="table-shell">
  <table>
    <thead>
      <tr>
        <th>Label</th>
        <th>Prefix</th>
        <th>Status</th>
        <th>Created</th>
        <th>Last used</th>
        <th>Action</th>
      </tr>
    </thead>
    <tbody>
      {}
    </tbody>
  </table>
</div>"#,
            indexer_api_keys
                .iter()
                .map(|key| {
                    let action = if key.status.eq_ignore_ascii_case("active") {
                        format!(
                            r#"<form method="post" action="/app/indexer/api-keys/{}/revoke" onsubmit="return confirm('Revoke this API key? Existing clients using it will stop working.');">
  <button class="cta ghost tiny danger" type="submit">Revoke</button>
</form>"#,
                            escape_html(&key.key_id)
                        )
                    } else {
                        "<span class=\"muted\">revoked</span>".to_owned()
                    };
                    format!(
                        r#"<tr>
  <td>{}</td>
  <td><code>{}</code></td>
  <td><span class="badge">{}</span></td>
  <td>{}</td>
  <td>{}</td>
  <td>{}</td>
</tr>"#,
                        escape_html(&key.label),
                        escape_html(&key.token_prefix),
                        escape_html(&key.status),
                        escape_html(&key.created_at.replace('T', " ").replace('Z', " UTC")),
                        key.last_used_at
                            .as_deref()
                            .map(|value| escape_html(&value.replace('T', " ").replace('Z', " UTC")))
                            .unwrap_or_else(|| "never".to_owned()),
                        action
                    )
                })
                .collect::<Vec<_>>()
                .join("")
        )
    };

    format!(
        r#"<section class="panel">
  <div class="panel-head"><h2>Indexer API keys</h2><span class="badge">external access</span></div>
  <div class="panel-body">
    <div class="panel-copy">
      <p>These keys are for external clients that call the indexer API directly. The signed-in portal keeps using the internal service token and does not consume these keys.</p>
      <p>Public indexer endpoints accept either <code>Authorization: Bearer &lt;api_key&gt;</code> or <code>x-api-key: &lt;api_key&gt;</code>.</p>
      <p>Owner: <strong>{}</strong></p>
    </div>
    {}
    {}
    <form class="stack-form" method="post" action="/app/indexer/api-keys/create">
      <label>Label<input type="text" name="label" placeholder="research dashboard / analytics bot / partner export" required></label>
      <button class="cta secondary" type="submit">Create API key</button>
    </form>
    {}
  </div>
</section>"#,
        escape_html(&user.email),
        render_notice_block(notice),
        one_time_key,
        list_body
    )
}

fn render_indexer_lookup_panel(
    lookup: Option<&IndexerAddressRefsPayload>,
    query: Option<&IndexerLookupQuery>,
) -> String {
    let address = query
        .and_then(|value| value.address.as_deref())
        .unwrap_or_default();
    let direction = query
        .and_then(|value| value.direction.as_deref())
        .unwrap_or_default();
    let limit = query.and_then(|value| value.limit).unwrap_or(100);
    let from_block = query
        .and_then(|value| value.from_block)
        .map(|value| value.to_string())
        .unwrap_or_default();
    let to_block = query
        .and_then(|value| value.to_block)
        .map(|value| value.to_string())
        .unwrap_or_default();
    let results = lookup
        .map(|payload| {
            if payload.refs.is_empty() {
                return "<div class=\"empty-state\">No top-level transaction references matched this query.</div>"
                    .to_owned();
            }
            format!(
                r#"<div class="stats-grid">
  <div class="stat"><span>Address</span><strong><code>{}</code></strong></div>
  <div class="stat"><span>Hits returned</span><strong>{}</strong></div>
</div>
<div class="table-shell">
  <table>
    <thead>
      <tr>
        <th>Tx hash</th>
        <th>Block</th>
        <th>Index</th>
        <th>Direction</th>
        <th>Chain</th>
      </tr>
    </thead>
    <tbody>
      {}
    </tbody>
  </table>
</div>"#,
                escape_html(&payload.address),
                payload.refs.len(),
                payload
                    .refs
                    .iter()
                    .take(200)
                    .map(|reference| {
                        format!(
                            "<tr><td><code>{}</code></td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
                            escape_html(&truncate_middle(&reference.tx_hash, 8)),
                            reference.block_number,
                            reference.tx_index,
                            match reference.direction {
                                AddressHitDirection::From => "from",
                                AddressHitDirection::To => "to",
                            },
                            reference.chain_id
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("")
            )
        })
        .unwrap_or_else(|| {
            "<div class=\"empty-state\">Query an address to inspect the raw discovery layer before you attach it to a cluster.</div>".to_owned()
        });

    format!(
        r#"<section class="panel">
  <div class="panel-head"><h2>Indexer address lookup</h2><span class="badge">producer view</span></div>
  <div class="panel-body">
    <p class="panel-copy">This page talks to the indexer directly under the same login session. Use it to verify address discovery, block windows, and sender/receiver direction before or after cluster onboarding.</p>
    <form class="stack-form" method="get" action="/app/indexer/address-lookup">
      <label>Address<input type="text" name="address" value="{}" placeholder="0x..." required></label>
      <div class="split-fields">
        <label>Limit<input type="number" name="limit" value="{}" min="1" max="10000"></label>
        <label>Direction
          <select name="direction">
            <option value="" {}>all</option>
            <option value="from" {}>from</option>
            <option value="to" {}>to</option>
          </select>
        </label>
      </div>
      <div class="split-fields">
        <label>From block<input type="number" name="from_block" value="{}" min="0"></label>
        <label>To block<input type="number" name="to_block" value="{}" min="0"></label>
      </div>
      <div class="button-row">
        <button class="cta secondary" type="submit">Query address refs</button>
        <a class="cta ghost" href="/app/indexer/progress">View indexer progress</a>
      </div>
    </form>
    {}
  </div>
</section>"#,
        escape_html(address),
        limit,
        if direction.is_empty() { "selected" } else { "" },
        if direction == "from" { "selected" } else { "" },
        if direction == "to" { "selected" } else { "" },
        escape_html(&from_block),
        escape_html(&to_block),
        results
    )
}

fn derive_cluster_status(coverage: Option<&ClusterCoverage>) -> (String, String) {
    if let Some(coverage) = coverage {
        if coverage.pending_tx_count > 0 {
            return ("Backfill in progress".to_owned(), "info".to_owned());
        }
        if coverage.failed_tx_count > 0 {
            return (
                "Analysis completed with failures".to_owned(),
                "warning".to_owned(),
            );
        }
        if coverage.discovered_tx_count > 0
            && coverage.analyzed_tx_count >= coverage.discovered_tx_count
        {
            return ("History fully analyzed".to_owned(), "success".to_owned());
        }
        if matches!(
            coverage.latest_run_status,
            Some(ClusterBackfillStatus::Pending | ClusterBackfillStatus::Running)
        ) {
            return if coverage.discovered_tx_count > 0 {
                ("Incremental sync queued".to_owned(), "info".to_owned())
            } else {
                (
                    "Waiting for first historical sync".to_owned(),
                    "info".to_owned(),
                )
            };
        }
    }

    (
        "Waiting for first historical sync".to_owned(),
        "info".to_owned(),
    )
}

fn render_cluster_coverage_cards(coverage: Option<&ClusterCoverage>) -> String {
    coverage
        .map(|coverage| {
            format!(
                r#"<div class="field-hint">Coverage is transaction-level, not block-level. <strong>Discovered</strong> means tx hashes found for this cluster, <strong>Analyzed</strong> means analysis rows already exist, <strong>Failed</strong> means analysis errored, and <strong>Pending</strong> means discovery happened but analysis has not finished yet.</div>
<div class="stats-grid">
  <div class="stat"><span>Discovered</span><strong>{}</strong></div>
  <div class="stat"><span>Analyzed</span><strong>{}</strong></div>
  <div class="stat"><span>Failed</span><strong>{}</strong></div>
  <div class="stat"><span>Pending</span><strong>{}</strong></div>
</div>"#,
                coverage.discovered_tx_count,
                coverage.analyzed_tx_count,
                coverage.failed_tx_count,
                coverage.pending_tx_count
            )
        })
        .unwrap_or_else(|| {
            "<div class=\"empty-state\">Coverage is not available yet. The worker may still be discovering transactions.</div>".to_owned()
        })
}

fn render_cluster_asset_changes_panel(asset_changes: Option<&ClusterAssetChangeReport>) -> String {
    asset_changes
        .map(|report| {
            let daily_rows = if report.daily_changes.is_empty() {
                "<tr><td colspan=\"9\">No analyzed asset changes in this window yet.</td></tr>"
                    .to_owned()
            } else {
                report
                    .daily_changes
                    .iter()
                    .take(40)
                    .map(|row| {
                        let asset_type = enum_label(&row.asset_type);
                        format!(
                            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
                            escape_html(&row.day),
                            render_asset_ref(
                                &asset_type,
                                row.token_address.as_deref(),
                                row.token_symbol.as_deref(),
                                row.token_name.as_deref()
                            ),
                            escape_html(&format_asset_display_amount(
                                &asset_type,
                                &row.incoming_raw,
                                row.incoming_decimal.as_deref(),
                                row.token_symbol.as_deref()
                            )),
                            escape_html(&format_asset_display_amount(
                                &asset_type,
                                &row.outgoing_raw,
                                row.outgoing_decimal.as_deref(),
                                row.token_symbol.as_deref()
                            )),
                            escape_html(&format_asset_display_amount(
                                &asset_type,
                                &row.gas_raw,
                                row.gas_decimal.as_deref(),
                                row.token_symbol.as_deref()
                            )),
                            escape_html(&format_asset_display_amount(
                                &asset_type,
                                &row.internal_raw,
                                row.internal_decimal.as_deref(),
                                row.token_symbol.as_deref()
                            )),
                            escape_html(&format_asset_display_amount(
                                &asset_type,
                                &row.net_raw,
                                row.net_decimal.as_deref(),
                                row.token_symbol.as_deref()
                            )),
                            escape_html(&format_usd_cell(row.net_usd.as_deref())),
                            row.tx_count,
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("")
            };
            let purpose_rows = if report.purpose_breakdowns.is_empty() {
                "<tr><td colspan=\"8\">No source/use breakdowns in this window yet.</td></tr>"
                    .to_owned()
            } else {
                report
                    .purpose_breakdowns
                    .iter()
                    .take(80)
                    .map(|row| {
                        let asset_type = enum_label(&row.asset_type);
                        let sample = row
                            .sample_tx_hashes
                            .first()
                            .map(|value| render_copyable_code(value, 6))
                            .unwrap_or_else(|| "n/a".to_owned());
                        format!(
                            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
                            escape_html(&row.day),
                            render_asset_ref(
                                &asset_type,
                                row.token_address.as_deref(),
                                row.token_symbol.as_deref(),
                                row.token_name.as_deref()
                            ),
                            escape_html(&row.purpose),
                            row.counterparty
                                .as_deref()
                                .map(|value| render_copyable_code(value, 6))
                                .unwrap_or_else(|| "n/a".to_owned()),
                            escape_html(&format_asset_display_amount(
                                &asset_type,
                                &row.amount_raw,
                                row.amount_decimal.as_deref(),
                                row.token_symbol.as_deref()
                            )),
                            escape_html(&format_usd_cell(row.amount_usd.as_deref())),
                            row.tx_count,
                            sample,
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("")
            };
            let warnings = report
                .metadata
                .get("warnings")
                .and_then(|value| value.as_array())
                .map(|items| {
                    items
                        .iter()
                        .filter_map(|item| item.as_str())
                        .collect::<Vec<_>>()
                })
                .filter(|items| !items.is_empty())
                .map(|items| {
                    format!(
                        "<div class=\"notice warning\">{}</div>",
                        escape_html(&items.join(" | "))
                    )
                })
                .unwrap_or_default();
            format!(
                r#"<div class="stats-grid">
  <div class="stat"><span>Analyzed txs</span><strong>{}</strong></div>
  <div class="stat"><span>Assets</span><strong>{}</strong></div>
  <div class="stat"><span>Breakdowns</span><strong>{}</strong></div>
  <div class="stat"><span>Formula</span><strong>in - out - gas</strong><small>internal excluded from net</small></div>
</div>
{}
<div class="table-shell">
  <table>
    <thead><tr><th>Day</th><th>Asset</th><th>In</th><th>Out</th><th>Gas</th><th>Internal</th><th>Net</th><th>Net USD</th><th>Txs</th></tr></thead>
    <tbody>{}</tbody>
  </table>
</div>
<div class="table-shell">
  <table>
    <thead><tr><th>Day</th><th>Asset</th><th>Source / use</th><th>Counterparty</th><th>Amount</th><th>USD</th><th>Txs</th><th>Sample</th></tr></thead>
    <tbody>{}</tbody>
  </table>
</div>"#,
                report.analyzed_tx_count,
                report.daily_changes.len(),
                report.purpose_breakdowns.len(),
                warnings,
                daily_rows,
                purpose_rows,
            )
        })
        .unwrap_or_else(|| {
            "<div class=\"empty-state\">Daily asset changes will appear once analyzed transactions exist for this cluster.</div>".to_owned()
        })
}

fn render_cluster_summary_panel(spend_summary: Option<&ClusterSpendSummary>) -> String {
    spend_summary
        .map(|summary| {
            format!(
                r#"<div class="stats-grid">
  <div class="stat"><span>Analyzed txs</span><strong>{}</strong></div>
  <div class="stat"><span>Gas spent</span><strong>{}</strong></div>
  <div class="stat"><span>Native outflow</span><strong>{}</strong></div>
  <div class="stat"><span>Incoming native</span><strong>{}</strong></div>
</div>
<div class="table-shell">
  <table>
    <thead>
      <tr>
        <th>Tx</th>
        <th>Block time</th>
        <th>Status</th>
        <th>Recognizers</th>
        <th>Gas</th>
        <th>Native outflow</th>
      </tr>
    </thead>
    <tbody>
      {}
    </tbody>
  </table>
</div>"#,
                summary.analyzed_tx_count,
                escape_html(&format_wei_compact(&summary.total_gas_cost_raw)),
                escape_html(&format_wei_compact(&summary.total_external_native_outflow_raw)),
                escape_html(&format_wei_compact(&summary.total_incoming_native_raw)),
                if summary.transactions.is_empty() {
                    "<tr><td colspan=\"6\">No analyzed transactions in this window yet.</td></tr>"
                        .to_owned()
                } else {
                    summary
                        .transactions
                        .iter()
                        .take(12)
                        .map(|tx| {
                            format!(
                                "<tr><td><code>{}</code></td><td>{}</td><td>{:?}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
                                escape_html(&truncate_middle(&tx.tx_hash, 8)),
                                tx.block_timestamp
                                    .map(|value| value.format("%Y-%m-%d %H:%M:%S UTC").to_string())
                                    .unwrap_or_else(|| "n/a".to_owned()),
                                tx.status,
                                escape_html(&tx.recognizer_labels.join(", ")),
                                escape_html(&tx.gas_cost_raw),
                                escape_html(&tx.external_native_outflow_raw)
                            )
                        })
                        .collect::<Vec<_>>()
                        .join("")
                }
            )
        })
        .unwrap_or_else(|| {
            "<div class=\"empty-state\">Spend summary will appear once some discovered transactions have been analyzed.</div>".to_owned()
        })
}

fn render_cluster_autorefresh_script(
    selected_cluster: Option<&DashboardClusterRecord>,
    start: Option<&str>,
    end: Option<&str>,
    selected_tab: &ClusterAnalysisTab,
) -> String {
    let Some(cluster) = selected_cluster else {
        return String::new();
    };

    format!(
        r#"<script>
(() => {{
  const clusterId = {cluster_id:?};
  const start = {start:?};
  const end = {end:?};
  const initialClusterTab = {selected_tab:?};
  const statusNode = document.getElementById("cluster-live-status");
  const trackingNode = document.getElementById("cluster-live-tracking");
  const metricsNode = document.getElementById("cluster-live-metrics");
  const behaviorNode = document.getElementById("cluster-live-behavior");
  const coverageNode = document.getElementById("cluster-live-coverage");
  const alertsNode = document.getElementById("cluster-live-alerts");
  const summaryNode = document.getElementById("cluster-live-summary");
  if (!clusterId || !statusNode) return;

  const escapeHtml = (value) => String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");

  const truncate = (value) => {{
    const text = String(value ?? "");
    return text.length > 16 ? `${{text.slice(0, 8)}}...${{text.slice(-8)}}` : text;
  }};

  const formatUsd = (value) => value == null ? "n/a" : `$${{Number(value).toFixed(2)}}`;
  const formatPct = (value) => value == null ? "n/a" : `${{(Number(value) * 100).toFixed(2)}}%`;
  const formatTime = (value) => value ? String(value).replace("T", " ").replace("Z", " UTC") : "n/a";
  const formatWeiCompact = (value) => {{
    const numeric = Number(value ?? 0);
    if (!Number.isFinite(numeric)) return String(value ?? "n/a");
    const eth = numeric / 1e18;
    if (eth >= 1000) return `${{eth.toFixed(2)}} ETH`;
    if (eth >= 1) return `${{eth.toFixed(4)}} ETH`;
    if (eth > 0) return `${{eth.toFixed(6)}} ETH`;
    return "0 ETH";
  }};

  const renderCoverage = (coverage) => {{
    if (!coverage) {{
      return '<div class="empty-state">Coverage is not available yet. The worker may still be discovering transactions.</div>';
    }}
    return `
      <div class="field-hint">Coverage is transaction-level, not block-level. <strong>Discovered</strong> means tx hashes found for this cluster, <strong>Analyzed</strong> means saved analysis exists, <strong>Failed</strong> means analysis errored, and <strong>Pending</strong> means discovery happened but analysis has not finished yet.</div>
      <div class="stats-grid">
        <div class="stat"><span>Discovered</span><strong>${{coverage.discovered_tx_count}}</strong></div>
        <div class="stat"><span>Analyzed</span><strong>${{coverage.analyzed_tx_count}}</strong></div>
        <div class="stat"><span>Failed</span><strong>${{coverage.failed_tx_count}}</strong></div>
        <div class="stat"><span>Pending</span><strong>${{coverage.pending_tx_count}}</strong></div>
      </div>
    `;
  }};

  const renderTracking = (tracking) => {{
    if (!tracking) {{
      return '<div class="empty-state">Tracking state has not been initialized yet.</div>';
    }}
    const lastProcessedBlock = tracking.last_processed_block ?? "n/a";
    const lastUpdated = tracking.last_updated_at ? String(tracking.last_updated_at).replace("T", " ").replace("Z", " UTC") : "n/a";
    const error = tracking.error_message ? `<div class="notice error">${{escapeHtml(tracking.error_message)}}</div>` : "";
    const actionPath = tracking.status === "active"
      ? `/app/clusters/${{clusterId}}/tracking/pause`
      : `/app/clusters/${{clusterId}}/tracking/enable`;
    const actionLabel = tracking.status === "active" ? "Pause tracking" : "Start tracking";
    return `
      <div class="stats-grid">
        <div class="stat"><span>Status</span><strong>${{escapeHtml(tracking.status)}}</strong></div>
        <div class="stat"><span>Last processed block</span><strong>${{escapeHtml(lastProcessedBlock)}}</strong></div>
        <div class="stat"><span>Last updated</span><strong>${{escapeHtml(lastUpdated)}}</strong></div>
        <div class="stat"><span>Tracked addresses</span><strong>${{tracking.tracked_address_count ?? 0}}</strong></div>
      </div>
      ${{error}}
      <form method="post" action="${{actionPath}}">
        <button class="cta secondary" type="submit">${{actionLabel}}</button>
      </form>
    `;
  }};

  const metricsByWindow = (metrics) => {{
    const map = new Map();
    for (const item of metrics || []) {{
      map.set(item.window, item);
    }}
    return map;
  }};

  const renderMetrics = (metrics) => {{
    if (!metrics || metrics.length === 0) {{
      return '<div class="empty-state">Rolling metrics have not been computed yet. They appear after classified transactions are written for this cluster.</div>';
    }}
    const byWindow = metricsByWindow(metrics);
    const oneHour = byWindow.get("1h");
    const twentyFourHours = byWindow.get("24h");
    const sevenDays = byWindow.get("7d");
    return `
      <div class="field-hint">Rolling metrics are fixed windows for the MVP: <code>1h</code>, <code>24h</code>, and <code>7d</code>. The custom start/end fields above only affect the spend summary table, not these rolling snapshots.</div>
      <div class="stats-grid">
        <div class="stat"><span>1h PnL / ROI</span><strong>${{formatUsd(oneHour?.realized_pnl_usd)}} / ${{formatPct(oneHour?.roi)}}</strong></div>
        <div class="stat"><span>24h PnL / ROI</span><strong>${{formatUsd(twentyFourHours?.realized_pnl_usd)}} / ${{formatPct(twentyFourHours?.roi)}}</strong></div>
        <div class="stat"><span>7d PnL / ROI</span><strong>${{formatUsd(sevenDays?.realized_pnl_usd)}} / ${{formatPct(sevenDays?.roi)}}</strong></div>
        <div class="stat"><span>24h tx count</span><strong>${{twentyFourHours?.tx_count ?? 0}}</strong></div>
        <div class="stat"><span>24h volume</span><strong>${{formatUsd(twentyFourHours?.volume_usd)}}</strong></div>
        <div class="stat"><span>24h gas cost</span><strong>${{formatUsd(twentyFourHours?.gas_cost_usd)}}</strong></div>
      </div>
    `;
  }};

  const renderBehavior = (metrics) => {{
    if (!metrics || metrics.length === 0) {{
      return '<div class="empty-state">Behavior breakdown will appear after enough classified transactions are available.</div>';
    }}
    const snapshot = metricsByWindow(metrics).get("24h");
    if (!snapshot || !snapshot.behavior_breakdown) {{
      return '<div class="empty-state">Behavior breakdown will appear after enough classified transactions are available.</div>';
    }}
    const keys = ["arbitrage", "position", "lp", "liquidation", "unknown"];
    const rows = keys.map((key) => {{
      const entry = snapshot.behavior_breakdown?.[key] || {{}};
      const count = entry.count ?? 0;
      const ratio = Number(entry.ratio ?? 0);
      return `
        <tr>
          <td>${{escapeHtml(key)}}</td>
          <td>${{count}}</td>
          <td>${{(ratio * 100).toFixed(1)}}%</td>
          <td><div class="meter"><span style="width:${{(ratio * 100).toFixed(1)}}%"></span></div></td>
        </tr>
      `;
    }}).join("");
    return `
      <div class="table-shell">
        <table>
          <thead>
            <tr>
              <th>Behavior</th>
              <th>Count</th>
              <th>Ratio</th>
              <th>Visual</th>
            </tr>
          </thead>
          <tbody>${{rows}}</tbody>
        </table>
      </div>
    `;
  }};

  const renderSummary = (summary) => {{
    if (!summary) {{
      return '<div class="empty-state">Spend summary will appear once some discovered transactions have been analyzed.</div>';
    }}
    const rows = (summary.transactions || []).slice(0, 12).map((tx) => `
      <tr>
        <td><code>${{escapeHtml(truncate(tx.tx_hash))}}</code></td>
        <td>${{escapeHtml(tx.block_timestamp ? String(tx.block_timestamp).replace("T", " ").replace("Z", " UTC") : "n/a")}}</td>
        <td>${{escapeHtml(tx.status)}}</td>
        <td>${{escapeHtml((tx.recognizer_labels || []).join(", "))}}</td>
        <td>${{escapeHtml(tx.gas_cost_raw)}}</td>
        <td>${{escapeHtml(tx.external_native_outflow_raw)}}</td>
      </tr>
    `).join("") || '<tr><td colspan="6">No analyzed transactions in this window yet.</td></tr>';
    return `
      <div class="stats-grid">
        <div class="stat"><span>Analyzed txs</span><strong>${{summary.analyzed_tx_count}}</strong></div>
        <div class="stat"><span>Gas spent</span><strong>${{escapeHtml(formatWeiCompact(summary.total_gas_cost_raw))}}</strong></div>
        <div class="stat"><span>Native outflow</span><strong>${{escapeHtml(formatWeiCompact(summary.total_external_native_outflow_raw))}}</strong></div>
        <div class="stat"><span>Incoming native</span><strong>${{escapeHtml(formatWeiCompact(summary.total_incoming_native_raw))}}</strong></div>
      </div>
      <div class="table-shell">
        <table>
          <thead>
            <tr>
              <th>Tx</th>
              <th>Block time</th>
              <th>Status</th>
              <th>Recognizers</th>
              <th>Gas</th>
              <th>Native outflow</th>
            </tr>
          </thead>
          <tbody>${{rows}}</tbody>
        </table>
      </div>
    `;
  }};

  const renderAlerts = (alerts) => {{
    if (!alerts || alerts.length === 0) {{
      return '<div class="empty-state">No active alerts right now. Alert rules run after rolling metrics are refreshed.</div>';
    }}
    return `<div class="alert-list">${{alerts.map((alert) => `
      <article class="alert-card severity-${{escapeHtml(String(alert.severity || "low"))}}">
        <div class="alert-card-head">
          <strong>${{escapeHtml(alert.title)}}</strong>
          <span>${{escapeHtml(String(alert.severity || ""))}}</span>
        </div>
        <p>${{escapeHtml(alert.message || "")}}</p>
        <small>${{escapeHtml(formatTime(alert.triggered_at))}}</small>
        <form method="post" action="/app/clusters/${{clusterId}}/alerts/${{alert.alert_id}}/read">
          <button class="cta ghost" type="submit">Mark as read</button>
        </form>
      </article>
    `).join("")}}</div>`;
  }};

  const renderState = (payload) => {{
    statusNode.className = `notice ${{payload.status_tone || "info"}}`;
    statusNode.textContent = payload.status_label || "Waiting for first historical sync";
    if (trackingNode && initialClusterTab === "overview") {{
      trackingNode.innerHTML = renderTracking(payload.tracking);
    }}
    if (coverageNode && initialClusterTab === "overview") {{
      coverageNode.innerHTML = renderCoverage(payload.coverage);
    }}
    if (alertsNode && initialClusterTab === "overview") {{
      alertsNode.innerHTML = renderAlerts(payload.alerts || []);
    }}
    if (metricsNode && initialClusterTab === "metrics") {{
      metricsNode.innerHTML = renderMetrics(payload.metrics || []);
    }}
    if (behaviorNode && initialClusterTab === "metrics") {{
      behaviorNode.innerHTML = renderBehavior(payload.metrics || []);
    }}
    if (summaryNode && initialClusterTab === "metrics") {{
      summaryNode.innerHTML = renderSummary(payload.spend_summary);
    }}
  }};

  const fetchState = async () => {{
    const url = new URL("/app/clusters/status", window.location.origin);
    url.searchParams.set("cluster_id", clusterId);
    url.searchParams.set("cluster_tab", initialClusterTab);
    if (start) url.searchParams.set("start", start);
    if (end) url.searchParams.set("end", end);
    const response = await fetch(url, {{ credentials: "same-origin" }});
    if (!response.ok) throw new Error(`status ${{response.status}}`);
    renderState(await response.json());
  }};

  fetchState().catch(() => {{}});
  window.setInterval(() => {{
    fetchState().catch(() => {{}});
  }}, 5000);
}})();
</script>"#,
        cluster_id = cluster.cluster_id.to_string(),
        start = start.unwrap_or_default(),
        end = end.unwrap_or_default(),
        selected_tab = selected_tab.as_str()
    )
}

fn active_class(current: &DashboardTab, expected: DashboardTab) -> &'static str {
    if *current == expected { "active" } else { "" }
}

fn render_notice_block(message: Option<&str>) -> String {
    message
        .map(|message| {
            format!(
                "<div class=\"notice success\">{}</div>",
                escape_html(message)
            )
        })
        .unwrap_or_default()
}

fn render_error_block(message: Option<&str>) -> String {
    message
        .map(|message| format!("<div class=\"notice error\">{}</div>", escape_html(message)))
        .unwrap_or_default()
}

fn brand_wordmark_html() -> String {
    format!(
        r#"<span class="brand-lockup"><span class="brand-mark">{}</span><span class="brand-text">LedgerFlow</span></span>"#,
        brand_mark_svg_source()
    )
}

fn brand_mark_svg_source() -> &'static str {
    r##"<svg viewBox="0 0 64 64" xmlns="http://www.w3.org/2000/svg" aria-hidden="true" role="img">
  <rect x="6" y="6" width="52" height="52" rx="12" fill="#111"/>
  <path d="M20 19H44V26H28V31H42V38H28V45H20V19Z" fill="#fff"/>
  <path d="M32 19H44V26H40V45H32V19Z" fill="#f60"/>
</svg>"##
}

fn truncate_middle(value: &str, keep: usize) -> String {
    if value.len() <= keep * 2 {
        return value.to_owned();
    }
    format!("{}...{}", &value[..keep], &value[value.len() - keep..])
}

fn truncate_for_display(value: &str, max_chars: usize) -> String {
    value.chars().take(max_chars).collect()
}

fn enum_label<T: Serialize>(value: &T) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|item| item.as_str().map(str::to_owned))
        .unwrap_or_else(|| "unknown".to_owned())
}

fn escape_html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

fn render_copyable_code(value: &str, keep: usize) -> String {
    let value = value.trim();
    if value.is_empty() || value.eq_ignore_ascii_case("n/a") {
        return "<code>n/a</code>".to_owned();
    }
    if value.eq_ignore_ascii_case("native") {
        return "<code>native</code>".to_owned();
    }
    let label = truncate_middle(value, keep);
    let escaped_value = escape_html(value);
    format!(
        r#"<span class="copy-code"><code title="{0}">{1}</code><button type="button" class="copy-button" data-copy="{0}" onclick="navigator.clipboard.writeText(this.dataset.copy).then(()=>{{const label=this.textContent;this.textContent='Copied';setTimeout(()=>this.textContent=label,900);}})">Copy</button></span>"#,
        escaped_value,
        escape_html(&label),
    )
}

fn render_copy_only_button(value: &str) -> String {
    let value = value.trim();
    if value.is_empty() || value.eq_ignore_ascii_case("n/a") || value.eq_ignore_ascii_case("native")
    {
        return "<code>native</code>".to_owned();
    }
    let escaped_value = escape_html(value);
    r#"<button type="button" class="copy-button copy-only" data-copy="" onclick="navigator.clipboard.writeText(this.dataset.copy).then(()=>{const label=this.textContent;this.textContent='Copied';setTimeout(()=>this.textContent=label,900);})">Copy contract</button>"#
        .replacen("data-copy=\"\"", &format!("data-copy=\"{}\"", escaped_value), 1)
}

fn shared_styles() -> &'static str {
    r#"
:root {
  --bg: #f7f7f7;
  --panel: #ffffff;
  --ink: #111111;
  --muted: #666666;
  --line: #e5e5e5;
  --accent: #f60;
  --accent-2: #f60;
  --cream: #f2f2f2;
  --shadow: none;
}
* { box-sizing: border-box; }
body { margin: 0; overflow-x: hidden; background: var(--bg); color: var(--ink); font-family: Inter, "Segoe UI", "Helvetica Neue", Arial, sans-serif; font-size: 13px; line-height: 1.42; }
a { color: inherit; text-decoration: none; }
.shell, .dashboard-shell { width: min(1320px, calc(100vw - 32px)); margin: 0 auto; }
.topbar { display: flex; justify-content: space-between; align-items: center; padding: 16px 0 10px; }
.brand { font-size: 1.15rem; font-weight: 700; }
.brand-lockup { display: inline-flex; align-items: center; gap: 10px; }
.brand-mark { width: 34px; height: 34px; display: inline-flex; align-items: center; justify-content: center; }
.brand-mark svg { width: 34px; height: 34px; display: block; }
.brand-text { display: inline-block; }
.navlinks { display: flex; gap: 12px; align-items: center; }
.cta { display: inline-flex; align-items: center; justify-content: center; border-radius: 8px; border: 1px solid transparent; padding: 9px 14px; font-weight: 600; }
.cta.tiny { padding: 8px 12px; font-size: .9rem; }
.cta.primary { background: var(--accent); color: white; }
.cta.secondary { background: var(--cream); }
.cta.ghost { background: transparent; border-color: var(--line); }
.cta.danger { color: var(--accent-2); border-color: rgba(197,95,12,.28); }
.hero { display: grid; grid-template-columns: 1.4fr 1fr; gap: 18px; padding: 18px 0 28px; align-items: center; }
.hero h1, .pricing-card h1, .auth-copy h1, .verify-shell h1, .dashboard-hero h1 { font-size: clamp(1.9rem, 3vw, 3rem); line-height: 1.06; margin: 0 0 10px; letter-spacing: -.02em; }
.hero p, .auth-copy p, .panel-copy, .dashboard-hero p { color: var(--muted); line-height: 1.55; }
.hero-card, .feature-card, .pricing-card, .auth-card, .panel, .cluster-card, .price-card { background: var(--panel); border: 1px solid var(--line); border-radius: 10px; box-shadow: var(--shadow); }
.hero-card { padding: 14px; display: grid; gap: 10px; }
.metric-card { background: var(--panel); border-radius: 8px; padding: 12px; border: 1px solid var(--line); }
.metric-card span, .eyebrow { color: var(--accent); text-transform: uppercase; letter-spacing: .12em; font-size: .7rem; font-weight: 700; }
.metric-card strong { display: block; margin-top: 8px; line-height: 1.3; font-size: 1.03rem; }
.hero-actions, .button-row { display: flex; gap: 12px; flex-wrap: wrap; }
.feature-grid, .pricing-grid, .auth-grid { display: grid; gap: 14px; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); }
.feature-card, .pricing-card, .auth-card { padding: 16px; }
.auth-card { display: grid; gap: 14px; align-content: start; }
.pricing-banner { margin: 18px 0 28px; padding: 16px; border: 1px solid var(--line); border-radius: 10px; background: #f8fbff; display: flex; justify-content: space-between; gap: 16px; align-items: center; }
.auth-shell, .verify-shell { display: grid; gap: 18px; padding: 24px 0; }
.auth-card.narrow { max-width: 420px; }
.mode-hint, .price-note { margin-top: 16px; color: var(--muted); }
.flat-list { padding-left: 18px; color: var(--muted); line-height: 1.7; }
.notice { margin: 12px 0; padding: 10px 12px; border-radius: 8px; font-size: .92rem; }
.notice.info { background: rgba(255,102,0,.08); color: var(--accent); }
.notice.success { background: rgba(255,102,0,.08); color: var(--accent); }
.notice.warning { background: rgba(255,102,0,.12); color: var(--accent-2); }
.notice.error { background: rgba(255,102,0,.1); color: var(--accent-2); }
label { display: grid; gap: 8px; font-size: .95rem; }
.label-row { display: inline-flex; align-items: center; gap: 6px; width: fit-content; }
.optional-icon { display: inline-grid; place-items: center; width: 16px; height: 16px; border-radius: 999px; border: 1px solid rgba(255,102,0,.32); color: var(--accent); background: rgba(255,102,0,.08); font-size: .68rem; font-weight: 800; line-height: 1; }
.field-hint { margin-top: -4px; color: var(--muted); font-size: .88rem; line-height: 1.5; }
.field-hint code { font-size: .85rem; }
input, textarea, select { width: 100%; padding: 10px 12px; border: 1px solid var(--line); border-radius: 8px; background: #fff; font: inherit; }
textarea { min-height: 140px; resize: vertical; }
.dashboard-body { background: var(--bg); }
.dashboard-shell { display: grid; grid-template-columns: 260px minmax(0, 1fr); gap: 16px; padding: 16px 0; }
.sidebar { position: sticky; top: 12px; height: fit-content; display: grid; gap: 12px; }
.user-card { padding: 14px; border: 1px solid var(--line); border-radius: 10px; background: var(--panel); }
.side-nav { display: grid; gap: 8px; }
.side-nav a { padding: 9px 10px; border-radius: 8px; border: 1px solid transparent; color: var(--muted); }
.side-nav a.active { background: #fff; border-color: var(--line); color: var(--ink); }
.dashboard-main { display: grid; gap: 14px; min-width: 0; }
.dashboard-hero { padding: 8px 0 0; }
.dashboard-grid { display: grid; grid-template-columns: minmax(0, 1.45fr) minmax(240px, .95fr); gap: 14px; min-width: 0; align-items: start; }
.dashboard-stack { display: grid; gap: 14px; min-width: 0; }
.main-column, .side-column, .compact-side-column { display: grid; gap: 14px; min-width: 0; }
.panel-head { display: flex; justify-content: space-between; align-items: center; gap: 10px; padding: 12px 14px 6px; }
.panel-head .badge { flex-shrink: 0; white-space: nowrap; }
.panel, .panel-body, .table-shell { min-width: 0; }
.panel-body { padding: 0 14px 14px; }
.badge { padding: 3px 8px; border-radius: 999px; background: var(--cream); color: var(--muted); font-size: .72rem; font-weight: 600; text-transform: uppercase; letter-spacing: .06em; }
.stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); gap: 10px; margin: 10px 0 14px; }
.stat { border: 1px solid var(--line); border-radius: 8px; background: #fff; padding: 10px; }
.stat span { display: block; color: var(--muted); font-size: .82rem; text-transform: uppercase; letter-spacing: .08em; }
.stat strong { display: block; margin-top: 8px; font-size: 1.05rem; line-height: 1.3; overflow-wrap: anywhere; }
.stat small { display: block; margin-top: 6px; color: var(--muted); line-height: 1.35; }
.strategy-report-shell { display: grid; gap: 14px; min-width: 0; }
.strategy-report-meta { display: flex; flex-wrap: wrap; gap: 8px; color: var(--muted); font-size: .82rem; }
.strategy-report-meta span { padding: 5px 8px; border: 1px solid var(--line); border-radius: 8px; background: #fff; }
.strategy-report-section, .strategy-detail-report, .report-table-group { display: grid; gap: 10px; min-width: 0; }
.strategy-detail-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 14px; align-items: start; }
.strategy-detail-report { padding-top: 4px; border-top: 1px solid var(--line); }
.compact-stats-grid { grid-template-columns: repeat(auto-fit, minmax(130px, 1fr)); margin-bottom: 4px; }
.strategy-report-section h4, .report-table-group h4 { margin: 4px 0 0; font-size: .92rem; color: var(--muted); }
.tx-hash-action { display: flex; gap: 6px; align-items: center; flex-wrap: wrap; min-width: 0; }
.inline-action-form.compact { margin: 0; display: inline-flex; gap: 4px; align-items: center; }
.cluster-tabbar { display: flex; flex-wrap: wrap; gap: 8px; margin: 6px 0 12px; }
.cluster-tab { border: 1px solid var(--line); background: #fff; color: var(--muted); border-radius: 8px; padding: 8px 12px; font: inherit; font-weight: 600; cursor: pointer; }
.cluster-tab.active { background: #eef5ff; color: var(--ink); border-color: rgba(31,111,235,.24); }
.cluster-tab-panel { display: none; gap: 0; }
.cluster-tab-panel.active { display: block; }
.stack-form, .inline-form { display: grid; gap: 12px; }
.split-fields { display: grid; gap: 12px; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); }
.split-panels { display: grid; gap: 12px; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); }
.inline-form { grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); align-items: end; margin-bottom: 12px; }
.cluster-list { display: grid; gap: 12px; }
.cluster-card { display: grid; gap: 6px; padding: 12px; min-width: 0; }
.cluster-card-link { display: grid; gap: 6px; min-width: 0; }
.cluster-card-actions { display: flex; gap: 10px; align-items: center; flex-wrap: wrap; }
.cluster-card-actions form { margin: 0; }
.cluster-card span, .cluster-card small { color: var(--muted); }
.json-shell { margin: 0; padding: 14px; border-radius: 8px; background: #0f172a; color: #f8fafc; overflow: auto; font-size: 12px; line-height: 1.55; }
.mini-card { display: grid; gap: 6px; padding: 12px; border: 1px solid var(--line); border-radius: 8px; background: #fff; min-width: 0; }
.mini-card span, .mini-card small { color: var(--muted); line-height: 1.45; }
.tx-workbench { overflow: hidden; }
.tx-workbench-head { display: flex; align-items: center; justify-content: space-between; gap: 16px; padding: 18px 18px 10px; }
.tx-workbench-head h1 { margin: 0; font-size: clamp(1.8rem, 2.4vw, 2.5rem); letter-spacing: -.03em; }
.tx-workbench-head p { margin: 6px 0 0; color: var(--muted); line-height: 1.5; }
.tx-search-form { display: grid; grid-template-columns: minmax(220px, 1fr) auto auto; gap: 10px; align-items: center; margin-bottom: 14px; }
.sr-only { position: absolute; width: 1px; height: 1px; padding: 0; margin: -1px; overflow: hidden; clip: rect(0, 0, 0, 0); white-space: nowrap; border: 0; }
.readonly-pill { display: inline-flex; align-items: center; gap: 6px; width: fit-content; padding: 6px 10px; border: 1px solid rgba(13,110,99,.18); border-radius: 999px; background: #ecfdf5; color: #047857; font-size: .78rem; font-weight: 700; white-space: nowrap; }
.tx-analysis-shell { display: grid; gap: 14px; counter-reset: tx-section; }
.tx-section-card { border: 1px solid var(--line); border-radius: 12px; background: #fff; box-shadow: 0 8px 22px rgba(15,23,42,.04); padding: 16px; min-width: 0; }
.tx-section-card > summary { cursor: pointer; list-style: none; }
.tx-section-card > summary::-webkit-details-marker, .trace-node > summary::-webkit-details-marker { display: none; }
.tx-section-card:not([open]) > summary { border-bottom: 0; margin-bottom: 0; padding-bottom: 0; }
.tx-section-title { display: flex; align-items: center; gap: 10px; padding-bottom: 12px; border-bottom: 1px solid var(--line); margin-bottom: 14px; }
.tx-section-title::after { content: "⌃"; margin-left: 8px; color: var(--muted); font-size: .82rem; }
.tx-section-card:not([open]) > .tx-section-title::after { content: "⌄"; }
.tx-section-title h2 { margin: 0; font-size: 1.08rem; line-height: 1.25; }
.tx-section-title h2 small { color: var(--muted); font-weight: 500; margin-left: 4px; }
.tx-section-title .badge, .tx-section-title .confidence-pill { margin-left: auto; }
.tx-section-index { width: 26px; height: 26px; border-radius: 999px; display: inline-grid; place-items: center; background: #2563eb; color: #fff; font-size: 0; font-weight: 800; flex: 0 0 auto; }
.tx-section-index::before { counter-increment: tx-section; content: counter(tx-section); font-size: .9rem; }
.tx-report-top { display: grid; gap: 14px; padding: 2px 0 0; }
.tx-report-heading { display: flex; justify-content: space-between; align-items: center; gap: 12px; border-bottom: 1px solid var(--line); padding-bottom: 12px; }
.tx-report-actions { display: flex; align-items: center; justify-content: flex-end; gap: 8px; flex-wrap: wrap; }
.tx-report-share-action, .inline-action-form { display: inline-flex; align-items: center; gap: 8px; }
.brand-lockup.mini .brand-mark, .brand-lockup.mini .brand-mark svg { width: 28px; height: 28px; }
.brand-lockup.mini { font-weight: 800; }
.tx-report-title-row { display: flex; justify-content: space-between; gap: 16px; align-items: end; }
.tx-report-title-row h1 { margin: 0; font-size: clamp(1.8rem, 2.6vw, 2.5rem); letter-spacing: -.03em; }
.tx-report-title-row p { margin: 4px 0 12px; color: var(--muted); font-weight: 600; }
.tx-report-title-row span { display: block; color: var(--muted); font-size: .84rem; margin-bottom: 4px; }
.basic-info-card { display: grid; gap: 14px; border: 1px solid var(--line); border-radius: 12px; background: #fff; padding: 14px; box-shadow: 0 8px 22px rgba(15,23,42,.04); }
.basic-info-card h2 { margin: 0; font-size: 1rem; }
.basic-info-grid { display: grid; grid-template-columns: repeat(5, minmax(0, 1fr)); gap: 14px 18px; }
.basic-info-grid > div { display: grid; gap: 6px; padding-bottom: 8px; border-bottom: 1px solid var(--line); min-width: 0; }
.basic-info-grid > div.wide { grid-column: span 2; }
.basic-info-grid span { color: var(--muted); font-size: .82rem; }
.basic-info-grid strong { font-size: .92rem; overflow-wrap: anywhere; }
.basic-info-more { display: grid; gap: 12px; }
.basic-info-more summary { cursor: pointer; color: var(--ink); font-weight: 700; padding: 2px 0; }
.status-chip { display: inline-flex; width: fit-content; padding: 3px 8px; border-radius: 999px; border: 1px solid var(--line); font-size: .8rem; }
.status-success { color: #047857; border-color: rgba(4,120,87,.25); background: #ecfdf5; }
.status-failure { color: #b91c1c; border-color: rgba(185,28,28,.25); background: #fef2f2; }
.gas-profile-summary { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 10px; }
.gas-profile-summary div { display: grid; gap: 5px; padding: 10px; border: 1px solid var(--line); border-radius: 8px; background: #fbfdff; }
.gas-profile-summary span { color: var(--muted); font-size: .78rem; }
.gas-profile-bar { height: 36px; border: 1px solid #bfdbfe; border-radius: 8px; overflow: hidden; background: #eff6ff; }
.gas-profile-bar span { display: flex; align-items: center; height: 100%; min-width: 160px; padding: 0 10px; color: #fff; font-weight: 800; background: linear-gradient(90deg, #2563eb, #60a5fa); }
.tx-meta-strip { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); border: 1px solid var(--line); border-radius: 10px; background: #fff; box-shadow: 0 8px 22px rgba(15,23,42,.04); overflow: hidden; }
.tx-meta-strip div { display: grid; gap: 5px; padding: 12px 18px; border-right: 1px solid var(--line); min-width: 0; }
.tx-meta-strip div:last-child { border-right: 0; }
.tx-meta-strip span { color: var(--muted); font-size: .78rem; }
.tx-meta-strip strong { font-size: .92rem; overflow-wrap: anywhere; }
.tx-report-hero { display: grid; gap: 0; padding: 16px; border-left-width: 1px; }
.tx-report-profit { border-left-color: #0d6e63; }
.tx-report-loss { border-left-color: #c55f0c; }
.tx-report-mixed { border-left-color: #1d5ea8; }
.tx-report-unknown { border-left-color: #6f5c40; }
.tx-conclusion-grid { display: grid; grid-template-columns: 72px minmax(120px, .7fr) minmax(180px, 1.05fr) minmax(180px, 1fr); gap: 0; align-items: center; }
.tx-conclusion-icon { width: 60px; height: 60px; border-radius: 999px; display: grid; place-items: center; color: #fff; background: linear-gradient(135deg, #2563eb, #7c3aed); font-size: 2rem; font-weight: 900; }
.tx-conclusion-cell { display: grid; gap: 7px; min-height: 76px; padding: 4px 18px; border-left: 1px solid var(--line); align-content: center; min-width: 0; }
.tx-conclusion-cell span { color: var(--muted); font-size: .78rem; }
.tx-conclusion-cell p { margin: 0; color: var(--ink); line-height: 1.55; }
.tx-conclusion-cell small { color: var(--muted); line-height: 1.45; }
.activity-pill, .verdict-pill { width: fit-content; padding: 7px 12px; border-radius: 999px; font-size: .92rem; }
.activity-pill { color: #6d28d9; background: #f5f3ff; border: 1px solid #ddd6fe; }
.verdict-pill { display: block; width: auto; max-width: 100%; padding: 0; border: 0; border-radius: 0; background: transparent; line-height: 1.25; overflow-wrap: anywhere; }
.verdict-profit { color: #047857; }
.verdict-loss { color: #b91c1c; }
.verdict-mixed { color: #1d4ed8; }
.verdict-unknown { color: #6f5c40; }
.profit-number { color: #059669; font-size: clamp(1.2rem, 2vw, 1.65rem); line-height: 1.15; overflow-wrap: anywhere; }
.tx-report-loss .profit-number { color: #dc2626; }
.confidence-pill { padding: 5px 9px; border-radius: 999px; background: #f1f5f9; color: var(--muted); font-size: .76rem; font-weight: 700; }
.storyline-summary-card { display: grid; gap: 8px; padding: 14px; border: 1px solid var(--line); border-radius: 10px; background: #fff; }
.storyline-summary-card strong { font-size: clamp(1.1rem, 2vw, 1.4rem); line-height: 1.25; }
.storyline-kicker { color: var(--accent); text-transform: uppercase; letter-spacing: .14em; font-size: .75rem; font-weight: 700; }
.address-pill-row { display: flex; gap: 8px; flex-wrap: wrap; }
.address-pill-row code { padding: 5px 8px; border: 1px solid var(--line); border-radius: 8px; background: #fff; }
.subject-scope-line { display: flex; align-items: center; flex-wrap: wrap; gap: 10px; }
.subject-scope-line > span { color: var(--muted); font-size: .85rem; font-weight: 700; }
.subject-scope-line small { color: var(--muted); }
.copy-code { display: inline-flex; align-items: center; gap: 4px; max-width: 100%; min-width: 0; vertical-align: middle; white-space: nowrap; }
.copy-code code { min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.copy-button { flex: 0 0 auto; white-space: nowrap; border: 1px solid var(--line); border-radius: 6px; background: #fff; color: var(--muted); padding: 3px 6px; font: inherit; font-size: .7rem; line-height: 1.1; cursor: pointer; }
.copy-button:hover { border-color: rgba(31,111,235,.35); color: var(--accent); }
.known-address { display: inline-flex; align-items: center; gap: 5px; min-width: 0; max-width: 100%; vertical-align: middle; }
.known-address small { flex: 0 1 auto; min-width: 0; max-width: 180px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; color: var(--accent); border: 1px solid rgba(31,111,235,.18); background: #eef5ff; border-radius: 999px; padding: 2px 6px; font-size: .68rem; line-height: 1.2; }
.storyline-net-effect, .storyline-steps { display: grid; gap: 12px; }
.compact-section { padding-top: 8px; }
.compact-table-shell { margin-top: 4px; }
.compact-table td, .compact-table th { padding: 6px 8px; }
.compact-table .flow-address-cell { vertical-align: top; border-right: 1px solid var(--line); background: #fbfdff; }
.address-label-chip, .address-type-pill { display: inline-flex; align-items: center; width: fit-content; margin: 2px 4px 2px 0; padding: 3px 7px; border: 1px solid var(--line); border-radius: 999px; background: #fff; color: var(--muted); font-size: .75rem; line-height: 1.2; }
.address-type-pill { color: var(--accent); border-color: rgba(13,110,99,.2); background: rgba(13,110,99,.06); }
.amount-stack { display: grid; gap: 2px; }
.amount-stack strong { font-size: .96rem; line-height: 1.25; }
.amount-stack small { color: var(--muted); line-height: 1.3; }
.estimated-usd { color: #047857 !important; font-weight: 700; }
.table-badge { display: inline-flex; align-items: center; padding: 3px 6px; border-radius: 999px; font-size: .75rem; line-height: 1.2; border: 1px solid var(--line); background: #fff; color: var(--muted); }
.table-badge.gained { border-color: rgba(13,110,99,.24); color: var(--accent); }
.table-badge.spent, .table-badge.gas { border-color: rgba(197,95,12,.24); color: var(--accent-2); }
.timeline-list { display: grid; border: 1px solid var(--line); border-radius: 10px; overflow: hidden; background: #fff; }
.timeline-step { display: grid; gap: 0; padding: 0; border: 0; border-bottom: 1px solid var(--line); background: #fff; grid-template-columns: minmax(190px, .85fr) minmax(0, 1.5fr); }
.timeline-step:last-child { border-bottom: 0; }
.timeline-head { display: grid; gap: 10px; grid-template-columns: 28px minmax(0, 1fr); align-items: center; padding: 12px; border-right: 1px solid var(--line); }
.timeline-index { width: 26px; height: 26px; border-radius: 999px; display: grid; place-items: center; background: #2563eb; color: #fff; font-weight: 800; font-size: .85rem; }
.timeline-copy { display: grid; gap: 4px; min-width: 0; }
.timeline-copy p, .timeline-copy small { margin: 0; color: var(--muted); line-height: 1.5; }
.timeline-hint { width: fit-content; margin-top: 2px; padding: 3px 6px; border-radius: 999px; background: #eef5ff; color: #1d5ea8; font-size: .72rem; line-height: 1.3; }
.timeline-flow { display: grid; gap: 0; grid-template-columns: minmax(0, .95fr) minmax(190px, .85fr) minmax(0, .95fr); align-items: stretch; }
.timeline-flow.compact { gap: 0; }
.timeline-side { display: grid; gap: 6px; min-width: 0; }
.timeline-side { padding: 12px; align-content: center; }
.timeline-side code { padding: 5px 7px; border: 1px solid var(--line); border-radius: 6px; background: #f8fafc; overflow-wrap: anywhere; }
.timeline-role-row { display: flex; gap: 6px; flex-wrap: wrap; align-items: center; }
.timeline-mid { display: grid; gap: 3px; justify-items: start; text-align: left; padding: 12px; border-left: 1px solid var(--line); border-right: 1px solid var(--line); align-content: center; }
.timeline-mid strong { font-size: 1rem; line-height: 1.2; color: var(--ink); }
.timeline-mid span { color: var(--muted); line-height: 1.35; }
.timeline-arrow-inline { font-size: 1rem; color: var(--accent); }
.trace-timeline-panel { gap: 10px; }
.trace-timeline-list { display: grid; border: 1px solid var(--line); border-radius: 8px; overflow-x: auto; background: #fff; font-family: "SFMono-Regular", Consolas, "Liberation Mono", monospace; font-size: .82rem; }
.trace-node { display: grid; min-width: 980px; }
.trace-node > summary { display: block; cursor: pointer; list-style: none; }
.trace-node > summary .trace-line::before { content: "−"; display: inline-grid; place-items: center; width: 14px; height: 14px; margin-right: 4px; border: 1px solid #94a3b8; border-radius: 3px; color: #334155; background: #fff; font-size: .72rem; }
.trace-node:not([open]) > summary .trace-line::before { content: "+"; }
.trace-children { display: grid; }
.trace-line { display: grid; grid-template-columns: 34px calc(var(--trace-depth, 0) * 18px) max-content minmax(84px, max-content) minmax(180px, 1fr) 18px minmax(180px, 1fr) max-content minmax(0, 1.1fr) max-content; gap: 6px; align-items: center; min-width: 980px; padding: 6px 10px; border-bottom: 1px solid var(--line); }
.trace-line:last-child { border-bottom: 0; }
.trace-order { color: var(--muted); text-align: right; font-variant-numeric: tabular-nums; }
.trace-depth-lines { height: 100%; min-height: 24px; border-left: 1px solid #cbd5e1; border-right: 1px solid transparent; opacity: .8; }
.trace-call-pill { display: inline-flex; align-items: center; width: fit-content; border: 1px solid rgba(37,99,235,.36); background: #eff6ff; color: #1d4ed8; border-radius: 6px; padding: 3px 6px; font-weight: 800; white-space: nowrap; }
.trace-method { display: inline-flex; align-items: center; width: fit-content; max-width: 220px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; border: 1px solid var(--line); border-radius: 6px; background: #f8fafc; padding: 3px 7px; color: #c2410c; font-weight: 800; }
.trace-endpoint { display: inline-flex; align-items: center; gap: 4px; min-width: 0; white-space: nowrap; }
.trace-address-chip { display: inline-flex; align-items: center; gap: 4px; min-width: 0; }
.trace-address-chip small { color: #1d5ea8; border: 1px solid rgba(29,94,168,.2); background: #eef5ff; border-radius: 999px; padding: 1px 5px; font-family: Inter, "Segoe UI", sans-serif; font-size: .68rem; }
.trace-arrow-inline { color: #2563eb; font-weight: 900; text-align: center; }
.trace-value-chip { display: inline-flex; width: fit-content; border: 1px solid rgba(13,110,99,.22); color: #047857; background: rgba(13,110,99,.06); border-radius: 999px; padding: 2px 6px; white-space: nowrap; }
.trace-transfer-group { display: flex; gap: 5px; flex-wrap: wrap; min-width: 0; }
.trace-transfer-group span { display: inline-flex; align-items: center; gap: 4px; max-width: 100%; border: 1px solid rgba(13,110,99,.18); background: rgba(13,110,99,.05); color: #0f766e; border-radius: 6px; padding: 2px 5px; white-space: nowrap; }
.trace-ref { color: var(--muted); white-space: nowrap; }
.trace-more { border: 1px solid var(--line); border-radius: 8px; background: #fff; overflow: hidden; }
.trace-more summary { cursor: pointer; padding: 8px 10px; color: var(--accent); font-weight: 800; }
.trace-more .trace-timeline-list { border: 0; border-top: 1px solid var(--line); border-radius: 0; }
.storyline-evidence-shell { border: 1px solid var(--line); border-radius: 8px; background: #fff; overflow: hidden; }
.storyline-evidence-shell summary { cursor: pointer; padding: 10px 12px; font-weight: 700; }
.storyline-evidence-shell[open] { padding-bottom: 14px; }
.storyline-evidence-shell[open] summary { border-bottom: 1px solid var(--line); margin-bottom: 14px; }
.section-head { display: flex; align-items: center; justify-content: space-between; gap: 10px; flex-wrap: wrap; }
.section-head h3 { margin: 0; }
.evidence-stack { display: grid; gap: 12px; padding: 0 12px; }
.storyline-visual-shell { display: grid; gap: 12px; }
.storyline-main-path { display: grid; gap: 10px; grid-template-columns: minmax(0, 1fr) auto minmax(0, 1fr) auto minmax(0, 1fr); align-items: stretch; }
.storyline-main-path.direct { grid-template-columns: minmax(0, 1fr) auto minmax(0, 1fr); }
.story-node { display: grid; gap: 6px; padding: 14px; border: 1px solid var(--line); border-radius: 10px; background: #fff; min-width: 0; }
.story-node.accent { border-color: rgba(31,111,235,.25); background: #f8fbff; }
.story-node.protocol { border-color: rgba(13,110,99,.22); background: #f4fbf7; }
.story-node-label { font-size: .75rem; text-transform: uppercase; letter-spacing: .12em; color: var(--muted); }
.story-node strong { font-size: 1rem; overflow-wrap: anywhere; }
.story-node small { color: var(--muted); line-height: 1.45; }
.story-arrow { display: grid; place-items: center; font-size: 1.4rem; color: var(--muted); padding: 0 4px; }
.storyline-lanes { display: grid; gap: 12px; }
.story-lane { display: grid; gap: 8px; padding: 10px; border: 1px dashed var(--line); border-radius: 8px; background: #fff; }
.story-lane-funding { border-color: rgba(29,94,168,.20); background: #f8fbff; }
.story-lane-protocol { border-color: rgba(197,95,12,.22); background: #fffaf5; }
.story-lane-receipt { border-color: rgba(13,110,99,.2); background: #f5fcf9; }
.story-lane-exit { border-color: rgba(111,92,64,.18); background: #fafafa; }
.story-lane-head { display: grid; gap: 4px; }
.story-lane-head small { color: var(--muted); line-height: 1.45; }
.story-chip-row { display: grid; gap: 10px; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); }
.story-chip { display: grid; gap: 6px; padding: 10px; border: 1px solid var(--line); border-radius: 8px; background: #fff; min-width: 0; }
.story-chip-head { display: flex; align-items: center; justify-content: space-between; gap: 8px; flex-wrap: wrap; }
.story-chip-badge { width: fit-content; padding: 4px 8px; border-radius: 999px; background: rgba(13,110,99,.08); color: var(--accent); font-size: .72rem; text-transform: uppercase; letter-spacing: .08em; }
.story-chip-lane-label { color: var(--muted); font-size: .72rem; text-transform: uppercase; letter-spacing: .08em; }
.story-chip strong { overflow-wrap: anywhere; }
.story-chip small { color: var(--muted); line-height: 1.45; overflow-wrap: anywhere; }
.story-role-tag { display: inline-flex; align-items: center; margin-left: 6px; padding: 2px 6px; border-radius: 999px; font-size: .68rem; line-height: 1.2; border: 1px solid var(--line); background: #fff; color: var(--muted); vertical-align: middle; }
.story-role-tag.initiator { border-color: rgba(29,94,168,.25); color: #1d5ea8; }
.story-role-tag.executor { border-color: rgba(13,110,99,.28); color: #0d6e63; }
.story-role-tag.protocol { border-color: rgba(197,95,12,.28); color: #b25a10; }
.story-role-tag.external { border-color: rgba(111,92,64,.2); color: #6f5c40; }
.tx-action-guide { margin-top: 4px; }
.tx-recognizer-grid { align-items: stretch; }
.tx-recognizer-card strong { font-size: 1rem; }
.tx-utilities-panel { display: grid; gap: 10px; }
.utility-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(190px, 1fr)); gap: 12px; }
.utility-card { display: grid; gap: 8px; border: 1px solid var(--line); border-radius: 10px; padding: 12px; background: #fbfdff; min-width: 0; }
.utility-card span { color: var(--muted); font-size: .78rem; font-weight: 700; }
.technical-evidence-panel { display: grid; gap: 8px; }
.raw-json-shell { border: 1px solid var(--line); border-radius: 8px; background: #fff; overflow: hidden; }
.raw-json-shell summary { cursor: pointer; padding: 10px 12px; font-weight: 700; }
.raw-json-shell[open] summary { border-bottom: 1px solid var(--line); }
.table-shell { overflow: auto; }
table { width: 100%; border-collapse: collapse; table-layout: fixed; background: #fff; }
th, td { text-align: left; padding: 8px 6px; border-bottom: 1px solid var(--line); vertical-align: top; overflow-wrap: anywhere; }
.meter { height: 8px; border-radius: 999px; background: #edf2f7; overflow: hidden; min-width: 120px; }
.meter span { display: block; height: 100%; border-radius: 999px; background: linear-gradient(90deg, var(--accent), #5b9cff); }
.alert-list { display: grid; gap: 12px; }
.price-card { padding: 14px; display: grid; gap: 8px; min-width: 0; }
.price-card strong { display: block; font-size: clamp(1.35rem, 2vw, 1.9rem); line-height: 1.15; overflow-wrap: anywhere; }
.price-card span { color: var(--muted); line-height: 1.6; }
.alert-card { border: 1px solid var(--line); border-radius: 10px; background: #fff; padding: 12px; display: grid; gap: 8px; }
.alert-card-head { display: flex; justify-content: space-between; gap: 12px; align-items: center; }
.alert-card p, .alert-card small { margin: 0; color: var(--muted); }
.alert-card.severity-low { border-color: rgba(13,110,99,.22); }
.alert-card.severity-medium { border-color: rgba(197,95,12,.28); background: #fffaf5; }
.alert-card.severity-high { border-color: rgba(197,95,12,.4); background: #fff6f0; }
.empty-state { padding: 14px; border: 1px dashed var(--line); border-radius: 8px; color: var(--muted); background: #fff; }
.shell, .dashboard-shell { width: min(1480px, calc(100vw - 24px)); }
.brand { font-size: .98rem; letter-spacing: 0; }
.brand-lockup { gap: 8px; }
.brand-mark, .brand-mark svg { width: 28px; height: 28px; }
.brand-text { font-weight: 800; }
.topbar { padding: 10px 0 8px; border-bottom: 1px solid var(--line); }
.hero { padding: 14px 0 20px; }
.hero h1, .pricing-card h1, .auth-copy h1, .verify-shell h1, .dashboard-hero h1 { font-size: clamp(1.45rem, 2vw, 2.25rem); letter-spacing: 0; }
.hero p, .auth-copy p, .panel-copy, .dashboard-hero p { line-height: 1.45; }
.cta { border-radius: 6px; padding: 7px 10px; font-size: .82rem; font-weight: 700; }
.cta.tiny { padding: 5px 8px; font-size: .76rem; }
.cta.primary { background: #111; color: #fff; }
.cta.secondary { background: var(--accent); border-color: var(--accent); color: #fff; }
.cta.ghost { background: #fff; color: #222; }
.cta.danger { color: var(--accent); border-color: rgba(255,102,0,.35); }
.panel, .cluster-card, .price-card, .metric-card, .mini-card, .auth-card, .pricing-card, .feature-card { border-radius: 6px; }
.panel-head { min-height: 38px; padding: 9px 11px 6px; border-bottom: 1px solid var(--line); background: #fff; }
.panel-head h2, .section-head h3 { display: inline-flex; align-items: center; gap: 7px; margin: 0; font-size: .9rem; line-height: 1.25; }
.panel-head h2::before, .section-head h3::before { content: ""; width: 7px; height: 7px; border-radius: 2px; background: var(--accent); box-shadow: 0 0 0 3px rgba(255,102,0,.1); }
.panel-body { padding: 10px 11px 11px; }
.badge, .table-badge, .confidence-pill, .status-chip { border-radius: 6px; background: #f5f5f5; color: #444; border: 1px solid var(--line); font-size: .68rem; letter-spacing: .04em; }
.eyebrow, .metric-card span, .storyline-kicker { color: var(--accent); letter-spacing: .08em; font-size: .65rem; }
.dashboard-shell { grid-template-columns: 220px minmax(0, 1fr); gap: 12px; padding: 12px 0; }
.sidebar { gap: 10px; }
.user-card { padding: 10px; border-radius: 6px; }
.user-card strong, .user-card small { overflow-wrap: anywhere; }
.side-nav { gap: 4px; }
.side-nav a { display: flex; align-items: center; gap: 8px; min-height: 30px; padding: 6px 8px; border-radius: 6px; color: #444; font-size: .82rem; }
.side-nav a::before { content: ""; width: 13px; height: 13px; flex: 0 0 auto; border-radius: 4px; border: 1px solid #d6d6d6; background: linear-gradient(135deg, #fff 0 55%, #f60 55% 100%); }
.side-nav a.active { background: #111; border-color: #111; color: #fff; }
.side-nav a.active::before { border-color: rgba(255,255,255,.35); background: var(--accent); }
.dashboard-main, .dashboard-stack, .main-column, .side-column, .compact-side-column { gap: 10px; }
.dashboard-hero { display: flex; align-items: end; justify-content: space-between; gap: 12px; padding: 4px 0 0; border-bottom: 1px solid var(--line); }
.dashboard-hero h1 { margin-bottom: 6px; }
.dashboard-grid { gap: 10px; }
.stats-grid { gap: 8px; margin: 8px 0 10px; }
.stat { border-radius: 6px; padding: 8px; }
.stat span, .tx-meta-strip span, .basic-info-grid span, .gas-profile-summary span { font-size: .68rem; letter-spacing: .05em; }
.stat strong { margin-top: 5px; font-size: .96rem; }
label { gap: 5px; font-size: .8rem; color: #333; }
input, textarea, select { border-radius: 6px; padding: 7px 9px; font-size: .82rem; }
textarea { min-height: 110px; }
.table-shell { border: 1px solid var(--line); border-radius: 6px; background: #fff; }
table { font-size: .78rem; }
th, td { padding: 6px 7px; }
th { background: #fafafa; color: #555; font-size: .68rem; text-transform: uppercase; letter-spacing: .05em; font-weight: 800; white-space: nowrap; }
td { color: #222; }
code, .copy-button { font-size: .72rem; }
.copy-button { border-radius: 4px; padding: 2px 5px; }
.json-shell { border-radius: 6px; background: #111; color: #fff; font-size: 11px; }
.cluster-tab { border-radius: 6px; padding: 6px 9px; font-size: .78rem; }
.cluster-tab.active { background: #111; border-color: #111; color: #fff; }
.tx-section-card, .basic-info-card, .tx-meta-strip, .storyline-summary-card, .timeline-list, .trace-timeline-list { border-radius: 6px; box-shadow: none; }
.tx-section-index, .timeline-index { background: var(--accent); }
.meter span, .gas-profile-bar span { background: var(--accent); }
.cta::before { content: ""; width: 6px; height: 6px; margin-right: 7px; border-radius: 2px; background: currentColor; opacity: .8; }
.cta.primary::before, .cta.secondary::before { background: #fff; }
.status-success, .status-failure, .activity-pill, .verdict-pill, .confidence-pill { color: #111; border-color: var(--line); background: #fff; }
.verdict-profit, .verdict-loss, .verdict-mixed, .verdict-unknown, .profit-number, .tx-report-loss .profit-number { color: #111; }
.tx-report-profit, .tx-report-loss, .tx-report-mixed, .tx-report-unknown { border-left-color: var(--accent); }
.table-badge.gained, .table-badge.spent, .table-badge.gas, .address-type-pill, .story-chip-badge { color: var(--accent); border-color: rgba(255,102,0,.28); background: rgba(255,102,0,.06); }
.known-address small, .trace-address-chip small, .timeline-hint, .trace-more summary { color: var(--accent); border-color: rgba(255,102,0,.25); background: rgba(255,102,0,.06); }
.story-node.accent, .story-node.protocol, .story-lane-funding, .story-lane-protocol, .story-lane-receipt, .story-lane-exit, .utility-card, .gas-profile-summary div { background: #fff; border-color: var(--line); }
.trace-call-pill, .trace-method, .trace-value-chip, .trace-transfer-group span { color: #111; border-color: var(--line); background: #fff; }
.trace-arrow-inline, .timeline-arrow-inline, .story-arrow { color: var(--accent); }
.tx-conclusion-icon { background: #111; color: #fff; border-radius: 12px; }
@media (max-width: 960px) {
  .hero, .dashboard-shell, .dashboard-grid, .tx-report-hero { grid-template-columns: 1fr; }
  .tx-search-form, .tx-meta-strip, .tx-conclusion-grid, .timeline-step, .timeline-flow { grid-template-columns: 1fr; }
  .basic-info-grid, .gas-profile-summary { grid-template-columns: 1fr; }
  .basic-info-grid > div.wide { grid-column: span 1; }
  .tx-meta-strip div, .timeline-head, .timeline-mid { border-right: 0; border-left: 0; }
  .tx-meta-strip div { border-bottom: 1px solid var(--line); }
  .tx-meta-strip div:last-child { border-bottom: 0; }
  .tx-conclusion-cell { border-left: 0; border-top: 1px solid var(--line); padding: 12px 0; }
  .tx-conclusion-icon { margin-bottom: 8px; }
  .sidebar { position: static; }
  .storyline-main-path { grid-template-columns: 1fr; }
  .story-arrow { transform: rotate(90deg); padding: 4px 0; }
  .timeline-head { border-bottom: 1px solid var(--line); }
}
"#
}

pub struct WebError {
    status: StatusCode,
    message: String,
}

impl WebError {
    fn upstream(error: reqwest::Error) -> Self {
        Self {
            status: StatusCode::BAD_GATEWAY,
            message: format!("web upstream error: {error}"),
        }
    }

    fn internal(message: String) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message,
        }
    }

    fn bad_request(message: String) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message,
        }
    }

    fn bad_gateway(message: String) -> Self {
        Self {
            status: StatusCode::BAD_GATEWAY,
            message,
        }
    }
}

impl From<LedgerError> for WebError {
    fn from(value: LedgerError) -> Self {
        match value {
            LedgerError::NotFound(message) => Self {
                status: StatusCode::NOT_FOUND,
                message,
            },
            LedgerError::TransientIngestion(message)
            | LedgerError::UnsupportedTrace(message)
            | LedgerError::MalformedEvidence(message)
            | LedgerError::AnalysisInvariant(message)
            | LedgerError::RecognizerExecution(message)
            | LedgerError::Persistence(message) => Self::internal(message),
        }
    }
}

impl IntoResponse for WebError {
    fn into_response(self) -> Response {
        (
            self.status,
            Html(render_marketing_page(
                "Error",
                None,
                format!(
                    "<section class=\"auth-shell\"><div class=\"auth-copy\"><h1>Request failed</h1>{}</div></section>",
                    render_error_block(Some(&self.message))
                ),
            )),
        )
            .into_response()
    }
}

impl std::fmt::Display for WebError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}
