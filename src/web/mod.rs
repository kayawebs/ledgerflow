use std::sync::Arc;

use axum::{
    Form, Json, Router,
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{Html, IntoResponse, Redirect, Response},
    routing::{get, post},
};
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
            AddressCluster, AppUser, ClassifiedTransaction, ClusterAlert,
            ClusterCoverage, ClusterMetricsSnapshot, ClusterSpendSummary, ClusterTrackingState,
            EvidencePack, TransactionAnalysis,
        },
        enums::{AddressHitDirection, ClusterBackfillStatus, VerificationPurpose},
    },
    services::{
        auth::{
            AuthService, LogVerificationSender, PostmarkVerificationSender,
            ResendVerificationSender, VerificationSender, verification_purpose_from_str,
            verification_purpose_to_str,
        },
        discovery::ClusterDiscoveryResponse,
        portal::{DashboardClusterRecord, PortalService},
    },
};

const DEFAULT_PRICE_ETH: &str = "0.5";

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
}

#[derive(Debug)]
struct TxAnalysisShareRecord {
    share_id: Uuid,
    title: String,
    summary: String,
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

#[derive(Debug, Serialize)]
struct ClusterLiveResponse {
    status_label: String,
    status_tone: String,
    tracking: Option<ClusterTrackingState>,
    coverage: Option<ClusterCoverage>,
    spend_summary: Option<ClusterSpendSummary>,
    metrics: Vec<ClusterMetricsSnapshot>,
    alerts: Vec<ClusterAlert>,
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

#[derive(Debug, Deserialize, Clone)]
struct ClusterDiscoveryQuery {
    address: Option<String>,
    max_hops: Option<u8>,
    notice: Option<String>,
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
enum ClusterAnalysisTab {
    Overview,
    Metrics,
    Summary,
    Activity,
}

impl ClusterAnalysisTab {
    fn from_query(value: Option<&str>) -> Self {
        match value {
            Some("metrics") => Self::Metrics,
            Some("summary") => Self::Summary,
            Some("activity") => Self::Activity,
            _ => Self::Overview,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Overview => "overview",
            Self::Metrics => "metrics",
            Self::Summary => "summary",
            Self::Activity => "activity",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DashboardTab {
    Clusters,
    ClusterAnalysis,
    ClusterDiscovery,
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
        DashboardTab::ClusterDiscovery => "Cluster discovery",
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
            Some("cluster-discovery") => Self::ClusterDiscovery,
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
            if let (Some(server_token), Some(from_email)) =
                (config.postmark_server_token.clone(), config.email_from.clone())
            {
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
        .route("/app/cluster-discovery", get(cluster_discovery_page))
        .route("/app/clusters/create", post(create_cluster))
        .route("/app/clusters/{cluster_id}/update", post(update_cluster))
        .route("/app/clusters/{cluster_id}/delete", post(delete_cluster))
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
        state.bind_addr,
        state.api_base_url,
        state.email_delivery_mode,
        state.email_delivery_ready
    ))
}

async fn brand_mark_svg() -> Response {
    (
        [(header::CONTENT_TYPE, "image/svg+xml; charset=utf-8")],
        brand_mark_svg_source(),
    )
        .into_response()
}

async fn homepage(State(state): State<WebState>, headers: HeaderMap) -> Result<Html<String>, WebError> {
    let user = current_user(&state, &headers).await?;
    Ok(Html(render_marketing_page(
        "LedgerFlow",
        user.as_ref(),
        render_homepage_content(user.as_ref()),
    )))
}

async fn pricing(State(state): State<WebState>, headers: HeaderMap) -> Result<Html<String>, WebError> {
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

async fn dashboard(
    Query(query): Query<DashboardQuery>,
) -> Result<Response, WebError> {
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
        DashboardTab::ClusterDiscovery => "/app/cluster-discovery".to_owned(),
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
        None,
    )
    .await
}

async fn cluster_discovery_page(
    State(state): State<WebState>,
    headers: HeaderMap,
    Query(query): Query<ClusterDiscoveryQuery>,
) -> Result<Response, WebError> {
    let Some(user) = current_user(&state, &headers).await? else {
        return Ok(Redirect::to("/auth?notice=auth-required").into_response());
    };
    let (payload, error) = if let Some(address) = query.address.as_deref().map(str::trim).filter(|value| !value.is_empty()) {
        let max_hops = query.max_hops.unwrap_or(1).clamp(1, 3);
        let path = format!(
            "/discovery/cluster?chain_id={}&seed_address={}&max_hops={}",
            state.default_chain_id,
            urlencoding::encode(address),
            max_hops
        );
        match api_get_json::<ClusterDiscoveryResponse>(&state, &path).await {
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
            tab: Some("cluster-discovery".to_owned()),
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
        None,
        None,
        payload,
        Some(query),
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
        None,
    )
    .await
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
        return Ok(Redirect::to("/app/indexer/api-keys?notice=indexer-key-label-required").into_response());
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
    if !clusters.iter().any(|cluster| cluster.cluster_id == cluster_id) {
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
            "/app/clusters?cluster_id={}&notice=cluster-updated",
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
    let load_path = format!("/analyses/{}?chain_id={}", form.tx_hash.trim(), form.chain_id);
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
        r#"
<section class="panel public-share-panel">
  <div class="panel-head">
    <div>
      <span class="eyebrow">Public transaction analysis</span>
      <h1>{}</h1>
    </div>
    <a class="cta ghost" href="/app/tx-analysis">Analyze another tx</a>
  </div>
  <div class="panel-body">
    <p class="panel-copy">{}</p>
    <div class="field-hint">Share URL <code>{}</code></div>
    {}
  </div>
</section>
"#,
        escape_html(&share.title),
        escape_html(&share.summary),
        escape_html(&share_url),
        render_tx_analysis_result_with_share(&share.snapshot, false)
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
    discovery_result: Option<ClusterDiscoveryResponse>,
    discovery_query: Option<ClusterDiscoveryQuery>,
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
        .and_then(|cluster_id| clusters.iter().find(|item| item.cluster_id == cluster_id).cloned())
        .or_else(|| clusters.first().cloned());

    let (
        tracking,
        coverage,
        spend_summary,
        metrics,
        alerts,
        cluster_status,
        cluster_error,
    ) =
        if tab == DashboardTab::ClusterAnalysis {
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
                        payload.metrics,
                        payload.alerts,
                        Some((payload.status_label, payload.status_tone)),
                        None::<String>,
                    ),
                    Err(err) => (
                        None,
                        None,
                        None,
                        Vec::new(),
                        Vec::new(),
                        None,
                        Some(err.to_string()),
                    ),
                }
            } else {
                (
                    None,
                    None,
                    None,
                    Vec::new(),
                    Vec::new(),
                    None,
                    None,
                )
            }
        } else {
            (
                None,
                None,
                None,
                Vec::new(),
                Vec::new(),
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
                format!("sort_dir={}", urlencoding::encode(&classified_query.sort_dir)),
            ];
            if let Some(behavior_tag) = classified_query.behavior_tag.as_deref() {
                params.push(format!("behavior_tag={}", urlencoding::encode(behavior_tag)));
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
            &format!("/admin/api-keys?owner_user_id={}", urlencoding::encode(&user.user_id.to_string())),
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
        &metrics,
        &alerts,
        classified_page.as_ref(),
        &classified_query,
        cluster_status.as_ref(),
        &cluster_analysis_tab,
        tx_result.as_ref(),
        discovery_result.as_ref(),
        discovery_query.as_ref(),
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
            format!("/clusters/{}/spend-summary?{}", cluster_id, params.join("&"))
        }
    };
    let (coverage, tracking, spend_summary, metrics, alerts) = match selected_tab {
        ClusterAnalysisTab::Overview => {
            let coverage_fut = api_get_json::<ClusterCoverage>(state, &coverage_path);
            let tracking_path = format!("/clusters/{}/tracking", cluster_id);
            let alerts_path = format!("/clusters/{}/alerts?include_read=false&limit=12", cluster_id);
            let tracking_fut = api_get_json::<ClusterTrackingState>(state, &tracking_path);
            let alerts_fut = api_get_json::<Vec<ClusterAlert>>(state, &alerts_path);
            let (coverage, tracking, alerts) = tokio::join!(coverage_fut, tracking_fut, alerts_fut);
            (
                coverage.ok(),
                tracking.ok(),
                None,
                Vec::new(),
                alerts.unwrap_or_default(),
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
                metrics.unwrap_or_default(),
                Vec::new(),
            )
        }
        ClusterAnalysisTab::Summary => {
            let coverage_fut = api_get_json::<ClusterCoverage>(state, &coverage_path);
            let summary_fut = api_get_json::<ClusterSpendSummary>(state, &summary_path);
            let (coverage, spend_summary) = tokio::join!(coverage_fut, summary_fut);
            (
                coverage.ok(),
                None,
                spend_summary.ok(),
                Vec::new(),
                Vec::new(),
            )
        }
        ClusterAnalysisTab::Activity => (
            api_get_json::<ClusterCoverage>(state, &coverage_path).await.ok(),
            None,
            None,
            Vec::new(),
            Vec::new(),
        ),
    };
    let (status_label, status_tone) = derive_cluster_status(coverage.as_ref());
    Ok(ClusterLiveResponse {
        status_label,
        status_tone,
        tracking,
        coverage,
        spend_summary,
        metrics,
        alerts,
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
        select share_id, title, summary, analysis_snapshot
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
        title: row
            .try_get("title")
            .map_err(|err| WebError::internal(format!("decode share title failed: {err}")))?,
        summary: row
            .try_get("summary")
            .map_err(|err| WebError::internal(format!("decode share summary failed: {err}")))?,
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
                label: parts.next().filter(|value| !value.is_empty()).map(str::to_owned),
                role: parts.next().filter(|value| !value.is_empty()).map(str::to_owned),
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
        .filter(|value| matches!(*value, "timestamp" | "block_number" | "value_usd" | "gas_cost_usd" | "estimated_pnl_usd"))
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
        .get(format!("{}{}", state.api_base_url.trim_end_matches('/'), path))
        .send()
        .await
        .map_err(WebError::upstream)?;
    parse_api_response(response).await
}

async fn indexer_get_json<T: DeserializeOwned>(state: &WebState, path: &str) -> Result<T, WebError> {
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
        .post(format!("{}{}", state.api_base_url.trim_end_matches('/'), path))
        .json(payload)
        .send()
        .await
        .map_err(WebError::upstream)?;
    parse_api_response(response).await
}

async fn parse_api_response<T: DeserializeOwned>(response: reqwest::Response) -> Result<T, WebError> {
    let status = response.status();
    let body = response.text().await.map_err(WebError::upstream)?;
    if !status.is_success() {
        return Err(WebError::bad_gateway(format!(
            "internal analysis api returned {}: {}",
            status,
            body
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
            status,
            body
        )));
    }
    serde_json::from_str(&body).map_err(|err| WebError::internal(err.to_string()))
}

fn build_indexer_lookup_path(query: &IndexerLookupQuery) -> Result<Option<String>, WebError> {
    let Some(address) = query.address.as_deref().map(str::trim).filter(|value| !value.is_empty()) else {
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
        "cluster-deleted" => Some("Cluster deleted."),
        "cluster-delete-error" => Some("Cluster delete failed. Inspect the latest server response or logs."),
        "tracking-enabled" => Some("Cluster tracking enabled."),
        "tracking-paused" => Some("Cluster tracking paused."),
        "tracking-error" => Some("Tracking action failed. Inspect the latest cluster state or server logs."),
        "alert-read" => Some("Alert marked as read."),
        "alert-error" => Some("Alert action failed. Refresh the cluster view and inspect server logs."),
        "indexer-key-created" => Some("Indexer API key created. Copy the full token now; the portal only shows it once."),
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
        if user.is_some() { "/app/clusters" } else { "/auth" },
        if user.is_some() { "Open dashboard" } else { "Request access" }
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
        DEFAULT_PRICE_ETH,
        DEFAULT_PRICE_ETH
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
    metrics: &[ClusterMetricsSnapshot],
    alerts: &[ClusterAlert],
    classified_page: Option<&ClassifiedTransactionsPagePayload>,
    classified_query: &ClusterTxTableState,
    cluster_status: Option<&(String, String)>,
    cluster_analysis_tab: &ClusterAnalysisTab,
    tx_result: Option<&serde_json::Value>,
    discovery_result: Option<&ClusterDiscoveryResponse>,
    discovery_query: Option<&ClusterDiscoveryQuery>,
    indexer_progress: Option<&IndexerProgressPayload>,
    indexer_lookup: Option<&IndexerAddressRefsPayload>,
    indexer_api_keys: &[IndexerApiKeyPayload],
    indexer_query: Option<&IndexerLookupQuery>,
    start: Option<&str>,
    end: Option<&str>,
    error: Option<&str>,
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
        let status_tone = cluster_status
            .map(|item| item.1.as_str())
            .unwrap_or("info");
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
        let overview_href = cluster_analysis_tab_href(
            cluster.cluster_id,
            start,
            end,
            ClusterAnalysisTab::Overview,
        );
        let metrics_href = cluster_analysis_tab_href(
            cluster.cluster_id,
            start,
            end,
            ClusterAnalysisTab::Metrics,
        );
        let summary_href = cluster_analysis_tab_href(
            cluster.cluster_id,
            start,
            end,
            ClusterAnalysisTab::Summary,
        );
        let activity_href = cluster_analysis_tab_href(
            cluster.cluster_id,
            start,
            end,
            ClusterAnalysisTab::Activity,
        );
        let tab_panel = match cluster_analysis_tab {
            ClusterAnalysisTab::Overview => format!(
                r#"<section class="cluster-tab-panel active" data-cluster-tab-panel="overview">
      <h3>Tracking</h3>
      <div id="cluster-live-tracking">{}</div>
      <h3>Coverage</h3>
      <div id="cluster-live-coverage">{}</div>
      <h3>Alerts</h3>
      <div id="cluster-live-alerts">{}</div>
    </section>"#,
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
            ClusterAnalysisTab::Summary => format!(
                r#"<section class="cluster-tab-panel active" data-cluster-tab-panel="summary">
      <h3>Spend summary</h3>
      <div id="cluster-live-summary">{}</div>
    </section>"#,
                render_cluster_summary_panel(spend_summary),
            ),
            ClusterAnalysisTab::Activity => format!(
                r#"<section class="cluster-tab-panel active" data-cluster-tab-panel="activity">
      <h3>Recent classified transactions</h3>
      <div id="cluster-live-classified">{}</div>
    </section>"#,
                render_recent_classified_transactions_panel(cluster, classified_page, classified_query),
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
    <div class="cluster-tabbar" role="tablist" aria-label="Cluster analysis sections">
      <a class="cluster-tab {}" href="{}">Overview</a>
      <a class="cluster-tab {}" href="{}">Metrics</a>
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
            overview_active,
            overview_href,
            metrics_active,
            metrics_href,
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
    let tx_panel = format!(
        r#"
<section class="panel">
  <div class="panel-head"><h2>Single transaction analysis</h2><span class="badge">debug layer</span></div>
  <div class="panel-body">
    <form class="stack-form" method="post" action="/app/tx-analysis/run">
      <label>Chain ID<input type="number" name="chain_id" value="{}" required></label>
      <label>Transaction hash<input type="text" name="tx_hash" value="{}" placeholder="0x..." required></label>
      <div class="button-row">
        <button class="cta secondary" type="submit" name="mode" value="analyze">Analyze transaction</button>
        <button class="cta ghost" type="submit" name="mode" value="reanalyze">Reanalyze</button>
      </div>
    </form>
    {}
  </div>
</section>
"#,
        chain_id_prefill,
        escape_html(&tx_hash_prefill),
        tx_result
            .map(render_tx_analysis_result)
            .unwrap_or_else(|| {
                "<div class=\"empty-state\">Enter one tx hash to load the saved result if it already exists. Use Reanalyze when you want LedgerFlow to fetch evidence again and rerun the recognizers.</div>".to_owned()
            })
    );
    let cluster_discovery_panel = render_cluster_discovery_panel(discovery_result, discovery_query);

    let graph_panel = render_cluster_graph_panel(clusters);
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
    } else if *tab == DashboardTab::ClusterDiscovery {
        cluster_discovery_panel.clone()
    } else if *tab == DashboardTab::TxAnalysis {
        tx_panel.clone()
    } else if *tab == DashboardTab::ClusterGraph {
        graph_panel.clone()
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
                DashboardTab::Clusters => render_cluster_management_panel(state.default_chain_id, editable_cluster),
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
          <a class="{}" href="/app/cluster-discovery">Cluster discovery</a>
          <a class="{}" href="/app/cluster-graph">Cluster graph</a>
          <a class="{}" href="/app/tx-analysis">Single tx analysis</a>
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
          <p>Use one signed-in portal for both layers: the Base indexer discovery surface and the analysis workflows built on top of it. Keep address lookup lightweight, then move into cluster backfill, spend attribution, graph planning, and dedicated single-tx debugging.</p>
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
        active_class(tab, DashboardTab::ClusterDiscovery),
        active_class(tab, DashboardTab::ClusterGraph),
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
      <label>Cluster name<input type="text" name="name" value="{}" placeholder="primary_searcher" required></label>
      <label>Description<input type="text" name="description" value="{}" placeholder="main bot + treasury EOAs"></label>
      <label>Addresses<textarea name="addresses" placeholder="0x111...,bot-1,bot&#10;0x222...,treasury,eoa" required>{}</textarea></label>
      <div class="field-hint">One record per line. Format: <code>address,label,role</code>. Only the address is required, so <code>0xabc...</code> or <code>0xabc...,bot-1</code> also works.</div>
      <div class="button-row">
        <button class="cta primary" type="submit">{}</button>
        {}
      </div>
    </form>
  </div>
</section>
"#,
        if is_editing { "Edit cluster" } else { "Create cluster" },
        if is_editing {
            "Update the cluster definition in place. Existing analyses stay in the database, while future tracking and backfill runs use the edited address set."
        } else {
            "Create a cluster for one operator, wallet family, or bot set. As soon as creation succeeds, LedgerFlow queues a historical backfill job for the separate worker process."
        },
        action,
        chain_id,
        escape_html(owner_label),
        escape_html(name),
        escape_html(description),
        escape_html(&addresses),
        if is_editing { "Save cluster changes" } else { "Create cluster" },
        if is_editing {
            "<a class=\"cta ghost\" href=\"/app/clusters\">Cancel edit</a>".to_owned()
        } else {
            String::new()
        }
    )
}

fn render_cluster_discovery_panel(
    discovery_result: Option<&ClusterDiscoveryResponse>,
    discovery_query: Option<&ClusterDiscoveryQuery>,
) -> String {
    let address = discovery_query
        .and_then(|value| value.address.as_deref())
        .unwrap_or_default();
    let max_hops = discovery_query.and_then(|value| value.max_hops).unwrap_or(1);
    let result_panel = discovery_result
        .map(|payload| {
            let warnings = if payload.warnings.is_empty() {
                String::new()
            } else {
                format!(
                    "<div class=\"notice warning\">{}</div>",
                    escape_html(&payload.warnings.join(" | "))
                )
            };
            format!(
                r#"
{}<div class="stats-grid">
  <div class="stat"><span>Seed</span><strong><code>{}</code></strong></div>
  <div class="stat"><span>Max hops</span><strong>{}</strong></div>
  <div class="stat"><span>Nodes</span><strong>{}</strong></div>
  <div class="stat"><span>Edges</span><strong>{}</strong></div>
</div>
<div class="split-panels">
  <div class="table-shell">
    <table>
      <thead>
        <tr>
          <th>Address</th>
          <th>Type</th>
          <th>First hop</th>
          <th>Inbound</th>
          <th>Outbound</th>
        </tr>
      </thead>
      <tbody>{}</tbody>
    </table>
  </div>
  <div class="table-shell">
    <table>
      <thead>
        <tr>
          <th>From</th>
          <th>To</th>
          <th>Hop</th>
          <th>Tx count</th>
          <th>Blocks</th>
        </tr>
      </thead>
      <tbody>{}</tbody>
    </table>
  </div>
</div>
"#,
                warnings,
                escape_html(&truncate_middle(&payload.seed_address, 8)),
                payload.max_hops,
                payload.total_nodes,
                payload.total_edges,
                payload
                    .nodes
                    .iter()
                    .take(200)
                    .map(|node| {
                        format!(
                            "<tr><td><code>{}</code></td><td>{:?}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
                            escape_html(&truncate_middle(&node.address, 8)),
                            node.address_type,
                            node.first_seen_hop,
                            node.inbound_edge_count,
                            node.outbound_edge_count
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(""),
                payload
                    .edges
                    .iter()
                    .take(200)
                    .map(|edge| {
                        format!(
                            "<tr><td><code>{}</code></td><td><code>{}</code></td><td>{}</td><td>{}</td><td>{} - {}</td></tr>",
                            escape_html(&truncate_middle(&edge.from_address, 8)),
                            escape_html(&truncate_middle(&edge.to_address, 8)),
                            edge.first_seen_hop,
                            edge.tx_count,
                            edge.min_block_number.map(|value| value.to_string()).unwrap_or_else(|| "n/a".to_owned()),
                            edge.max_block_number.map(|value| value.to_string()).unwrap_or_else(|| "n/a".to_owned())
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("")
            )
        })
        .unwrap_or_else(|| {
            "<div class=\"empty-state\">Enter one Base address, choose 1 to 3 hops, and LedgerFlow will scan indexed top-level history in both directions to build a first-pass connection graph.</div>".to_owned()
        });

    format!(
        r#"
<section class="panel">
  <div class="panel-head"><h2>Cluster discovery</h2><span class="badge">top-level graph</span></div>
  <div class="panel-body">
    <p class="panel-copy">This feature explores one-hop to three-hop top-level address connectivity from a seed Base address. It scans indexed history for both incoming and outgoing top-level links, then labels counterparties as EOA or contract via <code>eth_getCode</code>.</p>
    <form class="stack-form" method="get" action="/app/cluster-discovery">
      <label>Seed address<input type="text" name="address" value="{}" placeholder="0x..." required></label>
      <div class="split-fields">
        <label>Max hops
          <select name="max_hops">
            <option value="1" {}>1 hop</option>
            <option value="2" {}>2 hops</option>
            <option value="3" {}>3 hops</option>
          </select>
        </label>
      </div>
      <div class="field-hint">This is intentionally top-level only for now. It scans indexed history rather than sampling a fixed number of tx refs, but it is still not a substitute for trace-based fund-flow attribution.</div>
      <div class="button-row">
        <button class="cta secondary" type="submit">Discover connected addresses</button>
        <a class="cta ghost" href="/app/indexer/address-lookup">Open raw indexer lookup</a>
      </div>
    </form>
    {}
  </div>
</section>
"#,
        escape_html(address),
        if max_hops == 1 { "selected" } else { "" },
        if max_hops == 2 { "selected" } else { "" },
        if max_hops == 3 { "selected" } else { "" },
        result_panel
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
            "swap_like_activity" | "approval_then_swap_like" => {
                Some(("swap", "The tx shows token-in / token-out swap-like behavior."))
            }
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
        if best
            .as_ref()
            .map(|(_, confidence, _)| result.confidence > *confidence)
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
            "The current recognizers could not confidently classify this transaction yet.".to_owned(),
            "n/a".to_owned(),
        )
    }
}

fn render_tx_analysis_result(payload: &serde_json::Value) -> String {
    render_tx_analysis_result_with_share(payload, true)
}

fn render_tx_analysis_result_with_share(
    payload: &serde_json::Value,
    show_share_action: bool,
) -> String {
    let raw_json = serde_json::to_string_pretty(payload).unwrap_or_default();
    let Ok(envelope) = serde_json::from_value::<AnalysisEnvelope>(payload.clone()) else {
        return format!(
            "<details class=\"raw-json-shell\" open><summary>Raw analysis JSON</summary><pre class=\"json-shell\">{}</pre></details>",
            escape_html(&raw_json)
        );
    };
    let analysis = &envelope.analysis;
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
    let recognizer_rows = if analysis.recognizer_results.is_empty() {
        "<div class=\"empty-state\">No recognizers fired. The tx may still be analyzable, but the current recognizer set did not produce a label.</div>".to_owned()
    } else {
        analysis
            .recognizer_results
            .iter()
            .map(|result| {
                format!(
                    r#"<article class="mini-card tx-recognizer-card">
  <strong>{}</strong>
  <span>{} · confidence {:.2}</span>
  <small>id: {} · version: {}</small>
  {}
  {}
</article>"#,
                    escape_html(&result.label),
                    escape_html(&result.recognizer_id),
                    result.confidence,
                    escape_html(&result.recognizer_id),
                    escape_html(&result.recognizer_version),
                    if result.warnings.is_empty() {
                        String::new()
                    } else {
                        format!(
                            "<small>warnings: {}</small>",
                            escape_html(&result.warnings.join(" | "))
                        )
                    },
                    if result.unresolved_points.is_empty() {
                        String::new()
                    } else {
                        format!(
                            "<small>unresolved: {}</small>",
                            escape_html(
                                &result
                                    .unresolved_points
                                    .iter()
                                    .map(|item| item.code.clone())
                                    .collect::<Vec<_>>()
                                    .join(", ")
                            )
                        )
                    }
                )
            })
            .collect::<Vec<_>>()
            .join("")
    };
    let flow_rows = if analysis.derived_flows.is_empty() {
        "<div class=\"empty-state\">No normalized asset flows were derived for this tx.</div>".to_owned()
    } else {
        format!(
            r#"<div class="table-shell">
  <table>
    <thead>
      <tr>
        <th>Asset</th>
        <th>Token</th>
        <th>From</th>
        <th>To</th>
        <th>Amount raw</th>
        <th>Method</th>
        <th>Confidence</th>
      </tr>
    </thead>
    <tbody>{}</tbody>
  </table>
</div>"#,
            analysis
                .derived_flows
                .iter()
                .map(|flow| {
                    format!(
                        "<tr><td>{}</td><td><code>{}</code></td><td><code>{}</code></td><td><code>{}</code></td><td>{}</td><td>{}</td><td>{:.2}</td></tr>",
                        escape_html(&enum_label(&flow.asset_type)),
                        escape_html(&truncate_middle(flow.token_address.as_deref().unwrap_or("native"), 8)),
                        escape_html(&truncate_middle(&flow.from_address, 8)),
                        escape_html(&truncate_middle(&flow.to_address, 8)),
                        escape_html(&flow.amount_raw),
                        escape_html(&flow.derivation_method),
                        flow.confidence
                    )
                })
                .collect::<Vec<_>>()
                .join("")
        )
    };
    let validation_rows = if analysis.validation_results.is_empty() {
        "<div class=\"empty-state\">No validation messages were emitted for this tx.</div>".to_owned()
    } else {
        format!(
            r#"<div class="table-shell">
  <table>
    <thead>
      <tr>
        <th>Validator</th>
        <th>Status</th>
        <th>Message</th>
      </tr>
    </thead>
    <tbody>{}</tbody>
  </table>
</div>"#,
            analysis
                .validation_results
                .iter()
                .map(|message| {
                    format!(
                        "<tr><td>{}</td><td>{}</td><td>{}</td></tr>",
                        escape_html(&message.validator_id),
                        escape_html(&enum_label(&message.status)),
                        escape_html(&message.message)
                    )
                })
                .collect::<Vec<_>>()
                .join("")
        )
    };
    let unresolved_rows = if analysis.unresolved_items.is_empty() {
        "<div class=\"empty-state\">No unresolved points were recorded.</div>".to_owned()
    } else {
        format!(
            r#"<div class="table-shell">
  <table>
    <thead>
      <tr>
        <th>Code</th>
        <th>Message</th>
      </tr>
    </thead>
    <tbody>{}</tbody>
  </table>
</div>"#,
            analysis
                .unresolved_items
                .iter()
                .map(|item| {
                    format!(
                        "<tr><td>{}</td><td>{}</td></tr>",
                        escape_html(&item.code),
                        escape_html(&item.message)
                    )
                })
                .collect::<Vec<_>>()
            .join("")
        )
    };
    let storyline_panel = render_tx_storyline_panel(&analysis.scenario_outputs);

    format!(
        r#"<div class="tx-analysis-shell">
  <div class="mini-card">
    <strong>Most likely activity: {}</strong>
    <span>{}</span>
    <small>Confidence: {}</small>
    {}
  </div>
  <div class="field-hint">Tx <code>{}</code> · status {} · analysis {}.</div>
  <div class="field-hint">Selector <code>{}</code> · {} recognizers · {} derived flows.</div>
  <div class="field-hint">If the activity looks stale or obviously wrong, use <strong>Reanalyze</strong>. Analyze transaction may load a previously saved result first.</div>
  <div class="section-head"><h3>Execution storyline</h3></div>
  {}
  {}
  <div class="section-head"><h3>Derived flows</h3>{}</div>
  {}
  <div class="section-head"><h3>Unresolved points</h3></div>
  {}
  <details class="raw-json-shell">
    <summary>Raw analysis JSON</summary>
    <pre class="json-shell">{}</pre>
  </details>
  <details class="storyline-evidence-shell">
    <summary><span>Recognizer outputs</span>{}</summary>
    <div class="split-fields tx-recognizer-grid">{}</div>
  </details>
  <details class="storyline-evidence-shell">
    <summary><span>Validation results</span>{}</summary>
    {}
  </details>
</div>"#,
        escape_html(&primary_activity),
        escape_html(&primary_activity_reason),
        escape_html(&primary_confidence),
        share_action,
        escape_html(&truncate_middle(&analysis.tx_hash, 12)),
        escape_html(&enum_label(&analysis.status)),
        escape_html(&enum_label(&analysis.analysis_status)),
        escape_html(
            analysis
                .normalized_execution_facts
                .selector
                .as_deref()
                .unwrap_or("n/a")
        ),
        analysis.recognizer_results.len(),
        analysis.derived_flows.len(),
        storyline_panel,
        render_storyline_gap_explanation(&analysis.scenario_outputs)
            .map(|message| format!(r#"<div class="notice info">{}</div>"#, escape_html(&message)))
            .unwrap_or_default(),
        render_info_popover(
            "What are flows?",
            "Derived flows are normalized asset movements reconstructed from logs, value transfer, and execution facts. They do not try to tell the story in step order by themselves. The timeline takes selected flows and arranges them into ordered steps between addresses."
        ),
        flow_rows,
        unresolved_rows,
        escape_html(&raw_json),
        render_info_popover(
            "What am I looking at?",
            "Recognizers are rule-based detectors that classify the transaction into interpretable activities such as approve, swap, liquidation, arbitrage, or native transfer."
        ),
        recognizer_rows,
        render_info_popover(
            "What are validations?",
            "Validation results are internal checks that compare the inferred story against the available evidence and surface inconsistencies or missing information."
        ),
        validation_rows
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

fn render_info_popover(label: &str, body: &str) -> String {
    format!(
        r#"<details class="help-popover">
  <summary aria-label="{}">?</summary>
  <div class="help-popover-card">{}</div>
</details>"#,
        escape_html(label),
        escape_html(body)
    )
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

fn render_tx_storyline_panel(scenario_outputs: &serde_json::Value) -> String {
    let Some(storyline) = scenario_outputs.get("storyline") else {
        return "<div class=\"empty-state\">No execution storyline is available yet for this transaction.</div>".to_owned();
    };
    let headline = storyline
        .get("headline")
        .and_then(|value| value.as_str())
        .unwrap_or("No transaction headline is available yet.");
    let summary = storyline
        .get("summary")
        .and_then(|value| value.as_str())
        .unwrap_or("LedgerFlow did not derive a storyline summary.");
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
    let net_effect = render_net_effect_panel(storyline.get("net_effect"), subject_scope_label);
    let step_timeline =
        render_storyline_steps(storyline.get("steps"), initiator, executor, protocol_contract);
    let call_path = render_storyline_call_path(storyline.get("call_path"));
    let storyline_visual = render_storyline_visual(storyline, kind, protocol, initiator, executor);
    let funding_sources = render_storyline_flow_group("Funding into executor", storyline.get("funding_sources"));
    let repay_legs = render_storyline_flow_group("Repay / protocol-facing legs", storyline.get("repay_legs"));
    let collateral_receipts =
        render_storyline_flow_group("Collateral / reward receipts", storyline.get("collateral_receipts"));
    let disposition_legs =
        render_storyline_flow_group("Disposition / settlement legs", storyline.get("disposition_legs"));
    let cycle_legs = render_storyline_flow_group("Cycle legs", storyline.get("cycle_legs"));
    let exit_legs = render_storyline_flow_group("Exit legs", storyline.get("exit_legs"));

    format!(
        r#"<div class="storyline-summary-card">
  <span class="storyline-kicker">{}</span>
  <strong>{}</strong>
  <span>{}</span>
  <small>Protocol: {} · Initiator: <code>{}</code> · Executor: <code>{}</code></small>
</div>
{}
{}
{}
<details class="storyline-evidence-shell">
  <summary>Open execution evidence view</summary>
  {}
  {}
  {}
  {}
  {}
  {}
  {}
</details>
"#,
        escape_html(&format!("{} storyline", kind)),
        escape_html(headline),
        escape_html(summary),
        escape_html(protocol),
        escape_html(&truncate_middle(initiator, 10)),
        escape_html(&truncate_middle(executor, 10)),
        subject_scope,
        net_effect,
        step_timeline,
        storyline_visual,
        call_path,
        funding_sources,
        repay_legs,
        collateral_receipts,
        disposition_legs,
        if kind == "arbitrage" { format!("{}{}", cycle_legs, exit_legs) } else { String::new() },
    )
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
                .map(|value| format!("<code>{}</code>", escape_html(&truncate_middle(value, 10))))
                .collect::<Vec<_>>()
                .join("")
        })
        .unwrap_or_default();
    format!(
        r#"<div class="mini-card">
  <strong>Subject scope</strong>
  <span>{}</span>
  <div class="address-pill-row">{}</div>
</div>"#,
        escape_html(label),
        addresses
    )
}

fn render_net_effect_panel(value: Option<&serde_json::Value>, subject_scope_label: &str) -> String {
    let Some(effect) = value else {
        return String::new();
    };
    let rows = render_net_effect_rows(effect);
    format!(
        r#"<section class="storyline-net-effect">
  <div class="panel-head"><h4>Net effect for strategy</h4><span class="badge">subject scope view</span></div>
  <div class="panel-copy">Calculated across <strong>{}</strong>. Movements entirely inside this scope are ignored; only assets crossing this boundary show up here. <strong>In</strong> means assets entered the scope, <strong>Out</strong> means assets left it, and <strong>Net</strong> is <strong>In - Out</strong>.</div>
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
</section>"#,
        escape_html(subject_scope_label),
        rows
    )
}

fn render_net_effect_rows(effect: &serde_json::Value) -> String {
    let mut rows = effect
        .get("rows")
        .and_then(|value| value.as_array())
        .map(|items| {
            items
                .iter()
                .map(render_net_effect_asset_row)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if let Some(gas) = effect.get("gas").filter(|value| !value.is_null()) {
        rows.push(render_net_effect_gas_row(gas));
    }
    if rows.is_empty() {
        "<tr><td colspan=\"6\">No scope-crossing asset changes detected yet.</td></tr>".to_owned()
    } else {
        rows.join("")
    }
}

fn render_net_effect_asset_row(item: &serde_json::Value) -> String {
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
        .map(|value| truncate_middle(value, 8))
        .unwrap_or_else(|| "native".to_owned());
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
        "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{} · {}</td><td><code>{}</code></td></tr>",
        escape_html(symbol),
        render_amount_stack(inflow_display, inflow_raw),
        render_amount_stack(outflow_display, outflow_raw),
        render_amount_stack(net_display, net_raw),
        escape_html(symbol),
        escape_html(name),
        escape_html(&address),
    )
}

fn render_net_effect_gas_row(item: &serde_json::Value) -> String {
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
        render_amount_stack(amount_display, amount_raw),
        render_amount_stack(&format!("-{}", amount_display), &format!("-{}", amount_raw)),
        escape_html(symbol),
        escape_html(name),
    )
}

fn render_amount_stack(display: &str, raw: &str) -> String {
    let display = normalize_amount_display(display, raw);
    format!(
        "<div class=\"amount-stack\"><strong>{}</strong><small>raw: {}</small></div>",
        escape_html(&display),
        escape_html(raw),
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

fn render_storyline_steps(
    value: Option<&serde_json::Value>,
    initiator: &str,
    executor: &str,
    protocol_contract: Option<&str>,
) -> String {
    let Some(items) = value.and_then(|value| value.as_array()) else {
        return String::new();
    };
    if items.is_empty() {
        return String::new();
    }
    format!(
        r#"<section class="storyline-steps">
  <div class="panel-head"><h4>What happened, step by step</h4><span class="badge">timeline</span></div>
  <div class="timeline-list">{}</div>
</section>"#,
        items
            .iter()
            .map(|item| {
                let order = item.get("order").and_then(|value| value.as_u64()).unwrap_or(0);
                let title = item.get("title").and_then(|value| value.as_str()).unwrap_or("Step");
                let token = item.get("token").unwrap_or(&serde_json::Value::Null);
                let symbol = token.get("symbol").and_then(|value| value.as_str()).unwrap_or("asset");
                let name = token.get("name").and_then(|value| value.as_str()).unwrap_or("unlabeled");
                let token_address = token
                    .get("address")
                    .and_then(|value| value.as_str())
                    .map(|value| truncate_middle(value, 8))
                    .unwrap_or_else(|| "native".to_owned());
                let amount_display = item
                    .get("amount_display")
                    .and_then(|value| value.as_str())
                    .unwrap_or("n/a");
                let amount_raw = item
                    .get("amount_raw")
                    .and_then(|value| value.as_str())
                    .unwrap_or("n/a");
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
                let from_short = truncate_middle(from_raw, 10);
                let to_short = truncate_middle(to_raw, 10);
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
      <small>{} · {} · raw: {}</small>
      {}
    </div>
  </div>
  <div class="timeline-flow compact">
    <div class="timeline-side">
      <code>{}</code>
      <div class="timeline-role-row">{}</div>
    </div>
    <div class="timeline-mid">
      <strong>{}</strong>
      <span>{}</span>
      <span class="timeline-arrow-inline">→</span>
    </div>
    <div class="timeline-side">
      <code>{}</code>
      <div class="timeline-role-row">{}</div>
    </div>
  </div>
</article>"#,
                    order,
                    escape_html(title),
                    escape_html(symbol),
                    escape_html(name),
                    escape_html(amount_raw),
                    source_hint_label
                        .map(|value| format!(
                            "<span class=\"timeline-hint\">Likely source: {}</span>",
                            escape_html(value)
                        ))
                        .unwrap_or_default(),
                    escape_html(&from_short),
                    from_roles,
                    escape_html(&amount_display),
                    escape_html(&format!("{} · {}", name, token_address)),
                    escape_html(&to_short),
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
) -> String {
    let protocol_contract = storyline
        .get("protocol_contract")
        .and_then(|value| value.as_str());
    let funding_lane = render_storyline_lane(
        "Funding",
        "Where the executor got assets or approvals before protocol interaction.",
        storyline.get("funding_sources"),
        "funding",
        initiator,
        executor,
        protocol_contract,
    );
    let repay_lane = render_storyline_lane(
        "Protocol-facing legs",
        if kind == "liquidation" {
            "Debt repayment or direct protocol-facing asset movement."
        } else {
            "Venue-facing execution legs."
        },
        storyline.get("repay_legs").or_else(|| storyline.get("cycle_legs")),
        "protocol",
        initiator,
        executor,
        protocol_contract,
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
    );
    let exit_lane = render_storyline_lane(
        if kind == "liquidation" {
            "Disposition / settlement"
        } else {
            "Exit / settlement"
        },
        "Where assets moved next after the main action.",
        storyline.get("disposition_legs").or_else(|| storyline.get("exit_legs")),
        "exit",
        initiator,
        executor,
        protocol_contract,
    );

    format!(
        r#"<div class="storyline-visual-shell">
  <div class="storyline-main-path">
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
  </div>
  <div class="storyline-lanes">
    {}
    {}
    {}
    {}
  </div>
</div>"#,
        escape_html(&truncate_middle(initiator, 10)),
        escape_html(&truncate_middle(executor, 10)),
        escape_html(protocol),
        escape_html(if kind == "liquidation" {
            "Detected liquidation target"
        } else if kind == "arbitrage" {
            "Detected venue set"
        } else {
            "Resolved execution target"
        }),
        funding_lane,
        repay_lane,
        receipt_lane,
        exit_lane
    )
}

fn render_storyline_lane(
    title: &str,
    subtitle: &str,
    value: Option<&serde_json::Value>,
    lane_kind: &str,
    initiator: &str,
    executor: &str,
    protocol_contract: Option<&str>,
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
        .map(|value| truncate_middle(value, 8))
        .unwrap_or_else(|| "native".to_owned());
    let from_address = item
        .get("from_address")
        .and_then(|value| value.as_str())
        .map(|value| truncate_middle(value, 10))
        .unwrap_or_else(|| "n/a".to_owned());
    let to_address = item
        .get("to_address")
        .and_then(|value| value.as_str())
        .map(|value| truncate_middle(value, 10))
        .unwrap_or_else(|| "n/a".to_owned());
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
  <small>{} · <code>{}</code></small>
  <small><code>{}</code> {} → <code>{}</code> {}</small>
  <small>{}</small>
  {}
</article>"#,
        escape_html(lane_kind),
        escape_html(asset_type),
        escape_html(lane_kind),
        escape_html(token_symbol),
        escape_html(token_name),
        escape_html(&token_address),
        escape_html(&from_address),
        from_roles,
        escape_html(&to_address),
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

fn render_storyline_call_path(value: Option<&serde_json::Value>) -> String {
    let Some(items) = value.and_then(|value| value.as_array()) else {
        return "<div class=\"empty-state\">No trace call path was recorded.</div>".to_owned();
    };
    if items.is_empty() {
        return "<div class=\"empty-state\">No trace call path was recorded.</div>".to_owned();
    }
    format!(
        r#"<div class="table-shell">
  <table>
    <thead>
      <tr>
        <th>Depth</th>
        <th>From</th>
        <th>To</th>
        <th>Selector</th>
        <th>Call type</th>
        <th>Value</th>
      </tr>
    </thead>
    <tbody>{}</tbody>
  </table>
</div>"#,
        items
            .iter()
            .map(|item| {
                format!(
                    "<tr><td>{}</td><td><code>{}</code></td><td><code>{}</code></td><td><code>{}</code></td><td>{}</td><td>{}</td></tr>",
                    item.get("depth").and_then(|value| value.as_u64()).map(|value| value.to_string()).unwrap_or_else(|| "n/a".to_owned()),
                    escape_html(&truncate_middle(item.get("from_address").and_then(|value| value.as_str()).unwrap_or("n/a"), 10)),
                    escape_html(&truncate_middle(item.get("to_address").and_then(|value| value.as_str()).unwrap_or("n/a"), 10)),
                    escape_html(item.get("selector").and_then(|value| value.as_str()).unwrap_or("n/a")),
                    escape_html(item.get("call_type").and_then(|value| value.as_str()).unwrap_or("n/a")),
                    escape_html(item.get("value").and_then(|value| value.as_str()).unwrap_or("n/a")),
                )
            })
            .collect::<Vec<_>>()
            .join("")
    )
}

fn render_storyline_flow_group(title: &str, value: Option<&serde_json::Value>) -> String {
    let Some(items) = value.and_then(|value| value.as_array()) else {
        return String::new();
    };
    if items.is_empty() {
        return String::new();
    }
    format!(
        r#"<h4>{}</h4>
<div class="table-shell">
  <table>
    <thead>
      <tr>
        <th>Asset</th>
        <th>Token</th>
        <th>From</th>
        <th>To</th>
        <th>Amount raw</th>
        <th>Method</th>
      </tr>
    </thead>
    <tbody>{}</tbody>
  </table>
</div>"#,
        escape_html(title),
        items
            .iter()
            .map(|item| {
                format!(
                    "<tr><td>{}</td><td><code>{}</code></td><td><code>{}</code></td><td><code>{}</code></td><td>{}</td><td>{}</td></tr>",
                    escape_html(item.get("asset_type").and_then(|value| value.as_str()).unwrap_or("n/a")),
                    escape_html(&truncate_middle(item.get("token_address").and_then(|value| value.as_str()).unwrap_or("native"), 8)),
                    escape_html(&truncate_middle(item.get("from_address").and_then(|value| value.as_str()).unwrap_or("n/a"), 10)),
                    escape_html(&truncate_middle(item.get("to_address").and_then(|value| value.as_str()).unwrap_or("n/a"), 10)),
                    escape_html(item.get("amount_raw").and_then(|value| value.as_str()).unwrap_or("n/a")),
                    escape_html(item.get("derivation_method").and_then(|value| value.as_str()).unwrap_or("n/a")),
                )
            })
            .collect::<Vec<_>>()
            .join("")
    )
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
                    escape_html(&alert.triggered_at.format("%Y-%m-%d %H:%M:%S UTC").to_string()),
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
        if query.behavior_tag.is_none() { "selected" } else { "" },
        if query.behavior_tag.as_deref() == Some("arbitrage") { "selected" } else { "" },
        if query.behavior_tag.as_deref() == Some("position") { "selected" } else { "" },
        if query.behavior_tag.as_deref() == Some("lp") { "selected" } else { "" },
        if query.behavior_tag.as_deref() == Some("liquidation") { "selected" } else { "" },
        if query.behavior_tag.as_deref() == Some("unknown") { "selected" } else { "" },
        escape_html(query.protocol.as_deref().unwrap_or_default()),
        if query.sort_by == "timestamp" { "selected" } else { "" },
        if query.sort_by == "block_number" { "selected" } else { "" },
        if query.sort_by == "value_usd" { "selected" } else { "" },
        if query.sort_by == "gas_cost_usd" { "selected" } else { "" },
        if query.sort_by == "estimated_pnl_usd" { "selected" } else { "" },
        if query.sort_dir == "desc" { "selected" } else { "" },
        if query.sort_dir == "asc" { "selected" } else { "" },
        if query.page_size == 10 { "selected" } else { "" },
        if query.page_size == 25 { "selected" } else { "" },
        if query.page_size == 50 { "selected" } else { "" },
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
    let limit = query
        .and_then(|value| value.limit)
        .unwrap_or(100);
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
                ("Waiting for first historical sync".to_owned(), "info".to_owned())
            };
        }
    }

    ("Waiting for first historical sync".to_owned(), "info".to_owned())
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
        .map(|message| format!("<div class=\"notice success\">{}</div>", escape_html(message)))
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
  <defs>
    <linearGradient id="ledgerflowGradient" x1="8" y1="8" x2="56" y2="56" gradientUnits="userSpaceOnUse">
      <stop stop-color="#0d6e63"/>
      <stop offset="1" stop-color="#c55f0c"/>
    </linearGradient>
  </defs>
  <rect x="6" y="6" width="52" height="52" rx="16" fill="url(#ledgerflowGradient)"/>
  <path d="M21 20H43V27H29V31H41V38H29V44H21V20Z" fill="#fffaf3"/>
  <path d="M31 20H43V27H39V44H31V20Z" fill="#f3dfc8" opacity="0.92"/>
</svg>"##
}

fn truncate_middle(value: &str, keep: usize) -> String {
    if value.len() <= keep * 2 {
        return value.to_owned();
    }
    format!("{}...{}", &value[..keep], &value[value.len() - keep..])
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

fn shared_styles() -> &'static str {
    r#"
:root {
  --bg: #f8fafc;
  --panel: #ffffff;
  --ink: #0f172a;
  --muted: #5f6b7a;
  --line: #e2e8f0;
  --accent: #1f6feb;
  --accent-2: #b45309;
  --cream: #f1f5f9;
  --shadow: none;
}
* { box-sizing: border-box; }
body { margin: 0; overflow-x: hidden; background: var(--bg); color: var(--ink); font-family: Inter, "Segoe UI", "Helvetica Neue", Arial, sans-serif; }
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
.notice.info { background: rgba(13,110,99,.08); color: var(--accent); }
.notice.success { background: rgba(13,110,99,.1); color: var(--accent); }
.notice.warning { background: rgba(197,95,12,.12); color: var(--accent-2); }
.notice.error { background: rgba(197,95,12,.1); color: var(--accent-2); }
label { display: grid; gap: 8px; font-size: .95rem; }
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
.tx-analysis-shell { display: grid; gap: 14px; }
.storyline-summary-card { display: grid; gap: 8px; padding: 14px; border: 1px solid var(--line); border-radius: 10px; background: #fff; }
.storyline-summary-card strong { font-size: clamp(1.1rem, 2vw, 1.4rem); line-height: 1.25; }
.storyline-kicker { color: var(--accent); text-transform: uppercase; letter-spacing: .14em; font-size: .75rem; font-weight: 700; }
.address-pill-row { display: flex; gap: 8px; flex-wrap: wrap; }
.address-pill-row code { padding: 5px 8px; border: 1px solid var(--line); border-radius: 8px; background: #fff; }
.storyline-net-effect, .storyline-steps { display: grid; gap: 12px; }
.compact-table-shell { margin-top: 4px; }
.compact-table td, .compact-table th { padding: 6px 8px; }
.amount-stack { display: grid; gap: 2px; }
.amount-stack strong { font-size: .96rem; line-height: 1.25; }
.amount-stack small { color: var(--muted); line-height: 1.3; }
.table-badge { display: inline-flex; align-items: center; padding: 3px 6px; border-radius: 999px; font-size: .75rem; line-height: 1.2; border: 1px solid var(--line); background: #fff; color: var(--muted); }
.table-badge.gained { border-color: rgba(13,110,99,.24); color: var(--accent); }
.table-badge.spent, .table-badge.gas { border-color: rgba(197,95,12,.24); color: var(--accent-2); }
.timeline-list { display: grid; gap: 10px; }
.timeline-step { display: grid; gap: 8px; padding: 10px 12px; border: 1px solid var(--line); border-radius: 8px; background: #fff; }
.timeline-head { display: grid; gap: 10px; grid-template-columns: 28px minmax(0, 1fr); align-items: start; }
.timeline-index { width: 28px; height: 28px; border-radius: 6px; display: grid; place-items: center; background: #eef5ff; color: var(--accent); font-weight: 700; font-size: .85rem; }
.timeline-copy { display: grid; gap: 4px; min-width: 0; }
.timeline-copy p, .timeline-copy small { margin: 0; color: var(--muted); line-height: 1.5; }
.timeline-hint { width: fit-content; margin-top: 2px; padding: 3px 6px; border-radius: 999px; background: #eef5ff; color: #1d5ea8; font-size: .72rem; line-height: 1.3; }
.timeline-flow { display: grid; gap: 10px; grid-template-columns: minmax(0, 1fr) minmax(180px, .7fr) minmax(0, 1fr); align-items: center; }
.timeline-flow.compact { gap: 8px; }
.timeline-side { display: grid; gap: 6px; min-width: 0; }
.timeline-side code { padding: 6px 8px; border: 1px solid var(--line); border-radius: 8px; background: #fff; overflow-wrap: anywhere; }
.timeline-role-row { display: flex; gap: 6px; flex-wrap: wrap; align-items: center; }
.timeline-mid { display: grid; gap: 2px; justify-items: center; text-align: center; }
.timeline-mid strong { font-size: 1rem; line-height: 1.2; }
.timeline-mid span { color: var(--muted); line-height: 1.35; }
.timeline-arrow-inline { font-size: 1.1rem; color: var(--muted); }
.storyline-evidence-shell { border: 1px solid var(--line); border-radius: 8px; background: #fff; overflow: hidden; }
.storyline-evidence-shell summary { cursor: pointer; padding: 10px 12px; font-weight: 700; }
.storyline-evidence-shell[open] { padding-bottom: 14px; }
.storyline-evidence-shell[open] summary { border-bottom: 1px solid var(--line); margin-bottom: 14px; }
.section-head { display: flex; align-items: center; justify-content: space-between; gap: 10px; flex-wrap: wrap; }
.section-head h3 { margin: 0; }
.help-popover { position: relative; }
.help-popover summary { list-style: none; cursor: pointer; width: 22px; height: 22px; display: grid; place-items: center; border-radius: 999px; border: 1px solid var(--line); background: #fff; color: var(--muted); font-weight: 700; font-size: .78rem; }
.help-popover summary::-webkit-details-marker { display: none; }
.help-popover-card { position: absolute; right: 0; top: calc(100% + 8px); width: min(320px, 75vw); padding: 10px 12px; border: 1px solid var(--line); border-radius: 8px; background: #fff; box-shadow: 0 8px 18px rgba(15,23,42,.08); color: var(--muted); line-height: 1.55; z-index: 4; }
.storyline-visual-shell { display: grid; gap: 12px; padding: 12px; border: 1px solid var(--line); border-radius: 10px; background: #fff; }
.storyline-main-path { display: grid; gap: 10px; grid-template-columns: minmax(0, 1fr) auto minmax(0, 1fr) auto minmax(0, 1fr); align-items: stretch; }
.story-node { display: grid; gap: 6px; padding: 10px; border: 1px solid var(--line); border-radius: 8px; background: #fff; min-width: 0; }
.story-node.accent { border-color: rgba(31,111,235,.25); background: #f8fbff; }
.story-node.protocol { border-color: rgba(180,83,9,.18); background: #fffaf5; }
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
@media (max-width: 960px) {
  .hero, .dashboard-shell, .dashboard-grid { grid-template-columns: 1fr; }
  .sidebar { position: static; }
  .storyline-main-path { grid-template-columns: 1fr; }
  .story-arrow { transform: rotate(90deg); padding: 4px 0; }
  .timeline-step { grid-template-columns: 1fr; }
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
