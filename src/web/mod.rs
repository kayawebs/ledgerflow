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
use sqlx::postgres::PgPoolOptions;
use tracing::info;
use uuid::Uuid;

use crate::{
    core::errors::{LedgerError, LedgerResult},
    domain::{
        entities::{
            AddressCluster, AppUser, ClassifiedTransaction, ClusterBackfillRun, ClusterCoverage,
            ClusterSpendSummary, ClusterTrackingState, EvidencePack, TransactionAnalysis,
        },
        enums::{AddressHitDirection, ClusterBackfillStatus, VerificationPurpose},
    },
    services::{
        auth::{
            AuthService, LogVerificationSender, ResendVerificationSender, VerificationSender,
            verification_purpose_from_str, verification_purpose_to_str,
        },
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
    pub email_from: Option<String>,
    pub email_delivery_mode: EmailDeliveryMode,
    pub verification_ttl_minutes: i64,
    pub session_ttl_days: i64,
}

impl WebConfig {
    pub fn from_env() -> Self {
        let resend_api_key = first_env(&["LEDGERFLOW_RESEND_API_KEY", "RESEND_API_KEY"]);
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
            web_base_url: std::env::var("LEDGERFLOW_WEB_BASE_URL")
                .unwrap_or_else(|_| "http://127.0.0.1:3001".to_owned()),
            session_cookie_name: std::env::var("LEDGERFLOW_SESSION_COOKIE_NAME")
                .unwrap_or_else(|_| "ledgerflow_session".to_owned()),
            resend_api_key,
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
            "resend" => Self::Resend,
            _ => Self::Auto,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Log => "log",
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
    notice: Option<String>,
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
}

#[derive(Debug, Deserialize, Clone)]
struct ClusterDetailQuery {
    start: Option<String>,
    end: Option<String>,
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
struct RunTxForm {
    chain_id: u64,
    tx_hash: String,
    recognizer_ids: Option<String>,
    action: String,
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

#[derive(Debug, Deserialize, Serialize)]
struct ReplayTxRequest {
    chain_id: u64,
    tx_hash: String,
    ruleset_id: Option<String>,
    recognizer_ids: Option<Vec<String>>,
}

#[derive(Debug, Deserialize, Serialize)]
struct AnalysisEnvelope {
    analysis: TransactionAnalysis,
    evidence_pack: EvidencePack,
}

#[derive(Debug, Deserialize)]
struct IndexerProgressPayload {
    chain_id: u64,
    worker_id: String,
    last_indexed_block: u64,
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

#[derive(Debug, Serialize)]
struct ClusterLiveResponse {
    status_label: String,
    status_tone: String,
    tracking: Option<ClusterTrackingState>,
    coverage: Option<ClusterCoverage>,
    runs: Vec<ClusterBackfillRun>,
    spend_summary: Option<ClusterSpendSummary>,
    classified_transactions: Vec<ClassifiedTransaction>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DashboardTab {
    Clusters,
    ClusterAnalysis,
    TxAnalysis,
    ClusterGraph,
    IndexerProgress,
    IndexerLookup,
}

fn section_title(tab: &DashboardTab) -> &'static str {
    match tab {
        DashboardTab::Clusters => "Cluster management",
        DashboardTab::ClusterAnalysis => "Cluster analysis",
        DashboardTab::TxAnalysis => "Single tx analysis",
        DashboardTab::ClusterGraph => "Cluster graph",
        DashboardTab::IndexerProgress => "Indexer progress",
        DashboardTab::IndexerLookup => "Indexer address lookup",
    }
}

impl DashboardTab {
    fn from_query(value: Option<&str>) -> Self {
        match value {
            Some("cluster-analysis") => Self::ClusterAnalysis,
            Some("tx-analysis") => Self::TxAnalysis,
            Some("cluster-graph") => Self::ClusterGraph,
            Some("indexer-progress") => Self::IndexerProgress,
            Some("indexer-lookup") => Self::IndexerLookup,
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
            if let (Some(api_key), Some(from_email)) =
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
        .route("/app/clusters/create", post(create_cluster))
        .route(
            "/app/clusters/{cluster_id}/tracking/enable",
            post(enable_cluster_tracking),
        )
        .route(
            "/app/clusters/{cluster_id}/tracking/pause",
            post(pause_cluster_tracking),
        )
        .route("/app/tx-analysis", get(tx_analysis_page))
        .route("/app/tx-analysis/run", post(run_tx_analysis))
        .route("/app/cluster-graph", get(cluster_graph_page))
        .route("/app/indexer/progress", get(indexer_progress_page))
        .route("/app/indexer/address-lookup", get(indexer_lookup_page))
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
        DashboardTab::TxAnalysis => "/app/tx-analysis".to_owned(),
        DashboardTab::ClusterGraph => "/app/cluster-graph".to_owned(),
        DashboardTab::IndexerProgress => "/app/indexer/progress".to_owned(),
        DashboardTab::IndexerLookup => "/app/indexer/address-lookup".to_owned(),
    };
    Ok(Redirect::to(&location).into_response())
}

async fn app_home() -> Response {
    Redirect::to("/app/clusters").into_response()
}

async fn clusters_page(
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
            tab: Some("clusters".to_owned()),
            cluster_id: None,
            start: None,
            end: None,
            notice: query.notice,
        },
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
            notice: query.notice,
        },
        None,
        None,
        None,
        None,
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
            notice: query.notice,
        },
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
            notice: query.notice,
        },
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
            notice: query.notice,
        },
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
            notice: query.notice.clone(),
        },
        None,
        error,
        payload,
        Some(query),
    )
    .await
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
                notice: None,
            };
            render_dashboard_response(&state, user, query, None, Some(err.to_string()), None, None)
                .await
        }
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

async fn run_tx_analysis(
    State(state): State<WebState>,
    headers: HeaderMap,
    Form(form): Form<RunTxForm>,
) -> Result<Response, WebError> {
    let Some(user) = current_user(&state, &headers).await? else {
        return Ok(Redirect::to("/auth?notice=auth-required").into_response());
    };
    let result = match form.action.as_str() {
        "load" => {
            let path = format!(
                "/analyses/{}?chain_id={}",
                form.tx_hash.trim(),
                form.chain_id
            );
            api_get_json::<AnalysisEnvelope>(&state, &path)
                .await
                .map(|payload| serde_json::to_value(payload).unwrap_or_default())
        }
        "replay" => {
            let recognizer_ids = parse_recognizer_ids(form.recognizer_ids.as_deref());
            api_post_json::<_, AnalysisEnvelope>(
                &state,
                "/replay/tx",
                &ReplayTxRequest {
                    chain_id: form.chain_id,
                    tx_hash: form.tx_hash.clone(),
                    ruleset_id: Some("base.ruleset.v0".to_owned()),
                    recognizer_ids: recognizer_ids.clone(),
                },
            )
            .await
            .map(|payload| serde_json::to_value(payload).unwrap_or_default())
        }
        _ => api_post_json::<_, AnalysisEnvelope>(
            &state,
            "/analyses/tx",
            &AnalyzeTxRequest {
                chain_id: form.chain_id,
                tx_hash: form.tx_hash.clone(),
            },
        )
        .await
        .map(|payload| serde_json::to_value(payload).unwrap_or_default()),
    };

    let query = DashboardQuery {
        tab: Some("tx-analysis".to_owned()),
        cluster_id: None,
        start: None,
        end: None,
        notice: None,
    };
    match result {
        Ok(payload) => {
            render_dashboard_response(&state, user, query, Some(payload), None, None, None).await
        }
        Err(err) => {
            render_dashboard_response(&state, user, query, None, Some(err.to_string()), None, None)
                .await
        }
    }
}

async fn render_dashboard_response(
    state: &WebState,
    user: AppUser,
    query: DashboardQuery,
    tx_result: Option<serde_json::Value>,
    error: Option<String>,
    indexer_lookup: Option<IndexerAddressRefsPayload>,
    indexer_query: Option<IndexerLookupQuery>,
) -> Result<Response, WebError> {
    let tab = DashboardTab::from_query(query.tab.as_deref());
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

    let (tracking, coverage, runs, spend_summary, classified_transactions, cluster_status, cluster_error) =
        if tab == DashboardTab::ClusterAnalysis {
            if let Some(cluster) = selected_cluster.as_ref() {
                match load_cluster_live_response(
                    state,
                    cluster.cluster_id,
                    query.start.as_deref(),
                    query.end.as_deref(),
                )
                .await
                {
                    Ok(payload) => (
                        payload.tracking,
                        payload.coverage,
                        payload.runs,
                        payload.spend_summary,
                        payload.classified_transactions,
                        Some((payload.status_label, payload.status_tone)),
                        None::<String>,
                    ),
                    Err(err) => (None, None, Vec::new(), None, Vec::new(), None, Some(err.to_string())),
                }
            } else {
                (None, None, Vec::new(), None, Vec::new(), None, None)
            }
        } else {
            (None, None, Vec::new(), None, Vec::new(), None, None)
        };

    let (indexer_progress, indexer_error) = if tab == DashboardTab::IndexerProgress {
        match indexer_get_json::<IndexerProgressPayload>(state, "/progress").await {
            Ok(payload) => (Some(payload), None),
            Err(err) => (None, Some(err.to_string())),
        }
    } else {
        (None, None)
    };

    let notice = query.notice.as_deref().and_then(render_notice_message);
    Ok(Html(render_dashboard_page(
        state,
        &user,
        &tab,
        notice,
        &clusters,
        selected_cluster.as_ref(),
        tracking.as_ref(),
        coverage.as_ref(),
        &runs,
        spend_summary.as_ref(),
        &classified_transactions,
        cluster_status.as_ref(),
        tx_result.as_ref(),
        indexer_progress.as_ref(),
        indexer_lookup.as_ref(),
        indexer_query.as_ref(),
        query.start.as_deref(),
        query.end.as_deref(),
        error
            .as_deref()
            .or(cluster_error.as_deref())
            .or(indexer_error.as_deref()),
    ))
    .into_response())
}

async fn load_cluster_live_response(
    state: &WebState,
    cluster_id: Uuid,
    start: Option<&str>,
    end: Option<&str>,
) -> Result<ClusterLiveResponse, WebError> {
    let tracking = api_get_json::<ClusterTrackingState>(state, &format!("/clusters/{}/tracking", cluster_id))
        .await
        .ok();
    let coverage = api_get_json::<ClusterCoverage>(state, &format!("/clusters/{}/coverage", cluster_id))
        .await
        .ok();
    let runs = api_get_json::<Vec<ClusterBackfillRun>>(
        state,
        &format!("/clusters/{}/backfill-runs", cluster_id),
    )
    .await
    .unwrap_or_default();
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
    let spend_summary = api_get_json::<ClusterSpendSummary>(state, &summary_path).await.ok();
    let classified_transactions = api_get_json::<Vec<ClassifiedTransaction>>(
        state,
        &format!("/clusters/{}/classified-transactions?limit=12", cluster_id),
    )
    .await
    .unwrap_or_default();
    let (status_label, status_tone) = derive_cluster_status(coverage.as_ref(), &runs);
    Ok(ClusterLiveResponse {
        status_label,
        status_tone,
        tracking,
        coverage,
        runs,
        spend_summary,
        classified_transactions,
    })
}

async fn current_user(state: &WebState, headers: &HeaderMap) -> Result<Option<AppUser>, WebError> {
    let Some(token) = session_token_from_headers(headers, &state.session_cookie_name) else {
        return Ok(None);
    };
    Ok(state.auth_service.get_user_by_session_token(&token).await?)
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

fn parse_recognizer_ids(raw: Option<&str>) -> Option<Vec<String>> {
    let values = raw
        .unwrap_or_default()
        .split([',', '\n'])
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if values.is_empty() {
        None
    } else {
        Some(values)
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
        "tracking-enabled" => Some("Cluster tracking enabled."),
        "tracking-paused" => Some("Cluster tracking paused."),
        "tracking-error" => Some("Tracking action failed. Inspect the latest cluster state or server logs."),
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
    <style>{}</style>
  </head>
  <body class="marketing">
    <div class="shell">
      <header class="topbar">
        <a class="brand" href="/">LedgerFlow</a>
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
    <div class="price-note">Private beta override: <strong>currently free</strong></div>
    <a class="cta primary" href="/auth">Register for beta</a>
  </article>
  <article class="pricing-card">
    <span class="eyebrow">Status</span>
    <h2>Internal platform starter</h2>
    <p>The billing surface is productized now, but enforcement is intentionally disabled while the workflow and recognizers are still under active iteration.</p>
  </article>
</section>
"#,
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
        "<div class=\"notice warning\">Verification email delivery is in <strong>{}</strong> mode. Codes will be written to server logs unless you configure `LEDGERFLOW_EMAIL_FROM` and `LEDGERFLOW_RESEND_API_KEY` or force `LEDGERFLOW_EMAIL_DELIVERY_MODE=resend`.</div>",
        escape_html(email_delivery_mode)
    )
}

#[allow(clippy::too_many_arguments)]
fn render_dashboard_page(
    state: &WebState,
    user: &AppUser,
    tab: &DashboardTab,
    notice: Option<&str>,
    clusters: &[DashboardClusterRecord],
    selected_cluster: Option<&DashboardClusterRecord>,
    tracking: Option<&ClusterTrackingState>,
    coverage: Option<&ClusterCoverage>,
    runs: &[ClusterBackfillRun],
    spend_summary: Option<&ClusterSpendSummary>,
    classified_transactions: &[ClassifiedTransaction],
    cluster_status: Option<&(String, String)>,
    tx_result: Option<&serde_json::Value>,
    indexer_progress: Option<&IndexerProgressPayload>,
    indexer_lookup: Option<&IndexerAddressRefsPayload>,
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
                    r#"<a class="cluster-card" href="/app/clusters/{}">
<strong>{}</strong>
<span>{}</span>
<small>{} addresses · chain {}</small>
</a>"#,
                    cluster.cluster_id,
                    escape_html(&cluster.name),
                    escape_html(cluster.description.as_deref().unwrap_or("No description")),
                    cluster.address_count,
                    cluster.chain_id
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
        format!(
            r#"
<section class="panel">
  <div class="panel-head"><h2>{}</h2><span class="badge">cluster analysis</span></div>
  <div class="panel-body">
    <form class="inline-form" method="get" action="/app/clusters/{}">
      <div class="stat"><span>Cluster</span><strong>{}</strong></div>
      <label>Start<input type="text" name="start" value="{}" placeholder="2026-04-01T00:00:00Z"></label>
      <label>End<input type="text" name="end" value="{}" placeholder="2026-04-30T23:59:59Z"></label>
      <button class="cta secondary" type="submit">Refresh analysis</button>
    </form>
    <div id="cluster-live-root" data-cluster-id="{}" data-start="{}" data-end="{}"></div>
    <div id="cluster-live-status" class="notice {}">{}</div>
    <h3>Tracking</h3>
    <div id="cluster-live-tracking">{}</div>
    <h3>Coverage</h3>
    <div id="cluster-live-coverage">{}</div>
    <h3>Spend summary</h3>
    <div id="cluster-live-summary">{}</div>
    <h3>Recent backfill runs</h3>
    <div id="cluster-live-runs">{}</div>
    <h3>Recent classified transactions</h3>
    <div id="cluster-live-classified">{}</div>
  </div>
</section>
"#,
            escape_html(&cluster.name),
            cluster.cluster_id,
            escape_html(&format!("{} · {}", cluster.name, cluster.owner_label)),
            escape_html(start.unwrap_or_default()),
            escape_html(end.unwrap_or_default()),
            cluster.cluster_id,
            escape_html(start.unwrap_or_default()),
            escape_html(end.unwrap_or_default()),
            escape_html(status_tone),
            escape_html(status_label),
            render_tracking_status_panel(cluster, tracking),
            render_cluster_coverage_cards(coverage),
            render_cluster_summary_panel(spend_summary),
            render_cluster_runs_table(runs),
            render_recent_classified_transactions_panel(classified_transactions),
        )
    } else {
        "<section class=\"panel\"><div class=\"panel-body\"><div class=\"empty-state\">Select or create a cluster to view analysis.</div></div></section>".to_owned()
    };

    let tx_panel = format!(
        r#"
<section class="panel">
  <div class="panel-head"><h2>Single transaction analysis</h2><span class="badge">debug layer</span></div>
  <div class="panel-body">
    <form class="stack-form" method="post" action="/app/tx-analysis/run">
      <label>Chain ID<input type="number" name="chain_id" value="{}" required></label>
      <label>Transaction hash<input type="text" name="tx_hash" placeholder="0x..." required></label>
      <label>Recognizer overrides<input type="text" name="recognizer_ids" placeholder="selector_presence,native_transfer"></label>
      <div class="button-row">
        <button class="cta secondary" type="submit" name="action" value="analyze">Analyze</button>
        <button class="cta secondary" type="submit" name="action" value="load">Load saved</button>
        <button class="cta secondary" type="submit" name="action" value="replay">Replay</button>
      </div>
    </form>
    {}
  </div>
</section>
"#,
        state.default_chain_id,
        tx_result
            .map(|payload| {
                format!(
                    "<pre class=\"json-shell\">{}</pre>",
                    escape_html(&serde_json::to_string_pretty(payload).unwrap_or_default())
                )
            })
            .unwrap_or_else(|| {
                "<div class=\"empty-state\">Run one analyze / load / replay action to inspect a transaction.</div>".to_owned()
            })
    );

    let graph_panel = render_cluster_graph_panel(clusters);
    let indexer_progress_panel = render_indexer_progress_panel(indexer_progress);
    let indexer_lookup_panel = render_indexer_lookup_panel(indexer_lookup, indexer_query);

    format!(
        r#"<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>LedgerFlow Dashboard</title>
    <style>{}</style>
  </head>
  <body class="dashboard-body">
    <div class="dashboard-shell">
      <aside class="sidebar">
        <a class="brand" href="/">LedgerFlow</a>
        <div class="user-card">
          <span class="eyebrow">Signed in</span>
          <strong>{}</strong>
          <small>Private beta pricing: {} ETH / month, currently free.</small>
        </div>
        <nav class="side-nav">
          <a class="{}" href="/app/clusters">Cluster management</a>
          <a class="{}" href="{}">Cluster analysis</a>
          <a class="{}" href="/app/cluster-graph">Cluster graph</a>
          <a class="{}" href="/app/tx-analysis">Single tx analysis</a>
          <a class="{}" href="/app/indexer/progress">Indexer progress</a>
          <a class="{}" href="/app/indexer/address-lookup">Indexer address lookup</a>
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
        <section class="dashboard-grid">
          <div class="main-column">
            {}
          </div>
          <div class="side-column">
            <section class="panel">
              <div class="panel-head"><h2>Your clusters</h2><span class="badge">{}</span></div>
              <div class="panel-body cluster-list">{}</div>
            </section>
            <section class="panel">
              <div class="panel-head"><h2>Pricing</h2><span class="badge">beta</span></div>
              <div class="panel-body">
                <div class="price-card">
                  <strong>{} ETH / month</strong>
                  <span>Private beta override: free for now.</span>
                </div>
              </div>
            </section>
          </div>
        </section>
      </main>
    </div>
    {}
  </body>
</html>"#,
        shared_styles(),
        escape_html(&user.email),
        DEFAULT_PRICE_ETH,
        active_class(tab, DashboardTab::Clusters),
        active_class(tab, DashboardTab::ClusterAnalysis),
        selected_cluster
            .map(|cluster| format!("/app/clusters/{}", cluster.cluster_id))
            .unwrap_or_else(|| "/app/clusters".to_owned()),
        active_class(tab, DashboardTab::ClusterGraph),
        active_class(tab, DashboardTab::TxAnalysis),
        active_class(tab, DashboardTab::IndexerProgress),
        active_class(tab, DashboardTab::IndexerLookup),
        escape_html(section_title(tab)),
        render_notice_block(notice),
        render_error_block(error),
        match tab {
            DashboardTab::Clusters => render_cluster_management_panel(state.default_chain_id),
            DashboardTab::ClusterAnalysis => cluster_analysis_panel,
            DashboardTab::TxAnalysis => tx_panel,
            DashboardTab::ClusterGraph => graph_panel,
            DashboardTab::IndexerProgress => indexer_progress_panel,
            DashboardTab::IndexerLookup => indexer_lookup_panel,
        },
        clusters.len(),
        cluster_cards,
        DEFAULT_PRICE_ETH,
        render_cluster_autorefresh_script(selected_cluster, start, end)
    )
}

fn render_cluster_management_panel(default_chain_id: u64) -> String {
    format!(
        r#"
<section class="panel">
  <div class="panel-head"><h2>Create cluster</h2><span class="badge">management</span></div>
  <div class="panel-body">
    <p class="panel-copy">Create a cluster for one operator, wallet family, or bot set. As soon as creation succeeds, LedgerFlow queues a historical backfill job for the separate worker process.</p>
    <form class="stack-form" method="post" action="/app/clusters/create">
      <label>Chain ID<input type="number" name="chain_id" value="{}" required></label>
      <label>Owner label<input type="text" name="owner_label" placeholder="ops_team" required></label>
      <label>Cluster name<input type="text" name="name" placeholder="primary_searcher" required></label>
      <label>Description<input type="text" name="description" placeholder="main bot + treasury EOAs"></label>
      <label>Addresses<textarea name="addresses" placeholder="0x111...,bot-1,bot&#10;0x222...,treasury,eoa" required></textarea></label>
      <button class="cta primary" type="submit">Create cluster</button>
    </form>
  </div>
</section>
"#,
        default_chain_id
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

fn render_recent_classified_transactions_panel(txs: &[ClassifiedTransaction]) -> String {
    if txs.is_empty() {
        return "<div class=\"empty-state\">No classified transactions yet. Once tracking or backfill analyzes transactions, they will appear here.</div>".to_owned();
    }
    format!(
        r#"<div class="table-shell">
  <table>
    <thead>
      <tr>
        <th>Tx</th>
        <th>Time</th>
        <th>Behavior</th>
        <th>Protocol</th>
        <th>Gas USD</th>
        <th>Estimated PnL USD</th>
      </tr>
    </thead>
    <tbody>
      {}
    </tbody>
  </table>
</div>"#,
        txs
            .iter()
            .map(|tx| {
                format!(
                    "<tr><td><code>{}</code></td><td>{}</td><td>{:?}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
                    escape_html(&truncate_middle(&tx.tx_hash, 8)),
                    tx.timestamp
                        .map(|value| value.format("%Y-%m-%d %H:%M:%S UTC").to_string())
                        .unwrap_or_else(|| "n/a".to_owned()),
                    tx.behavior_tag,
                    escape_html(tx.protocol.as_deref().unwrap_or("n/a")),
                    tx.gas_cost_usd
                        .map(|value| format!("{value:.2}"))
                        .unwrap_or_else(|| "n/a".to_owned()),
                    tx.estimated_pnl_usd
                        .map(|value| format!("{value:.2}"))
                        .unwrap_or_else(|| "n/a".to_owned())
                )
            })
            .collect::<Vec<_>>()
            .join("")
    )
}

fn render_indexer_progress_panel(progress: Option<&IndexerProgressPayload>) -> String {
    let body = progress
        .map(|progress| {
            format!(
                r#"<div class="stats-grid">
  <div class="stat"><span>Chain ID</span><strong>{}</strong></div>
  <div class="stat"><span>Worker ID</span><strong>{}</strong></div>
  <div class="stat"><span>Last indexed block</span><strong>{}</strong></div>
</div>
<div class="panel-copy">This portal keeps indexer and analysis separate. The indexer discovers address-level transaction references; the analysis stack consumes those references for cluster backfill, tx interpretation, and spend attribution.</div>
<div class="button-row">
  <a class="cta secondary" href="/app/indexer/address-lookup">Open address lookup</a>
  <a class="cta ghost" href="/app/clusters">Open clusters</a>
</div>"#,
                progress.chain_id,
                escape_html(&progress.worker_id),
                progress.last_indexed_block
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

fn derive_cluster_status(
    coverage: Option<&ClusterCoverage>,
    runs: &[ClusterBackfillRun],
) -> (String, String) {
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
            return ("Backfill queued".to_owned(), "info".to_owned());
        }
    }

    if let Some(run) = runs.first() {
        return match run.status {
            ClusterBackfillStatus::Pending | ClusterBackfillStatus::Running => {
                ("Backfill queued".to_owned(), "info".to_owned())
            }
            ClusterBackfillStatus::Partial => (
                "Analysis completed with failures".to_owned(),
                "warning".to_owned(),
            ),
            ClusterBackfillStatus::Completed => {
                ("History fully analyzed".to_owned(), "success".to_owned())
            }
            ClusterBackfillStatus::Failed => ("Backfill failed".to_owned(), "error".to_owned()),
        };
    }

    ("Waiting for discovery".to_owned(), "info".to_owned())
}

fn render_cluster_coverage_cards(coverage: Option<&ClusterCoverage>) -> String {
    coverage
        .map(|coverage| {
            format!(
                r#"<div class="stats-grid">
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
                escape_html(&summary.total_gas_cost_raw),
                escape_html(&summary.total_external_native_outflow_raw),
                escape_html(&summary.total_incoming_native_raw),
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

fn render_cluster_runs_table(runs: &[ClusterBackfillRun]) -> String {
    if runs.is_empty() {
        return "<div class=\"empty-state\">No backfill runs recorded yet.</div>".to_owned();
    }

    format!(
        r#"<div class="table-shell">
  <table>
    <thead>
      <tr>
        <th>Requested</th>
        <th>Status</th>
        <th>Range</th>
        <th>Discovered</th>
        <th>Analyzed</th>
        <th>Failed</th>
      </tr>
    </thead>
    <tbody>
      {}
    </tbody>
  </table>
</div>"#,
        runs.iter()
            .take(10)
            .map(|run| {
                format!(
                    "<tr><td>{}</td><td>{:?}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
                    run.requested_at.format("%Y-%m-%d %H:%M:%S UTC"),
                    run.status,
                    match (run.start_block, run.end_block) {
                        (Some(start), Some(end)) => format!("{start} - {end}"),
                        (Some(start), None) => format!("{start} - latest"),
                        (None, Some(end)) => format!("0 - {end}"),
                        (None, None) => "full history".to_owned(),
                    },
                    run.discovered_tx_count,
                    run.analyzed_tx_count,
                    run.failed_tx_count
                )
            })
            .collect::<Vec<_>>()
            .join("")
    )
}

fn render_cluster_autorefresh_script(
    selected_cluster: Option<&DashboardClusterRecord>,
    start: Option<&str>,
    end: Option<&str>,
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
  const statusNode = document.getElementById("cluster-live-status");
  const trackingNode = document.getElementById("cluster-live-tracking");
  const coverageNode = document.getElementById("cluster-live-coverage");
  const summaryNode = document.getElementById("cluster-live-summary");
  const runsNode = document.getElementById("cluster-live-runs");
  const classifiedNode = document.getElementById("cluster-live-classified");
  if (!clusterId || !statusNode || !trackingNode || !coverageNode || !summaryNode || !runsNode || !classifiedNode) return;

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

  const renderCoverage = (coverage) => {{
    if (!coverage) {{
      return '<div class="empty-state">Coverage is not available yet. The worker may still be discovering transactions.</div>';
    }}
    return `
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
        <div class="stat"><span>Gas spent</span><strong>${{escapeHtml(summary.total_gas_cost_raw)}}</strong></div>
        <div class="stat"><span>Native outflow</span><strong>${{escapeHtml(summary.total_external_native_outflow_raw)}}</strong></div>
        <div class="stat"><span>Incoming native</span><strong>${{escapeHtml(summary.total_incoming_native_raw)}}</strong></div>
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

  const renderRuns = (runs) => {{
    if (!runs || runs.length === 0) {{
      return '<div class="empty-state">No backfill runs recorded yet.</div>';
    }}
    const rows = runs.slice(0, 10).map((run) => {{
      let range = "full history";
      if (run.start_block != null && run.end_block != null) range = `${{run.start_block}} - ${{run.end_block}}`;
      else if (run.start_block != null) range = `${{run.start_block}} - latest`;
      else if (run.end_block != null) range = `0 - ${{run.end_block}}`;
      const requestedAt = escapeHtml(String(run.requested_at || "").replace("T", " ").replace("Z", " UTC"));
      return `
        <tr>
          <td>${{requestedAt}}</td>
          <td>${{escapeHtml(run.status)}}</td>
          <td>${{escapeHtml(range)}}</td>
          <td>${{run.discovered_tx_count}}</td>
          <td>${{run.analyzed_tx_count}}</td>
          <td>${{run.failed_tx_count}}</td>
        </tr>
      `;
    }}).join("");
    return `
      <div class="table-shell">
        <table>
          <thead>
            <tr>
              <th>Requested</th>
              <th>Status</th>
              <th>Range</th>
              <th>Discovered</th>
              <th>Analyzed</th>
              <th>Failed</th>
            </tr>
          </thead>
          <tbody>${{rows}}</tbody>
        </table>
      </div>
    `;
  }};

  const renderClassified = (txs) => {{
    if (!txs || txs.length === 0) {{
      return '<div class="empty-state">No classified transactions yet. Once tracking or backfill analyzes transactions, they will appear here.</div>';
    }}
    const rows = txs.map((tx) => `
      <tr>
        <td><code>${{escapeHtml(truncate(tx.tx_hash))}}</code></td>
        <td>${{escapeHtml(tx.timestamp ? String(tx.timestamp).replace("T", " ").replace("Z", " UTC") : "n/a")}}</td>
        <td>${{escapeHtml(tx.behavior_tag)}}</td>
        <td>${{escapeHtml(tx.protocol || "n/a")}}</td>
        <td>${{escapeHtml(tx.gas_cost_usd != null ? Number(tx.gas_cost_usd).toFixed(2) : "n/a")}}</td>
        <td>${{escapeHtml(tx.estimated_pnl_usd != null ? Number(tx.estimated_pnl_usd).toFixed(2) : "n/a")}}</td>
      </tr>
    `).join("");
    return `
      <div class="table-shell">
        <table>
          <thead>
            <tr>
              <th>Tx</th>
              <th>Time</th>
              <th>Behavior</th>
              <th>Protocol</th>
              <th>Gas USD</th>
              <th>Estimated PnL USD</th>
            </tr>
          </thead>
          <tbody>${{rows}}</tbody>
        </table>
      </div>
    `;
  }};

  const renderState = (payload) => {{
    statusNode.className = `notice ${{payload.status_tone || "info"}}`;
    statusNode.textContent = payload.status_label || "Waiting for discovery";
    trackingNode.innerHTML = renderTracking(payload.tracking);
    coverageNode.innerHTML = renderCoverage(payload.coverage);
    summaryNode.innerHTML = renderSummary(payload.spend_summary);
    runsNode.innerHTML = renderRuns(payload.runs || []);
    classifiedNode.innerHTML = renderClassified(payload.classified_transactions || []);
  }};

  const fetchState = async () => {{
    const url = new URL("/app/clusters/status", window.location.origin);
    url.searchParams.set("cluster_id", clusterId);
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
        end = end.unwrap_or_default()
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

fn truncate_middle(value: &str, keep: usize) -> String {
    if value.len() <= keep * 2 {
        return value.to_owned();
    }
    format!("{}...{}", &value[..keep], &value[value.len() - keep..])
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
  --bg: #f5f0e8;
  --panel: #fffdf8;
  --ink: #16120e;
  --muted: #6c6257;
  --line: #d8ccbc;
  --accent: #0d6e63;
  --accent-2: #c55f0c;
  --cream: #efe6d6;
  --shadow: 0 20px 40px rgba(34, 24, 14, 0.08);
}
* { box-sizing: border-box; }
body { margin: 0; background: radial-gradient(circle at top left, rgba(13,110,99,.12), transparent 28rem), linear-gradient(180deg, #f7f2ea 0%, var(--bg) 100%); color: var(--ink); font-family: "Iowan Old Style", Georgia, serif; }
a { color: inherit; text-decoration: none; }
.shell, .dashboard-shell { width: min(1240px, calc(100vw - 40px)); margin: 0 auto; }
.topbar { display: flex; justify-content: space-between; align-items: center; padding: 28px 0 16px; }
.brand { font-size: 1.4rem; font-weight: 700; }
.navlinks { display: flex; gap: 12px; align-items: center; }
.cta { display: inline-flex; align-items: center; justify-content: center; border-radius: 999px; border: 1px solid transparent; padding: 11px 16px; font-weight: 700; }
.cta.primary { background: var(--accent); color: white; }
.cta.secondary { background: var(--cream); }
.cta.ghost { background: transparent; border-color: var(--line); }
.hero { display: grid; grid-template-columns: 1.4fr 1fr; gap: 24px; padding: 28px 0 48px; align-items: center; }
.hero h1, .pricing-card h1, .auth-copy h1, .verify-shell h1, .dashboard-hero h1 { font-size: clamp(2.2rem, 4vw, 4.2rem); line-height: .98; margin: 0 0 14px; }
.hero p, .auth-copy p, .panel-copy, .dashboard-hero p { color: var(--muted); line-height: 1.6; }
.hero-card, .feature-card, .pricing-card, .auth-card, .panel, .cluster-card, .price-card { background: rgba(255,255,255,.8); border: 1px solid var(--line); border-radius: 22px; box-shadow: var(--shadow); }
.hero-card { padding: 18px; display: grid; gap: 12px; }
.metric-card { background: var(--panel); border-radius: 16px; padding: 16px; border: 1px solid var(--line); }
.metric-card span, .eyebrow { color: var(--accent); text-transform: uppercase; letter-spacing: .14em; font-size: .73rem; font-weight: 700; }
.metric-card strong { display: block; margin-top: 8px; line-height: 1.3; font-size: 1.03rem; }
.hero-actions, .button-row { display: flex; gap: 12px; flex-wrap: wrap; }
.feature-grid, .pricing-grid, .auth-grid { display: grid; gap: 18px; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); }
.feature-card, .pricing-card, .auth-card { padding: 22px; }
.pricing-banner { margin: 30px 0 48px; padding: 22px; border: 1px solid var(--line); border-radius: 24px; background: linear-gradient(135deg, rgba(13,110,99,.08), rgba(197,95,12,.08)); display: flex; justify-content: space-between; gap: 16px; align-items: center; }
.auth-shell, .verify-shell { display: grid; gap: 24px; padding: 40px 0; }
.auth-card.narrow { max-width: 420px; }
.mode-hint, .price-note { margin-top: 16px; color: var(--muted); }
.flat-list { padding-left: 18px; color: var(--muted); line-height: 1.7; }
.notice { margin: 12px 0; padding: 12px 14px; border-radius: 14px; font-size: .95rem; }
.notice.info { background: rgba(13,110,99,.08); color: var(--accent); }
.notice.success { background: rgba(13,110,99,.1); color: var(--accent); }
.notice.warning { background: rgba(197,95,12,.12); color: var(--accent-2); }
.notice.error { background: rgba(197,95,12,.1); color: var(--accent-2); }
label { display: grid; gap: 8px; font-size: .95rem; }
input, textarea, select { width: 100%; padding: 12px 14px; border: 1px solid var(--line); border-radius: 14px; background: rgba(255,255,255,.92); font: inherit; }
textarea { min-height: 140px; resize: vertical; }
.dashboard-body { background: #f2ede5; }
.dashboard-shell { display: grid; grid-template-columns: 280px 1fr; gap: 24px; padding: 24px 0; }
.sidebar { position: sticky; top: 18px; height: fit-content; display: grid; gap: 16px; }
.user-card { padding: 18px; border: 1px solid var(--line); border-radius: 20px; background: rgba(255,255,255,.72); }
.side-nav { display: grid; gap: 8px; }
.side-nav a { padding: 12px 14px; border-radius: 14px; border: 1px solid transparent; color: var(--muted); }
.side-nav a.active { background: var(--panel); border-color: var(--line); color: var(--ink); }
.dashboard-main { display: grid; gap: 20px; }
.dashboard-hero { padding: 8px 0 0; }
.dashboard-grid { display: grid; grid-template-columns: 1.45fr .95fr; gap: 20px; }
.main-column, .side-column { display: grid; gap: 20px; }
.panel-head { display: flex; justify-content: space-between; align-items: center; gap: 12px; padding: 18px 20px 8px; }
.panel-body { padding: 0 20px 20px; }
.badge { padding: 6px 10px; border-radius: 999px; background: var(--cream); color: var(--muted); font-size: .82rem; }
.stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); gap: 12px; margin: 14px 0 20px; }
.stat { border: 1px solid var(--line); border-radius: 16px; background: rgba(255,255,255,.78); padding: 14px; }
.stat span { display: block; color: var(--muted); font-size: .82rem; text-transform: uppercase; letter-spacing: .08em; }
.stat strong { display: block; margin-top: 8px; font-size: 1.1rem; }
.stack-form, .inline-form { display: grid; gap: 12px; }
.split-fields { display: grid; gap: 12px; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); }
.inline-form { grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); align-items: end; margin-bottom: 12px; }
.cluster-list { display: grid; gap: 12px; }
.cluster-card { display: grid; gap: 6px; padding: 16px; }
.cluster-card span, .cluster-card small { color: var(--muted); }
.json-shell { margin: 0; padding: 16px; border-radius: 16px; background: #14110d; color: #f7f2ea; overflow: auto; font-size: 12px; line-height: 1.55; }
.table-shell { overflow: auto; }
table { width: 100%; border-collapse: collapse; }
th, td { text-align: left; padding: 10px 8px; border-bottom: 1px solid var(--line); vertical-align: top; }
.empty-state { padding: 18px; border: 1px dashed var(--line); border-radius: 16px; color: var(--muted); background: rgba(255,255,255,.5); }
@media (max-width: 960px) {
  .hero, .dashboard-shell, .dashboard-grid { grid-template-columns: 1fr; }
  .sidebar { position: static; }
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
