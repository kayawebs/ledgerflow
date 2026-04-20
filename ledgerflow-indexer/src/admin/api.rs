use std::sync::Arc;

use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode, header},
    response::{Html, IntoResponse, Redirect, Response},
    routing::{get, post},
};
use serde_json::json;
use uuid::Uuid;

use crate::admin::billing::BaseEthPaymentVerifier;
use crate::admin::email::PostmarkEmailSender;
use crate::admin::models::{
    CreateApiKeyRequest, PasswordLoginRequest, PasswordResetStartRequest,
    PasswordResetVerifyRequest, RegisterStartRequest, RegisterVerifyRequest, StartEmailRequest,
    SubmitBaseEthPaymentRequest, VerifyEmailRequest,
};
use crate::admin::pages;
use crate::admin::store::PostgresAdminStore;
use crate::core::errors::LedgerError;
use crate::indexer::service::IndexerService;

#[derive(Clone)]
pub struct AdminState {
    pub store: Arc<PostgresAdminStore>,
    pub indexer_service: Arc<IndexerService>,
    pub payment_verifier: Arc<BaseEthPaymentVerifier>,
    pub postmark_server_token: Option<String>,
    pub email_from: String,
    pub billing_treasury_address: String,
    pub billing_min_confirmations: u64,
}

pub fn router(state: AdminState) -> Router {
    Router::new()
        .route("/", get(home_page))
        .route("/login", get(login_page))
        .route("/pricing", get(pricing_page))
        .route("/subscribe", get(subscribe_page))
        .route("/referrals", get(referrals_page))
        .route("/dashboard", get(dashboard_page))
        .route("/docs", get(docs_page))
        .route("/admin", get(dashboard_page))
        .route("/admin/health", get(admin_health))
        .route("/admin/config", get(admin_config))
        .route("/admin/progress", get(admin_progress))
        .route("/admin/billing/plans", get(list_billing_plans))
        .route("/admin/account/summary", get(account_summary))
        .route("/admin/usage/summary", get(usage_summary))
        .route("/admin/referral", get(referral_summary))
        .route("/admin/auth/me", get(auth_me))
        .route("/admin/auth/register/start", post(register_start))
        .route("/admin/auth/register/verify", post(register_verify))
        .route("/admin/auth/login", post(password_login))
        .route("/admin/auth/logout", post(logout))
        .route(
            "/admin/auth/password-reset/start",
            post(password_reset_start),
        )
        .route(
            "/admin/auth/password-reset/verify",
            post(password_reset_verify),
        )
        .route("/admin/auth/email/start", post(start_email_auth))
        .route("/admin/auth/email/verify", post(verify_email_auth))
        .route("/admin/api-keys", post(create_api_key))
        .route(
            "/admin/billing/base-eth/payments",
            post(submit_base_eth_payment),
        )
        .with_state(state)
}

async fn home_page() -> Html<&'static str> {
    Html(pages::home())
}

async fn login_page() -> Html<&'static str> {
    Html(pages::login())
}

async fn pricing_page() -> Html<&'static str> {
    Html(pages::pricing())
}

async fn subscribe_page(
    State(state): State<AdminState>,
    headers: HeaderMap,
) -> Result<Response, AdminApiError> {
    if current_user(&state, &headers).await?.is_some() {
        return Ok(Html(pages::subscribe()).into_response());
    }
    Ok(Redirect::to("/login").into_response())
}

async fn referrals_page(
    State(state): State<AdminState>,
    headers: HeaderMap,
) -> Result<Response, AdminApiError> {
    if current_user(&state, &headers).await?.is_some() {
        return Ok(Html(pages::referrals()).into_response());
    }
    Ok(Redirect::to("/login").into_response())
}

async fn dashboard_page(
    State(state): State<AdminState>,
    headers: HeaderMap,
) -> Result<Response, AdminApiError> {
    if current_user(&state, &headers).await?.is_some() {
        return Ok(Html(pages::dashboard()).into_response());
    }
    Ok(Redirect::to("/login").into_response())
}

async fn docs_page() -> Html<&'static str> {
    Html(pages::docs())
}

async fn admin_health() -> Json<serde_json::Value> {
    Json(json!({ "status": "ok", "service": "ledgerflow-indexer-admin" }))
}

async fn admin_config(State(state): State<AdminState>) -> Json<serde_json::Value> {
    Json(json!({
        "billing": {
            "treasury_address": state.billing_treasury_address,
            "required_confirmations": state.billing_min_confirmations
        }
    }))
}

async fn list_billing_plans(
    State(state): State<AdminState>,
) -> Result<Json<serde_json::Value>, AdminApiError> {
    let plans = state.store.list_plans().await?;
    Ok(Json(json!({ "plans": plans })))
}

async fn admin_progress(
    State(state): State<AdminState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, AdminApiError> {
    require_user(&state, &headers).await?;
    let progress = state.indexer_service.get_progress().await?;
    Ok(Json(json!({
        "chain": "base",
        "chain_id": state.indexer_service.config.chain_id,
        "worker_id": state.indexer_service.config.worker_id,
        "last_indexed_block": progress
    })))
}

async fn account_summary(
    State(state): State<AdminState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, AdminApiError> {
    let user = require_user(&state, &headers).await?;
    let summary = state.store.get_account_usage_summary(user.user_id).await?;
    Ok(Json(json!({ "summary": summary })))
}

async fn usage_summary(
    State(state): State<AdminState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, AdminApiError> {
    let user = require_user(&state, &headers).await?;
    let stats = state.store.get_usage_endpoint_stats(user.user_id).await?;
    let failures = state.store.get_recent_failures(user.user_id, 20).await?;
    Ok(Json(json!({
        "stats": stats,
        "recent_failures": failures
    })))
}

async fn auth_me(
    State(state): State<AdminState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, AdminApiError> {
    let user = require_user(&state, &headers).await?;
    Ok(Json(json!({ "user": user })))
}

async fn referral_summary(
    State(state): State<AdminState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, AdminApiError> {
    let user = require_user(&state, &headers).await?;
    let referral = state.store.get_referral_summary(user.user_id).await?;
    Ok(Json(json!({ "referral": referral })))
}

async fn register_start(
    State(state): State<AdminState>,
    Json(request): Json<RegisterStartRequest>,
) -> Result<Json<serde_json::Value>, AdminApiError> {
    send_code(&state, &request.email, "verify_email").await
}

async fn register_verify(
    State(state): State<AdminState>,
    Json(request): Json<RegisterVerifyRequest>,
) -> Result<Response, AdminApiError> {
    state
        .store
        .verify_email_otp_for_purpose(&request.email, &request.code, "verify_email")
        .await?;
    let user = state
        .store
        .set_user_password_with_referral(
            &request.email,
            &request.password,
            request.referral_code.as_deref(),
        )
        .await?;
    let token = state.store.create_session(user.user_id).await?;
    Ok((
        [(header::SET_COOKIE, session_cookie(&token))],
        Json(json!({ "status": "registered", "user": user })),
    )
        .into_response())
}

async fn password_login(
    State(state): State<AdminState>,
    Json(request): Json<PasswordLoginRequest>,
) -> Result<Response, AdminApiError> {
    let user = state
        .store
        .authenticate_password(&request.email, &request.password)
        .await?;
    let token = state.store.create_session(user.user_id).await?;
    Ok((
        [(header::SET_COOKIE, session_cookie(&token))],
        Json(json!({ "status": "logged_in", "user": user })),
    )
        .into_response())
}

async fn logout(
    State(state): State<AdminState>,
    headers: HeaderMap,
) -> Result<Response, AdminApiError> {
    if let Some(token) = session_token(&headers) {
        state.store.revoke_session(token).await?;
    }
    Ok((
        [(header::SET_COOKIE, expired_session_cookie())],
        Json(json!({ "status": "logged_out" })),
    )
        .into_response())
}

async fn password_reset_start(
    State(state): State<AdminState>,
    Json(request): Json<PasswordResetStartRequest>,
) -> Result<Json<serde_json::Value>, AdminApiError> {
    send_code(&state, &request.email, "password_reset").await
}

async fn password_reset_verify(
    State(state): State<AdminState>,
    Json(request): Json<PasswordResetVerifyRequest>,
) -> Result<Response, AdminApiError> {
    state
        .store
        .verify_email_otp_for_purpose(&request.email, &request.code, "password_reset")
        .await?;
    let user = state
        .store
        .set_user_password(&request.email, &request.new_password)
        .await?;
    let token = state.store.create_session(user.user_id).await?;
    Ok((
        [(header::SET_COOKIE, session_cookie(&token))],
        Json(json!({ "status": "password_reset", "user": user })),
    )
        .into_response())
}

async fn start_email_auth(
    State(state): State<AdminState>,
    Json(request): Json<StartEmailRequest>,
) -> Result<Json<serde_json::Value>, AdminApiError> {
    send_code(&state, &request.email, "verify_email").await
}

async fn verify_email_auth(
    State(state): State<AdminState>,
    Json(request): Json<VerifyEmailRequest>,
) -> Result<Json<serde_json::Value>, AdminApiError> {
    let user = state
        .store
        .verify_email_otp(&request.email, &request.code)
        .await?;
    Ok(Json(json!({ "status": "verified", "user": user })))
}

async fn send_code(
    state: &AdminState,
    email: &str,
    purpose: &str,
) -> Result<Json<serde_json::Value>, AdminApiError> {
    let code = short_code();
    state
        .store
        .create_email_otp_for_purpose(email, &code, purpose)
        .await?;
    let sender = PostmarkEmailSender::new(
        state.postmark_server_token.clone(),
        state.email_from.clone(),
    );
    let delivered = sender.send_verification_code(email, &code).await?;
    Ok(Json(json!({
        "status": "pending_verification",
        "email": email,
        "purpose": purpose,
        "resend_after_seconds": 60,
        "email_delivery": if delivered { "postmark" } else { "disabled" },
        "dev_code_hint": if delivered { serde_json::Value::Null } else { json!(code) },
    })))
}

fn session_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::COOKIE)?
        .to_str()
        .ok()?
        .split(';')
        .map(str::trim)
        .find_map(|part| part.strip_prefix("ledgerflow_session="))
        .filter(|value| !value.is_empty())
}

async fn current_user(
    state: &AdminState,
    headers: &HeaderMap,
) -> Result<Option<crate::admin::models::User>, AdminApiError> {
    let Some(token) = session_token(headers) else {
        return Ok(None);
    };
    state
        .store
        .get_user_by_session(token)
        .await
        .map_err(AdminApiError::from)
}

async fn require_user(
    state: &AdminState,
    headers: &HeaderMap,
) -> Result<crate::admin::models::User, AdminApiError> {
    current_user(state, headers)
        .await?
        .ok_or_else(|| LedgerError::Unauthorized("not logged in".to_owned()).into())
}

fn session_cookie(token: &str) -> String {
    format!("ledgerflow_session={token}; Path=/; Max-Age=2592000; HttpOnly; SameSite=Lax")
}

fn expired_session_cookie() -> String {
    "ledgerflow_session=; Path=/; Max-Age=0; HttpOnly; SameSite=Lax".to_owned()
}

async fn create_api_key(
    State(state): State<AdminState>,
    headers: HeaderMap,
    Json(request): Json<CreateApiKeyRequest>,
) -> Result<Json<serde_json::Value>, AdminApiError> {
    let user = require_user(&state, &headers).await?;
    let api_key = state
        .store
        .create_api_key(&user.email, &request.name)
        .await?;
    Ok(Json(json!({
        "status": "created",
        "api_key": api_key,
        "warning": "store the api_key now; only its hash is persisted"
    })))
}

async fn submit_base_eth_payment(
    State(state): State<AdminState>,
    headers: HeaderMap,
    Json(request): Json<SubmitBaseEthPaymentRequest>,
) -> Result<Json<serde_json::Value>, AdminApiError> {
    let user = require_user(&state, &headers).await?;
    let minimum_amount_wei = state.store.get_plan_price_wei(&request.plan_id).await?;
    let (payment_id, user_id) = state
        .store
        .record_submitted_payment(
            &user.email,
            &request.plan_id,
            &request.tx_hash,
            request.from_address.as_deref(),
        )
        .await?;
    let verified = state
        .payment_verifier
        .verify(
            &request.tx_hash,
            request.from_address.as_deref(),
            minimum_amount_wei,
        )
        .await?;
    let subscription_id = state
        .store
        .mark_payment_verified_and_activate_subscription(
            user_id,
            payment_id,
            &request.plan_id,
            &verified,
        )
        .await?;
    Ok(Json(json!({
        "status": "verified",
        "payment_id": payment_id,
        "subscription_id": subscription_id,
        "chain_id": 8453,
        "treasury_address": state.billing_treasury_address,
        "required_confirmations": state.billing_min_confirmations,
        "payment": verified
    })))
}

fn short_code() -> String {
    Uuid::new_v4()
        .simple()
        .to_string()
        .chars()
        .take(6)
        .collect::<String>()
}

pub struct AdminApiError {
    status: StatusCode,
    message: String,
}

impl From<LedgerError> for AdminApiError {
    fn from(value: LedgerError) -> Self {
        match value {
            LedgerError::Conflict(message) | LedgerError::MalformedEvidence(message) => Self {
                status: StatusCode::CONFLICT,
                message,
            },
            LedgerError::Unauthorized(message) => Self {
                status: StatusCode::UNAUTHORIZED,
                message,
            },
            LedgerError::PaymentRequired(message) => Self {
                status: StatusCode::PAYMENT_REQUIRED,
                message,
            },
            LedgerError::TransientIngestion(message) | LedgerError::Persistence(message) => Self {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                message,
            },
        }
    }
}

impl IntoResponse for AdminApiError {
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
