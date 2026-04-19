use std::sync::Arc;

use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::{Html, IntoResponse},
    routing::{get, post},
};
use serde_json::json;
use uuid::Uuid;

use crate::admin::billing::BaseEthPaymentVerifier;
use crate::admin::email::PostmarkEmailSender;
use crate::admin::models::{
    CreateApiKeyRequest, StartEmailRequest, SubmitBaseEthPaymentRequest, VerifyEmailRequest,
};
use crate::admin::store::PostgresAdminStore;
use crate::core::errors::LedgerError;

#[derive(Clone)]
pub struct AdminState {
    pub store: Arc<PostgresAdminStore>,
    pub payment_verifier: Arc<BaseEthPaymentVerifier>,
    pub postmark_server_token: Option<String>,
    pub email_from: String,
    pub billing_treasury_address: String,
    pub billing_min_confirmations: u64,
}

pub fn router(state: AdminState) -> Router {
    Router::new()
        .route("/admin", get(admin_console))
        .route("/admin/health", get(admin_health))
        .route("/admin/auth/email/start", post(start_email_auth))
        .route("/admin/auth/email/verify", post(verify_email_auth))
        .route("/admin/api-keys", post(create_api_key))
        .route(
            "/admin/billing/base-eth/payments",
            post(submit_base_eth_payment),
        )
        .with_state(state)
}

async fn admin_console() -> Html<&'static str> {
    Html(
        r#"<!doctype html>
<html>
<head>
  <title>LedgerFlow Indexer Admin</title>
  <style>
    body { font-family: ui-sans-serif, system-ui; margin: 40px; max-width: 920px; }
    code, pre { background: #f4f1ea; padding: 2px 4px; border-radius: 4px; }
    section { border: 1px solid #ddd2c0; border-radius: 12px; padding: 18px; margin: 16px 0; }
  </style>
</head>
<body>
  <h1>LedgerFlow Indexer Admin</h1>
  <p>This is the first admin shell. Use the JSON API for email verification, API keys, and Base ETH payment submission.</p>
  <section>
    <h2>Core endpoints</h2>
    <pre>POST /admin/auth/email/start
POST /admin/auth/email/verify
POST /admin/api-keys
POST /admin/billing/base-eth/payments</pre>
  </section>
  <section>
    <h2>Public API</h2>
    <pre>GET /v1/chains/base/progress
GET /v1/base/address/{address}/txs
GET /v1/base/address/{address}/tx-hashes
POST /v1/base/addresses/txs</pre>
  </section>
</body>
</html>"#,
    )
}

async fn admin_health() -> Json<serde_json::Value> {
    Json(json!({ "status": "ok", "service": "ledgerflow-indexer-admin" }))
}

async fn start_email_auth(
    State(state): State<AdminState>,
    Json(request): Json<StartEmailRequest>,
) -> Result<Json<serde_json::Value>, AdminApiError> {
    let code = short_code();
    state.store.create_email_otp(&request.email, &code).await?;
    let sender = PostmarkEmailSender::new(
        state.postmark_server_token.clone(),
        state.email_from.clone(),
    );
    let delivered = sender.send_verification_code(&request.email, &code).await?;
    Ok(Json(json!({
        "status": "pending_verification",
        "email": request.email,
        "email_delivery": if delivered { "postmark" } else { "disabled" },
        "dev_code_hint": if delivered { serde_json::Value::Null } else { json!(code) },
    })))
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

async fn create_api_key(
    State(state): State<AdminState>,
    Json(request): Json<CreateApiKeyRequest>,
) -> Result<Json<serde_json::Value>, AdminApiError> {
    let api_key = state
        .store
        .create_api_key(&request.email, &request.name)
        .await?;
    Ok(Json(json!({
        "status": "created",
        "api_key": api_key,
        "warning": "store the api_key now; only its hash is persisted"
    })))
}

async fn submit_base_eth_payment(
    State(state): State<AdminState>,
    Json(request): Json<SubmitBaseEthPaymentRequest>,
) -> Result<Json<serde_json::Value>, AdminApiError> {
    let minimum_amount_wei = state.store.get_plan_price_wei(&request.plan_id).await?;
    let (payment_id, user_id) = state
        .store
        .record_submitted_payment(
            &request.email,
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
