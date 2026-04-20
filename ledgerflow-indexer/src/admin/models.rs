use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Serialize)]
pub struct User {
    pub user_id: Uuid,
    pub email: String,
    pub email_verified: bool,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
pub struct ApiKeyCreated {
    pub key_id: Uuid,
    pub key_prefix: String,
    pub api_key: String,
}

#[derive(Debug, Serialize)]
pub struct Plan {
    pub plan_id: String,
    pub name: String,
    pub monthly_price_wei: String,
    pub monthly_request_quota: i64,
}

#[derive(Debug, Deserialize)]
pub struct StartEmailRequest {
    pub email: String,
}

#[derive(Debug, Deserialize)]
pub struct VerifyEmailRequest {
    pub email: String,
    pub code: String,
}

#[derive(Debug, Deserialize)]
pub struct CreateApiKeyRequest {
    pub email: String,
    pub name: String,
}

#[derive(Debug, Deserialize)]
pub struct SubmitBaseEthPaymentRequest {
    pub email: String,
    pub plan_id: String,
    pub tx_hash: String,
    pub from_address: Option<String>,
}
