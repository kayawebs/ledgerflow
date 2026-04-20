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

#[derive(Debug, Serialize)]
pub struct AccountUsageSummary {
    pub has_active_subscription: bool,
    pub plan_id: Option<String>,
    pub current_period_end: Option<DateTime<Utc>>,
    pub subscription_quota: i64,
    pub subscription_used_this_month: i64,
    pub subscription_remaining_this_month: i64,
    pub signup_bonus_requests: i64,
    pub referral_bonus_requests_this_month: i64,
    pub bonus_used_this_month: i64,
    pub bonus_remaining_this_month: i64,
}

#[derive(Debug, Serialize)]
pub struct UsageEndpointStat {
    pub endpoint: String,
    pub usage_source: String,
    pub outcome: String,
    pub request_count: i64,
}

#[derive(Debug, Serialize)]
pub struct ApiFailureDetail {
    pub endpoint: String,
    pub status_code: i32,
    pub error: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
pub struct ReferralSummary {
    pub referral_code: String,
    pub reward_per_verified_referral: i64,
    pub monthly_reward_cap: i64,
    pub rewarded_referrals_this_month: i64,
    pub bonus_requests_this_month: i64,
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
pub struct RegisterStartRequest {
    pub email: String,
}

#[derive(Debug, Deserialize)]
pub struct RegisterVerifyRequest {
    pub email: String,
    pub code: String,
    pub password: String,
    pub referral_code: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct PasswordLoginRequest {
    pub email: String,
    pub password: String,
}

#[derive(Debug, Deserialize)]
pub struct PasswordResetStartRequest {
    pub email: String,
}

#[derive(Debug, Deserialize)]
pub struct PasswordResetVerifyRequest {
    pub email: String,
    pub code: String,
    pub new_password: String,
}

#[derive(Debug, Deserialize)]
pub struct CreateApiKeyRequest {
    pub name: String,
}

#[derive(Debug, Deserialize)]
pub struct SubmitBaseEthPaymentRequest {
    pub plan_id: String,
    pub tx_hash: String,
    pub from_address: Option<String>,
}
