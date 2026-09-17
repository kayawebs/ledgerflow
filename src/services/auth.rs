use std::sync::Arc;

use argon2::{Argon2, PasswordHash, PasswordHasher, PasswordVerifier, password_hash::SaltString};
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use reqwest::Client;
use serde_json::json;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};
use tracing::info;
use urlencoding::encode;
use uuid::Uuid;

const MIN_PASSWORD_CHARACTERS: usize = 8;
const MAX_PASSWORD_BYTES: usize = 128;

use crate::{
    core::errors::{LedgerError, LedgerResult},
    domain::{entities::AppUser, enums::VerificationPurpose},
};

#[derive(Clone)]
pub struct AuthService {
    pool: PgPool,
    sender: Arc<dyn VerificationSender>,
    web_base_url: String,
    session_ttl: Duration,
    verification_ttl: Duration,
}

impl AuthService {
    pub fn new(
        pool: PgPool,
        sender: Arc<dyn VerificationSender>,
        web_base_url: String,
        session_ttl_days: i64,
        verification_ttl_minutes: i64,
    ) -> Self {
        Self {
            pool,
            sender,
            web_base_url,
            session_ttl: Duration::days(session_ttl_days.max(1)),
            verification_ttl: Duration::minutes(verification_ttl_minutes.max(1)),
        }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn request_code(
        &self,
        email_raw: &str,
        purpose: VerificationPurpose,
        signup_password: Option<&str>,
        requested_ip: Option<&str>,
        requested_user_agent: Option<&str>,
    ) -> LedgerResult<String> {
        let email = normalize_email(email_raw)?;
        let existing_user = self.find_user_by_email(&email).await?;
        match purpose {
            VerificationPurpose::Signup if existing_user.is_some() => {
                return Err(LedgerError::AnalysisInvariant(
                    "account already exists; use sign in".to_owned(),
                ));
            }
            VerificationPurpose::Login if existing_user.is_none() => {
                return Err(LedgerError::NotFound(
                    "account not found; create one first".to_owned(),
                ));
            }
            _ => {}
        }

        let pending_password_hash = match purpose {
            VerificationPurpose::Signup => {
                let password = signup_password.ok_or_else(|| {
                    LedgerError::AnalysisInvariant(
                        "a password is required to create an account".to_owned(),
                    )
                })?;
                Some(hash_password(password).await?)
            }
            VerificationPurpose::Login => None,
        };

        let code = generate_verification_code();
        let code_hash = hash_verification_code(&email, &purpose, &code);
        let verify_url = format!(
            "{}/auth?mode={}&stage=verify&email={}&purpose={}",
            self.web_base_url.trim_end_matches('/'),
            encode(verification_purpose_to_str(&purpose)),
            encode(&email),
            encode(verification_purpose_to_str(&purpose))
        );
        let expires_at = Utc::now() + self.verification_ttl;

        sqlx::query(
            r#"
            update email_verification_codes
            set consumed_at = now(),
                pending_password_hash = null
            where email = $1
              and purpose = $2
              and consumed_at is null
              and expires_at > now()
            "#,
        )
        .bind(&email)
        .bind(verification_purpose_to_str(&purpose))
        .execute(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;

        sqlx::query(
            r#"
            insert into email_verification_codes (
              code_id, email, purpose, code_hash, pending_password_hash,
              requested_ip, requested_user_agent, expires_at
            )
            values ($1,$2,$3,$4,$5,$6,$7,$8)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(&email)
        .bind(verification_purpose_to_str(&purpose))
        .bind(&code_hash)
        .bind(&pending_password_hash)
        .bind(requested_ip)
        .bind(requested_user_agent)
        .bind(expires_at)
        .execute(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;

        self.sender
            .send_verification_code(&email, &code, purpose.clone(), &verify_url)
            .await?;
        info!(email = %email, purpose = %verification_purpose_to_str(&purpose), "verification code requested");
        Ok(email)
    }

    pub async fn verify_code(
        &self,
        email_raw: &str,
        purpose: VerificationPurpose,
        code: &str,
        user_agent: Option<&str>,
    ) -> LedgerResult<(AppUser, String)> {
        let email = normalize_email(email_raw)?;
        let code_hash = hash_verification_code(&email, &purpose, code);

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|err| LedgerError::Persistence(err.to_string()))?;

        let code_row = sqlx::query(
            r#"
            select code_id, expires_at, consumed_at, pending_password_hash
            from email_verification_codes
            where email = $1 and purpose = $2 and code_hash = $3
            order by created_at desc
            limit 1
            "#,
        )
        .bind(&email)
        .bind(verification_purpose_to_str(&purpose))
        .bind(&code_hash)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;

        let Some(code_row) = code_row else {
            return Err(LedgerError::AnalysisInvariant(
                "invalid or expired verification code".to_owned(),
            ));
        };
        let expires_at: DateTime<Utc> = code_row
            .try_get("expires_at")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        let consumed_at: Option<DateTime<Utc>> = code_row
            .try_get("consumed_at")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        if consumed_at.is_some() || expires_at <= Utc::now() {
            return Err(LedgerError::AnalysisInvariant(
                "invalid or expired verification code".to_owned(),
            ));
        }

        let user = match purpose {
            VerificationPurpose::Signup => {
                let password_hash: Option<String> = code_row
                    .try_get("pending_password_hash")
                    .map_err(|err| LedgerError::Persistence(err.to_string()))?;
                let Some(password_hash) = password_hash else {
                    return Err(LedgerError::AnalysisInvariant(
                        "registration has expired; start again to set your password".to_owned(),
                    ));
                };
                let user_id = Uuid::new_v4();
                let row = sqlx::query(
                    r#"
                    insert into app_users (user_id, email, email_verified_at, password_hash)
                    values ($1,$2,now(),$3)
                    on conflict (email) do update set
                      email_verified_at = coalesce(app_users.email_verified_at, now()),
                      password_hash = coalesce(app_users.password_hash, excluded.password_hash),
                      updated_at = now()
                    returning user_id, email, workspace_plan, email_verified_at, created_at
                    "#,
                )
                .bind(user_id)
                .bind(&email)
                .bind(password_hash)
                .fetch_one(&mut *tx)
                .await
                .map_err(|err| LedgerError::Persistence(err.to_string()))?;
                row_to_app_user(row)?
            }
            VerificationPurpose::Login => {
                let row = sqlx::query(
                    r#"
                    update app_users
                    set email_verified_at = coalesce(email_verified_at, now()),
                        updated_at = now()
                    where email = $1
                    returning user_id, email, workspace_plan, email_verified_at, created_at
                    "#,
                )
                .bind(&email)
                .fetch_optional(&mut *tx)
                .await
                .map_err(|err| LedgerError::Persistence(err.to_string()))?;
                let Some(row) = row else {
                    return Err(LedgerError::NotFound(
                        "account not found; create one first".to_owned(),
                    ));
                };
                row_to_app_user(row)?
            }
        };

        sqlx::query(
            r#"
            update email_verification_codes
            set consumed_at = now(),
                pending_password_hash = null
            where code_id = $1
            "#,
        )
        .bind(
            code_row
                .try_get::<Uuid, _>("code_id")
                .map_err(|err| LedgerError::Persistence(err.to_string()))?,
        )
        .execute(&mut *tx)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;

        let session_token = format!("{}.{}", Uuid::new_v4(), Uuid::new_v4());
        let session_hash = hash_session_token(&session_token);
        let now = Utc::now();
        let expires_at = now + self.session_ttl;
        sqlx::query(
            r#"
            insert into app_user_sessions (
              session_id, user_id, token_hash, user_agent, created_at, last_seen_at, expires_at
            )
            values ($1,$2,$3,$4,$5,$6,$7)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(user.user_id)
        .bind(session_hash)
        .bind(user_agent)
        .bind(now)
        .bind(now)
        .bind(expires_at)
        .execute(&mut *tx)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;

        tx.commit()
            .await
            .map_err(|err| LedgerError::Persistence(err.to_string()))?;

        Ok((user, session_token))
    }

    pub async fn login_with_password(
        &self,
        email_raw: &str,
        password: &str,
        user_agent: Option<&str>,
    ) -> LedgerResult<(AppUser, String)> {
        let email = normalize_email(email_raw)?;
        let row = sqlx::query(
            r#"
            select user_id, email, workspace_plan, email_verified_at, created_at, password_hash
            from app_users
            where email = $1 and email_verified_at is not null
            limit 1
            "#,
        )
        .bind(&email)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;

        let password_hash = match row.as_ref() {
            Some(row) => row
                .try_get::<Option<String>, _>("password_hash")
                .map_err(|err| LedgerError::Persistence(err.to_string()))?,
            None => None,
        };
        let password_is_valid = verify_password(password, password_hash.as_deref()).await?;
        if row.is_none() || !password_is_valid {
            return Err(LedgerError::AnalysisInvariant(
                "invalid email or password".to_owned(),
            ));
        }

        let user = row_to_app_user(row.expect("checked above"))?;
        let session_token = self.create_session(user.user_id, user_agent).await?;
        Ok((user, session_token))
    }

    pub async fn get_user_by_session_token(
        &self,
        session_token: &str,
    ) -> LedgerResult<Option<AppUser>> {
        let token_hash = hash_session_token(session_token);
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        let row = sqlx::query(
            r#"
            select users.user_id, users.email, users.workspace_plan,
                   users.email_verified_at, users.created_at, sessions.session_id
            from app_user_sessions sessions
            join app_users users on users.user_id = sessions.user_id
            where sessions.token_hash = $1
              and sessions.revoked_at is null
              and sessions.expires_at > now()
            limit 1
            "#,
        )
        .bind(token_hash)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;

        let Some(row) = row else {
            tx.commit()
                .await
                .map_err(|err| LedgerError::Persistence(err.to_string()))?;
            return Ok(None);
        };

        let session_id: Uuid = row
            .try_get("session_id")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        sqlx::query("update app_user_sessions set last_seen_at = now() where session_id = $1")
            .bind(session_id)
            .execute(&mut *tx)
            .await
            .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        tx.commit()
            .await
            .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        Ok(Some(row_to_app_user(row)?))
    }

    pub async fn revoke_session(&self, session_token: &str) -> LedgerResult<()> {
        let token_hash = hash_session_token(session_token);
        sqlx::query(
            "update app_user_sessions set revoked_at = now() where token_hash = $1 and revoked_at is null",
        )
        .bind(token_hash)
        .execute(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        Ok(())
    }

    async fn create_session(
        &self,
        user_id: Uuid,
        user_agent: Option<&str>,
    ) -> LedgerResult<String> {
        let session_token = format!("{}.{}", Uuid::new_v4(), Uuid::new_v4());
        let session_hash = hash_session_token(&session_token);
        let now = Utc::now();
        let expires_at = now + self.session_ttl;
        sqlx::query(
            r#"
            insert into app_user_sessions (
              session_id, user_id, token_hash, user_agent, created_at, last_seen_at, expires_at
            )
            values ($1,$2,$3,$4,$5,$6,$7)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(user_id)
        .bind(session_hash)
        .bind(user_agent)
        .bind(now)
        .bind(now)
        .bind(expires_at)
        .execute(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        Ok(session_token)
    }

    async fn find_user_by_email(&self, email: &str) -> LedgerResult<Option<AppUser>> {
        let row = sqlx::query(
            "select user_id, email, workspace_plan, email_verified_at, created_at from app_users where email = $1",
        )
        .bind(email)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        row.map(row_to_app_user).transpose()
    }
}

#[async_trait]
pub trait VerificationSender: Send + Sync {
    async fn send_verification_code(
        &self,
        email: &str,
        code: &str,
        purpose: VerificationPurpose,
        verify_url: &str,
    ) -> LedgerResult<()>;
}

#[derive(Default)]
pub struct LogVerificationSender;

#[async_trait]
impl VerificationSender for LogVerificationSender {
    async fn send_verification_code(
        &self,
        email: &str,
        code: &str,
        purpose: VerificationPurpose,
        verify_url: &str,
    ) -> LedgerResult<()> {
        info!(
            email = %email,
            code = %code,
            purpose = %verification_purpose_to_str(&purpose),
            verify_url = %verify_url,
            delivery_mode = "log",
            "verification email logged"
        );
        Ok(())
    }
}

pub struct ResendVerificationSender {
    client: Client,
    api_key: String,
    from_email: String,
}

impl ResendVerificationSender {
    pub fn new(api_key: String, from_email: String) -> Self {
        Self {
            client: Client::new(),
            api_key,
            from_email,
        }
    }
}

#[async_trait]
impl VerificationSender for ResendVerificationSender {
    async fn send_verification_code(
        &self,
        email: &str,
        code: &str,
        purpose: VerificationPurpose,
        verify_url: &str,
    ) -> LedgerResult<()> {
        let subject = match purpose {
            VerificationPurpose::Signup => "Verify your ChainActors signup",
            VerificationPurpose::Login => "Your ChainActors sign-in code",
        };
        let response = self
            .client
            .post("https://api.resend.com/emails")
            .bearer_auth(&self.api_key)
            .json(&json!({
                "from": self.from_email,
                "to": [email],
                "subject": subject,
                "text": format!(
                    "Your ChainActors verification code is {}.\n\nYou can also open this link:\n{}",
                    code, verify_url
                ),
                "html": format!(
                    "<div style=\"font-family:ui-sans-serif,system-ui,sans-serif;line-height:1.5\"><p>Your ChainActors verification code is <strong style=\"font-size:24px\">{}</strong>.</p><p><a href=\"{}\">Verify in browser</a></p><p>This code expires in 15 minutes.</p></div>",
                    code, verify_url
                ),
            }))
            .send()
            .await
            .map_err(|err| LedgerError::TransientIngestion(err.to_string()))?;
        if !response.status().is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(LedgerError::TransientIngestion(format!(
                "resend email delivery failed: {} {}",
                subject, body
            )));
        }
        info!(
            email = %email,
            purpose = %verification_purpose_to_str(&purpose),
            delivery_mode = "resend",
            "verification email sent"
        );
        Ok(())
    }
}

pub struct PostmarkVerificationSender {
    client: Client,
    server_token: String,
    from_email: String,
    message_stream: String,
}

impl PostmarkVerificationSender {
    pub fn new(server_token: String, from_email: String, message_stream: String) -> Self {
        Self {
            client: Client::new(),
            server_token,
            from_email,
            message_stream,
        }
    }
}

#[async_trait]
impl VerificationSender for PostmarkVerificationSender {
    async fn send_verification_code(
        &self,
        email: &str,
        code: &str,
        purpose: VerificationPurpose,
        verify_url: &str,
    ) -> LedgerResult<()> {
        let subject = match purpose {
            VerificationPurpose::Signup => "Verify your ChainActors signup",
            VerificationPurpose::Login => "Your ChainActors sign-in code",
        };
        let response = self
            .client
            .post("https://api.postmarkapp.com/email")
            .header("Accept", "application/json")
            .header("Content-Type", "application/json")
            .header("X-Postmark-Server-Token", &self.server_token)
            .json(&json!({
                "From": self.from_email,
                "To": email,
                "Subject": subject,
                "TextBody": format!(
                    "Your ChainActors verification code is {}.\n\nYou can also open this link:\n{}",
                    code, verify_url
                ),
                "HtmlBody": format!(
                    "<div style=\"font-family:ui-sans-serif,system-ui,sans-serif;line-height:1.5\"><p>Your ChainActors verification code is <strong style=\"font-size:24px\">{}</strong>.</p><p><a href=\"{}\">Verify in browser</a></p><p>This code expires in 15 minutes.</p></div>",
                    code, verify_url
                ),
                "MessageStream": self.message_stream,
            }))
            .send()
            .await
            .map_err(|err| LedgerError::TransientIngestion(err.to_string()))?;
        if !response.status().is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(LedgerError::TransientIngestion(format!(
                "postmark email delivery failed: {} {}",
                subject, body
            )));
        }
        info!(
            email = %email,
            purpose = %verification_purpose_to_str(&purpose),
            delivery_mode = "postmark",
            "verification email sent"
        );
        Ok(())
    }
}

fn validate_password(password: &str) -> LedgerResult<()> {
    if password.chars().count() < MIN_PASSWORD_CHARACTERS {
        return Err(LedgerError::AnalysisInvariant(format!(
            "password must contain at least {MIN_PASSWORD_CHARACTERS} characters"
        )));
    }
    if password.len() > MAX_PASSWORD_BYTES {
        return Err(LedgerError::AnalysisInvariant(format!(
            "password must not exceed {MAX_PASSWORD_BYTES} bytes"
        )));
    }
    Ok(())
}

async fn hash_password(password: &str) -> LedgerResult<String> {
    validate_password(password)?;
    let password = password.to_owned();
    tokio::task::spawn_blocking(move || hash_password_blocking(&password))
        .await
        .map_err(|err| LedgerError::Persistence(format!("password hashing task failed: {err}")))?
}

async fn verify_password(password: &str, password_hash: Option<&str>) -> LedgerResult<bool> {
    if password.len() > MAX_PASSWORD_BYTES {
        return Ok(false);
    }
    let password = password.to_owned();
    let password_hash = password_hash.map(str::to_owned);
    tokio::task::spawn_blocking(move || match password_hash {
        Some(password_hash) => {
            let parsed_hash = PasswordHash::new(&password_hash).map_err(|err| {
                LedgerError::Persistence(format!("stored password hash is invalid: {err}"))
            })?;
            Ok(Argon2::default()
                .verify_password(password.as_bytes(), &parsed_hash)
                .is_ok())
        }
        None => {
            let _ = hash_password_blocking(&password)?;
            Ok(false)
        }
    })
    .await
    .map_err(|err| LedgerError::Persistence(format!("password verification task failed: {err}")))?
}

fn hash_password_blocking(password: &str) -> LedgerResult<String> {
    let salt = SaltString::encode_b64(Uuid::new_v4().as_bytes())
        .map_err(|err| LedgerError::Persistence(format!("password salt failed: {err}")))?;
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map(|hash| hash.to_string())
        .map_err(|err| LedgerError::Persistence(format!("password hashing failed: {err}")))
}

pub fn normalize_email(email_raw: &str) -> LedgerResult<String> {
    let email = email_raw.trim().to_lowercase();
    if email.is_empty() || !email.contains('@') || email.starts_with('@') || email.ends_with('@') {
        return Err(LedgerError::AnalysisInvariant(
            "a valid email address is required".to_owned(),
        ));
    }
    Ok(email)
}

pub fn verification_purpose_to_str(purpose: &VerificationPurpose) -> &'static str {
    match purpose {
        VerificationPurpose::Signup => "signup",
        VerificationPurpose::Login => "login",
    }
}

pub fn verification_purpose_from_str(value: &str) -> Option<VerificationPurpose> {
    match value {
        "signup" => Some(VerificationPurpose::Signup),
        "login" => Some(VerificationPurpose::Login),
        _ => None,
    }
}

fn generate_verification_code() -> String {
    let uuid = Uuid::new_v4();
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&uuid.as_bytes()[..8]);
    let value = u64::from_be_bytes(bytes) % 1_000_000;
    format!("{value:06}")
}

fn hash_verification_code(email: &str, purpose: &VerificationPurpose, code: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(format!(
        "ledgerflow:verification:{}:{}:{}",
        email,
        verification_purpose_to_str(purpose),
        code.trim()
    ));
    hex::encode(hasher.finalize())
}

fn hash_session_token(session_token: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(format!("ledgerflow:session:{}", session_token.trim()));
    hex::encode(hasher.finalize())
}

fn row_to_app_user(row: sqlx::postgres::PgRow) -> LedgerResult<AppUser> {
    Ok(AppUser {
        user_id: row
            .try_get("user_id")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?,
        email: row
            .try_get("email")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?,
        workspace_plan: row
            .try_get("workspace_plan")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?,
        email_verified_at: row
            .try_get("email_verified_at")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?,
        created_at: row
            .try_get("created_at")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_email_lowercases_and_trims() {
        let email = normalize_email("  Alice@Example.com ").unwrap();
        assert_eq!(email, "alice@example.com");
    }

    #[test]
    fn verification_code_hash_depends_on_purpose() {
        let signup =
            hash_verification_code("alice@example.com", &VerificationPurpose::Signup, "123456");
        let login =
            hash_verification_code("alice@example.com", &VerificationPurpose::Login, "123456");
        assert_ne!(signup, login);
    }

    #[test]
    fn password_validation_enforces_length_limits() {
        assert!(validate_password("long-enough-password").is_ok());
        assert!(validate_password("short").is_err());
        assert!(validate_password(&"x".repeat(MAX_PASSWORD_BYTES + 1)).is_err());
    }

    #[test]
    fn session_user_projection_includes_workspace_plan() {
        let source = include_str!("auth.rs");
        let session_lookup = source
            .split_once("pub async fn get_user_by_session_token")
            .and_then(|(_, remainder)| remainder.split_once("pub async fn revoke_session"))
            .map(|(lookup, _)| lookup)
            .expect("session lookup source boundaries");
        assert!(session_lookup.contains("users.workspace_plan"));
    }

    #[tokio::test]
    async fn password_hash_round_trip_rejects_wrong_password() {
        let hash = hash_password("correct horse battery staple").await.unwrap();
        assert!(
            verify_password("correct horse battery staple", Some(&hash))
                .await
                .unwrap()
        );
        assert!(
            !verify_password("wrong password", Some(&hash))
                .await
                .unwrap()
        );
    }
}
