use chrono::{Duration, Utc};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::admin::billing::VerifiedBaseEthPayment;
use crate::admin::models::{ApiKeyCreated, Plan, User};
use crate::core::errors::{LedgerError, LedgerResult};

#[derive(Clone)]
pub struct PostgresAdminStore {
    pool: PgPool,
}

#[derive(Clone, Debug)]
pub struct AuthenticatedApiKey {
    pub user_id: Uuid,
    pub key_id: Uuid,
}

impl PostgresAdminStore {
    pub async fn connect(database_url: &str) -> LedgerResult<Self> {
        let pool = PgPool::connect(database_url)
            .await
            .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        Ok(Self { pool })
    }

    pub async fn bootstrap(&self) -> LedgerResult<()> {
        let statements = [
            r#"
            create table if not exists indexer_users (
              user_id uuid primary key,
              email text not null unique,
              email_verified boolean not null default false,
              created_at timestamptz not null default now(),
              updated_at timestamptz not null default now()
            )
            "#,
            r#"
            create table if not exists indexer_email_otps (
              otp_id uuid primary key,
              email text not null,
              code_hash text not null,
              expires_at timestamptz not null,
              consumed_at timestamptz,
              created_at timestamptz not null default now()
            )
            "#,
            r#"
            create table if not exists indexer_api_keys (
              key_id uuid primary key,
              user_id uuid not null references indexer_users(user_id),
              name text not null,
              key_prefix text not null,
              key_hash text not null unique,
              revoked_at timestamptz,
              created_at timestamptz not null default now()
            )
            "#,
            r#"
            create table if not exists indexer_plans (
              plan_id text primary key,
              name text not null,
              monthly_price_wei numeric(78,0) not null,
              monthly_request_quota bigint not null,
              created_at timestamptz not null default now()
            )
            "#,
            r#"
            create table if not exists indexer_subscriptions (
              subscription_id uuid primary key,
              user_id uuid not null references indexer_users(user_id),
              plan_id text not null references indexer_plans(plan_id),
              status text not null,
              current_period_start timestamptz not null,
              current_period_end timestamptz not null,
              created_at timestamptz not null default now(),
              updated_at timestamptz not null default now()
            )
            "#,
            r#"
            create table if not exists indexer_payments (
              payment_id uuid primary key,
              user_id uuid references indexer_users(user_id),
              plan_id text not null,
              chain_id bigint not null,
              tx_hash text not null unique,
              from_address text,
              to_address text,
              amount_wei numeric(78,0),
              status text not null,
              metadata jsonb not null default '{}'::jsonb,
              created_at timestamptz not null default now(),
              updated_at timestamptz not null default now()
            )
            "#,
            r#"
            create table if not exists indexer_usage_daily (
              usage_date date not null,
              user_id uuid not null references indexer_users(user_id),
              api_key_id uuid references indexer_api_keys(key_id),
              endpoint text not null,
              request_count bigint not null default 0,
              primary key (usage_date, user_id, api_key_id, endpoint)
            )
            "#,
            r#"
            insert into indexer_plans (plan_id, name, monthly_price_wei, monthly_request_quota)
            values
              ('developer', 'Developer', 10000000000000000, 1000000),
              ('operator', 'Operator', 50000000000000000, 10000000)
            on conflict (plan_id) do nothing
            "#,
        ];

        for statement in statements {
            sqlx::query(statement)
                .execute(&self.pool)
                .await
                .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        }
        Ok(())
    }

    pub async fn create_email_otp(&self, email: &str, code: &str) -> LedgerResult<()> {
        let email = normalize_email(email)?;
        let code_hash = hash_secret(code);
        let expires_at = Utc::now() + Duration::minutes(10);
        sqlx::query(
            r#"
            insert into indexer_email_otps (otp_id, email, code_hash, expires_at)
            values ($1, $2, $3, $4)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(email)
        .bind(code_hash)
        .bind(expires_at)
        .execute(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        Ok(())
    }

    pub async fn verify_email_otp(&self, email: &str, code: &str) -> LedgerResult<User> {
        let email = normalize_email(email)?;
        let code_hash = hash_secret(code);
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|err| LedgerError::Persistence(err.to_string()))?;

        let row = sqlx::query(
            r#"
            select otp_id
            from indexer_email_otps
            where email = $1
              and code_hash = $2
              and consumed_at is null
              and expires_at > now()
            order by created_at desc
            limit 1
            "#,
        )
        .bind(&email)
        .bind(code_hash)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;

        let Some(row) = row else {
            return Err(LedgerError::Conflict("invalid or expired code".to_owned()));
        };
        let otp_id: Uuid = row
            .try_get("otp_id")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?;

        sqlx::query("update indexer_email_otps set consumed_at = now() where otp_id = $1")
            .bind(otp_id)
            .execute(&mut *tx)
            .await
            .map_err(|err| LedgerError::Persistence(err.to_string()))?;

        let user_id = Uuid::new_v4();
        let row = sqlx::query(
            r#"
            insert into indexer_users (user_id, email, email_verified)
            values ($1, $2, true)
            on conflict (email) do update set
              email_verified = true,
              updated_at = now()
            returning user_id, email, email_verified, created_at
            "#,
        )
        .bind(user_id)
        .bind(&email)
        .fetch_one(&mut *tx)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;

        tx.commit()
            .await
            .map_err(|err| LedgerError::Persistence(err.to_string()))?;

        row_to_user(row)
    }

    pub async fn create_api_key(&self, email: &str, name: &str) -> LedgerResult<ApiKeyCreated> {
        let email = normalize_email(email)?;
        let user_row = sqlx::query(
            "select user_id from indexer_users where email = $1 and email_verified = true",
        )
        .bind(&email)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        let Some(user_row) = user_row else {
            return Err(LedgerError::Conflict("email is not verified".to_owned()));
        };
        let user_id: Uuid = user_row
            .try_get("user_id")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?;

        let key_id = Uuid::new_v4();
        let raw_key = format!(
            "lf_live_{}{}",
            Uuid::new_v4().simple(),
            Uuid::new_v4().simple()
        );
        let key_prefix = raw_key.chars().take(16).collect::<String>();
        let key_hash = hash_secret(&raw_key);
        sqlx::query(
            r#"
            insert into indexer_api_keys (key_id, user_id, name, key_prefix, key_hash)
            values ($1, $2, $3, $4, $5)
            "#,
        )
        .bind(key_id)
        .bind(user_id)
        .bind(name)
        .bind(&key_prefix)
        .bind(key_hash)
        .execute(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        Ok(ApiKeyCreated {
            key_id,
            key_prefix,
            api_key: raw_key,
        })
    }

    pub async fn authenticate_api_key(&self, raw_key: &str) -> LedgerResult<AuthenticatedApiKey> {
        let key_hash = hash_secret(raw_key);
        let key_row = sqlx::query(
            r#"
            select
              k.key_id,
              k.user_id
            from indexer_api_keys k
            join indexer_users u on u.user_id = k.user_id
            where k.key_hash = $1
              and k.revoked_at is null
              and u.email_verified = true
            limit 1
            "#,
        )
        .bind(key_hash)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;

        let Some(key_row) = key_row else {
            return Err(LedgerError::Unauthorized("invalid api key".to_owned()));
        };
        let auth = AuthenticatedApiKey {
            key_id: key_row
                .try_get("key_id")
                .map_err(|err| LedgerError::Persistence(err.to_string()))?,
            user_id: key_row
                .try_get("user_id")
                .map_err(|err| LedgerError::Persistence(err.to_string()))?,
        };

        let has_active_subscription = sqlx::query(
            r#"
            select 1
            from indexer_subscriptions
            where user_id = $1
              and status = 'active'
              and current_period_start <= now()
              and current_period_end > now()
            order by current_period_end desc
            limit 1
            "#,
        )
        .bind(auth.user_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?
        .is_some();

        if !has_active_subscription {
            return Err(LedgerError::PaymentRequired(
                "active subscription required".to_owned(),
            ));
        }

        Ok(auth)
    }

    pub async fn record_usage(
        &self,
        user_id: Uuid,
        api_key_id: Uuid,
        endpoint: &str,
    ) -> LedgerResult<()> {
        let usage_date = Utc::now().date_naive();
        sqlx::query(
            r#"
            insert into indexer_usage_daily (
              usage_date, user_id, api_key_id, endpoint, request_count
            )
            values ($1, $2, $3, $4, 1)
            on conflict (usage_date, user_id, api_key_id, endpoint)
            do update set request_count = indexer_usage_daily.request_count + 1
            "#,
        )
        .bind(usage_date)
        .bind(user_id)
        .bind(api_key_id)
        .bind(endpoint)
        .execute(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        Ok(())
    }

    pub async fn get_plan_price_wei(&self, plan_id: &str) -> LedgerResult<u128> {
        let row = sqlx::query(
            "select monthly_price_wei::text as monthly_price_wei from indexer_plans where plan_id = $1",
        )
        .bind(plan_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        let Some(row) = row else {
            return Err(LedgerError::Conflict(format!("unknown plan: {plan_id}")));
        };
        let amount: String = row
            .try_get("monthly_price_wei")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        amount
            .parse::<u128>()
            .map_err(|err| LedgerError::Persistence(err.to_string()))
    }

    pub async fn list_plans(&self) -> LedgerResult<Vec<Plan>> {
        let rows = sqlx::query(
            r#"
            select
              plan_id,
              name,
              monthly_price_wei::text as monthly_price_wei,
              monthly_request_quota
            from indexer_plans
            order by monthly_price_wei asc
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;

        rows.into_iter()
            .map(|row| {
                Ok(Plan {
                    plan_id: row
                        .try_get("plan_id")
                        .map_err(|err| LedgerError::Persistence(err.to_string()))?,
                    name: row
                        .try_get("name")
                        .map_err(|err| LedgerError::Persistence(err.to_string()))?,
                    monthly_price_wei: row
                        .try_get("monthly_price_wei")
                        .map_err(|err| LedgerError::Persistence(err.to_string()))?,
                    monthly_request_quota: row
                        .try_get("monthly_request_quota")
                        .map_err(|err| LedgerError::Persistence(err.to_string()))?,
                })
            })
            .collect()
    }

    pub async fn record_submitted_payment(
        &self,
        email: &str,
        plan_id: &str,
        tx_hash: &str,
        from_address: Option<&str>,
    ) -> LedgerResult<(Uuid, Uuid)> {
        let email = normalize_email(email)?;
        let user_id = sqlx::query(
            "select user_id from indexer_users where email = $1 and email_verified = true",
        )
        .bind(email)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?
        .map(|row| row.try_get::<Uuid, _>("user_id"))
        .transpose()
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        let Some(user_id) = user_id else {
            return Err(LedgerError::Conflict(
                "email must be verified before payment submission".to_owned(),
            ));
        };
        let payment_id = Uuid::new_v4();
        let row = sqlx::query(
            r#"
            insert into indexer_payments (
              payment_id, user_id, plan_id, chain_id, tx_hash, from_address, status
            )
            values ($1, $2, $3, 8453, $4, $5, 'submitted')
            on conflict (tx_hash) do update set
              updated_at = now()
            returning payment_id
            "#,
        )
        .bind(payment_id)
        .bind(user_id)
        .bind(plan_id)
        .bind(tx_hash)
        .bind(from_address)
        .fetch_one(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        let payment_id = row
            .try_get("payment_id")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        Ok((payment_id, user_id))
    }

    pub async fn mark_payment_verified_and_activate_subscription(
        &self,
        user_id: Uuid,
        payment_id: Uuid,
        plan_id: &str,
        verified: &VerifiedBaseEthPayment,
    ) -> LedgerResult<Uuid> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|err| LedgerError::Persistence(err.to_string()))?;

        sqlx::query(
            r#"
            update indexer_payments
            set
              status = 'verified',
              from_address = $2,
              to_address = $3,
              amount_wei = $4::numeric,
              metadata = jsonb_build_object(
                'block_number', $5::bigint,
                'confirmations', $6::bigint
              ),
              updated_at = now()
            where payment_id = $1
            "#,
        )
        .bind(payment_id)
        .bind(&verified.from_address)
        .bind(&verified.to_address)
        .bind(&verified.amount_wei)
        .bind(verified.block_number as i64)
        .bind(verified.confirmations as i64)
        .execute(&mut *tx)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;

        let subscription_id = Uuid::new_v4();
        let row = sqlx::query(
            r#"
            insert into indexer_subscriptions (
              subscription_id,
              user_id,
              plan_id,
              status,
              current_period_start,
              current_period_end
            )
            values ($1, $2, $3, 'active', now(), now() + interval '30 days')
            returning subscription_id
            "#,
        )
        .bind(subscription_id)
        .bind(user_id)
        .bind(plan_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;

        tx.commit()
            .await
            .map_err(|err| LedgerError::Persistence(err.to_string()))?;

        row.try_get("subscription_id")
            .map_err(|err| LedgerError::Persistence(err.to_string()))
    }
}

fn row_to_user(row: sqlx::postgres::PgRow) -> LedgerResult<User> {
    Ok(User {
        user_id: row
            .try_get("user_id")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?,
        email: row
            .try_get("email")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?,
        email_verified: row
            .try_get("email_verified")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?,
        created_at: row
            .try_get("created_at")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?,
    })
}

fn normalize_email(email: &str) -> LedgerResult<String> {
    let email = email.trim().to_ascii_lowercase();
    if !email.contains('@') || email.len() > 320 {
        return Err(LedgerError::Conflict("invalid email".to_owned()));
    }
    Ok(email)
}

fn hash_secret(secret: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(secret.as_bytes());
    hex::encode(hasher.finalize())
}
