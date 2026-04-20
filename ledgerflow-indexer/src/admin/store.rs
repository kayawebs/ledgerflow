use argon2::{
    Argon2,
    password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
};
use chrono::{Datelike, Duration, Utc};
use rand_core::OsRng;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::admin::billing::VerifiedBaseEthPayment;
use crate::admin::models::{ApiKeyCreated, Plan, ReferralSummary, User};
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
              password_hash text,
              referral_code text unique,
              referred_by_user_id uuid references indexer_users(user_id),
              email_verified boolean not null default false,
              created_at timestamptz not null default now(),
              updated_at timestamptz not null default now()
            )
            "#,
            r#"
            create table if not exists indexer_email_otps (
              otp_id uuid primary key,
              email text not null,
              purpose text not null default 'verify_email',
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
            create table if not exists indexer_sessions (
              session_id uuid primary key,
              user_id uuid not null references indexer_users(user_id),
              token_hash text not null unique,
              expires_at timestamptz not null,
              revoked_at timestamptz,
              created_at timestamptz not null default now()
            )
            "#,
            r#"
            create table if not exists indexer_referral_rewards (
              reward_id uuid primary key,
              referrer_user_id uuid not null references indexer_users(user_id),
              referred_user_id uuid not null references indexer_users(user_id),
              reward_requests bigint not null,
              reward_month date not null,
              created_at timestamptz not null default now(),
              unique (referrer_user_id, referred_user_id)
            )
            "#,
            "alter table indexer_users add column if not exists password_hash text",
            "alter table indexer_users add column if not exists referral_code text unique",
            "alter table indexer_users add column if not exists referred_by_user_id uuid references indexer_users(user_id)",
            "alter table indexer_email_otps add column if not exists purpose text not null default 'verify_email'",
            "create index if not exists indexer_email_otps_email_purpose_created_idx on indexer_email_otps (email, purpose, created_at desc)",
            "create index if not exists indexer_sessions_token_hash_idx on indexer_sessions (token_hash)",
            "create index if not exists indexer_referral_rewards_month_idx on indexer_referral_rewards (referrer_user_id, reward_month)",
            r#"
            insert into indexer_plans (plan_id, name, monthly_price_wei, monthly_request_quota)
            values
              ('launch', 'Launch', 9000000000000000, 1000000)
            on conflict (plan_id) do update set
              name = excluded.name,
              monthly_price_wei = excluded.monthly_price_wei,
              monthly_request_quota = excluded.monthly_request_quota
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

    pub async fn create_email_otp_for_purpose(
        &self,
        email: &str,
        code: &str,
        purpose: &str,
    ) -> LedgerResult<()> {
        let email = normalize_email(email)?;
        ensure_valid_purpose(purpose)?;
        let recent = sqlx::query(
            r#"
            select created_at
            from indexer_email_otps
            where email = $1
              and purpose = $2
              and created_at > now() - interval '60 seconds'
            order by created_at desc
            limit 1
            "#,
        )
        .bind(&email)
        .bind(purpose)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        if recent.is_some() {
            return Err(LedgerError::Conflict(
                "verification code already sent; wait 60 seconds before retrying".to_owned(),
            ));
        }

        let code_hash = hash_secret(code);
        let expires_at = Utc::now() + Duration::minutes(10);
        sqlx::query(
            r#"
            insert into indexer_email_otps (otp_id, email, purpose, code_hash, expires_at)
            values ($1, $2, $3, $4, $5)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(email)
        .bind(purpose)
        .bind(code_hash)
        .bind(expires_at)
        .execute(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        Ok(())
    }

    pub async fn verify_email_otp(&self, email: &str, code: &str) -> LedgerResult<User> {
        self.verify_email_otp_for_purpose(email, code, "verify_email")
            .await
    }

    pub async fn verify_email_otp_for_purpose(
        &self,
        email: &str,
        code: &str,
        purpose: &str,
    ) -> LedgerResult<User> {
        let email = normalize_email(email)?;
        ensure_valid_purpose(purpose)?;
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
              and purpose = $2
              and code_hash = $3
              and consumed_at is null
              and expires_at > now()
            order by created_at desc
            limit 1
            "#,
        )
        .bind(&email)
        .bind(purpose)
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

    pub async fn set_user_password(&self, email: &str, password: &str) -> LedgerResult<User> {
        self.set_user_password_with_referral(email, password, None)
            .await
    }

    pub async fn set_user_password_with_referral(
        &self,
        email: &str,
        password: &str,
        referral_code: Option<&str>,
    ) -> LedgerResult<User> {
        let email = normalize_email(email)?;
        let password_hash = hash_password(password)?;
        let referral_code = referral_code
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_ascii_lowercase);
        let referred_by_user_id = if let Some(referral_code) = referral_code.as_deref() {
            sqlx::query("select user_id from indexer_users where referral_code = $1")
                .bind(referral_code)
                .fetch_optional(&self.pool)
                .await
                .map_err(|err| LedgerError::Persistence(err.to_string()))?
                .map(|row| row.try_get::<Uuid, _>("user_id"))
                .transpose()
                .map_err(|err| LedgerError::Persistence(err.to_string()))?
        } else {
            None
        };
        let row = sqlx::query(
            r#"
            update indexer_users
            set password_hash = $2,
                email_verified = true,
                referral_code = coalesce(referral_code, $3),
                referred_by_user_id = coalesce(referred_by_user_id, $4),
                updated_at = now()
            where email = $1
            returning user_id, email, email_verified, created_at
            "#,
        )
        .bind(&email)
        .bind(password_hash)
        .bind(new_referral_code())
        .bind(referred_by_user_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        let Some(row) = row else {
            return Err(LedgerError::Conflict("user not found".to_owned()));
        };
        let user = row_to_user(row)?;
        if let Some(referrer_user_id) = referred_by_user_id {
            if referrer_user_id != user.user_id {
                self.record_referral_reward(referrer_user_id, user.user_id)
                    .await?;
            }
        }
        Ok(user)
    }

    pub async fn authenticate_password(&self, email: &str, password: &str) -> LedgerResult<User> {
        let email = normalize_email(email)?;
        let row = sqlx::query(
            r#"
            select user_id, email, email_verified, created_at, password_hash
            from indexer_users
            where email = $1
              and email_verified = true
            "#,
        )
        .bind(&email)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        let Some(row) = row else {
            return Err(LedgerError::Unauthorized(
                "invalid email or password".to_owned(),
            ));
        };
        let password_hash = row
            .try_get::<Option<String>, _>("password_hash")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        let Some(password_hash) = password_hash else {
            return Err(LedgerError::Unauthorized(
                "password login is not enabled for this account".to_owned(),
            ));
        };
        if !verify_password(password, &password_hash)? {
            return Err(LedgerError::Unauthorized(
                "invalid email or password".to_owned(),
            ));
        }
        row_to_user(row)
    }

    pub async fn create_session(&self, user_id: Uuid) -> LedgerResult<String> {
        let token = format!("lfs_{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
        let token_hash = hash_secret(&token);
        let expires_at = Utc::now() + Duration::days(30);
        sqlx::query(
            r#"
            insert into indexer_sessions (session_id, user_id, token_hash, expires_at)
            values ($1, $2, $3, $4)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(user_id)
        .bind(token_hash)
        .bind(expires_at)
        .execute(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        Ok(token)
    }

    pub async fn get_user_by_session(&self, token: &str) -> LedgerResult<Option<User>> {
        let token_hash = hash_secret(token);
        let row = sqlx::query(
            r#"
            select u.user_id, u.email, u.email_verified, u.created_at
            from indexer_sessions s
            join indexer_users u on u.user_id = s.user_id
            where s.token_hash = $1
              and s.revoked_at is null
              and s.expires_at > now()
            limit 1
            "#,
        )
        .bind(token_hash)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        row.map(row_to_user).transpose()
    }

    pub async fn revoke_session(&self, token: &str) -> LedgerResult<()> {
        let token_hash = hash_secret(token);
        sqlx::query("update indexer_sessions set revoked_at = now() where token_hash = $1")
            .bind(token_hash)
            .execute(&self.pool)
            .await
            .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        Ok(())
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
              k.user_id,
              s.monthly_request_quota,
              coalesce((
                select sum(reward_requests)
                from indexer_referral_rewards r
                where r.referrer_user_id = k.user_id
                  and r.reward_month = date_trunc('month', current_date)::date
              ), 0) as bonus_requests,
              coalesce((
                select sum(request_count)
                from indexer_usage_daily u2
                where u2.user_id = k.user_id
                  and u2.usage_date >= date_trunc('month', current_date)::date
                  and u2.usage_date < (date_trunc('month', current_date) + interval '1 month')::date
              ), 0) as used_requests
            from indexer_api_keys k
            join indexer_users u on u.user_id = k.user_id
            join indexer_subscriptions sub on sub.user_id = k.user_id
            join indexer_plans s on s.plan_id = sub.plan_id
            where k.key_hash = $1
              and k.revoked_at is null
              and u.email_verified = true
              and sub.status = 'active'
              and sub.current_period_start <= now()
              and sub.current_period_end > now()
            order by sub.current_period_end desc
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
        let monthly_quota = key_row
            .try_get::<i64, _>("monthly_request_quota")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        let bonus_requests = key_row
            .try_get::<i64, _>("bonus_requests")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        let used_requests = key_row
            .try_get::<i64, _>("used_requests")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        let auth = AuthenticatedApiKey {
            key_id: key_row
                .try_get("key_id")
                .map_err(|err| LedgerError::Persistence(err.to_string()))?,
            user_id: key_row
                .try_get("user_id")
                .map_err(|err| LedgerError::Persistence(err.to_string()))?,
        };
        if used_requests >= monthly_quota + bonus_requests {
            return Err(LedgerError::PaymentRequired(
                "monthly request quota exhausted".to_owned(),
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

    async fn ensure_referral_code(&self, user_id: Uuid) -> LedgerResult<()> {
        sqlx::query(
            r#"
            update indexer_users
            set referral_code = coalesce(referral_code, $2),
                updated_at = now()
            where user_id = $1
              and referral_code is null
            "#,
        )
        .bind(user_id)
        .bind(new_referral_code())
        .execute(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        Ok(())
    }

    async fn record_referral_reward(
        &self,
        referrer_user_id: Uuid,
        referred_user_id: Uuid,
    ) -> LedgerResult<()> {
        let reward_month = Utc::now()
            .date_naive()
            .with_day(1)
            .ok_or_else(|| LedgerError::Persistence("invalid reward month".to_owned()))?;
        let current_bonus = sqlx::query(
            r#"
            select coalesce(sum(reward_requests), 0)::bigint as current_bonus
            from indexer_referral_rewards
            where referrer_user_id = $1
              and reward_month = $2
            "#,
        )
        .bind(referrer_user_id)
        .bind(reward_month)
        .fetch_one(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?
        .try_get::<i64, _>("current_bonus")
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        let remaining = 250_000_i64.saturating_sub(current_bonus);
        let reward = remaining.min(25_000);
        if reward <= 0 {
            return Ok(());
        }
        sqlx::query(
            r#"
            insert into indexer_referral_rewards (
              reward_id, referrer_user_id, referred_user_id, reward_requests, reward_month
            )
            values ($1, $2, $3, $4, $5)
            on conflict (referrer_user_id, referred_user_id) do nothing
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(referrer_user_id)
        .bind(referred_user_id)
        .bind(reward)
        .bind(reward_month)
        .execute(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        Ok(())
    }

    pub async fn get_plan_price_wei(&self, plan_id: &str) -> LedgerResult<u128> {
        if plan_id != "launch" {
            return Err(LedgerError::Conflict(
                "only the launch plan is currently available".to_owned(),
            ));
        }
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
            where plan_id = 'launch'
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

    pub async fn get_referral_summary(&self, user_id: Uuid) -> LedgerResult<ReferralSummary> {
        self.ensure_referral_code(user_id).await?;
        let row = sqlx::query(
            r#"
            select
              referral_code,
              coalesce((
                select count(*)
                from indexer_referral_rewards r
                where r.referrer_user_id = indexer_users.user_id
                  and r.reward_month = date_trunc('month', current_date)::date
              ), 0)::bigint as rewarded_referrals_this_month,
              coalesce((
                select sum(reward_requests)
                from indexer_referral_rewards r
                where r.referrer_user_id = indexer_users.user_id
                  and r.reward_month = date_trunc('month', current_date)::date
              ), 0)::bigint as bonus_requests_this_month
            from indexer_users
            where user_id = $1
            "#,
        )
        .bind(user_id)
        .fetch_one(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;

        Ok(ReferralSummary {
            referral_code: row
                .try_get("referral_code")
                .map_err(|err| LedgerError::Persistence(err.to_string()))?,
            reward_per_verified_referral: 25_000,
            monthly_reward_cap: 250_000,
            rewarded_referrals_this_month: row
                .try_get("rewarded_referrals_this_month")
                .map_err(|err| LedgerError::Persistence(err.to_string()))?,
            bonus_requests_this_month: row
                .try_get("bonus_requests_this_month")
                .map_err(|err| LedgerError::Persistence(err.to_string()))?,
        })
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

fn new_referral_code() -> String {
    format!("lf{}", Uuid::new_v4().simple())
        .chars()
        .take(12)
        .collect::<String>()
        .to_ascii_lowercase()
}

fn ensure_valid_purpose(purpose: &str) -> LedgerResult<()> {
    match purpose {
        "verify_email" | "password_reset" => Ok(()),
        _ => Err(LedgerError::Conflict(format!(
            "unsupported verification purpose: {purpose}"
        ))),
    }
}

fn hash_password(password: &str) -> LedgerResult<String> {
    if password.len() < 8 {
        return Err(LedgerError::Conflict(
            "password must be at least 8 characters".to_owned(),
        ));
    }
    let salt = SaltString::generate(&mut OsRng);
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map(|hash| hash.to_string())
        .map_err(|err| LedgerError::Persistence(err.to_string()))
}

fn verify_password(password: &str, encoded: &str) -> LedgerResult<bool> {
    let parsed =
        PasswordHash::new(encoded).map_err(|err| LedgerError::Persistence(err.to_string()))?;
    Ok(Argon2::default()
        .verify_password(password.as_bytes(), &parsed)
        .is_ok())
}

fn hash_secret(secret: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(secret.as_bytes());
    hex::encode(hasher.finalize())
}
