use std::{collections::BTreeMap, env};

use ledgerflow::{
    core::logging::init_logging,
    pilot::{
        PilotEvidencePack,
        alerts::{format_telegram_alert, retry_delay_seconds},
    },
};
use reqwest::{Client, StatusCode};
use serde_json::Value;
use sqlx::{PgPool, Row, postgres::PgPoolOptions, types::Json};
use tokio::time::{Duration, sleep};
use tracing::{info, warn};
use uuid::Uuid;

#[derive(Debug)]
struct ClaimedDelivery {
    delivery_id: Uuid,
    destination_id: Uuid,
    group_key: String,
    attempt_count: i32,
    chat_id: String,
    secret_ref: Option<String>,
    project_name: String,
    pack: PilotEvidencePack,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    dotenvy::dotenv().ok();
    init_logging();
    let database_url = env::var("LEDGERFLOW_DATABASE_URL").or_else(|_| env::var("DATABASE_URL"))?;
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&database_url)
        .await?;
    sqlx::migrate!("./migrations").run(&pool).await?;
    let client = Client::new();
    let interval_ms = env_u64("LEDGERFLOW_PILOT_ALERT_POLL_INTERVAL_MS", 2_000);
    let batch_size = env_u64("LEDGERFLOW_PILOT_ALERT_BATCH_SIZE", 50).clamp(1, 200) as i64;
    let group_size = env_u64("LEDGERFLOW_PILOT_ALERT_GROUP_SIZE", 5).clamp(1, 20) as usize;
    let rate_limit =
        env_u64("LEDGERFLOW_PILOT_TELEGRAM_RATE_LIMIT_PER_MINUTE", 20).clamp(1, 60) as i64;
    let max_attempts = env_u64("LEDGERFLOW_PILOT_ALERT_MAX_ATTEMPTS", 8).clamp(1, 32) as i32;
    let web_base =
        env::var("LEDGERFLOW_WEB_BASE_URL").unwrap_or_else(|_| "http://127.0.0.1:3001".into());
    let telegram_api_base = env::var("LEDGERFLOW_TELEGRAM_API_BASE_URL")
        .unwrap_or_else(|_| "https://api.telegram.org".into());
    loop {
        recover_stale_claims(&pool).await?;
        let deliveries = claim_due(&pool, batch_size).await?;
        let mut groups: BTreeMap<(Uuid, String), Vec<ClaimedDelivery>> = BTreeMap::new();
        for delivery in deliveries {
            groups
                .entry((delivery.destination_id, delivery.group_key.clone()))
                .or_default()
                .push(delivery);
        }
        for ((destination_id, _group_key), deliveries) in groups {
            let mut sent_recent:i64=sqlx::query_scalar("SELECT count(DISTINCT telegram_message_id) FROM pilot_alert_deliveries WHERE destination_id=$1 AND status='sent' AND sent_at > now()-interval '1 minute'").bind(destination_id).fetch_one(&pool).await?;
            if sent_recent >= rate_limit {
                release_rate_limited(&pool, &deliveries).await?;
                continue;
            }
            for chunk in deliveries.chunks(group_size) {
                if sent_recent >= rate_limit {
                    release_rate_limited(&pool, chunk).await?;
                    continue;
                }
                let ids = chunk.iter().map(|v| v.delivery_id).collect::<Vec<_>>();
                let attempt = chunk.iter().map(|v| v.attempt_count).max().unwrap_or(0);
                let Some(token) = telegram_token(chunk[0].secret_ref.as_deref()) else {
                    mark_retry(
                        &pool,
                        &ids,
                        attempt,
                        max_attempts,
                        60,
                        "Telegram token environment variable is not configured",
                    )
                    .await?;
                    continue;
                };
                let mut parts = Vec::new();
                for item in chunk {
                    let link = format!(
                        "{}/app/tx-analysis?chain_id={}&tx_hash={}",
                        web_base.trim_end_matches('/'),
                        item.pack.chain_id,
                        item.pack.tx_hash
                    );
                    parts.push(format_telegram_alert(&item.project_name, &item.pack, &link));
                }
                let text = if parts.len() == 1 {
                    parts.remove(0)
                } else {
                    format!(
                        "LedgerFlow grouped alert ({} events)\n\n{}",
                        parts.len(),
                        parts.join("\n\n---\n\n")
                    )
                };
                match send(
                    &client,
                    &telegram_api_base,
                    &token,
                    &chunk[0].chat_id,
                    &truncate_telegram(&text),
                )
                .await
                {
                    Ok(message_id) => {
                        mark_sent(&pool, &ids, message_id).await?;
                        sent_recent += 1;
                        info!(destination_id=%destination_id,event_count=ids.len(),"pilot Telegram alert sent");
                    }
                    Err(SendError::RateLimited(seconds)) => {
                        mark_retry(
                            &pool,
                            &ids,
                            attempt,
                            max_attempts,
                            seconds,
                            "Telegram rate limited delivery",
                        )
                        .await?
                    }
                    Err(SendError::Other(message)) => {
                        warn!(destination_id=%destination_id,error=%message,"pilot Telegram delivery failed");
                        mark_retry(
                            &pool,
                            &ids,
                            attempt,
                            max_attempts,
                            retry_delay_seconds(attempt.max(0) as u32),
                            &message,
                        )
                        .await?;
                    }
                }
            }
        }
        sleep(Duration::from_millis(interval_ms.max(250))).await;
    }
}

async fn claim_due(pool: &PgPool, limit: i64) -> Result<Vec<ClaimedDelivery>, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let ids:Vec<Uuid>=sqlx::query_scalar("SELECT delivery_id FROM pilot_alert_deliveries WHERE status IN ('queued','retry') AND next_attempt_at<=now() ORDER BY next_attempt_at,created_at LIMIT $1 FOR UPDATE SKIP LOCKED").bind(limit).fetch_all(&mut *tx).await?;
    if ids.is_empty() {
        tx.commit().await?;
        return Ok(Vec::new());
    }
    sqlx::query("UPDATE pilot_alert_deliveries SET status='sending',claimed_at=now(),updated_at=now() WHERE delivery_id=ANY($1)").bind(&ids).execute(&mut *tx).await?;
    tx.commit().await?;
    let rows=sqlx::query("SELECT d.delivery_id,d.destination_id,d.group_key,d.attempt_count,t.chat_id,t.secret_ref,p.name AS project_name,e.evidence_pack FROM pilot_alert_deliveries d JOIN pilot_telegram_destinations t ON t.destination_id=d.destination_id JOIN pilot_events e ON e.event_id=d.event_id JOIN pilot_projects p ON p.project_id=e.project_id WHERE d.delivery_id=ANY($1) ORDER BY d.destination_id,d.group_key,e.created_at,e.event_id").bind(&ids).fetch_all(pool).await?;
    rows.into_iter()
        .map(|row| {
            let Json(value): Json<Value> = row.try_get("evidence_pack")?;
            let pack = serde_json::from_value(value)
                .map_err(|error| sqlx::Error::Decode(Box::new(error)))?;
            Ok(ClaimedDelivery {
                delivery_id: row.try_get("delivery_id")?,
                destination_id: row.try_get("destination_id")?,
                group_key: row
                    .try_get::<Option<String>, _>("group_key")?
                    .unwrap_or_default(),
                attempt_count: row.try_get("attempt_count")?,
                chat_id: row.try_get("chat_id")?,
                secret_ref: row.try_get("secret_ref")?,
                project_name: row.try_get("project_name")?,
                pack,
            })
        })
        .collect()
}

async fn recover_stale_claims(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE pilot_alert_deliveries SET status='retry',next_attempt_at=now(),last_error='recovered stale sending claim',updated_at=now() WHERE status='sending' AND claimed_at<now()-interval '5 minutes'").execute(pool).await?;
    Ok(())
}
async fn release_rate_limited(pool: &PgPool, items: &[ClaimedDelivery]) -> Result<(), sqlx::Error> {
    let ids = items.iter().map(|v| v.delivery_id).collect::<Vec<_>>();
    sqlx::query("UPDATE pilot_alert_deliveries SET status='retry',next_attempt_at=now()+interval '60 seconds',last_error='local destination rate limit',updated_at=now() WHERE delivery_id=ANY($1)").bind(ids).execute(pool).await?;
    Ok(())
}
async fn mark_sent(pool: &PgPool, ids: &[Uuid], message_id: i64) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE pilot_alert_deliveries SET status='sent',sent_at=now(),telegram_message_id=$2,attempt_count=attempt_count+1,last_error=null,updated_at=now() WHERE delivery_id=ANY($1)").bind(ids).bind(message_id.to_string()).execute(pool).await?;
    Ok(())
}
async fn mark_retry(
    pool: &PgPool,
    ids: &[Uuid],
    attempt: i32,
    max_attempts: i32,
    delay: u64,
    error: &str,
) -> Result<(), sqlx::Error> {
    let status = if attempt + 1 >= max_attempts {
        "dead_letter"
    } else {
        "retry"
    };
    sqlx::query("UPDATE pilot_alert_deliveries SET status=$2,attempt_count=attempt_count+1,next_attempt_at=now()+($3||' seconds')::interval,last_error=$4,updated_at=now() WHERE delivery_id=ANY($1)").bind(ids).bind(status).bind(delay.to_string()).bind(error).execute(pool).await?;
    Ok(())
}

enum SendError {
    RateLimited(u64),
    Other(String),
}
async fn send(
    client: &Client,
    api_base: &str,
    token: &str,
    chat_id: &str,
    text: &str,
) -> Result<i64, SendError> {
    let response = client
        .post(format!(
            "{}/bot{token}/sendMessage",
            api_base.trim_end_matches('/')
        ))
        .json(&serde_json::json!({"chat_id":chat_id,"text":text,"disable_web_page_preview":true}))
        .send()
        .await
        .map_err(|e| SendError::Other(e.to_string()))?;
    let status = response.status();
    let body: Value = response
        .json()
        .await
        .map_err(|e| SendError::Other(e.to_string()))?;
    if status == StatusCode::TOO_MANY_REQUESTS {
        return Err(SendError::RateLimited(
            body.pointer("/parameters/retry_after")
                .and_then(Value::as_u64)
                .unwrap_or(60),
        ));
    }
    if !status.is_success() {
        return Err(SendError::Other(format!(
            "Telegram HTTP {status}: {}",
            body.get("description")
                .and_then(Value::as_str)
                .unwrap_or("unknown error")
        )));
    }
    body.pointer("/result/message_id")
        .and_then(Value::as_i64)
        .ok_or_else(|| SendError::Other("Telegram response omitted message_id".into()))
}
fn telegram_token(secret_ref: Option<&str>) -> Option<String> {
    secret_ref
        .and_then(|name| env::var(name).ok())
        .or_else(|| env::var("LEDGERFLOW_TELEGRAM_BOT_TOKEN").ok())
        .or_else(|| env::var("TELEGRAM_BOT_TOKEN").ok())
        .filter(|v| !v.trim().is_empty())
}
fn truncate_telegram(text: &str) -> String {
    if text.chars().count() <= 4096 {
        return text.to_owned();
    }
    text.chars().take(4050).collect::<String>() + "\n… grouped alert truncated"
}
fn env_u64(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Json, Router, http::StatusCode, routing::post};

    async fn start_server(router: Router) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        format!("http://{address}")
    }

    #[tokio::test]
    async fn telegram_success_returns_message_id() {
        let router = Router::new().route(
            "/bottest/sendMessage",
            post(|| async { Json(serde_json::json!({"ok":true,"result":{"message_id":42}})) }),
        );
        let base = start_server(router).await;
        let result = send(&Client::new(), &base, "test", "123", "hello").await;
        assert!(matches!(result, Ok(42)));
    }

    #[tokio::test]
    async fn telegram_429_honors_retry_after() {
        let router = Router::new().route(
            "/bottest/sendMessage",
            post(|| async {
                (
                    StatusCode::TOO_MANY_REQUESTS,
                    Json(serde_json::json!({
                        "ok":false,
                        "parameters":{"retry_after":17}
                    })),
                )
            }),
        );
        let base = start_server(router).await;
        let result = send(&Client::new(), &base, "test", "123", "hello").await;
        assert!(matches!(result, Err(SendError::RateLimited(17))));
    }
}
