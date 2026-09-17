use std::{env, time::Duration};

use reqwest::{Client, StatusCode};
use serde_json::Value;
use sqlx::{PgPool, Row, postgres::PgPoolOptions};
use tracing::{info, warn};
use uuid::Uuid;

use ledgerflow::{core::logging::init_logging, pilot::alerts::retry_delay_seconds};

#[derive(Debug)]
struct ObservationRoute {
    monitor_id: Uuid,
    workspace_id: Option<Uuid>,
    chain_id: u64,
    address: String,
    destination_id: Uuid,
    destination_ref: String,
    api_base_url: String,
}

#[derive(Debug)]
struct DueDelivery {
    delivery_id: Uuid,
    monitor_id: Uuid,
    event_id: String,
    destination_ref: String,
    attempt_count: i32,
    chain_id: u64,
    address: String,
    workspace_id: Option<Uuid>,
    signal: Value,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    dotenvy::dotenv().ok();
    init_logging();
    let database_url = env::var("LEDGERFLOW_DATABASE_URL").or_else(|_| env::var("DATABASE_URL"))?;
    let pool = PgPoolOptions::new()
        .max_connections(6)
        .connect(&database_url)
        .await?;
    sqlx::migrate!("./migrations").run(&pool).await?;
    let client = Client::new();
    let interval = Duration::from_millis(env_u64(
        "LEDGERFLOW_OBSERVATION_ALERT_POLL_INTERVAL_MS",
        5_000,
    ));
    let max_attempts = env_u64("LEDGERFLOW_OBSERVATION_ALERT_MAX_ATTEMPTS", 8).clamp(1, 32) as i32;
    let rate_limit =
        env_u64("LEDGERFLOW_OBSERVATION_ALERT_RATE_LIMIT_PER_MINUTE", 20).clamp(1, 60) as i64;
    let telegram_api_base = env::var("LEDGERFLOW_TELEGRAM_API_BASE_URL")
        .unwrap_or_else(|_| "https://api.telegram.org".to_owned());
    let web_base =
        env::var("LEDGERFLOW_WEB_BASE_URL").unwrap_or_else(|_| "http://127.0.0.1:3001".to_owned());
    info!("observation change notification worker started");
    loop {
        if let Err(error) = discover_deliveries(&pool, &client).await {
            warn!(error=%error, "observation delivery discovery failed");
        }
        if let Err(error) = deliver_due(
            &pool,
            &client,
            &telegram_api_base,
            &web_base,
            max_attempts,
            rate_limit,
        )
        .await
        {
            warn!(error=%error, "observation delivery attempt failed");
        }
        tokio::time::sleep(interval.max(Duration::from_millis(500))).await;
    }
}

async fn discover_deliveries(pool: &PgPool, client: &Client) -> Result<(), sqlx::Error> {
    let rows = sqlx::query(
        r#"
        select monitors.monitor_id, monitors.workspace_id, monitors.chain_id,
               lower(entities.address) as address,
               destinations.destination_id, destinations.destination_ref,
               registry.api_base_url
        from user_monitors monitors
        join user_monitor_entities entities on entities.monitor_id=monitors.monitor_id
        join user_monitor_destinations destinations on destinations.monitor_id=monitors.monitor_id
        join chain_registry registry on registry.chain_id=monitors.chain_id and registry.enabled
        where monitors.status='active' and destinations.enabled
          and destinations.destination_type='telegram'
          and destinations.destination_ref is not null
        order by monitors.updated_at,monitors.monitor_id
        "#,
    )
    .fetch_all(pool)
    .await?;
    for row in rows {
        let route = ObservationRoute {
            monitor_id: row.get("monitor_id"),
            workspace_id: row.get("workspace_id"),
            chain_id: row.get::<i64, _>("chain_id") as u64,
            address: row.get("address"),
            destination_id: row.get("destination_id"),
            destination_ref: row.get("destination_ref"),
            api_base_url: row.get("api_base_url"),
        };
        let url = format!(
            "{}/observations/{}?chain_id={}&timeline_limit=1",
            route.api_base_url.trim_end_matches('/'),
            route.address,
            route.chain_id
        );
        let response = match client.get(url).timeout(Duration::from_secs(3)).send().await {
            Ok(response) if response.status().is_success() => response,
            Ok(response) if response.status() == StatusCode::NOT_FOUND => continue,
            Ok(response) => {
                warn!(chain_id=route.chain_id,address=%route.address,status=%response.status(),"observation snapshot unavailable for notification discovery");
                continue;
            }
            Err(error) => {
                warn!(chain_id=route.chain_id,address=%route.address,error=%error,"observation snapshot request failed");
                continue;
            }
        };
        let snapshot: Value = match response.json().await {
            Ok(value) => value,
            Err(error) => {
                warn!(chain_id=route.chain_id,address=%route.address,error=%error,"invalid observation snapshot response");
                continue;
            }
        };
        for signal in snapshot
            .get("changes")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter(|signal| {
                signal.get("status").and_then(Value::as_str) == Some("detected")
                    && matches!(
                        signal.get("severity").and_then(Value::as_str),
                        Some("medium" | "high" | "critical")
                    )
            })
        {
            let Some(event_id) = signal.get("id").and_then(Value::as_str) else {
                continue;
            };
            sqlx::query(
                r#"
                insert into user_monitor_delivery_history (
                  monitor_id,event_id,destination_id,status,next_attempt_at,metadata
                ) values ($1,$2,$3,'pending',now(),$4)
                on conflict (monitor_id,event_id,destination_id) where event_id is not null and destination_id is not null
                do nothing
                "#,
            )
            .bind(route.monitor_id)
            .bind(event_id)
            .bind(route.destination_id)
            .bind(serde_json::json!({
                "source":"observed_address_change",
                "chain_id":route.chain_id,
                "address":route.address,
                "workspace_id":route.workspace_id,
                "destination_ref":route.destination_ref,
                "signal":signal,
            }))
            .execute(pool)
            .await?;
        }
    }
    Ok(())
}

async fn deliver_due(
    pool: &PgPool,
    client: &Client,
    telegram_api_base: &str,
    web_base: &str,
    max_attempts: i32,
    rate_limit: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "update user_monitor_delivery_history set status='retrying',claimed_at=null,next_attempt_at=now(),last_error='recovered stale delivery claim',updated_at=now() where metadata->>'source'='observed_address_change' and status in ('pending','retrying') and claimed_at<now()-interval '5 minutes'",
    )
    .execute(pool)
    .await?;
    let sent_recent: i64 = sqlx::query_scalar(
        "select count(*) from user_monitor_delivery_history where status='delivered' and delivered_at>now()-interval '1 minute'",
    )
    .fetch_one(pool)
    .await?;
    if sent_recent >= rate_limit {
        return Ok(());
    }
    let mut transaction = pool.begin().await?;
    let ids: Vec<Uuid> = sqlx::query_scalar(
        "select delivery_id from user_monitor_delivery_history where metadata->>'source'='observed_address_change' and status in ('pending','retrying') and next_attempt_at<=now() and claimed_at is null order by next_attempt_at,created_at limit $1 for update skip locked",
    )
    .bind((rate_limit - sent_recent).min(20))
    .fetch_all(&mut *transaction)
    .await?;
    if ids.is_empty() {
        transaction.commit().await?;
        return Ok(());
    }
    sqlx::query(
        "update user_monitor_delivery_history set claimed_at=now(),updated_at=now() where delivery_id=any($1)",
    )
    .bind(&ids)
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    let rows = sqlx::query(
        r#"
        select history.delivery_id,history.monitor_id,history.event_id,history.attempt_count,
               destinations.destination_ref,monitors.chain_id,monitors.workspace_id,
               lower(entities.address) as address,history.metadata->'signal' as signal
        from user_monitor_delivery_history history
        join user_monitors monitors on monitors.monitor_id=history.monitor_id
        join user_monitor_destinations destinations on destinations.destination_id=history.destination_id
        join lateral (
          select address from user_monitor_entities
          where monitor_id=history.monitor_id order by created_at limit 1
        ) entities on true
        where history.delivery_id=any($1)
        order by history.created_at
        "#,
    )
    .bind(&ids)
    .fetch_all(pool)
    .await?;
    for row in rows {
        let delivery = DueDelivery {
            delivery_id: row.get("delivery_id"),
            monitor_id: row.get("monitor_id"),
            event_id: row.get("event_id"),
            destination_ref: row.get("destination_ref"),
            attempt_count: row.get("attempt_count"),
            chain_id: row.get::<i64, _>("chain_id") as u64,
            address: row.get("address"),
            workspace_id: row.get("workspace_id"),
            signal: row.get("signal"),
        };
        let Some(token) = env::var("LEDGERFLOW_TELEGRAM_BOT_TOKEN")
            .ok()
            .filter(|value| !value.trim().is_empty())
        else {
            mark_retry(
                pool,
                &delivery,
                max_attempts,
                "Telegram bot token is not configured",
            )
            .await?;
            continue;
        };
        let workspace_query = delivery
            .workspace_id
            .map(|id| format!("?workspace=workspace:{id}"))
            .unwrap_or_default();
        let object_url = format!(
            "{}/app/observations/{}{}",
            web_base.trim_end_matches('/'),
            delivery.address,
            workspace_query
        );
        let text = format_signal(&delivery, &object_url);
        let response = client
            .post(format!(
                "{}/bot{}/sendMessage",
                telegram_api_base.trim_end_matches('/'),
                token.trim()
            ))
            .json(&serde_json::json!({
                "chat_id":delivery.destination_ref,
                "text":text,
                "disable_web_page_preview":true
            }))
            .send()
            .await;
        match response {
            Ok(response) if response.status().is_success() => {
                sqlx::query("update user_monitor_delivery_history set status='delivered',attempt_count=attempt_count+1,delivered_at=now(),claimed_at=null,last_error=null,updated_at=now() where delivery_id=$1")
                    .bind(delivery.delivery_id).execute(pool).await?;
                info!(delivery_id=%delivery.delivery_id,monitor_id=%delivery.monitor_id,event_id=%delivery.event_id,"observation Telegram change delivered");
            }
            Ok(response) => {
                let error = format!("Telegram returned HTTP {}", response.status());
                mark_retry(pool, &delivery, max_attempts, &error).await?;
            }
            Err(error) => mark_retry(pool, &delivery, max_attempts, &error.to_string()).await?,
        }
    }
    Ok(())
}

fn format_signal(delivery: &DueDelivery, object_url: &str) -> String {
    let kind = delivery
        .signal
        .get("signal_type")
        .and_then(Value::as_str)
        .unwrap_or("observed_change");
    let severity = delivery
        .signal
        .get("severity")
        .and_then(Value::as_str)
        .unwrap_or("medium");
    let conclusion = delivery
        .signal
        .get("conclusion")
        .and_then(Value::as_str)
        .unwrap_or("Evidence-backed change detected");
    let confidence = delivery
        .signal
        .get("confidence")
        .and_then(Value::as_f64)
        .map(|value| format!("{:.0}%", value * 100.0))
        .unwrap_or_else(|| "Unavailable".to_owned());
    format!(
        "LedgerFlow Observation Alert\n\nSeverity: {}\nChange: {}\nObject: {}\nNetwork: {}\nConclusion: {}\nConfidence: {}\n\n{}",
        severity.to_ascii_uppercase(),
        kind,
        delivery.address,
        delivery.chain_id,
        conclusion,
        confidence,
        object_url
    )
}

async fn mark_retry(
    pool: &PgPool,
    delivery: &DueDelivery,
    max_attempts: i32,
    error: &str,
) -> Result<(), sqlx::Error> {
    let next_attempt = delivery.attempt_count + 1;
    let status = if next_attempt >= max_attempts {
        "dead_letter"
    } else {
        "retrying"
    };
    let delay = retry_delay_seconds(delivery.attempt_count.max(0) as u32);
    sqlx::query(
        "update user_monitor_delivery_history set status=$2,attempt_count=attempt_count+1,next_attempt_at=now()+make_interval(secs=>$3),claimed_at=null,last_error=$4,updated_at=now() where delivery_id=$1",
    )
    .bind(delivery.delivery_id)
    .bind(status)
    .bind(delay as f64)
    .bind(error.chars().take(2_000).collect::<String>())
    .execute(pool)
    .await?;
    Ok(())
}

fn env_u64(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}
