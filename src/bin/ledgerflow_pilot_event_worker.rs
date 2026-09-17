use std::env;

use ledgerflow::{
    core::logging::init_logging,
    ingestion::rpc::HttpRpcClient,
    pilot::{
        PilotEventType,
        classifier::{ClassificationContext, classify, failure_window_minutes},
        load_yaml,
        profit::verify_deterministic_profit,
        repository::{
            failure_count, load_enabled_configs, load_unprocessed_analyses, mark_failed,
            save_classification, sync_config,
        },
    },
};
use sqlx::postgres::PgPoolOptions;
use tokio::time::{Duration, sleep};
use tracing::{error, info, warn};

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
    if let Ok(path) = env::var("LEDGERFLOW_PILOT_CONFIG") {
        let config = load_yaml(path)?;
        sync_config(&pool, &config).await?;
    }
    let interval_ms = env::var("LEDGERFLOW_PILOT_POLL_INTERVAL_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5_000);
    let batch_size = env::var("LEDGERFLOW_PILOT_BATCH_SIZE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100_i64);
    let rpc = env::var("LEDGERFLOW_RPC_URL")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map(HttpRpcClient::new);
    let once = env::var("LEDGERFLOW_PILOT_RUN_ONCE")
        .ok()
        .is_some_and(|v| matches!(v.as_str(), "1" | "true" | "yes"));
    loop {
        for config in load_enabled_configs(&pool).await? {
            let analyses = load_unprocessed_analyses(&pool, &config, batch_size).await?;
            for mut analysis in analyses {
                if let Some(rpc) = rpc.as_ref() {
                    match verify_deterministic_profit(rpc, &config, &analysis).await {
                        Ok(Some(profit)) => {
                            if let Some(facts) = analysis
                                .normalized_execution_facts
                                .derived_facts
                                .as_object_mut()
                            {
                                facts.insert("deterministic_profit".to_owned(), profit);
                            }
                        }
                        Ok(None) => {}
                        Err(err) => warn!(
                            project_id = config.project.project_id,
                            tx_hash = analysis.tx_hash,
                            error = %err,
                            "deterministic profit verification unavailable"
                        ),
                    }
                }
                let window = config
                    .rules
                    .iter()
                    .filter(|r| r.enabled && r.event_type == PilotEventType::RepeatedFailedAttempts)
                    .map(failure_window_minutes)
                    .max()
                    .unwrap_or(60);
                let context = match failure_count(
                    &pool,
                    &config.project.project_id,
                    &analysis,
                    window,
                )
                .await
                {
                    Ok(count) => ClassificationContext {
                        repeated_failure_count: count,
                    },
                    Err(err) => {
                        let _ = mark_failed(&pool, &config, &analysis, &err.to_string()).await;
                        error!(project_id=config.project.project_id,tx_hash=analysis.tx_hash,error=%err,"failure history query failed");
                        continue;
                    }
                };
                let events = classify(&config, &analysis, &context);
                if let Err(err) = save_classification(&pool, &config, &analysis, &events).await {
                    let _ = mark_failed(&pool, &config, &analysis, &err.to_string()).await;
                    error!(project_id=config.project.project_id,tx_hash=analysis.tx_hash,error=%err,"pilot classification persistence failed");
                } else {
                    info!(
                        project_id = config.project.project_id,
                        tx_hash = analysis.tx_hash,
                        event_count = events.len(),
                        "pilot analysis classified"
                    );
                }
            }
        }
        if once {
            break;
        }
        sleep(Duration::from_millis(interval_ms.max(250))).await;
    }
    Ok(())
}
