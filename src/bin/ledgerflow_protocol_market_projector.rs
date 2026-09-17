use std::{env, time::Duration};

use ledgerflow::{
    core::{config::AppConfig, env::load_dotenv, logging::init_logging},
    repositories::sqlx::SqlxAnalysisRepository,
    services::protocol_market_projector::{ProtocolMarketProjector, ProtocolMarketProjectorConfig},
};
use tracing::{error, info};

#[tokio::main]
async fn main() {
    load_dotenv();
    init_logging();
    let app = AppConfig::from_env();
    let repository = SqlxAnalysisRepository::connect(&app.database_url)
        .await
        .expect("database connection should succeed");
    sqlx::migrate!("./migrations")
        .run(repository.pool())
        .await
        .expect("migrations should succeed");

    let config = ProtocolMarketProjectorConfig {
        chain_id: env_u64("LEDGERFLOW_PROTOCOL_MARKET_CHAIN_ID", 4663),
        batch_size: env_usize("LEDGERFLOW_PROTOCOL_MARKET_BATCH_SIZE", 100),
        fact_scan_limit: env_i64("LEDGERFLOW_PROTOCOL_MARKET_FACT_SCAN_LIMIT", 10_000),
        history_days: env_i64("LEDGERFLOW_PROTOCOL_MARKET_HISTORY_DAYS", 30),
    };
    let poll = Duration::from_millis(env_u64(
        "LEDGERFLOW_PROTOCOL_MARKET_POLL_INTERVAL_MS",
        5_000,
    ));
    let active_delay =
        Duration::from_millis(env_u64("LEDGERFLOW_PROTOCOL_MARKET_ACTIVE_DELAY_MS", 250));
    let error_delay = Duration::from_millis(env_u64(
        "LEDGERFLOW_PROTOCOL_MARKET_ERROR_INTERVAL_MS",
        15_000,
    ));
    let run_once = env_bool("LEDGERFLOW_PROTOCOL_MARKET_RUN_ONCE", false);
    let service = ProtocolMarketProjector::new(repository.pool().clone(), config.clone());

    info!(
        chain_id = config.chain_id,
        batch_size = config.batch_size,
        fact_scan_limit = config.fact_scan_limit,
        history_days = config.history_days,
        "protocol market projector started"
    );
    loop {
        let delay = match service.project_once().await {
            Ok(summary) => {
                info!(
                    domains = summary.domains_considered,
                    launches = summary.launches_selected,
                    snapshots = summary.snapshots_projected,
                    participant_outcomes = summary.participant_outcomes_projected,
                    "protocol market projection completed"
                );
                if summary.launches_selected > 0 {
                    active_delay
                } else {
                    poll
                }
            }
            Err(error) => {
                error!(error = %error, "protocol market projection failed");
                error_delay
            }
        };
        if run_once {
            break;
        }
        tokio::time::sleep(delay.max(Duration::from_millis(25))).await;
    }
}

fn env_u64(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
        .max(1)
}

fn env_i64(name: &str, default: i64) -> i64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_bool(name: &str, default: bool) -> bool {
    env::var(name)
        .ok()
        .and_then(|value| match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Some(true),
            "0" | "false" | "no" | "off" => Some(false),
            _ => None,
        })
        .unwrap_or(default)
}
