use std::{env, time::Duration};

use ledgerflow::{
    core::{config::AppConfig, env::load_dotenv, logging::init_logging},
    repositories::sqlx::SqlxAnalysisRepository,
    services::behavior_assertions::{BehaviorAssertionConfig, BehaviorAssertionService},
};
use tracing::{error, info};

#[tokio::main]
async fn main() {
    load_dotenv();
    init_logging();
    let app_config = AppConfig::from_env();
    let poll_interval = Duration::from_millis(env_u64(
        "LEDGERFLOW_BEHAVIOR_WORKER_POLL_INTERVAL_MS",
        60_000,
    ));
    let error_interval = Duration::from_millis(env_u64(
        "LEDGERFLOW_BEHAVIOR_WORKER_ERROR_INTERVAL_MS",
        15_000,
    ));
    let run_once = env_bool("LEDGERFLOW_BEHAVIOR_WORKER_RUN_ONCE", false);
    let config = BehaviorAssertionConfig {
        chain_id: env_u32("LEDGERFLOW_DEFAULT_CHAIN_ID", 4663),
        worker_id: env::var("LEDGERFLOW_BEHAVIOR_WORKER_ID")
            .unwrap_or_else(|_| "robinhood_behavior_assertions".to_owned()),
        source_worker_id: env::var("LEDGERFLOW_BEHAVIOR_SOURCE_WORKER_ID")
            .unwrap_or_else(|_| "robinhood_address_facts".to_owned()),
        rule_version: env::var("LEDGERFLOW_BEHAVIOR_RULE_VERSION")
            .unwrap_or_else(|_| "sender_frequency_v1".to_owned()),
        lookback_blocks: env_u32("LEDGERFLOW_BEHAVIOR_LOOKBACK_BLOCKS", 50_000),
        minimum_transaction_count: env_u64("LEDGERFLOW_BEHAVIOR_MIN_TRANSACTION_COUNT", 100),
        minimum_active_windows: env_u64("LEDGERFLOW_BEHAVIOR_MIN_ACTIVE_WINDOWS", 8),
        minimum_span_blocks: env_u32("LEDGERFLOW_BEHAVIOR_MIN_SPAN_BLOCKS", 1_000),
        minimum_transactions_per_1000_blocks: env_f64(
            "LEDGERFLOW_BEHAVIOR_MIN_TX_PER_1000_BLOCKS",
            2.0,
        ),
        maximum_candidates: env_u32("LEDGERFLOW_BEHAVIOR_MAX_CANDIDATES", 1_000),
        assertion_ttl_hours: env_i64("LEDGERFLOW_BEHAVIOR_ASSERTION_TTL_HOURS", 24),
    };

    let repository = SqlxAnalysisRepository::connect(&app_config.database_url)
        .await
        .expect("database connection should succeed");
    sqlx::migrate!("./migrations")
        .run(repository.pool())
        .await
        .expect("migrations should succeed");
    let service = BehaviorAssertionService::new(repository, config.clone());

    info!(
        worker_id = config.worker_id,
        source_worker_id = config.source_worker_id,
        chain_id = config.chain_id,
        rule_version = config.rule_version,
        lookback_blocks = config.lookback_blocks,
        minimum_transaction_count = config.minimum_transaction_count,
        minimum_active_windows = config.minimum_active_windows,
        minimum_span_blocks = config.minimum_span_blocks,
        minimum_transactions_per_1000_blocks = config.minimum_transactions_per_1000_blocks,
        assertion_ttl_hours = config.assertion_ttl_hours,
        run_once,
        "behavior assertion worker started"
    );
    loop {
        match service.process_once().await {
            Ok(iteration) => {
                info!(
                    window_start_block = iteration.window_start_block,
                    window_end_block = iteration.window_end_block,
                    evaluated_addresses = iteration.evaluated_addresses,
                    candidate_addresses = iteration.candidate_addresses,
                    assertions_written = iteration.assertions_written,
                    reviews_queued = iteration.reviews_queued,
                    "behavior assertion window processed"
                );
            }
            Err(error) => {
                error!(error = %error, "behavior assertion iteration failed");
                if run_once {
                    std::process::exit(1);
                }
                tokio::time::sleep(error_interval.max(Duration::from_secs(1))).await;
                continue;
            }
        }
        if run_once {
            break;
        }
        tokio::time::sleep(poll_interval.max(Duration::from_secs(1))).await;
    }
}

fn env_u64(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_u32(name: &str, default: u32) -> u32 {
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
        .max(1)
}

fn env_f64(name: &str, default: f64) -> f64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value: &f64| value.is_finite() && *value >= 0.0)
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
