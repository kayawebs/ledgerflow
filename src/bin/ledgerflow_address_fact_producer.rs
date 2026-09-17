use std::{env, time::Duration};

use ledgerflow::{
    core::{config::AppConfig, env::load_dotenv, logging::init_logging},
    indexer::{clickhouse::ClickHouseClient, config::IndexerConfig},
    repositories::sqlx::SqlxAnalysisRepository,
    services::address_fact_producer::{AddressFactProducerConfig, AddressFactProducerService},
};
use tracing::{error, info};

#[tokio::main]
async fn main() {
    load_dotenv();
    init_logging();
    let app_config = AppConfig::from_env();
    let indexer_config = IndexerConfig::from_env();
    let poll_interval = Duration::from_millis(env_u64(
        "LEDGERFLOW_ADDRESS_PRODUCER_POLL_INTERVAL_MS",
        5_000,
    ));
    let error_interval = Duration::from_millis(env_u64(
        "LEDGERFLOW_ADDRESS_PRODUCER_ERROR_INTERVAL_MS",
        10_000,
    ));
    let inter_batch_interval = Duration::from_millis(env_u64(
        "LEDGERFLOW_ADDRESS_PRODUCER_INTER_BATCH_DELAY_MS",
        25,
    ));
    let run_once = env_bool("LEDGERFLOW_ADDRESS_PRODUCER_RUN_ONCE", false);
    let producer_config = AddressFactProducerConfig {
        chain_id: indexer_config.chain_id,
        worker_id: env::var("LEDGERFLOW_ADDRESS_PRODUCER_WORKER_ID")
            .unwrap_or_else(|_| "robinhood_address_facts".to_owned()),
        batch_size: env_u32("LEDGERFLOW_ADDRESS_PRODUCER_BATCH_SIZE", 250),
        reorg_lookback_blocks: env_u32("LEDGERFLOW_ADDRESS_PRODUCER_REORG_LOOKBACK_BLOCKS", 128),
        maximum_enqueued_priority: env_i32("LEDGERFLOW_ADDRESS_PRODUCER_MAX_ENQUEUE_PRIORITY", 100)
            .clamp(0, 100),
    };

    let repository = SqlxAnalysisRepository::connect(&app_config.database_url)
        .await
        .expect("database connection should succeed");
    sqlx::migrate!("./migrations")
        .run(repository.pool())
        .await
        .expect("migrations should succeed");
    let clickhouse = ClickHouseClient::new(
        indexer_config.clickhouse_url,
        indexer_config.clickhouse_database,
        indexer_config.clickhouse_user,
        indexer_config.clickhouse_password,
    );
    let service = AddressFactProducerService::new(repository, clickhouse, producer_config.clone());

    info!(
        worker_id = producer_config.worker_id,
        chain_id = producer_config.chain_id,
        batch_size = producer_config.batch_size,
        reorg_lookback_blocks = producer_config.reorg_lookback_blocks,
        maximum_enqueued_priority = producer_config.maximum_enqueued_priority,
        inter_batch_delay_ms = inter_batch_interval.as_millis(),
        run_once,
        "address fact producer started"
    );
    loop {
        let mut sleep_for = inter_batch_interval;
        match service.process_once().await {
            Ok(iteration) => {
                if iteration.start_block.is_some() {
                    info!(
                        start_block = iteration.start_block,
                        end_block = iteration.end_block,
                        max_available_block = iteration.max_available_block,
                        observation_rows = iteration.observation_rows,
                        unique_addresses = iteration.unique_addresses,
                        queued_addresses = iteration.queued_addresses,
                        reorg_rewind = iteration.reorg_rewind,
                        caught_up = iteration.caught_up,
                        "canonical address facts processed"
                    );
                }
                if iteration.caught_up || iteration.start_block.is_none() {
                    sleep_for = poll_interval.max(Duration::from_secs(1));
                }
            }
            Err(error) => {
                error!(error = %error, "address fact producer iteration failed");
                sleep_for = error_interval.max(Duration::from_secs(1));
            }
        }
        if run_once {
            break;
        }
        tokio::time::sleep(sleep_for).await;
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

fn env_i32(name: &str, default: i32) -> i32 {
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
