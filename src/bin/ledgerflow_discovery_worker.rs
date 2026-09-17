use std::{env, sync::Arc, time::Duration};

use ledgerflow::{
    core::{config::AppConfig, env::load_dotenv, logging::init_logging},
    indexer::{clickhouse::ClickHouseClient, config::IndexerConfig},
    ingestion::rpc::HttpRpcClient,
    repositories::sqlx::SqlxAnalysisRepository,
    services::contract_discovery::{ContractDiscoveryConfig, ContractDiscoveryService},
};
use tracing::{error, info};

#[tokio::main]
async fn main() {
    load_dotenv();
    init_logging();
    let app_config = AppConfig::from_env();
    let indexer_config = IndexerConfig::from_env();
    let poll_interval =
        Duration::from_millis(env_u64("LEDGERFLOW_DISCOVERY_POLL_INTERVAL_MS", 5_000));
    let error_interval =
        Duration::from_millis(env_u64("LEDGERFLOW_DISCOVERY_ERROR_INTERVAL_MS", 10_000));
    let run_once = env_bool("LEDGERFLOW_DISCOVERY_RUN_ONCE", false);
    let discovery_config = ContractDiscoveryConfig {
        chain_id: indexer_config.chain_id,
        worker_id: env::var("LEDGERFLOW_DISCOVERY_WORKER_ID")
            .unwrap_or_else(|_| "robinhood_contract_discovery".to_owned()),
        batch_size: env_u32("LEDGERFLOW_DISCOVERY_BATCH_SIZE", 500),
        lookback_blocks: env_u32("LEDGERFLOW_DISCOVERY_LOOKBACK_BLOCKS", 128),
        rpc_concurrency: env_usize("LEDGERFLOW_DISCOVERY_RPC_CONCURRENCY", 1),
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
    let rpc_client = Arc::new(HttpRpcClient::new(app_config.rpc_url));
    let service =
        ContractDiscoveryService::new(repository, clickhouse, rpc_client, discovery_config.clone())
            .await
            .expect("contract discovery service should initialize");

    info!(
        worker_id = discovery_config.worker_id,
        chain_id = discovery_config.chain_id,
        batch_size = discovery_config.batch_size,
        lookback_blocks = discovery_config.lookback_blocks,
        rpc_concurrency = discovery_config.rpc_concurrency,
        run_once,
        "contract discovery worker started"
    );
    loop {
        let mut sleep_for = Duration::from_millis(25);
        match service.process_once().await {
            Ok(iteration) => {
                if iteration.start_block.is_some() {
                    info!(
                        start_block = iteration.start_block,
                        end_block = iteration.end_block,
                        max_available_block = iteration.max_available_block,
                        observed_logs = iteration.observed_logs,
                        pool_events = iteration.pool_events,
                        validated_pools = iteration.validated_pools,
                        rejected_pools = iteration.rejected_pools,
                        multiplier_events = iteration.multiplier_events,
                        ui_transfer_events = iteration.ui_transfer_events,
                        queued_addresses = iteration.queued_addresses,
                        caught_up = iteration.caught_up,
                        "contract discovery range processed"
                    );
                }
                if iteration.caught_up || iteration.start_block.is_none() {
                    sleep_for = poll_interval.max(Duration::from_secs(1));
                }
            }
            Err(err) => {
                error!(error = %err, "contract discovery iteration failed");
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

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
        .max(1)
}

fn env_bool(name: &str, default: bool) -> bool {
    env::var(name)
        .map(|value| matches!(value.as_str(), "true" | "1" | "yes"))
        .unwrap_or(default)
}
