use std::{env, sync::Arc, time::Duration};

use ledgerflow::{
    core::{config::AppConfig, env::load_dotenv, logging::init_logging},
    indexer::rpc::IndexerRpcClient,
    ingestion::rpc::HttpRpcClient,
    repositories::sqlx::SqlxAnalysisRepository,
    services::{
        protocol_launch_indexer::{ProtocolLaunchIndexerConfig, ProtocolLaunchIndexerService},
        token_metadata::TokenMetadataService,
    },
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

    let config = ProtocolLaunchIndexerConfig {
        chain_id: env_u64("LEDGERFLOW_PROTOCOL_LAUNCH_CHAIN_ID", 4663),
        worker_id_prefix: env::var("LEDGERFLOW_PROTOCOL_LAUNCH_WORKER_ID_PREFIX")
            .unwrap_or_else(|_| "protocol_launch_rpc".to_owned()),
        batch_size: env_u64("LEDGERFLOW_PROTOCOL_LAUNCH_BATCH_SIZE", 5_000).max(1),
        head_lag_blocks: env_u64("LEDGERFLOW_PROTOCOL_LAUNCH_HEAD_LAG_BLOCKS", 2),
        lookback_blocks: env_u64("LEDGERFLOW_PROTOCOL_LAUNCH_LOOKBACK_BLOCKS", 128).max(1),
        header_batch_size: env_usize("LEDGERFLOW_PROTOCOL_LAUNCH_HEADER_BATCH_SIZE", 100),
        metadata_fetch_limit: env_usize("LEDGERFLOW_PROTOCOL_LAUNCH_METADATA_FETCH_LIMIT", 8),
        metadata_fetch_timeout_ms: env_u64(
            "LEDGERFLOW_PROTOCOL_LAUNCH_METADATA_FETCH_TIMEOUT_MS",
            5_000,
        ),
        start_block_override: env_optional_u64("LEDGERFLOW_PROTOCOL_LAUNCH_START_BLOCK_OVERRIDE"),
    };
    let poll = Duration::from_millis(env_u64(
        "LEDGERFLOW_PROTOCOL_LAUNCH_POLL_INTERVAL_MS",
        2_000,
    ));
    let active_delay =
        Duration::from_millis(env_u64("LEDGERFLOW_PROTOCOL_LAUNCH_ACTIVE_DELAY_MS", 250));
    let error_delay = Duration::from_millis(env_u64(
        "LEDGERFLOW_PROTOCOL_LAUNCH_ERROR_INTERVAL_MS",
        10_000,
    ));
    let run_once = env_bool("LEDGERFLOW_PROTOCOL_LAUNCH_RUN_ONCE", false);
    let rpc_url = env::var("LEDGERFLOW_PROTOCOL_LAUNCH_RPC_URL").unwrap_or(app.rpc_url);
    let token_metadata = TokenMetadataService::new(
        repository.pool().clone(),
        Arc::new(HttpRpcClient::new(rpc_url.clone())),
    );
    let service = ProtocolLaunchIndexerService::new(
        repository.pool().clone(),
        IndexerRpcClient::new(rpc_url),
        token_metadata,
        config.clone(),
    );

    info!(
        chain_id = config.chain_id,
        batch_size = config.batch_size,
        head_lag_blocks = config.head_lag_blocks,
        "LedgerFlow protocol launch indexer started"
    );
    loop {
        let delay = match service.process_once().await {
            Ok(iteration) => {
                info!(
                    chain_head = iteration.chain_head,
                    safe_head = iteration.safe_head,
                    domains = iteration.domains_processed,
                    ranges = iteration.ranges_processed,
                    logs = iteration.logs_observed,
                    curve_trade_logs = iteration.curve_trade_logs_observed,
                    curve_trades = iteration.curve_trades_projected,
                    launches = iteration.launches_projected,
                    graduations = iteration.graduations_projected,
                    token_metadata_ready = iteration.token_metadata_ready,
                    caught_up_domains = iteration.caught_up_domains,
                    "protocol launch ranges processed"
                );
                if iteration.domains_processed == 0
                    || iteration.caught_up_domains == iteration.domains_processed
                {
                    poll
                } else {
                    active_delay
                }
            }
            Err(error) => {
                error!(error = %error, "protocol launch indexing iteration failed");
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

fn env_optional_u64(name: &str) -> Option<u64> {
    env::var(name).ok().and_then(|value| value.parse().ok())
}

fn env_bool(name: &str, default: bool) -> bool {
    env::var(name)
        .map(|value| matches!(value.as_str(), "true" | "1" | "yes"))
        .unwrap_or(default)
}
