use std::{env, sync::Arc, time::Duration};

use ledgerflow::{
    core::{config::AppConfig, env::load_dotenv, logging::init_logging},
    ingestion::rpc::HttpRpcClient,
    repositories::sqlx::SqlxAnalysisRepository,
    services::registry_sync::RegistrySyncService,
};
use tracing::{error, info};

#[tokio::main]
async fn main() {
    load_dotenv();
    init_logging();
    let config = AppConfig::from_env();
    let poll_interval =
        Duration::from_millis(env_u64("LEDGERFLOW_REGISTRY_SYNC_POLL_INTERVAL_MS", 60_000));
    let max_endpoints = env_usize("LEDGERFLOW_REGISTRY_SYNC_MAX_ENDPOINTS_PER_TICK", 32);
    let rpc_concurrency = env_usize("LEDGERFLOW_REGISTRY_SYNC_RPC_CONCURRENCY", 4);
    let run_once = env_bool("LEDGERFLOW_REGISTRY_SYNC_RUN_ONCE", false);

    let repository = SqlxAnalysisRepository::connect(&config.database_url)
        .await
        .expect("database connection should succeed");
    sqlx::migrate!("./migrations")
        .run(repository.pool())
        .await
        .expect("migrations should succeed");
    let rpc_client = Arc::new(HttpRpcClient::new(config.rpc_url));
    let service = RegistrySyncService::new(repository.pool().clone(), rpc_client, rpc_concurrency)
        .expect("registry sync service should initialize");

    info!(
        poll_interval_ms = poll_interval.as_millis(),
        rpc_concurrency, max_endpoints, run_once, "registry sync worker started"
    );
    loop {
        match service.sync_due_endpoints(max_endpoints).await {
            Ok(summary) if summary.claimed > 0 => {
                info!(
                    claimed = summary.claimed,
                    succeeded = summary.succeeded,
                    unchanged = summary.unchanged,
                    not_modified = summary.not_modified,
                    failed = summary.failed,
                    "registry sync tick finished"
                );
            }
            Ok(_) => {}
            Err(err) => error!(error = %err, "registry sync tick failed"),
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
