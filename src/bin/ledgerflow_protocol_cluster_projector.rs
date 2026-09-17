use std::{env, time::Duration};

use ledgerflow::{
    core::{config::AppConfig, env::load_dotenv, logging::init_logging},
    repositories::sqlx::SqlxAnalysisRepository,
    services::protocol_cluster_projector::{
        ProtocolClusterProjector, ProtocolClusterProjectorConfig,
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
    let config = ProtocolClusterProjectorConfig {
        chain_id: env_u64("LEDGERFLOW_PROTOCOL_CLUSTER_CHAIN_ID", 4663),
        minimum_repeated_launches: env_i64("LEDGERFLOW_PROTOCOL_CLUSTER_MIN_REPEATED_LAUNCHES", 2)
            .max(2),
        minimum_verified_executions: env_i64(
            "LEDGERFLOW_PROTOCOL_CLUSTER_MIN_VERIFIED_EXECUTIONS",
            2,
        )
        .max(2),
    };
    let poll = Duration::from_millis(env_u64(
        "LEDGERFLOW_PROTOCOL_CLUSTER_POLL_INTERVAL_MS",
        60_000,
    ));
    let error_delay = Duration::from_millis(env_u64(
        "LEDGERFLOW_PROTOCOL_CLUSTER_ERROR_INTERVAL_MS",
        15_000,
    ));
    let run_once = env_bool("LEDGERFLOW_PROTOCOL_CLUSTER_RUN_ONCE", false);
    let service = ProtocolClusterProjector::new(repository.pool().clone(), config.clone());
    info!(
        chain_id = config.chain_id,
        minimum_repeated_launches = config.minimum_repeated_launches,
        minimum_verified_executions = config.minimum_verified_executions,
        "LedgerFlow protocol cluster projector started"
    );
    loop {
        let delay = match service.project_once().await {
            Ok(summary) => {
                info!(
                    launch_domains = summary.launch_domains,
                    changed_launch_creators = summary.changed_launch_creators,
                    issuer_clusters = summary.issuer_clusters_projected,
                    repeat_buyers = summary.repeat_buyer_members_projected,
                    changed_arbitrage_subjects = summary.changed_arbitrage_subjects,
                    arbitrage_clusters = summary.arbitrage_clusters_projected,
                    execution_members = summary.execution_members_projected,
                    "protocol-scoped clusters projected"
                );
                poll
            }
            Err(error) => {
                error!(error = %error, "protocol cluster projection failed");
                error_delay
            }
        };
        if run_once {
            break;
        }
        tokio::time::sleep(delay.max(Duration::from_millis(100))).await;
    }
}

fn env_u64(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_i64(name: &str, default: i64) -> i64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_bool(name: &str, default: bool) -> bool {
    env::var(name)
        .map(|value| matches!(value.as_str(), "true" | "1" | "yes"))
        .unwrap_or(default)
}
