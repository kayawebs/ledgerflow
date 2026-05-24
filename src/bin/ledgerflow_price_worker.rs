use std::{sync::Arc, time::Duration};

use ledgerflow::{
    core::{config::AppConfig, env::load_dotenv, logging::init_logging},
    ingestion::rpc::HttpRpcClient,
    repositories::sqlx::SqlxAnalysisRepository,
    services::{pricing::PriceService, seeds::seed_known_metadata},
};
use tracing::{error, info};

#[tokio::main]
async fn main() {
    load_dotenv();
    init_logging();
    let config = AppConfig::from_env();

    let repository = SqlxAnalysisRepository::connect(&config.database_url)
        .await
        .expect("database connection should succeed");
    sqlx::migrate!("./migrations")
        .run(repository.pool())
        .await
        .expect("migrations should succeed");
    let seed_summary = seed_known_metadata(repository.pool(), config.default_chain_id)
        .await
        .expect("seed metadata should succeed");

    let rpc_client = Arc::new(HttpRpcClient::new(config.rpc_url.clone()));
    let price_service = Arc::new(
        PriceService::new(repository.pool().clone()).with_rpc_client(rpc_client),
    );
    let poll_interval = Duration::from_millis(config.price_worker_poll_interval_ms.max(1_000));

    info!(
        poll_interval_ms = poll_interval.as_millis(),
        token_seed_rows = seed_summary.token_rows,
        label_seed_rows = seed_summary.label_rows,
        protocol_seed_rows = seed_summary.protocol_rows,
        "price worker started"
    );

    loop {
        match price_service.process_next_price_request().await {
            Ok(result) if result.processed => {
                info!(
                    request_id = ?result.request_id,
                    token_address = ?result.token_address,
                    completed = result.completed,
                    method = ?result.method,
                    "price request processed"
                );
            }
            Ok(_) => {
                tokio::time::sleep(poll_interval).await;
            }
            Err(err) => {
                error!(error = %err, "price worker iteration failed");
                tokio::time::sleep(poll_interval).await;
            }
        }
    }
}
