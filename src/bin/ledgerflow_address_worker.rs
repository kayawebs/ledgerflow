use std::{sync::Arc, time::Duration};

use ledgerflow::{
    core::{config::AppConfig, env::load_dotenv, logging::init_logging},
    ingestion::rpc::HttpRpcClient,
    repositories::sqlx::SqlxAnalysisRepository,
    services::address_intelligence::AddressIntelligenceService,
};
use tracing::{error, info};

const IDLE_POLL_INTERVAL: Duration = Duration::from_secs(5);
const ERROR_POLL_INTERVAL: Duration = Duration::from_secs(15);

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

    let rpc_client = Arc::new(HttpRpcClient::new(config.rpc_url.clone()));
    let service = AddressIntelligenceService::new(Arc::new(repository), rpc_client);

    info!("address intelligence worker started");
    loop {
        match service.process_next_job().await {
            Ok(Some(profile)) => {
                info!(
                    chain_id = profile.chain_id,
                    address = %profile.address,
                    address_type = %profile.address_type,
                    bytecode_length = ?profile.bytecode_length,
                    "address analysis completed"
                );
            }
            Ok(None) => tokio::time::sleep(IDLE_POLL_INTERVAL).await,
            Err(err) => {
                error!(error = %err, "address analysis worker iteration failed");
                tokio::time::sleep(ERROR_POLL_INTERVAL).await;
            }
        }
    }
}
