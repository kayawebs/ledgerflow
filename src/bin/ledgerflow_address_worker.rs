use std::{sync::Arc, time::Duration};

use ledgerflow::{
    core::{config::AppConfig, env::load_dotenv, logging::init_logging},
    domain::interfaces::ClusterTxSource,
    ingestion::{fetchers::TransactionBundleFetcher, rpc::HttpRpcClient},
    plugins::loader::load_plugins,
    recognition::registry::RecognizerRegistry,
    repositories::sqlx::SqlxAnalysisRepository,
    services::{
        address_intelligence::AddressIntelligenceService, analysis::AnalysisService,
        indexer_client::IndexerHttpClient, pricing::PriceService,
        token_metadata::TokenMetadataService,
    },
};
use tracing::{error, info};

const IDLE_POLL_INTERVAL: Duration = Duration::from_secs(5);
const ERROR_POLL_INTERVAL: Duration = Duration::from_secs(15);

#[tokio::main]
async fn main() {
    load_dotenv();
    init_logging();
    let config = AppConfig::from_env();

    let mut registry = RecognizerRegistry::new();
    let plugin_context = load_plugins(&config.plugins, &mut registry);

    let repository = SqlxAnalysisRepository::connect(&config.database_url)
        .await
        .expect("database connection should succeed");
    sqlx::migrate!("./migrations")
        .run(repository.pool())
        .await
        .expect("migrations should succeed");
    let rpc_client = Arc::new(HttpRpcClient::new(config.rpc_url.clone()));
    let token_metadata_service = Arc::new(TokenMetadataService::new(
        repository.pool().clone(),
        rpc_client.clone(),
    ));
    let price_service =
        Arc::new(PriceService::new(repository.pool().clone()).with_rpc_client(rpc_client.clone()));
    let fetcher = TransactionBundleFetcher::new(rpc_client.clone(), config.trace_enabled);
    let repository = Arc::new(repository);
    let analysis_service = Arc::new(
        AnalysisService::new(
            fetcher,
            registry,
            plugin_context,
            repository.clone(),
            config.enabled_recognizers.clone(),
        )
        .with_token_metadata_service(token_metadata_service)
        .with_price_service(price_service.clone()),
    );
    let service = if let Some(indexer_url) = config.indexer_api_base_url.clone() {
        let tx_source = Arc::new(IndexerHttpClient::new(
            indexer_url,
            config.indexer_api_bearer_token.clone(),
        )) as Arc<dyn ClusterTxSource>;
        AddressIntelligenceService::new(repository, rpc_client)
            .with_portfolio_classification(
                price_service.clone(),
                config.address_whale_threshold_usd,
            )
            .with_native_asset(
                config.wrapped_native_token_address.clone(),
                config.native_token_symbol.clone(),
            )
            .with_history_backfill(
                analysis_service,
                tx_source,
                config.cluster_backfill_query_limit,
            )
    } else {
        AddressIntelligenceService::new(repository, rpc_client)
            .with_portfolio_classification(price_service, config.address_whale_threshold_usd)
            .with_native_asset(
                config.wrapped_native_token_address.clone(),
                config.native_token_symbol.clone(),
            )
    };

    info!(
        whale_threshold_usd = config.address_whale_threshold_usd,
        "address intelligence worker started"
    );
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
