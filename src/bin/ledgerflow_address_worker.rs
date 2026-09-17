use std::{env, sync::Arc, time::Duration};

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
    let concurrency = env::var("LEDGERFLOW_ADDRESS_WORKER_CONCURRENCY")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(4)
        .clamp(1, 32);
    let maximum_job_priority = env::var("LEDGERFLOW_ADDRESS_WORKER_MAX_PRIORITY")
        .ok()
        .and_then(|value| value.parse::<i32>().ok())
        .unwrap_or(100)
        .clamp(0, 100);
    let history_lookback_blocks = env::var("LEDGERFLOW_ADDRESS_HISTORY_LOOKBACK_BLOCKS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(50_000)
        .max(1);
    let history_analysis_limit = env::var("LEDGERFLOW_ADDRESS_HISTORY_ANALYSIS_LIMIT")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(32)
        .clamp(1, 256);

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
        .with_token_metadata_service(token_metadata_service.clone())
        .with_price_service(price_service.clone()),
    );
    let service = if let Some(indexer_url) = config.indexer_api_base_url.clone() {
        let tx_source = Arc::new(IndexerHttpClient::new(
            indexer_url,
            config.indexer_api_bearer_token.clone(),
        )) as Arc<dyn ClusterTxSource>;
        AddressIntelligenceService::new(repository, rpc_client)
            .with_worker_chain_id(config.default_chain_id)
            .with_maximum_job_priority(maximum_job_priority)
            .with_token_metadata_service(token_metadata_service.clone())
            .with_portfolio_classification(
                price_service.clone(),
                config.address_whale_threshold_usd,
            )
            .with_native_asset(config.native_token_symbol.clone())
            .with_history_backfill(
                analysis_service,
                tx_source,
                config.cluster_backfill_query_limit,
            )
    } else {
        AddressIntelligenceService::new(repository, rpc_client)
            .with_worker_chain_id(config.default_chain_id)
            .with_maximum_job_priority(maximum_job_priority)
            .with_token_metadata_service(token_metadata_service)
            .with_portfolio_classification(price_service, config.address_whale_threshold_usd)
            .with_native_asset(config.native_token_symbol.clone())
            .with_transaction_analysis(analysis_service)
    }
    .with_history_limits(history_lookback_blocks, history_analysis_limit);

    info!(
        whale_threshold_usd = config.address_whale_threshold_usd,
        concurrency,
        maximum_job_priority,
        history_lookback_blocks,
        history_analysis_limit,
        "address intelligence worker started"
    );
    let mut workers = tokio::task::JoinSet::new();
    loop {
        while workers.len() < concurrency {
            let worker_slot = workers.len();
            workers.spawn(run_worker(service.clone(), worker_slot));
        }
        if let Some(result) = workers.join_next().await {
            error!(result = ?result, "address worker task exited; restarting slot");
            tokio::time::sleep(ERROR_POLL_INTERVAL).await;
        }
    }
}

async fn run_worker(service: AddressIntelligenceService, worker_slot: usize) {
    loop {
        match service.process_next_job().await {
            Ok(Some(profile)) => {
                info!(
                    worker_slot,
                    chain_id = profile.chain_id,
                    address = %profile.address,
                    address_type = %profile.address_type,
                    bytecode_length = ?profile.bytecode_length,
                    "address analysis completed"
                );
            }
            Ok(None) => tokio::time::sleep(IDLE_POLL_INTERVAL).await,
            Err(err) => {
                error!(worker_slot, error = %err, "address analysis worker iteration failed");
                tokio::time::sleep(ERROR_POLL_INTERVAL).await;
            }
        }
    }
}
