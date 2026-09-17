use std::{env, sync::Arc, time::Duration};

use ledgerflow::{
    core::{config::AppConfig, env::load_dotenv, logging::init_logging},
    domain::interfaces::ClusterTxSource,
    ingestion::{fetchers::TransactionBundleFetcher, rpc::HttpRpcClient},
    plugins::loader::load_plugins,
    recognition::registry::RecognizerRegistry,
    repositories::sqlx::SqlxAnalysisRepository,
    services::{
        address_history_source::{AddressHistorySource, BlockscoutAddressHistorySource},
        analysis::AnalysisService,
        indexer_client::IndexerHttpClient,
        observation_intelligence::{ObservationIntelligenceConfig, ObservationIntelligenceService},
        pricing::PriceService,
        token_metadata::TokenMetadataService,
    },
};
use tracing::{error, info};

#[tokio::main]
async fn main() {
    load_dotenv();
    init_logging();
    let app = AppConfig::from_env();
    let indexer_url = app
        .indexer_api_base_url
        .clone()
        .expect("observation worker requires LEDGERFLOW_INDEXER_API_BASE_URL");
    let poll = Duration::from_millis(env_u64(
        "LEDGERFLOW_OBSERVATION_WORKER_POLL_INTERVAL_MS",
        2_000,
    ));
    let run_once = env_bool("LEDGERFLOW_OBSERVATION_WORKER_RUN_ONCE", false);
    let config = ObservationIntelligenceConfig {
        chain_id: app.default_chain_id,
        worker_id: env::var("LEDGERFLOW_OBSERVATION_WORKER_ID")
            .unwrap_or_else(|_| format!("observation_{}", app.default_chain_id)),
        maximum_transactions: env_usize("LEDGERFLOW_OBSERVATION_MAX_TRANSACTIONS", 10_000)
            .clamp(1, 10_000),
        analysis_batch_size: env_usize("LEDGERFLOW_OBSERVATION_ANALYSIS_BATCH_SIZE", 32)
            .clamp(1, 32),
        analysis_concurrency: env_usize("LEDGERFLOW_OBSERVATION_ANALYSIS_CONCURRENCY", 8)
            .clamp(1, 16),
        history_days: env_i64("LEDGERFLOW_OBSERVATION_HISTORY_DAYS", 30).clamp(1, 30),
        log_chunk_blocks: env_u64("LEDGERFLOW_OBSERVATION_LOG_CHUNK_BLOCKS", 50_000)
            .clamp(500, 200_000),
        discovery_chunk_budget: env_usize(
            "LEDGERFLOW_OBSERVATION_DISCOVERY_CHUNKS_PER_ITERATION",
            64,
        )
        .clamp(1, 512),
        maximum_attempts: env_i64("LEDGERFLOW_OBSERVATION_MAX_ATTEMPTS", 3).clamp(1, 10) as i32,
        stale_claim_seconds: env_i64("LEDGERFLOW_OBSERVATION_STALE_CLAIM_SECONDS", 120).max(60),
        incremental_lag_blocks: env_u64("LEDGERFLOW_OBSERVATION_INCREMENTAL_LAG_BLOCKS", 2).max(1),
    };

    let mut registry = RecognizerRegistry::new();
    let plugins = load_plugins(&app.plugins, &mut registry);
    let repository = SqlxAnalysisRepository::connect(&app.database_url)
        .await
        .expect("database connection should succeed");
    sqlx::migrate!("./migrations")
        .run(repository.pool())
        .await
        .expect("migrations should succeed");
    let pool = repository.pool().clone();
    let rpc = Arc::new(HttpRpcClient::new(app.rpc_url.clone()));
    let token_metadata = Arc::new(TokenMetadataService::new(pool.clone(), rpc.clone()));
    let pricing = Arc::new(PriceService::new(pool.clone()).with_rpc_client(rpc.clone()));
    let analysis = Arc::new(
        AnalysisService::new(
            TransactionBundleFetcher::new(rpc.clone(), app.trace_enabled),
            registry,
            plugins,
            Arc::new(repository),
            app.enabled_recognizers.clone(),
        )
        .with_token_metadata_service(token_metadata)
        .with_price_service(pricing),
    );
    let tx_source = Arc::new(
        IndexerHttpClient::new(indexer_url, app.indexer_api_bearer_token.clone()).known_only(),
    ) as Arc<dyn ClusterTxSource>;
    let mut service =
        ObservationIntelligenceService::new(pool, rpc, analysis, tx_source, config.clone());
    if let Ok(base_url) = env::var("LEDGERFLOW_OBSERVATION_HISTORY_API_BASE_URL")
        && !base_url.trim().is_empty()
    {
        let source = Arc::new(BlockscoutAddressHistorySource::new(
            base_url,
            env_usize("LEDGERFLOW_OBSERVATION_HISTORY_API_PAGE_SIZE", 1_000),
            env_usize("LEDGERFLOW_OBSERVATION_HISTORY_API_PAGES_PER_ITERATION", 1),
            env_bool(
                "LEDGERFLOW_OBSERVATION_HISTORY_API_INCLUDE_INTERNAL_TRANSACTIONS",
                false,
            ),
        )) as Arc<dyn AddressHistorySource>;
        service = service.with_address_history_source(source);
    }
    service
        .validate_rpc_chain()
        .await
        .expect("observation worker RPC chain must match configured chain");
    info!(
        chain_id = config.chain_id,
        worker_id = config.worker_id,
        maximum_transactions = config.maximum_transactions,
        analysis_batch_size = config.analysis_batch_size,
        analysis_concurrency = config.analysis_concurrency,
        discovery_chunk_budget = config.discovery_chunk_budget,
        history_days = config.history_days,
        "observed address intelligence worker started"
    );
    loop {
        match service.process_once().await {
            Ok(iteration) if iteration.processed => info!(
                chain_id = iteration.chain_id,
                address = iteration.address,
                stage = iteration.stage,
                "observation stage processed"
            ),
            Ok(_) => {}
            Err(error) => error!(error = %error, "observation iteration failed"),
        }
        if run_once {
            break;
        }
        tokio::time::sleep(poll.max(Duration::from_millis(500))).await;
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

fn env_usize(name: &str, default: usize) -> usize {
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
