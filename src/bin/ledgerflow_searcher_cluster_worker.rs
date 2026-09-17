use std::{env, sync::Arc, time::Duration};

use ledgerflow::{
    core::{config::AppConfig, env::load_dotenv, logging::init_logging},
    domain::interfaces::ClusterTxSource,
    ingestion::{fetchers::TransactionBundleFetcher, rpc::HttpRpcClient},
    plugins::loader::load_plugins,
    recognition::registry::RecognizerRegistry,
    repositories::sqlx::SqlxAnalysisRepository,
    services::{
        analysis::AnalysisService,
        indexer_client::IndexerHttpClient,
        pricing::PriceService,
        searcher_cluster_intelligence::{
            SearcherClusterConfig, SearcherClusterIntelligenceService,
        },
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
        .expect("searcher cluster worker requires LEDGERFLOW_INDEXER_API_BASE_URL");
    let poll = Duration::from_millis(env_u64(
        "LEDGERFLOW_SEARCHER_CLUSTER_POLL_INTERVAL_MS",
        1_000,
    ));
    let run_once = env_bool("LEDGERFLOW_SEARCHER_CLUSTER_RUN_ONCE", false);
    let config = SearcherClusterConfig {
        chain_id: app.default_chain_id,
        worker_id: env::var("LEDGERFLOW_SEARCHER_CLUSTER_WORKER_ID")
            .unwrap_or_else(|_| format!("searcher_cluster_{}", app.default_chain_id)),
        execution_batch_size: env_usize("LEDGERFLOW_SEARCHER_CLUSTER_EXECUTION_BATCH_SIZE", 8)
            .clamp(1, 32),
        basic_reconstruction_batch_size: env_usize(
            "LEDGERFLOW_SEARCHER_CLUSTER_BASIC_BATCH_SIZE",
            128,
        )
        .clamp(16, 512),
        initial_deep_execution_sample: env_usize(
            "LEDGERFLOW_SEARCHER_CLUSTER_INITIAL_DEEP_SAMPLE",
            64,
        )
        .clamp(20, 2_000),
        maximum_relationships: env_usize("LEDGERFLOW_SEARCHER_CLUSTER_MAX_RELATIONSHIPS", 20)
            .clamp(5, 100),
        maximum_cluster_members: env_usize("LEDGERFLOW_SEARCHER_CLUSTER_MAX_MEMBERS", 32)
            .clamp(4, 100),
        history_days: env_i64("LEDGERFLOW_SEARCHER_CLUSTER_HISTORY_DAYS", 30).clamp(1, 30),
        stale_claim_seconds: env_i64("LEDGERFLOW_SEARCHER_CLUSTER_STALE_CLAIM_SECONDS", 180)
            .max(60),
        incremental_lag_blocks: env_u64("LEDGERFLOW_SEARCHER_CLUSTER_INCREMENTAL_LAG_BLOCKS", 2)
            .max(1),
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
    let source = Arc::new(
        IndexerHttpClient::new(indexer_url, app.indexer_api_bearer_token.clone()).known_only(),
    ) as Arc<dyn ClusterTxSource>;
    let service =
        SearcherClusterIntelligenceService::new(pool, rpc, analysis, source, config.clone());
    service
        .data_source_status(config.chain_id)
        .await
        .expect("searcher cluster worker data source must match configured chain");
    if let Ok(seed_address) = env::var("LEDGERFLOW_SEARCHER_CLUSTER_SEED_ADDRESS")
        && !seed_address.trim().is_empty()
    {
        service
            .enqueue(config.chain_id, seed_address.trim())
            .await
            .expect("configured searcher cluster seed should enqueue");
    }
    info!(
        chain_id = config.chain_id,
        worker_id = config.worker_id,
        execution_batch_size = config.execution_batch_size,
        basic_reconstruction_batch_size = config.basic_reconstruction_batch_size,
        maximum_relationships = config.maximum_relationships,
        maximum_cluster_members = config.maximum_cluster_members,
        history_days = config.history_days,
        "searcher cluster intelligence worker started"
    );
    loop {
        match service.process_once().await {
            Ok(iteration) if iteration.processed => info!(
                chain_id = iteration.chain_id,
                cluster_id = ?iteration.cluster_id,
                seed_address = ?iteration.seed_address,
                stage = ?iteration.stage,
                "searcher cluster stage processed"
            ),
            Ok(_) => {}
            Err(error) => error!(error = %error, "searcher cluster iteration failed"),
        }
        if run_once {
            break;
        }
        tokio::time::sleep(poll.max(Duration::from_millis(250))).await;
    }
}

fn env_u64(name: &str, fallback: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(fallback)
}

fn env_usize(name: &str, fallback: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(fallback)
}

fn env_i64(name: &str, fallback: i64) -> i64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(fallback)
}

fn env_bool(name: &str, fallback: bool) -> bool {
    env::var(name)
        .ok()
        .and_then(|value| match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Some(true),
            "0" | "false" | "no" | "off" => Some(false),
            _ => None,
        })
        .unwrap_or(fallback)
}
