use std::sync::Arc;

use ledgerflow::{
    api,
    core::{config::AppConfig, logging::init_logging},
    domain::interfaces::ClusterTxSource,
    ingestion::{fetchers::TransactionBundleFetcher, rpc::HttpRpcClient},
    plugins::loader::load_plugins,
    recognition::registry::RecognizerRegistry,
    repositories::sqlx::SqlxAnalysisRepository,
    services::{
        analysis::AnalysisService, cluster_backfill::ClusterBackfillService,
        indexer_client::IndexerHttpClient,
    },
};
use std::sync::Arc as StdArc;

#[tokio::main]
async fn main() {
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

    let rpc_client = StdArc::new(HttpRpcClient::new(config.rpc_url.clone()));
    let fetcher = TransactionBundleFetcher::new(rpc_client, config.trace_enabled);
    let service = Arc::new(AnalysisService::new(
        fetcher,
        registry,
        plugin_context,
        Arc::new(repository),
        config.enabled_recognizers.clone(),
    ));
    let tx_source = config.indexer_api_base_url.clone().map(|url| {
        Arc::new(IndexerHttpClient::new(
            url,
            config.indexer_api_bearer_token.clone(),
        )) as Arc<dyn ClusterTxSource>
    });
    let backfill_service = Arc::new(ClusterBackfillService::new(
        Arc::clone(&service),
        tx_source,
        config.cluster_backfill_query_limit,
    ));

    let listener = tokio::net::TcpListener::bind(&config.bind_addr)
        .await
        .expect("bind should succeed");
    axum::serve(listener, api::router(service, backfill_service))
        .await
        .expect("server should run");
}
