use std::sync::Arc;

use ledgerflow::{
    api,
    core::{config::AppConfig, logging::init_logging},
    ingestion::{fetchers::TransactionBundleFetcher, rpc::HttpRpcClient},
    plugins::loader::load_plugins,
    recognition::registry::RecognizerRegistry,
    repositories::sqlx::SqlxAnalysisRepository,
    services::analysis::AnalysisService,
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

    let listener = tokio::net::TcpListener::bind(&config.bind_addr)
        .await
        .expect("bind should succeed");
    axum::serve(listener, api::router(service))
        .await
        .expect("server should run");
}
