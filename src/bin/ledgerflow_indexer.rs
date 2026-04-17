use std::sync::Arc;

use ledgerflow::{
    core::logging::init_logging,
    indexer::{api, config::IndexerConfig, service::IndexerService},
};

#[tokio::main]
async fn main() {
    init_logging();
    let config = IndexerConfig::from_env();
    let bind_addr = config.bind_addr.clone();
    let service = Arc::new(IndexerService::new(config));
    service
        .bootstrap()
        .await
        .expect("indexer bootstrap should succeed");

    let worker = service.clone();
    tokio::spawn(async move {
        worker.run_worker_forever().await;
    });

    let listener = tokio::net::TcpListener::bind(&bind_addr)
        .await
        .expect("indexer bind should succeed");
    axum::serve(listener, api::router(service))
        .await
        .expect("indexer server should run");
}
