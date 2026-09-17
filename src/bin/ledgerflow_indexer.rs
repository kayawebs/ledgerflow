use std::sync::Arc;

use ledgerflow::{
    core::{env::load_dotenv, logging::init_logging},
    indexer::{
        api,
        config::{IndexerConfig, IndexerWorkerMode},
        service::IndexerService,
    },
};

#[tokio::main]
async fn main() {
    load_dotenv();
    init_logging();
    let config = IndexerConfig::from_env();
    let bind_addr = config.bind_addr.clone();
    let api_enabled = config.api_enabled;
    let run_auxiliary_lazy_worker = config.lazy_enabled
        && config.lazy_worker_enabled
        && config.worker_mode != IndexerWorkerMode::Lazy;
    let service = Arc::new(IndexerService::new(config));
    service
        .bootstrap()
        .await
        .expect("indexer bootstrap should succeed");

    if run_auxiliary_lazy_worker {
        let lazy_worker = service.clone();
        tokio::spawn(async move {
            lazy_worker.run_lazy_worker_forever().await;
        });
    }

    let endpoint_backfill = service.clone();
    tokio::spawn(async move {
        endpoint_backfill
            .run_endpoint_backfill_until_caught_up()
            .await;
    });

    if !api_enabled {
        service.run_worker_forever().await;
        return;
    }

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
