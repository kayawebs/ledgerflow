mod admin;
mod core;
mod indexer;
mod public_api;

use std::sync::Arc;

use axum::Router;
use tokio::net::TcpListener;

use crate::admin::{AdminState, PostgresAdminStore};
use crate::core::logging::init_logging;
use crate::indexer::{IndexerConfig, IndexerService};

#[tokio::main]
async fn main() {
    init_logging();

    let config = IndexerConfig::from_env();
    let bind_addr = config.bind_addr.clone();
    let service = Arc::new(IndexerService::new(config.clone()));
    service
        .bootstrap()
        .await
        .expect("indexer bootstrap should succeed");

    let admin_store = PostgresAdminStore::connect(&config.admin_database_url)
        .await
        .expect("admin database connection should succeed");
    admin_store
        .bootstrap()
        .await
        .expect("admin database bootstrap should succeed");

    let worker = service.clone();
    tokio::spawn(async move {
        worker.run_worker_forever().await;
    });

    let admin_state = AdminState {
        store: Arc::new(admin_store),
        postmark_server_token: config.postmark_server_token.clone(),
        email_from: config.email_from.clone(),
        billing_treasury_address: config.billing_treasury_address.clone(),
        billing_min_confirmations: config.billing_min_confirmations,
    };

    let app = Router::new()
        .merge(public_api::router(service.clone()))
        .merge(indexer::legacy_api::router(service))
        .merge(admin::router(admin_state));

    let listener = TcpListener::bind(&bind_addr)
        .await
        .expect("indexer bind should succeed");
    axum::serve(listener, app)
        .await
        .expect("indexer server should run");
}
