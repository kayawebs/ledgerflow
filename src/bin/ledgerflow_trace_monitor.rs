use std::{sync::Arc, time::Duration};

use axum::{
    Json, Router,
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use ledgerflow::{
    core::errors::LedgerError,
    core::{config::AppConfig, logging::init_logging},
    indexer::rpc::IndexerRpcClient,
    ingestion::{fetchers::TransactionBundleFetcher, rpc::HttpRpcClient},
    repositories::sqlx::SqlxAnalysisRepository,
    services::trace_monitor::{TopAddressDeltasQuery, TraceMonitorConfig, TraceMonitorService},
};
use serde_json::json;
use tracing::{error, info};

#[tokio::main]
async fn main() {
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
    let block_rpc = Arc::new(IndexerRpcClient::new(config.rpc_url.clone()));
    let fetcher = TransactionBundleFetcher::new(rpc_client, true);
    let service = TraceMonitorService::new(
        repository,
        block_rpc,
        fetcher,
        TraceMonitorConfig {
            chain_id: config.default_chain_id,
            worker_id: config.trace_monitor_worker_id.clone(),
            start_block: config.trace_monitor_start_block,
            confirmations: config.trace_monitor_confirmations,
            block_batch_size: config.trace_monitor_block_batch_size,
            retention_days: config.trace_monitor_retention_days,
        },
    );
    let service = Arc::new(service);
    let poll_interval = Duration::from_millis(config.trace_monitor_poll_interval_ms.max(1000));
    let bind_addr = config.trace_monitor_bind_addr.clone();

    info!(
        worker_id = config.trace_monitor_worker_id,
        chain_id = config.default_chain_id,
        bind_addr,
        poll_interval_ms = poll_interval.as_millis(),
        block_batch_size = config.trace_monitor_block_batch_size,
        retention_days = config.trace_monitor_retention_days,
        "trace monitor started"
    );

    let http_service = Arc::clone(&service);
    tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(&bind_addr)
            .await
            .expect("trace monitor bind should succeed");
        axum::serve(listener, trace_monitor_router(http_service))
            .await
            .expect("trace monitor http server should run");
    });

    loop {
        if !config.trace_monitor_enabled {
            info!("trace monitor disabled");
            tokio::time::sleep(poll_interval).await;
            continue;
        }
        match service.process_once().await {
            Ok(iteration) if iteration.processed_blocks > 0 => {
                info!(
                    latest_block = iteration.latest_block,
                    target_block = iteration.target_block,
                    start_block = ?iteration.start_block,
                    end_block = ?iteration.end_block,
                    processed_blocks = iteration.processed_blocks,
                    processed_txs = iteration.processed_txs,
                    failed_txs = iteration.failed_txs,
                    address_delta_rows = iteration.address_delta_rows,
                    cluster_delta_rows = iteration.cluster_delta_rows,
                    "trace monitor iteration completed"
                );
            }
            Ok(iteration) => {
                info!(
                    latest_block = iteration.latest_block,
                    target_block = iteration.target_block,
                    "trace monitor caught up"
                );
                tokio::time::sleep(poll_interval).await;
            }
            Err(err) => {
                error!(error = %err, "trace monitor iteration failed");
                tokio::time::sleep(poll_interval).await;
            }
        }
    }
}

fn trace_monitor_router(service: Arc<TraceMonitorService>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/top-address-deltas", get(top_address_deltas))
        .with_state(service)
}

async fn health() -> &'static str {
    "ok"
}

async fn top_address_deltas(
    State(service): State<Arc<TraceMonitorService>>,
    Query(query): Query<TopAddressDeltasQuery>,
) -> Result<Json<serde_json::Value>, TraceMonitorApiError> {
    let rows = service.top_address_deltas(&query).await?;
    Ok(Json(json!({
        "start": query.start,
        "end": query.end,
        "top_n": query.top_n.unwrap_or(25).clamp(1, 200),
        "items": rows
    })))
}

struct TraceMonitorApiError(LedgerError);

impl From<LedgerError> for TraceMonitorApiError {
    fn from(value: LedgerError) -> Self {
        Self(value)
    }
}

impl IntoResponse for TraceMonitorApiError {
    fn into_response(self) -> Response {
        let status = match self.0 {
            LedgerError::MalformedEvidence(_) => StatusCode::BAD_REQUEST,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (
            status,
            Json(json!({
                "error": self.0.to_string()
            })),
        )
            .into_response()
    }
}
