use std::{env, sync::Arc, time::Duration};

use axum::{
    Json, Router,
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use ledgerflow::{
    core::errors::LedgerError,
    core::{config::AppConfig, env::load_dotenv, logging::init_logging},
    indexer::rpc::IndexerRpcClient,
    ingestion::{fetchers::TransactionBundleFetcher, rpc::HttpRpcClient},
    repositories::sqlx::SqlxAnalysisRepository,
    services::{
        monitor_reporter::{MonitorReporter, MonitorReporterConfig},
        seeds::seed_known_metadata,
        trace_monitor::{TopAddressDeltasQuery, TraceMonitorConfig, TraceMonitorService},
    },
};
use serde_json::json;
use tracing::{error, info};

#[tokio::main]
async fn main() {
    load_dotenv();
    init_logging();
    let config = AppConfig::from_env();

    let repository = SqlxAnalysisRepository::connect(&config.database_url)
        .await
        .expect("database connection should succeed");
    sqlx::migrate!("./migrations")
        .run(repository.pool())
        .await
        .expect("migrations should succeed");
    seed_known_metadata(repository.pool(), config.default_chain_id)
        .await
        .expect("seed metadata should succeed");
    let pool = repository.pool().clone();

    let rpc_client = Arc::new(HttpRpcClient::new(config.rpc_url.clone()));
    let block_rpc = Arc::new(IndexerRpcClient::new(config.rpc_url.clone()));
    let fetcher = TransactionBundleFetcher::new(rpc_client.clone(), false);
    let service = TraceMonitorService::new(
        repository,
        block_rpc,
        rpc_client,
        fetcher,
        TraceMonitorConfig {
            chain_id: config.default_chain_id,
            worker_id: config.trace_monitor_worker_id.clone(),
            start_block: config.trace_monitor_start_block,
            confirmations: config.trace_monitor_confirmations,
            block_batch_size: config.trace_monitor_block_batch_size,
            retention_days: config.trace_monitor_retention_days,
            fast_arbitrage_enabled: config.trace_monitor_fast_arbitrage_enabled,
            fast_arbitrage_min_swaps: config.trace_monitor_fast_arbitrage_min_swaps,
            light_deltas_enabled: config.trace_monitor_light_deltas_enabled,
        },
    );
    let service = Arc::new(service);
    let analysis_api_base_url = local_http_base_url(&config.bind_addr);
    let web_base_url = env::var("LEDGERFLOW_TRACE_DOMAIN")
        .map(|value| public_http_base_url(&value))
        .unwrap_or_else(|_| {
            let web_bind_addr = env::var("LEDGERFLOW_WEB_BIND_ADDR")
                .unwrap_or_else(|_| "127.0.0.1:3001".to_owned());
            local_http_base_url(&web_bind_addr)
        });
    let reporter = MonitorReporter::new(
        pool,
        MonitorReporterConfig {
            chain_id: config.default_chain_id,
            enabled: config.trace_monitor_report_enabled,
            interval_ms: config.trace_monitor_report_interval_ms,
            window_minutes: config.trace_monitor_report_window_minutes,
            top_n: config.trace_monitor_report_top_n,
            min_raw_score: config.trace_monitor_report_min_raw_score.clone(),
            notify_min_usd: config.trace_monitor_notify_min_usd,
            run_on_start: config.trace_monitor_report_run_on_start,
            analysis_api_base_url,
            web_base_url,
            telegram_bot_token: config.telegram_bot_token.clone(),
            telegram_chat_id: config.telegram_chat_id.clone(),
        },
    );
    let poll_interval = Duration::from_millis(config.trace_monitor_poll_interval_ms.max(1000));
    let bind_addr = config.trace_monitor_bind_addr.clone();

    info!(
        worker_id = config.trace_monitor_worker_id,
        chain_id = config.default_chain_id,
        bind_addr,
        poll_interval_ms = poll_interval.as_millis(),
        block_batch_size = config.trace_monitor_block_batch_size,
        retention_days = config.trace_monitor_retention_days,
        fast_arbitrage_enabled = config.trace_monitor_fast_arbitrage_enabled,
        fast_arbitrage_min_swaps = config.trace_monitor_fast_arbitrage_min_swaps,
        light_deltas_enabled = config.trace_monitor_light_deltas_enabled,
        report_enabled = config.trace_monitor_report_enabled,
        report_interval_ms = config.trace_monitor_report_interval_ms,
        report_top_n = config.trace_monitor_report_top_n,
        telegram_delivery_configured =
            config.telegram_bot_token.is_some() && config.telegram_chat_id.is_some(),
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

    tokio::spawn(async move {
        if reporter.run_on_start() {
            if let Err(err) = reporter.run_once().await {
                error!(error = %err, "trace monitor startup report failed");
            }
        }
        let report_interval = Duration::from_millis(reporter.interval_ms().max(60_000));
        loop {
            tokio::time::sleep(report_interval).await;
            if let Err(err) = reporter.run_once().await {
                error!(error = %err, "trace monitor hourly report failed");
            }
        }
    });

    if config.trace_monitor_enabled {
        match service.anchor_to_live_start().await {
            Ok((latest_block, target_block)) => {
                info!(
                    latest_block,
                    target_block, "trace monitor anchored live start"
                );
            }
            Err(err) => {
                error!(error = %err, "trace monitor live start anchor failed");
            }
        }
    }

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
                    candidate_rows = iteration.candidate_rows,
                    skipped_blocks = iteration.skipped_blocks,
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

fn local_http_base_url(bind_addr: &str) -> String {
    let bind_addr = bind_addr.trim();
    if bind_addr.starts_with("http://") || bind_addr.starts_with("https://") {
        return bind_addr.trim_end_matches('/').to_owned();
    }
    let normalized = bind_addr
        .strip_prefix("0.0.0.0:")
        .map(|port| format!("127.0.0.1:{port}"))
        .or_else(|| {
            bind_addr
                .strip_prefix("[::]:")
                .map(|port| format!("127.0.0.1:{port}"))
        })
        .or_else(|| {
            bind_addr
                .strip_prefix(":::")
                .map(|port| format!("127.0.0.1:{port}"))
        })
        .unwrap_or_else(|| bind_addr.to_owned());
    format!("http://{}", normalized.trim_end_matches('/'))
}

fn public_http_base_url(domain_or_url: &str) -> String {
    let value = domain_or_url.trim().trim_end_matches('/');
    if value.starts_with("http://") || value.starts_with("https://") {
        value.to_owned()
    } else {
        format!("https://{value}")
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
