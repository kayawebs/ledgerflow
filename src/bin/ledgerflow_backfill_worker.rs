use std::{sync::Arc, time::Duration};

use ledgerflow::{
    core::{config::AppConfig, env::load_dotenv, logging::init_logging},
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
use tracing::{error, info};

#[tokio::main]
async fn main() {
    load_dotenv();
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

    let rpc_client = Arc::new(HttpRpcClient::new(config.rpc_url.clone()));
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
    let backfill_service = ClusterBackfillService::new(
        Arc::clone(&service),
        tx_source,
        config.cluster_backfill_query_limit,
        config.cluster_continuous_watch_lookback_blocks,
    );
    let poll_interval =
        Duration::from_millis(config.cluster_backfill_worker_poll_interval_ms.max(250));

    info!(
        poll_interval_ms = poll_interval.as_millis(),
        "cluster backfill worker started"
    );

    loop {
        match backfill_service.process_next_pending_run().await {
            Ok(Some(result)) => {
                info!(
                    cluster_id = %result.run.cluster_id,
                    run_id = %result.run.run_id,
                    status = ?result.run.status,
                    discovered_tx_count = result.run.discovered_tx_count,
                    analyzed_tx_count = result.run.analyzed_tx_count,
                    failed_tx_count = result.run.failed_tx_count,
                    "cluster backfill run completed"
                );
            }
            Ok(None) => {
                if config.cluster_continuous_watch_enabled {
                    match backfill_service.enqueue_incremental_runs().await {
                        Ok(enqueued) if enqueued > 0 => {
                            info!(enqueued, "enqueued incremental cluster backfill runs");
                            continue;
                        }
                        Ok(_) => {}
                        Err(err) => {
                            error!(error = %err, "cluster watch enqueue iteration failed");
                        }
                    }
                }
                tokio::time::sleep(poll_interval).await;
            }
            Err(err) => {
                error!(error = %err, "cluster backfill worker iteration failed");
                tokio::time::sleep(poll_interval).await;
            }
        }
    }
}
