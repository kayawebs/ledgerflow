use std::{
    env,
    time::{Duration, Instant},
};

use ledgerflow::{
    core::{config::AppConfig, env::load_dotenv, logging::init_logging},
    indexer::{clickhouse::ClickHouseClient, rpc::IndexerRpcClient},
    services::{
        market_arbitrage::{MarketArbitrageConfig, MarketArbitrageEstimator},
        market_data::{MarketProjector, MarketProjectorConfig, run_market_migrations},
        market_liquidations::{LiquidationIndexer, LiquidationIndexerConfig},
        market_pnl::{MarketPnlConfig, MarketPnlProjector},
        market_pools::{PoolIndexer, PoolIndexerConfig},
    },
};
use sqlx::postgres::PgPoolOptions;
use tracing::{error, info, warn};

#[tokio::main]
async fn main() {
    load_dotenv();
    init_logging();
    let app = AppConfig::from_env();
    let database_url =
        env::var("LEDGERFLOW_MARKET_DATABASE_URL").unwrap_or_else(|_| app.database_url.clone());
    run_market_migrations(&database_url)
        .await
        .expect("market migrations should succeed");
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .acquire_timeout(Duration::from_secs(30))
        .connect(&database_url)
        .await
        .expect("database connection should succeed");

    let defaults = MarketProjectorConfig::default();
    let config = MarketProjectorConfig {
        chain_id: env_parse("LEDGERFLOW_MARKET_CHAIN_ID", defaults.chain_id),
        wrapped_native_asset: env::var("LEDGERFLOW_MARKET_WRAPPED_NATIVE_ASSET").ok(),
        price_bucket_batch: env_parse(
            "LEDGERFLOW_MARKET_PRICE_BUCKET_BATCH",
            defaults.price_bucket_batch,
        ),
        price_bucket_overlap: env_parse(
            "LEDGERFLOW_MARKET_PRICE_BUCKET_OVERLAP",
            defaults.price_bucket_overlap,
        ),
        min_price_confidence: env_parse(
            "LEDGERFLOW_MARKET_MIN_PRICE_CONFIDENCE",
            defaults.min_price_confidence,
        ),
        max_quote_price_age_hours: env_parse(
            "LEDGERFLOW_MARKET_MAX_QUOTE_PRICE_AGE_HOURS",
            defaults.max_quote_price_age_hours,
        ),
        candle_block_batch: env_parse(
            "LEDGERFLOW_MARKET_CANDLE_BLOCK_BATCH",
            defaults.candle_block_batch,
        ),
        candle_reprocess_blocks: env_parse(
            "LEDGERFLOW_MARKET_CANDLE_REPROCESS_BLOCKS",
            defaults.candle_reprocess_blocks,
        ),
        candle_minute_lookback_blocks: env_parse(
            "LEDGERFLOW_MARKET_CANDLE_MINUTE_LOOKBACK_BLOCKS",
            defaults.candle_minute_lookback_blocks,
        ),
        stats_candle_scan_limit: env_parse(
            "LEDGERFLOW_MARKET_STATS_CANDLE_SCAN_LIMIT",
            defaults.stats_candle_scan_limit,
        ),
        stats_batch_size: env_parse(
            "LEDGERFLOW_MARKET_STATS_BATCH_SIZE",
            defaults.stats_batch_size,
        ),
        stats_decay_seconds: env_parse(
            "LEDGERFLOW_MARKET_STATS_DECAY_SECONDS",
            defaults.stats_decay_seconds,
        ),
        supply_fetch_limit: env_parse(
            "LEDGERFLOW_MARKET_SUPPLY_FETCH_LIMIT",
            defaults.supply_fetch_limit,
        ),
        supply_fetch_timeout_ms: env_parse(
            "LEDGERFLOW_MARKET_SUPPLY_FETCH_TIMEOUT_MS",
            defaults.supply_fetch_timeout_ms,
        ),
    };
    let rpc = env::var("LEDGERFLOW_MARKET_RPC_URL")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map(|url| IndexerRpcClient::with_timeout(url, Duration::from_secs(10)));
    if rpc.is_none() {
        warn!("LEDGERFLOW_MARKET_RPC_URL is unset; total supply and market cap stay pending");
    }

    let poll = Duration::from_millis(env_parse("LEDGERFLOW_MARKET_POLL_INTERVAL_MS", 3_000_u64));
    let error_delay =
        Duration::from_millis(env_parse("LEDGERFLOW_MARKET_ERROR_INTERVAL_MS", 15_000_u64));
    let repair_interval = Duration::from_secs(env_parse(
        "LEDGERFLOW_MARKET_REPAIR_INTERVAL_SECONDS",
        300_u64,
    ));
    let run_once = env_parse("LEDGERFLOW_MARKET_RUN_ONCE", false);
    let pnl_defaults = MarketPnlConfig::default();
    let pnl = MarketPnlProjector::new(
        pool.clone(),
        MarketPnlConfig {
            chain_id: config.chain_id,
            position_block_batch: env_parse(
                "LEDGERFLOW_MARKET_POSITION_BLOCK_BATCH",
                pnl_defaults.position_block_batch,
            ),
            leaderboard_size: env_parse(
                "LEDGERFLOW_MARKET_LEADERBOARD_SIZE",
                pnl_defaults.leaderboard_size,
            ),
            new_cluster_rows: env_parse(
                "LEDGERFLOW_MARKET_NEW_CLUSTER_ROWS",
                pnl_defaults.new_cluster_rows,
            ),
            min_wallet_cost_usd: env_parse(
                "LEDGERFLOW_MARKET_MIN_WALLET_COST_USD",
                pnl_defaults.min_wallet_cost_usd,
            ),
        },
    );
    if let Some(rpc) = rpc.clone() {
        let log_store = env::var("LEDGERFLOW_MARKET_CLICKHOUSE_URL")
            .or_else(|_| env::var("LEDGERFLOW_INDEXER_CLICKHOUSE_URL"))
            .ok()
            .filter(|url| !url.trim().is_empty())
            .map(|url| {
                ClickHouseClient::new(
                    url,
                    env::var("LEDGERFLOW_INDEXER_CLICKHOUSE_DATABASE")
                        .unwrap_or_else(|_| "ledgerflow_indexer".to_owned()),
                    env::var("LEDGERFLOW_INDEXER_CLICKHOUSE_USER")
                        .unwrap_or_else(|_| "ledgerflow".to_owned()),
                    env::var("LEDGERFLOW_INDEXER_CLICKHOUSE_PASSWORD").ok(),
                )
            });
        if log_store.is_none() {
            warn!("no ClickHouse log store configured; pool swaps use node eth_getLogs");
        }
        let mut pools = PoolIndexer::new(
            pool.clone(),
            rpc,
            log_store,
            PoolIndexerConfig {
                chain_id: config.chain_id,
                ..PoolIndexerConfig::default()
            },
        );
        tokio::spawn(async move {
            loop {
                let delay = match pools.process_once().await {
                    Ok(iteration) => {
                        if iteration.has_backlog() || iteration.swaps > 0 {
                            info!(
                                mapped = iteration.mapped,
                                unmapped = iteration.unmapped,
                                from = iteration.from_block,
                                to = iteration.to_block,
                                head = iteration.head_block,
                                swaps = iteration.swaps,
                                range = iteration.range,
                                "market pool swap indexing completed"
                            );
                        }
                        if iteration.has_backlog() {
                            Duration::from_millis(100)
                        } else {
                            Duration::from_secs(3)
                        }
                    }
                    Err(pool_error) => {
                        error!(error = %pool_error, "market pool swap indexing failed");
                        Duration::from_secs(20)
                    }
                };
                tokio::time::sleep(delay).await;
            }
        });
    }
    if let Some(rpc) = rpc.clone() {
        let defaults = LiquidationIndexerConfig::default();
        let mut indexer = LiquidationIndexer::new(
            pool.clone(),
            rpc,
            LiquidationIndexerConfig {
                chain_id: config.chain_id,
                start_block: env_parse(
                    "LEDGERFLOW_MARKET_LIQUIDATION_START_BLOCK",
                    defaults.start_block,
                ),
                ..defaults
            },
        );
        tokio::spawn(async move {
            let mut last_valuation = Instant::now() - Duration::from_secs(3_600);
            loop {
                let delay = match indexer.process_once().await {
                    Ok(iteration) => {
                        if iteration.logs > 0 || iteration.has_backlog() {
                            info!(
                                from = iteration.from_block,
                                to = iteration.to_block,
                                head = iteration.head_block,
                                logs = iteration.logs,
                                accepted = iteration.accepted,
                                rejected = iteration.rejected,
                                range = iteration.range,
                                "market liquidation indexing completed"
                            );
                        }
                        if iteration.has_backlog() {
                            Duration::from_millis(100)
                        } else {
                            Duration::from_secs(10)
                        }
                    }
                    Err(index_error) => {
                        error!(error = %index_error, "market liquidation indexing failed");
                        Duration::from_secs(30)
                    }
                };
                if last_valuation.elapsed() >= Duration::from_secs(120) {
                    match indexer.value_pending().await {
                        Ok(rows) => info!(rows, "market liquidation valuation completed"),
                        Err(value_error) => {
                            warn!(error = %value_error, "market liquidation valuation failed")
                        }
                    }
                    last_valuation = Instant::now();
                }
                tokio::time::sleep(delay).await;
            }
        });
    }
    if let Some(rpc) = rpc.clone() {
        let defaults = MarketArbitrageConfig::default();
        let estimator = MarketArbitrageEstimator::new(
            pool.clone(),
            rpc,
            MarketArbitrageConfig {
                chain_id: config.chain_id,
                backfill_days: env_parse(
                    "LEDGERFLOW_MARKET_ARBITRAGE_BACKFILL_DAYS",
                    defaults.backfill_days,
                ),
                batch_size: env_parse(
                    "LEDGERFLOW_MARKET_ARBITRAGE_BATCH_SIZE",
                    defaults.batch_size,
                ),
                receipt_batch_size: env_parse(
                    "LEDGERFLOW_MARKET_ARBITRAGE_RECEIPT_BATCH_SIZE",
                    defaults.receipt_batch_size,
                ),
                receipt_concurrency: env_parse(
                    "LEDGERFLOW_MARKET_ARBITRAGE_RECEIPT_CONCURRENCY",
                    defaults.receipt_concurrency,
                ),
                leaderboard_size: env_parse(
                    "LEDGERFLOW_MARKET_ARBITRAGE_LEADERBOARD_SIZE",
                    defaults.leaderboard_size,
                ),
            },
        );
        let poll = Duration::from_millis(env_parse(
            "LEDGERFLOW_MARKET_ARBITRAGE_POLL_INTERVAL_MS",
            5_000_u64,
        ));
        tokio::spawn(async move {
            let mut last_revaluation = Instant::now();
            loop {
                if last_revaluation.elapsed() >= Duration::from_secs(300) {
                    match estimator.revalue_unknown().await {
                        Ok(rows) => info!(rows, "market arbitrage revaluation completed"),
                        Err(revaluation_error) => {
                            warn!(error = %revaluation_error, "market arbitrage revaluation failed")
                        }
                    }
                    last_revaluation = Instant::now();
                }
                let delay = match estimator.process_once().await {
                    Ok(summary) => {
                        info!(
                            candidates = summary.candidates,
                            estimated = summary.estimated,
                            cursor = ?summary.cursor_time,
                            leaderboard_rows = summary.leaderboard_rows,
                            receipt_ms = summary.receipt_ms,
                            persist_ms = summary.persist_ms,
                            "market arbitrage estimation completed"
                        );
                        if summary.backlog {
                            Duration::from_millis(50)
                        } else {
                            poll
                        }
                    }
                    Err(estimation_error) => {
                        error!(error = %estimation_error, "market arbitrage estimation failed");
                        Duration::from_secs(15)
                    }
                };
                tokio::time::sleep(delay).await;
            }
        });
    }
    tokio::spawn(async move {
        loop {
            let delay = match pnl.project_once().await {
                Ok(pnl_summary) => {
                    info!(
                        position_from = pnl_summary.position_from_block,
                        position_to = pnl_summary.position_to_block,
                        position_limit = pnl_summary.position_limit_block,
                        positions = pnl_summary.positions_upserted,
                        positions_deleted = pnl_summary.positions_deleted,
                        wallet_rows = pnl_summary.wallet_rows,
                        cluster_rows = pnl_summary.cluster_rows,
                        position_ms = pnl_summary.position_ms,
                        leaderboard_ms = pnl_summary.leaderboard_ms,
                        "market pnl projection completed"
                    );
                    if pnl_summary.has_backlog() {
                        Duration::from_millis(50)
                    } else {
                        Duration::from_secs(5)
                    }
                }
                Err(pnl_error) => {
                    error!(error = %pnl_error, "market pnl projection failed");
                    Duration::from_secs(15)
                }
            };
            tokio::time::sleep(delay).await;
        }
    });
    let projector = MarketProjector::new(pool, rpc, config.clone());
    info!(chain_id = config.chain_id, "market projector started");

    let mut last_repair = Instant::now();
    loop {
        let started = Instant::now();
        let delay = match projector.project_once().await {
            Ok(summary) => {
                info!(
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    price_cursor = summary.price_cursor,
                    price_rows = summary.price_rows_upserted,
                    candle_from = summary.candle_from_block,
                    candle_to = summary.candle_to_block,
                    candle_head = summary.candle_head_block,
                    candles = summary.candles_upserted,
                    candles_deleted = summary.candles_deleted,
                    launches = summary.launches_touched,
                    stats = summary.stats_refreshed,
                    supplies = summary.supplies_fetched,
                    candle_ms = summary.candle_ms,
                    stats_ms = summary.stats_ms,
                    "market projection completed"
                );
                if summary.has_backlog() {
                    Duration::from_millis(50)
                } else {
                    poll
                }
            }
            Err(projection_error) => {
                error!(error = %projection_error, "market projection failed");
                if let Err(record_error) =
                    projector.record_error(&projection_error.to_string()).await
                {
                    warn!(error = %record_error, "failed to record market projection error");
                }
                error_delay
            }
        };
        match projector.backfill_registered_assets().await {
            Ok(0) => {}
            Ok(rows) => info!(rows, "market price asset backfill completed"),
            Err(backfill_error) => {
                warn!(error = %backfill_error, "market price asset backfill failed")
            }
        }
        if last_repair.elapsed() >= repair_interval {
            match projector.repair_candle_prices().await {
                Ok(repaired) => info!(repaired, "market candle price repair completed"),
                Err(repair_error) => {
                    warn!(error = %repair_error, "market candle price repair failed")
                }
            }
            last_repair = Instant::now();
        }
        if run_once {
            break;
        }
        tokio::time::sleep(delay).await;
    }
}

fn env_parse<T: std::str::FromStr>(name: &str, default: T) -> T {
    env::var(name)
        .ok()
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(default)
}
