use std::{env, time::Duration};

use ledgerflow::{
    core::{config::AppConfig, env::load_dotenv, logging::init_logging},
    repositories::sqlx::SqlxAnalysisRepository,
    services::seed_intelligence::{SeedIntelligenceConfig, SeedIntelligenceService},
};
use tracing::{error, info};

#[tokio::main]
async fn main() {
    load_dotenv();
    init_logging();
    let app_config = AppConfig::from_env();
    let poll_interval =
        Duration::from_millis(env_u64("LEDGERFLOW_SEED_WORKER_POLL_INTERVAL_MS", 300_000));
    let error_interval =
        Duration::from_millis(env_u64("LEDGERFLOW_SEED_WORKER_ERROR_INTERVAL_MS", 30_000));
    let run_once = env_bool("LEDGERFLOW_SEED_WORKER_RUN_ONCE", false);
    let config = SeedIntelligenceConfig {
        chain_id: env_u32("LEDGERFLOW_DEFAULT_CHAIN_ID", 4663),
        worker_id: env::var("LEDGERFLOW_SEED_WORKER_ID")
            .unwrap_or_else(|_| "robinhood_seed_intelligence".to_owned()),
        minimum_mev_candidate_transactions: env_u32(
            "LEDGERFLOW_SEED_MIN_MEV_CANDIDATE_TRANSACTIONS",
            3,
        ),
        minimum_mev_distinct_routes: env_u32("LEDGERFLOW_SEED_MIN_MEV_DISTINCT_ROUTES", 2),
        maximum_mev_candidates: env_u32("LEDGERFLOW_SEED_MAX_MEV_CANDIDATES", 1_000),
        maximum_active_pools: env_u32("LEDGERFLOW_SEED_MAX_ACTIVE_POOLS", 500),
        maximum_queued_priority: env_i32("LEDGERFLOW_SEED_MAX_QUEUED_PRIORITY", 39).clamp(0, 100),
        maximum_evidence_transactions_per_seed: env_u32(
            "LEDGERFLOW_SEED_MAX_EVIDENCE_TRANSACTIONS_PER_SEED",
            8,
        ),
        maximum_related_addresses: env_u32("LEDGERFLOW_SEED_MAX_RELATED_ADDRESSES", 5_000),
    };

    let repository = SqlxAnalysisRepository::connect(&app_config.database_url)
        .await
        .expect("database connection should succeed");
    sqlx::migrate!("./migrations")
        .run(repository.pool())
        .await
        .expect("migrations should succeed");
    let service = SeedIntelligenceService::new(repository.pool().clone(), config.clone());

    info!(
        worker_id = config.worker_id,
        chain_id = config.chain_id,
        minimum_mev_candidate_transactions = config.minimum_mev_candidate_transactions,
        minimum_mev_distinct_routes = config.minimum_mev_distinct_routes,
        maximum_mev_candidates = config.maximum_mev_candidates,
        maximum_active_pools = config.maximum_active_pools,
        maximum_queued_priority = config.maximum_queued_priority,
        maximum_evidence_transactions_per_seed = config.maximum_evidence_transactions_per_seed,
        maximum_related_addresses = config.maximum_related_addresses,
        run_once,
        "seed intelligence worker started"
    );
    loop {
        match service.process_once().await {
            Ok(iteration) => {
                let summary = service.summary().await.ok();
                info!(
                    curated_seed_changes = iteration.curated_seed_changes,
                    protocol_seed_changes = iteration.protocol_seed_changes,
                    active_pool_seed_changes = iteration.active_pool_seed_changes,
                    asset_seed_changes = iteration.asset_seed_changes,
                    mev_candidate_changes = iteration.mev_candidate_changes,
                    relationship_changes = iteration.relationship_changes,
                    address_jobs_changed = iteration.address_jobs_changed,
                    relationship_address_jobs_changed = iteration.relationship_address_jobs_changed,
                    total_seed_addresses = summary.as_ref().map(|value| value.total_seed_addresses),
                    active_seed_addresses =
                        summary.as_ref().map(|value| value.active_seed_addresses),
                    mev_candidate_addresses =
                        summary.as_ref().map(|value| value.mev_candidate_addresses),
                    relationship_count = summary.as_ref().map(|value| value.relationship_count),
                    seeds_with_relationships =
                        summary.as_ref().map(|value| value.seeds_with_relationships),
                    analyzed_evidence_transaction_count = summary
                        .as_ref()
                        .map(|value| value.analyzed_evidence_transaction_count),
                    queued_seed_jobs = summary.as_ref().map(|value| value.queued_seed_jobs),
                    deferred_generic_jobs =
                        summary.as_ref().map(|value| value.deferred_generic_jobs),
                    "seed intelligence iteration completed"
                );
            }
            Err(error) => {
                error!(error = %error, "seed intelligence iteration failed");
                if run_once {
                    std::process::exit(1);
                }
                tokio::time::sleep(error_interval.max(Duration::from_secs(1))).await;
                continue;
            }
        }
        if run_once {
            break;
        }
        tokio::time::sleep(poll_interval.max(Duration::from_secs(1))).await;
    }
}

fn env_u64(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_u32(name: &str, default: u32) -> u32 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
        .max(1)
}

fn env_i32(name: &str, default: i32) -> i32 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_bool(name: &str, default: bool) -> bool {
    env::var(name)
        .ok()
        .and_then(|value| match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Some(true),
            "0" | "false" | "no" | "off" => Some(false),
            _ => None,
        })
        .unwrap_or(default)
}
