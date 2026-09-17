use std::{env, sync::Arc, time::Duration};

use ledgerflow::{
    core::{config::AppConfig, env::load_dotenv, logging::init_logging},
    ingestion::{fetchers::TransactionBundleFetcher, rpc::HttpRpcClient},
    plugins::loader::load_plugins,
    recognition::registry::RecognizerRegistry,
    repositories::sqlx::SqlxAnalysisRepository,
    services::{
        analysis::AnalysisService,
        economic_verifier::{EconomicVerifierConfig, EconomicVerifierService},
        pricing::PriceService,
        token_metadata::TokenMetadataService,
    },
};
use tracing::{error, info};

#[tokio::main]
async fn main() {
    load_dotenv();
    init_logging();
    let app_config = AppConfig::from_env();
    let poll_interval = Duration::from_millis(env_u64(
        "LEDGERFLOW_ECONOMIC_VERIFIER_POLL_INTERVAL_MS",
        5_000,
    ));
    let run_once = env_bool("LEDGERFLOW_ECONOMIC_VERIFIER_RUN_ONCE", false);
    let config = EconomicVerifierConfig {
        chain_id: env_u64("LEDGERFLOW_DEFAULT_CHAIN_ID", 4663),
        worker_id: env::var("LEDGERFLOW_ECONOMIC_VERIFIER_WORKER_ID")
            .unwrap_or_else(|_| "robinhood_seed_economic_verifier".to_owned()),
        batch_size: env_u32("LEDGERFLOW_ECONOMIC_VERIFIER_BATCH_SIZE", 2),
        maximum_new_jobs_per_pass: env_u32(
            "LEDGERFLOW_ECONOMIC_VERIFIER_MAX_NEW_JOBS_PER_PASS",
            10,
        ),
        maximum_pending_jobs: env_u32("LEDGERFLOW_ECONOMIC_VERIFIER_MAX_PENDING_JOBS", 50),
        maximum_candidates_per_seed: env_u32(
            "LEDGERFLOW_ECONOMIC_VERIFIER_MAX_CANDIDATES_PER_SEED",
            2,
        ),
        maximum_attempts: env_u32("LEDGERFLOW_ECONOMIC_VERIFIER_MAX_ATTEMPTS", 3),
        retry_delay_seconds: env_i64("LEDGERFLOW_ECONOMIC_VERIFIER_RETRY_DELAY_SECONDS", 300),
        stale_claim_seconds: env_i64("LEDGERFLOW_ECONOMIC_VERIFIER_STALE_CLAIM_SECONDS", 120),
    };

    let mut registry = RecognizerRegistry::new();
    let plugin_context = load_plugins(&app_config.plugins, &mut registry);
    let repository = SqlxAnalysisRepository::connect(&app_config.database_url)
        .await
        .expect("database connection should succeed");
    sqlx::migrate!("./migrations")
        .run(repository.pool())
        .await
        .expect("migrations should succeed");
    let rpc = Arc::new(HttpRpcClient::new(app_config.rpc_url.clone()));
    let token_metadata = Arc::new(TokenMetadataService::new(
        repository.pool().clone(),
        rpc.clone(),
    ));
    let price_service =
        Arc::new(PriceService::new(repository.pool().clone()).with_rpc_client(rpc.clone()));
    // Trace is intentionally enabled for this bounded worker even if broad
    // transaction analysis leaves it disabled. Missing trace support remains a
    // safe `insufficient_evidence` outcome.
    let fetcher = TransactionBundleFetcher::new(rpc.clone(), true);
    let repository = Arc::new(repository);
    let analysis_service = Arc::new(
        AnalysisService::new(
            fetcher,
            registry,
            plugin_context,
            repository.clone(),
            app_config.enabled_recognizers.clone(),
        )
        .with_token_metadata_service(token_metadata)
        .with_price_service(price_service),
    );
    let service = EconomicVerifierService::new(
        repository.pool().clone(),
        rpc,
        analysis_service,
        config.clone(),
    );
    service
        .validate_rpc_chain()
        .await
        .expect("economic verifier RPC chain must match configured chain");

    info!(
        worker_id = config.worker_id,
        chain_id = config.chain_id,
        batch_size = config.batch_size,
        maximum_new_jobs_per_pass = config.maximum_new_jobs_per_pass,
        maximum_pending_jobs = config.maximum_pending_jobs,
        maximum_candidates_per_seed = config.maximum_candidates_per_seed,
        maximum_attempts = config.maximum_attempts,
        run_once,
        "economic verifier worker started"
    );
    loop {
        match service.process_once().await {
            Ok(iteration) => info!(
                recovered_jobs = iteration.recovered_jobs,
                enqueued_jobs = iteration.enqueued_jobs,
                completed_jobs = iteration.completed_jobs,
                verified_jobs = iteration.verified_jobs,
                insufficient_evidence_jobs = iteration.insufficient_evidence_jobs,
                dismissed_jobs = iteration.dismissed_jobs,
                failed_jobs = iteration.failed_jobs,
                "economic verifier iteration completed"
            ),
            Err(error) => error!(error = %error, "economic verifier iteration failed"),
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

fn env_i64(name: &str, default: i64) -> i64 {
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
