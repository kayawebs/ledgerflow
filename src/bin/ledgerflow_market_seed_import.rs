use std::{env, fs};

use ledgerflow::{
    core::{config::AppConfig, env::load_dotenv, logging::init_logging},
    repositories::sqlx::SqlxAnalysisRepository,
    services::protocol_market::{import_protocol_market_manifest, parse_protocol_market_manifest},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    load_dotenv();
    init_logging();
    let paths = env::args().skip(1).collect::<Vec<_>>();
    if paths.is_empty() {
        return Err(
            "usage: ledgerflow_market_seed_import <manifest.json> [manifest.json ...]".into(),
        );
    }
    let config = AppConfig::from_env();
    let repository = SqlxAnalysisRepository::connect(&config.database_url).await?;
    sqlx::migrate!("./migrations")
        .run(repository.pool())
        .await?;
    let mut summaries = Vec::with_capacity(paths.len());
    for path in paths {
        let raw = fs::read_to_string(path)?;
        let manifest = parse_protocol_market_manifest(&raw)?;
        summaries.push(import_protocol_market_manifest(repository.pool(), &manifest).await?);
    }
    println!("{}", serde_json::to_string_pretty(&summaries)?);
    Ok(())
}
