use std::{env, fs};

use ledgerflow::{
    core::{config::AppConfig, env::load_dotenv, logging::init_logging},
    repositories::sqlx::SqlxAnalysisRepository,
    services::intelligence_import::{dataset_checksum, import_intelligence_dataset, parse_dataset},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    load_dotenv();
    init_logging();
    let dataset_path = env::args()
        .nth(1)
        .ok_or("usage: ledgerflow_intelligence_import <dataset.json>")?;
    let raw = fs::read_to_string(&dataset_path)?;
    let manifest = parse_dataset(&raw)?;
    let checksum = dataset_checksum(&raw);
    let config = AppConfig::from_env();
    let repository = SqlxAnalysisRepository::connect(&config.database_url).await?;
    sqlx::migrate!("./migrations")
        .run(repository.pool())
        .await?;
    let summary = import_intelligence_dataset(repository.pool(), &manifest, &checksum).await?;
    println!("{}", serde_json::to_string_pretty(&summary)?);
    Ok(())
}
