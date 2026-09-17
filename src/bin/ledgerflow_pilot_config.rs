use std::env;

use ledgerflow::{
    core::logging::init_logging,
    pilot::{
        PilotEvidencePack, load_yaml,
        repository::{import_events, sync_config},
    },
};
use sqlx::postgres::PgPoolOptions;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    dotenvy::dotenv().ok();
    init_logging();
    let args = env::args().skip(1).collect::<Vec<_>>();
    let path = args
        .first()
        .cloned()
        .unwrap_or_else(|| "config/pilots/example-base-project.yaml".to_owned());
    let config = load_yaml(&path)?;
    if args.iter().any(|value| value == "--check") {
        println!(
            "valid Base Pilot config: {} ({} entities, {} rules)",
            config.project.project_id,
            config.entities.len(),
            config.rules.len()
        );
        return Ok(());
    }
    let database_url = env::var("LEDGERFLOW_DATABASE_URL").or_else(|_| env::var("DATABASE_URL"))?;
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&database_url)
        .await?;
    sqlx::migrate!("./migrations").run(&pool).await?;
    sync_config(&pool, &config).await?;
    println!("imported Base Pilot project {}", config.project.project_id);
    if let Some(index) = args.iter().position(|value| value == "--events") {
        let events_path = args
            .get(index + 1)
            .ok_or("--events requires a JSON file path")?;
        let raw = std::fs::read_to_string(events_path)?;
        let events: Vec<PilotEvidencePack> = serde_json::from_str(&raw)?;
        let imported = import_events(&pool, &config, &events).await?;
        println!("imported {imported} validated Evidence Packs");
    }
    Ok(())
}
