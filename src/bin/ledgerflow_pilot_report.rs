use chrono::{DateTime, NaiveDate, Utc};
use ledgerflow::pilot::{
    report::{report_inputs_digest, weekly_html, weekly_markdown},
    repository::{load_project_events, load_week_events, save_weekly_report},
};
use sqlx::postgres::PgPoolOptions;
use std::{env, fs, path::PathBuf};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    dotenvy::dotenv().ok();
    let mut args = env::args().skip(1);
    let project = args
        .next()
        .ok_or("usage: ledgerflow_pilot_report PROJECT WEEK_START WEEK_END [OUTPUT_DIR]")?;
    let start = parse_time(&args.next().ok_or("WEEK_START is required")?)?;
    let end = parse_time(&args.next().ok_or("WEEK_END is required")?)?;
    if end <= start {
        return Err("WEEK_END must be after WEEK_START".into());
    }
    let output = PathBuf::from(
        args.next()
            .unwrap_or_else(|| format!("demo/{}/{}", project, start.date_naive())),
    );
    let database_url = env::var("LEDGERFLOW_DATABASE_URL").or_else(|_| env::var("DATABASE_URL"))?;
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&database_url)
        .await?;
    sqlx::migrate!("./migrations").run(&pool).await?;
    let events = load_week_events(&pool, &project, start, end).await?;
    let start_text = start.to_rfc3339();
    let end_text = end.to_rfc3339();
    let markdown = weekly_markdown(&project, &start_text, &end_text, &events);
    let html = weekly_html(&project, &start_text, &end_text, &events);
    let digest = report_inputs_digest(&events);
    let all_events = load_project_events(&pool, &project, 1000).await?;
    fs::create_dir_all(&output)?;
    fs::write(output.join("weekly_report.md"), &markdown)?;
    fs::write(output.join("weekly_report.html"), &html)?;
    fs::write(
        output.join("events.json"),
        serde_json::to_vec_pretty(&events)?,
    )?;
    fs::write(
        output.join("all_events.json"),
        serde_json::to_vec_pretty(&all_events)?,
    )?;
    let report_id =
        save_weekly_report(&pool, &project, start, end, &markdown, &html, &digest).await?;
    println!(
        "wrote {} events to {} (report_id={})",
        events.len(),
        output.display(),
        report_id
    );
    Ok(())
}
fn parse_time(value: &str) -> Result<DateTime<Utc>, Box<dyn std::error::Error + Send + Sync>> {
    if let Ok(v) = DateTime::parse_from_rfc3339(value) {
        return Ok(v.with_timezone(&Utc));
    }
    let day = NaiveDate::parse_from_str(value, "%Y-%m-%d")?;
    Ok(day.and_hms_opt(0, 0, 0).ok_or("invalid date")?.and_utc())
}
