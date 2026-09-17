use std::{env, fs, path::Path, time::Duration};

use chrono::{DateTime, Datelike, Duration as ChronoDuration, NaiveDate, Utc};
use ledgerflow::{
    core::logging::init_logging,
    pilot::{
        report::{report_inputs_digest, weekly_html, weekly_markdown},
        repository::{load_enabled_configs, load_week_events, save_weekly_report},
    },
};
use sqlx::{PgPool, postgres::PgPoolOptions};
use tokio::time::sleep;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    dotenvy::dotenv().ok();
    init_logging();
    let database_url = env::var("LEDGERFLOW_DATABASE_URL").or_else(|_| env::var("DATABASE_URL"))?;
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&database_url)
        .await?;
    sqlx::migrate!("./migrations").run(&pool).await?;
    let output =
        env::var("LEDGERFLOW_PILOT_REPORT_OUTPUT_DIR").unwrap_or_else(|_| "reports".to_owned());
    let interval_ms = env::var("LEDGERFLOW_PILOT_REPORT_INTERVAL_MS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(3_600_000_u64);
    let once = env::var("LEDGERFLOW_PILOT_REPORT_RUN_ONCE")
        .ok()
        .is_some_and(|value| matches!(value.as_str(), "1" | "true" | "yes"));

    loop {
        generate_previous_week(&pool, Path::new(&output), Utc::now()).await?;
        if once {
            break;
        }
        sleep(Duration::from_millis(interval_ms.max(60_000))).await;
    }
    Ok(())
}

async fn generate_previous_week(
    pool: &PgPool,
    output_root: &Path,
    now: DateTime<Utc>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (start, end) = previous_utc_week(now);
    for config in load_enabled_configs(pool).await? {
        let events = load_week_events(pool, &config.project.project_id, start, end).await?;
        let markdown = weekly_markdown(
            &config.project.project_id,
            &start.to_rfc3339(),
            &end.to_rfc3339(),
            &events,
        );
        let html = weekly_html(
            &config.project.project_id,
            &start.to_rfc3339(),
            &end.to_rfc3339(),
            &events,
        );
        let digest = report_inputs_digest(&events);
        let directory = output_root
            .join(&config.project.project_id)
            .join(start.date_naive().to_string());
        fs::create_dir_all(&directory)?;
        fs::write(directory.join("weekly_report.md"), &markdown)?;
        fs::write(directory.join("weekly_report.html"), &html)?;
        let report_id = save_weekly_report(
            pool,
            &config.project.project_id,
            start,
            end,
            &markdown,
            &html,
            &digest,
        )
        .await?;
        info!(project_id=config.project.project_id,event_count=events.len(),report_id=%report_id,"generated completed UTC weekly Pilot report");
    }
    Ok(())
}

fn previous_utc_week(now: DateTime<Utc>) -> (DateTime<Utc>, DateTime<Utc>) {
    let today = now.date_naive();
    let current_monday =
        today - ChronoDuration::days(today.weekday().num_days_from_monday() as i64);
    let previous_monday = current_monday - ChronoDuration::days(7);
    (midnight(previous_monday), midnight(current_monday))
}

fn midnight(day: NaiveDate) -> DateTime<Utc> {
    day.and_hms_opt(0, 0, 0)
        .expect("midnight is valid")
        .and_utc()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn previous_week_is_complete_monday_to_monday_utc() {
        let now = "2026-08-11T12:00:00Z".parse().unwrap();
        let (start, end) = previous_utc_week(now);
        assert_eq!(start.to_rfc3339(), "2026-08-03T00:00:00+00:00");
        assert_eq!(end.to_rfc3339(), "2026-08-10T00:00:00+00:00");
    }
}
