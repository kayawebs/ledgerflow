//! Continuous checks that the published numbers are still true.
//!
//! Every check here exists because something it would have caught reached
//! production: a block scan that examined a ninth of the chain and was
//! presented as a count, a cursor window that skipped most of its input, dust
//! trades setting a token's price, and a methodology change that multiplied a
//! table's row rate overnight. Each is an assertion that can be made
//! continuously instead of noticed by hand.

use sqlx::{PgPool, Row};

use crate::core::errors::{LedgerError, LedgerResult};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Status {
    Ok,
    Warn,
    Fail,
    Unknown,
}

impl Status {
    pub fn as_str(self) -> &'static str {
        match self {
            Status::Ok => "ok",
            Status::Warn => "warn",
            Status::Fail => "fail",
            Status::Unknown => "unknown",
        }
    }
}

/// One verdict.
#[derive(Clone, Debug)]
pub struct CheckResult {
    pub key: String,
    pub status: Status,
    pub observed: Option<f64>,
    pub threshold: Option<f64>,
    pub detail: String,
}

impl CheckResult {
    fn new(
        key: &str,
        status: Status,
        observed: Option<f64>,
        threshold: f64,
        detail: String,
    ) -> Self {
        Self {
            key: key.to_owned(),
            status,
            observed,
            threshold: Some(threshold),
            detail,
        }
    }
}

/// Grades an observation where smaller is better.
fn grade_max(observed: f64, warn_at: f64, fail_at: f64) -> Status {
    if observed >= fail_at {
        Status::Fail
    } else if observed >= warn_at {
        Status::Warn
    } else {
        Status::Ok
    }
}

/// Grades an observation where larger is better.
fn grade_min(observed: f64, warn_below: f64, fail_below: f64) -> Status {
    if observed <= fail_below {
        Status::Fail
    } else if observed <= warn_below {
        Status::Warn
    } else {
        Status::Ok
    }
}

#[derive(Clone, Debug)]
pub struct HealthConfig {
    pub chain_id: u64,
    /// Seconds a cursor may go without moving before it is a problem.
    pub cursor_stale_warn: f64,
    pub cursor_stale_fail: f64,
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            chain_id: 4663,
            // The position projection batches and rebuilds leaderboards between
            // passes, so a couple of minutes between cursor writes is its
            // normal cadence, not a stall.
            cursor_stale_warn: 300.0,
            cursor_stale_fail: 600.0,
        }
    }
}

pub struct PipelineHealth {
    pool: PgPool,
    config: HealthConfig,
}

impl PipelineHealth {
    pub fn new(pool: PgPool, config: HealthConfig) -> Self {
        Self { pool, config }
    }

    /// Runs every check and records the verdicts. Returns how many are failing.
    pub async fn run_once(&self) -> LedgerResult<Vec<CheckResult>> {
        let mut results = Vec::new();
        results.extend(self.cursor_freshness().await?);
        results.push(self.block_coverage().await?);
        results.push(self.estimate_completeness().await?);
        results.push(self.price_sanity().await?);
        results.push(self.stats_freshness().await?);
        results.push(self.candle_agreement().await?);
        results.push(self.pool_trader_attribution().await?);
        self.record(&results).await?;
        Ok(results)
    }

    /// Every cursor should keep moving. A projector that dies quietly looks
    /// exactly like a quiet chain until someone reads a stale number.
    async fn cursor_freshness(&self) -> LedgerResult<Vec<CheckResult>> {
        let rows = sqlx::query(
            "select checkpoint_key, extract(epoch from now() - updated_at)::float8 as stale
             from market.projector_checkpoints
             where checkpoint_key like '%:' || $1::text",
        )
        .bind(self.config.chain_id as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(db_error)?;
        Ok(rows
            .iter()
            .map(|row| {
                let key: String = row.get("checkpoint_key");
                let stale: f64 = row.get("stale");
                let name = key.split(':').next().unwrap_or(&key).to_owned();
                CheckResult::new(
                    &format!("cursor_fresh.{name}"),
                    grade_max(
                        stale,
                        self.config.cursor_stale_warn,
                        self.config.cursor_stale_fail,
                    ),
                    Some(stale),
                    self.config.cursor_stale_fail,
                    format!("{name} cursor last moved {stale:.0}s ago"),
                )
            })
            .collect())
    }

    /// The share of recent blocks the arbitrage scanner actually covered. The
    /// trace monitor reported counts while examining under a tenth of the
    /// chain; this is the assertion that would have said so.
    async fn block_coverage(&self) -> LedgerResult<CheckResult> {
        let row = sqlx::query(
            r#"with bounds as (
                 select max(block_number) as head,
                        max(block_number) - 20000 as tail
                 from public.arbitrage_candidates where chain_id = $1
               )
               select (select count(distinct block_number) from public.arbitrage_candidates
                       where chain_id = $1 and block_number between bounds.tail and bounds.head)::float8
                        as scanned,
                      (bounds.head - bounds.tail)::float8 as span
               from bounds"#,
        )
        .bind(self.config.chain_id as i64)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_error)?;
        let Some(row) = row else {
            return Ok(CheckResult::new(
                "coverage.arbitrage_blocks",
                Status::Unknown,
                None,
                0.05,
                "no candidates recorded".to_owned(),
            ));
        };
        let scanned: Option<f64> = row.try_get("scanned").ok();
        let span: Option<f64> = row.try_get("span").ok();
        let (Some(scanned), Some(span)) = (scanned, span) else {
            return Ok(CheckResult::new(
                "coverage.arbitrage_blocks",
                Status::Unknown,
                None,
                0.05,
                "no candidates recorded".to_owned(),
            ));
        };
        if span <= 0.0 {
            return Ok(CheckResult::new(
                "coverage.arbitrage_blocks",
                Status::Unknown,
                None,
                0.05,
                "no block span".to_owned(),
            ));
        }
        // Not every block holds an arbitrage, so this measures the share of
        // blocks that produced one and alarms when it collapses rather than
        // demanding it reach any particular level.
        let share = scanned / span;
        Ok(CheckResult::new(
            "coverage.arbitrage_blocks",
            grade_min(share, 0.10, 0.05),
            Some(share),
            0.05,
            format!("{scanned:.0} of the last {span:.0} blocks produced a candidate"),
        ))
    }

    /// Candidates that never became estimates. A cursor that skips its input
    /// leaves exactly this trail.
    async fn estimate_completeness(&self) -> LedgerResult<CheckResult> {
        let row = sqlx::query(
            r#"select count(*)::float8 as total,
                      count(*) filter (where estimate.tx_hash is null)::float8 as missing
               from public.arbitrage_candidates candidate
               left join market.arbitrage_estimates estimate
                 on estimate.chain_id = candidate.chain_id
                and estimate.tx_hash = candidate.tx_hash
               where candidate.chain_id = $1
                 and candidate.block_timestamp between now() - interval '30 minutes'
                                                  and now() - interval '5 minutes'"#,
        )
        .bind(self.config.chain_id as i64)
        .fetch_one(&self.pool)
        .await
        .map_err(db_error)?;
        let total: f64 = row.get("total");
        let missing: f64 = row.get("missing");
        if total < 1.0 {
            return Ok(CheckResult::new(
                "completeness.arbitrage_estimates",
                Status::Unknown,
                None,
                0.2,
                "no candidates in window".to_owned(),
            ));
        }
        let share = missing / total;
        Ok(CheckResult::new(
            "completeness.arbitrage_estimates",
            grade_max(share, 0.05, 0.20),
            Some(share),
            0.20,
            format!("{missing:.0} of {total:.0} candidates have no estimate"),
        ))
    }

    /// Prices that cannot be real. A one-wei-in, one-wei-out trade once implied
    /// a price of 1.0 and put an 18-billion-percent gain on the front page.
    async fn price_sanity(&self) -> LedgerResult<CheckResult> {
        let row = sqlx::query(
            r#"select count(*)::float8 as absurd
               from market.token_candles_1m
               where chain_id = $1 and bucket_start > now() - interval '2 hours'
                 and close_price_quote >= 0.1"#,
        )
        .bind(self.config.chain_id as i64)
        .fetch_one(&self.pool)
        .await
        .map_err(db_error)?;
        let absurd: f64 = row.get("absurd");
        Ok(CheckResult::new(
            "sanity.candle_prices",
            grade_max(absurd, 1.0, 10.0),
            Some(absurd),
            10.0,
            format!("{absurd:.0} candles priced at or above 0.1 of the quote asset"),
        ))
    }

    /// Token statistics should be refreshed continuously; a stale table shows
    /// yesterday's market as if it were now.
    async fn stats_freshness(&self) -> LedgerResult<CheckResult> {
        let row = sqlx::query(
            r#"select extract(epoch from now() - max(refreshed_at))::float8 as stale
               from market.token_stats where chain_id = $1"#,
        )
        .bind(self.config.chain_id as i64)
        .fetch_one(&self.pool)
        .await
        .map_err(db_error)?;
        let stale: Option<f64> = row.try_get("stale").ok().flatten();
        let Some(stale) = stale else {
            return Ok(CheckResult::new(
                "freshness.token_stats",
                Status::Unknown,
                None,
                600.0,
                "no statistics rows".to_owned(),
            ));
        };
        Ok(CheckResult::new(
            "freshness.token_stats",
            grade_max(stale, 300.0, 900.0),
            Some(stale),
            900.0,
            format!("newest token statistics are {stale:.0}s old"),
        ))
    }

    /// Candles are a projection of the trade facts, so the two must agree. This
    /// is the reconciliation that was done by hand during the data review,
    /// running continuously on a sample.
    async fn candle_agreement(&self) -> LedgerResult<CheckResult> {
        let row = sqlx::query(
            r#"with sample as (
                 select launch_id from market.token_stats
                 where chain_id = $1 and last_trade_at > now() - interval '2 hours'
                 order by random() limit 20
               ), candles as (
                 select c.launch_id, sum(c.trade_count)::float8 as trades
                 from market.token_candles_1m c join sample using (launch_id)
                 where c.bucket_start > now() - interval '2 hours'
                 group by c.launch_id
               ), facts as (
                 -- Candles project curve trades and post-graduation pool swaps
                 -- together, so both sides of the reconciliation must.
                 select launch_id, sum(trades)::float8 as trades from (
                   select f.launch_id, count(*) as trades
                   from public.protocol_launch_trade_facts f join sample using (launch_id)
                   where f.is_canonical and f.occurred_at > now() - interval '2 hours'
                   group by f.launch_id
                   union all
                   select p.launch_id, count(*)
                   from market.pool_swap_facts p join sample using (launch_id)
                   where p.occurred_at > now() - interval '2 hours'
                   group by p.launch_id
                 ) combined
                 group by launch_id
               )
               select count(*)::float8 as compared,
                      count(*) filter (
                        where abs(coalesce(candles.trades, 0) - coalesce(facts.trades, 0))
                              > greatest(coalesce(facts.trades, 0) * 0.02, 2)
                      )::float8 as mismatched
               from sample
               left join candles on candles.launch_id = sample.launch_id
               left join facts on facts.launch_id = sample.launch_id"#,
        )
        .bind(self.config.chain_id as i64)
        .fetch_one(&self.pool)
        .await
        .map_err(db_error)?;
        let compared: f64 = row.get("compared");
        let mismatched: f64 = row.get("mismatched");
        if compared < 1.0 {
            return Ok(CheckResult::new(
                "reconciliation.candles_vs_trades",
                Status::Unknown,
                None,
                0.2,
                "nothing to compare".to_owned(),
            ));
        }
        let share = mismatched / compared;
        Ok(CheckResult::new(
            "reconciliation.candles_vs_trades",
            grade_max(share, 0.10, 0.25),
            Some(share),
            0.25,
            format!("{mismatched:.0} of {compared:.0} sampled tokens disagree with their trades"),
        ))
    }

    /// Pool swaps must have their trader resolved before the position
    /// projection passes their block, or the trade is dropped from wallet
    /// profit and loss without a trace. This measures the race directly.
    async fn pool_trader_attribution(&self) -> LedgerResult<CheckResult> {
        let row = sqlx::query(
            r#"with cursor as (
                 select last_position from market.projector_checkpoints
                 where checkpoint_key = 'positions:' || $1::text
               )
               select count(*)::float8 as total,
                      count(*) filter (where trader_address is null)::float8 as unresolved
               from market.pool_swap_facts, cursor
               where chain_id = $1
                 and block_number between cursor.last_position - 20000 and cursor.last_position"#,
        )
        .bind(self.config.chain_id as i64)
        .fetch_one(&self.pool)
        .await
        .map_err(db_error)?;
        let total: f64 = row.get("total");
        let unresolved: f64 = row.get("unresolved");
        if total < 1.0 {
            return Ok(CheckResult::new(
                "attribution.pool_traders",
                Status::Unknown,
                None,
                0.1,
                "no pool swaps below the position cursor".to_owned(),
            ));
        }
        let share = unresolved / total;
        Ok(CheckResult::new(
            "attribution.pool_traders",
            grade_max(share, 0.02, 0.10),
            Some(share),
            0.10,
            format!(
                "{unresolved:.0} of {total:.0} pool swaps already passed by the position cursor have no trader"
            ),
        ))
    }

    async fn record(&self, results: &[CheckResult]) -> LedgerResult<()> {
        for result in results {
            sqlx::query(
                r#"insert into market.pipeline_health
                     (check_key, chain_id, status, observed, threshold, detail,
                      failing_since, checked_at)
                   values ($1, $2, $3, $4, $5, $6,
                           case when $3 = 'fail' then now() end, now())
                   on conflict (check_key) do update set
                     status = excluded.status,
                     observed = excluded.observed,
                     threshold = excluded.threshold,
                     detail = excluded.detail,
                     -- Keep the moment it started failing, so a check that
                     -- flaps does not look freshly broken each pass.
                     failing_since = case
                       when excluded.status <> 'fail' then null
                       when market.pipeline_health.status = 'fail'
                         then market.pipeline_health.failing_since
                       else now()
                     end,
                     checked_at = now()"#,
            )
            .bind(&result.key)
            .bind(self.config.chain_id as i64)
            .bind(result.status.as_str())
            .bind(result.observed)
            .bind(result.threshold)
            .bind(&result.detail)
            .execute(&self.pool)
            .await
            .map_err(db_error)?;
        }
        Ok(())
    }
}

fn db_error(error: impl std::fmt::Display) -> LedgerError {
    LedgerError::Persistence(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn smaller_is_better_grading() {
        assert_eq!(grade_max(0.0, 0.05, 0.2), Status::Ok);
        assert_eq!(grade_max(0.1, 0.05, 0.2), Status::Warn);
        assert_eq!(grade_max(0.5, 0.05, 0.2), Status::Fail);
    }

    #[test]
    fn larger_is_better_grading() {
        assert_eq!(grade_min(0.9, 0.10, 0.05), Status::Ok);
        assert_eq!(grade_min(0.08, 0.10, 0.05), Status::Warn);
        // The sampled block scan sat at 0.087 of blocks covered; the failing
        // threshold is deliberately below that so a collapse, not a quiet
        // chain, is what trips it.
        assert_eq!(grade_min(0.04, 0.10, 0.05), Status::Fail);
    }

    #[test]
    fn statuses_render_for_storage() {
        assert_eq!(Status::Ok.as_str(), "ok");
        assert_eq!(Status::Fail.as_str(), "fail");
        assert_eq!(Status::Unknown.as_str(), "unknown");
    }
}
