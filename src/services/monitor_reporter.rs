use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::{PgPool, Row, types::Json as SqlJson};
use tracing::{info, warn};
use uuid::Uuid;

use crate::{
    core::errors::{LedgerError, LedgerResult},
    domain::entities::{EvidencePack, TransactionAnalysis},
};

#[derive(Clone, Debug)]
pub struct MonitorReporterConfig {
    pub chain_id: u64,
    pub enabled: bool,
    pub interval_ms: u64,
    pub window_minutes: i64,
    pub top_n: u32,
    pub min_raw_score: String,
    pub run_on_start: bool,
    pub analysis_api_base_url: String,
    pub web_base_url: String,
    pub email_to: String,
    pub email_from: Option<String>,
    pub email_delivery_mode: String,
    pub postmark_server_token: Option<String>,
    pub postmark_message_stream: String,
    pub resend_api_key: Option<String>,
}

#[derive(Clone)]
pub struct MonitorReporter {
    pool: PgPool,
    client: Client,
    sender: Arc<dyn ReportEmailSender>,
    config: MonitorReporterConfig,
}

#[derive(Clone, Debug, Default)]
pub struct MonitorReportSummary {
    pub inspected_txs: usize,
    pub reportable_txs: usize,
    pub email_sent: bool,
}

#[derive(Clone, Debug)]
struct TopTxCandidate {
    tx_hash: String,
    block_number: i64,
    block_timestamp: Option<DateTime<Utc>>,
    volume_score_raw: String,
    address_count: i64,
}

#[derive(Clone, Debug)]
struct ReportableTx {
    tx: TopTxCandidate,
    signal: TxSignal,
    share_id: Uuid,
    share_url: String,
    tweet_text: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TxSignal {
    activity_kind: String,
    reason: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct AnalysisEnvelope {
    analysis: TransactionAnalysis,
    evidence_pack: EvidencePack,
}

impl MonitorReporter {
    pub fn new(pool: PgPool, config: MonitorReporterConfig) -> Self {
        let sender = build_report_sender(&config);
        Self {
            pool,
            client: Client::new(),
            sender,
            config,
        }
    }

    pub fn run_on_start(&self) -> bool {
        self.config.run_on_start
    }

    pub fn interval_ms(&self) -> u64 {
        self.config.interval_ms
    }

    pub async fn run_once(&self) -> LedgerResult<MonitorReportSummary> {
        if !self.config.enabled {
            return Ok(MonitorReportSummary::default());
        }
        let window_end = Utc::now();
        let window_start = window_end - Duration::minutes(self.config.window_minutes.max(1));
        let candidates = self.load_top_txs(window_start, window_end).await?;
        let mut reportable = Vec::new();

        for tx in candidates.iter().cloned() {
            match self.analyze_tx(&tx.tx_hash).await {
                Ok(envelope) => {
                    let Some(signal) = detect_signal(&envelope.analysis) else {
                        continue;
                    };
                    let share_id = self.create_share(&envelope, &tx, &signal).await?;
                    let share_url = format!(
                        "{}/share/tx/{}",
                        self.config.web_base_url.trim_end_matches('/'),
                        share_id
                    );
                    let tweet_text = build_tweet_text(&tx, &signal, &share_url);
                    reportable.push(ReportableTx {
                        tx,
                        signal,
                        share_id,
                        share_url,
                        tweet_text,
                    });
                }
                Err(err) => {
                    warn!(
                        tx_hash = tx.tx_hash,
                        error = %err,
                        "monitor report analysis failed"
                    );
                }
            }
        }

        let email_sent = if reportable.is_empty() {
            false
        } else {
            self.sender
                .send_report(&self.config.email_to, window_start, window_end, &reportable)
                .await?;
            for item in &reportable {
                self.record_report(
                    &item.tx,
                    &item.signal,
                    item.share_id,
                    &item.tweet_text,
                    window_start,
                    window_end,
                )
                .await?;
            }
            true
        };

        info!(
            inspected_txs = candidates.len(),
            reportable_txs = reportable.len(),
            email_sent,
            "trace monitor hourly report completed"
        );
        Ok(MonitorReportSummary {
            inspected_txs: candidates.len(),
            reportable_txs: reportable.len(),
            email_sent,
        })
    }

    async fn load_top_txs(
        &self,
        window_start: DateTime<Utc>,
        window_end: DateTime<Utc>,
    ) -> LedgerResult<Vec<TopTxCandidate>> {
        let top_n = self.config.top_n.clamp(1, 100);
        let rows = sqlx::query(
            r#"
            select
              d.tx_hash,
              max(d.block_number)::bigint as block_number,
              max(d.block_timestamp) as block_timestamp,
              sum(greatest(d.amount_in_raw, d.amount_out_raw))::text as volume_score_raw,
              count(distinct d.address)::bigint as address_count
            from trace_address_asset_deltas d
            where d.chain_id = $1
              and d.block_timestamp >= $2
              and d.block_timestamp < $3
              and not exists (
                select 1
                from trace_monitor_tx_reports r
                where r.chain_id = d.chain_id
                  and r.tx_hash = d.tx_hash
              )
            group by d.tx_hash
            having sum(greatest(d.amount_in_raw, d.amount_out_raw)) >= $4::numeric
            order by sum(greatest(d.amount_in_raw, d.amount_out_raw)) desc
            limit $5
            "#,
        )
        .bind(self.config.chain_id as i64)
        .bind(window_start)
        .bind(window_end)
        .bind(&self.config.min_raw_score)
        .bind(top_n as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;

        rows.into_iter()
            .map(|row| {
                Ok(TopTxCandidate {
                    tx_hash: row
                        .try_get("tx_hash")
                        .map_err(|err| LedgerError::Persistence(err.to_string()))?,
                    block_number: row
                        .try_get("block_number")
                        .map_err(|err| LedgerError::Persistence(err.to_string()))?,
                    block_timestamp: row
                        .try_get("block_timestamp")
                        .map_err(|err| LedgerError::Persistence(err.to_string()))?,
                    volume_score_raw: row
                        .try_get("volume_score_raw")
                        .map_err(|err| LedgerError::Persistence(err.to_string()))?,
                    address_count: row
                        .try_get("address_count")
                        .map_err(|err| LedgerError::Persistence(err.to_string()))?,
                })
            })
            .collect()
    }

    async fn analyze_tx(&self, tx_hash: &str) -> LedgerResult<AnalysisEnvelope> {
        let response = self
            .client
            .post(format!(
                "{}/analyses/tx",
                self.config.analysis_api_base_url.trim_end_matches('/')
            ))
            .json(&json!({
                "chain_id": self.config.chain_id,
                "tx_hash": tx_hash,
            }))
            .send()
            .await
            .map_err(|err| LedgerError::TransientIngestion(err.to_string()))?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(LedgerError::TransientIngestion(format!(
                "analysis api returned {status}: {body}"
            )));
        }
        response
            .json::<AnalysisEnvelope>()
            .await
            .map_err(|err| LedgerError::MalformedEvidence(err.to_string()))
    }

    async fn create_share(
        &self,
        envelope: &AnalysisEnvelope,
        tx: &TopTxCandidate,
        signal: &TxSignal,
    ) -> LedgerResult<Uuid> {
        let share_id = Uuid::new_v4();
        let snapshot = serde_json::to_value(envelope)
            .map_err(|err| LedgerError::MalformedEvidence(err.to_string()))?;
        let title = format!(
            "{} signal {}",
            signal.activity_kind,
            truncate_middle(&tx.tx_hash, 10)
        );
        let summary = format!(
            "{} Top monitor volume score(raw): {}.",
            signal.reason, tx.volume_score_raw
        );
        sqlx::query(
            r#"
            insert into tx_analysis_shares (
              share_id, chain_id, tx_hash, title, summary, analysis_snapshot, created_by
            )
            values ($1, $2, $3, $4, $5, $6, null)
            "#,
        )
        .bind(share_id)
        .bind(self.config.chain_id as i64)
        .bind(&tx.tx_hash)
        .bind(title)
        .bind(summary)
        .bind(SqlJson(snapshot))
        .execute(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        Ok(share_id)
    }

    async fn record_report(
        &self,
        tx: &TopTxCandidate,
        signal: &TxSignal,
        share_id: Uuid,
        tweet_text: &str,
        window_start: DateTime<Utc>,
        window_end: DateTime<Utc>,
    ) -> LedgerResult<()> {
        sqlx::query(
            r#"
            insert into trace_monitor_tx_reports (
              chain_id, tx_hash, window_start, window_end, activity_kind, volume_score_raw,
              share_id, tweet_text, email_to, sent_at, metadata
            )
            values ($1,$2,$3,$4,$5,$6::numeric,$7,$8,$9,now(),$10)
            on conflict (chain_id, tx_hash) do nothing
            "#,
        )
        .bind(self.config.chain_id as i64)
        .bind(&tx.tx_hash)
        .bind(window_start)
        .bind(window_end)
        .bind(&signal.activity_kind)
        .bind(&tx.volume_score_raw)
        .bind(share_id)
        .bind(tweet_text)
        .bind(&self.config.email_to)
        .bind(SqlJson(json!({
            "block_number": tx.block_number,
            "block_timestamp": tx.block_timestamp,
            "address_count": tx.address_count,
            "reason": signal.reason,
        })))
        .execute(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        Ok(())
    }
}

#[async_trait]
trait ReportEmailSender: Send + Sync {
    async fn send_report(
        &self,
        email: &str,
        window_start: DateTime<Utc>,
        window_end: DateTime<Utc>,
        reportable: &[ReportableTx],
    ) -> LedgerResult<()>;
}

struct LogReportEmailSender;

#[async_trait]
impl ReportEmailSender for LogReportEmailSender {
    async fn send_report(
        &self,
        email: &str,
        window_start: DateTime<Utc>,
        window_end: DateTime<Utc>,
        reportable: &[ReportableTx],
    ) -> LedgerResult<()> {
        info!(
            email,
            window_start = %window_start,
            window_end = %window_end,
            count = reportable.len(),
            delivery_mode = "log",
            "trace monitor report email logged"
        );
        for item in reportable {
            info!(tweet_text = %item.tweet_text, share_url = %item.share_url, "trace monitor report item");
        }
        Ok(())
    }
}

struct PostmarkReportEmailSender {
    client: Client,
    server_token: String,
    from_email: String,
    message_stream: String,
}

#[async_trait]
impl ReportEmailSender for PostmarkReportEmailSender {
    async fn send_report(
        &self,
        email: &str,
        window_start: DateTime<Utc>,
        window_end: DateTime<Utc>,
        reportable: &[ReportableTx],
    ) -> LedgerResult<()> {
        let subject = report_subject(reportable);
        let (text_body, html_body) = report_bodies(window_start, window_end, reportable);
        let response = self
            .client
            .post("https://api.postmarkapp.com/email")
            .header("Accept", "application/json")
            .header("Content-Type", "application/json")
            .header("X-Postmark-Server-Token", &self.server_token)
            .json(&json!({
                "From": self.from_email,
                "To": email,
                "Subject": subject,
                "TextBody": text_body,
                "HtmlBody": html_body,
                "MessageStream": self.message_stream,
            }))
            .send()
            .await
            .map_err(|err| LedgerError::TransientIngestion(err.to_string()))?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(LedgerError::TransientIngestion(format!(
                "postmark monitor report failed: {status} {body}"
            )));
        }
        Ok(())
    }
}

struct ResendReportEmailSender {
    client: Client,
    api_key: String,
    from_email: String,
}

#[async_trait]
impl ReportEmailSender for ResendReportEmailSender {
    async fn send_report(
        &self,
        email: &str,
        window_start: DateTime<Utc>,
        window_end: DateTime<Utc>,
        reportable: &[ReportableTx],
    ) -> LedgerResult<()> {
        let subject = report_subject(reportable);
        let (text_body, html_body) = report_bodies(window_start, window_end, reportable);
        let response = self
            .client
            .post("https://api.resend.com/emails")
            .bearer_auth(&self.api_key)
            .json(&json!({
                "from": self.from_email,
                "to": [email],
                "subject": subject,
                "text": text_body,
                "html": html_body,
            }))
            .send()
            .await
            .map_err(|err| LedgerError::TransientIngestion(err.to_string()))?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(LedgerError::TransientIngestion(format!(
                "resend monitor report failed: {status} {body}"
            )));
        }
        Ok(())
    }
}

fn build_report_sender(config: &MonitorReporterConfig) -> Arc<dyn ReportEmailSender> {
    let mode = config.email_delivery_mode.to_ascii_lowercase();
    match mode.as_str() {
        "postmark" => build_postmark_sender(config).unwrap_or_else(|| {
            warn!("postmark report email config missing; falling back to log delivery");
            Arc::new(LogReportEmailSender)
        }),
        "resend" => build_resend_sender(config).unwrap_or_else(|| {
            warn!("resend report email config missing; falling back to log delivery");
            Arc::new(LogReportEmailSender)
        }),
        "log" => Arc::new(LogReportEmailSender),
        _ => build_postmark_sender(config)
            .or_else(|| build_resend_sender(config))
            .unwrap_or_else(|| Arc::new(LogReportEmailSender)),
    }
}

fn build_postmark_sender(config: &MonitorReporterConfig) -> Option<Arc<dyn ReportEmailSender>> {
    Some(Arc::new(PostmarkReportEmailSender {
        client: Client::new(),
        server_token: config.postmark_server_token.clone()?,
        from_email: config.email_from.clone()?,
        message_stream: config.postmark_message_stream.clone(),
    }))
}

fn build_resend_sender(config: &MonitorReporterConfig) -> Option<Arc<dyn ReportEmailSender>> {
    Some(Arc::new(ResendReportEmailSender {
        client: Client::new(),
        api_key: config.resend_api_key.clone()?,
        from_email: config.email_from.clone()?,
    }))
}

fn detect_signal(analysis: &TransactionAnalysis) -> Option<TxSignal> {
    let scenario_kind = analysis
        .scenario_outputs
        .pointer("/storyline/kind")
        .and_then(Value::as_str)
        .map(str::to_ascii_lowercase);
    let recognizer_kind = analysis.recognizer_results.iter().find_map(|result| {
        let label = result.label.to_ascii_lowercase();
        if label.contains("liquidation") {
            Some("liquidation".to_owned())
        } else if label.contains("arbitrage") {
            Some("arbitrage".to_owned())
        } else {
            None
        }
    });
    let activity_kind = scenario_kind
        .or(recognizer_kind)
        .filter(|kind| kind == "liquidation" || kind == "arbitrage")?;
    if !has_positive_strategy_net(analysis) {
        return None;
    }
    Some(TxSignal {
        activity_kind,
        reason:
            "LedgerFlow detected liquidation/arbitrage behavior with positive strategy net effect."
                .to_owned(),
    })
}

fn has_positive_strategy_net(analysis: &TransactionAnalysis) -> bool {
    let Some(net_effect) = analysis.scenario_outputs.pointer("/storyline/net_effect") else {
        return false;
    };
    if net_effect
        .get("gained")
        .and_then(Value::as_array)
        .map(|items| !items.is_empty())
        .unwrap_or(false)
    {
        return true;
    }
    net_effect
        .get("rows")
        .and_then(Value::as_array)
        .map(|rows| {
            rows.iter().any(|row| {
                row.get("net_raw")
                    .and_then(Value::as_str)
                    .and_then(|value| value.parse::<i128>().ok())
                    .map(|value| value > 0)
                    .unwrap_or(false)
            })
        })
        .unwrap_or(false)
}

fn build_tweet_text(tx: &TopTxCandidate, signal: &TxSignal, share_url: &str) -> String {
    let base = format!(
        "LedgerFlow Base signal: {} candidate. tx {}. Positive strategy net detected; monitor volume score(raw) {}. Analysis: {}",
        signal.activity_kind,
        truncate_middle(&tx.tx_hash, 8),
        tx.volume_score_raw,
        share_url
    );
    truncate_chars(&base, 280)
}

fn report_subject(reportable: &[ReportableTx]) -> String {
    format!(
        "LedgerFlow monitor: {} liquidation/arbitrage signal{}",
        reportable.len(),
        if reportable.len() == 1 { "" } else { "s" }
    )
}

fn report_bodies(
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
    reportable: &[ReportableTx],
) -> (String, String) {
    let mut text = format!("Window: {window_start} - {window_end}\n\n");
    let mut html = format!(
        "<div style=\"font-family:ui-sans-serif,system-ui,sans-serif;line-height:1.5\"><p><strong>Window:</strong> {} - {}</p>",
        html_escape(&window_start.to_string()),
        html_escape(&window_end.to_string())
    );
    for item in reportable {
        text.push_str(&format!(
            "- {} {}\n  tx: {}\n  share: {}\n  tweet:\n{}\n\n",
            item.signal.activity_kind,
            item.tx.volume_score_raw,
            item.tx.tx_hash,
            item.share_url,
            item.tweet_text
        ));
        html.push_str(&format!(
            "<hr><p><strong>{}</strong> volume score(raw): {}</p><p>tx: <code>{}</code></p><p><a href=\"{}\">Open share</a></p><pre>{}</pre>",
            html_escape(&item.signal.activity_kind),
            html_escape(&item.tx.volume_score_raw),
            html_escape(&item.tx.tx_hash),
            html_escape(&item.share_url),
            html_escape(&item.tweet_text)
        ));
    }
    html.push_str("</div>");
    (text, html)
}

fn truncate_middle(value: &str, keep_each_side: usize) -> String {
    if value.len() <= keep_each_side.saturating_mul(2).saturating_add(3) {
        return value.to_owned();
    }
    format!(
        "{}...{}",
        &value[..keep_each_side],
        &value[value.len().saturating_sub(keep_each_side)..]
    )
}

fn truncate_chars(value: &str, max_chars: usize) -> String {
    let mut iter = value.chars();
    let truncated = iter.by_ref().take(max_chars).collect::<String>();
    if iter.next().is_some() {
        let mut trimmed = truncated
            .chars()
            .take(max_chars.saturating_sub(1))
            .collect::<String>();
        trimmed.push('…');
        trimmed
    } else {
        truncated
    }
}

fn html_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tweet_text_stays_under_x_limit() {
        let tx = TopTxCandidate {
            tx_hash: "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
                .to_owned(),
            block_number: 1,
            block_timestamp: None,
            volume_score_raw: "123456789000000000000000000000000000000000".to_owned(),
            address_count: 4,
        };
        let signal = TxSignal {
            activity_kind: "liquidation".to_owned(),
            reason: "test".to_owned(),
        };
        let text = build_tweet_text(&tx, &signal, "https://ledgerflow.top/share/tx/123");
        assert!(text.chars().count() <= 280);
    }
}
