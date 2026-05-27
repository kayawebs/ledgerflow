use std::{collections::BTreeMap, sync::Arc};

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
    domain::{
        entities::{AddressAnalysisCandidate, EvidencePack, TransactionAnalysis},
        interfaces::AnalysisRepository,
    },
    repositories::sqlx::SqlxAnalysisRepository,
};

#[derive(Clone, Debug)]
pub struct MonitorReporterConfig {
    pub chain_id: u64,
    pub enabled: bool,
    pub interval_ms: u64,
    pub window_minutes: i64,
    pub top_n: u32,
    pub min_raw_score: String,
    pub notify_min_usd: f64,
    pub run_on_start: bool,
    pub analysis_api_base_url: String,
    pub web_base_url: String,
    pub email_to: String,
    pub email_from: Option<String>,
    pub email_delivery_mode: String,
    pub postmark_server_token: Option<String>,
    pub postmark_message_stream: String,
    pub resend_api_key: Option<String>,
    pub telegram_bot_token: Option<String>,
    pub telegram_chat_id: Option<String>,
}

#[derive(Clone)]
pub struct MonitorReporter {
    pool: PgPool,
    repository: SqlxAnalysisRepository,
    client: Client,
    sender: Arc<dyn ReportEmailSender>,
    telegram: Option<TelegramNotifier>,
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
    candidate_kind: Option<String>,
}

#[derive(Clone, Debug)]
struct ReportableTx {
    tx: TopTxCandidate,
    signal: TxSignal,
    share_id: Uuid,
    share_url: String,
    tweet_text: String,
}

#[derive(Clone, Debug, PartialEq)]
struct TxSignal {
    activity_kind: String,
    level: SignalLevel,
    reason: String,
    positive_strategy_net: bool,
    amount_summary: String,
    estimated_usd: f64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SignalLevel {
    Confirmed,
}

impl SignalLevel {
    fn as_str(self) -> &'static str {
        match self {
            SignalLevel::Confirmed => "confirmed",
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct AnalysisEnvelope {
    analysis: TransactionAnalysis,
    evidence_pack: EvidencePack,
}

fn top_tx_from_row(row: sqlx::postgres::PgRow) -> LedgerResult<TopTxCandidate> {
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
        candidate_kind: row
            .try_get("candidate_kind")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?,
    })
}

impl MonitorReporter {
    pub fn new(pool: PgPool, config: MonitorReporterConfig) -> Self {
        let sender = build_report_sender(&config);
        let telegram = build_telegram_notifier(&config);
        Self {
            repository: SqlxAnalysisRepository::from_pool(pool.clone()),
            pool,
            client: Client::new(),
            sender,
            telegram,
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
            if let Err(err) = self.enqueue_candidate_addresses(&tx).await {
                warn!(
                    tx_hash = tx.tx_hash,
                    error = %err,
                    "monitor address intelligence enqueue failed"
                );
            }
            match self.analyze_tx(&tx.tx_hash).await {
                Ok(envelope) => {
                    if let Err(err) = self
                        .enqueue_analysis_addresses(&tx, &envelope.analysis)
                        .await
                    {
                        warn!(
                            tx_hash = tx.tx_hash,
                            error = %err,
                            "monitor analyzed address intelligence enqueue failed"
                        );
                    }
                    let initiator_is_whale = if should_lookup_whale_label(
                        &envelope.analysis,
                        self.config.notify_min_usd,
                    ) {
                        self.has_whale_label(&envelope.analysis.from_address)
                            .await?
                    } else {
                        false
                    };
                    let Some(signal) = detect_confirmed_signal(
                        &envelope.analysis,
                        self.config.notify_min_usd,
                        initiator_is_whale,
                    ) else {
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

        let mut delivered = false;
        if let Some(telegram) = self.telegram.as_ref() {
            match telegram.send_alerts(&reportable).await {
                Ok(()) if !reportable.is_empty() => delivered = true,
                Ok(()) => {}
                Err(err) => warn!(error = %err, "telegram monitor alerts failed"),
            }
        }
        let mut email_sent = false;
        if !reportable.is_empty() {
            match self
                .sender
                .send_report(&self.config.email_to, window_start, window_end, &reportable)
                .await
            {
                Ok(()) => {
                    delivered = true;
                    email_sent = true;
                }
                Err(err) => warn!(error = %err, "monitor email alert delivery failed"),
            }
        }
        if delivered {
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
        }

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
        let candidate_rows = sqlx::query(
            r#"
            select
              c.tx_hash,
              c.block_number,
              c.block_timestamp,
              coalesce(sum(greatest(d.amount_in_raw, d.amount_out_raw)), 0)::text as volume_score_raw,
              count(distinct d.address)::bigint as address_count,
              c.candidate_kind
            from trace_monitor_candidates c
            left join trace_address_asset_deltas d
              on d.chain_id = c.chain_id
             and d.tx_hash = c.tx_hash
            where c.chain_id = $1
              and c.block_timestamp >= $2
              and c.block_timestamp < $3
              and not exists (
                select 1
                from trace_monitor_tx_reports r
                where r.chain_id = c.chain_id
                  and r.tx_hash = c.tx_hash
              )
            group by c.tx_hash, c.block_number, c.block_timestamp, c.candidate_kind, c.score
            order by c.score desc, c.block_timestamp desc
            limit $4
            "#,
        )
        .bind(self.config.chain_id as i64)
        .bind(window_start)
        .bind(window_end)
        .bind(top_n as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        let volume_rows = sqlx::query(
            r#"
            select
              d.tx_hash,
              max(d.block_number)::bigint as block_number,
              max(d.block_timestamp) as block_timestamp,
              sum(greatest(d.amount_in_raw, d.amount_out_raw))::text as volume_score_raw,
              count(distinct d.address)::bigint as address_count,
              null::text as candidate_kind
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

        let mut selected = BTreeMap::<String, TopTxCandidate>::new();
        for row in candidate_rows.into_iter().chain(volume_rows) {
            let candidate = top_tx_from_row(row)?;
            selected
                .entry(candidate.tx_hash.clone())
                .or_insert(candidate);
        }
        Ok(selected.into_values().collect())
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
            signal_title(signal),
            truncate_middle(&tx.tx_hash, 10)
        );
        let summary = format!(
            "{} level={} amount={} estimated_usd={:.2} positive_strategy_net={} Top monitor volume score(raw): {}.",
            signal.reason,
            signal.level.as_str(),
            signal.amount_summary,
            signal.estimated_usd,
            signal.positive_strategy_net,
            tx.volume_score_raw
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
            "signal_level": signal.level.as_str(),
            "positive_strategy_net": signal.positive_strategy_net,
            "amount_summary": signal.amount_summary,
            "estimated_usd": signal.estimated_usd,
            "candidate_kind": tx.candidate_kind,
        })))
        .execute(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        Ok(())
    }

    async fn enqueue_candidate_addresses(&self, tx: &TopTxCandidate) -> LedgerResult<u64> {
        let rows = sqlx::query(
            r#"
            select address, asset_type, token_address, metadata
            from trace_address_asset_deltas
            where chain_id = $1 and tx_hash = $2
            limit 500
            "#,
        )
        .bind(self.config.chain_id as i64)
        .bind(&tx.tx_hash)
        .fetch_all(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;

        let mut candidates = BTreeMap::<String, AddressAnalysisCandidate>::new();
        for row in rows {
            let address = row
                .try_get::<String, _>("address")
                .map_err(|err| LedgerError::Persistence(err.to_string()))?;
            let asset_type = row
                .try_get::<String, _>("asset_type")
                .map_err(|err| LedgerError::Persistence(err.to_string()))?;
            let token_address = row
                .try_get::<String, _>("token_address")
                .map_err(|err| LedgerError::Persistence(err.to_string()))?;
            let metadata = row
                .try_get::<SqlJson<Value>, _>("metadata")
                .map_err(|err| LedgerError::Persistence(err.to_string()))?
                .0;
            insert_monitor_candidate(
                &mut candidates,
                &address,
                30,
                json!({
                    "source": "trace_monitor",
                    "tx_hash": tx.tx_hash,
                    "block_number": tx.block_number,
                    "roles": ["monitor_delta_address"],
                }),
            );
            if asset_type == "erc20" {
                insert_monitor_candidate(
                    &mut candidates,
                    &token_address,
                    15,
                    json!({
                        "source": "trace_monitor",
                        "tx_hash": tx.tx_hash,
                        "block_number": tx.block_number,
                        "roles": ["erc20_token"],
                    }),
                );
            }
            if let Some(tx_from) = metadata.get("tx_from").and_then(Value::as_str) {
                insert_monitor_candidate(
                    &mut candidates,
                    tx_from,
                    20,
                    json!({
                        "source": "trace_monitor",
                        "tx_hash": tx.tx_hash,
                        "block_number": tx.block_number,
                        "roles": ["tx_from"],
                    }),
                );
            }
            if let Some(tx_to) = metadata.get("tx_to").and_then(Value::as_str) {
                insert_monitor_candidate(
                    &mut candidates,
                    tx_to,
                    25,
                    json!({
                        "source": "trace_monitor",
                        "tx_hash": tx.tx_hash,
                        "block_number": tx.block_number,
                        "roles": ["tx_to"],
                    }),
                );
            }
        }

        let candidates = candidates.into_values().collect::<Vec<_>>();
        if candidates.is_empty() {
            return Ok(0);
        }
        self.repository
            .enqueue_address_analysis_candidates(self.config.chain_id, &candidates)
            .await
    }

    async fn enqueue_analysis_addresses(
        &self,
        tx: &TopTxCandidate,
        analysis: &TransactionAnalysis,
    ) -> LedgerResult<u64> {
        let mut candidates = BTreeMap::<String, AddressAnalysisCandidate>::new();
        let base = |roles: Vec<&str>| {
            json!({
                "source": "trace_monitor",
                "tx_hash": tx.tx_hash,
                "block_number": tx.block_number,
                "reason": "top_volume_candidate",
                "roles": roles,
            })
        };

        insert_monitor_candidate(
            &mut candidates,
            &analysis.from_address,
            20,
            base(vec!["tx_from"]),
        );
        if let Some(to_address) = analysis.to_address.as_deref() {
            insert_monitor_candidate(
                &mut candidates,
                to_address,
                25,
                base(vec!["tx_to", "executor"]),
            );
        }

        for participant in &analysis.normalized_execution_facts.participants {
            insert_monitor_candidate(&mut candidates, participant, 60, base(vec!["participant"]));
        }
        for log in &analysis.normalized_execution_facts.log_summaries {
            if let Some(address) = log.get("address").and_then(Value::as_str) {
                let mut roles = vec!["log_emitter"];
                if is_dex_event_log(log) {
                    roles.push("suspected_pool");
                }
                if is_liquidation_event_log(log) {
                    roles.push("suspected_protocol");
                }
                insert_monitor_candidate(&mut candidates, address, 35, base(roles));
            }
        }
        for flow in &analysis.derived_flows {
            insert_monitor_candidate(
                &mut candidates,
                &flow.from_address,
                40,
                base(vec!["flow_sender"]),
            );
            insert_monitor_candidate(
                &mut candidates,
                &flow.to_address,
                40,
                base(vec!["flow_receiver"]),
            );
            if let Some(token_address) = flow.token_address.as_deref() {
                insert_monitor_candidate(
                    &mut candidates,
                    token_address,
                    15,
                    base(vec!["erc20_token", "token"]),
                );
            }
        }
        if let Some(items) = analysis
            .normalized_execution_facts
            .derived_facts
            .get("approval_edges")
            .and_then(Value::as_array)
        {
            for item in items.iter().take(100) {
                if let Some(spender) = item.get("spender").and_then(Value::as_str) {
                    insert_monitor_candidate(
                        &mut candidates,
                        spender,
                        22,
                        base(vec!["approval_spender", "suspected_router"]),
                    );
                }
                if let Some(token) = item.get("token_address").and_then(Value::as_str) {
                    insert_monitor_candidate(
                        &mut candidates,
                        token,
                        15,
                        base(vec!["erc20_token", "token"]),
                    );
                }
            }
        }
        if let Some(items) = analysis
            .normalized_execution_facts
            .derived_facts
            .get("execution_timeline")
            .and_then(Value::as_array)
        {
            for item in items.iter().take(200) {
                if let Some(from) = item.get("from_address").and_then(Value::as_str) {
                    insert_monitor_candidate(&mut candidates, from, 65, base(vec!["call_sender"]));
                }
                if let Some(to) = item.get("to_address").and_then(Value::as_str) {
                    insert_monitor_candidate(
                        &mut candidates,
                        to,
                        45,
                        base(vec!["call_target", "suspected_router_or_protocol"]),
                    );
                }
            }
        }
        if let Some(contracts) = analysis
            .normalized_execution_facts
            .derived_facts
            .get("touched_contracts")
            .and_then(Value::as_array)
        {
            for contract in contracts.iter().filter_map(Value::as_str) {
                insert_monitor_candidate(
                    &mut candidates,
                    contract,
                    50,
                    base(vec!["touched_contract", "suspected_protocol"]),
                );
            }
        }

        let candidates = candidates.into_values().collect::<Vec<_>>();
        if candidates.is_empty() {
            return Ok(0);
        }
        self.repository
            .enqueue_address_analysis_candidates(self.config.chain_id, &candidates)
            .await
    }

    async fn has_whale_label(&self, address: &str) -> LedgerResult<bool> {
        let labels = self
            .repository
            .list_address_labels(self.config.chain_id, &[address.to_ascii_lowercase()])
            .await?;
        Ok(labels
            .iter()
            .any(|label| label.label.eq_ignore_ascii_case("actor:whale")))
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

#[derive(Clone)]
struct TelegramNotifier {
    client: Client,
    bot_token: String,
    chat_id: String,
}

impl TelegramNotifier {
    async fn send_alerts(&self, reportable: &[ReportableTx]) -> LedgerResult<()> {
        for item in reportable {
            let response = self
                .client
                .post(format!(
                    "https://api.telegram.org/bot{}/sendMessage",
                    self.bot_token
                ))
                .json(&json!({
                    "chat_id": self.chat_id,
                    "text": telegram_message(item),
                    "disable_web_page_preview": true,
                }))
                .send()
                .await
                .map_err(|err| LedgerError::TransientIngestion(err.to_string()))?;
            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                return Err(LedgerError::TransientIngestion(format!(
                    "telegram alert failed: {status} {body}"
                )));
            }
        }
        Ok(())
    }
}

fn build_telegram_notifier(config: &MonitorReporterConfig) -> Option<TelegramNotifier> {
    Some(TelegramNotifier {
        client: Client::new(),
        bot_token: config.telegram_bot_token.clone()?,
        chat_id: config.telegram_chat_id.clone()?,
    })
}

#[derive(Clone, Debug)]
struct PricedMovement {
    symbol: String,
    amount_decimal: String,
    price_decimal: String,
    value_usd: f64,
}

fn detect_confirmed_signal(
    analysis: &TransactionAnalysis,
    notify_min_usd: f64,
    initiator_is_whale: bool,
) -> Option<TxSignal> {
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
    let primary_movement = largest_priced_movement(analysis)?;
    let activity_kind = scenario_kind.or(recognizer_kind);
    if activity_kind.as_deref() == Some("liquidation") {
        return Some(TxSignal {
            activity_kind: "liquidation".to_owned(),
            level: SignalLevel::Confirmed,
            reason: "Protocol liquidation event confirmed from transaction analysis.".to_owned(),
            positive_strategy_net: estimated_subject_net_usd(analysis)
                .map(|value| value > 0.0)
                .unwrap_or(false),
            amount_summary: format_movement(&primary_movement),
            estimated_usd: primary_movement.value_usd,
        });
    }

    if activity_kind.as_deref() == Some("arbitrage") || is_probable_arbitrage_route(analysis) {
        let net_usd = estimated_subject_net_usd(analysis)?;
        if net_usd <= 0.0 {
            return None;
        }
        return Some(TxSignal {
            activity_kind: "arbitrage".to_owned(),
            level: SignalLevel::Confirmed,
            reason: "Arbitrage-like route confirmed with positive estimated subject value change."
                .to_owned(),
            positive_strategy_net: true,
            amount_summary: format!(
                "est. net +${:.2}; largest leg {}",
                net_usd,
                format_movement(&primary_movement)
            ),
            estimated_usd: net_usd,
        });
    }

    if initiator_is_whale
        && is_swap_activity(analysis)
        && primary_movement.value_usd >= notify_min_usd
    {
        return Some(TxSignal {
            activity_kind: "whale-buy".to_owned(),
            level: SignalLevel::Confirmed,
            reason: "Watched address swap confirmed above the configured USD threshold.".to_owned(),
            positive_strategy_net: estimated_subject_net_usd(analysis)
                .map(|value| value > 0.0)
                .unwrap_or(false),
            amount_summary: format_movement(&primary_movement),
            estimated_usd: primary_movement.value_usd,
        });
    }

    (primary_movement.value_usd >= notify_min_usd).then(|| TxSignal {
        activity_kind: "large-transfer".to_owned(),
        level: SignalLevel::Confirmed,
        reason: "Priced asset movement exceeds the configured USD alert threshold.".to_owned(),
        positive_strategy_net: estimated_subject_net_usd(analysis)
            .map(|value| value > 0.0)
            .unwrap_or(false),
        amount_summary: format_movement(&primary_movement),
        estimated_usd: primary_movement.value_usd,
    })
}

fn should_lookup_whale_label(analysis: &TransactionAnalysis, notify_min_usd: f64) -> bool {
    is_swap_activity(analysis)
        && largest_priced_movement(analysis)
            .map(|movement| movement.value_usd >= notify_min_usd)
            .unwrap_or(false)
}

fn largest_priced_movement(analysis: &TransactionAnalysis) -> Option<PricedMovement> {
    analysis
        .derived_flows
        .iter()
        .filter_map(|flow| {
            let estimate = flow.metadata.get("estimated_usd")?;
            let value_usd = estimate
                .get("value_decimal")?
                .as_str()?
                .parse::<f64>()
                .ok()?;
            let amount_decimal = estimate.get("amount_decimal")?.as_str()?.to_owned();
            let price_decimal = estimate.get("price_decimal")?.as_str()?.to_owned();
            let symbol = flow
                .metadata
                .pointer("/token/symbol")
                .and_then(Value::as_str)
                .unwrap_or("TOKEN")
                .to_owned();
            Some(PricedMovement {
                symbol,
                amount_decimal,
                price_decimal,
                value_usd,
            })
        })
        .max_by(|left, right| left.value_usd.total_cmp(&right.value_usd))
}

fn format_movement(movement: &PricedMovement) -> String {
    format!(
        "{} {} (~${:.2}, ${}/token)",
        movement.amount_decimal, movement.symbol, movement.value_usd, movement.price_decimal
    )
}

fn estimated_subject_net_usd(analysis: &TransactionAnalysis) -> Option<f64> {
    let scope = analysis
        .scenario_outputs
        .pointer("/storyline/subject_scope/addresses")
        .and_then(Value::as_array)?
        .iter()
        .filter_map(Value::as_str)
        .map(str::to_ascii_lowercase)
        .collect::<std::collections::BTreeSet<_>>();
    if scope.is_empty() {
        return None;
    }
    let mut net = 0.0;
    let mut priced_boundary_flow = false;
    for flow in &analysis.derived_flows {
        let from_scope = scope.contains(&flow.from_address.to_ascii_lowercase());
        let to_scope = scope.contains(&flow.to_address.to_ascii_lowercase());
        if from_scope == to_scope {
            continue;
        }
        let Some(value) = flow
            .metadata
            .pointer("/estimated_usd/value_decimal")
            .and_then(Value::as_str)
            .and_then(|value| value.parse::<f64>().ok())
        else {
            continue;
        };
        priced_boundary_flow = true;
        net += if to_scope { value } else { -value };
    }
    priced_boundary_flow.then_some(net)
}

fn is_swap_activity(analysis: &TransactionAnalysis) -> bool {
    analysis
        .scenario_outputs
        .pointer("/storyline/kind")
        .and_then(Value::as_str)
        .map(|kind| kind.eq_ignore_ascii_case("swap"))
        .unwrap_or(false)
}

fn is_probable_arbitrage_route(analysis: &TransactionAnalysis) -> bool {
    let labels = analysis
        .recognizer_results
        .iter()
        .map(|result| result.label.to_ascii_lowercase())
        .collect::<Vec<_>>();
    let has_route_label = labels.iter().any(|label| {
        matches!(
            label.as_str(),
            "arbitrage_like_activity" | "swap_like_activity" | "approval_then_swap_like"
        )
    });
    let token_count = distinct_token_count(analysis);
    let address_count = distinct_flow_address_count(analysis);
    has_route_label && analysis.derived_flows.len() >= 2 && token_count >= 2 && address_count >= 3
}

fn is_dex_event_log(log: &Value) -> bool {
    const UNI_V2_SWAP: &str = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822";
    const UNI_V2_SYNC: &str = "0x1c411e9a96e0712416f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1";
    const UNI_V2_MINT: &str = "0x4c209b5fc8ad50758f13e2e1088ba56f10dff1ff73af114a4f0efc9c4b1d797";
    const UNI_V2_BURN: &str = "0xdccd412f0b1252819cb1fd330b93224ca42612892bb3f4f789976e6d81936496";
    const UNI_V3_SWAP: &str = "0xc42079f94a6350d7e6235f291749249f928cc2ac818eb64b52398e603a73a2f";
    const UNI_V3_MINT: &str = "0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488daaf0c7f8d9d54eaf";
    const UNI_V3_BURN: &str = "0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c";
    const UNI_V3_COLLECT: &str =
        "0x70935338e69775456f7e439f86c08d6f7828d3e1118c1b7802b2039f521c9f0f";
    let topic = log
        .get("topic_0")
        .and_then(Value::as_str)
        .map(str::to_ascii_lowercase);
    matches!(
        topic.as_deref(),
        Some(UNI_V2_SWAP)
            | Some(UNI_V2_SYNC)
            | Some(UNI_V2_MINT)
            | Some(UNI_V2_BURN)
            | Some(UNI_V3_SWAP)
            | Some(UNI_V3_MINT)
            | Some(UNI_V3_BURN)
            | Some(UNI_V3_COLLECT)
    )
}

fn is_liquidation_event_log(log: &Value) -> bool {
    const MORPHO_BLUE_LIQUIDATE: &str =
        "0xa4946ede45d0c6f06a0f5ce92c9ad3b4751452d2fe0e25010783bcab57a67e41";
    const AAVE_LIQUIDATION_CALL: &str =
        "0xe413a321e8681d83188be0d2364605a8f594ac97918daee13129e6b4f89b8f3";
    let topic = log
        .get("topic_0")
        .and_then(Value::as_str)
        .map(str::to_ascii_lowercase);
    matches!(
        topic.as_deref(),
        Some(MORPHO_BLUE_LIQUIDATE) | Some(AAVE_LIQUIDATION_CALL)
    )
}

fn distinct_token_count(analysis: &TransactionAnalysis) -> usize {
    analysis
        .derived_flows
        .iter()
        .map(|flow| {
            flow.token_address
                .clone()
                .unwrap_or_else(|| "native".to_owned())
                .to_ascii_lowercase()
        })
        .collect::<std::collections::BTreeSet<_>>()
        .len()
}

fn distinct_flow_address_count(analysis: &TransactionAnalysis) -> usize {
    analysis
        .derived_flows
        .iter()
        .flat_map(|flow| {
            [
                flow.from_address.to_ascii_lowercase(),
                flow.to_address.to_ascii_lowercase(),
            ]
        })
        .collect::<std::collections::BTreeSet<_>>()
        .len()
}

fn signal_title(signal: &TxSignal) -> String {
    format!("{} {}", signal.level.as_str(), signal.activity_kind)
}

fn insert_monitor_candidate(
    candidates: &mut BTreeMap<String, AddressAnalysisCandidate>,
    address: &str,
    priority: i32,
    metadata: Value,
) {
    let Some(address) = normalize_monitor_address(address) else {
        return;
    };
    candidates
        .entry(address.clone())
        .and_modify(|candidate| {
            candidate.priority = candidate.priority.min(priority);
            candidate.metadata =
                merge_monitor_metadata(candidate.metadata.clone(), metadata.clone());
        })
        .or_insert(AddressAnalysisCandidate {
            address,
            priority,
            metadata,
        });
}

fn normalize_monitor_address(address: &str) -> Option<String> {
    let address = address.trim().to_ascii_lowercase();
    if address.len() == 42 && address.starts_with("0x") {
        Some(address)
    } else {
        None
    }
}

fn merge_monitor_metadata(left: Value, right: Value) -> Value {
    match (left, right) {
        (Value::Object(mut left), Value::Object(right)) => {
            for (key, value) in right {
                left.insert(key, value);
            }
            Value::Object(left)
        }
        (_, right) => right,
    }
}

fn build_tweet_text(tx: &TopTxCandidate, signal: &TxSignal, share_url: &str) -> String {
    let base = format!(
        "LedgerFlow Base: confirmed {}. {} Estimated USD ${:.2}. Tx {}. Analysis: {}",
        signal.activity_kind,
        signal.amount_summary,
        signal.estimated_usd,
        truncate_middle(&tx.tx_hash, 8),
        share_url
    );
    truncate_chars(&base, 280)
}

fn telegram_message(item: &ReportableTx) -> String {
    format!(
        "[CONFIRMED {}]\n{}\nEstimated value: ${:.2}\nTx: {}\n{}",
        item.signal.activity_kind.to_ascii_uppercase(),
        item.signal.amount_summary,
        item.signal.estimated_usd,
        truncate_middle(&item.tx.tx_hash, 10),
        item.share_url
    )
}

fn report_subject(reportable: &[ReportableTx]) -> String {
    format!(
        "LedgerFlow monitor: {} confirmed alert{}",
        reportable.len(),
        if reportable.len() == 1 { "" } else { "s" },
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
            "- [{}] {}\n  amount: {}\n  estimated USD: ${:.2}\n  reason: {}\n  tx: {}\n  share: {}\n\n",
            item.signal.level.as_str(),
            item.signal.activity_kind,
            item.signal.amount_summary,
            item.signal.estimated_usd,
            item.signal.reason,
            item.tx.tx_hash,
            item.share_url,
        ));
        html.push_str(&format!(
            "<hr><p><strong>[{}] {}</strong></p><p>{} (~${:.2})</p><p>{}</p><p>tx: <code>{}</code></p><p><a href=\"{}\">Open analysis</a></p>",
            html_escape(item.signal.level.as_str()),
            html_escape(&item.signal.activity_kind),
            html_escape(&item.signal.amount_summary),
            item.signal.estimated_usd,
            html_escape(&item.signal.reason),
            html_escape(&item.tx.tx_hash),
            html_escape(&item.share_url),
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
            candidate_kind: Some("liquidation_event".to_owned()),
        };
        let signal = TxSignal {
            activity_kind: "liquidation".to_owned(),
            level: SignalLevel::Confirmed,
            reason: "test".to_owned(),
            positive_strategy_net: true,
            amount_summary: "12.5 WETH (~$32000.00, $2560/token)".to_owned(),
            estimated_usd: 32_000.0,
        };
        let text = build_tweet_text(&tx, &signal, "https://ledgerflow.top/share/tx/123");
        assert!(text.chars().count() <= 280);
    }
}
