use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row, types::Json};
use uuid::Uuid;

use crate::{
    core::errors::{LedgerError, LedgerResult},
    domain::{entities::AddressLabelAssertion, interfaces::AnalysisRepository},
    repositories::sqlx::SqlxAnalysisRepository,
};

const LABEL: &str = "actor:automation_candidate";
const SOURCE: &str = "behavior_window_classifier";
const MANUAL_SOURCE: &str = "manual_behavior_review";
const RULE_KEY: &str = "sender_frequency";

#[derive(Clone, Debug)]
pub struct BehaviorAssertionConfig {
    pub chain_id: u32,
    pub worker_id: String,
    pub source_worker_id: String,
    pub rule_version: String,
    pub lookback_blocks: u32,
    pub minimum_transaction_count: u64,
    pub minimum_active_windows: u64,
    pub minimum_span_blocks: u32,
    pub minimum_transactions_per_1000_blocks: f64,
    pub maximum_candidates: u32,
    pub assertion_ttl_hours: i64,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct BehaviorAssertionIteration {
    pub window_start_block: Option<u32>,
    pub window_end_block: Option<u32>,
    pub evaluated_addresses: u64,
    pub candidate_addresses: u64,
    pub assertions_written: u64,
    pub reviews_queued: u64,
    pub caught_up: bool,
}

#[derive(Clone, Debug)]
struct SenderWindowStats {
    address: String,
    first_seen_block: u32,
    last_seen_block: u32,
    transaction_count: u64,
    active_window_count: u64,
    sample_tx_hashes: Vec<String>,
}

impl SenderWindowStats {
    fn span_blocks(&self) -> u32 {
        self.last_seen_block
            .saturating_sub(self.first_seen_block)
            .saturating_add(1)
    }

    fn transactions_per_1000_blocks(&self) -> f64 {
        self.transaction_count as f64 * 1_000.0 / f64::from(self.span_blocks().max(1))
    }
}

#[derive(Clone)]
pub struct BehaviorAssertionService {
    repository: SqlxAnalysisRepository,
    pool: PgPool,
    config: BehaviorAssertionConfig,
}

impl BehaviorAssertionService {
    pub fn new(repository: SqlxAnalysisRepository, config: BehaviorAssertionConfig) -> Self {
        let pool = repository.pool().clone();
        Self {
            repository,
            pool,
            config,
        }
    }

    pub async fn process_once(&self) -> LedgerResult<BehaviorAssertionIteration> {
        let Some(window_end_block) = self.source_cursor_block().await? else {
            return Ok(BehaviorAssertionIteration {
                caught_up: true,
                ..BehaviorAssertionIteration::default()
            });
        };
        let window_start_block =
            window_end_block.saturating_sub(self.config.lookback_blocks.saturating_sub(1));
        let run_id = self.begin_run(window_start_block, window_end_block).await?;
        let result = self
            .process_window(window_start_block, window_end_block)
            .await;
        match result {
            Ok(mut iteration) => {
                self.complete_run(run_id, &iteration).await?;
                iteration.caught_up = true;
                Ok(iteration)
            }
            Err(error) => {
                let _ = self.fail_run(run_id, &error.to_string()).await;
                Err(error)
            }
        }
    }

    async fn process_window(
        &self,
        window_start_block: u32,
        window_end_block: u32,
    ) -> LedgerResult<BehaviorAssertionIteration> {
        let stats = self
            .load_sender_stats(window_start_block, window_end_block)
            .await?;
        let evaluated_addresses = stats.len() as u64;
        let candidates = stats
            .into_iter()
            .filter(|stats| qualifies(stats, &self.config))
            .collect::<Vec<_>>();
        let candidate_addresses = candidates.len() as u64;
        let now = Utc::now();
        let expires_at = now + Duration::hours(self.config.assertion_ttl_hours.max(1));
        let mut assertions = candidates
            .iter()
            .map(|stats| build_assertion(stats, &self.config, now, expires_at))
            .collect::<Vec<_>>();
        let reviewed = self
            .terminal_reviewed_fingerprints(
                &assertions
                    .iter()
                    .map(|assertion| assertion.claim_fingerprint.clone())
                    .collect::<Vec<_>>(),
            )
            .await?;
        assertions.retain(|assertion| !reviewed.contains(&assertion.claim_fingerprint));

        self.repository
            .upsert_address_label_assertions(&assertions)
            .await?;
        let fingerprints = assertions
            .iter()
            .map(|assertion| assertion.claim_fingerprint.clone())
            .collect::<Vec<_>>();
        self.supersede_previous_rule_versions(&fingerprints).await?;
        let reviews_queued = self.queue_reviews(&fingerprints).await?;

        Ok(BehaviorAssertionIteration {
            window_start_block: Some(window_start_block),
            window_end_block: Some(window_end_block),
            evaluated_addresses,
            candidate_addresses,
            assertions_written: assertions.len() as u64,
            reviews_queued,
            caught_up: true,
        })
    }

    async fn source_cursor_block(&self) -> LedgerResult<Option<u32>> {
        let block = sqlx::query_scalar::<_, Option<i64>>(
            r#"
            select last_processed_block
            from onchain_event_cursors
            where worker_id = $1 and chain_id = $2
            "#,
        )
        .bind(&self.config.source_worker_id)
        .bind(self.config.chain_id as i64)
        .fetch_optional(&self.pool)
        .await
        .map_err(persistence_error)?
        .flatten();
        Ok(block.map(|value| value.max(0) as u32))
    }

    async fn load_sender_stats(
        &self,
        window_start_block: u32,
        window_end_block: u32,
    ) -> LedgerResult<Vec<SenderWindowStats>> {
        let rows = sqlx::query(
            r#"
            select
              windows.address,
              min(windows.first_seen_block)::bigint as first_seen_block,
              max(windows.last_seen_block)::bigint as last_seen_block,
              sum(windows.observation_count)::bigint as transaction_count,
              count(*)::bigint as active_window_count,
              (array_agg(
                windows.sample_tx_hash
                order by windows.first_seen_block, windows.sample_tx_hash
              ) filter (where windows.sample_tx_hash <> ''))[1:8] as sample_tx_hashes
            from address_observation_windows windows
            join chain_addresses addresses
              on addresses.chain_id = windows.chain_id
             and addresses.address = windows.address
             and addresses.address_kind = 'eoa'
            where windows.worker_id = $1
              and windows.chain_id = $2
              and windows.observation_role = 'transaction_sender'
              and windows.window_end_block >= $3
              and windows.window_start_block <= $4
            group by windows.address
            having sum(windows.observation_count) >= $5
               and count(*) >= $6
            order by sum(windows.observation_count) desc, windows.address
            limit $7
            "#,
        )
        .bind(&self.config.source_worker_id)
        .bind(self.config.chain_id as i64)
        .bind(window_start_block as i64)
        .bind(window_end_block as i64)
        .bind(self.config.minimum_transaction_count as i64)
        .bind(self.config.minimum_active_windows as i64)
        .bind(self.config.maximum_candidates as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(persistence_error)?;
        rows.into_iter()
            .map(|row| {
                Ok(SenderWindowStats {
                    address: row.try_get::<String, _>("address")?,
                    first_seen_block: nonnegative_u32(row.try_get::<i64, _>("first_seen_block")?),
                    last_seen_block: nonnegative_u32(row.try_get::<i64, _>("last_seen_block")?),
                    transaction_count: nonnegative_u64(row.try_get::<i64, _>("transaction_count")?),
                    active_window_count: nonnegative_u64(
                        row.try_get::<i64, _>("active_window_count")?,
                    ),
                    sample_tx_hashes: row
                        .try_get::<Option<Vec<String>>, _>("sample_tx_hashes")?
                        .unwrap_or_default(),
                })
            })
            .collect::<Result<Vec<_>, sqlx::Error>>()
            .map_err(persistence_error)
    }

    async fn terminal_reviewed_fingerprints(
        &self,
        fingerprints: &[String],
    ) -> LedgerResult<std::collections::BTreeSet<String>> {
        if fingerprints.is_empty() {
            return Ok(std::collections::BTreeSet::new());
        }
        let rows = sqlx::query_scalar::<_, String>(
            r#"
            select assertions.claim_fingerprint
            from address_label_assertions assertions
            join behavior_assertion_reviews reviews
              on reviews.assertion_id = assertions.assertion_id
            where assertions.claim_fingerprint = any($1)
              and reviews.status in ('accepted', 'rejected', 'retracted')
            "#,
        )
        .bind(fingerprints)
        .fetch_all(&self.pool)
        .await
        .map_err(persistence_error)?;
        Ok(rows.into_iter().collect())
    }

    async fn supersede_previous_rule_versions(&self, fingerprints: &[String]) -> LedgerResult<()> {
        if fingerprints.is_empty() {
            return Ok(());
        }
        sqlx::query(
            r#"
            with current_assertions as (
              select assertion_id, chain_id, address, label_key, observed_at
              from address_label_assertions
              where claim_fingerprint = any($1)
            ),
            latest_previous as (
              select distinct on (current.assertion_id)
                current.assertion_id as current_id,
                previous.assertion_id as previous_id
              from current_assertions current
              join address_label_assertions previous
                on previous.chain_id = current.chain_id
               and previous.address = current.address
               and previous.label_key = current.label_key
               and previous.source_key = $2
               and previous.method_version <> $3
               and previous.assertion_id <> current.assertion_id
              order by current.assertion_id, previous.observed_at desc, previous.assertion_id desc
            ),
            link_current as (
              update address_label_assertions current
              set supersedes_assertion_id = latest.previous_id,
                  updated_at = now()
              from latest_previous latest
              where current.assertion_id = latest.current_id
              returning current.assertion_id
            ),
            superseded as (
              update address_label_assertions previous
              set status = 'superseded', updated_at = now()
              from current_assertions current
              where previous.chain_id = current.chain_id
                and previous.address = current.address
                and previous.label_key = current.label_key
                and previous.source_key = $2
                and previous.method_version <> $3
                and previous.assertion_id <> current.assertion_id
                and previous.status = 'active'
              returning previous.assertion_id
            ),
            closed_reviews as (
              update behavior_assertion_reviews reviews
              set status = 'retracted',
                  resolved_by = $4,
                  resolution_reason = 'Superseded by behavior rule version ' || $3,
                  reviewed_at = now(),
                  updated_at = now()
              from superseded
              where reviews.assertion_id = superseded.assertion_id
                and reviews.status = 'pending'
              returning reviews.review_id, reviews.assertion_id
            )
            insert into behavior_assertion_review_events (
              review_id, assertion_id, action, actor, rationale,
              previous_status, new_status, metadata
            )
            select
              review_id,
              assertion_id,
              'retract',
              $4,
              'Superseded by a newer behavior rule version',
              'pending',
              'retracted',
              jsonb_build_object('new_rule_version', $3::text)
            from closed_reviews
            "#,
        )
        .bind(fingerprints)
        .bind(SOURCE)
        .bind(&self.config.rule_version)
        .bind(&self.config.worker_id)
        .execute(&self.pool)
        .await
        .map_err(persistence_error)?;
        Ok(())
    }

    async fn queue_reviews(&self, fingerprints: &[String]) -> LedgerResult<u64> {
        if fingerprints.is_empty() {
            return Ok(0);
        }
        let result = sqlx::query(
            r#"
            with inserted as (
              insert into behavior_assertion_reviews (
                assertion_id, chain_id, address, label_key, priority,
                evidence_snapshot, created_at, updated_at
              )
              select
                assertions.assertion_id,
                assertions.chain_id,
                assertions.address,
                assertions.label_key,
                greatest(1, round((1.0 - assertions.confidence) * 100)::integer),
                assertions.evidence_summary,
                now(),
                now()
              from address_label_assertions assertions
              where assertions.claim_fingerprint = any($1)
              on conflict (assertion_id) do nothing
              returning review_id, assertion_id
            )
            insert into behavior_assertion_review_events (
              review_id, assertion_id, action, actor, previous_status,
              new_status, metadata
            )
            select
              review_id, assertion_id, 'queued', $2, null, 'pending',
              jsonb_build_object('rule_version', $3::text)
            from inserted
            "#,
        )
        .bind(fingerprints)
        .bind(&self.config.worker_id)
        .bind(&self.config.rule_version)
        .execute(&self.pool)
        .await
        .map_err(persistence_error)?;
        Ok(result.rows_affected())
    }

    async fn begin_run(&self, start_block: u32, end_block: u32) -> LedgerResult<Uuid> {
        let run_id = Uuid::new_v4();
        sqlx::query(
            r#"
            insert into behavior_analysis_runs (
              run_id, worker_id, source_worker_id, chain_id, rule_key,
              rule_version, window_start_block, window_end_block, status,
              thresholds, started_at, created_at, updated_at
            )
            values ($1,$2,$3,$4,$5,$6,$7,$8,'running',$9,now(),now(),now())
            "#,
        )
        .bind(run_id)
        .bind(&self.config.worker_id)
        .bind(&self.config.source_worker_id)
        .bind(self.config.chain_id as i64)
        .bind(RULE_KEY)
        .bind(&self.config.rule_version)
        .bind(start_block as i64)
        .bind(end_block as i64)
        .bind(Json(self.thresholds_json()))
        .execute(&self.pool)
        .await
        .map_err(persistence_error)?;
        Ok(run_id)
    }

    async fn complete_run(
        &self,
        run_id: Uuid,
        iteration: &BehaviorAssertionIteration,
    ) -> LedgerResult<()> {
        sqlx::query(
            r#"
            update behavior_analysis_runs
            set status = 'completed',
                evaluated_address_count = $2,
                candidate_count = $3,
                assertion_count = $4,
                queued_review_count = $5,
                last_error = null,
                completed_at = now(),
                updated_at = now()
            where run_id = $1
            "#,
        )
        .bind(run_id)
        .bind(iteration.evaluated_addresses as i64)
        .bind(iteration.candidate_addresses as i64)
        .bind(iteration.assertions_written as i64)
        .bind(iteration.reviews_queued as i64)
        .execute(&self.pool)
        .await
        .map_err(persistence_error)?;
        Ok(())
    }

    async fn fail_run(&self, run_id: Uuid, error: &str) -> LedgerResult<()> {
        sqlx::query(
            r#"
            update behavior_analysis_runs
            set status = 'failed', last_error = $2,
                completed_at = now(), updated_at = now()
            where run_id = $1
            "#,
        )
        .bind(run_id)
        .bind(truncate_text(error, 8_000))
        .execute(&self.pool)
        .await
        .map_err(persistence_error)?;
        Ok(())
    }

    fn thresholds_json(&self) -> Value {
        json!({
            "minimum_transaction_count": self.config.minimum_transaction_count,
            "minimum_active_windows": self.config.minimum_active_windows,
            "minimum_span_blocks": self.config.minimum_span_blocks,
            "minimum_transactions_per_1000_blocks": self.config.minimum_transactions_per_1000_blocks,
            "lookback_blocks": self.config.lookback_blocks,
            "assertion_ttl_hours": self.config.assertion_ttl_hours,
        })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BehaviorReviewItem {
    pub review_id: Uuid,
    pub assertion_id: Uuid,
    pub chain_id: u64,
    pub address: String,
    pub label: String,
    pub status: String,
    pub priority: i32,
    pub confidence: f32,
    pub method_version: String,
    pub valid_from_block: Option<u64>,
    pub valid_to_block: Option<u64>,
    pub expires_at: Option<DateTime<Utc>>,
    pub evidence: Value,
    pub resolved_by: Option<String>,
    pub resolution_reason: Option<String>,
    pub reviewed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone)]
pub struct BehaviorReviewService {
    pool: PgPool,
}

impl BehaviorReviewService {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn list(
        &self,
        chain_id: Option<u64>,
        status: Option<&str>,
        limit: u32,
    ) -> LedgerResult<Vec<BehaviorReviewItem>> {
        let status = status.map(normalize_review_status).transpose()?;
        let rows = sqlx::query(
            r#"
            select
              reviews.review_id,
              reviews.assertion_id,
              reviews.chain_id,
              reviews.address,
              reviews.label_key,
              reviews.status,
              reviews.priority,
              assertions.confidence,
              assertions.method_version,
              assertions.valid_from_block,
              assertions.valid_to_block,
              assertions.expires_at,
              assertions.evidence_summary,
              reviews.resolved_by,
              reviews.resolution_reason,
              reviews.reviewed_at,
              reviews.created_at,
              reviews.updated_at
            from behavior_assertion_reviews reviews
            join address_label_assertions assertions
              on assertions.assertion_id = reviews.assertion_id
            where ($1::bigint is null or reviews.chain_id = $1)
              and ($2::text is null or reviews.status = $2)
            order by
              case when reviews.status = 'pending' then 0 else 1 end,
              reviews.priority,
              reviews.created_at
            limit $3
            "#,
        )
        .bind(chain_id.map(|value| value as i64))
        .bind(status)
        .bind(limit.clamp(1, 200) as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(persistence_error)?;
        rows.into_iter().map(row_to_review_item).collect()
    }

    pub async fn decide(
        &self,
        review_id: Uuid,
        decision: &str,
        reviewer: &str,
        rationale: Option<&str>,
    ) -> LedgerResult<BehaviorReviewItem> {
        let decision = normalize_decision(decision)?;
        let reviewer = reviewer.trim();
        if reviewer.is_empty() {
            return Err(LedgerError::AnalysisInvariant(
                "reviewer must not be empty".to_owned(),
            ));
        }
        let rationale = rationale.map(str::trim).filter(|value| !value.is_empty());
        let mut tx = self.pool.begin().await.map_err(persistence_error)?;
        let row = sqlx::query(
            r#"
            select
              reviews.assertion_id,
              reviews.status,
              assertions.chain_id,
              assertions.address,
              assertions.label_key,
              assertions.evidence_summary
            from behavior_assertion_reviews reviews
            join address_label_assertions assertions
              on assertions.assertion_id = reviews.assertion_id
            where reviews.review_id = $1
            for update of reviews, assertions
            "#,
        )
        .bind(review_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(persistence_error)?
        .ok_or_else(|| LedgerError::NotFound(format!("behavior review {review_id}")))?;
        let assertion_id = row
            .try_get::<Uuid, _>("assertion_id")
            .map_err(persistence_error)?;
        let previous_status = row
            .try_get::<String, _>("status")
            .map_err(persistence_error)?;
        let chain_id = nonnegative_u64(
            row.try_get::<i64, _>("chain_id")
                .map_err(persistence_error)?,
        );
        let address = row
            .try_get::<String, _>("address")
            .map_err(persistence_error)?;
        let label = row
            .try_get::<String, _>("label_key")
            .map_err(persistence_error)?;
        let model_evidence = row
            .try_get::<Json<Value>, _>("evidence_summary")
            .map_err(persistence_error)?
            .0;
        let new_status = match decision {
            "accept" => "accepted",
            "reject" => "rejected",
            "retract" => "retracted",
            _ => unreachable!(),
        };

        sqlx::query(
            r#"
            update address_label_assertions
            set status = 'retracted', updated_at = now()
            where supersedes_assertion_id = $1
              and source_key = $2
              and status = 'active'
            "#,
        )
        .bind(assertion_id)
        .bind(MANUAL_SOURCE)
        .execute(&mut *tx)
        .await
        .map_err(persistence_error)?;

        if matches!(decision, "accept" | "reject") {
            let assertion_value = decision == "accept";
            let manual_assertion_id = Uuid::new_v4();
            let fingerprint = manual_review_fingerprint(review_id, decision);
            let evidence = json!({
                "review_id": review_id,
                "reviewed_assertion_id": assertion_id,
                "decision": decision,
                "reviewer": reviewer,
                "rationale": rationale,
                "model_evidence": model_evidence,
            });
            let returned = sqlx::query_scalar::<_, Uuid>(
                r#"
                insert into address_label_assertions (
                  assertion_id, claim_fingerprint, chain_id, address, label_key,
                  source_key, origin_kind, method_version, assertion_value,
                  confidence, status, observed_at, supersedes_assertion_id,
                  evidence_summary, created_at, updated_at
                )
                values (
                  $1,$2,$3,$4,$5,$6,'manual','manual_review_v1',$7,
                  1.0,'active',now(),$8,$9,now(),now()
                )
                on conflict (claim_fingerprint) do update set
                  assertion_value = excluded.assertion_value,
                  confidence = excluded.confidence,
                  status = 'active',
                  observed_at = now(),
                  evidence_summary = excluded.evidence_summary,
                  updated_at = now()
                returning assertion_id
                "#,
            )
            .bind(manual_assertion_id)
            .bind(&fingerprint)
            .bind(chain_id as i64)
            .bind(&address)
            .bind(&label)
            .bind(MANUAL_SOURCE)
            .bind(assertion_value)
            .bind(assertion_id)
            .bind(Json(evidence.clone()))
            .fetch_one(&mut *tx)
            .await
            .map_err(persistence_error)?;
            let evidence_hash = sha256_hex(&format!("{fingerprint}|{}", evidence));
            sqlx::query(
                r#"
                insert into assertion_evidence (
                  assertion_id, evidence_type, evidence_ref, evidence_hash,
                  payload, observed_at
                )
                values ($1,'manual_review',$2,$3,$4,now())
                on conflict (assertion_id, evidence_hash) do nothing
                "#,
            )
            .bind(returned)
            .bind(review_id.to_string())
            .bind(evidence_hash)
            .bind(Json(evidence))
            .execute(&mut *tx)
            .await
            .map_err(persistence_error)?;
        }

        let model_status = if decision == "accept" {
            "superseded"
        } else {
            "retracted"
        };
        sqlx::query(
            "update address_label_assertions set status = $2, updated_at = now() where assertion_id = $1",
        )
        .bind(assertion_id)
        .bind(model_status)
        .execute(&mut *tx)
        .await
        .map_err(persistence_error)?;
        sqlx::query(
            r#"
            update behavior_assertion_reviews
            set status = $2, resolved_by = $3, resolution_reason = $4,
                reviewed_at = now(), updated_at = now()
            where review_id = $1
            "#,
        )
        .bind(review_id)
        .bind(new_status)
        .bind(reviewer)
        .bind(rationale)
        .execute(&mut *tx)
        .await
        .map_err(persistence_error)?;
        sqlx::query(
            r#"
            insert into behavior_assertion_review_events (
              review_id, assertion_id, action, actor, rationale,
              previous_status, new_status, metadata
            )
            values ($1,$2,$3,$4,$5,$6,$7,$8)
            "#,
        )
        .bind(review_id)
        .bind(assertion_id)
        .bind(decision)
        .bind(reviewer)
        .bind(rationale)
        .bind(&previous_status)
        .bind(new_status)
        .bind(Json(json!({ "model_status": model_status })))
        .execute(&mut *tx)
        .await
        .map_err(persistence_error)?;
        tx.commit().await.map_err(persistence_error)?;
        self.get(review_id).await
    }

    async fn get(&self, review_id: Uuid) -> LedgerResult<BehaviorReviewItem> {
        let row = sqlx::query(
            r#"
            select
              reviews.review_id,
              reviews.assertion_id,
              reviews.chain_id,
              reviews.address,
              reviews.label_key,
              reviews.status,
              reviews.priority,
              assertions.confidence,
              assertions.method_version,
              assertions.valid_from_block,
              assertions.valid_to_block,
              assertions.expires_at,
              assertions.evidence_summary,
              reviews.resolved_by,
              reviews.resolution_reason,
              reviews.reviewed_at,
              reviews.created_at,
              reviews.updated_at
            from behavior_assertion_reviews reviews
            join address_label_assertions assertions
              on assertions.assertion_id = reviews.assertion_id
            where reviews.review_id = $1
            "#,
        )
        .bind(review_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(persistence_error)?
        .ok_or_else(|| LedgerError::NotFound(format!("behavior review {review_id}")))?;
        row_to_review_item(row)
    }
}

fn qualifies(stats: &SenderWindowStats, config: &BehaviorAssertionConfig) -> bool {
    stats.transaction_count >= config.minimum_transaction_count
        && stats.active_window_count >= config.minimum_active_windows
        && stats.span_blocks() >= config.minimum_span_blocks
        && stats.transactions_per_1000_blocks() >= config.minimum_transactions_per_1000_blocks
}

fn build_assertion(
    stats: &SenderWindowStats,
    config: &BehaviorAssertionConfig,
    now: DateTime<Utc>,
    expires_at: DateTime<Utc>,
) -> AddressLabelAssertion {
    let address = stats.address.to_ascii_lowercase();
    let confidence = confidence_for(stats, config);
    let claim_fingerprint = sha256_hex(&format!(
        "{}|{}|{}|{}|{}",
        config.chain_id, address, LABEL, SOURCE, config.rule_version
    ));
    AddressLabelAssertion {
        assertion_id: Uuid::new_v4(),
        claim_fingerprint,
        chain_id: u64::from(config.chain_id),
        address,
        label: LABEL.to_owned(),
        dimension: "behavior".to_owned(),
        exclusive_group: None,
        source: SOURCE.to_owned(),
        source_kind: "model".to_owned(),
        source_trust: 0.72,
        origin_kind: "model".to_owned(),
        method_version: config.rule_version.clone(),
        dataset_key: None,
        dataset_version: None,
        assertion_value: true,
        confidence,
        status: "active".to_owned(),
        valid_from_block: Some(u64::from(stats.first_seen_block)),
        valid_to_block: Some(u64::from(stats.last_seen_block)),
        observed_at: now,
        expires_at: Some(expires_at),
        evidence_json: json!({
            "claim_scope": "candidate_only",
            "canonical_source_worker": config.source_worker_id,
            "rule_key": RULE_KEY,
            "rule_version": config.rule_version,
            "statistical_window": {
                "first_seen_block": stats.first_seen_block,
                "last_seen_block": stats.last_seen_block,
                "span_blocks": stats.span_blocks(),
                "lookback_blocks": config.lookback_blocks,
            },
            "statistics": {
                "transaction_count": stats.transaction_count,
                "active_window_count": stats.active_window_count,
                "transactions_per_1000_blocks": stats.transactions_per_1000_blocks(),
            },
            "thresholds": {
                "minimum_transaction_count": config.minimum_transaction_count,
                "minimum_active_windows": config.minimum_active_windows,
                "minimum_span_blocks": config.minimum_span_blocks,
                "minimum_transactions_per_1000_blocks": config.minimum_transactions_per_1000_blocks,
            },
            "sample_transaction_hashes": stats.sample_tx_hashes,
            "requires_manual_review": true,
        }),
        created_at: now,
        updated_at: now,
    }
}

fn confidence_for(stats: &SenderWindowStats, config: &BehaviorAssertionConfig) -> f32 {
    let count_ratio =
        stats.transaction_count as f64 / config.minimum_transaction_count.max(1) as f64;
    let window_ratio =
        stats.active_window_count as f64 / config.minimum_active_windows.max(1) as f64;
    let rate_ratio = stats.transactions_per_1000_blocks()
        / config.minimum_transactions_per_1000_blocks.max(0.000_001);
    (0.55
        + 0.08 * count_ratio.log2().max(0.0).min(2.0)
        + 0.06 * window_ratio.log2().max(0.0).min(2.0)
        + 0.05 * rate_ratio.log2().max(0.0).min(2.0))
    .min(0.85) as f32
}

fn row_to_review_item(row: sqlx::postgres::PgRow) -> LedgerResult<BehaviorReviewItem> {
    Ok(BehaviorReviewItem {
        review_id: row.try_get("review_id").map_err(persistence_error)?,
        assertion_id: row.try_get("assertion_id").map_err(persistence_error)?,
        chain_id: nonnegative_u64(
            row.try_get::<i64, _>("chain_id")
                .map_err(persistence_error)?,
        ),
        address: row.try_get("address").map_err(persistence_error)?,
        label: row.try_get("label_key").map_err(persistence_error)?,
        status: row.try_get("status").map_err(persistence_error)?,
        priority: row.try_get("priority").map_err(persistence_error)?,
        confidence: row
            .try_get::<f64, _>("confidence")
            .map_err(persistence_error)? as f32,
        method_version: row.try_get("method_version").map_err(persistence_error)?,
        valid_from_block: row
            .try_get::<Option<i64>, _>("valid_from_block")
            .map_err(persistence_error)?
            .map(nonnegative_u64),
        valid_to_block: row
            .try_get::<Option<i64>, _>("valid_to_block")
            .map_err(persistence_error)?
            .map(nonnegative_u64),
        expires_at: row.try_get("expires_at").map_err(persistence_error)?,
        evidence: row
            .try_get::<Json<Value>, _>("evidence_summary")
            .map_err(persistence_error)?
            .0,
        resolved_by: row.try_get("resolved_by").map_err(persistence_error)?,
        resolution_reason: row
            .try_get("resolution_reason")
            .map_err(persistence_error)?,
        reviewed_at: row.try_get("reviewed_at").map_err(persistence_error)?,
        created_at: row.try_get("created_at").map_err(persistence_error)?,
        updated_at: row.try_get("updated_at").map_err(persistence_error)?,
    })
}

fn normalize_review_status(value: &str) -> LedgerResult<String> {
    let value = value.trim().to_ascii_lowercase();
    if matches!(
        value.as_str(),
        "pending" | "accepted" | "rejected" | "retracted"
    ) {
        Ok(value)
    } else {
        Err(LedgerError::AnalysisInvariant(format!(
            "invalid review status: {value}"
        )))
    }
}

fn normalize_decision(value: &str) -> LedgerResult<&'static str> {
    match value.trim().to_ascii_lowercase().as_str() {
        "accept" | "accepted" => Ok("accept"),
        "reject" | "rejected" => Ok("reject"),
        "retract" | "retracted" => Ok("retract"),
        other => Err(LedgerError::AnalysisInvariant(format!(
            "invalid review decision: {other}"
        ))),
    }
}

fn manual_review_fingerprint(review_id: Uuid, decision: &str) -> String {
    sha256_hex(&format!(
        "{review_id}|{MANUAL_SOURCE}|{decision}|manual_review_v1"
    ))
}

fn sha256_hex(value: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value.as_bytes());
    hex::encode(hasher.finalize())
}

fn nonnegative_u32(value: i64) -> u32 {
    value.max(0).min(i64::from(u32::MAX)) as u32
}

fn nonnegative_u64(value: i64) -> u64 {
    value.max(0) as u64
}

fn truncate_text(value: &str, max_chars: usize) -> String {
    value.chars().take(max_chars).collect()
}

fn persistence_error(error: sqlx::Error) -> LedgerError {
    LedgerError::Persistence(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> BehaviorAssertionConfig {
        BehaviorAssertionConfig {
            chain_id: 4663,
            worker_id: "behavior".to_owned(),
            source_worker_id: "facts".to_owned(),
            rule_version: "sender_frequency_v1".to_owned(),
            lookback_blocks: 50_000,
            minimum_transaction_count: 100,
            minimum_active_windows: 8,
            minimum_span_blocks: 1_000,
            minimum_transactions_per_1000_blocks: 2.0,
            maximum_candidates: 1_000,
            assertion_ttl_hours: 24,
        }
    }

    fn stats() -> SenderWindowStats {
        SenderWindowStats {
            address: "0x1111111111111111111111111111111111111111".to_owned(),
            first_seen_block: 100,
            last_seen_block: 50_099,
            transaction_count: 250,
            active_window_count: 80,
            sample_tx_hashes: vec!["0x01".to_owned()],
        }
    }

    #[test]
    fn statistical_candidate_requires_every_threshold() {
        let config = config();
        assert!(qualifies(&stats(), &config));
        let mut sparse = stats();
        sparse.transaction_count = 99;
        assert!(!qualifies(&sparse, &config));
        let mut short = stats();
        short.last_seen_block = 500;
        assert!(!qualifies(&short, &config));
    }

    #[test]
    fn assertion_is_expiring_candidate_with_a_stable_rule_fingerprint() {
        let config = config();
        let now = Utc::now();
        let first = build_assertion(&stats(), &config, now, now + Duration::hours(24));
        let second = build_assertion(&stats(), &config, now, now + Duration::hours(24));
        assert_eq!(first.claim_fingerprint, second.claim_fingerprint);
        assert_eq!(first.label, LABEL);
        assert!(first.expires_at.is_some());
        assert_eq!(first.evidence_json["claim_scope"], "candidate_only");
        assert!(first.confidence <= 0.85);
    }

    #[test]
    fn review_decisions_accept_present_and_past_tense_forms() {
        assert_eq!(normalize_decision("accepted").unwrap(), "accept");
        assert_eq!(normalize_decision("reject").unwrap(), "reject");
        assert_eq!(normalize_decision("retracted").unwrap(), "retract");
        assert!(normalize_decision("confirm_bot").is_err());
    }
}
