use std::{collections::BTreeSet, sync::Arc};

use num_bigint::BigInt;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row, types::Json};
use tracing::{info, warn};

use crate::{
    core::errors::{LedgerError, LedgerResult},
    domain::entities::TransactionAnalysis,
    ingestion::rpc::RpcClient,
    pilot::profit::{PROFIT_METHOD, verify_deterministic_profit_for_scope},
    services::analysis::AnalysisService,
};

pub const ECONOMIC_VERIFIER_RULESET_ID: &str = "ledgerflow.economic-verifier.v1";
pub const ECONOMIC_VERIFIER_METHOD_VERSION: &str = "seed_sender_accounting_v1";

#[derive(Clone, Debug)]
pub struct EconomicVerifierConfig {
    pub chain_id: u64,
    pub worker_id: String,
    pub batch_size: u32,
    pub maximum_new_jobs_per_pass: u32,
    pub maximum_pending_jobs: u32,
    pub maximum_candidates_per_seed: u32,
    pub maximum_attempts: u32,
    pub retry_delay_seconds: i64,
    pub stale_claim_seconds: i64,
}

#[derive(Clone)]
pub struct EconomicVerifierService {
    pool: PgPool,
    rpc: Arc<dyn RpcClient>,
    analysis_service: Arc<AnalysisService>,
    config: EconomicVerifierConfig,
}

#[derive(Clone, Debug)]
struct VerificationJob {
    chain_id: u64,
    tx_hash: String,
    subject_address: String,
    attempts: u32,
    source_digest: String,
}

#[derive(Clone, Debug)]
struct CandidateContext {
    detection_source: String,
    score: i32,
    swap_log_count: i32,
    distinct_pool_count: i32,
    route_fingerprint: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VerificationResultStatus {
    Verified,
    InsufficientEvidence,
    Dismissed,
}

impl VerificationResultStatus {
    pub fn key(self) -> &'static str {
        match self {
            Self::Verified => "verified",
            Self::InsufficientEvidence => "insufficient_evidence",
            Self::Dismissed => "dismissed",
        }
    }

    fn candidate_status(self) -> &'static str {
        match self {
            Self::Verified => "verified_profit",
            Self::InsufficientEvidence => "insufficient_evidence",
            Self::Dismissed => "dismissed",
        }
    }
}

#[derive(Clone, Debug)]
struct VerificationOutcome {
    status: VerificationResultStatus,
    severity: &'static str,
    confidence: f64,
    conclusion: String,
    profit_recipient: Option<String>,
    profit_token_address: Option<String>,
    profit_amount_raw: Option<String>,
    gas_cost_raw: Option<String>,
    inputs_digest: String,
    evidence: Value,
    limitations: Vec<String>,
}

#[derive(Clone, Debug, Default)]
pub struct EconomicVerifierIteration {
    pub recovered_jobs: u64,
    pub enqueued_jobs: u64,
    pub completed_jobs: u64,
    pub verified_jobs: u64,
    pub insufficient_evidence_jobs: u64,
    pub dismissed_jobs: u64,
    pub failed_jobs: u64,
}

impl EconomicVerifierService {
    pub fn new(
        pool: PgPool,
        rpc: Arc<dyn RpcClient>,
        analysis_service: Arc<AnalysisService>,
        config: EconomicVerifierConfig,
    ) -> Self {
        Self {
            pool,
            rpc,
            analysis_service,
            config,
        }
    }

    pub async fn validate_rpc_chain(&self) -> LedgerResult<()> {
        let actual = self.rpc.get_chain_id().await?;
        if actual != self.config.chain_id {
            return Err(LedgerError::AnalysisInvariant(format!(
                "economic verifier configured for chain {} but RPC returned {actual}",
                self.config.chain_id
            )));
        }
        Ok(())
    }

    pub async fn process_once(&self) -> LedgerResult<EconomicVerifierIteration> {
        let mut iteration = EconomicVerifierIteration {
            recovered_jobs: self.recover_stale_jobs().await?,
            enqueued_jobs: self.enqueue_seed_jobs().await?,
            ..EconomicVerifierIteration::default()
        };
        for _ in 0..self.config.batch_size.max(1) {
            let Some(job) = self.claim_next_job().await? else {
                break;
            };
            match self.verify_job(&job).await {
                Ok(outcome) => {
                    self.persist_outcome(&job, &outcome).await?;
                    iteration.completed_jobs += 1;
                    match outcome.status {
                        VerificationResultStatus::Verified => iteration.verified_jobs += 1,
                        VerificationResultStatus::InsufficientEvidence => {
                            iteration.insufficient_evidence_jobs += 1
                        }
                        VerificationResultStatus::Dismissed => iteration.dismissed_jobs += 1,
                    }
                    info!(
                        chain_id = job.chain_id,
                        tx_hash = job.tx_hash,
                        subject_address = job.subject_address,
                        result_status = outcome.status.key(),
                        "economic verification completed"
                    );
                }
                Err(error) => {
                    self.mark_failed(&job, &error).await?;
                    iteration.failed_jobs += 1;
                    warn!(
                        chain_id = job.chain_id,
                        tx_hash = job.tx_hash,
                        attempts = job.attempts,
                        error = %error,
                        "economic verification failed"
                    );
                }
            }
        }
        Ok(iteration)
    }

    async fn recover_stale_jobs(&self) -> LedgerResult<u64> {
        let result = sqlx::query(
            r#"
            update economic_verification_jobs
            set status = 'failed', available_at = now(),
                last_error = 'recovered stale running claim', updated_at = now()
            where chain_id = $1 and status = 'running'
              and claimed_at < now() - ($2::text || ' seconds')::interval
            "#,
        )
        .bind(self.config.chain_id as i64)
        .bind(self.config.stale_claim_seconds.max(60))
        .execute(&self.pool)
        .await
        .map_err(persistence_error)?;
        Ok(result.rows_affected())
    }

    async fn enqueue_seed_jobs(&self) -> LedgerResult<u64> {
        let result = sqlx::query(
            r#"
            with active_seeds as materialized (
              select chain_id,address,priority,seed_digest
              from effective_intelligence_seeds
              where chain_id=$1 and 'mev_actor'=any(seed_kinds)
            ), prepared as (
              select seeds.chain_id,candidates.tx_hash,seeds.address as subject_address,
                     seeds.priority as seed_priority,candidates.score,
                     candidates.analysis_input_digest,seeds.seed_digest,
                     md5(concat_ws(':', seeds.chain_id::text,candidates.tx_hash,
                       seeds.address,candidates.analysis_input_digest,seeds.seed_digest,$3::text))
                       as source_digest
              from active_seeds seeds
              cross join lateral (
                select candidates.tx_hash,candidates.score,analyses.input_digest
                         as analysis_input_digest
                from arbitrage_candidates candidates
                join transaction_analysis analyses
                  on analyses.chain_id=candidates.chain_id
                 and analyses.tx_hash=candidates.tx_hash
                where candidates.chain_id=seeds.chain_id
                  and candidates.sender_address=seeds.address
                  and candidates.block_timestamp>=now()-interval '30 days'
                  and candidates.confirmation_status in (
                    'candidate','analyzed_multi_pool_unconfirmed'
                  )
                  and not exists (
                    select 1 from economic_verification_jobs jobs
                    where jobs.chain_id=candidates.chain_id
                      and jobs.tx_hash=candidates.tx_hash
                  )
                order by candidates.score desc,candidates.block_number desc,
                         candidates.tx_hash
                limit $2
              ) candidates
            ), capacity as (
              select greatest(0, $4::bigint - count(*))::bigint as slots
              from economic_verification_jobs
              where chain_id = $1
                and (
                  status in ('queued', 'running')
                  or (status = 'failed' and attempts < $6)
                )
            ), eligible as (
              select prepared.*
              from prepared
              order by seed_priority asc,score desc,tx_hash
              limit least($5::bigint, (select slots from capacity))
            )
            insert into economic_verification_jobs (
              chain_id, tx_hash, subject_address, event_type, priority, status,
              attempts, source_digest, available_at, claimed_at, completed_at,
              last_error, created_at, updated_at
            )
            select chain_id, tx_hash, subject_address, 'dex_arbitrage',
                   greatest(0, seed_priority * 1000 - score), 'queued', 0,
                   source_digest, now(), null, null, null, now(), now()
            from eligible
            on conflict (chain_id, tx_hash) do update set
              subject_address = excluded.subject_address,
              priority = excluded.priority,
              status = 'queued', attempts = 0,
              source_digest = excluded.source_digest,
              available_at = now(), claimed_at = null, completed_at = null,
              last_error = null, updated_at = now()
            where economic_verification_jobs.source_digest
                  is distinct from excluded.source_digest
            "#,
        )
        .bind(self.config.chain_id as i64)
        .bind(self.config.maximum_candidates_per_seed.max(1) as i64)
        .bind(ECONOMIC_VERIFIER_METHOD_VERSION)
        .bind(self.config.maximum_pending_jobs.max(1) as i64)
        .bind(self.config.maximum_new_jobs_per_pass.max(1) as i64)
        .bind(self.config.maximum_attempts.max(1) as i32)
        .execute(&self.pool)
        .await
        .map_err(persistence_error)?;
        Ok(result.rows_affected())
    }

    async fn claim_next_job(&self) -> LedgerResult<Option<VerificationJob>> {
        let row = sqlx::query(
            r#"
            with next_job as (
              select chain_id, tx_hash
              from economic_verification_jobs
              where chain_id = $1 and status in ('queued', 'failed')
                and available_at <= now() and attempts < $2
              order by priority asc, available_at asc, created_at asc
              for update skip locked
              limit 1
            )
            update economic_verification_jobs jobs
            set status = 'running', attempts = jobs.attempts + 1,
                claimed_at = now(), last_error = null, updated_at = now()
            from next_job
            where jobs.chain_id = next_job.chain_id
              and jobs.tx_hash = next_job.tx_hash
            returning jobs.chain_id, jobs.tx_hash, jobs.subject_address,
                      jobs.attempts, jobs.source_digest
            "#,
        )
        .bind(self.config.chain_id as i64)
        .bind(self.config.maximum_attempts.max(1) as i32)
        .fetch_optional(&self.pool)
        .await
        .map_err(persistence_error)?;
        Ok(row.map(|row| VerificationJob {
            chain_id: row.get::<i64, _>("chain_id").max(0) as u64,
            tx_hash: row.get("tx_hash"),
            subject_address: row.get("subject_address"),
            attempts: row.get::<i32, _>("attempts").max(0) as u32,
            source_digest: row.get("source_digest"),
        }))
    }

    async fn verify_job(&self, job: &VerificationJob) -> LedgerResult<VerificationOutcome> {
        let context = self.load_candidate_context(job).await?;
        let mut analysis = self.load_analysis(job).await?;
        let trace_available = analysis
            .normalized_execution_facts
            .derived_facts
            .get("trace_available")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        if !trace_available {
            analysis = self
                .analysis_service
                .analyze_transaction(job.chain_id, &job.tx_hash)
                .await?
                .0;
        }
        let scope = BTreeSet::from([job.subject_address.clone()]);
        let profit = verify_deterministic_profit_for_scope(self.rpc.as_ref(), &analysis, &scope)
            .await?
            .unwrap_or_else(|| {
                json!({
                    "status": "unavailable",
                    "accounting_complete": false,
                    "method": PROFIT_METHOD,
                    "reason": "profit verifier returned no result"
                })
            });
        Ok(build_outcome(job, &context, &analysis, profit))
    }

    async fn load_candidate_context(
        &self,
        job: &VerificationJob,
    ) -> LedgerResult<CandidateContext> {
        let row = sqlx::query(
            r#"
            select detection_source, score, swap_log_count, distinct_pool_count,
                   route_fingerprint
            from arbitrage_candidates
            where chain_id = $1 and lower(tx_hash) = lower($2)
            "#,
        )
        .bind(job.chain_id as i64)
        .bind(&job.tx_hash)
        .fetch_optional(&self.pool)
        .await
        .map_err(persistence_error)?
        .ok_or_else(|| LedgerError::NotFound(format!("candidate {}", job.tx_hash)))?;
        Ok(CandidateContext {
            detection_source: row.get("detection_source"),
            score: row.get("score"),
            swap_log_count: row.get("swap_log_count"),
            distinct_pool_count: row.get("distinct_pool_count"),
            route_fingerprint: row.get("route_fingerprint"),
        })
    }

    async fn load_analysis(&self, job: &VerificationJob) -> LedgerResult<TransactionAnalysis> {
        let row = sqlx::query(
            "select payload from transaction_analysis where chain_id = $1 and lower(tx_hash) = lower($2)",
        )
        .bind(job.chain_id as i64)
        .bind(&job.tx_hash)
        .fetch_optional(&self.pool)
        .await
        .map_err(persistence_error)?
        .ok_or_else(|| LedgerError::NotFound(format!("analysis {}", job.tx_hash)))?;
        let Json(payload): Json<Value> = row.get("payload");
        serde_json::from_value(payload).map_err(|error| LedgerError::Persistence(error.to_string()))
    }

    async fn persist_outcome(
        &self,
        job: &VerificationJob,
        outcome: &VerificationOutcome,
    ) -> LedgerResult<()> {
        let mut transaction = self.pool.begin().await.map_err(persistence_error)?;
        sqlx::query(
            r#"
            insert into economic_verification_results (
              chain_id, tx_hash, subject_address, event_type, result_status,
              severity, confidence, conclusion, profit_recipient,
              profit_token_address, profit_amount_raw, gas_cost_raw, ruleset_id,
              method_version, inputs_digest, evidence, limitations, verified_at,
              created_at, updated_at
            )
            values ($1,$2,$3,'dex_arbitrage',$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,
                    case when $4 = 'verified' then now() else null end,now(),now())
            on conflict (chain_id, tx_hash) do update set
              subject_address = excluded.subject_address,
              result_status = excluded.result_status,
              severity = excluded.severity,
              confidence = excluded.confidence,
              conclusion = excluded.conclusion,
              profit_recipient = excluded.profit_recipient,
              profit_token_address = excluded.profit_token_address,
              profit_amount_raw = excluded.profit_amount_raw,
              gas_cost_raw = excluded.gas_cost_raw,
              ruleset_id = excluded.ruleset_id,
              method_version = excluded.method_version,
              inputs_digest = excluded.inputs_digest,
              evidence = excluded.evidence,
              limitations = excluded.limitations,
              verified_at = excluded.verified_at,
              updated_at = now()
            "#,
        )
        .bind(job.chain_id as i64)
        .bind(&job.tx_hash)
        .bind(&job.subject_address)
        .bind(outcome.status.key())
        .bind(outcome.severity)
        .bind(outcome.confidence)
        .bind(&outcome.conclusion)
        .bind(outcome.profit_recipient.as_deref())
        .bind(outcome.profit_token_address.as_deref())
        .bind(outcome.profit_amount_raw.as_deref())
        .bind(outcome.gas_cost_raw.as_deref())
        .bind(ECONOMIC_VERIFIER_RULESET_ID)
        .bind(ECONOMIC_VERIFIER_METHOD_VERSION)
        .bind(&outcome.inputs_digest)
        .bind(Json(outcome.evidence.clone()))
        .bind(&outcome.limitations)
        .execute(&mut *transaction)
        .await
        .map_err(persistence_error)?;

        sqlx::query(
            r#"
            update arbitrage_candidates
            set confirmation_status = case
                  when confirmation_status in (
                    'verified', 'verified_profit', 'confirmed_economic'
                  ) then confirmation_status
                  when confirmation_status like 'confirmed_%'
                       and $3 <> 'verified_profit' then confirmation_status
                  else $3
                end,
                profit_recipient = case when $3 = 'verified_profit' then $4 else null end,
                profit_token_address = case when $3 = 'verified_profit' then $5 else null end,
                profit_amount_raw = case when $3 = 'verified_profit' then $6 else null end,
                confirmation_metadata = case
                  when $3 = 'verified_profit'
                       and confirmation_status not in (
                         'verified', 'verified_profit', 'confirmed_economic'
                       )
                    then confirmation_metadata
                         || jsonb_build_object(
                              'method', 'economic_verifier',
                              'reason', $8::text
                            )
                         || $7
                  else confirmation_metadata || $7
                end,
                updated_at = now()
            where chain_id = $1 and lower(tx_hash) = lower($2)
            "#,
        )
        .bind(job.chain_id as i64)
        .bind(&job.tx_hash)
        .bind(outcome.status.candidate_status())
        .bind(outcome.profit_recipient.as_deref())
        .bind(outcome.profit_token_address.as_deref())
        .bind(outcome.profit_amount_raw.as_deref())
        .bind(Json(json!({
            "economic_verifier": {
                "result_status": outcome.status.key(),
                "ruleset_id": ECONOMIC_VERIFIER_RULESET_ID,
                "method_version": ECONOMIC_VERIFIER_METHOD_VERSION,
                "inputs_digest": outcome.inputs_digest,
                "confidence": outcome.confidence,
            }
        })))
        .bind(&outcome.conclusion)
        .execute(&mut *transaction)
        .await
        .map_err(persistence_error)?;

        sqlx::query(
            r#"
            update economic_verification_jobs jobs
            set status = 'completed', completed_at = now(), last_error = null,
                source_digest = coalesce((
                  select md5(concat_ws(
                    ':', candidates.chain_id::text,
                    lower(candidates.tx_hash),
                    lower(candidates.sender_address),
                    analyses.input_digest,
                    seeds.seed_digest,
                    $4::text
                  ))
                  from arbitrage_candidates candidates
                  join transaction_analysis analyses
                    on analyses.chain_id = candidates.chain_id
                   and lower(analyses.tx_hash) = lower(candidates.tx_hash)
                  join effective_intelligence_seeds seeds
                    on seeds.chain_id = candidates.chain_id
                   and seeds.address = lower(candidates.sender_address)
                   and 'mev_actor' = any(seeds.seed_kinds)
                  where candidates.chain_id = jobs.chain_id
                    and lower(candidates.tx_hash) = lower(jobs.tx_hash)
                  limit 1
                ), jobs.source_digest),
                updated_at = now()
            where jobs.chain_id = $1 and jobs.tx_hash = $2
              and jobs.source_digest = $3
            "#,
        )
        .bind(job.chain_id as i64)
        .bind(&job.tx_hash)
        .bind(&job.source_digest)
        .bind(ECONOMIC_VERIFIER_METHOD_VERSION)
        .execute(&mut *transaction)
        .await
        .map_err(persistence_error)?;
        transaction.commit().await.map_err(persistence_error)
    }

    async fn mark_failed(&self, job: &VerificationJob, error: &LedgerError) -> LedgerResult<()> {
        sqlx::query(
            r#"
            update economic_verification_jobs
            set status = 'failed', last_error = $4,
                available_at = now() + ($3::text || ' seconds')::interval,
                updated_at = now()
            where chain_id = $1 and tx_hash = $2
            "#,
        )
        .bind(job.chain_id as i64)
        .bind(&job.tx_hash)
        .bind(self.config.retry_delay_seconds.max(1))
        .bind(error.to_string())
        .execute(&self.pool)
        .await
        .map_err(persistence_error)?;
        Ok(())
    }
}

fn build_outcome(
    job: &VerificationJob,
    context: &CandidateContext,
    analysis: &TransactionAnalysis,
    profit: Value,
) -> VerificationOutcome {
    let profit_status = profit.get("status").and_then(Value::as_str);
    let accounting_complete = profit
        .get("accounting_complete")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let profit_amount_raw = profit
        .get("profit_raw")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let profit_token_address = profit
        .get("profit_token_address")
        .and_then(Value::as_str)
        .map(str::to_ascii_lowercase);
    let positive_profit = profit_amount_raw
        .as_deref()
        .and_then(|value| BigInt::parse_bytes(value.as_bytes(), 10))
        .is_some_and(|value| value > BigInt::from(0_u8));
    let reason = profit
        .get("reason")
        .and_then(Value::as_str)
        .unwrap_or("deterministic accounting did not produce a complete result");
    let status = if profit_status == Some("verified")
        && accounting_complete
        && positive_profit
        && profit_token_address.is_some()
    {
        VerificationResultStatus::Verified
    } else if matches!(
        reason,
        "transaction did not succeed"
            | "RPC receipt is not successful"
            | "fewer than two distinct deterministic swap-log emitters"
    ) {
        VerificationResultStatus::Dismissed
    } else {
        VerificationResultStatus::InsufficientEvidence
    };
    let inputs_digest = verification_inputs_digest(job, context, analysis);
    let mut limitations = profit
        .get("limitations")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect::<Vec<_>>();
    limitations.push(
        "Accounting scope contains only the recorded transaction sender; related ownership is not inferred."
            .to_owned(),
    );
    if status == VerificationResultStatus::InsufficientEvidence {
        limitations.push(reason.to_owned());
    }
    limitations.sort();
    limitations.dedup();
    let conclusion = match status {
        VerificationResultStatus::Verified => format!(
            "Deterministic accounting verified a multi-pool execution with a net surplus of {} raw units in {} for the explicit sender scope.",
            profit_amount_raw.as_deref().unwrap_or("unknown"),
            profit_token_address.as_deref().unwrap_or("unknown token")
        ),
        VerificationResultStatus::Dismissed => format!(
            "Deterministic evidence dismissed this multi-pool economic candidate: {reason}."
        ),
        VerificationResultStatus::InsufficientEvidence => format!(
            "The multi-pool signal remains unverified because deterministic accounting is incomplete: {reason}."
        ),
    };
    let confidence = match status {
        VerificationResultStatus::Verified => 0.98,
        VerificationResultStatus::Dismissed => 0.95,
        VerificationResultStatus::InsufficientEvidence => 0.40,
    };
    let severity = if status == VerificationResultStatus::Verified {
        "medium"
    } else {
        "low"
    };
    let gas_cost_raw = profit
        .get("gas_cost_raw")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let profit_recipient = profit
        .get("profit_recipient")
        .and_then(Value::as_str)
        .map(str::to_ascii_lowercase);
    let evidence = json!({
        "source": "economic_verifier",
        "result_status": status.key(),
        "ruleset_id": ECONOMIC_VERIFIER_RULESET_ID,
        "method_version": ECONOMIC_VERIFIER_METHOD_VERSION,
        "inputs_digest": inputs_digest,
        "confidence": confidence,
        "scope": [job.subject_address.clone()],
        "scope_semantics": "recorded_candidate_sender_only",
        "candidate": {
            "detection_source": context.detection_source,
            "score": context.score,
            "swap_log_count": context.swap_log_count,
            "distinct_pool_count": context.distinct_pool_count,
            "route_fingerprint": context.route_fingerprint,
        },
        "analysis": {
            "analysis_id": analysis.analysis_id,
            "analysis_input_digest": analysis.input_digest,
            "transaction_status": analysis.status,
            "analysis_status": analysis.analysis_status,
            "trace_available": analysis.normalized_execution_facts.derived_facts
                .get("trace_available").and_then(Value::as_bool).unwrap_or(false),
        },
        "profit": profit,
        "limitations": limitations,
    });
    VerificationOutcome {
        status,
        severity,
        confidence,
        conclusion,
        profit_recipient,
        profit_token_address: (status == VerificationResultStatus::Verified)
            .then_some(profit_token_address)
            .flatten(),
        profit_amount_raw: (status == VerificationResultStatus::Verified)
            .then_some(profit_amount_raw)
            .flatten(),
        gas_cost_raw,
        inputs_digest,
        evidence,
        limitations,
    }
}

fn verification_inputs_digest(
    job: &VerificationJob,
    context: &CandidateContext,
    analysis: &TransactionAnalysis,
) -> String {
    let value = json!({
        "ruleset_id": ECONOMIC_VERIFIER_RULESET_ID,
        "method_version": ECONOMIC_VERIFIER_METHOD_VERSION,
        "chain_id": job.chain_id,
        "tx_hash": job.tx_hash,
        "subject_address": job.subject_address,
        "source_digest": job.source_digest,
        "analysis_input_digest": analysis.input_digest,
        "candidate": {
            "detection_source": context.detection_source,
            "score": context.score,
            "swap_log_count": context.swap_log_count,
            "distinct_pool_count": context.distinct_pool_count,
            "route_fingerprint": context.route_fingerprint,
        }
    });
    let mut hasher = Sha256::new();
    hasher.update(serde_json::to_vec(&value).expect("verification inputs are serializable"));
    hex::encode(hasher.finalize())
}

fn persistence_error(error: impl std::fmt::Display) -> LedgerError {
    LedgerError::Persistence(error.to_string())
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use serde_json::json;
    use uuid::Uuid;

    use super::*;
    use crate::{
        core::versioning::VersionBundle,
        domain::{
            entities::NormalizedExecutionFacts,
            enums::{AnalysisStatus, TransactionStatus},
        },
    };

    fn analysis(status: TransactionStatus) -> TransactionAnalysis {
        TransactionAnalysis {
            analysis_id: Uuid::nil(),
            chain_id: 4663,
            tx_hash: "0xabc".to_owned(),
            block_number: Some(10),
            block_timestamp: Some(Utc::now()),
            from_address: "0x1111111111111111111111111111111111111111".to_owned(),
            to_address: None,
            status,
            value: "0".to_owned(),
            input_data: "0x".to_owned(),
            code_identity_summary: None,
            raw_evidence_refs: vec![],
            normalized_execution_facts: NormalizedExecutionFacts {
                selector: None,
                call_count: 0,
                internal_call_count: 0,
                event_count: 0,
                call_tree: json!({}),
                participants: vec![],
                log_summaries: vec![],
                derived_facts: json!({"trace_available": true}),
                evidence_refs: vec![],
            },
            derived_flows: vec![],
            recognizer_results: vec![],
            scenario_outputs: json!({}),
            accounting_outputs: json!({}),
            analysis_status: AnalysisStatus::Completed,
            analysis_version_metadata: VersionBundle::default(),
            unresolved_items: vec![],
            validation_results: vec![],
            input_digest: "analysis-digest".to_owned(),
            attempt_id: None,
        }
    }

    fn job() -> VerificationJob {
        VerificationJob {
            chain_id: 4663,
            tx_hash: "0xabc".to_owned(),
            subject_address: "0x1111111111111111111111111111111111111111".to_owned(),
            attempts: 1,
            source_digest: "source-digest".to_owned(),
        }
    }

    fn context() -> CandidateContext {
        CandidateContext {
            detection_source: "rpc_logs".to_owned(),
            score: 80,
            swap_log_count: 3,
            distinct_pool_count: 2,
            route_fingerprint: "route".to_owned(),
        }
    }

    #[test]
    fn complete_positive_accounting_is_verified_and_deterministic() {
        let profit = json!({
            "status": "verified",
            "accounting_complete": true,
            "profit_raw": "42",
            "profit_token_address": "0x2222222222222222222222222222222222222222",
            "profit_recipient": "0x1111111111111111111111111111111111111111",
            "gas_cost_raw": "7"
        });
        let first = build_outcome(
            &job(),
            &context(),
            &analysis(TransactionStatus::Success),
            profit.clone(),
        );
        let second = build_outcome(
            &job(),
            &context(),
            &analysis(TransactionStatus::Success),
            profit,
        );
        assert_eq!(first.status, VerificationResultStatus::Verified);
        assert_eq!(first.profit_amount_raw.as_deref(), Some("42"));
        assert_eq!(first.inputs_digest, second.inputs_digest);
    }

    #[test]
    fn missing_evidence_never_becomes_zero_profit() {
        let outcome = build_outcome(
            &job(),
            &context(),
            &analysis(TransactionStatus::Success),
            json!({"status":"unavailable","accounting_complete":false,"reason":"trace unavailable"}),
        );
        assert_eq!(
            outcome.status,
            VerificationResultStatus::InsufficientEvidence
        );
        assert_eq!(outcome.profit_amount_raw, None);
        assert!(
            outcome
                .limitations
                .iter()
                .any(|value| value == "trace unavailable")
        );
    }

    #[test]
    fn contradictory_execution_evidence_is_dismissed() {
        let outcome = build_outcome(
            &job(),
            &context(),
            &analysis(TransactionStatus::Failure),
            json!({"status":"unavailable","accounting_complete":false,"reason":"transaction did not succeed"}),
        );
        assert_eq!(outcome.status, VerificationResultStatus::Dismissed);
        assert_eq!(outcome.profit_token_address, None);
    }
}
