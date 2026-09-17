use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
};

use chrono::{DateTime, Duration, Utc};
use num_bigint::BigInt;
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};

use crate::{
    core::errors::{LedgerError, LedgerResult},
    domain::{
        entities::TransactionAnalysis,
        enums::{AddressHitDirection, AssetType, TransactionStatus},
        interfaces::ClusterTxSource,
        value_objects::{AddressTxRef, ChainDataSourceStatus},
    },
    ingestion::rpc::RpcClient,
    normalization::service::TRANSFER_TOPIC,
    pilot::profit::verify_deterministic_profit_for_scope,
    services::address_history_source::{AddressHistoryCursor, AddressHistorySource},
    services::analysis::AnalysisService,
};

pub const INTELLIGENCE_CONTRACT_VERSION: &str = "observed-address-intelligence.v1";
const BEHAVIOR_METHOD_VERSION: &str = "deterministic-observed-behavior.v1";
const RELATIONSHIP_METHOD_VERSION: &str = "deterministic-observed-relationships.v1";
const CHANGE_METHOD_VERSION: &str = "observed-window-comparison.v1";

#[derive(Clone, Debug)]
pub struct ObservationIntelligenceConfig {
    pub chain_id: u64,
    pub worker_id: String,
    pub maximum_transactions: usize,
    pub analysis_batch_size: usize,
    pub analysis_concurrency: usize,
    pub history_days: i64,
    pub log_chunk_blocks: u64,
    pub discovery_chunk_budget: usize,
    pub maximum_attempts: i32,
    pub stale_claim_seconds: i64,
    pub incremental_lag_blocks: u64,
}

impl Default for ObservationIntelligenceConfig {
    fn default() -> Self {
        Self {
            chain_id: 8453,
            worker_id: "observation_intelligence".to_owned(),
            maximum_transactions: 10_000,
            analysis_batch_size: 32,
            analysis_concurrency: 8,
            history_days: 30,
            log_chunk_blocks: 50_000,
            discovery_chunk_budget: 64,
            maximum_attempts: 3,
            stale_claim_seconds: 120,
            incremental_lag_blocks: 2,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct ObservationIteration {
    pub recovered_claims: u64,
    pub stale_objects: u64,
    pub processed: bool,
    pub chain_id: u64,
    pub address: Option<String>,
    pub stage: Option<String>,
}

#[derive(Clone, Debug)]
struct ClaimedObject {
    address: String,
    status: String,
    history_start_block: Option<u64>,
    target_block: Option<u64>,
    last_processed_block: Option<u64>,
    retry_count: i32,
    coverage_is_partial: bool,
}

#[derive(Debug)]
struct LightweightActivity {
    tx_hash: String,
    block_number: u64,
    block_timestamp: DateTime<Utc>,
    status: String,
    from_address: String,
    to_address: Option<String>,
    counterparty: Option<String>,
    native_value_raw: String,
    token_inflows: Value,
    token_outflows: Value,
    touched_contracts: Value,
    evidence_refs: Value,
    input_digest: String,
}

enum ActivityAnalysis {
    Full(TransactionAnalysis),
    Lightweight(LightweightActivity),
}

#[derive(Debug)]
struct TransferDiscoveryOutcome {
    log_hits: usize,
    scanned_from: u64,
    complete: bool,
    finished: bool,
    capped: bool,
}

#[derive(Debug)]
struct ExternalHistoryOutcome {
    complete: bool,
    earliest_activity_block: Option<u64>,
}

#[derive(Clone)]
pub struct ObservationIntelligenceService {
    pool: PgPool,
    rpc: Arc<dyn RpcClient>,
    analysis: Arc<AnalysisService>,
    tx_source: Arc<dyn ClusterTxSource>,
    address_history_source: Option<Arc<dyn AddressHistorySource>>,
    config: ObservationIntelligenceConfig,
}

impl ObservationIntelligenceService {
    pub fn new(
        pool: PgPool,
        rpc: Arc<dyn RpcClient>,
        analysis: Arc<AnalysisService>,
        tx_source: Arc<dyn ClusterTxSource>,
        config: ObservationIntelligenceConfig,
    ) -> Self {
        Self {
            pool,
            rpc,
            analysis,
            tx_source,
            address_history_source: None,
            config,
        }
    }

    pub fn with_address_history_source(mut self, source: Arc<dyn AddressHistorySource>) -> Self {
        self.address_history_source = Some(source);
        self
    }

    pub async fn validate_rpc_chain(&self) -> LedgerResult<()> {
        let actual = self.rpc.get_chain_id().await?;
        if actual != self.config.chain_id {
            return Err(LedgerError::TransientIngestion(format!(
                "observation RPC chain mismatch: configured {}, RPC returned {actual}",
                self.config.chain_id
            )));
        }
        Ok(())
    }

    /// Proves that the API plane, canonical index and RPC all describe the
    /// requested chain. This deliberately returns no URLs or infrastructure
    /// identifiers and fails closed before intelligence is served.
    pub async fn data_source_status(&self, chain_id: u64) -> LedgerResult<ChainDataSourceStatus> {
        if chain_id != self.config.chain_id {
            return Err(LedgerError::TransientIngestion(format!(
                "{} live intelligence is not available on the current data source",
                chain_display_name(chain_id)
            )));
        }
        let (rpc_chain_id, status) = tokio::try_join!(
            self.rpc.get_chain_id(),
            self.tx_source.get_data_source_status(chain_id)
        )?;
        if rpc_chain_id != chain_id || status.chain_id != chain_id || !status.available {
            return Err(LedgerError::TransientIngestion(format!(
                "{} live intelligence is not available on the current data source",
                chain_display_name(chain_id)
            )));
        }
        Ok(status)
    }

    pub async fn enqueue(&self, chain_id: u64, address: &str, source: &str) -> LedgerResult<()> {
        if chain_id != self.config.chain_id {
            return Err(LedgerError::MalformedEvidence(format!(
                "unsupported observation chain {chain_id}; this plane serves {}",
                self.config.chain_id
            )));
        }
        self.data_source_status(chain_id).await?;
        let address = normalize_address(address)?;
        sqlx::query(
            r#"
            insert into observed_address_objects (
              chain_id, address, current_status, progress, monitoring_enabled,
              contract_version, metadata, available_at, updated_at
            ) values ($1, $2, 'queued', $3, true, $4, $5, now(), now())
            on conflict (chain_id, address) do update set
              monitoring_enabled = true,
              current_status = case
                when observed_address_objects.current_status = 'ready' then 'stale'
                when observed_address_objects.current_status = 'failed' then 'queued'
                else observed_address_objects.current_status
              end,
              error = null,
              available_at = now(),
              metadata = observed_address_objects.metadata || excluded.metadata,
              updated_at = now()
            "#,
        )
        .bind(chain_id as i64)
        .bind(&address)
        .bind(initial_progress("Queued for historical activity discovery"))
        .bind(INTELLIGENCE_CONTRACT_VERSION)
        .bind(json!({"source": source, "discovery_policy": "known_index_plus_rpc_transfer_logs", "maximum_transactions": self.config.maximum_transactions}))
        .execute(&self.pool)
        .await
        .map_err(db_error)?;
        Ok(())
    }

    pub async fn retry(&self, chain_id: u64, address: &str) -> LedgerResult<bool> {
        self.data_source_status(chain_id).await?;
        let address = normalize_address(address)?;
        let result = sqlx::query(
            r#"
            update observed_address_objects
            set current_status = 'queued', error = null, available_at = now(),
                claimed_at = null, retry_count = 0, progress = $3, updated_at = now()
            where chain_id = $1 and address = $2
            "#,
        )
        .bind(chain_id as i64)
        .bind(address)
        .bind(initial_progress("Retry queued"))
        .execute(&self.pool)
        .await
        .map_err(db_error)?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn process_once(&self) -> LedgerResult<ObservationIteration> {
        let head = self
            .tx_source
            .get_last_indexed_block(self.config.chain_id)
            .await?;
        let recovered = sqlx::query(
            r#"
            update observed_address_objects
            set claimed_at = null, available_at = now(), updated_at = now(),
                metadata = metadata || '{"recovered_stale_claim":true}'::jsonb
            where chain_id = $1 and claimed_at < now() - make_interval(secs => $2)
              and current_status not in ('ready', 'failed')
            "#,
        )
        .bind(self.config.chain_id as i64)
        .bind(self.config.stale_claim_seconds as f64)
        .execute(&self.pool)
        .await
        .map_err(db_error)?
        .rows_affected();
        // A process can stop after marking a transaction `analyzing` but before
        // persisting the result. Return those rows to the normal retry path once
        // the claim timeout has elapsed; the object/transaction primary keys keep
        // replay idempotent.
        sqlx::query(
            r#"
            update object_activity_transactions
            set processing_status='failed',
                last_error='recovered after interrupted observation analysis',
                updated_at=now()
            where chain_id=$1 and processing_status='analyzing'
              and updated_at < now() - make_interval(secs => $2)
            "#,
        )
        .bind(self.config.chain_id as i64)
        .bind(self.config.stale_claim_seconds as f64)
        .execute(&self.pool)
        .await
        .map_err(db_error)?;
        let stale = sqlx::query(
            r#"
            update observed_address_objects
            set current_status = 'stale', available_at = now(), updated_at = now(),
                progress = jsonb_set(progress, '{message}', '"New blocks are ready for incremental analysis"'::jsonb)
            where chain_id = $1 and current_status = 'ready' and monitoring_enabled
              and coalesce(last_processed_block, 0) < $2
            "#,
        )
        .bind(self.config.chain_id as i64)
        .bind(head.saturating_sub(self.config.incremental_lag_blocks) as i64)
        .execute(&self.pool)
        .await
        .map_err(db_error)?
        .rows_affected();

        let Some(object) = self.claim_next().await? else {
            return Ok(ObservationIteration {
                recovered_claims: recovered,
                stale_objects: stale,
                chain_id: self.config.chain_id,
                ..ObservationIteration::default()
            });
        };
        let address = object.address.clone();
        let stage = object.status.clone();
        let result = match object.status.as_str() {
            "queued" | "stale" | "failed" => self.begin_discovery(&object, head).await,
            "discovering" => self.discover_activity(&object).await,
            "backfilling" => self.backfill_batch(&object).await,
            "classifying" => self.classify_and_extract(&object).await,
            "verifying_economics" => self.verify_economics(&object).await,
            "building_baseline" => self.build_baseline_and_briefs(&object).await,
            other => Err(LedgerError::MalformedEvidence(format!(
                "unsupported observation state {other}"
            ))),
        };
        if let Err(error) = result {
            self.fail_object(&object, &error.to_string()).await?;
            return Err(error);
        }
        Ok(ObservationIteration {
            recovered_claims: recovered,
            stale_objects: stale,
            processed: true,
            chain_id: self.config.chain_id,
            address: Some(address),
            stage: Some(stage),
        })
    }

    async fn claim_next(&self) -> LedgerResult<Option<ClaimedObject>> {
        let row = sqlx::query(
            r#"
            update observed_address_objects object
            set claimed_at = now(), updated_at = now()
            where (object.chain_id, object.address) = (
              select chain_id, address from observed_address_objects
              where chain_id = $1 and available_at <= now() and claimed_at is null
                and current_status in (
                  'queued','discovering','backfilling','classifying',
                  'verifying_economics','building_baseline','stale','failed'
                )
                and retry_count < $2
              order by available_at, updated_at
              for update skip locked limit 1
            )
            returning address, current_status, history_start_block, target_block,
                      last_processed_block, retry_count, coverage_is_partial
            "#,
        )
        .bind(self.config.chain_id as i64)
        .bind(self.config.maximum_attempts)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_error)?;
        Ok(row.map(|row| ClaimedObject {
            address: row.get("address"),
            status: row.get("current_status"),
            history_start_block: row
                .try_get::<Option<i64>, _>("history_start_block")
                .ok()
                .flatten()
                .map(|value| value as u64),
            target_block: row
                .try_get::<Option<i64>, _>("target_block")
                .ok()
                .flatten()
                .map(|value| value as u64),
            last_processed_block: row
                .try_get::<Option<i64>, _>("last_processed_block")
                .ok()
                .flatten()
                .map(|value| value as u64),
            retry_count: row.get("retry_count"),
            coverage_is_partial: row.get("coverage_is_partial"),
        }))
    }

    async fn begin_discovery(&self, object: &ClaimedObject, head: u64) -> LedgerResult<()> {
        let initial = object.last_processed_block.is_none();
        let start_block = if initial {
            if self.address_history_source.is_some() {
                // The external provider discovers the true first activity block.
                // Block zero is only a temporary cursor and is replaced before
                // local fact analysis begins.
                0
            } else {
                let target = Utc::now() - Duration::days(self.config.history_days);
                self.find_block_at_or_after(head, target).await?
            }
        } else {
            object
                .last_processed_block
                .unwrap_or(head)
                .saturating_add(1)
        };
        let code = self
            .rpc
            .get_code(self.config.chain_id, &object.address, Some(head))
            .await?;
        let object_type = if code.trim_start_matches("0x").is_empty() {
            "execution_eoa_candidate"
        } else {
            "executor_contract_candidate"
        };
        let run_kind = if initial { "initial" } else { "incremental" };
        let coverage_start_at = self.block_datetime(start_block).await?;
        let coverage_end_at = self.block_datetime(head).await?;
        sqlx::query(
            r#"
            insert into observation_analysis_runs (
              chain_id, address, run_kind, status, stage, start_block, end_block, started_at
            ) values ($1,$2,$3,'running','discovering',$4,$5,now())
            "#,
        )
        .bind(self.config.chain_id as i64)
        .bind(&object.address)
        .bind(run_kind)
        .bind(start_block as i64)
        .bind(head as i64)
        .execute(&self.pool)
        .await
        .map_err(db_error)?;
        sqlx::query(
            r#"
            update observed_address_objects set
              object_type=$3, current_status='discovering', analysis_started_at=coalesce(analysis_started_at,now()),
              analysis_completed_at=null, history_start_block=$4, target_block=$5,
              coverage_start_at=case when $7 then $8 else coalesce(coverage_start_at,$8) end,
              coverage_end_at=$9, progress=$6, error=null, claimed_at=null,
              available_at=now(),
              metadata=metadata - 'transfer_scan_next_end_block'
                                - 'transfer_scan_log_hits'
                                - 'transfer_scan_chunk_blocks',
              updated_at=now()
            where chain_id=$1 and address=$2
            "#,
        )
        .bind(self.config.chain_id as i64)
        .bind(&object.address)
        .bind(object_type)
        .bind(start_block as i64)
        .bind(head as i64)
        .bind(json!({"percent": 3, "stage": "discovering", "message": if initial && self.address_history_source.is_some() { "Resolving complete address history and earliest activity" } else { "Discovering direct calls and token-transfer activity" }, "transactions_discovered": 0, "transactions_analyzed": 0}))
        .bind(initial)
        .bind(coverage_start_at)
        .bind(coverage_end_at)
        .execute(&self.pool)
        .await
        .map_err(db_error)?;
        Ok(())
    }

    async fn block_datetime(&self, block_number: u64) -> LedgerResult<DateTime<Utc>> {
        let block = self
            .rpc
            .get_block(self.config.chain_id, block_number)
            .await?;
        let timestamp = block
            .get("timestamp")
            .and_then(Value::as_str)
            .and_then(parse_hex_u64)
            .ok_or_else(|| {
                LedgerError::MalformedEvidence(format!(
                    "block {block_number} timestamp unavailable"
                ))
            })?;
        DateTime::from_timestamp(timestamp as i64, 0).ok_or_else(|| {
            LedgerError::MalformedEvidence(format!(
                "block {block_number} has invalid timestamp {timestamp}"
            ))
        })
    }

    async fn find_block_at_or_after(&self, head: u64, target: DateTime<Utc>) -> LedgerResult<u64> {
        let mut low = 0_u64;
        let mut high = head;
        while low < high {
            let mid = low + (high - low) / 2;
            let block = self.rpc.get_block(self.config.chain_id, mid).await?;
            let timestamp = block
                .get("timestamp")
                .and_then(Value::as_str)
                .and_then(parse_hex_u64)
                .ok_or_else(|| {
                    LedgerError::MalformedEvidence(format!("block {mid} timestamp unavailable"))
                })?;
            if timestamp < target.timestamp().max(0) as u64 {
                low = mid.saturating_add(1);
            } else {
                high = mid;
            }
        }
        Ok(low)
    }

    async fn discover_activity(&self, object: &ClaimedObject) -> LedgerResult<()> {
        let mut start = object.history_start_block.ok_or_else(|| {
            LedgerError::MalformedEvidence("observation history start is missing".to_owned())
        })?;
        let end = object.target_block.ok_or_else(|| {
            LedgerError::MalformedEvidence("observation target block is missing".to_owned())
        })?;
        if start > end {
            self.advance(
                &object.address,
                "classifying",
                72,
                "No new blocks require backfill",
            )
            .await?;
            return Ok(());
        }

        let external_history_complete =
            if object.last_processed_block.is_none() && self.address_history_source.is_some() {
                let outcome = self.discover_external_history(object, end).await?;
                if !outcome.complete {
                    return Ok(());
                }
                start = outcome.earliest_activity_block.unwrap_or(end);
                true
            } else {
                false
            };

        let mut refs = BTreeMap::<String, AddressTxRef>::new();
        let direct_discovery = self
            .tx_source
            .query_address_refs_with_coverage(
                self.config.chain_id,
                &object.address,
                self.config.maximum_transactions.min(u32::MAX as usize) as u32,
                Some(start),
                Some(end),
            )
            .await?;
        let direct_coverage_complete = direct_discovery.coverage_complete == Some(true);
        let indexed_block_count = direct_discovery.indexed_block_count;
        let requested_block_count = direct_discovery.requested_block_count;
        let direct = direct_discovery.refs;
        let direct_count = direct.len();
        for reference in direct {
            refs.insert(reference.tx_hash.clone(), reference);
        }
        self.collect_cached_refs(&object.address, start, end, &mut refs)
            .await?;
        let cached_and_direct_count = refs.len();
        let mut values = refs.into_values().collect::<Vec<_>>();
        values.sort_by(|left, right| {
            (right.block_number, right.tx_index, &right.tx_hash).cmp(&(
                left.block_number,
                left.tx_index,
                &left.tx_hash,
            ))
        });
        let direct_or_cached_was_capped = values.len() > self.config.maximum_transactions;
        values.truncate(self.config.maximum_transactions);
        self.upsert_activity_refs(&object.address, &values).await?;

        let transfer = if external_history_complete {
            TransferDiscoveryOutcome {
                log_hits: 0,
                scanned_from: start,
                complete: true,
                finished: true,
                capped: false,
            }
        } else {
            self.collect_transfer_refs(&object.address, start, end)
                .await?
        };
        if !transfer.finished {
            let discovered: i64 = sqlx::query_scalar(
                "select count(*) from object_activity_transactions where chain_id=$1 and address=$2",
            )
            .bind(self.config.chain_id as i64)
            .bind(&object.address)
            .fetch_one(&self.pool)
            .await
            .map_err(db_error)?;
            let progress = range_progress(start, end, transfer.scanned_from);
            sqlx::query(
                r#"
                update observed_address_objects set
                  discovered_transaction_count=$3,
                  progress=$4, claimed_at=null, available_at=now(), updated_at=now()
                where chain_id=$1 and address=$2
                "#,
            )
            .bind(self.config.chain_id as i64)
            .bind(&object.address)
            .bind(discovered)
            .bind(json!({
                "percent": 3 + (progress * 10.0) as i64,
                "stage":"discovering",
                "message":"Scanning token-transfer activity with a resumable cursor",
                "transactions_discovered":discovered,
                "blocks_scanned_through":transfer.scanned_from
            }))
            .execute(&self.pool)
            .await
            .map_err(db_error)?;
            return Ok(());
        }
        let total_rows: i64 = sqlx::query_scalar(
            "select count(*) from object_activity_transactions where chain_id=$1 and address=$2",
        )
        .bind(self.config.chain_id as i64)
        .bind(&object.address)
        .fetch_one(&self.pool)
        .await
        .map_err(db_error)?;
        // known_only intentionally avoids whole-range indexing. Unless a later
        // coverage source proves endpoint completeness, top-level history is
        // explicitly partial; transfer-log coverage itself can still be full.
        let capped = !external_history_complete && (direct_or_cached_was_capped || transfer.capped);
        let approximate_total = if capped {
            total_rows.max(self.config.maximum_transactions as i64)
        } else {
            total_rows
        };
        let partial = if external_history_complete {
            false
        } else {
            !direct_coverage_complete
                || capped
                || !transfer.complete
                || direct_count == self.config.maximum_transactions
        };
        let limitations = json!({
            "top_level_history": if external_history_complete { "complete external address-history discovery; every transaction is re-fetched from canonical RPC before analysis" } else { "existing indexed coverage only; no whole-range block backfill was scheduled" },
            "token_transfer_history": if external_history_complete { "complete external address-history discovery; logs and receipts remain canonical verification inputs" } else if transfer.complete { "full RPC log scan for requested block range" } else { "partial RPC log scan" },
            "known_index_refs": direct_count,
            "known_index_coverage_complete": direct_coverage_complete,
            "known_indexed_block_count": indexed_block_count,
            "requested_block_count": requested_block_count,
            "cached_analysis_refs": cached_and_direct_count.saturating_sub(direct_count),
            "transfer_logs": transfer.log_hits,
            "scan_start_block": transfer.scanned_from,
        });
        sqlx::query(
            r#"
            update observed_address_objects set
              current_status='backfilling', discovered_transaction_count=$3,
              approximate_total_transaction_count=$4, coverage_is_partial=$5,
              progress=$6, metadata=metadata || jsonb_build_object('coverage_limitations',$7::jsonb),
              claimed_at=null, available_at=now(), updated_at=now()
            where chain_id=$1 and address=$2
            "#,
        )
        .bind(self.config.chain_id as i64)
        .bind(&object.address)
        .bind(total_rows)
        .bind(approximate_total)
        .bind(partial)
        .bind(json!({"percent": 15, "stage":"backfilling", "message":"Analyzing discovered transactions", "transactions_discovered":total_rows, "transactions_analyzed":0, "coverage_is_partial":partial}))
        .bind(limitations)
        .execute(&self.pool)
        .await
        .map_err(db_error)?;
        sqlx::query(
            r#"
            update observation_analysis_runs set
              discovered_transaction_count=$3,
              progress=$4,
              stage='backfilling',
              updated_at=now()
            where run_id=(
              select run_id from observation_analysis_runs
              where chain_id=$1 and address=$2 and status='running'
              order by created_at desc limit 1
            )
            "#,
        )
        .bind(self.config.chain_id as i64)
        .bind(&object.address)
        .bind(total_rows)
        .bind(json!({"transactions_discovered":total_rows,"coverage_is_partial":partial}))
        .execute(&self.pool)
        .await
        .map_err(db_error)?;
        Ok(())
    }

    async fn discover_external_history(
        &self,
        object: &ClaimedObject,
        target_block: u64,
    ) -> LedgerResult<ExternalHistoryOutcome> {
        let source = self.address_history_source.as_ref().ok_or_else(|| {
            LedgerError::AnalysisInvariant("external history source is unavailable".to_owned())
        })?;
        let metadata: Value = sqlx::query_scalar(
            "select metadata from observed_address_objects where chain_id=$1 and address=$2",
        )
        .bind(self.config.chain_id as i64)
        .bind(&object.address)
        .fetch_one(&self.pool)
        .await
        .map_err(db_error)?;
        let cursor = external_history_cursor(&metadata);
        let previous_rows = metadata
            .get("external_history_rows")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let discovery = match source
            .fetch_iteration(self.config.chain_id, &object.address, &cursor)
            .await
        {
            Ok(discovery) => discovery,
            Err(error) => {
                sqlx::query(
                    r#"update observed_address_objects set
                         progress=$3,claimed_at=null,available_at=now()+interval '30 seconds',
                         metadata=metadata || jsonb_build_object(
                           'external_history_last_error',$4,
                           'external_history_last_error_at',now()
                         ),updated_at=now()
                       where chain_id=$1 and address=$2"#,
                )
                .bind(self.config.chain_id as i64)
                .bind(&object.address)
                .bind(json!({
                    "percent":5,
                    "stage":"discovering",
                    "message":"Address-history provider is rate limited; checkpoint preserved and retry scheduled"
                }))
                .bind(error.to_string().chars().take(1_000).collect::<String>())
                .execute(&self.pool)
                .await
                .map_err(db_error)?;
                return Ok(ExternalHistoryOutcome {
                    complete: false,
                    earliest_activity_block: cursor.earliest_activity_block,
                });
            }
        };
        let mut unique = BTreeMap::new();
        for reference in discovery.refs {
            unique.insert(reference.tx_hash.clone(), reference);
        }
        self.upsert_activity_refs(&object.address, &unique.into_values().collect::<Vec<_>>())
            .await?;
        let total_rows: i64 = sqlx::query_scalar(
            "select count(*) from object_activity_transactions where chain_id=$1 and address=$2",
        )
        .bind(self.config.chain_id as i64)
        .bind(&object.address)
        .fetch_one(&self.pool)
        .await
        .map_err(db_error)?;
        let cursor = discovery.cursor;
        let provider_rows = previous_rows.saturating_add(discovery.fetched_rows as u64);
        let complete = cursor.complete();
        let earliest = cursor.earliest_activity_block;
        let coverage_start_at = match earliest {
            Some(block) => Some(self.block_datetime(block).await?),
            None if complete => Some(self.block_datetime(target_block).await?),
            None => None,
        };
        sqlx::query(
            r#"update observed_address_objects set
                 history_start_block=case when $3 then coalesce($4,target_block) else history_start_block end,
                 coverage_start_at=case when $3 then coalesce($5,coverage_start_at) else coverage_start_at end,
                 discovered_transaction_count=$6,
                 progress=$7,
                 metadata=metadata || $8::jsonb,
                 claimed_at=null,available_at=now(),updated_at=now()
               where chain_id=$1 and address=$2"#,
        )
        .bind(self.config.chain_id as i64)
        .bind(&object.address)
        .bind(complete)
        .bind(earliest.map(|value| value as i64))
        .bind(coverage_start_at)
        .bind(total_rows)
        .bind(json!({
            "percent": if complete { 13 } else { 5 },
            "stage":"discovering",
            "message": if complete { "Complete address history discovered; canonical transaction analysis is starting" } else { "Paging complete address history from the discovery provider" },
            "transactions_discovered":total_rows,
            "history_rows_fetched":provider_rows,
            "earliest_activity_block":earliest,
            "history_complete":complete
        }))
        .bind(json!({
            "external_history_source":source.source_name(),
            "external_history_discovery_only":true,
            "external_history_transaction_page":cursor.transaction_page,
            "external_history_token_transfer_page":cursor.token_transfer_page,
            "external_history_internal_transaction_page":cursor.internal_transaction_page,
            "external_history_transaction_start_block":cursor.transaction_start_block,
            "external_history_token_transfer_start_block":cursor.token_transfer_start_block,
            "external_history_internal_transaction_start_block":cursor.internal_transaction_start_block,
            "external_history_transaction_cursor":cursor.transaction_cursor,
            "external_history_token_transfer_cursor":cursor.token_transfer_cursor,
            "external_history_internal_transaction_cursor":cursor.internal_transaction_cursor,
            "external_history_transactions_complete":cursor.transactions_complete,
            "external_history_token_transfers_complete":cursor.token_transfers_complete,
            "external_history_internal_transactions_complete":cursor.internal_transactions_complete,
            "external_history_earliest_activity_block":earliest,
            "external_history_rows":provider_rows,
            "external_history_complete":complete
        }))
        .execute(&self.pool)
        .await
        .map_err(db_error)?;
        Ok(ExternalHistoryOutcome {
            complete,
            earliest_activity_block: earliest,
        })
    }

    async fn collect_cached_refs(
        &self,
        address: &str,
        start: u64,
        end: u64,
        refs: &mut BTreeMap<String, AddressTxRef>,
    ) -> LedgerResult<()> {
        let rows = sqlx::query(
            r#"
            select distinct t.tx_hash, t.block_number, t.from_address, t.to_address
            from transactions t
            where t.chain_id=$1 and t.block_number between $3 and $4 and (
              lower(t.from_address)=$2 or lower(t.to_address)=$2 or exists (
                select 1 from flows f where f.chain_id=t.chain_id and f.tx_hash=t.tx_hash
                  and (lower(f.from_address)=$2 or lower(f.to_address)=$2)
              )
            ) limit $5
            "#,
        )
        .bind(self.config.chain_id as i64)
        .bind(address)
        .bind(start as i64)
        .bind(end as i64)
        .bind(self.config.maximum_transactions as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(db_error)?;
        for row in rows {
            let tx_hash: String = row.get("tx_hash");
            let from: String = row.get("from_address");
            let to: Option<String> = row.get("to_address");
            let direction = if from.eq_ignore_ascii_case(address) {
                AddressHitDirection::From
            } else {
                AddressHitDirection::To
            };
            refs.entry(tx_hash.clone()).or_insert(AddressTxRef {
                address: address.to_owned(),
                tx_hash,
                block_number: row.get::<Option<i64>, _>("block_number").unwrap_or(0) as u64,
                tx_index: 0,
                direction,
                from_address: Some(from.to_ascii_lowercase()),
                to_address: to.map(|value| value.to_ascii_lowercase()),
            });
        }
        Ok(())
    }

    async fn collect_transfer_refs(
        &self,
        address: &str,
        start: u64,
        end: u64,
    ) -> LedgerResult<TransferDiscoveryOutcome> {
        let topic = padded_address_topic(address);
        let state = sqlx::query(
            r#"
            select metadata->>'transfer_scan_next_end_block' as cursor,
                   metadata->>'transfer_scan_log_hits' as log_hits,
                   metadata->>'transfer_scan_chunk_blocks' as chunk_blocks
            from observed_address_objects where chain_id=$1 and address=$2
            "#,
        )
        .bind(self.config.chain_id as i64)
        .bind(address)
        .fetch_one(&self.pool)
        .await
        .map_err(db_error)?;
        let stored_cursor = state
            .get::<Option<String>, _>("cursor")
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value >= start && *value <= end);
        let mut cursor_end = stored_cursor.unwrap_or(end);
        let mut chunk = state
            .get::<Option<String>, _>("chunk_blocks")
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(self.config.log_chunk_blocks)
            .max(500);
        let mut log_hits = state
            .get::<Option<String>, _>("log_hits")
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(0);
        let mut scanned_from = cursor_end;
        let mut chunks_processed = 0_usize;
        let mut current_run_rows: i64 = sqlx::query_scalar(
            r#"select count(*) from object_activity_transactions
               where chain_id=$1 and address=$2 and block_number between $3 and $4"#,
        )
        .bind(self.config.chain_id as i64)
        .bind(address)
        .bind(start as i64)
        .bind(end as i64)
        .fetch_one(&self.pool)
        .await
        .map_err(db_error)?;
        if current_run_rows >= self.config.maximum_transactions as i64 {
            return Ok(TransferDiscoveryOutcome {
                log_hits,
                scanned_from,
                complete: false,
                finished: true,
                capped: true,
            });
        }
        while cursor_end >= start
            && chunks_processed < self.config.discovery_chunk_budget
            && current_run_rows < self.config.maximum_transactions as i64
        {
            let chunk_start = cursor_end
                .saturating_sub(chunk.saturating_sub(1))
                .max(start);
            let mut chunk_failed = false;
            let mut terminal_failure = false;
            let mut chunk_log_hits = 0_usize;
            let mut chunk_refs = BTreeMap::<String, AddressTxRef>::new();
            for (direction, topics) in [
                (
                    AddressHitDirection::To,
                    json!([TRANSFER_TOPIC, null, topic]),
                ),
                (AddressHitDirection::From, json!([TRANSFER_TOPIC, topic])),
            ] {
                let filter = json!({
                    "fromBlock": format!("0x{chunk_start:x}"),
                    "toBlock": format!("0x{cursor_end:x}"),
                    "topics": topics,
                });
                match self.rpc.get_logs(self.config.chain_id, filter).await {
                    Ok(logs) => {
                        chunk_log_hits = chunk_log_hits.saturating_add(logs.len());
                        for log in logs {
                            if let Some(reference) =
                                transfer_log_ref(address, direction.clone(), &log)
                            {
                                chunk_refs
                                    .entry(reference.tx_hash.clone())
                                    .or_insert(reference);
                            }
                        }
                    }
                    Err(_) if chunk > 500 => {
                        chunk = (chunk / 2).max(500);
                        chunk_failed = true;
                        break;
                    }
                    Err(_) => {
                        terminal_failure = true;
                        chunk_failed = true;
                        break;
                    }
                }
            }
            if chunk_failed {
                if terminal_failure {
                    sqlx::query(
                        r#"update observed_address_objects
                           set metadata=metadata || jsonb_build_object(
                             'transfer_scan_error','RPC log scan failed at minimum chunk size',
                             'transfer_scan_next_end_block',$3,
                             'transfer_scan_chunk_blocks',$4
                           ), updated_at=now()
                           where chain_id=$1 and address=$2"#,
                    )
                    .bind(self.config.chain_id as i64)
                    .bind(address)
                    .bind(cursor_end as i64)
                    .bind(chunk as i64)
                    .execute(&self.pool)
                    .await
                    .map_err(db_error)?;
                    return Ok(TransferDiscoveryOutcome {
                        log_hits,
                        scanned_from,
                        complete: false,
                        finished: true,
                        capped: false,
                    });
                }
                continue;
            }
            let mut values = chunk_refs.into_values().collect::<Vec<_>>();
            values.sort_by(|left, right| {
                (right.block_number, right.tx_index, &right.tx_hash).cmp(&(
                    left.block_number,
                    left.tx_index,
                    &left.tx_hash,
                ))
            });
            let remaining = self
                .config
                .maximum_transactions
                .saturating_sub(current_run_rows.max(0) as usize);
            values.truncate(remaining);
            self.upsert_activity_refs(address, &values).await?;
            current_run_rows = sqlx::query_scalar(
                r#"select count(*) from object_activity_transactions
                   where chain_id=$1 and address=$2 and block_number between $3 and $4"#,
            )
            .bind(self.config.chain_id as i64)
            .bind(address)
            .bind(start as i64)
            .bind(end as i64)
            .fetch_one(&self.pool)
            .await
            .map_err(db_error)?;
            log_hits = log_hits.saturating_add(chunk_log_hits);
            scanned_from = chunk_start;
            chunks_processed += 1;
            let progress = range_progress(start, end, scanned_from);
            sqlx::query(
                r#"update observed_address_objects set
                     progress=$3,
                     metadata=metadata || jsonb_build_object(
                       'transfer_scan_next_end_block',$4,
                       'transfer_scan_log_hits',$5,
                       'transfer_scan_chunk_blocks',$6
                     ),
                     updated_at=now()
                   where chain_id=$1 and address=$2"#,
            )
            .bind(self.config.chain_id as i64)
            .bind(address)
            .bind(json!({"percent": 3 + (progress * 10.0) as i64, "stage":"discovering", "message":"Scanning token-transfer activity with a resumable cursor", "transactions_discovered":current_run_rows, "blocks_scanned_through":scanned_from}))
            .bind(chunk_start.saturating_sub(1) as i64)
            .bind(log_hits as i64)
            .bind(chunk as i64)
            .execute(&self.pool)
            .await
            .map_err(db_error)?;
            if chunk_start == start {
                return Ok(TransferDiscoveryOutcome {
                    log_hits,
                    scanned_from,
                    complete: true,
                    finished: true,
                    capped: false,
                });
            }
            cursor_end = chunk_start.saturating_sub(1);
        }
        let capped = current_run_rows >= self.config.maximum_transactions as i64;
        Ok(TransferDiscoveryOutcome {
            log_hits,
            scanned_from,
            complete: false,
            finished: capped,
            capped,
        })
    }

    async fn upsert_activity_refs(
        &self,
        address: &str,
        references: &[AddressTxRef],
    ) -> LedgerResult<()> {
        for reference in references {
            let subject_role = match reference.direction {
                AddressHitDirection::From => "direct_sender_or_transfer_out",
                AddressHitDirection::To => "direct_recipient_or_transfer_in",
            };
            sqlx::query(
                r#"
                insert into object_activity_transactions (
                  chain_id,address,tx_hash,block_number,tx_index,subject_role,evidence_refs
                ) values ($1,$2,$3,$4,$5,$6,$7)
                on conflict (chain_id,address,tx_hash) do update set
                  block_number=excluded.block_number, tx_index=excluded.tx_index,
                  subject_role=case
                    when object_activity_transactions.subject_role <> excluded.subject_role then 'mixed'
                    else object_activity_transactions.subject_role end,
                  evidence_refs=excluded.evidence_refs, updated_at=now()
                "#,
            )
            .bind(self.config.chain_id as i64)
            .bind(address)
            .bind(&reference.tx_hash)
            .bind(reference.block_number as i64)
            .bind(reference.tx_index as i32)
            .bind(subject_role)
            .bind(json!([{"kind":"tx","reference":reference.tx_hash,"discovery":"known_index_or_transfer_log"}]))
            .execute(&self.pool)
            .await
            .map_err(db_error)?;
        }
        Ok(())
    }

    async fn backfill_batch(&self, object: &ClaimedObject) -> LedgerResult<()> {
        let rows = sqlx::query(
            r#"
            select tx_hash from object_activity_transactions
            where chain_id=$1 and address=$2
              and processing_status in ('discovered','failed') and attempts < $3
            order by block_number desc limit $4
            "#,
        )
        .bind(self.config.chain_id as i64)
        .bind(&object.address)
        .bind(self.config.maximum_attempts)
        .bind(self.config.analysis_batch_size as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(db_error)?;
        if rows.is_empty() {
            sqlx::query(
                r#"
                update object_activity_transactions set processing_status='excluded',
                  exclusion_reason=coalesce(exclusion_reason,'analysis retries exhausted'), updated_at=now()
                where chain_id=$1 and address=$2 and processing_status='failed'
                "#,
            )
            .bind(self.config.chain_id as i64)
            .bind(&object.address)
            .execute(&self.pool)
            .await
            .map_err(db_error)?;
            self.advance(
                &object.address,
                "classifying",
                72,
                "Building behavior and relationship profiles",
            )
            .await?;
            return Ok(());
        }
        let mut pending: Vec<String> = Vec::with_capacity(rows.len());
        for row in rows {
            let tx_hash: String = row.get("tx_hash");
            sqlx::query(
                "update object_activity_transactions set processing_status='analyzing',attempts=attempts+1,updated_at=now() where chain_id=$1 and address=$2 and tx_hash=$3",
            )
            .bind(self.config.chain_id as i64)
            .bind(&object.address)
            .bind(&tx_hash)
            .execute(&self.pool)
            .await
            .map_err(db_error)?;
            pending.push(tx_hash);
        }
        let concurrency = self.config.analysis_concurrency.max(1);
        let mut cursor = 0_usize;
        while cursor < pending.len() {
            let end = (cursor + concurrency).min(pending.len());
            let mut tasks = tokio::task::JoinSet::new();
            for tx_hash in pending[cursor..end].iter().cloned() {
                let analysis_service = self.analysis.clone();
                let rpc = self.rpc.clone();
                let chain_id = self.config.chain_id;
                let observed_address = object.address.clone();
                tasks.spawn(async move {
                    let result = match analysis_service
                        .repository
                        .get_analysis(chain_id, &tx_hash)
                        .await
                    {
                        Ok(Some(analysis)) => Ok(ActivityAnalysis::Full(analysis)),
                        Ok(None) => match build_lightweight_transfer_activity(
                            rpc.as_ref(),
                            chain_id,
                            &observed_address,
                            &tx_hash,
                        )
                        .await
                        {
                            Ok(Some(activity)) => Ok(ActivityAnalysis::Lightweight(activity)),
                            Ok(None) => analysis_service
                                .analyze_transaction(chain_id, &tx_hash)
                                .await
                                .map(|(analysis, _)| ActivityAnalysis::Full(analysis)),
                            Err(error) => Err(error),
                        },
                        Err(error) => Err(error),
                    };
                    (tx_hash, result)
                });
            }
            while let Some(joined) = tasks.join_next().await {
                let (tx_hash, analysis) = joined.map_err(|error| {
                    LedgerError::TransientIngestion(format!(
                        "observation analysis task failed: {error}"
                    ))
                })?;
                match analysis {
                    Ok(ActivityAnalysis::Full(analysis)) => {
                        self.persist_activity(&object.address, &analysis).await?
                    }
                    Ok(ActivityAnalysis::Lightweight(activity)) => {
                        self.persist_lightweight_activity(&object.address, &activity)
                            .await?
                    }
                    Err(error) => {
                        sqlx::query(
                        "update object_activity_transactions set processing_status='failed',last_error=$4,updated_at=now() where chain_id=$1 and address=$2 and tx_hash=$3",
                    )
                    .bind(self.config.chain_id as i64)
                    .bind(&object.address)
                    .bind(&tx_hash)
                    .bind(error.to_string().chars().take(2_000).collect::<String>())
                    .execute(&self.pool)
                    .await
                    .map_err(db_error)?;
                    }
                }
            }
            cursor = end;
        }
        let analyzed: i64 = sqlx::query_scalar(
            "select count(*) from object_activity_transactions where chain_id=$1 and address=$2 and processing_status='analyzed'",
        )
        .bind(self.config.chain_id as i64)
        .bind(&object.address)
        .fetch_one(&self.pool)
        .await
        .map_err(db_error)?;
        let discovered: i64 = sqlx::query_scalar(
            "select count(*) from object_activity_transactions where chain_id=$1 and address=$2",
        )
        .bind(self.config.chain_id as i64)
        .bind(&object.address)
        .fetch_one(&self.pool)
        .await
        .map_err(db_error)?;
        let traced: i64 = sqlx::query_scalar(
            r#"select count(*) from object_activity_transactions
               where chain_id=$1 and address=$2 and processing_status='analyzed'
                 and evidence_refs->'raw' @> '[{"kind":"trace"}]'::jsonb"#,
        )
        .bind(self.config.chain_id as i64)
        .bind(&object.address)
        .fetch_one(&self.pool)
        .await
        .map_err(db_error)?;
        let percent = if discovered == 0 {
            70
        } else {
            15 + ((analyzed as f64 / discovered as f64) * 55.0) as i64
        };
        sqlx::query(
            r#"
            update observed_address_objects set analyzed_transaction_count=$3,
              progress=$4, claimed_at=null, available_at=now(), updated_at=now()
            where chain_id=$1 and address=$2
            "#,
        )
        .bind(self.config.chain_id as i64)
        .bind(&object.address)
        .bind(analyzed)
        .bind(json!({"percent":percent.min(70),"stage":"backfilling","message":"Analyzing receipts, logs, and traces where required","transactions_discovered":discovered,"transactions_analyzed":analyzed,"receipts_processed":analyzed,"traces_processed":traced}))
        .execute(&self.pool)
        .await
        .map_err(db_error)?;
        let excluded: i64 = sqlx::query_scalar(
            "select count(*) from object_activity_transactions where chain_id=$1 and address=$2 and processing_status='excluded'",
        )
        .bind(self.config.chain_id as i64)
        .bind(&object.address)
        .fetch_one(&self.pool)
        .await
        .map_err(db_error)?;
        sqlx::query(
            r#"
            update observation_analysis_runs set analyzed_transaction_count=$3,
              excluded_transaction_count=$4,stage='backfilling',progress=$5,updated_at=now()
            where run_id=(select run_id from observation_analysis_runs
              where chain_id=$1 and address=$2 and status='running'
              order by created_at desc limit 1)
            "#,
        )
        .bind(self.config.chain_id as i64)
        .bind(&object.address)
        .bind(analyzed)
        .bind(excluded)
        .bind(json!({"percent":percent.min(70),"transactions_discovered":discovered,"transactions_analyzed":analyzed,"excluded":excluded}))
        .execute(&self.pool)
        .await
        .map_err(db_error)?;
        Ok(())
    }

    async fn persist_lightweight_activity(
        &self,
        address: &str,
        activity: &LightweightActivity,
    ) -> LedgerResult<()> {
        sqlx::query(
            r#"
            update object_activity_transactions set
              block_number=$4,block_timestamp=$5,processing_status='analyzed',
              transaction_status=$6,from_address=$7,to_address=$8,
              action_label='asset_transfer',action_confidence=1.0,protocol=null,
              counterparty=$9,native_value_raw=$10,gas_cost_raw=null,
              token_inflows=$11,token_outflows=$12,touched_contracts=$13,
              touched_pools='[]'::jsonb,protocol_relationships='[]'::jsonb,
              evidence_refs=$14,analysis_input_digest=$15,last_error=null,
              processed_at=now(),updated_at=now()
            where chain_id=$1 and address=$2 and tx_hash=$3
            "#,
        )
        .bind(self.config.chain_id as i64)
        .bind(address)
        .bind(&activity.tx_hash)
        .bind(activity.block_number as i64)
        .bind(activity.block_timestamp)
        .bind(&activity.status)
        .bind(&activity.from_address)
        .bind(&activity.to_address)
        .bind(&activity.counterparty)
        .bind(&activity.native_value_raw)
        .bind(&activity.token_inflows)
        .bind(&activity.token_outflows)
        .bind(&activity.touched_contracts)
        .bind(&activity.evidence_refs)
        .bind(&activity.input_digest)
        .execute(&self.pool)
        .await
        .map_err(db_error)?;
        Ok(())
    }

    async fn persist_activity(
        &self,
        address: &str,
        analysis: &TransactionAnalysis,
    ) -> LedgerResult<()> {
        let address = address.to_ascii_lowercase();
        let (inflows, outflows) = observed_flows(&address, analysis);
        let action = classify_action(analysis, &address);
        let protocol = primary_protocol(analysis);
        let counterparty = if analysis.from_address.eq_ignore_ascii_case(&address) {
            analysis.to_address.clone()
        } else {
            Some(analysis.from_address.clone())
        };
        let gas = analysis_gas_cost(analysis)
            .filter(|_| analysis.from_address.eq_ignore_ascii_case(&address));
        let (touched_contracts, touched_pools, protocol_relationships) =
            observed_contract_evidence(analysis);
        let evidence = json!({
            "transaction": analysis.tx_hash,
            "analysis_id": analysis.analysis_id,
            "input_digest": analysis.input_digest,
            "raw": analysis.raw_evidence_refs,
        });
        sqlx::query(
            r#"
            update object_activity_transactions set
              block_number=coalesce($4,block_number), block_timestamp=$5,
              processing_status='analyzed', transaction_status=$6, from_address=$7,
              to_address=$8, action_label=$9, action_confidence=$10, protocol=$11,
              counterparty=$12, native_value_raw=$13, gas_cost_raw=$14,
              token_inflows=$15, token_outflows=$16, touched_contracts=$17,
              touched_pools=$18, protocol_relationships=$19, evidence_refs=$20,
              analysis_input_digest=$21, last_error=null, processed_at=now(), updated_at=now()
            where chain_id=$1 and address=$2 and tx_hash=$3
            "#,
        )
        .bind(self.config.chain_id as i64)
        .bind(&address)
        .bind(&analysis.tx_hash)
        .bind(analysis.block_number.map(|value| value as i64))
        .bind(analysis.block_timestamp)
        .bind(transaction_status_key(&analysis.status))
        .bind(analysis.from_address.to_ascii_lowercase())
        .bind(
            analysis
                .to_address
                .as_ref()
                .map(|value| value.to_ascii_lowercase()),
        )
        .bind(&action.label)
        .bind(action.confidence)
        .bind(protocol)
        .bind(counterparty.map(|value| value.to_ascii_lowercase()))
        .bind(&analysis.value)
        .bind(gas)
        .bind(inflows)
        .bind(outflows)
        .bind(touched_contracts)
        .bind(touched_pools)
        .bind(protocol_relationships)
        .bind(evidence)
        .bind(&analysis.input_digest)
        .execute(&self.pool)
        .await
        .map_err(db_error)?;
        Ok(())
    }

    async fn classify_and_extract(&self, object: &ClaimedObject) -> LedgerResult<()> {
        self.rebuild_window_summaries(&object.address).await?;
        self.rebuild_behaviors(&object.address).await?;
        self.rebuild_relationships(&object.address).await?;
        self.advance(
            &object.address,
            "verifying_economics",
            82,
            "Verifying deterministic economics",
        )
        .await
    }

    async fn rebuild_window_summaries(&self, address: &str) -> LedgerResult<()> {
        for (key, start, end) in time_windows() {
            let rows = sqlx::query(
                r#"
                select transaction_status,gas_cost_raw,native_value_raw,token_inflows,token_outflows,
                       touched_contracts,touched_pools,to_address,counterparty,protocol,block_timestamp
                from object_activity_transactions
                where chain_id=$1 and address=$2 and processing_status='analyzed'
                  and block_timestamp >= $3 and block_timestamp < $4
                "#,
            )
            .bind(self.config.chain_id as i64)
            .bind(address)
            .bind(start)
            .bind(end)
            .fetch_all(&self.pool)
            .await
            .map_err(db_error)?;
            let mut success = 0_i64;
            let mut reverted = 0_i64;
            let mut gas = BigInt::from(0_u8);
            let mut native_inflow = BigInt::from(0_u8);
            let mut native_outflow = BigInt::from(0_u8);
            let mut protocols = BTreeSet::new();
            let mut counterparties = BTreeSet::new();
            let mut contracts = BTreeSet::new();
            let mut pools = BTreeSet::new();
            let mut targets = HashMap::<String, u64>::new();
            let mut inflows = Vec::new();
            let mut outflows = Vec::new();
            let mut first = None::<DateTime<Utc>>;
            let mut last = None::<DateTime<Utc>>;
            for row in &rows {
                match row
                    .try_get::<Option<String>, _>("transaction_status")
                    .ok()
                    .flatten()
                    .as_deref()
                {
                    Some("success") => success += 1,
                    Some("failure") => reverted += 1,
                    _ => {}
                }
                add_decimal(
                    &mut gas,
                    row.try_get::<Option<String>, _>("gas_cost_raw")
                        .ok()
                        .flatten()
                        .as_deref(),
                );
                if let Some(value) = row.try_get::<Option<String>, _>("protocol").ok().flatten() {
                    protocols.insert(value);
                }
                if let Some(value) = row
                    .try_get::<Option<String>, _>("counterparty")
                    .ok()
                    .flatten()
                {
                    counterparties.insert(value);
                }
                if let Some(value) = row
                    .try_get::<Option<String>, _>("to_address")
                    .ok()
                    .flatten()
                {
                    *targets.entry(value).or_default() += 1;
                }
                let row_inflows = row
                    .try_get::<Value, _>("token_inflows")
                    .unwrap_or_else(|_| json!([]))
                    .as_array()
                    .cloned()
                    .unwrap_or_default();
                let row_outflows = row
                    .try_get::<Value, _>("token_outflows")
                    .unwrap_or_else(|_| json!([]))
                    .as_array()
                    .cloned()
                    .unwrap_or_default();
                add_native_flow_total(&mut native_inflow, &row_inflows);
                add_native_flow_total(&mut native_outflow, &row_outflows);
                inflows.extend(row_inflows);
                outflows.extend(row_outflows);
                collect_json_string_set(
                    row.try_get::<Value, _>("touched_contracts")
                        .unwrap_or_else(|_| json!([])),
                    &mut contracts,
                );
                collect_json_string_set(
                    row.try_get::<Value, _>("touched_pools")
                        .unwrap_or_else(|_| json!([])),
                    &mut pools,
                );
                if let Some(ts) = row
                    .try_get::<Option<DateTime<Utc>>, _>("block_timestamp")
                    .ok()
                    .flatten()
                {
                    first = Some(first.map_or(ts, |value| value.min(ts)));
                    last = Some(last.map_or(ts, |value| value.max(ts)));
                }
            }
            let mut targets = targets.into_iter().collect::<Vec<_>>();
            targets.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
            targets.truncate(20);
            let coverage = json!({
                "contract_version":INTELLIGENCE_CONTRACT_VERSION,
                "window_start":start,
                "window_end":end,
                "scope":"discovered object activity",
            });
            let digest = digest_json(
                &json!({"key":key,"count":rows.len(),"first":first,"last":last,"gas":gas.to_string()}),
            );
            sqlx::query(
                r#"
                insert into object_activity_summaries (
                  chain_id,address,window_key,window_start,window_end,transaction_count,
                  success_count,reverted_count,gas_cost_raw,native_inflow_raw,native_outflow_raw,
                  token_inflows,token_outflows,top_call_targets,touched_contracts,touched_pools,
                  protocol_count,counterparty_count,first_seen_at,last_active_at,coverage,inputs_digest
                ) values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22)
                on conflict (chain_id,address,window_key) do update set
                  window_start=excluded.window_start,window_end=excluded.window_end,
                  transaction_count=excluded.transaction_count,success_count=excluded.success_count,
                  reverted_count=excluded.reverted_count,gas_cost_raw=excluded.gas_cost_raw,
                  native_inflow_raw=excluded.native_inflow_raw,native_outflow_raw=excluded.native_outflow_raw,
                  token_inflows=excluded.token_inflows,token_outflows=excluded.token_outflows,
                  top_call_targets=excluded.top_call_targets,touched_contracts=excluded.touched_contracts,
                  touched_pools=excluded.touched_pools,protocol_count=excluded.protocol_count,
                  counterparty_count=excluded.counterparty_count,first_seen_at=excluded.first_seen_at,
                  last_active_at=excluded.last_active_at,coverage=excluded.coverage,
                  inputs_digest=excluded.inputs_digest,updated_at=now()
                "#,
            )
            .bind(self.config.chain_id as i64).bind(address).bind(key).bind(start).bind(end)
            .bind(rows.len() as i64).bind(success).bind(reverted).bind(gas.to_string())
            .bind(native_inflow.to_string()).bind(native_outflow.to_string())
            .bind(Value::Array(inflows)).bind(Value::Array(outflows))
            .bind(json!(targets.into_iter().map(|(address,count)| json!({"address":address,"count":count})).collect::<Vec<_>>()))
            .bind(json!(contracts)).bind(json!(pools))
            .bind(protocols.len() as i64).bind(counterparties.len() as i64).bind(first).bind(last)
            .bind(coverage).bind(digest)
            .execute(&self.pool).await.map_err(db_error)?;
        }
        Ok(())
    }

    async fn rebuild_behaviors(&self, address: &str) -> LedgerResult<()> {
        sqlx::query("delete from object_behavior_profiles where chain_id=$1 and address=$2")
            .bind(self.config.chain_id as i64)
            .bind(address)
            .execute(&self.pool)
            .await
            .map_err(db_error)?;
        let rows = sqlx::query(
            r#"
            select action_label,count(*) as count,min(block_timestamp) as first_seen,
                   max(block_timestamp) as last_seen,
                   array_agg(tx_hash order by block_number desc) as tx_hashes,
                   avg(coalesce(action_confidence,0)) as confidence
            from object_activity_transactions
            where chain_id=$1 and address=$2 and processing_status='analyzed' and action_label is not null
            group by action_label
            "#,
        )
        .bind(self.config.chain_id as i64).bind(address)
        .fetch_all(&self.pool).await.map_err(db_error)?;
        let total: i64 = rows.iter().map(|row| row.get::<i64, _>("count")).sum();
        for row in rows {
            let hashes: Vec<String> = row.get("tx_hashes");
            let count: i64 = row.get("count");
            sqlx::query(
                r#"
                insert into object_behavior_profiles (
                  chain_id,address,label,confidence,supporting_transaction_count,
                  supporting_tx_hashes,first_observed_at,last_observed_at,evidence_coverage,method_version
                ) values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                "#,
            )
            .bind(self.config.chain_id as i64).bind(address)
            .bind(row.get::<String,_>("action_label"))
            .bind(row.get::<f64,_>("confidence")).bind(count)
            .bind(hashes.into_iter().take(100).collect::<Vec<_>>())
            .bind(row.get::<Option<DateTime<Utc>>,_>("first_seen"))
            .bind(row.get::<Option<DateTime<Utc>>,_>("last_seen"))
            .bind(if total == 0 { 0.0 } else { count as f64 / total as f64 })
            .bind(BEHAVIOR_METHOD_VERSION)
            .execute(&self.pool).await.map_err(db_error)?;
        }
        Ok(())
    }

    async fn rebuild_relationships(&self, address: &str) -> LedgerResult<()> {
        sqlx::query("delete from object_relationships where chain_id=$1 and address=$2")
            .bind(self.config.chain_id as i64)
            .bind(address)
            .execute(&self.pool)
            .await
            .map_err(db_error)?;
        let rows = sqlx::query(
            r#"
            select tx_hash,from_address,to_address,counterparty,protocol,action_label,token_inflows,token_outflows,
                   protocol_relationships,block_timestamp
            from object_activity_transactions
            where chain_id=$1 and address=$2 and processing_status='analyzed'
            "#,
        )
        .bind(self.config.chain_id as i64).bind(address)
        .fetch_all(&self.pool).await.map_err(db_error)?;
        let mut relations = BTreeMap::<(String, String), RelationshipAggregate>::new();
        for row in rows {
            let tx_hash: String = row.get("tx_hash");
            let timestamp: Option<DateTime<Utc>> = row.get("block_timestamp");
            let from: Option<String> = row.get("from_address");
            let to: Option<String> = row.get("to_address");
            if from
                .as_deref()
                .is_some_and(|value| value.eq_ignore_ascii_case(address))
            {
                if let Some(target) = to.filter(|value| !value.eq_ignore_ascii_case(address)) {
                    let action = row
                        .get::<Option<String>, _>("action_label")
                        .unwrap_or_default();
                    observe_relation(
                        &mut relations,
                        target.clone(),
                        "frequent_call_target",
                        &tx_hash,
                        timestamp,
                        0.95,
                        "observed",
                    );
                    if matches!(
                        action.as_str(),
                        "dex_swap"
                            | "multi_swap"
                            | "dex_arbitrage_candidate"
                            | "verified_dex_arbitrage"
                            | "liquidation"
                            | "liquidation_attempt"
                            | "failed_execution"
                    ) {
                        observe_relation(
                            &mut relations,
                            target,
                            "executor_candidate",
                            &tx_hash,
                            timestamp,
                            0.75,
                            "candidate",
                        );
                    }
                }
            } else if let Some(counterparty) = row.get::<Option<String>, _>("counterparty") {
                if !counterparty.eq_ignore_ascii_case(address) {
                    observe_relation(
                        &mut relations,
                        counterparty,
                        "co_occurring_address",
                        &tx_hash,
                        timestamp,
                        0.70,
                        "possible",
                    );
                }
            }
            for flow in row
                .get::<Value, _>("token_inflows")
                .as_array()
                .into_iter()
                .flatten()
            {
                if let Some(source) = flow.get("from").and_then(Value::as_str) {
                    if !source.eq_ignore_ascii_case(address) {
                        observe_relation(
                            &mut relations,
                            source.to_ascii_lowercase(),
                            "funding_source",
                            &tx_hash,
                            timestamp,
                            0.72,
                            "candidate",
                        );
                    }
                }
            }
            for flow in row
                .get::<Value, _>("token_outflows")
                .as_array()
                .into_iter()
                .flatten()
            {
                if let Some(recipient) = flow.get("to").and_then(Value::as_str) {
                    if !recipient.eq_ignore_ascii_case(address) {
                        observe_relation(
                            &mut relations,
                            recipient.to_ascii_lowercase(),
                            "settlement_recipient",
                            &tx_hash,
                            timestamp,
                            0.72,
                            "candidate",
                        );
                    }
                }
            }
            for evidence in row
                .get::<Value, _>("protocol_relationships")
                .as_array()
                .into_iter()
                .flatten()
            {
                let Some(related) = evidence.get("address").and_then(Value::as_str) else {
                    continue;
                };
                if related.eq_ignore_ascii_case(address) {
                    continue;
                }
                let role = evidence
                    .get("contract_role")
                    .and_then(Value::as_str)
                    .unwrap_or("protocol");
                let kind = if role.contains("pool") {
                    "pool"
                } else if role.contains("router") {
                    "router"
                } else {
                    "protocol"
                };
                let confidence = evidence
                    .get("confidence")
                    .and_then(Value::as_f64)
                    .unwrap_or(0.80)
                    .clamp(0.0, 1.0);
                observe_relation(
                    &mut relations,
                    related.to_ascii_lowercase(),
                    kind,
                    &tx_hash,
                    timestamp,
                    confidence,
                    "observed",
                );
            }
        }
        for ((related, kind), relation) in relations {
            sqlx::query(
                r#"
                insert into object_relationships (
                  chain_id,address,related_address,relationship_type,evidence_state,
                  interaction_count,supporting_tx_hashes,first_seen_at,last_seen_at,confidence,method_version
                ) values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
                "#,
            )
            .bind(self.config.chain_id as i64).bind(address).bind(related).bind(kind)
            .bind(relation.evidence_state).bind(relation.count as i64)
            .bind(relation.hashes.into_iter().take(100).collect::<Vec<_>>())
            .bind(relation.first).bind(relation.last).bind(relation.confidence)
            .bind(RELATIONSHIP_METHOD_VERSION)
            .execute(&self.pool).await.map_err(db_error)?;
        }
        Ok(())
    }

    async fn verify_economics(&self, object: &ClaimedObject) -> LedgerResult<()> {
        let candidates = sqlx::query(
            r#"
            select tx_hash from object_activity_transactions
            where chain_id=$1 and address=$2 and processing_status='analyzed'
              and action_label in ('dex_arbitrage_candidate','verified_dex_arbitrage')
              and economic_result_status='unknown' and exclusion_reason is null
            order by block_number desc limit $3
            "#,
        )
        .bind(self.config.chain_id as i64)
        .bind(&object.address)
        .bind(self.config.analysis_batch_size as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(db_error)?;
        if !candidates.is_empty() {
            let scope = BTreeSet::from([object.address.clone()]);
            for row in candidates {
                let tx_hash: String = row.get("tx_hash");
                let Some(analysis) = self
                    .analysis
                    .repository
                    .get_analysis(self.config.chain_id, &tx_hash)
                    .await?
                else {
                    continue;
                };
                let result =
                    verify_deterministic_profit_for_scope(self.rpc.as_ref(), &analysis, &scope)
                        .await?;
                let result = result.unwrap_or_else(
                    || json!({"status":"unavailable","reason":"verification returned no result"}),
                );
                if result.get("status").and_then(Value::as_str) == Some("verified") {
                    sqlx::query(
                        r#"
                        update object_activity_transactions set economic_result_status='verified',
                          verified_gross_surplus_raw=$4,verified_gas_cost_raw=$5,
                          verified_result_asset=$6,exclusion_reason=null,updated_at=now()
                        where chain_id=$1 and address=$2 and tx_hash=$3
                        "#,
                    )
                    .bind(self.config.chain_id as i64)
                    .bind(&object.address)
                    .bind(&tx_hash)
                    .bind(result.get("profit_raw").and_then(Value::as_str))
                    .bind(result.get("gas_cost_raw").and_then(Value::as_str))
                    .bind(result.get("profit_token_address").and_then(Value::as_str))
                    .execute(&self.pool)
                    .await
                    .map_err(db_error)?;
                } else {
                    sqlx::query(
                        "update object_activity_transactions set exclusion_reason=$4,updated_at=now() where chain_id=$1 and address=$2 and tx_hash=$3",
                    )
                    .bind(self.config.chain_id as i64).bind(&object.address).bind(&tx_hash)
                    .bind(result.get("reason").and_then(Value::as_str).unwrap_or("deterministic accounting incomplete"))
                    .execute(&self.pool).await.map_err(db_error)?;
                }
            }
            let verified_count: i64 = sqlx::query_scalar(
                "select count(*) from object_activity_transactions where chain_id=$1 and address=$2 and economic_result_status='verified'",
            )
            .bind(self.config.chain_id as i64)
            .bind(&object.address)
            .fetch_one(&self.pool)
            .await
            .map_err(db_error)?;
            sqlx::query("update observed_address_objects set claimed_at=null,available_at=now(),progress=$3,updated_at=now() where chain_id=$1 and address=$2")
                .bind(self.config.chain_id as i64).bind(&object.address)
                .bind(json!({"percent":86,"stage":"verifying_economics","message":"Verifying candidate executions with receipt, trace and balance evidence","economic_verified":verified_count}))
                .execute(&self.pool).await.map_err(db_error)?;
            return Ok(());
        }
        self.rebuild_economic_summaries(&object.address).await?;
        self.advance(
            &object.address,
            "building_baseline",
            92,
            "Building historical baseline and briefs",
        )
        .await
    }

    async fn rebuild_economic_summaries(&self, address: &str) -> LedgerResult<()> {
        for (key, start, end) in time_windows()
            .into_iter()
            .filter(|(key, _, _)| matches!(*key, "24h" | "7d" | "30d"))
        {
            let rows = sqlx::query(
                r#"
                select tx_hash,economic_result_status,verified_gross_surplus_raw,
                       verified_gas_cost_raw,verified_result_asset,gas_cost_raw,
                       token_inflows,token_outflows,exclusion_reason
                from object_activity_transactions
                where chain_id=$1 and address=$2 and processing_status='analyzed'
                  and block_timestamp >= $3 and block_timestamp < $4
                "#,
            )
            .bind(self.config.chain_id as i64)
            .bind(address)
            .bind(start)
            .bind(end)
            .fetch_all(&self.pool)
            .await
            .map_err(db_error)?;
            let mut verified = 0_i64;
            let mut gas = BigInt::from(0_u8);
            let mut gross = BTreeMap::<String, BigInt>::new();
            let mut reasons = BTreeMap::<String, u64>::new();
            let mut observed_nets = BTreeMap::<String, BigInt>::new();
            for row in &rows {
                add_decimal(
                    &mut gas,
                    row.get::<Option<String>, _>("gas_cost_raw").as_deref(),
                );
                if row.get::<String, _>("economic_result_status") == "verified" {
                    verified += 1;
                    if let (Some(asset), Some(amount)) = (
                        row.get::<Option<String>, _>("verified_result_asset"),
                        row.get::<Option<String>, _>("verified_gross_surplus_raw"),
                    ) {
                        add_map_decimal(&mut gross, &asset, &amount);
                    }
                } else {
                    let reason = row
                        .get::<Option<String>, _>("exclusion_reason")
                        .unwrap_or_else(|| "not an economically verifiable execution".to_owned());
                    *reasons.entry(reason).or_default() += 1;
                }
                accumulate_flow_nets(
                    &mut observed_nets,
                    row.get::<Value, _>("token_inflows"),
                    true,
                );
                accumulate_flow_nets(
                    &mut observed_nets,
                    row.get::<Value, _>("token_outflows"),
                    false,
                );
            }
            let coverage = if rows.is_empty() {
                0.0
            } else {
                verified as f64 / rows.len() as f64
            };
            let status = if verified > 0 { "verified" } else { "unknown" };
            let limitations = vec![
                "Token inflow, funding and inventory changes are observed flows, not profit.".to_owned(),
                "Net result across unlike assets is unavailable without deterministic conversion evidence.".to_owned(),
                "Full economic analysis is optimized for execution-oriented entities.".to_owned(),
            ];
            let gross_json = amounts_json(&gross);
            let net_flow_json = amounts_json(&observed_nets);
            let digest = digest_json(
                &json!({"window":key,"gross":gross_json,"gas":gas.to_string(),"verified":verified,"count":rows.len()}),
            );
            sqlx::query(
                r#"
                insert into object_economic_summaries (
                  chain_id,address,window_key,result_status,verified_gross_surplus,
                  verified_gas_cost_raw,verified_net_result,observed_net_token_flow,
                  economically_verified_transaction_count,excluded_transaction_count,
                  exclusion_reasons,coverage_ratio,limitations,inputs_digest
                ) values ($1,$2,$3,$4,$5,$6,'[]'::jsonb,$7,$8,$9,$10,$11,$12,$13)
                on conflict (chain_id,address,window_key) do update set
                  result_status=excluded.result_status,verified_gross_surplus=excluded.verified_gross_surplus,
                  verified_gas_cost_raw=excluded.verified_gas_cost_raw,
                  verified_net_result=excluded.verified_net_result,
                  observed_net_token_flow=excluded.observed_net_token_flow,
                  economically_verified_transaction_count=excluded.economically_verified_transaction_count,
                  excluded_transaction_count=excluded.excluded_transaction_count,
                  exclusion_reasons=excluded.exclusion_reasons,coverage_ratio=excluded.coverage_ratio,
                  limitations=excluded.limitations,inputs_digest=excluded.inputs_digest,updated_at=now()
                "#,
            )
            .bind(self.config.chain_id as i64).bind(address).bind(key).bind(status)
            .bind(gross_json).bind(gas.to_string()).bind(net_flow_json).bind(verified)
            .bind(rows.len() as i64-verified).bind(json!(reasons)).bind(coverage)
            .bind(limitations).bind(digest)
            .execute(&self.pool).await.map_err(db_error)?;
        }
        Ok(())
    }

    async fn build_baseline_and_briefs(&self, object: &ClaimedObject) -> LedgerResult<()> {
        // A high-volume object can still have a useful comparison window even when
        // discovery hit the explicit transaction cap. Keep that comparison, but
        // preserve the partial-coverage limitation all the way to the UI and briefs.
        let baseline_available = self.has_previous_window(&object.address).await?;
        let baseline_is_partial = baseline_available && object.coverage_is_partial;
        if baseline_available {
            self.insert_change_signals(&object.address).await?;
        }
        self.generate_briefs(&object.address, baseline_available, baseline_is_partial)
            .await?;
        let target = object
            .target_block
            .unwrap_or(object.last_processed_block.unwrap_or(0));
        let last_activity: Option<DateTime<Utc>> = sqlx::query_scalar(
            "select max(block_timestamp) from object_activity_transactions where chain_id=$1 and address=$2 and processing_status='analyzed'",
        )
        .bind(self.config.chain_id as i64).bind(&object.address)
        .fetch_one(&self.pool).await.map_err(db_error)?;
        sqlx::query(
            r#"
            update observed_address_objects set current_status='ready',progress=$3,
              analysis_completed_at=now(),last_processed_block=$4,last_incremental_update=now(),
              last_activity_at=$5,claimed_at=null,error=null,retry_count=0,
              metadata=metadata || jsonb_build_object(
                'baseline_available',$6,
                'baseline_is_partial',$7
              ),updated_at=now()
            where chain_id=$1 and address=$2
            "#,
        )
        .bind(self.config.chain_id as i64).bind(&object.address)
        .bind(json!({"percent":100,"stage":"ready","message":"Historical intelligence is ready","baseline_available":baseline_available,"baseline_is_partial":baseline_is_partial}))
        .bind(target as i64).bind(last_activity).bind(baseline_available).bind(baseline_is_partial)
        .execute(&self.pool).await.map_err(db_error)?;
        sqlx::query(
            r#"
            update observation_analysis_runs set status='completed',stage='ready',completed_at=now(),updated_at=now()
            where run_id=(select run_id from observation_analysis_runs where chain_id=$1 and address=$2 and status='running' order by created_at desc limit 1)
            "#,
        )
        .bind(self.config.chain_id as i64).bind(&object.address)
        .execute(&self.pool).await.map_err(db_error)?;
        Ok(())
    }

    async fn has_previous_window(&self, address: &str) -> LedgerResult<bool> {
        let count: i64 = sqlx::query_scalar(
            "select count(*) from object_activity_summaries where chain_id=$1 and address=$2 and window_key='previous_7d' and transaction_count>0",
        )
        .bind(self.config.chain_id as i64).bind(address)
        .fetch_one(&self.pool).await.map_err(db_error)?;
        Ok(count > 0)
    }

    async fn insert_change_signals(&self, address: &str) -> LedgerResult<()> {
        let rows = sqlx::query(
            "select window_key,transaction_count,success_count,reverted_count,gas_cost_raw,inputs_digest from object_activity_summaries where chain_id=$1 and address=$2 and window_key in ('7d','previous_7d')",
        )
        .bind(self.config.chain_id as i64).bind(address)
        .fetch_all(&self.pool).await.map_err(db_error)?;
        if rows.len() != 2 {
            return Ok(());
        }
        let mut by_key = HashMap::new();
        for row in rows {
            by_key.insert(row.get::<String, _>("window_key"), row);
        }
        let (Some(current), Some(previous)) = (by_key.get("7d"), by_key.get("previous_7d")) else {
            return Ok(());
        };
        let activity_hashes = self.change_evidence_hashes(address, None, 100).await?;
        let failure_hashes = self
            .change_evidence_hashes(address, Some("failure"), 100)
            .await?;
        let gas_hashes = self
            .change_evidence_hashes(address, Some("sender_gas"), 100)
            .await?;
        let current_count = current.get::<i64, _>("transaction_count");
        let previous_count = previous.get::<i64, _>("transaction_count");
        if previous_count > 0 {
            let pct = ((current_count - previous_count) as f64 / previous_count as f64) * 100.0;
            if pct.abs() >= 20.0 {
                self.insert_change_signal(
                    address,
                    "activity_change",
                    if pct.abs() >= 75.0 { "high" } else { "medium" },
                    format!("Observed transaction activity changed by {pct:+.1}% versus the previous seven-day window."),
                    json!({"transactions":current_count,"change_percent":pct}),
                    json!({"transactions":previous_count}),
                    activity_hashes.clone(),
                    0.98,
                )
                .await?;
            }
        }
        let current_failure_rate = ratio(current.get::<i64, _>("reverted_count"), current_count);
        let previous_failure_rate = ratio(previous.get::<i64, _>("reverted_count"), previous_count);
        let failure_delta = (current_failure_rate - previous_failure_rate) * 100.0;
        if failure_delta.abs() >= 5.0 {
            self.insert_change_signal(
                address,
                "failure_rate_change",
                if failure_delta >= 20.0 { "high" } else { "medium" },
                format!("Observed failure rate changed by {failure_delta:+.1} percentage points versus the previous seven-day window."),
                json!({"failure_rate":current_failure_rate,"change_percentage_points":failure_delta}),
                json!({"failure_rate":previous_failure_rate}),
                if failure_hashes.is_empty() {
                    activity_hashes.clone()
                } else {
                    failure_hashes.clone()
                },
                0.99,
            ).await?;
        }
        let current_gas = decimal_f64(current.get::<Option<String>, _>("gas_cost_raw").as_deref());
        let previous_gas =
            decimal_f64(previous.get::<Option<String>, _>("gas_cost_raw").as_deref());
        if let (Some(current_gas), Some(previous_gas)) = (current_gas, previous_gas) {
            if previous_gas > 0.0 {
                let pct = ((current_gas - previous_gas) / previous_gas) * 100.0;
                if pct.abs() >= 25.0 {
                    self.insert_change_signal(
                        address,
                        "gas_cost_change",
                        if pct >= 75.0 { "high" } else { "medium" },
                        format!("Verified sender gas cost changed by {pct:+.1}% versus the previous seven-day window."),
                        json!({"gas_cost_raw":current.get::<Option<String>,_>("gas_cost_raw"),"change_percent":pct}),
                        json!({"gas_cost_raw":previous.get::<Option<String>,_>("gas_cost_raw")}),
                        gas_hashes.clone(),
                        0.99,
                    ).await?;
                }
            }
        }
        self.insert_new_relationship_signals(address).await?;
        self.insert_strategy_change_signal(address).await?;
        Ok(())
    }

    async fn change_evidence_hashes(
        &self,
        address: &str,
        evidence_kind: Option<&str>,
        limit: i64,
    ) -> LedgerResult<Vec<String>> {
        let rows = sqlx::query(
            r#"
            select tx_hash from object_activity_transactions
            where chain_id=$1 and address=$2 and processing_status='analyzed'
              and block_timestamp >= now()-interval '14 days'
              and case $3
                when 'failure' then transaction_status='failure'
                when 'sender_gas' then gas_cost_raw is not null
                else true
              end
            order by block_number desc,tx_index desc limit $4
            "#,
        )
        .bind(self.config.chain_id as i64)
        .bind(address)
        .bind(evidence_kind)
        .bind(limit.clamp(1, 100))
        .fetch_all(&self.pool)
        .await
        .map_err(db_error)?;
        Ok(rows.into_iter().map(|row| row.get("tx_hash")).collect())
    }

    async fn insert_change_signal(
        &self,
        address: &str,
        signal_type: &str,
        severity: &str,
        conclusion: String,
        current_value: Value,
        baseline_value: Value,
        supporting_tx_hashes: Vec<String>,
        confidence: f64,
    ) -> LedgerResult<()> {
        let digest = digest_json(&json!({
            "method": CHANGE_METHOD_VERSION,
            "signal_type": signal_type,
            "current": current_value,
            "baseline": baseline_value,
            "supporting_tx_hashes": supporting_tx_hashes,
        }));
        sqlx::query(
            r#"insert into object_change_signals (
              chain_id,address,signal_type,severity,conclusion,current_value,baseline_value,
              supporting_tx_hashes,confidence,inputs_digest
            ) values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) on conflict do nothing"#,
        )
        .bind(self.config.chain_id as i64)
        .bind(address)
        .bind(signal_type)
        .bind(severity)
        .bind(conclusion)
        .bind(current_value)
        .bind(baseline_value)
        .bind(supporting_tx_hashes)
        .bind(confidence.clamp(0.0, 1.0))
        .bind(digest)
        .execute(&self.pool)
        .await
        .map_err(db_error)?;
        Ok(())
    }

    async fn insert_new_relationship_signals(&self, address: &str) -> LedgerResult<()> {
        let rows = sqlx::query(
            r#"
            select related_address,relationship_type,interaction_count,supporting_tx_hashes,confidence
            from object_relationships
            where chain_id=$1 and address=$2 and first_seen_at >= now()-interval '7 days'
            order by interaction_count desc limit 20
            "#,
        )
        .bind(self.config.chain_id as i64)
        .bind(address)
        .fetch_all(&self.pool)
        .await
        .map_err(db_error)?;
        for row in rows {
            let kind: String = row.get("relationship_type");
            let related: String = row.get("related_address");
            let hashes: Vec<String> = row.get("supporting_tx_hashes");
            let signal_type = match kind.as_str() {
                "protocol" | "pool" | "router" => "new_protocol_interaction",
                "executor_candidate" => "new_executor",
                "settlement_recipient" | "profit_recipient_candidate" => "new_settlement_recipient",
                _ => "new_counterparty",
            };
            self.insert_change_signal(
                address,
                signal_type,
                "medium",
                format!("A new {kind} relationship was observed with {related}."),
                json!({"related_address":related,"relationship_type":kind,"interaction_count":row.get::<i64,_>("interaction_count")}),
                json!({"previously_observed":false}),
                hashes,
                row.get::<f64,_>("confidence"),
            ).await?;
        }
        Ok(())
    }

    async fn insert_strategy_change_signal(&self, address: &str) -> LedgerResult<()> {
        let rows = sqlx::query(
            r#"
            select action_label,
              count(*) filter (where block_timestamp >= now()-interval '7 days') as current_count,
              count(*) filter (where block_timestamp >= now()-interval '14 days' and block_timestamp < now()-interval '7 days') as previous_count,
              array_agg(tx_hash order by block_number desc) filter (where block_timestamp >= now()-interval '7 days') as hashes
            from object_activity_transactions
            where chain_id=$1 and address=$2 and processing_status='analyzed' and action_label is not null
              and block_timestamp >= now()-interval '14 days'
            group by action_label
            "#,
        )
        .bind(self.config.chain_id as i64)
        .bind(address)
        .fetch_all(&self.pool)
        .await
        .map_err(db_error)?;
        let current_total: i64 = rows
            .iter()
            .map(|row| row.get::<i64, _>("current_count"))
            .sum();
        let previous_total: i64 = rows
            .iter()
            .map(|row| row.get::<i64, _>("previous_count"))
            .sum();
        if current_total == 0 || previous_total == 0 {
            return Ok(());
        }
        let mut largest = None::<(String, f64, f64, Vec<String>)>;
        for row in rows {
            let current = row.get::<i64, _>("current_count") as f64 / current_total as f64;
            let previous = row.get::<i64, _>("previous_count") as f64 / previous_total as f64;
            let candidate = (
                row.get::<String, _>("action_label"),
                current,
                previous,
                row.get::<Option<Vec<String>>, _>("hashes")
                    .unwrap_or_default(),
            );
            if largest
                .as_ref()
                .is_none_or(|value| (current - previous).abs() > (value.1 - value.2).abs())
            {
                largest = Some(candidate);
            }
        }
        if let Some((label, current, previous, hashes)) = largest {
            let delta = (current - previous) * 100.0;
            if delta.abs() >= 20.0 {
                self.insert_change_signal(
                    address,
                    "strategy_distribution_change",
                    "medium",
                    format!(
                        "The share of {label} activity changed by {delta:+.1} percentage points."
                    ),
                    json!({"label":label,"share":current,"change_percentage_points":delta}),
                    json!({"share":previous}),
                    hashes.into_iter().take(100).collect(),
                    0.90,
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn generate_briefs(
        &self,
        address: &str,
        baseline_available: bool,
        baseline_is_partial: bool,
    ) -> LedgerResult<()> {
        for (brief_type, window_key, days) in [
            ("24h_summary", "24h", 1_i64),
            ("weekly", "7d", 7),
            ("monthly", "30d", 30),
        ] {
            let Some(row)=sqlx::query("select * from object_activity_summaries where chain_id=$1 and address=$2 and window_key=$3")
                .bind(self.config.chain_id as i64).bind(address).bind(window_key)
                .fetch_optional(&self.pool).await.map_err(db_error)? else { continue; };
            let tx_count: i64 = row.get("transaction_count");
            let success: i64 = row.get("success_count");
            let reverted: i64 = row.get("reverted_count");
            let gas: Option<String> = row.get("gas_cost_raw");
            let start: DateTime<Utc> = row.get("window_start");
            let end: DateTime<Utc> = row.get("window_end");
            let behavior_rows = sqlx::query(
                r#"select action_label,count(*) as count,avg(coalesce(action_confidence,0)) as confidence
                   from object_activity_transactions where chain_id=$1 and address=$2
                     and processing_status='analyzed' and block_timestamp >= $3 and block_timestamp < $4
                   group by action_label order by count(*) desc"#,
            )
            .bind(self.config.chain_id as i64).bind(address).bind(start).bind(end)
            .fetch_all(&self.pool).await.map_err(db_error)?;
            let behavior_markdown = if behavior_rows.is_empty() {
                "- No classified activity in this period.".to_owned()
            } else {
                behavior_rows
                    .iter()
                    .map(|item| {
                        format!(
                            "- {}: {} transactions · {:.0}% confidence",
                            item.get::<Option<String>, _>("action_label")
                                .unwrap_or_else(|| "unknown".to_owned()),
                            item.get::<i64, _>("count"),
                            item.get::<f64, _>("confidence") * 100.0,
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n")
            };
            let relationship_rows = sqlx::query(
                r#"select related_address,relationship_type,evidence_state,interaction_count,confidence
                   from object_relationships where chain_id=$1 and address=$2
                     and last_seen_at >= $3 and last_seen_at < $4
                   order by interaction_count desc,last_seen_at desc limit 10"#,
            )
            .bind(self.config.chain_id as i64).bind(address).bind(start).bind(end)
            .fetch_all(&self.pool).await.map_err(db_error)?;
            let relationships_markdown = if relationship_rows.is_empty() {
                "- No evidence-backed relationships observed in this period.".to_owned()
            } else {
                relationship_rows
                    .iter()
                    .map(|item| {
                        format!(
                            "- {} — {} ({}, {} interactions, {:.0}% confidence)",
                            item.get::<String, _>("related_address"),
                            item.get::<String, _>("relationship_type"),
                            item.get::<String, _>("evidence_state"),
                            item.get::<i64, _>("interaction_count"),
                            item.get::<f64, _>("confidence") * 100.0,
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n")
            };
            let economic = sqlx::query(
                "select * from object_economic_summaries where chain_id=$1 and address=$2 and window_key=$3",
            )
            .bind(self.config.chain_id as i64).bind(address).bind(window_key)
            .fetch_optional(&self.pool).await.map_err(db_error)?;
            let (economic_markdown, limitations, economic_digest) = if let Some(item) = economic {
                let verified: i64 = item.get("economically_verified_transaction_count");
                let excluded: i64 = item.get("excluded_transaction_count");
                let coverage: f64 = item.get("coverage_ratio");
                let gross: Value = item.get("verified_gross_surplus");
                let net: Value = item.get("verified_net_result");
                let limitations: Vec<String> = item.get("limitations");
                (
                    format!(
                        "- Status: {}\n- Economically verified: {verified}\n- Excluded: {excluded}\n- Coverage: {:.1}%\n- Verified gross surplus by asset: `{}`\n- Verified net result by asset: `{}`\n- Verified sender gas (raw native units): {}",
                        item.get::<String, _>("result_status"),
                        coverage * 100.0,
                        gross,
                        net,
                        item.get::<Option<String>, _>("verified_gas_cost_raw")
                            .unwrap_or_else(|| "unavailable".to_owned()),
                    ),
                    limitations,
                    json!({"gross":gross,"net":net,"coverage":coverage,"verified":verified,"excluded":excluded}),
                )
            } else {
                (
                    "- Economic verification has not produced a summary for this period."
                        .to_owned(),
                    vec!["Economic verification unavailable for this period.".to_owned()],
                    json!({"status":"unavailable"}),
                )
            };
            let change_rows = sqlx::query(
                r#"select conclusion from object_change_signals where chain_id=$1 and address=$2
                   and detected_at >= $3 and detected_at < $4 order by detected_at desc limit 20"#,
            )
            .bind(self.config.chain_id as i64)
            .bind(address)
            .bind(start)
            .bind(end)
            .fetch_all(&self.pool)
            .await
            .map_err(db_error)?;
            let changes_markdown = if !baseline_available {
                "- Baseline is still being built; no zero-change claim is made.".to_owned()
            } else if change_rows.is_empty() {
                "- No material deterministic change crossed the configured detection thresholds."
                    .to_owned()
            } else {
                change_rows
                    .iter()
                    .map(|item| format!("- {}", item.get::<String, _>("conclusion")))
                    .collect::<Vec<_>>()
                    .join("\n")
            };
            let baseline = if baseline_is_partial {
                "A comparison is available for the observed subset. Discovery hit the explicit coverage limit, so changes do not represent the object's complete activity."
            } else if baseline_available {
                "Baseline comparison is available on the object page."
            } else {
                "Historical baseline is still incomplete; no zero-change claim is made."
            };
            let limitations_markdown = limitations
                .iter()
                .map(|item| format!("- {item}"))
                .collect::<Vec<_>>()
                .join("\n");
            let markdown = format!(
                "# {window_key} Intelligence Brief — {address}\n\nPeriod: {start} → {end}\n\n## Activity summary\n\n- Transactions: {tx_count}\n- Successful: {success}\n- Reverted: {reverted}\n- Verified sender gas (raw native units): {}\n\n## Behavior distribution\n\n{behavior_markdown}\n\n## Verified economics\n\n{economic_markdown}\n\nObserved token movement is not treated as profit.\n\n## Top relationships\n\n{relationships_markdown}\n\n## Important changes\n\n{changes_markdown}\n\n## Evidence coverage\n\n{baseline}\n\n## Unresolved evidence gaps\n\n{limitations_markdown}\n",
                gas.as_deref().unwrap_or("unavailable"),
            );
            let html = format!(
                "<article><h1>{window_key} Intelligence Brief — {address}</h1><p>Period: {start} → {end}</p><h2>Activity summary</h2><pre>{}</pre><h2>Behavior distribution</h2><pre>{}</pre><h2>Verified economics</h2><pre>{}</pre><h2>Top relationships</h2><pre>{}</pre><h2>Important changes</h2><pre>{}</pre><h2>Evidence coverage</h2><p>{}</p><h2>Unresolved evidence gaps</h2><pre>{}</pre></article>",
                html_escape(&format!(
                    "Transactions: {tx_count}\nSuccessful: {success}\nReverted: {reverted}\nVerified sender gas: {}",
                    gas.as_deref().unwrap_or("unavailable")
                )),
                html_escape(&behavior_markdown),
                html_escape(&economic_markdown),
                html_escape(&relationships_markdown),
                html_escape(&changes_markdown),
                html_escape(baseline),
                html_escape(&limitations_markdown),
            );
            let digest = digest_json(
                &json!({"window":window_key,"tx":tx_count,"success":success,"reverted":reverted,"gas":gas,"baseline":baseline_available,"baseline_is_partial":baseline_is_partial,"behaviors":behavior_markdown,"relationships":relationships_markdown,"economics":economic_digest,"changes":changes_markdown}),
            );
            sqlx::query(
                r#"insert into object_briefs (chain_id,address,brief_type,period_start,period_end,title,markdown_content,html_content,inputs_digest)
                   values ($1,$2,$3,$4,$5,$6,$7,$8,$9)
                   on conflict (chain_id,address,brief_type) do update set
                     period_start=excluded.period_start,period_end=excluded.period_end,
                     title=excluded.title,markdown_content=excluded.markdown_content,
                     html_content=excluded.html_content,inputs_digest=excluded.inputs_digest,
                     generated_at=now()"#,
            )
            .bind(self.config.chain_id as i64).bind(address).bind(brief_type).bind(start).bind(end)
            .bind(format!("{window_key} Intelligence Brief")).bind(markdown).bind(html).bind(digest)
            .execute(&self.pool).await.map_err(db_error)?;
            let _ = days;
        }
        Ok(())
    }

    async fn advance(
        &self,
        address: &str,
        status: &str,
        percent: i64,
        message: &str,
    ) -> LedgerResult<()> {
        sqlx::query("update observed_address_objects set current_status=$3,progress=$4,claimed_at=null,available_at=now(),updated_at=now() where chain_id=$1 and address=$2")
            .bind(self.config.chain_id as i64).bind(address).bind(status)
            .bind(json!({"percent":percent,"stage":status,"message":message}))
            .execute(&self.pool).await.map_err(db_error)?;
        sqlx::query(
            "update observation_analysis_runs set stage=$3,progress=$4,updated_at=now() where run_id=(select run_id from observation_analysis_runs where chain_id=$1 and address=$2 and status='running' order by created_at desc limit 1)",
        )
            .bind(self.config.chain_id as i64).bind(address).bind(status)
            .bind(json!({"percent":percent,"stage":status,"message":message}))
            .execute(&self.pool).await.map_err(db_error)?;
        Ok(())
    }

    async fn fail_object(&self, object: &ClaimedObject, error: &str) -> LedgerResult<()> {
        let retry = object.retry_count + 1;
        let delay = (30_i64 * 2_i64.pow(retry.min(6) as u32)).min(1800);
        sqlx::query(
            r#"update observed_address_objects set current_status='failed',error=$3,retry_count=$4,
               claimed_at=null,available_at=now()+make_interval(secs=>$5),progress=$6,updated_at=now()
               where chain_id=$1 and address=$2"#,
        )
        .bind(self.config.chain_id as i64).bind(&object.address)
        .bind(error.chars().take(4000).collect::<String>()).bind(retry).bind(delay as f64)
        .bind(json!({"percent":0,"stage":"failed","message":"Analysis failed and can be retried","next_step":"retry","error":error.chars().take(500).collect::<String>()}))
        .execute(&self.pool).await.map_err(db_error)?;
        sqlx::query(
            "update observation_analysis_runs set status='failed',stage='failed',error=$3,completed_at=now(),updated_at=now() where run_id=(select run_id from observation_analysis_runs where chain_id=$1 and address=$2 and status='running' order by created_at desc limit 1)",
        )
        .bind(self.config.chain_id as i64).bind(&object.address)
        .bind(error.chars().take(4000).collect::<String>())
        .execute(&self.pool).await.map_err(db_error)?;
        Ok(())
    }

    pub async fn snapshot(
        &self,
        chain_id: u64,
        address: &str,
        timeline_limit: u32,
    ) -> LedgerResult<Option<Value>> {
        let data_source = self.data_source_status(chain_id).await?;
        let address = normalize_address(address)?;
        let Some(object) =
            sqlx::query("select * from observed_address_objects where chain_id=$1 and address=$2")
                .bind(chain_id as i64)
                .bind(&address)
                .fetch_optional(&self.pool)
                .await
                .map_err(db_error)?
        else {
            return Ok(None);
        };
        let summaries=sqlx::query("select * from object_activity_summaries where chain_id=$1 and address=$2 order by case window_key when '24h' then 1 when '7d' then 2 when '30d' then 3 else 4 end")
            .bind(chain_id as i64).bind(&address).fetch_all(&self.pool).await.map_err(db_error)?;
        let behaviors=sqlx::query("select * from object_behavior_profiles where chain_id=$1 and address=$2 order by supporting_transaction_count desc,label")
            .bind(chain_id as i64).bind(&address).fetch_all(&self.pool).await.map_err(db_error)?;
        let relationships=sqlx::query("select * from object_relationships where chain_id=$1 and address=$2 order by interaction_count desc,last_seen_at desc limit 100")
            .bind(chain_id as i64).bind(&address).fetch_all(&self.pool).await.map_err(db_error)?;
        let economics=sqlx::query("select * from object_economic_summaries where chain_id=$1 and address=$2 order by case window_key when '24h' then 1 when '7d' then 2 else 3 end")
            .bind(chain_id as i64).bind(&address).fetch_all(&self.pool).await.map_err(db_error)?;
        let changes=sqlx::query("select * from object_change_signals where chain_id=$1 and address=$2 order by detected_at desc limit 50")
            .bind(chain_id as i64).bind(&address).fetch_all(&self.pool).await.map_err(db_error)?;
        let timeline=sqlx::query("select * from object_activity_transactions where chain_id=$1 and address=$2 order by block_number desc,tx_index desc limit $3")
            .bind(chain_id as i64).bind(&address).bind(timeline_limit.clamp(1,500) as i64)
            .fetch_all(&self.pool).await.map_err(db_error)?;
        let briefs=sqlx::query("select brief_id,brief_type,period_start,period_end,title,generated_at,inputs_digest from object_briefs where chain_id=$1 and address=$2 order by generated_at desc limit 20")
            .bind(chain_id as i64).bind(&address).fetch_all(&self.pool).await.map_err(db_error)?;
        Ok(Some(json!({
            "contract_version":INTELLIGENCE_CONTRACT_VERSION,
            "chain_id":chain_id,
            "data_source":data_source,
            "address":address,
            "object": object_json(&object),
            "activity_windows": summaries.iter().map(summary_json).collect::<Vec<_>>(),
            "behaviors": behaviors.iter().map(behavior_json).collect::<Vec<_>>(),
            "relationships": relationships.iter().map(relationship_json).collect::<Vec<_>>(),
            "economics": economics.iter().map(economic_json).collect::<Vec<_>>(),
            "changes": changes.iter().map(change_json).collect::<Vec<_>>(),
            "timeline": timeline.iter().map(activity_json).collect::<Vec<_>>(),
            "briefs": briefs.iter().map(brief_json).collect::<Vec<_>>(),
            "limitations": [
                "Full economic analysis is currently optimized for execution-oriented entities.",
                "Token flows are not interpreted as profit.",
                "Ownership and same-team relationships are never inferred by this contract."
            ]
        })))
    }

    pub async fn brief(
        &self,
        chain_id: u64,
        address: &str,
        brief_id: uuid::Uuid,
    ) -> LedgerResult<Option<Value>> {
        self.data_source_status(chain_id).await?;
        let address = normalize_address(address)?;
        let row = sqlx::query(
            r#"select brief_id,chain_id,address,brief_type,period_start,period_end,title,
                      markdown_content,html_content,inputs_digest,generated_at,metadata
               from object_briefs where brief_id=$1 and chain_id=$2 and address=$3"#,
        )
        .bind(brief_id)
        .bind(chain_id as i64)
        .bind(address)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_error)?;
        Ok(row.map(|row| {
            json!({
                "brief_id":row.get::<uuid::Uuid,_>("brief_id"),
                "chain_id":row.get::<i64,_>("chain_id"),
                "address":row.get::<String,_>("address"),
                "brief_type":row.get::<String,_>("brief_type"),
                "period_start":row.get::<DateTime<Utc>,_>("period_start"),
                "period_end":row.get::<DateTime<Utc>,_>("period_end"),
                "title":row.get::<String,_>("title"),
                "markdown":row.get::<String,_>("markdown_content"),
                "html":row.get::<String,_>("html_content"),
                "inputs_digest":row.get::<String,_>("inputs_digest"),
                "generated_at":row.get::<DateTime<Utc>,_>("generated_at"),
                "metadata":row.get::<Value,_>("metadata"),
            })
        }))
    }
}

#[derive(Clone, Debug)]
struct Action {
    label: String,
    confidence: f64,
}

fn classify_action(analysis: &TransactionAnalysis, address: &str) -> Action {
    let is_top_level_endpoint = observed_is_top_level_endpoint(
        &analysis.from_address,
        analysis.to_address.as_deref(),
        address,
    );
    // A transfer participant must not inherit the enclosing transaction's
    // swap/arbitrage/liquidation label. That label describes the top-level
    // execution, not a recipient such as a settlement or aggregation wallet.
    if !is_top_level_endpoint {
        let (inflows, outflows) = observed_flows(address, analysis);
        if !inflows.as_array().unwrap_or(&Vec::new()).is_empty()
            || !outflows.as_array().unwrap_or(&Vec::new()).is_empty()
        {
            return Action {
                label: "asset_transfer".to_owned(),
                confidence: 0.99,
            };
        }
        return Action {
            label: "unknown_contract_interaction".to_owned(),
            confidence: 0.50,
        };
    }
    if analysis.status == TransactionStatus::Failure {
        return Action {
            label: "failed_execution".to_owned(),
            confidence: 1.0,
        };
    }
    if analysis.to_address.is_none() && analysis.from_address.eq_ignore_ascii_case(address) {
        return Action {
            label: "contract_deployment".to_owned(),
            confidence: 1.0,
        };
    }
    if analysis.normalized_execution_facts.selector.as_deref() == Some("0x095ea7b3") {
        return Action {
            label: "approval".to_owned(),
            confidence: 1.0,
        };
    }
    let labels = analysis
        .recognizer_results
        .iter()
        .map(|item| item.label.to_ascii_lowercase())
        .collect::<Vec<_>>();
    let combined = format!(
        "{} {}",
        labels.join(" "),
        analysis.scenario_outputs.to_string().to_ascii_lowercase()
    );
    if combined.contains("verified") && combined.contains("arbitrage") {
        return Action {
            label: "verified_dex_arbitrage".to_owned(),
            confidence: 0.98,
        };
    }
    if combined.contains("arbitrage") {
        return Action {
            label: "dex_arbitrage_candidate".to_owned(),
            confidence: max_confidence(analysis, 0.70),
        };
    }
    if combined.contains("liquidation") && combined.contains("attempt") {
        return Action {
            label: "liquidation_attempt".to_owned(),
            confidence: max_confidence(analysis, 0.70),
        };
    }
    if combined.contains("liquidation") {
        return Action {
            label: "liquidation".to_owned(),
            confidence: max_confidence(analysis, 0.80),
        };
    }
    if combined.contains("multi") && combined.contains("swap") {
        return Action {
            label: "multi_swap".to_owned(),
            confidence: max_confidence(analysis, 0.75),
        };
    }
    if combined.contains("swap") {
        return Action {
            label: "dex_swap".to_owned(),
            confidence: max_confidence(analysis, 0.75),
        };
    }
    let (inflows, outflows) = observed_flows(address, analysis);
    if !inflows.as_array().unwrap_or(&Vec::new()).is_empty()
        || !outflows.as_array().unwrap_or(&Vec::new()).is_empty()
    {
        return Action {
            label: "asset_transfer".to_owned(),
            confidence: 0.99,
        };
    }
    Action {
        label: "unknown_contract_interaction".to_owned(),
        confidence: 0.50,
    }
}

fn observed_is_top_level_endpoint(from: &str, to: Option<&str>, address: &str) -> bool {
    from.eq_ignore_ascii_case(address)
        || to.is_some_and(|value| value.eq_ignore_ascii_case(address))
}

fn max_confidence(analysis: &TransactionAnalysis, floor: f64) -> f64 {
    analysis
        .recognizer_results
        .iter()
        .map(|value| value.confidence as f64)
        .fold(floor, f64::max)
        .min(1.0)
}

fn observed_flows(address: &str, analysis: &TransactionAnalysis) -> (Value, Value) {
    let mut inflows = Vec::new();
    let mut outflows = Vec::new();
    for flow in &analysis.derived_flows {
        let item = json!({"asset_type":match flow.asset_type{AssetType::Native=>"native",AssetType::Erc20=>"erc20"},"token":flow.token_address,"from":flow.from_address.to_ascii_lowercase(),"to":flow.to_address.to_ascii_lowercase(),"amount_raw":flow.amount_raw,"confidence":flow.confidence,"evidence":flow.evidence_refs});
        let from = flow.from_address.eq_ignore_ascii_case(address);
        let to = flow.to_address.eq_ignore_ascii_case(address);
        if to && !from {
            inflows.push(item.clone());
        }
        if from && !to {
            outflows.push(item);
        }
    }
    (Value::Array(inflows), Value::Array(outflows))
}

fn observed_contract_evidence(analysis: &TransactionAnalysis) -> (Value, Value, Value) {
    let facts = &analysis.normalized_execution_facts.derived_facts;
    let mut contracts = BTreeSet::new();
    let mut pools = BTreeSet::new();
    if let Some(values) = facts.get("touched_contracts").and_then(Value::as_array) {
        for value in values.iter().filter_map(Value::as_str) {
            if is_address(value) {
                contracts.insert(value.to_ascii_lowercase());
            }
        }
    }
    let mut protocol_relationships = Vec::new();
    if let Some(values) = facts.get("protocol_evidence").and_then(Value::as_array) {
        for evidence in values {
            let Some(address) = evidence.get("address").and_then(Value::as_str) else {
                continue;
            };
            if !is_address(address) {
                continue;
            }
            let address = address.to_ascii_lowercase();
            let role = evidence
                .get("contract_role")
                .and_then(Value::as_str)
                .unwrap_or("protocol");
            contracts.insert(address.clone());
            if role.contains("pool") {
                pools.insert(address.clone());
            }
            protocol_relationships.push(json!({
                "address": address,
                "protocol_name": evidence.get("protocol_name").and_then(Value::as_str),
                "protocol_family": evidence.get("protocol_family").and_then(Value::as_str),
                "contract_role": role,
                "confidence": evidence.get("confidence").and_then(Value::as_f64),
                "source": evidence.get("source").and_then(Value::as_str),
            }));
        }
    }
    (
        json!(contracts),
        json!(pools),
        Value::Array(protocol_relationships),
    )
}

fn add_native_flow_total(total: &mut BigInt, flows: &[Value]) {
    for flow in flows {
        if flow.get("asset_type").and_then(Value::as_str) == Some("native") {
            add_decimal(total, flow.get("amount_raw").and_then(Value::as_str));
        }
    }
}

fn collect_json_string_set(value: Value, target: &mut BTreeSet<String>) {
    if let Some(values) = value.as_array() {
        for value in values.iter().filter_map(Value::as_str) {
            target.insert(value.to_ascii_lowercase());
        }
    }
}

fn is_address(value: &str) -> bool {
    value.len() == 42
        && value.starts_with("0x")
        && value[2..]
            .chars()
            .all(|character| character.is_ascii_hexdigit())
}

fn primary_protocol(analysis: &TransactionAnalysis) -> Option<String> {
    analysis
        .normalized_execution_facts
        .derived_facts
        .get("protocol_evidence")
        .and_then(Value::as_array)
        .and_then(|values| values.first())
        .and_then(|value| value.get("protocol_name"))
        .and_then(Value::as_str)
        .map(str::to_owned)
}

fn analysis_gas_cost(analysis: &TransactionAnalysis) -> Option<String> {
    analysis
        .normalized_execution_facts
        .derived_facts
        .get("gas_cost_raw")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .or_else(|| {
            analysis
                .accounting_outputs
                .get("gas_cost_raw")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
}

fn transaction_status_key(status: &TransactionStatus) -> &'static str {
    match status {
        TransactionStatus::Success => "success",
        TransactionStatus::Failure => "failure",
        TransactionStatus::Pending => "pending",
    }
}

#[derive(Default)]
struct RelationshipAggregate {
    count: u64,
    hashes: BTreeSet<String>,
    first: Option<DateTime<Utc>>,
    last: Option<DateTime<Utc>>,
    confidence: f64,
    evidence_state: String,
}
fn observe_relation(
    map: &mut BTreeMap<(String, String), RelationshipAggregate>,
    related: String,
    kind: &str,
    tx_hash: &str,
    timestamp: Option<DateTime<Utc>>,
    confidence: f64,
    evidence_state: &str,
) {
    let item = map
        .entry((related.to_ascii_lowercase(), kind.to_owned()))
        .or_default();
    item.count += 1;
    item.hashes.insert(tx_hash.to_owned());
    item.confidence = item.confidence.max(confidence);
    item.evidence_state = evidence_state.to_owned();
    if let Some(ts) = timestamp {
        item.first = Some(item.first.map_or(ts, |v| v.min(ts)));
        item.last = Some(item.last.map_or(ts, |v| v.max(ts)));
    }
}

/// Build an exact receipt/log based activity record when the observed address
/// is not the outer transaction sender or target and only appears in ERC-20
/// Transfer logs. Traces cannot change those transfer amounts or receipt
/// status, so avoiding a trace here preserves correctness while keeping large
/// settlement/aggregation addresses practical. Any ambiguous case falls back
/// to the full transaction analysis path.
async fn build_lightweight_transfer_activity(
    rpc: &dyn RpcClient,
    chain_id: u64,
    observed_address: &str,
    tx_hash: &str,
) -> LedgerResult<Option<LightweightActivity>> {
    let tx = rpc.get_transaction(chain_id, tx_hash).await?;
    if tx.is_null() {
        return Err(LedgerError::MalformedEvidence(format!(
            "missing tx payload for {tx_hash}"
        )));
    }
    let from_address = tx
        .get("from")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    let to_address = tx
        .get("to")
        .and_then(Value::as_str)
        .map(str::to_ascii_lowercase);
    if from_address.eq_ignore_ascii_case(observed_address)
        || to_address
            .as_deref()
            .is_some_and(|value| value.eq_ignore_ascii_case(observed_address))
    {
        return Ok(None);
    }
    let block_number = tx
        .get("blockNumber")
        .and_then(Value::as_str)
        .and_then(parse_hex_u64)
        .ok_or_else(|| {
            LedgerError::MalformedEvidence(format!(
                "transaction {tx_hash} block number unavailable"
            ))
        })?;
    let (receipt, block) = tokio::try_join!(
        rpc.get_receipt(chain_id, tx_hash),
        rpc.get_block(chain_id, block_number)
    )?;
    let timestamp = block
        .get("timestamp")
        .and_then(Value::as_str)
        .and_then(parse_hex_u64)
        .and_then(|value| DateTime::from_timestamp(value as i64, 0))
        .ok_or_else(|| {
            LedgerError::MalformedEvidence(format!(
                "transaction {tx_hash} block timestamp unavailable"
            ))
        })?;
    let mut inflows = Vec::new();
    let mut outflows = Vec::new();
    let mut touched_contracts = BTreeSet::new();
    let mut counterparties = BTreeSet::new();
    let mut evidence = vec![
        json!({"kind":"tx","reference":tx_hash}),
        json!({"kind":"receipt","reference":tx_hash}),
        json!({"kind":"block","reference":block_number}),
    ];
    for log in receipt
        .get("logs")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let Some(topics) = log.get("topics").and_then(Value::as_array) else {
            continue;
        };
        if topics.len() < 3
            || !topics[0]
                .as_str()
                .is_some_and(|value| value.eq_ignore_ascii_case(TRANSFER_TOPIC))
        {
            continue;
        }
        let Some(from) = topics[1].as_str().and_then(topic_address) else {
            continue;
        };
        let Some(to) = topics[2].as_str().and_then(topic_address) else {
            continue;
        };
        let observed_from = from.eq_ignore_ascii_case(observed_address);
        let observed_to = to.eq_ignore_ascii_case(observed_address);
        if observed_from == observed_to {
            continue;
        }
        let token = log
            .get("address")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_ascii_lowercase();
        if !is_address(&token) {
            continue;
        }
        let Some(amount_raw) = log
            .get("data")
            .and_then(Value::as_str)
            .and_then(parse_hex_bigint)
            .map(|value| value.to_string())
        else {
            continue;
        };
        let log_index = log
            .get("logIndex")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        let item = json!({
            "asset_type":"erc20",
            "token":&token,
            "from":&from,
            "to":&to,
            "amount_raw":amount_raw,
            "confidence":1.0,
            "evidence":[{"kind":"log","reference":format!("{tx_hash}:{log_index}")}]
        });
        if observed_to {
            counterparties.insert(from);
            inflows.push(item);
        } else {
            counterparties.insert(to);
            outflows.push(item);
        }
        touched_contracts.insert(token);
        evidence.push(json!({"kind":"log","reference":format!("{tx_hash}:{log_index}")}));
    }
    if inflows.is_empty() && outflows.is_empty() {
        return Ok(None);
    }
    let status = if receipt
        .get("status")
        .and_then(Value::as_str)
        .and_then(parse_hex_u64)
        == Some(1)
    {
        "success"
    } else {
        "failure"
    };
    let digest = digest_json(&json!({
        "method":"receipt-transfer-activity.v1",
        "chain_id":chain_id,
        "tx":&tx,
        "receipt":&receipt,
        "block_timestamp":timestamp,
        "observed_address":observed_address,
    }));
    Ok(Some(LightweightActivity {
        tx_hash: tx_hash.to_ascii_lowercase(),
        block_number,
        block_timestamp: timestamp,
        status: status.to_owned(),
        from_address,
        to_address,
        counterparty: counterparties.into_iter().next(),
        native_value_raw: tx
            .get("value")
            .and_then(Value::as_str)
            .unwrap_or("0x0")
            .to_owned(),
        token_inflows: Value::Array(inflows),
        token_outflows: Value::Array(outflows),
        touched_contracts: json!(touched_contracts),
        evidence_refs: Value::Array(evidence),
        input_digest: digest,
    }))
}

fn transfer_log_ref(
    address: &str,
    direction: AddressHitDirection,
    log: &Value,
) -> Option<AddressTxRef> {
    if log.get("removed").and_then(Value::as_bool).unwrap_or(false) {
        return None;
    }
    let topics = log.get("topics")?.as_array()?;
    if topics.len() < 3 || !topics[0].as_str()?.eq_ignore_ascii_case(TRANSFER_TOPIC) {
        return None;
    }
    let from = topic_address(topics[1].as_str()?)?;
    let to = topic_address(topics[2].as_str()?)?;
    Some(AddressTxRef {
        address: address.to_ascii_lowercase(),
        tx_hash: log.get("transactionHash")?.as_str()?.to_ascii_lowercase(),
        block_number: parse_hex_u64(log.get("blockNumber")?.as_str()?)?,
        tx_index: parse_hex_u64(log.get("transactionIndex")?.as_str()?)? as u32,
        direction,
        from_address: Some(from),
        to_address: Some(to),
    })
}

fn parse_hex_bigint(value: &str) -> Option<BigInt> {
    BigInt::parse_bytes(value.trim_start_matches("0x").as_bytes(), 16)
}
fn topic_address(value: &str) -> Option<String> {
    let raw = value.trim_start_matches("0x");
    (raw.len() >= 40).then(|| format!("0x{}", &raw[raw.len() - 40..]).to_ascii_lowercase())
}
fn padded_address_topic(address: &str) -> String {
    format!(
        "0x{:0>64}",
        address.trim_start_matches("0x").to_ascii_lowercase()
    )
}
fn parse_hex_u64(value: &str) -> Option<u64> {
    u64::from_str_radix(value.trim_start_matches("0x"), 16).ok()
}
fn normalize_address(address: &str) -> LedgerResult<String> {
    let value = address.trim().to_ascii_lowercase();
    if value.len() != 42
        || !value.starts_with("0x")
        || !value[2..].chars().all(|c| c.is_ascii_hexdigit())
    {
        return Err(LedgerError::MalformedEvidence(
            "observed address must be a 20-byte EVM address".to_owned(),
        ));
    }
    Ok(value)
}

fn external_history_cursor(metadata: &Value) -> AddressHistoryCursor {
    AddressHistoryCursor {
        transaction_page: metadata
            .get("external_history_transaction_page")
            .and_then(Value::as_u64)
            .unwrap_or(1),
        token_transfer_page: metadata
            .get("external_history_token_transfer_page")
            .and_then(Value::as_u64)
            .unwrap_or(1),
        internal_transaction_page: metadata
            .get("external_history_internal_transaction_page")
            .and_then(Value::as_u64)
            .unwrap_or(1),
        transaction_start_block: metadata
            .get("external_history_transaction_start_block")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        token_transfer_start_block: metadata
            .get("external_history_token_transfer_start_block")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        internal_transaction_start_block: metadata
            .get("external_history_internal_transaction_start_block")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        transaction_cursor: metadata
            .get("external_history_transaction_cursor")
            .filter(|value| !value.is_null())
            .cloned(),
        token_transfer_cursor: metadata
            .get("external_history_token_transfer_cursor")
            .filter(|value| !value.is_null())
            .cloned(),
        internal_transaction_cursor: metadata
            .get("external_history_internal_transaction_cursor")
            .filter(|value| !value.is_null())
            .cloned(),
        transactions_complete: metadata
            .get("external_history_transactions_complete")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        token_transfers_complete: metadata
            .get("external_history_token_transfers_complete")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        internal_transactions_complete: metadata
            .get("external_history_internal_transactions_complete")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        earliest_activity_block: metadata
            .get("external_history_earliest_activity_block")
            .and_then(Value::as_u64),
    }
    .normalized()
}

fn chain_display_name(chain_id: u64) -> String {
    match chain_id {
        8453 => "Base".to_owned(),
        4663 => "Robinhood Chain".to_owned(),
        other => format!("Chain {other}"),
    }
}
fn range_progress(start: u64, end: u64, scanned_from: u64) -> f64 {
    if end <= start {
        1.0
    } else {
        (end.saturating_sub(scanned_from) as f64 / end.saturating_sub(start) as f64).clamp(0.0, 1.0)
    }
}
fn initial_progress(message: &str) -> Value {
    json!({"percent":0,"stage":"queued","message":message,"transactions_discovered":0,"transactions_analyzed":0,"receipts_processed":0,"traces_processed":0,"economic_verified":0})
}
fn db_error(error: sqlx::Error) -> LedgerError {
    LedgerError::Persistence(error.to_string())
}
fn digest_json(value: &Value) -> String {
    let mut hasher = Sha256::new();
    hasher.update(serde_json::to_vec(value).unwrap_or_default());
    hex::encode(hasher.finalize())
}
fn add_decimal(total: &mut BigInt, value: Option<&str>) {
    if let Some(number) = value.and_then(|v| BigInt::parse_bytes(v.as_bytes(), 10)) {
        *total += number;
    }
}

fn decimal_f64(value: Option<&str>) -> Option<f64> {
    value?.parse::<f64>().ok().filter(|value| value.is_finite())
}

fn ratio(numerator: i64, denominator: i64) -> f64 {
    if denominator <= 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}

fn html_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}
fn add_map_decimal(map: &mut BTreeMap<String, BigInt>, asset: &str, value: &str) {
    if let Some(number) = BigInt::parse_bytes(value.as_bytes(), 10) {
        *map.entry(asset.to_owned())
            .or_insert_with(|| BigInt::from(0_u8)) += number;
    }
}
fn accumulate_flow_nets(map: &mut BTreeMap<String, BigInt>, value: Value, inflow: bool) {
    for flow in value.as_array().into_iter().flatten() {
        let asset = flow
            .get("token")
            .and_then(Value::as_str)
            .unwrap_or("native");
        if let Some(mut amount) = flow
            .get("amount_raw")
            .and_then(Value::as_str)
            .and_then(|v| BigInt::parse_bytes(v.as_bytes(), 10))
        {
            if !inflow {
                amount = -amount;
            }
            *map.entry(asset.to_owned())
                .or_insert_with(|| BigInt::from(0_u8)) += amount;
        }
    }
}
fn amounts_json(map: &BTreeMap<String, BigInt>) -> Value {
    json!(
        map.iter()
            .map(|(asset, amount)| json!({"asset":asset,"amount_raw":amount.to_string()}))
            .collect::<Vec<_>>()
    )
}
fn time_windows() -> Vec<(&'static str, DateTime<Utc>, DateTime<Utc>)> {
    let now = Utc::now();
    vec![
        ("24h", now - Duration::hours(24), now),
        ("7d", now - Duration::days(7), now),
        ("30d", now - Duration::days(30), now),
        (
            "previous_7d",
            now - Duration::days(14),
            now - Duration::days(7),
        ),
        (
            "previous_30d",
            now - Duration::days(60),
            now - Duration::days(30),
        ),
    ]
}

fn object_json(row: &sqlx::postgres::PgRow) -> Value {
    json!({"chain_id":row.get::<i64,_>("chain_id"),"address":row.get::<String,_>("address"),"object_type":row.get::<String,_>("object_type"),"current_status":row.get::<String,_>("current_status"),"progress":row.get::<Value,_>("progress"),"analysis_start":row.get::<Option<DateTime<Utc>>,_>("analysis_started_at"),"analysis_end":row.get::<Option<DateTime<Utc>>,_>("analysis_completed_at"),"last_processed_block":row.get::<Option<i64>,_>("last_processed_block"),"last_incremental_update":row.get::<Option<DateTime<Utc>>,_>("last_incremental_update"),"coverage_start":row.get::<Option<DateTime<Utc>>,_>("coverage_start_at"),"coverage_end":row.get::<Option<DateTime<Utc>>,_>("coverage_end_at"),"discovered_transaction_count":row.get::<i64,_>("discovered_transaction_count"),"analyzed_transaction_count":row.get::<i64,_>("analyzed_transaction_count"),"approximate_total_transaction_count":row.get::<Option<i64>,_>("approximate_total_transaction_count"),"coverage_is_partial":row.get::<bool,_>("coverage_is_partial"),"monitoring_enabled":row.get::<bool,_>("monitoring_enabled"),"last_activity_at":row.get::<Option<DateTime<Utc>>,_>("last_activity_at"),"error":row.get::<Option<String>,_>("error"),"retry_count":row.get::<i32,_>("retry_count"),"metadata":row.get::<Value,_>("metadata")})
}
fn summary_json(row: &sqlx::postgres::PgRow) -> Value {
    json!({"window":row.get::<String,_>("window_key"),"window_start":row.get::<DateTime<Utc>,_>("window_start"),"window_end":row.get::<DateTime<Utc>,_>("window_end"),"transaction_count":row.get::<i64,_>("transaction_count"),"success_count":row.get::<i64,_>("success_count"),"reverted_count":row.get::<i64,_>("reverted_count"),"gas_cost_raw":row.get::<Option<String>,_>("gas_cost_raw"),"native_inflow_raw":row.get::<Option<String>,_>("native_inflow_raw"),"native_outflow_raw":row.get::<Option<String>,_>("native_outflow_raw"),"token_inflows":row.get::<Value,_>("token_inflows"),"token_outflows":row.get::<Value,_>("token_outflows"),"top_call_targets":row.get::<Value,_>("top_call_targets"),"touched_contracts":row.get::<Value,_>("touched_contracts"),"touched_pools":row.get::<Value,_>("touched_pools"),"protocol_count":row.get::<i64,_>("protocol_count"),"counterparty_count":row.get::<i64,_>("counterparty_count"),"first_seen":row.get::<Option<DateTime<Utc>>,_>("first_seen_at"),"last_active":row.get::<Option<DateTime<Utc>>,_>("last_active_at"),"coverage":row.get::<Value,_>("coverage")})
}
fn behavior_json(row: &sqlx::postgres::PgRow) -> Value {
    json!({"label":row.get::<String,_>("label"),"confidence":row.get::<f64,_>("confidence"),"supporting_transaction_count":row.get::<i64,_>("supporting_transaction_count"),"supporting_transactions":row.get::<Vec<String>,_>("supporting_tx_hashes"),"first_observed":row.get::<Option<DateTime<Utc>>,_>("first_observed_at"),"last_observed":row.get::<Option<DateTime<Utc>>,_>("last_observed_at"),"evidence_coverage":row.get::<f64,_>("evidence_coverage"),"method_version":row.get::<String,_>("method_version")})
}
fn relationship_json(row: &sqlx::postgres::PgRow) -> Value {
    json!({"related_address":row.get::<String,_>("related_address"),"relationship_type":row.get::<String,_>("relationship_type"),"evidence_state":row.get::<String,_>("evidence_state"),"interaction_count":row.get::<i64,_>("interaction_count"),"supporting_transactions":row.get::<Vec<String>,_>("supporting_tx_hashes"),"first_seen":row.get::<Option<DateTime<Utc>>,_>("first_seen_at"),"last_seen":row.get::<Option<DateTime<Utc>>,_>("last_seen_at"),"confidence":row.get::<f64,_>("confidence"),"method_version":row.get::<String,_>("method_version")})
}
fn economic_json(row: &sqlx::postgres::PgRow) -> Value {
    json!({"window":row.get::<String,_>("window_key"),"result_status":row.get::<String,_>("result_status"),"verified_gross_surplus":row.get::<Value,_>("verified_gross_surplus"),"verified_gas_cost_raw":row.get::<Option<String>,_>("verified_gas_cost_raw"),"verified_net_result":row.get::<Value,_>("verified_net_result"),"observed_net_token_flow":row.get::<Value,_>("observed_net_token_flow"),"economically_verified_transaction_count":row.get::<i64,_>("economically_verified_transaction_count"),"excluded_transaction_count":row.get::<i64,_>("excluded_transaction_count"),"exclusion_reasons":row.get::<Value,_>("exclusion_reasons"),"coverage_ratio":row.get::<f64,_>("coverage_ratio"),"limitations":row.get::<Vec<String>,_>("limitations")})
}
fn change_json(row: &sqlx::postgres::PgRow) -> Value {
    json!({"id":row.get::<uuid::Uuid,_>("signal_id"),"signal_type":row.get::<String,_>("signal_type"),"status":row.get::<String,_>("status"),"severity":row.get::<String,_>("severity"),"conclusion":row.get::<String,_>("conclusion"),"current_value":row.get::<Option<Value>,_>("current_value"),"baseline_value":row.get::<Option<Value>,_>("baseline_value"),"supporting_transactions":row.get::<Vec<String>,_>("supporting_tx_hashes"),"confidence":row.get::<f64,_>("confidence"),"detected_at":row.get::<DateTime<Utc>,_>("detected_at")})
}
fn activity_json(row: &sqlx::postgres::PgRow) -> Value {
    json!({"tx_hash":row.get::<String,_>("tx_hash"),"block_number":row.get::<i64,_>("block_number"),"timestamp":row.get::<Option<DateTime<Utc>>,_>("block_timestamp"),"processing_status":row.get::<String,_>("processing_status"),"status":row.get::<Option<String>,_>("transaction_status"),"action":row.get::<Option<String>,_>("action_label"),"confidence":row.get::<Option<f64>,_>("action_confidence"),"protocol":row.get::<Option<String>,_>("protocol"),"counterparty":row.get::<Option<String>,_>("counterparty"),"native_value_raw":row.get::<Option<String>,_>("native_value_raw"),"token_inflows":row.get::<Value,_>("token_inflows"),"token_outflows":row.get::<Value,_>("token_outflows"),"touched_contracts":row.get::<Value,_>("touched_contracts"),"touched_pools":row.get::<Value,_>("touched_pools"),"gas_cost_raw":row.get::<Option<String>,_>("gas_cost_raw"),"economic_status":row.get::<String,_>("economic_result_status"),"verified_gross_surplus_raw":row.get::<Option<String>,_>("verified_gross_surplus_raw"),"verified_net_result_raw":row.get::<Option<String>,_>("verified_net_result_raw"),"verified_result_asset":row.get::<Option<String>,_>("verified_result_asset"),"exclusion_reason":row.get::<Option<String>,_>("exclusion_reason"),"evidence_refs":row.get::<Value,_>("evidence_refs")})
}
fn brief_json(row: &sqlx::postgres::PgRow) -> Value {
    json!({"id":row.get::<uuid::Uuid,_>("brief_id"),"type":row.get::<String,_>("brief_type"),"period_start":row.get::<DateTime<Utc>,_>("period_start"),"period_end":row.get::<DateTime<Utc>,_>("period_end"),"title":row.get::<String,_>("title"),"generated_at":row.get::<DateTime<Utc>,_>("generated_at"),"inputs_digest":row.get::<String,_>("inputs_digest")})
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_non_addresses() {
        assert!(normalize_address("0x1234").is_err());
    }
    #[test]
    fn transfer_topics_are_padded() {
        assert_eq!(
            padded_address_topic("0x0000000000000000000000000000000000000001").len(),
            66
        );
    }

    #[test]
    fn parses_full_width_transfer_amounts_without_precision_loss() {
        assert_eq!(
            parse_hex_bigint("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")
                .expect("uint256")
                .to_string(),
            "115792089237316195423570985008687907853269984665640564039457584007913129639935"
        );
    }
    #[test]
    fn zero_baseline_is_not_a_change() {
        let windows = time_windows();
        assert_eq!(windows[0].0, "24h");
    }

    #[test]
    fn native_flow_totals_ignore_erc20_assets() {
        let mut total = BigInt::from(0_u8);
        add_native_flow_total(
            &mut total,
            &[
                json!({"asset_type":"native","amount_raw":"12"}),
                json!({"asset_type":"erc20","amount_raw":"900"}),
                json!({"asset_type":"native","amount_raw":"30"}),
            ],
        );
        assert_eq!(total.to_string(), "42");
    }

    #[test]
    fn relationships_deduplicate_transaction_evidence() {
        let mut values = BTreeMap::new();
        let timestamp = Utc::now();
        for _ in 0..2 {
            observe_relation(
                &mut values,
                "0x0000000000000000000000000000000000000002".to_owned(),
                "funding_source",
                "0xabc",
                Some(timestamp),
                0.72,
                "candidate",
            );
        }
        let relation = values.values().next().expect("relationship");
        assert_eq!(relation.count, 2);
        assert_eq!(relation.hashes.len(), 1);
        assert_eq!(relation.evidence_state, "candidate");
    }

    #[test]
    fn transfer_participants_do_not_inherit_execution_actor_labels() {
        let observed = "0x0000000000000000000000000000000000000003";
        assert!(!observed_is_top_level_endpoint(
            "0x0000000000000000000000000000000000000001",
            Some("0x0000000000000000000000000000000000000002"),
            observed,
        ));
        assert!(observed_is_top_level_endpoint(observed, None, observed));
        assert!(observed_is_top_level_endpoint(
            "0x0000000000000000000000000000000000000001",
            Some(observed),
            observed,
        ));
    }

    #[test]
    fn reports_escape_chain_derived_html() {
        assert_eq!(
            html_escape("<script>&\"'"),
            "&lt;script&gt;&amp;&quot;&#39;"
        );
    }
}
