use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
};

use chrono::{DateTime, Duration, Utc};
use num_bigint::BigInt;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::{
    core::errors::{LedgerError, LedgerResult},
    domain::{
        entities::TransactionAnalysis,
        enums::{AssetType, TransactionStatus},
        interfaces::ClusterTxSource,
        value_objects::ChainDataSourceStatus,
    },
    ingestion::rpc::RpcClient,
    pilot::profit::verify_deterministic_profit_for_scope,
    services::analysis::AnalysisService,
};

pub const SEARCHER_CLUSTER_CONTRACT_VERSION: &str = "searcher-cluster-intelligence.v1";
const RELATIONSHIP_METHOD_VERSION: &str = "cluster-relationship-expansion.v1";
const ROLE_METHOD_VERSION: &str = "seed-role-rules.v1";
const ECONOMIC_METHOD_VERSION: &str = "candidate-cluster-scope-accounting.v1";
const MAX_EVIDENCE_HASHES: usize = 100;

#[derive(Clone, Debug)]
pub struct SearcherClusterConfig {
    pub chain_id: u64,
    pub worker_id: String,
    pub execution_batch_size: usize,
    pub basic_reconstruction_batch_size: usize,
    pub initial_deep_execution_sample: usize,
    pub maximum_relationships: usize,
    pub maximum_cluster_members: usize,
    pub history_days: i64,
    pub stale_claim_seconds: i64,
    pub incremental_lag_blocks: u64,
}

impl Default for SearcherClusterConfig {
    fn default() -> Self {
        Self {
            chain_id: 8453,
            worker_id: "searcher_cluster_intelligence".to_owned(),
            execution_batch_size: 8,
            basic_reconstruction_batch_size: 128,
            initial_deep_execution_sample: 64,
            maximum_relationships: 20,
            maximum_cluster_members: 32,
            history_days: 30,
            stale_claim_seconds: 180,
            incremental_lag_blocks: 2,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct SearcherClusterIteration {
    pub recovered_claims: u64,
    pub stale_clusters: u64,
    pub processed: bool,
    pub chain_id: u64,
    pub cluster_id: Option<Uuid>,
    pub seed_address: Option<String>,
    pub stage: Option<String>,
}

#[derive(Clone, Debug)]
struct ClaimedCluster {
    cluster_id: Uuid,
    seed_address: String,
    status: String,
    retry_count: i32,
}

#[derive(Clone, Debug, Default)]
struct RelationshipEvidence {
    source_address: String,
    related_address: String,
    relationship_type: String,
    evidence_state: String,
    interaction_count: u64,
    values: BTreeMap<String, BigInt>,
    first_seen: Option<DateTime<Utc>>,
    last_seen: Option<DateTime<Utc>>,
    tx_hashes: BTreeSet<String>,
    confidence: f64,
    reason_facts: BTreeSet<String>,
    depth: i16,
}

#[derive(Clone, Debug)]
struct MemberCandidate {
    address: String,
    role: String,
    confidence: f64,
    interaction_count: u64,
    evidence_state: String,
    hashes: BTreeSet<String>,
    reasons: BTreeSet<String>,
    first_seen: Option<DateTime<Utc>>,
    last_seen: Option<DateTime<Utc>>,
    depth: i16,
}

#[derive(Clone, Debug)]
struct ExecutionInput {
    tx_hash: String,
    block_number: u64,
    block_timestamp: Option<DateTime<Utc>>,
    from_address: Option<String>,
    to_address: Option<String>,
    seed_token_inflows: Value,
    seed_token_outflows: Value,
    seed_protocol_relationships: Value,
    seed_touched_pools: Value,
}

#[derive(Default)]
struct WindowAccumulator {
    execution_count: u64,
    success_count: u64,
    failure_count: u64,
    gas_cost: BigInt,
    verified_gross: BTreeMap<String, BigInt>,
    verified_net: BTreeMap<String, BigInt>,
    verified_gas: BigInt,
    verified_count: u64,
    settlement_inflows: BTreeMap<String, BigInt>,
    capital_sent: BTreeMap<String, BigInt>,
    capital_returned: BTreeMap<String, BigInt>,
    failed_count: u64,
    failed_gas: BigInt,
    strategies: HashMap<String, u64>,
    workers: HashMap<String, u64>,
    executors: HashMap<String, u64>,
    protocols: HashMap<String, u64>,
    pools: BTreeSet<String>,
    included: u64,
    excluded: u64,
    exclusion_reasons: HashMap<String, u64>,
    last_active: Option<DateTime<Utc>>,
    evidence_hashes: Vec<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ClusterMemberOverride {
    pub action: String,
    pub role: Option<String>,
    pub note: Option<String>,
    pub actor_user_id: Option<Uuid>,
}

#[derive(Clone)]
pub struct SearcherClusterIntelligenceService {
    pool: PgPool,
    rpc: Arc<dyn RpcClient>,
    analysis: Arc<AnalysisService>,
    tx_source: Arc<dyn ClusterTxSource>,
    config: SearcherClusterConfig,
}

impl SearcherClusterIntelligenceService {
    pub fn new(
        pool: PgPool,
        rpc: Arc<dyn RpcClient>,
        analysis: Arc<AnalysisService>,
        tx_source: Arc<dyn ClusterTxSource>,
        config: SearcherClusterConfig,
    ) -> Self {
        Self {
            pool,
            rpc,
            analysis,
            tx_source,
            config,
        }
    }

    pub async fn data_source_status(&self, chain_id: u64) -> LedgerResult<ChainDataSourceStatus> {
        if chain_id != self.config.chain_id {
            return Err(source_unavailable(chain_id));
        }
        let (rpc_chain, status) = tokio::try_join!(
            self.rpc.get_chain_id(),
            self.tx_source.get_data_source_status(chain_id)
        )?;
        if rpc_chain != chain_id || status.chain_id != chain_id || !status.available {
            return Err(source_unavailable(chain_id));
        }
        Ok(status)
    }

    pub async fn enqueue(&self, chain_id: u64, seed_address: &str) -> LedgerResult<Uuid> {
        self.data_source_status(chain_id).await?;
        let seed_address = normalize_address(seed_address)?;
        let mut tx = self.pool.begin().await.map_err(db_error)?;
        let observed = sqlx::query_scalar::<_, bool>(
            "select exists(select 1 from observed_address_objects where chain_id=$1 and address=$2)",
        )
        .bind(chain_id as i64)
        .bind(&seed_address)
        .fetch_one(&mut *tx)
        .await
        .map_err(db_error)?;
        if !observed {
            return Err(LedgerError::NotFound(
                "observation seed must be created before cluster analysis".to_owned(),
            ));
        }
        let cluster_id = sqlx::query_scalar::<_, Uuid>(
            r#"
            insert into candidate_execution_clusters (
              chain_id,seed_address,current_status,progress,available_at,updated_at
            ) values ($1,$2,'queued',$3,now(),now())
            on conflict (chain_id,seed_address) do update set
              current_status=case
                when candidate_execution_clusters.current_status='ready' then 'stale'
                when candidate_execution_clusters.current_status='failed' then 'queued'
                else candidate_execution_clusters.current_status
              end,
              error=null,available_at=now(),updated_at=now()
            returning cluster_id
            "#,
        )
        .bind(chain_id as i64)
        .bind(&seed_address)
        .bind(progress(0, "queued", "Queued for seed classification"))
        .fetch_one(&mut *tx)
        .await
        .map_err(db_error)?;
        sqlx::query(
            r#"update observed_address_objects
               set seed_address=$2,candidate_cluster_id=$3,
                   analysis_scope='seed_and_candidate_execution_cluster',updated_at=now()
               where chain_id=$1 and address=$2"#,
        )
        .bind(chain_id as i64)
        .bind(&seed_address)
        .bind(cluster_id)
        .execute(&mut *tx)
        .await
        .map_err(db_error)?;
        tx.commit().await.map_err(db_error)?;
        Ok(cluster_id)
    }

    pub async fn retry(&self, chain_id: u64, seed_address: &str) -> LedgerResult<bool> {
        self.data_source_status(chain_id).await?;
        let seed_address = normalize_address(seed_address)?;
        let result = sqlx::query(
            r#"update candidate_execution_clusters
               set current_status='queued',progress=$3,error=null,retry_count=0,
                   claimed_at=null,available_at=now(),updated_at=now()
               where chain_id=$1 and seed_address=$2"#,
        )
        .bind(chain_id as i64)
        .bind(seed_address)
        .bind(progress(0, "queued", "Retry queued"))
        .execute(&self.pool)
        .await
        .map_err(db_error)?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn process_once(&self) -> LedgerResult<SearcherClusterIteration> {
        let source = self.data_source_status(self.config.chain_id).await?;
        let recovered = sqlx::query(
            r#"update candidate_execution_clusters
               set claimed_at=null,available_at=now(),updated_at=now(),
                   metadata=metadata||'{"recovered_stale_claim":true}'::jsonb
               where chain_id=$1 and claimed_at < now()-make_interval(secs=>$2)
                 and current_status not in ('ready','failed')"#,
        )
        .bind(self.config.chain_id as i64)
        .bind(self.config.stale_claim_seconds as f64)
        .execute(&self.pool)
        .await
        .map_err(db_error)?
        .rows_affected();
        sqlx::query(
            r#"update candidate_execution_clusters clusters
               set current_status='queued',progress=$2,error=null,claimed_at=null,
                   available_at=now(),updated_at=now()
               from observed_address_objects objects
               where clusters.chain_id=$1 and clusters.current_status='failed'
                 and clusters.error like '%seed deterministic activity is not ready%'
                 and objects.chain_id=clusters.chain_id
                 and objects.address=clusters.seed_address
                 and objects.analysis_completed_at is not null"#,
        )
        .bind(self.config.chain_id as i64)
        .bind(progress(
            0,
            "queued",
            "Observed activity is ready; cluster analysis resumed",
        ))
        .execute(&self.pool)
        .await
        .map_err(db_error)?;
        let stale = sqlx::query(
            r#"update candidate_execution_clusters clusters
               set current_status='stale',available_at=now(),updated_at=now(),
                   progress=$2
               from observed_address_objects objects
               where clusters.chain_id=$1 and clusters.current_status='ready'
                 and objects.chain_id=clusters.chain_id
                 and objects.address=clusters.seed_address
                 and coalesce(objects.last_processed_block,0)
                     > coalesce(clusters.last_processed_block,0)"#,
        )
        .bind(self.config.chain_id as i64)
        .bind(progress(
            0,
            "stale",
            "New observed activity is ready for cluster refresh",
        ))
        .execute(&self.pool)
        .await
        .map_err(db_error)?
        .rows_affected();
        let Some(cluster) = self.claim_next().await? else {
            return Ok(SearcherClusterIteration {
                recovered_claims: recovered,
                stale_clusters: stale,
                chain_id: self.config.chain_id,
                ..Default::default()
            });
        };
        let claimed_stage = cluster.status.clone();
        let result = self.process_claimed(&cluster, &source).await;
        if let Err(error) = &result {
            if is_seed_activity_pending(error) {
                self.defer_for_seed_activity(&cluster).await?;
                return Ok(SearcherClusterIteration {
                    recovered_claims: recovered,
                    stale_clusters: stale,
                    processed: true,
                    chain_id: self.config.chain_id,
                    cluster_id: Some(cluster.cluster_id),
                    seed_address: Some(cluster.seed_address),
                    stage: Some("waiting_for_seed_activity".to_owned()),
                });
            }
            self.fail_cluster(&cluster, &error.to_string()).await?;
        }
        result?;
        Ok(SearcherClusterIteration {
            recovered_claims: recovered,
            stale_clusters: stale,
            processed: true,
            chain_id: self.config.chain_id,
            cluster_id: Some(cluster.cluster_id),
            seed_address: Some(cluster.seed_address),
            stage: Some(claimed_stage),
        })
    }

    async fn claim_next(&self) -> LedgerResult<Option<ClaimedCluster>> {
        let row = sqlx::query(
            r#"
            with candidate as (
              select cluster_id from candidate_execution_clusters
              where chain_id=$1 and current_status not in ('ready','failed')
                and claimed_at is null and available_at<=now()
              order by case current_status
                when 'execution_reconstruction' then 1
                when 'economic_verification' then 2
                else 0 end,
                updated_at,created_at
              for update skip locked limit 1
            )
            update candidate_execution_clusters clusters
            set claimed_at=now(),analysis_started_at=coalesce(analysis_started_at,now()),updated_at=now()
            from candidate where clusters.cluster_id=candidate.cluster_id
            returning clusters.cluster_id,clusters.seed_address,
                      clusters.current_status,clusters.retry_count
            "#,
        )
        .bind(self.config.chain_id as i64)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_error)?;
        Ok(row.map(|row| ClaimedCluster {
            cluster_id: row.get("cluster_id"),
            seed_address: row.get("seed_address"),
            status: row.get("current_status"),
            retry_count: row.get("retry_count"),
        }))
    }

    async fn process_claimed(
        &self,
        cluster: &ClaimedCluster,
        source: &ChainDataSourceStatus,
    ) -> LedgerResult<()> {
        match cluster.status.as_str() {
            "queued" | "stale" => {
                self.transition(
                    cluster,
                    "seed_classification",
                    5,
                    "Classifying the observation seed from deterministic activity",
                )
                .await
            }
            "seed_classification" => {
                self.classify_seed(cluster).await?;
                self.transition(
                    cluster,
                    "relationship_discovery",
                    18,
                    "Expanding evidence-backed execution relationships",
                )
                .await
            }
            "relationship_discovery" => {
                self.discover_relationships(cluster).await?;
                self.transition(
                    cluster,
                    "cluster_building",
                    34,
                    "Scoring candidate workers, executors, and settlement recipients",
                )
                .await
            }
            "cluster_building" => {
                self.finalize_cluster_members(cluster).await?;
                self.transition(
                    cluster,
                    "execution_reconstruction",
                    45,
                    "Reconstructing cluster execution activity",
                )
                .await
            }
            "execution_reconstruction" => {
                if self.reconstruct_execution_batch(cluster).await? {
                    self.transition(
                        cluster,
                        "economic_verification",
                        70,
                        "Separating funding, settlement, gas, and verified execution profit",
                    )
                    .await
                } else {
                    self.release_same_stage(cluster, 55, "Reconstructing cluster executions")
                        .await
                }
            }
            "economic_verification" => {
                if self.verify_economic_batch(cluster).await? {
                    self.transition(
                        cluster,
                        "baseline_building",
                        88,
                        "Building 24h, 7d, and 30d intelligence windows",
                    )
                    .await
                } else {
                    self.release_same_stage(cluster, 76, "Verifying candidate execution economics")
                        .await
                }
            }
            "baseline_building" => {
                self.rebuild_windows_and_changes(cluster).await?;
                sqlx::query(
                    r#"update candidate_execution_clusters
                       set current_status='ready',progress=$2,claimed_at=null,error=null,
                           analysis_completed_at=now(),last_incremental_update=now(),
                           last_processed_block=$3,retry_count=0,updated_at=now()
                       where cluster_id=$1"#,
                )
                .bind(cluster.cluster_id)
                .bind(progress(
                    100,
                    "ready",
                    "Candidate searcher intelligence is ready",
                ))
                .bind(source.indexed_to_block.unwrap_or_default() as i64)
                .execute(&self.pool)
                .await
                .map_err(db_error)?;
                Ok(())
            }
            other => Err(LedgerError::AnalysisInvariant(format!(
                "unsupported cluster analysis state {other}"
            ))),
        }
    }

    async fn transition(
        &self,
        cluster: &ClaimedCluster,
        status: &str,
        percent: u8,
        message: &str,
    ) -> LedgerResult<()> {
        sqlx::query(
            r#"update candidate_execution_clusters
               set current_status=$2,progress=$3,claimed_at=null,available_at=now(),updated_at=now()
               where cluster_id=$1"#,
        )
        .bind(cluster.cluster_id)
        .bind(status)
        .bind(progress(percent, status, message))
        .execute(&self.pool)
        .await
        .map_err(db_error)?;
        Ok(())
    }

    async fn release_same_stage(
        &self,
        cluster: &ClaimedCluster,
        percent: u8,
        message: &str,
    ) -> LedgerResult<()> {
        sqlx::query(
            r#"update candidate_execution_clusters
               set progress=$2,claimed_at=null,available_at=now(),updated_at=now()
               where cluster_id=$1"#,
        )
        .bind(cluster.cluster_id)
        .bind(progress(percent, &cluster.status, message))
        .execute(&self.pool)
        .await
        .map_err(db_error)?;
        Ok(())
    }

    async fn fail_cluster(&self, cluster: &ClaimedCluster, error: &str) -> LedgerResult<()> {
        let retry_count = cluster.retry_count + 1;
        sqlx::query(
            r#"update candidate_execution_clusters
               set current_status='failed',progress=$2,error=$3,retry_count=$4,
                   claimed_at=null,available_at=now()+make_interval(secs=>$5),updated_at=now()
               where cluster_id=$1"#,
        )
        .bind(cluster.cluster_id)
        .bind(progress(
            0,
            "failed",
            "Cluster analysis failed and can be retried",
        ))
        .bind(error.chars().take(4000).collect::<String>())
        .bind(retry_count)
        .bind((30_i64 * 2_i64.pow(retry_count.min(6) as u32)).min(1800) as f64)
        .execute(&self.pool)
        .await
        .map_err(db_error)?;
        Ok(())
    }

    async fn defer_for_seed_activity(&self, cluster: &ClaimedCluster) -> LedgerResult<()> {
        sqlx::query(
            r#"update candidate_execution_clusters
               set current_status='queued',progress=$2,error=null,claimed_at=null,
                   available_at=now()+interval '30 seconds',updated_at=now()
               where cluster_id=$1"#,
        )
        .bind(cluster.cluster_id)
        .bind(progress(
            0,
            "queued",
            "Waiting for deterministic seed activity",
        ))
        .execute(&self.pool)
        .await
        .map_err(db_error)?;
        Ok(())
    }

    async fn classify_seed(&self, cluster: &ClaimedCluster) -> LedgerResult<()> {
        let rows = sqlx::query(
            r#"select tx_hash,block_timestamp,from_address,to_address,action_label,
                      token_inflows,token_outflows
               from object_activity_transactions
               where chain_id=$1 and address=$2 and processing_status='analyzed'"#,
        )
        .bind(self.config.chain_id as i64)
        .bind(&cluster.seed_address)
        .fetch_all(&self.pool)
        .await
        .map_err(db_error)?;
        if rows.is_empty() {
            let initial_activity_complete = sqlx::query_scalar::<_, bool>(
                "select analysis_completed_at is not null from observed_address_objects where chain_id=$1 and address=$2",
            )
            .bind(self.config.chain_id as i64)
            .bind(&cluster.seed_address)
            .fetch_one(&self.pool)
            .await
            .map_err(db_error)?;
            if !initial_activity_complete {
                return Err(LedgerError::TransientIngestion(
                    "seed deterministic activity is not ready; address backfill must complete first"
                        .to_owned(),
                ));
            }
        }
        let code = self
            .rpc
            .get_code(self.config.chain_id, &cluster.seed_address, None)
            .await?;
        let is_contract = code
            .trim_start_matches("0x")
            .chars()
            .any(|value| value != '0');
        let mut sent = 0_u64;
        let mut received_top_level = 0_u64;
        let mut execution_actions = 0_u64;
        let mut inflow_transfers = 0_u64;
        let mut outflow_transfers = 0_u64;
        let mut inflow_sources = BTreeSet::new();
        let mut outflow_recipients = BTreeSet::new();
        let mut hashes = BTreeSet::new();
        let mut transfer_edges = Vec::new();
        for row in &rows {
            let tx_hash: String = row.get("tx_hash");
            let from: Option<String> = row.get("from_address");
            let to: Option<String> = row.get("to_address");
            if from
                .as_deref()
                .is_some_and(|value| value.eq_ignore_ascii_case(&cluster.seed_address))
            {
                sent += 1;
                hashes.insert(tx_hash.clone());
            }
            if to
                .as_deref()
                .is_some_and(|value| value.eq_ignore_ascii_case(&cluster.seed_address))
            {
                received_top_level += 1;
            }
            if row
                .get::<Option<String>, _>("action_label")
                .as_deref()
                .is_some_and(is_execution_action)
            {
                execution_actions += 1;
            }
            for flow in json_array(&row.get::<Value, _>("token_inflows")) {
                inflow_transfers += 1;
                if let Some(source) = flow.get("from").and_then(Value::as_str) {
                    inflow_sources.insert(source.to_ascii_lowercase());
                    hashes.insert(tx_hash.clone());
                    if transfer_edges.len() < MAX_EVIDENCE_HASHES {
                        transfer_edges.push(json!({
                            "tx_hash":tx_hash,"direction":"inflow","from":source,
                            "to":cluster.seed_address,"token":flow.get("token"),
                            "amount_raw":flow.get("amount_raw")
                        }));
                    }
                }
            }
            for flow in json_array(&row.get::<Value, _>("token_outflows")) {
                outflow_transfers += 1;
                if let Some(recipient) = flow.get("to").and_then(Value::as_str) {
                    outflow_recipients.insert(recipient.to_ascii_lowercase());
                    hashes.insert(tx_hash.clone());
                    if transfer_edges.len() < MAX_EVIDENCE_HASHES {
                        transfer_edges.push(json!({
                            "tx_hash":tx_hash,"direction":"outflow","from":cluster.seed_address,
                            "to":recipient,"token":flow.get("token"),
                            "amount_raw":flow.get("amount_raw")
                        }));
                    }
                }
            }
        }
        let total = rows.len() as u64;
        let direct_execution_ratio = ratio(execution_actions, total);
        let (role, confidence, limitations) = if is_contract
            && received_top_level >= 5
            && direct_execution_ratio >= 0.20
        {
            (
                "executor_candidate",
                bounded_confidence(0.70, received_top_level, 0.98),
                vec![
                    "Frequent calls and execution evidence do not prove who controls the contract.",
                ],
            )
        } else if sent >= 20 && direct_execution_ratio >= 0.40 {
            (
                "worker_candidate",
                bounded_confidence(0.72, sent, 0.98),
                vec!["Execution activity supports a worker role, not real-world ownership."],
            )
        } else if inflow_transfers >= 10
            && sent <= 3.max(inflow_transfers / 50)
            && inflow_sources.len() >= 1
        {
            (
                "settlement_recipient_candidate",
                bounded_confidence(0.76, inflow_transfers, 0.99),
                vec![
                    "Inbound transfers are observed settlement-like flows, not necessarily profit.",
                    "The sending contracts and callers remain candidate relationships.",
                ],
            )
        } else if outflow_transfers >= 10
            && outflow_recipients.len() >= 2
            && direct_execution_ratio < 0.20
        {
            (
                "funding_treasury_candidate",
                bounded_confidence(0.72, outflow_transfers, 0.96),
                vec!["Capital distribution does not prove common ownership of recipients."],
            )
        } else if total <= 3 {
            (
                "ordinary_address",
                0.55,
                vec!["Available activity is insufficient for an execution-oriented role."],
            )
        } else {
            (
                "unknown",
                0.40,
                vec!["No supported seed-role rule crossed its evidence threshold."],
            )
        };
        let evidence = json!({
            "method_version":ROLE_METHOD_VERSION,
            "evidence_state":if confidence >= 0.85 {"candidate"} else {"unverified"},
            "facts":{
                "is_contract":is_contract,
                "transactions_observed":total,
                "top_level_sent":sent,
                "top_level_received":received_top_level,
                "execution_actions":execution_actions,
                "direct_execution_ratio":direct_execution_ratio,
                "token_inflow_transfers":inflow_transfers,
                "token_outflow_transfers":outflow_transfers,
                "distinct_inflow_sources":inflow_sources.len(),
                "distinct_outflow_recipients":outflow_recipients.len()
            },
            "supporting_tx_hashes":hashes.iter().take(MAX_EVIDENCE_HASHES).collect::<Vec<_>>(),
            "supporting_transfer_edges":transfer_edges,
            "limitations":limitations
        });
        let mut tx = self.pool.begin().await.map_err(db_error)?;
        sqlx::query(
            r#"update observed_address_objects
               set seed_role=$3,seed_role_confidence=$4,seed_role_evidence=$5,
                   object_type=case when $3='executor_candidate' then 'executor_contract_candidate'
                                    when $3='worker_candidate' then 'execution_eoa_candidate'
                                    else object_type end,
                   updated_at=now()
               where chain_id=$1 and address=$2"#,
        )
        .bind(self.config.chain_id as i64)
        .bind(&cluster.seed_address)
        .bind(role)
        .bind(confidence)
        .bind(&evidence)
        .execute(&mut *tx)
        .await
        .map_err(db_error)?;
        sqlx::query(
            r#"insert into candidate_execution_cluster_members (
                 cluster_id,chain_id,address,candidate_role,membership_state,evidence_state,
                 membership_confidence,evidence,supporting_tx_hashes,
                 include_in_economic_scope,include_in_behavior_scope,discovery_depth,
                 first_seen_at,last_seen_at
               ) values ($1,$2,$3,'seed','included','observed',1,$4,$5,true,true,0,$6,$7)
               on conflict (cluster_id,address) do update set
                 evidence=excluded.evidence,supporting_tx_hashes=excluded.supporting_tx_hashes,
                 first_seen_at=excluded.first_seen_at,last_seen_at=excluded.last_seen_at,
                 updated_at=now()
               where not candidate_execution_cluster_members.user_overridden"#,
        )
        .bind(cluster.cluster_id)
        .bind(self.config.chain_id as i64)
        .bind(&cluster.seed_address)
        .bind(evidence)
        .bind(
            hashes
                .into_iter()
                .take(MAX_EVIDENCE_HASHES)
                .collect::<Vec<_>>(),
        )
        .bind(
            rows.iter()
                .filter_map(|row| row.get::<Option<DateTime<Utc>>, _>("block_timestamp"))
                .min(),
        )
        .bind(
            rows.iter()
                .filter_map(|row| row.get::<Option<DateTime<Utc>>, _>("block_timestamp"))
                .max(),
        )
        .execute(&mut *tx)
        .await
        .map_err(db_error)?;
        tx.commit().await.map_err(db_error)?;
        Ok(())
    }

    async fn discover_relationships(&self, cluster: &ClaimedCluster) -> LedgerResult<()> {
        let rows = sqlx::query(
            r#"select tx_hash,block_timestamp,from_address,to_address,action_label,
                      token_inflows,token_outflows
               from object_activity_transactions
               where chain_id=$1 and address=$2 and processing_status='analyzed'"#,
        )
        .bind(self.config.chain_id as i64)
        .bind(&cluster.seed_address)
        .fetch_all(&self.pool)
        .await
        .map_err(db_error)?;
        let mut relations = BTreeMap::<(String, String, String), RelationshipEvidence>::new();
        for row in rows {
            let tx_hash: String = row.get("tx_hash");
            let timestamp: Option<DateTime<Utc>> = row.get("block_timestamp");
            let from = row
                .get::<Option<String>, _>("from_address")
                .map(|value| value.to_ascii_lowercase());
            let to = row
                .get::<Option<String>, _>("to_address")
                .map(|value| value.to_ascii_lowercase());
            if from
                .as_deref()
                .is_some_and(|value| value == cluster.seed_address)
            {
                if let Some(target) = to.as_deref().filter(|value| *value != cluster.seed_address) {
                    observe_relationship(
                        &mut relations,
                        &cluster.seed_address,
                        target,
                        "frequent_call_target",
                        &tx_hash,
                        timestamp,
                        0.80,
                        "observed",
                        None,
                        "Seed is the recorded top-level transaction sender and called this address.",
                        1,
                    );
                }
            }
            for flow in json_array(&row.get::<Value, _>("token_inflows")) {
                let Some(source) = flow.get("from").and_then(Value::as_str) else {
                    continue;
                };
                let source = source.to_ascii_lowercase();
                if source == cluster.seed_address {
                    continue;
                }
                let amount = flow_amount(flow);
                observe_relationship(
                    &mut relations,
                    &source,
                    &cluster.seed_address,
                    "settlement_recipient_candidate",
                    &tx_hash,
                    timestamp,
                    0.72,
                    "candidate",
                    amount.clone(),
                    "The related address received an ERC-20 transfer after this transaction; this is settlement-like inflow, not verified profit.",
                    1,
                );
                observe_relationship(
                    &mut relations,
                    &cluster.seed_address,
                    &source,
                    "executor_candidate",
                    &tx_hash,
                    timestamp,
                    0.72,
                    "candidate",
                    amount,
                    "This address repeatedly transferred transaction output to the seed and is a candidate executor, not a confirmed common-control member.",
                    1,
                );
                if let Some(worker) = from
                    .as_deref()
                    .filter(|value| *value != cluster.seed_address && *value != source.as_str())
                {
                    observe_relationship(
                        &mut relations,
                        &cluster.seed_address,
                        worker,
                        "worker_candidate",
                        &tx_hash,
                        timestamp,
                        0.68,
                        "candidate",
                        None,
                        "This address is the recorded transaction sender in repeated executions that settled to the seed.",
                        2,
                    );
                    observe_relationship(
                        &mut relations,
                        worker,
                        &source,
                        "co_execution_relationship",
                        &tx_hash,
                        timestamp,
                        0.82,
                        "observed",
                        None,
                        "The sender called an execution path that produced a transfer from the candidate executor to the seed in the same transaction.",
                        2,
                    );
                    if let Some(target) = to.as_deref().filter(|target| *target != source.as_str())
                    {
                        observe_relationship(
                            &mut relations,
                            worker,
                            target,
                            "frequent_call_target",
                            &tx_hash,
                            timestamp,
                            0.75,
                            "observed",
                            None,
                            "The candidate worker is the recorded sender and this address is the top-level call target.",
                            2,
                        );
                    }
                }
            }
            for flow in json_array(&row.get::<Value, _>("token_outflows")) {
                let Some(recipient) = flow.get("to").and_then(Value::as_str) else {
                    continue;
                };
                let recipient = recipient.to_ascii_lowercase();
                if recipient == cluster.seed_address {
                    continue;
                }
                let amount = flow_amount(flow);
                observe_relationship(
                    &mut relations,
                    &cluster.seed_address,
                    &recipient,
                    "funded_address",
                    &tx_hash,
                    timestamp,
                    0.80,
                    "observed",
                    amount.clone(),
                    "The seed transferred an ERC-20 asset to this address; common ownership is not asserted.",
                    1,
                );
                observe_relationship(
                    &mut relations,
                    &recipient,
                    &cluster.seed_address,
                    "funded_by",
                    &tx_hash,
                    timestamp,
                    0.80,
                    "observed",
                    amount,
                    "This address received an ERC-20 transfer from the seed; the economic purpose remains unverified.",
                    1,
                );
            }
        }
        let infrastructure = sqlx::query(
            r#"select related_address,relationship_type,evidence_state,interaction_count,
                      supporting_tx_hashes,first_seen_at,last_seen_at,confidence
               from object_relationships
               where chain_id=$1 and address=$2
                 and relationship_type in ('router','pool','protocol')"#,
        )
        .bind(self.config.chain_id as i64)
        .bind(&cluster.seed_address)
        .fetch_all(&self.pool)
        .await
        .map_err(db_error)?;
        for row in infrastructure {
            let related: String = row.get("related_address");
            let kind: String = row.get("relationship_type");
            for tx_hash in row.get::<Vec<String>, _>("supporting_tx_hashes") {
                observe_relationship(
                    &mut relations,
                    &cluster.seed_address,
                    &related,
                    &kind,
                    &tx_hash,
                    row.get("last_seen_at"),
                    row.get("confidence"),
                    "observed",
                    None,
                    "Protocol registry or decoded transaction evidence identifies this infrastructure role; it is excluded from the control cluster.",
                    1,
                );
            }
        }
        let mut values = relations.into_values().collect::<Vec<_>>();
        values.sort_by(|left, right| {
            right
                .interaction_count
                .cmp(&left.interaction_count)
                .then_with(|| right.confidence.total_cmp(&left.confidence))
        });
        values.truncate(self.config.maximum_relationships.saturating_mul(4));
        let mut tx = self.pool.begin().await.map_err(db_error)?;
        sqlx::query("delete from candidate_execution_cluster_relationships where cluster_id=$1")
            .bind(cluster.cluster_id)
            .execute(&mut *tx)
            .await
            .map_err(db_error)?;
        for mut relation in values {
            relation.confidence = relationship_confidence(
                relation.confidence,
                relation.interaction_count,
                &relation.relationship_type,
            );
            let reason = format!(
                "{} Observed in {} supporting transaction(s).",
                relation
                    .reason_facts
                    .iter()
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(" "),
                relation.interaction_count
            );
            let evidence = json!({
                "method_version":RELATIONSHIP_METHOD_VERSION,
                "facts":relation.reason_facts,
                "ownership_claimed":false
            });
            sqlx::query(
                r#"insert into candidate_execution_cluster_relationships (
                     cluster_id,chain_id,source_address,related_address,relationship_type,
                     evidence_state,interaction_count,value_transferred,first_seen_at,last_seen_at,
                     supporting_tx_hashes,confidence,evidence_reason,evidence,discovery_depth
                   ) values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)"#,
            )
            .bind(cluster.cluster_id)
            .bind(self.config.chain_id as i64)
            .bind(relation.source_address)
            .bind(relation.related_address)
            .bind(relation.relationship_type)
            .bind(relation.evidence_state)
            .bind(relation.interaction_count as i64)
            .bind(amount_map_json(&relation.values))
            .bind(relation.first_seen)
            .bind(relation.last_seen)
            .bind(
                relation
                    .tx_hashes
                    .into_iter()
                    .take(MAX_EVIDENCE_HASHES)
                    .collect::<Vec<_>>(),
            )
            .bind(relation.confidence)
            .bind(reason)
            .bind(evidence)
            .bind(relation.depth)
            .execute(&mut *tx)
            .await
            .map_err(db_error)?;
        }
        tx.commit().await.map_err(db_error)?;
        Ok(())
    }

    async fn finalize_cluster_members(&self, cluster: &ClaimedCluster) -> LedgerResult<()> {
        let rows = sqlx::query(
            r#"select source_address,related_address,relationship_type,evidence_state,
                      interaction_count,supporting_tx_hashes,confidence,evidence_reason,
                      first_seen_at,last_seen_at,discovery_depth
               from candidate_execution_cluster_relationships
               where cluster_id=$1
               order by interaction_count desc,confidence desc"#,
        )
        .bind(cluster.cluster_id)
        .fetch_all(&self.pool)
        .await
        .map_err(db_error)?;
        let mut candidates = HashMap::<String, MemberCandidate>::new();
        for row in rows {
            let source: String = row.get("source_address");
            let related: String = row.get("related_address");
            let kind: String = row.get("relationship_type");
            let member = match kind.as_str() {
                "worker_candidate" => Some((related, "worker_candidate")),
                "executor_candidate" => Some((related, "executor_candidate")),
                "co_execution_relationship" => Some((source, "worker_candidate")),
                "settlement_recipient_candidate" => {
                    Some((related, "settlement_recipient_candidate"))
                }
                "profit_recipient_candidate" => Some((related, "profit_recipient_candidate")),
                "funded_address" => Some((related, "unresolved")),
                // Routers, pools, protocols and generic call targets are
                // intentionally relationships only, never control members.
                _ => None,
            };
            let Some((address, role)) = member else {
                continue;
            };
            if address == cluster.seed_address {
                continue;
            }
            let count = row.get::<i64, _>("interaction_count").max(0) as u64;
            let confidence = row.get::<f64, _>("confidence");
            let entry = candidates
                .entry(address.clone())
                .or_insert_with(|| MemberCandidate {
                    address,
                    role: role.to_owned(),
                    confidence,
                    interaction_count: 0,
                    evidence_state: row.get("evidence_state"),
                    hashes: BTreeSet::new(),
                    reasons: BTreeSet::new(),
                    first_seen: row.get("first_seen_at"),
                    last_seen: row.get("last_seen_at"),
                    depth: row.get("discovery_depth"),
                });
            if role_precedence(role) > role_precedence(&entry.role) {
                entry.role = role.to_owned();
            }
            entry.interaction_count = entry.interaction_count.saturating_add(count);
            entry.confidence = entry.confidence.max(confidence);
            entry
                .hashes
                .extend(row.get::<Vec<String>, _>("supporting_tx_hashes"));
            entry
                .reasons
                .insert(row.get::<String, _>("evidence_reason"));
            entry.first_seen = min_time(entry.first_seen, row.get("first_seen_at"));
            entry.last_seen = max_time(entry.last_seen, row.get("last_seen_at"));
            entry.depth = entry.depth.min(row.get("discovery_depth"));
        }
        let mut candidates = candidates.into_values().collect::<Vec<_>>();
        candidates.sort_by(|left, right| {
            right
                .confidence
                .total_cmp(&left.confidence)
                .then_with(|| right.interaction_count.cmp(&left.interaction_count))
        });
        candidates.truncate(self.config.maximum_cluster_members.saturating_sub(1));
        let mut tx = self.pool.begin().await.map_err(db_error)?;
        let addresses = candidates
            .iter()
            .map(|value| value.address.clone())
            .collect::<Vec<_>>();
        sqlx::query(
            r#"delete from candidate_execution_cluster_members
               where cluster_id=$1 and address<>$2 and not user_overridden
                 and not (address=any($3::text[]))"#,
        )
        .bind(cluster.cluster_id)
        .bind(&cluster.seed_address)
        .bind(&addresses)
        .execute(&mut *tx)
        .await
        .map_err(db_error)?;
        for member in candidates {
            let strong_execution_member = matches!(
                member.role.as_str(),
                "worker_candidate" | "executor_candidate" | "settlement_recipient_candidate"
            ) && member.confidence >= 0.85
                && member.interaction_count >= 3;
            let evidence = json!({
                "method_version":RELATIONSHIP_METHOD_VERSION,
                "interaction_count":member.interaction_count,
                "reasons":member.reasons,
                "ownership_claimed":false,
                "economic_scope_rule":"confidence >= 0.85 and at least 3 repeated execution relationships"
            });
            sqlx::query(
                r#"insert into candidate_execution_cluster_members (
                     cluster_id,chain_id,address,candidate_role,membership_state,evidence_state,
                     membership_confidence,evidence,supporting_tx_hashes,
                     include_in_economic_scope,include_in_behavior_scope,discovery_depth,
                     first_seen_at,last_seen_at
                   ) values ($1,$2,$3,$4,'candidate',$5,$6,$7,$8,$9,true,$10,$11,$12)
                   on conflict (cluster_id,address) do update set
                     candidate_role=excluded.candidate_role,
                     membership_state=excluded.membership_state,
                     evidence_state=excluded.evidence_state,
                     membership_confidence=excluded.membership_confidence,
                     evidence=excluded.evidence,
                     supporting_tx_hashes=excluded.supporting_tx_hashes,
                     include_in_economic_scope=excluded.include_in_economic_scope,
                     include_in_behavior_scope=excluded.include_in_behavior_scope,
                     discovery_depth=excluded.discovery_depth,
                     first_seen_at=excluded.first_seen_at,last_seen_at=excluded.last_seen_at,
                     updated_at=now()
                   where not candidate_execution_cluster_members.user_overridden"#,
            )
            .bind(cluster.cluster_id)
            .bind(self.config.chain_id as i64)
            .bind(&member.address)
            .bind(member.role)
            .bind(member.evidence_state)
            .bind(member.confidence)
            .bind(evidence)
            .bind(
                member
                    .hashes
                    .into_iter()
                    .take(MAX_EVIDENCE_HASHES)
                    .collect::<Vec<_>>(),
            )
            .bind(strong_execution_member)
            .bind(member.depth)
            .bind(member.first_seen)
            .bind(member.last_seen)
            .execute(&mut *tx)
            .await
            .map_err(db_error)?;
        }
        tx.commit().await.map_err(db_error)?;
        Ok(())
    }

    async fn reconstruct_execution_batch(&self, cluster: &ClaimedCluster) -> LedgerResult<bool> {
        let initial_profile_complete = sqlx::query_scalar::<_, bool>(
            "select analysis_completed_at is not null from candidate_execution_clusters where cluster_id=$1",
        )
        .bind(cluster.cluster_id)
        .fetch_one(&self.pool)
        .await
        .map_err(db_error)?;
        let missing_rows = sqlx::query(
            r#"select activity.tx_hash,activity.block_number,activity.block_timestamp,
                      activity.from_address,activity.to_address,activity.token_inflows,
                      activity.token_outflows,activity.protocol_relationships,
                      activity.touched_pools
               from object_activity_transactions activity
               where activity.chain_id=$1 and activity.address=$2
                 and activity.processing_status='analyzed'
                 and activity.block_timestamp >= now()-interval '24 hours'
                 and not exists (
                   select 1 from candidate_cluster_executions executions
                   where executions.cluster_id=$3 and executions.tx_hash=activity.tx_hash
                 )
                 and (
                   exists (
                     select 1 from candidate_execution_cluster_members members
                     where members.cluster_id=$3 and members.include_in_behavior_scope
                       and members.membership_state<>'excluded'
                       and members.address in (activity.from_address,activity.to_address)
                   )
                   or jsonb_array_length(activity.token_inflows)>0
                   or jsonb_array_length(activity.token_outflows)>0
                 )
               order by (activity.block_timestamp >= now()-interval '24 hours') desc,
                        activity.block_number desc
               limit $4"#,
        )
        .bind(self.config.chain_id as i64)
        .bind(&cluster.seed_address)
        .bind(cluster.cluster_id)
        .bind(self.config.basic_reconstruction_batch_size as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(db_error)?;
        if !missing_rows.is_empty() {
            let inputs = execution_inputs(missing_rows);
            self.persist_basic_execution_batch(cluster, inputs).await?;
            return Ok(false);
        }
        let deep_24h_count = sqlx::query_scalar::<_, i64>(
            r#"select count(*) from candidate_cluster_executions
               where cluster_id=$1 and block_timestamp>=now()-interval '24 hours'
                 and inputs_digest not like 'basic:%'"#,
        )
        .bind(cluster.cluster_id)
        .fetch_one(&self.pool)
        .await
        .map_err(db_error)?;
        if !initial_profile_complete
            && deep_24h_count >= self.config.initial_deep_execution_sample as i64
        {
            return Ok(true);
        }
        let enrichment_rows = sqlx::query(
            r#"select activity.tx_hash,activity.block_number,activity.block_timestamp,
                      activity.from_address,activity.to_address,activity.token_inflows,
                      activity.token_outflows,activity.protocol_relationships,
                      activity.touched_pools
               from object_activity_transactions activity
               join candidate_cluster_executions executions
                 on executions.cluster_id=$3 and executions.tx_hash=activity.tx_hash
               where activity.chain_id=$1 and activity.address=$2
                 and executions.inputs_digest like 'basic:%'
                 and activity.block_timestamp>=now()-interval '24 hours'
               order by activity.block_number desc limit $4"#,
        )
        .bind(self.config.chain_id as i64)
        .bind(&cluster.seed_address)
        .bind(cluster.cluster_id)
        .bind(self.config.execution_batch_size as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(db_error)?;
        if enrichment_rows.is_empty() {
            if !initial_profile_complete {
                return Ok(true);
            }
            return self
                .reconstruct_background_history_batch(cluster)
                .await
                .map(|_| true);
        }
        let inputs = execution_inputs(enrichment_rows);
        self.enrich_execution_inputs(cluster, inputs).await?;
        // Initial profiles intentionally enrich a bounded sample before they
        // become ready. Incremental cycles enrich one bounded batch and then
        // publish refreshed windows; otherwise a high-frequency address can
        // remain stuck in reconstruction for hours while live summaries age.
        Ok(initial_profile_complete)
    }

    async fn reconstruct_background_history_batch(
        &self,
        cluster: &ClaimedCluster,
    ) -> LedgerResult<()> {
        let missing_rows = sqlx::query(
            r#"select activity.tx_hash,activity.block_number,activity.block_timestamp,
                      activity.from_address,activity.to_address,activity.token_inflows,
                      activity.token_outflows,activity.protocol_relationships,
                      activity.touched_pools
               from object_activity_transactions activity
               where activity.chain_id=$1 and activity.address=$2
                 and activity.processing_status='analyzed'
                 and activity.block_timestamp >= now()-make_interval(days=>$3)
                 and activity.block_timestamp < now()-interval '24 hours'
                 and not exists (
                   select 1 from candidate_cluster_executions executions
                   where executions.cluster_id=$4 and executions.tx_hash=activity.tx_hash
                 )
                 and (
                   exists (
                     select 1 from candidate_execution_cluster_members members
                     where members.cluster_id=$4 and members.include_in_behavior_scope
                       and members.membership_state<>'excluded'
                       and members.address in (activity.from_address,activity.to_address)
                   )
                   or jsonb_array_length(activity.token_inflows)>0
                   or jsonb_array_length(activity.token_outflows)>0
                 )
               order by activity.block_number desc limit $5"#,
        )
        .bind(self.config.chain_id as i64)
        .bind(&cluster.seed_address)
        .bind(self.config.history_days as i32)
        .bind(cluster.cluster_id)
        .bind(self.config.basic_reconstruction_batch_size as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(db_error)?;
        if !missing_rows.is_empty() {
            return self
                .persist_basic_execution_batch(cluster, execution_inputs(missing_rows))
                .await;
        }
        let enrichment_rows = sqlx::query(
            r#"select activity.tx_hash,activity.block_number,activity.block_timestamp,
                      activity.from_address,activity.to_address,activity.token_inflows,
                      activity.token_outflows,activity.protocol_relationships,
                      activity.touched_pools
               from object_activity_transactions activity
               join candidate_cluster_executions executions
                 on executions.cluster_id=$3 and executions.tx_hash=activity.tx_hash
               where activity.chain_id=$1 and activity.address=$2
                 and executions.inputs_digest like 'basic:%'
                 and activity.block_timestamp < now()-interval '24 hours'
               order by activity.block_number desc limit $4"#,
        )
        .bind(self.config.chain_id as i64)
        .bind(&cluster.seed_address)
        .bind(cluster.cluster_id)
        .bind(self.config.execution_batch_size as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(db_error)?;
        if !enrichment_rows.is_empty() {
            self.enrich_execution_inputs(cluster, execution_inputs(enrichment_rows))
                .await?;
        }
        Ok(())
    }

    async fn enrich_execution_inputs(
        &self,
        cluster: &ClaimedCluster,
        inputs: Vec<ExecutionInput>,
    ) -> LedgerResult<()> {
        let mut tasks = tokio::task::JoinSet::new();
        for input in inputs {
            let service = self.analysis.clone();
            let chain_id = self.config.chain_id;
            tasks.spawn(async move {
                let cached = service
                    .repository
                    .get_analysis(chain_id, &input.tx_hash)
                    .await;
                let result = match cached {
                    Ok(Some(analysis)) => Ok(analysis),
                    Ok(None) => service
                        .analyze_transaction(chain_id, &input.tx_hash)
                        .await
                        .map(|(analysis, _)| analysis),
                    Err(error) => Err(error),
                };
                (input, result)
            });
        }
        while let Some(joined) = tasks.join_next().await {
            let (input, result) = joined.map_err(|error| {
                LedgerError::TransientIngestion(format!(
                    "cluster execution analysis task failed: {error}"
                ))
            })?;
            match result {
                Ok(analysis) => {
                    self.persist_reconstructed_execution(cluster, &input, &analysis)
                        .await?
                }
                Err(error) => {
                    self.persist_failed_reconstruction(cluster, &input, &error.to_string())
                        .await?
                }
            }
        }
        Ok(())
    }

    async fn persist_basic_execution_batch(
        &self,
        cluster: &ClaimedCluster,
        inputs: Vec<ExecutionInput>,
    ) -> LedgerResult<()> {
        let mut tasks = tokio::task::JoinSet::new();
        for input in inputs {
            let rpc = self.rpc.clone();
            let chain_id = self.config.chain_id;
            tasks.spawn(async move {
                let receipt = rpc.get_receipt(chain_id, &input.tx_hash).await;
                (input, receipt)
            });
        }
        while let Some(joined) = tasks.join_next().await {
            let (input, receipt) = joined.map_err(|error| {
                LedgerError::TransientIngestion(format!(
                    "basic execution receipt task failed: {error}"
                ))
            })?;
            let receipt = receipt?;
            let status = match receipt.get("status").and_then(Value::as_str) {
                Some("0x1") => "success",
                Some("0x0") => "failure",
                _ => "pending",
            };
            let gas_cost = receipt_gas_cost(&receipt);
            let execution_type = if status == "failure" {
                "failed_execution"
            } else {
                "unknown_interaction"
            };
            let settlement_transfers = json_array(&input.seed_token_inflows).to_vec();
            let capital_outflows = json_array(&input.seed_token_outflows).to_vec();
            let digest = format!(
                "basic:{}",
                digest_json(&json!({
                    "tx_hash":input.tx_hash,
                    "receipt_status":receipt.get("status"),
                    "gas_cost_raw":gas_cost,
                    "method":"receipt-transfer-cluster-reconstruction.v1"
                }))
            );
            sqlx::query(
                r#"insert into candidate_cluster_executions (
                     cluster_id,chain_id,tx_hash,block_number,block_timestamp,worker_address,
                     executor_address,execution_type,transaction_status,protocols,pools,
                     input_assets,gas_cost_raw,settlement_transfers,candidate_profit_recipient,
                     economic_verification_status,evidence_refs,inputs_digest
                   ) values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,'unknown',$16,$17)
                   on conflict (cluster_id,tx_hash) do nothing"#,
            )
            .bind(cluster.cluster_id)
            .bind(self.config.chain_id as i64)
            .bind(&input.tx_hash)
            .bind(input.block_number as i64)
            .bind(input.block_timestamp)
            .bind(input.from_address.as_deref())
            .bind(input.to_address.as_deref())
            .bind(execution_type)
            .bind(status)
            .bind(json!(unique_protocols(&input.seed_protocol_relationships)))
            .bind(json!(unique_strings(&input.seed_touched_pools)))
            .bind(json!(capital_outflows))
            .bind(gas_cost)
            .bind(json!(settlement_transfers))
            .bind(
                (!json_array(&input.seed_token_inflows).is_empty())
                    .then(|| cluster.seed_address.clone()),
            )
            .bind(json!([
                {"kind":"tx","reference":input.tx_hash},
                {"kind":"receipt","reference":input.tx_hash},
                {"kind":"block","reference":input.block_number}
            ]))
            .bind(digest)
            .execute(&self.pool)
            .await
            .map_err(db_error)?;
        }
        Ok(())
    }

    async fn persist_reconstructed_execution(
        &self,
        cluster: &ClaimedCluster,
        input: &ExecutionInput,
        analysis: &TransactionAnalysis,
    ) -> LedgerResult<()> {
        let scope = self.cluster_scope(cluster.cluster_id, true).await?;
        let behavior_scope = self.cluster_scope(cluster.cluster_id, false).await?;
        let execution_type = classify_cluster_execution(analysis, &cluster.seed_address, &scope);
        let worker = behavior_scope
            .contains(&analysis.from_address.to_ascii_lowercase())
            .then(|| analysis.from_address.to_ascii_lowercase())
            .or_else(|| input.from_address.as_deref().map(str::to_ascii_lowercase));
        let executor = analysis
            .to_address
            .as_deref()
            .map(str::to_ascii_lowercase)
            .filter(|address| behavior_scope.contains(address))
            .or_else(|| input.to_address.as_deref().map(str::to_ascii_lowercase));
        let protocols = unique_protocols(&input.seed_protocol_relationships);
        let pools = unique_strings(&input.seed_touched_pools);
        let (input_assets, output_assets) = scoped_assets(analysis, &scope);
        let settlement_transfers = analysis
            .derived_flows
            .iter()
            .filter(|flow| {
                flow.to_address.eq_ignore_ascii_case(&cluster.seed_address)
                    && !scope.contains(&flow.from_address.to_ascii_lowercase())
                    || (flow.to_address.eq_ignore_ascii_case(&cluster.seed_address)
                        && flow.from_address.to_ascii_lowercase() != cluster.seed_address)
            })
            .map(flow_json)
            .collect::<Vec<_>>();
        let candidate_profit_recipient =
            (!settlement_transfers.is_empty()).then(|| cluster.seed_address.clone());
        let gas_cost = analysis_gas_cost(analysis);
        let evidence_refs = analysis
            .raw_evidence_refs
            .iter()
            .map(|item| serde_json::to_value(item).unwrap_or_default())
            .collect::<Vec<_>>();
        let status = transaction_status(&analysis.status);
        sqlx::query(
            r#"insert into candidate_cluster_executions (
                 cluster_id,chain_id,tx_hash,block_number,block_timestamp,worker_address,
                 executor_address,execution_type,transaction_status,protocols,pools,route,
                 input_assets,output_assets,gas_cost_raw,settlement_transfers,
                 candidate_profit_recipient,economic_verification_status,evidence_refs,inputs_digest
               ) values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,'unknown',$18,$19)
               on conflict (cluster_id,tx_hash) do update set
                 worker_address=excluded.worker_address,executor_address=excluded.executor_address,
                 execution_type=excluded.execution_type,transaction_status=excluded.transaction_status,
                 protocols=excluded.protocols,pools=excluded.pools,route=excluded.route,
                 input_assets=excluded.input_assets,output_assets=excluded.output_assets,
                 gas_cost_raw=excluded.gas_cost_raw,settlement_transfers=excluded.settlement_transfers,
                 candidate_profit_recipient=excluded.candidate_profit_recipient,
                 evidence_refs=excluded.evidence_refs,inputs_digest=excluded.inputs_digest,updated_at=now()"#,
        )
        .bind(cluster.cluster_id)
        .bind(self.config.chain_id as i64)
        .bind(&input.tx_hash)
        .bind(input.block_number as i64)
        .bind(input.block_timestamp.or(analysis.block_timestamp))
        .bind(worker)
        .bind(executor)
        .bind(execution_type)
        .bind(status)
        .bind(json!(protocols))
        .bind(json!(pools))
        .bind(json!(analysis.normalized_execution_facts.participants))
        .bind(json!(input_assets))
        .bind(json!(output_assets))
        .bind(gas_cost)
        .bind(json!(settlement_transfers))
        .bind(candidate_profit_recipient)
        .bind(json!(evidence_refs))
        .bind(&analysis.input_digest)
        .execute(&self.pool)
        .await
        .map_err(db_error)?;
        Ok(())
    }

    async fn persist_failed_reconstruction(
        &self,
        cluster: &ClaimedCluster,
        input: &ExecutionInput,
        error: &str,
    ) -> LedgerResult<()> {
        let digest = digest_json(&json!({
            "tx_hash":input.tx_hash,
            "error":error,
            "method":"failed-reconstruction.v1"
        }));
        sqlx::query(
            r#"insert into candidate_cluster_executions (
                 cluster_id,chain_id,tx_hash,block_number,block_timestamp,worker_address,
                 executor_address,execution_type,transaction_status,
                 economic_verification_status,exclusion_reason,evidence_refs,inputs_digest
               ) values ($1,$2,$3,$4,$5,$6,$7,'unknown_interaction','pending','excluded',$8,$9,$10)
               on conflict (cluster_id,tx_hash) do update set
                 execution_type='unknown_interaction',transaction_status='pending',
                 economic_verification_status='excluded',exclusion_reason=excluded.exclusion_reason,
                 evidence_refs=excluded.evidence_refs,inputs_digest=excluded.inputs_digest,updated_at=now()"#,
        )
        .bind(cluster.cluster_id)
        .bind(self.config.chain_id as i64)
        .bind(&input.tx_hash)
        .bind(input.block_number as i64)
        .bind(input.block_timestamp)
        .bind(input.from_address.as_deref())
        .bind(input.to_address.as_deref())
        .bind(format!(
            "execution reconstruction unavailable: {}",
            error.chars().take(300).collect::<String>()
        ))
        .bind(json!([{"kind":"tx","reference":input.tx_hash}]))
        .bind(format!("failed:{digest}"))
        .execute(&self.pool)
        .await
        .map_err(db_error)?;
        Ok(())
    }

    async fn cluster_scope(
        &self,
        cluster_id: Uuid,
        economic: bool,
    ) -> LedgerResult<BTreeSet<String>> {
        let field = if economic {
            "include_in_economic_scope"
        } else {
            "include_in_behavior_scope"
        };
        let query = format!(
            "select address from candidate_execution_cluster_members where cluster_id=$1 and membership_state<>'excluded' and {field}"
        );
        let rows = sqlx::query(&query)
            .bind(cluster_id)
            .fetch_all(&self.pool)
            .await
            .map_err(db_error)?;
        Ok(rows
            .into_iter()
            .map(|row| row.get::<String, _>("address"))
            .collect())
    }

    async fn verify_economic_batch(&self, cluster: &ClaimedCluster) -> LedgerResult<bool> {
        let initial_profile_complete = sqlx::query_scalar::<_, bool>(
            "select analysis_completed_at is not null from candidate_execution_clusters where cluster_id=$1",
        )
        .bind(cluster.cluster_id)
        .fetch_one(&self.pool)
        .await
        .map_err(db_error)?;
        let rows = sqlx::query(
            r#"select tx_hash,execution_type,transaction_status,gas_cost_raw,
                      worker_address,executor_address,candidate_profit_recipient
               from candidate_cluster_executions
               where cluster_id=$1 and economic_verification_status='unknown'
                 and inputs_digest not like 'basic:%'
               order by block_timestamp desc nulls last,block_number desc limit $2"#,
        )
        .bind(cluster.cluster_id)
        .bind(self.config.execution_batch_size as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(db_error)?;
        if rows.is_empty() {
            return Ok(true);
        }
        let cluster_scope = self.cluster_scope(cluster.cluster_id, true).await?;
        for row in rows {
            let tx_hash: String = row.get("tx_hash");
            let execution_type: String = row.get("execution_type");
            let transaction_status: String = row.get("transaction_status");
            let gas_cost: Option<String> = row.get("gas_cost_raw");
            // Balance snapshots are block-scoped. Restrict each verification to
            // the evidence-backed members that actually participate in this
            // execution, rather than every candidate member in the cluster.
            // Besides making archive verification bounded, this prevents an
            // unrelated member transaction in the same block from contaminating
            // the execution's accounting scope.
            let mut execution_scope = BTreeSet::new();
            for address in [
                Some(cluster.seed_address.as_str()),
                row.get::<Option<String>, _>("worker_address").as_deref(),
                row.get::<Option<String>, _>("executor_address").as_deref(),
                row.get::<Option<String>, _>("candidate_profit_recipient")
                    .as_deref(),
            ]
            .into_iter()
            .flatten()
            {
                let address = address.to_ascii_lowercase();
                if cluster_scope.contains(&address) {
                    execution_scope.insert(address);
                }
            }
            if execution_scope.is_empty() {
                self.exclude_economic_result(
                    cluster.cluster_id,
                    &tx_hash,
                    "no evidence-backed execution participants are in economic scope",
                    gas_cost.as_deref(),
                )
                .await?;
                continue;
            }
            if transaction_status == "failure" {
                self.exclude_economic_result(
                    cluster.cluster_id,
                    &tx_hash,
                    "failed execution has no verified execution profit; exact gas remains a failed cost",
                    gas_cost.as_deref(),
                )
                .await?;
                continue;
            }
            if matches!(
                execution_type.as_str(),
                "funding" | "settlement" | "approval" | "contract_deployment"
            ) {
                self.exclude_economic_result(
                    cluster.cluster_id,
                    &tx_hash,
                    &format!(
                        "{execution_type} is accounted separately and is not execution profit"
                    ),
                    gas_cost.as_deref(),
                )
                .await?;
                continue;
            }
            let Some(analysis) = self
                .analysis
                .repository
                .get_analysis(self.config.chain_id, &tx_hash)
                .await?
            else {
                self.exclude_economic_result(
                    cluster.cluster_id,
                    &tx_hash,
                    "normalized transaction analysis is unavailable",
                    gas_cost.as_deref(),
                )
                .await?;
                continue;
            };
            let verification = verify_deterministic_profit_for_scope(
                self.rpc.as_ref(),
                &analysis,
                &execution_scope,
            )
            .await?
            .unwrap_or_else(
                || json!({"status":"unavailable","reason":"economic verifier returned no result"}),
            );
            let reconciled_surpluses = reconciled_cluster_surpluses(&verification);
            if verification.get("status").and_then(Value::as_str) != Some("verified")
                && reconciled_surpluses.is_none()
            {
                let reason = verification
                    .get("reason")
                    .and_then(Value::as_str)
                    .unwrap_or("deterministic economic scope could not be reconciled");
                self.exclude_economic_result(
                    cluster.cluster_id,
                    &tx_hash,
                    reason,
                    gas_cost.as_deref(),
                )
                .await?;
                continue;
            }
            let gross = if let Some(surpluses) = reconciled_surpluses {
                surpluses
            } else {
                let profit_token = verification
                    .get("profit_token_address")
                    .and_then(Value::as_str)
                    .map(str::to_ascii_lowercase)
                    .ok_or_else(|| {
                        LedgerError::AnalysisInvariant(
                            "verified economic result omitted profit token".to_owned(),
                        )
                    })?;
                let profit_raw = verification
                    .get("profit_raw")
                    .and_then(Value::as_str)
                    .and_then(decimal_bigint)
                    .ok_or_else(|| {
                        LedgerError::AnalysisInvariant(
                            "verified economic result omitted profit amount".to_owned(),
                        )
                    })?;
                vec![json!({"asset":profit_token,"amount_raw":profit_raw.to_string()})]
            };
            let verified_gas = verification
                .get("gas_cost_raw")
                .and_then(Value::as_str)
                .and_then(decimal_bigint)
                .unwrap_or_default();
            let net = (gross.len() == 1)
                .then(|| {
                    let asset = gross[0].get("asset").and_then(Value::as_str)?;
                    let amount = gross[0]
                        .get("amount_raw")
                        .and_then(Value::as_str)
                        .and_then(decimal_bigint)?;
                    wrapped_native_address(self.config.chain_id)
                        .filter(|wrapped| wrapped.eq_ignore_ascii_case(asset))
                        .map(|_| amount - &verified_gas)
                })
                .flatten();
            let net_json = net
                .map(|amount| {
                    json!([{
                        "asset":gross[0].get("asset").and_then(Value::as_str),
                        "amount_raw":amount.to_string()
                    }])
                })
                .unwrap_or_else(|| json!([]));
            sqlx::query(
                r#"update candidate_cluster_executions
                   set economic_verification_status='verified',verified_gross_profit=$3,
                       verified_gas_cost_raw=$4,verified_net_result=$5,
                       candidate_profit_recipient=coalesce($6,candidate_profit_recipient),
                       execution_type=case
                         when execution_type in ('dex_swap','multi_pool_swap','unknown_interaction')
                           then 'closed_cycle_arbitrage'
                         else execution_type end,
                       exclusion_reason=null,
                       evidence_refs=evidence_refs||$7::jsonb,updated_at=now()
                   where cluster_id=$1 and tx_hash=$2"#,
            )
            .bind(cluster.cluster_id)
            .bind(&tx_hash)
            .bind(json!(gross))
            .bind(verified_gas.to_string())
            .bind(net_json)
            .bind(verification.get("profit_recipient").and_then(Value::as_str))
            .bind(json!([{
                "kind":"economic_verification",
                "method":ECONOMIC_METHOD_VERSION,
                "inputs":verification.get("evidence_refs").cloned().unwrap_or_else(|| json!([
                    tx_hash,
                    format!("{}:receipt",tx_hash),
                    format!("{}:trace",tx_hash),
                    format!("{}:archive-balance-reconciliation",tx_hash)
                ])),
                "result_semantics":if gross.len()>1 {
                    "verified multi-asset surplus vector; no cross-asset total"
                } else {
                    "verified single-asset surplus"
                }
            }]))
            .execute(&self.pool)
            .await
            .map_err(db_error)?;
        }
        // As with reconstruction, incremental refreshes verify one bounded
        // batch before rematerializing user-visible windows. Remaining deep
        // candidates are continued by later stale cycles.
        Ok(initial_profile_complete)
    }

    async fn exclude_economic_result(
        &self,
        cluster_id: Uuid,
        tx_hash: &str,
        reason: &str,
        gas_cost: Option<&str>,
    ) -> LedgerResult<()> {
        sqlx::query(
            r#"update candidate_cluster_executions
               set economic_verification_status='excluded',exclusion_reason=$3,
                   verified_gas_cost_raw=$4,updated_at=now()
               where cluster_id=$1 and tx_hash=$2"#,
        )
        .bind(cluster_id)
        .bind(tx_hash)
        .bind(reason.chars().take(500).collect::<String>())
        .bind(gas_cost)
        .execute(&self.pool)
        .await
        .map_err(db_error)?;
        Ok(())
    }

    async fn rebuild_windows_and_changes(&self, cluster: &ClaimedCluster) -> LedgerResult<()> {
        let executions = sqlx::query(
            r#"select tx_hash,block_timestamp,worker_address,executor_address,execution_type,
                      transaction_status,protocols,pools,gas_cost_raw,settlement_transfers,
                      economic_verification_status,verified_gross_profit,
                      verified_gas_cost_raw,verified_net_result,exclusion_reason,
                      input_assets,output_assets,inputs_digest
               from candidate_cluster_executions where cluster_id=$1"#,
        )
        .bind(cluster.cluster_id)
        .fetch_all(&self.pool)
        .await
        .map_err(db_error)?;
        let now = Utc::now();
        let windows = [
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
        ];
        for (window_key, start, end) in windows {
            let mut aggregate = WindowAccumulator::default();
            for row in &executions {
                let Some(timestamp) = row.get::<Option<DateTime<Utc>>, _>("block_timestamp") else {
                    continue;
                };
                if timestamp < start || timestamp >= end {
                    continue;
                }
                aggregate_execution(&mut aggregate, row, timestamp);
            }
            let relationships = sqlx::query_scalar::<_, i64>(
                r#"select count(*) from candidate_execution_cluster_relationships
                   where cluster_id=$1 and first_seen_at >= $2 and first_seen_at < $3"#,
            )
            .bind(cluster.cluster_id)
            .bind(start)
            .bind(end)
            .fetch_one(&self.pool)
            .await
            .map_err(db_error)?;
            let economic_candidates = aggregate.included + aggregate.excluded;
            let coverage_ratio = ratio(aggregate.included, economic_candidates);
            let net_funding =
                subtract_amount_maps(&aggregate.capital_returned, &aggregate.capital_sent);
            let coverage = json!({
                "candidate_executions":economic_candidates,
                "economically_verified":aggregate.included,
                "full_history_claimed":false,
                "scope":"evidence-scored candidate members only",
                "limitation":"This is not complete wallet or real-world entity PnL."
            });
            let input = json!({
                "cluster_id":cluster.cluster_id,"window":window_key,
                "executions":aggregate.evidence_hashes,"coverage":coverage
            });
            sqlx::query(
                r#"insert into candidate_cluster_window_summaries (
                     cluster_id,chain_id,window_key,window_start,window_end,
                     execution_count,success_count,failure_count,protocol_count,pool_count,
                     gas_cost_raw,verified_gross_profit,verified_gas_cost_raw,
                     verified_net_result,verified_execution_count,observed_settlement_inflows,
                     capital_sent_to_workers,capital_returned,net_funding_flow,
                     failed_tx_count,failed_gas_cost_raw,spray_attempt_cost_raw,
                     strategy_distribution,top_workers,top_executors,top_protocols,top_routes,
                     new_relationship_count,last_active_at,included_transaction_count,
                     excluded_transaction_count,exclusion_reasons,coverage_ratio,coverage,inputs_digest
                   ) values (
                     $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,
                     $20,$21,$22,$23,$24,$25,$26,'[]'::jsonb,$27,$28,$29,$30,$31,$32,$33,$34
                   )
                   on conflict (cluster_id,window_key) do update set
                     window_start=excluded.window_start,window_end=excluded.window_end,
                     execution_count=excluded.execution_count,success_count=excluded.success_count,
                     failure_count=excluded.failure_count,protocol_count=excluded.protocol_count,
                     pool_count=excluded.pool_count,gas_cost_raw=excluded.gas_cost_raw,
                     verified_gross_profit=excluded.verified_gross_profit,
                     verified_gas_cost_raw=excluded.verified_gas_cost_raw,
                     verified_net_result=excluded.verified_net_result,
                     verified_execution_count=excluded.verified_execution_count,
                     observed_settlement_inflows=excluded.observed_settlement_inflows,
                     capital_sent_to_workers=excluded.capital_sent_to_workers,
                     capital_returned=excluded.capital_returned,
                     net_funding_flow=excluded.net_funding_flow,
                     failed_tx_count=excluded.failed_tx_count,
                     failed_gas_cost_raw=excluded.failed_gas_cost_raw,
                     spray_attempt_cost_raw=excluded.spray_attempt_cost_raw,
                     strategy_distribution=excluded.strategy_distribution,
                     top_workers=excluded.top_workers,top_executors=excluded.top_executors,
                     top_protocols=excluded.top_protocols,
                     new_relationship_count=excluded.new_relationship_count,
                     last_active_at=excluded.last_active_at,
                     included_transaction_count=excluded.included_transaction_count,
                     excluded_transaction_count=excluded.excluded_transaction_count,
                     exclusion_reasons=excluded.exclusion_reasons,
                     coverage_ratio=excluded.coverage_ratio,coverage=excluded.coverage,
                     inputs_digest=excluded.inputs_digest,updated_at=now()"#,
            )
            .bind(cluster.cluster_id)
            .bind(self.config.chain_id as i64)
            .bind(window_key)
            .bind(start)
            .bind(end)
            .bind(aggregate.execution_count as i64)
            .bind(aggregate.success_count as i64)
            .bind(aggregate.failure_count as i64)
            .bind(aggregate.protocols.len() as i64)
            .bind(aggregate.pools.len() as i64)
            .bind(aggregate.gas_cost.to_string())
            .bind(amount_map_json(&aggregate.verified_gross))
            .bind(aggregate.verified_gas.to_string())
            .bind(amount_map_json(&aggregate.verified_net))
            .bind(aggregate.verified_count as i64)
            .bind(amount_map_json(&aggregate.settlement_inflows))
            .bind(amount_map_json(&aggregate.capital_sent))
            .bind(amount_map_json(&aggregate.capital_returned))
            .bind(amount_map_json(&net_funding))
            .bind(aggregate.failed_count as i64)
            .bind(aggregate.failed_gas.to_string())
            .bind(aggregate.failed_gas.to_string())
            .bind(rank_counts(&aggregate.strategies, "strategy"))
            .bind(rank_counts(&aggregate.workers, "address"))
            .bind(rank_counts(&aggregate.executors, "address"))
            .bind(rank_counts(&aggregate.protocols, "protocol"))
            .bind(relationships)
            .bind(aggregate.last_active)
            .bind(aggregate.included as i64)
            .bind(aggregate.excluded as i64)
            .bind(rank_counts(&aggregate.exclusion_reasons, "reason"))
            .bind(coverage_ratio)
            .bind(coverage)
            .bind(digest_json(&input))
            .execute(&self.pool)
            .await
            .map_err(db_error)?;
        }
        self.rebuild_changes(cluster.cluster_id).await
    }

    async fn rebuild_changes(&self, cluster_id: Uuid) -> LedgerResult<()> {
        let rows = sqlx::query(
            r#"select window_key,execution_count,success_count,failure_count,
                      gas_cost_raw,verified_net_result,inputs_digest
               from candidate_cluster_window_summaries
               where cluster_id=$1 and window_key in ('7d','previous_7d')"#,
        )
        .bind(cluster_id)
        .fetch_all(&self.pool)
        .await
        .map_err(db_error)?;
        let map = rows
            .into_iter()
            .map(|row| (row.get::<String, _>("window_key"), row))
            .collect::<HashMap<_, _>>();
        let (Some(current), Some(baseline)) = (map.get("7d"), map.get("previous_7d")) else {
            return Ok(());
        };
        let baseline_count = baseline.get::<i64, _>("execution_count");
        sqlx::query("delete from candidate_cluster_change_signals where cluster_id=$1")
            .bind(cluster_id)
            .execute(&self.pool)
            .await
            .map_err(db_error)?;
        if baseline_count <= 0 {
            return Ok(());
        }
        let current_count = current.get::<i64, _>("execution_count");
        let delta = (current_count - baseline_count) as f64 / baseline_count as f64;
        if delta.abs() >= 0.20 {
            let input = json!({
                "current_digest":current.get::<String,_>("inputs_digest"),
                "baseline_digest":baseline.get::<String,_>("inputs_digest"),
                "type":"activity_change"
            });
            sqlx::query(
                r#"insert into candidate_cluster_change_signals (
                     cluster_id,chain_id,signal_type,severity,conclusion,current_value,
                     baseline_value,confidence,inputs_digest
                   ) values ($1,$2,'activity_change',$3,$4,$5,$6,0.95,$7)"#,
            )
            .bind(cluster_id)
            .bind(self.config.chain_id as i64)
            .bind(if delta.abs() >= 1.0 { "high" } else { "medium" })
            .bind(format!(
                "Execution activity changed by {:+.1}% versus the previous 7-day window.",
                delta * 100.0
            ))
            .bind(json!({"execution_count":current_count}))
            .bind(json!({"execution_count":baseline_count}))
            .bind(digest_json(&input))
            .execute(&self.pool)
            .await
            .map_err(db_error)?;
        }
        Ok(())
    }

    pub async fn snapshot(
        &self,
        chain_id: u64,
        seed_address: &str,
        execution_limit: u32,
        execution_offset: u64,
    ) -> LedgerResult<Option<Value>> {
        let data_source = self.data_source_status(chain_id).await?;
        let seed_address = normalize_address(seed_address)?;
        let Some(cluster) = sqlx::query(
            r#"select clusters.*,objects.seed_role,objects.seed_role_confidence,
                      objects.seed_role_evidence,objects.last_activity_at,
                      objects.coverage_start_at,objects.coverage_end_at,
                      objects.coverage_is_partial
               from candidate_execution_clusters clusters
               join observed_address_objects objects
                 on objects.chain_id=clusters.chain_id and objects.address=clusters.seed_address
               where clusters.chain_id=$1 and clusters.seed_address=$2"#,
        )
        .bind(chain_id as i64)
        .bind(&seed_address)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_error)?
        else {
            return Ok(None);
        };
        let cluster_id: Uuid = cluster.get("cluster_id");
        let members = sqlx::query(
            r#"select address,candidate_role,membership_state,evidence_state,
                      membership_confidence,evidence,supporting_tx_hashes,
                      include_in_economic_scope,include_in_behavior_scope,
                      discovery_depth,first_seen_at,last_seen_at,user_overridden,override_note
               from candidate_execution_cluster_members where cluster_id=$1
               order by case candidate_role when 'seed' then 0 when 'worker_candidate' then 1
                        when 'executor_candidate' then 2
                        when 'settlement_recipient_candidate' then 3 else 4 end,
                        membership_confidence desc"#,
        )
        .bind(cluster_id)
        .fetch_all(&self.pool)
        .await
        .map_err(db_error)?;
        let relationships = sqlx::query(
            r#"select relationship_id,source_address,related_address,relationship_type,
                      evidence_state,interaction_count,value_transferred,first_seen_at,last_seen_at,
                      supporting_tx_hashes,confidence,evidence_reason,evidence,discovery_depth
               from candidate_execution_cluster_relationships where cluster_id=$1
               order by interaction_count desc,confidence desc limit 100"#,
        )
        .bind(cluster_id)
        .fetch_all(&self.pool)
        .await
        .map_err(db_error)?;
        let windows = sqlx::query(
            r#"select * from candidate_cluster_window_summaries where cluster_id=$1
               order by case window_key when '24h' then 1 when '7d' then 2
                                     when '30d' then 3 when 'previous_7d' then 4 else 5 end"#,
        )
        .bind(cluster_id)
        .fetch_all(&self.pool)
        .await
        .map_err(db_error)?;
        let executions = sqlx::query(
            r#"select * from candidate_cluster_executions where cluster_id=$1
               order by block_timestamp desc nulls last,block_number desc limit $2 offset $3"#,
        )
        .bind(cluster_id)
        .bind(execution_limit.clamp(1, 500) as i64)
        .bind(execution_offset.min(i64::MAX as u64) as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(db_error)?;
        let execution_total = sqlx::query_scalar::<_, i64>(
            "select count(*) from candidate_cluster_executions where cluster_id=$1",
        )
        .bind(cluster_id)
        .fetch_one(&self.pool)
        .await
        .map_err(db_error)?;
        let changes = sqlx::query(
            r#"select * from candidate_cluster_change_signals where cluster_id=$1
               order by detected_at desc limit 50"#,
        )
        .bind(cluster_id)
        .fetch_all(&self.pool)
        .await
        .map_err(db_error)?;
        let exclusions = sqlx::query(
            r#"select exclusion_reason,count(*) count
               from candidate_cluster_executions
               where cluster_id=$1 and economic_verification_status='excluded'
                 and exclusion_reason is not null
               group by exclusion_reason order by count(*) desc limit 10"#,
        )
        .bind(cluster_id)
        .fetch_all(&self.pool)
        .await
        .map_err(db_error)?;
        let partial_24h = sqlx::query(
            r#"select count(*) execution_count,
                      count(*) filter(where transaction_status='success') success_count,
                      count(*) filter(where transaction_status='failure') failure_count,
                      coalesce(sum(coalesce(nullif(gas_cost_raw,''),'0')::numeric),0)::text gas_cost_raw,
                      count(*) filter(where inputs_digest like 'basic:%') basic_count,
                      count(*) filter(where economic_verification_status='verified') verified_count,
                      max(block_timestamp) last_active_at
               from candidate_cluster_executions
               where cluster_id=$1 and block_timestamp>=now()-interval '24 hours'"#,
        )
        .bind(cluster_id)
        .fetch_one(&self.pool)
        .await
        .map_err(db_error)?;
        let assets = sqlx::query(
            r#"with observed_assets as (
                 select distinct lower(item->>'asset') token_address
                 from candidate_cluster_window_summaries summaries
                 cross join lateral jsonb_array_elements(
                   coalesce(summaries.verified_gross_profit,'[]'::jsonb)
                   ||coalesce(summaries.verified_net_result,'[]'::jsonb)
                   ||coalesce(summaries.observed_settlement_inflows,'[]'::jsonb)
                   ||coalesce(summaries.capital_sent_to_workers,'[]'::jsonb)
                   ||coalesce(summaries.capital_returned,'[]'::jsonb)
                 ) item
                 where summaries.cluster_id=$1 and item ? 'asset'
                 union
                 select distinct lower(item->>'asset') token_address
                 from candidate_cluster_executions executions
                 cross join lateral jsonb_array_elements(
                   coalesce(executions.verified_gross_profit,'[]'::jsonb)
                   ||coalesce(executions.verified_net_result,'[]'::jsonb)
                   ||coalesce(executions.settlement_transfers,'[]'::jsonb)
                 ) item
                 where executions.cluster_id=$1 and item ? 'asset'
               )
               select metadata.token_address,metadata.symbol,metadata.name,metadata.decimals,
                      metadata.fetch_status
               from observed_assets assets
               join erc20_token_metadata metadata
                 on metadata.chain_id=$2 and lower(metadata.token_address)=assets.token_address
               order by metadata.symbol nulls last,metadata.token_address"#,
        )
        .bind(cluster_id)
        .bind(chain_id as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(db_error)?;
        Ok(Some(json!({
            "contract_version":SEARCHER_CLUSTER_CONTRACT_VERSION,
            "chain_id":chain_id,
            "data_source":data_source,
            "seed":{
                "seed_address":seed_address,
                "seed_role":cluster.get::<Option<String>,_>("seed_role"),
                "seed_role_confidence":cluster.get::<Option<f64>,_>("seed_role_confidence"),
                "seed_role_evidence":evidence_preview(&cluster.get::<Value,_>("seed_role_evidence"),12),
                "role_semantics":"candidate; ownership and same-team control are not asserted"
            },
            "cluster":{
                "cluster_id":cluster_id,
                "kind":cluster.get::<String,_>("cluster_kind"),
                "status":cluster.get::<String,_>("current_status"),
                "progress":cluster.get::<Value,_>("progress"),
                "checkpoint":cluster.get::<Value,_>("checkpoint"),
                "analysis_started_at":cluster.get::<Option<DateTime<Utc>>,_>("analysis_started_at"),
                "analysis_completed_at":cluster.get::<Option<DateTime<Utc>>,_>("analysis_completed_at"),
                "last_processed_block":cluster.get::<Option<i64>,_>("last_processed_block"),
                "last_incremental_update":cluster.get::<Option<DateTime<Utc>>,_>("last_incremental_update"),
                "last_activity_at":cluster.get::<Option<DateTime<Utc>>,_>("last_activity_at"),
                "coverage_start":cluster.get::<Option<DateTime<Utc>>,_>("coverage_start_at"),
                "coverage_end":cluster.get::<Option<DateTime<Utc>>,_>("coverage_end_at"),
                "coverage_is_partial":cluster.get::<bool,_>("coverage_is_partial"),
                "error":cluster.get::<Option<String>,_>("error"),
                "retry_count":cluster.get::<i32,_>("retry_count")
            },
            "members":members.iter().map(cluster_member_json).collect::<Vec<_>>(),
            "relationships":relationships.iter().map(cluster_relationship_json).collect::<Vec<_>>(),
            "windows":windows.iter().map(cluster_window_json).collect::<Vec<_>>(),
            "partial_24h":{
                "executions":partial_24h.get::<i64,_>("execution_count"),
                "successes":partial_24h.get::<i64,_>("success_count"),
                "failures":partial_24h.get::<i64,_>("failure_count"),
                "gas_cost_raw":partial_24h.get::<String,_>("gas_cost_raw"),
                "basic_reconstruction_count":partial_24h.get::<i64,_>("basic_count"),
                "economically_verified_count":partial_24h.get::<i64,_>("verified_count"),
                "last_active":partial_24h.get::<Option<DateTime<Utc>>,_>("last_active_at"),
                "status":if cluster.get::<String,_>("current_status")=="ready" {"complete"} else {"partial"}
            },
            "executions":executions.iter().map(cluster_execution_json).collect::<Vec<_>>(),
            "execution_page":{
                "offset":execution_offset,
                "limit":execution_limit.clamp(1,500),
                "returned":executions.len(),
                "total":execution_total,
                "has_more":execution_offset.saturating_add(executions.len() as u64)<execution_total as u64
            },
            "changes":changes.iter().map(cluster_change_json).collect::<Vec<_>>(),
            "economic_limitations":exclusions.iter().map(|row| json!({
                "reason":row.get::<String,_>("exclusion_reason"),
                "transaction_count":row.get::<i64,_>("count")
            })).collect::<Vec<_>>(),
            "assets":assets.iter().map(|row| json!({
                "address":row.get::<String,_>("token_address"),
                "symbol":row.get::<Option<String>,_>("symbol"),
                "name":row.get::<Option<String>,_>("name"),
                "decimals":row.get::<Option<i32>,_>("decimals"),
                "status":row.get::<String,_>("fetch_status")
            })).collect::<Vec<_>>(),
            "limitations":[
                "All cluster memberships are candidates unless manually included; no ownership or same-team conclusion is made.",
                "Observed settlement inflow is reported separately and is not necessarily profit.",
                "Funding movements are excluded from verified execution profit.",
                "Verified observed result is not complete wallet or real-world entity PnL."
            ]
        })))
    }

    pub async fn override_member(
        &self,
        chain_id: u64,
        seed_address: &str,
        member_address: &str,
        request: ClusterMemberOverride,
    ) -> LedgerResult<()> {
        self.data_source_status(chain_id).await?;
        let seed_address = normalize_address(seed_address)?;
        let member_address = normalize_address(member_address)?;
        if !matches!(
            request.action.as_str(),
            "include" | "exclude" | "mark_role" | "keep_candidate"
        ) {
            return Err(LedgerError::MalformedEvidence(
                "invalid candidate cluster member action".to_owned(),
            ));
        }
        if request.action == "mark_role"
            && request
                .role
                .as_deref()
                .is_none_or(|role| !valid_member_role(role))
        {
            return Err(LedgerError::MalformedEvidence(
                "mark_role requires a supported candidate role".to_owned(),
            ));
        }
        let mut tx = self.pool.begin().await.map_err(db_error)?;
        let row = sqlx::query(
            r#"select members.cluster_id,members.candidate_role,members.membership_state
               from candidate_execution_cluster_members members
               join candidate_execution_clusters clusters on clusters.cluster_id=members.cluster_id
               where clusters.chain_id=$1 and clusters.seed_address=$2 and members.address=$3
               for update"#,
        )
        .bind(chain_id as i64)
        .bind(&seed_address)
        .bind(&member_address)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db_error)?
        .ok_or_else(|| LedgerError::NotFound("candidate cluster member not found".to_owned()))?;
        let cluster_id: Uuid = row.get("cluster_id");
        let previous_role: String = row.get("candidate_role");
        let previous_state: String = row.get("membership_state");
        let selected_role = request
            .role
            .clone()
            .unwrap_or_else(|| previous_role.clone());
        let selected_state = match request.action.as_str() {
            "include" => "included",
            "exclude" => "excluded",
            "keep_candidate" | "mark_role" => "candidate",
            _ => unreachable!(),
        };
        let active = selected_state != "excluded";
        let economic = active
            && matches!(
                selected_role.as_str(),
                "seed"
                    | "worker_candidate"
                    | "executor_candidate"
                    | "settlement_recipient_candidate"
                    | "profit_recipient_candidate"
            );
        sqlx::query(
            r#"update candidate_execution_cluster_members
               set candidate_role=$3,membership_state=$4,user_overridden=true,override_note=$5,
                   include_in_economic_scope=$6,include_in_behavior_scope=$7,updated_at=now()
               where cluster_id=$1 and address=$2"#,
        )
        .bind(cluster_id)
        .bind(&member_address)
        .bind(&selected_role)
        .bind(selected_state)
        .bind(request.note.as_deref())
        .bind(economic)
        .bind(active)
        .execute(&mut *tx)
        .await
        .map_err(db_error)?;
        sqlx::query(
            r#"insert into candidate_cluster_member_overrides (
                 cluster_id,chain_id,address,action,previous_role,selected_role,
                 previous_membership_state,selected_membership_state,note,actor_user_id
               ) values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)"#,
        )
        .bind(cluster_id)
        .bind(chain_id as i64)
        .bind(member_address)
        .bind(&request.action)
        .bind(previous_role)
        .bind(selected_role)
        .bind(previous_state)
        .bind(selected_state)
        .bind(request.note)
        .bind(request.actor_user_id)
        .execute(&mut *tx)
        .await
        .map_err(db_error)?;
        sqlx::query(
            r#"update candidate_execution_clusters
               set current_status='economic_verification',progress=$2,
                   available_at=now(),claimed_at=null,updated_at=now()
               where cluster_id=$1"#,
        )
        .bind(cluster_id)
        .bind(progress(
            70,
            "economic_verification",
            "Recomputing economics after a member correction",
        ))
        .execute(&mut *tx)
        .await
        .map_err(db_error)?;
        tx.commit().await.map_err(db_error)?;
        Ok(())
    }
}

fn source_unavailable(chain_id: u64) -> LedgerError {
    let name = match chain_id {
        8453 => "Base".to_owned(),
        4663 => "Robinhood Chain".to_owned(),
        other => format!("Chain {other}"),
    };
    LedgerError::TransientIngestion(format!(
        "{name} live intelligence is not available on the current data source"
    ))
}

fn normalize_address(address: &str) -> LedgerResult<String> {
    let address = address.trim().to_ascii_lowercase();
    if address.len() != 42
        || !address.starts_with("0x")
        || !address[2..].chars().all(|value| value.is_ascii_hexdigit())
    {
        return Err(LedgerError::MalformedEvidence(format!(
            "invalid EVM address {address}"
        )));
    }
    Ok(address)
}

fn progress(percent: u8, stage: &str, message: &str) -> Value {
    json!({"percent":percent,"stage":stage,"message":message})
}

fn execution_inputs(rows: Vec<sqlx::postgres::PgRow>) -> Vec<ExecutionInput> {
    rows.into_iter()
        .map(|row| ExecutionInput {
            tx_hash: row.get("tx_hash"),
            block_number: row.get::<i64, _>("block_number").max(0) as u64,
            block_timestamp: row.get("block_timestamp"),
            from_address: row.get::<Option<String>, _>("from_address"),
            to_address: row.get::<Option<String>, _>("to_address"),
            seed_token_inflows: row.get("token_inflows"),
            seed_token_outflows: row.get("token_outflows"),
            seed_protocol_relationships: row.get("protocol_relationships"),
            seed_touched_pools: row.get("touched_pools"),
        })
        .collect()
}

fn db_error(error: sqlx::Error) -> LedgerError {
    LedgerError::Persistence(error.to_string())
}

fn json_array(value: &Value) -> &[Value] {
    value.as_array().map(Vec::as_slice).unwrap_or(&[])
}

fn ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        (numerator as f64 / denominator as f64).clamp(0.0, 1.0)
    }
}

fn bounded_confidence(base: f64, evidence_count: u64, maximum: f64) -> f64 {
    (base + (evidence_count.max(1) as f64).log10() * 0.08).min(maximum)
}

fn relationship_confidence(base: f64, count: u64, relationship_type: &str) -> f64 {
    let ceiling = if matches!(relationship_type, "pool" | "router" | "protocol") {
        1.0
    } else {
        0.98
    };
    bounded_confidence(base, count, ceiling)
}

fn is_execution_action(action: &str) -> bool {
    matches!(
        action,
        "dex_swap"
            | "multi_swap"
            | "dex_arbitrage_candidate"
            | "verified_dex_arbitrage"
            | "liquidation"
            | "liquidation_attempt"
            | "failed_execution"
    )
}

#[allow(clippy::too_many_arguments)]
fn observe_relationship(
    relations: &mut BTreeMap<(String, String, String), RelationshipEvidence>,
    source: &str,
    related: &str,
    relationship_type: &str,
    tx_hash: &str,
    timestamp: Option<DateTime<Utc>>,
    confidence: f64,
    evidence_state: &str,
    amount: Option<(String, BigInt)>,
    reason: &str,
    depth: i16,
) {
    if source.eq_ignore_ascii_case(related) {
        return;
    }
    let source = source.to_ascii_lowercase();
    let related = related.to_ascii_lowercase();
    let key = (
        source.clone(),
        related.clone(),
        relationship_type.to_owned(),
    );
    let entry = relations
        .entry(key)
        .or_insert_with(|| RelationshipEvidence {
            source_address: source,
            related_address: related,
            relationship_type: relationship_type.to_owned(),
            evidence_state: evidence_state.to_owned(),
            depth,
            ..Default::default()
        });
    entry.interaction_count = entry.interaction_count.saturating_add(1);
    entry.tx_hashes.insert(tx_hash.to_owned());
    entry.confidence = entry.confidence.max(confidence);
    entry.reason_facts.insert(reason.to_owned());
    entry.first_seen = min_time(entry.first_seen, timestamp);
    entry.last_seen = max_time(entry.last_seen, timestamp);
    entry.depth = entry.depth.min(depth);
    if let Some((asset, amount)) = amount {
        *entry.values.entry(asset).or_default() += amount;
    }
}

fn flow_amount(flow: &Value) -> Option<(String, BigInt)> {
    let asset = flow
        .get("token")
        .or_else(|| flow.get("token_address"))
        .and_then(Value::as_str)
        .map(str::to_ascii_lowercase)
        .unwrap_or_else(|| "native".to_owned());
    let amount = flow
        .get("amount_raw")
        .and_then(Value::as_str)
        .and_then(decimal_bigint)?;
    Some((asset, amount))
}

fn amount_map_json(values: &BTreeMap<String, BigInt>) -> Value {
    json!(
        values
            .iter()
            .map(|(asset, amount)| json!({"asset":asset,"amount_raw":amount.to_string()}))
            .collect::<Vec<_>>()
    )
}

fn subtract_amount_maps(
    left: &BTreeMap<String, BigInt>,
    right: &BTreeMap<String, BigInt>,
) -> BTreeMap<String, BigInt> {
    let mut result = left.clone();
    for (asset, amount) in right {
        *result.entry(asset.clone()).or_default() -= amount;
    }
    result
}

fn min_time(left: Option<DateTime<Utc>>, right: Option<DateTime<Utc>>) -> Option<DateTime<Utc>> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (left, right) => left.or(right),
    }
}

fn max_time(left: Option<DateTime<Utc>>, right: Option<DateTime<Utc>>) -> Option<DateTime<Utc>> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (left, right) => left.or(right),
    }
}

fn role_precedence(role: &str) -> u8 {
    match role {
        "executor_candidate" => 5,
        "worker_candidate" => 4,
        "settlement_recipient_candidate" | "profit_recipient_candidate" => 3,
        "funding_treasury_candidate" => 2,
        _ => 1,
    }
}

fn valid_member_role(role: &str) -> bool {
    matches!(
        role,
        "seed"
            | "funding_treasury_candidate"
            | "worker_candidate"
            | "executor_candidate"
            | "settlement_recipient_candidate"
            | "profit_recipient_candidate"
            | "operational_contract_candidate"
            | "unresolved"
    )
}

/// The shared verifier intentionally exposes only a single-token legacy profit
/// field. A cluster execution can finish with more than one positive asset.
/// Promote that result only after the shared verifier has already reconciled
/// receipt logs, archive balances, trace-native flow, exact gas, swap-pool
/// count, and same-block isolation, and only when every scoped asset delta is
/// non-negative. The result remains an asset vector; no cross-asset total is
/// invented.
fn reconciled_cluster_surpluses(verification: &Value) -> Option<Vec<Value>> {
    if verification.get("reason").and_then(Value::as_str)
        != Some("scope does not have one unambiguous token surplus with no principal outflow")
    {
        return None;
    }
    let details = verification.get("details")?;
    if details.get("trace_native_net_raw").and_then(Value::as_str) != Some("0") {
        return None;
    }
    if details
        .get("negative_token_nets")
        .and_then(Value::as_object)
        .is_none_or(|values| !values.is_empty())
    {
        return None;
    }
    let mut result = details
        .get("token_nets")
        .and_then(Value::as_object)?
        .iter()
        .filter_map(|(asset, raw)| {
            let raw = raw.as_str()?;
            let amount = decimal_bigint(raw)?;
            (amount > BigInt::default())
                .then(|| json!({"asset":asset.to_ascii_lowercase(),"amount_raw":raw}))
        })
        .collect::<Vec<_>>();
    result.sort_by(|left, right| {
        left.get("asset")
            .and_then(Value::as_str)
            .cmp(&right.get("asset").and_then(Value::as_str))
    });
    (!result.is_empty()).then_some(result)
}

fn classify_cluster_execution(
    analysis: &TransactionAnalysis,
    seed_address: &str,
    scope: &BTreeSet<String>,
) -> &'static str {
    if analysis.status == TransactionStatus::Failure {
        return "failed_execution";
    }
    if analysis.to_address.is_none() {
        return "contract_deployment";
    }
    if analysis.normalized_execution_facts.selector.as_deref() == Some("0x095ea7b3") {
        return "approval";
    }
    let combined = format!(
        "{} {}",
        analysis
            .recognizer_results
            .iter()
            .map(|result| result.label.to_ascii_lowercase())
            .collect::<Vec<_>>()
            .join(" "),
        analysis.scenario_outputs.to_string().to_ascii_lowercase()
    );
    if combined.contains("liquidation") && combined.contains("attempt") {
        return "liquidation_attempt";
    }
    if combined.contains("liquidation") {
        return "liquidation";
    }
    if combined.contains("arbitrage") {
        return "closed_cycle_arbitrage";
    }
    if combined.contains("multi") && combined.contains("swap") {
        return "multi_pool_swap";
    }
    if combined.contains("swap") {
        return "dex_swap";
    }
    let seed_inflow = analysis.derived_flows.iter().any(|flow| {
        flow.to_address.eq_ignore_ascii_case(seed_address)
            && !flow.from_address.eq_ignore_ascii_case(seed_address)
    });
    let seed_outflow = analysis.derived_flows.iter().any(|flow| {
        flow.from_address.eq_ignore_ascii_case(seed_address)
            && !flow.to_address.eq_ignore_ascii_case(seed_address)
    });
    if seed_outflow {
        return "funding";
    }
    if seed_inflow {
        return "settlement";
    }
    if analysis.derived_flows.iter().any(|flow| {
        scope.contains(&flow.from_address.to_ascii_lowercase())
            || scope.contains(&flow.to_address.to_ascii_lowercase())
    }) {
        return "rebalance";
    }
    "unknown_interaction"
}

fn scoped_assets(
    analysis: &TransactionAnalysis,
    scope: &BTreeSet<String>,
) -> (Vec<Value>, Vec<Value>) {
    let mut inputs = Vec::new();
    let mut outputs = Vec::new();
    for flow in &analysis.derived_flows {
        let from_scope = scope.contains(&flow.from_address.to_ascii_lowercase());
        let to_scope = scope.contains(&flow.to_address.to_ascii_lowercase());
        if from_scope && !to_scope {
            inputs.push(flow_json(flow));
        }
        if to_scope && !from_scope {
            outputs.push(flow_json(flow));
        }
    }
    (inputs, outputs)
}

fn flow_json(flow: &crate::domain::value_objects::AssetFlow) -> Value {
    let asset = match &flow.asset_type {
        AssetType::Native => "native".to_owned(),
        _ => flow
            .token_address
            .clone()
            .unwrap_or_else(|| "unknown".to_owned()),
    };
    json!({
        "asset":asset,
        "asset_type":format!("{:?}",flow.asset_type).to_ascii_lowercase(),
        "amount_raw":flow.amount_raw,
        "from":flow.from_address,
        "to":flow.to_address,
        "confidence":flow.confidence,
        "derivation_method":flow.derivation_method,
        "evidence":flow.evidence_refs
    })
}

fn unique_protocols(value: &Value) -> Vec<String> {
    let mut protocols = BTreeSet::new();
    for item in json_array(value) {
        if let Some(protocol) = item
            .get("protocol_name")
            .or_else(|| item.get("protocol"))
            .and_then(Value::as_str)
        {
            protocols.insert(protocol.to_owned());
        }
    }
    protocols.into_iter().collect()
}

fn unique_strings(value: &Value) -> Vec<String> {
    let mut values = BTreeSet::new();
    for item in json_array(value) {
        if let Some(value) = item
            .as_str()
            .or_else(|| item.get("address").and_then(Value::as_str))
        {
            values.insert(value.to_owned());
        }
    }
    values.into_iter().collect()
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

fn transaction_status(status: &TransactionStatus) -> &'static str {
    match status {
        TransactionStatus::Success => "success",
        TransactionStatus::Failure => "failure",
        TransactionStatus::Pending => "pending",
    }
}

fn decimal_bigint(value: &str) -> Option<BigInt> {
    BigInt::parse_bytes(value.as_bytes(), 10)
}

fn hex_bigint(value: Option<&str>) -> Option<BigInt> {
    let value = value?.trim().trim_start_matches("0x");
    if value.is_empty() {
        return Some(BigInt::default());
    }
    BigInt::parse_bytes(value.as_bytes(), 16)
}

fn receipt_gas_cost(receipt: &Value) -> Option<String> {
    let gas_used = hex_bigint(receipt.get("gasUsed").and_then(Value::as_str))?;
    let gas_price = hex_bigint(receipt.get("effectiveGasPrice").and_then(Value::as_str))?;
    let l1_fee = hex_bigint(receipt.get("l1Fee").and_then(Value::as_str)).unwrap_or_default();
    Some((gas_used * gas_price + l1_fee).to_string())
}

fn wrapped_native_address(chain_id: u64) -> Option<&'static str> {
    match chain_id {
        8453 => Some("0x4200000000000000000000000000000000000006"),
        _ => None,
    }
}

fn digest_json(value: &Value) -> String {
    let bytes = serde_json::to_vec(value).unwrap_or_default();
    format!("{:x}", Sha256::digest(bytes))
}

fn aggregate_execution(
    aggregate: &mut WindowAccumulator,
    row: &sqlx::postgres::PgRow,
    timestamp: DateTime<Utc>,
) {
    aggregate.execution_count += 1;
    aggregate.last_active = max_time(aggregate.last_active, Some(timestamp));
    let tx_hash: String = row.get("tx_hash");
    if aggregate.evidence_hashes.len() < MAX_EVIDENCE_HASHES {
        aggregate.evidence_hashes.push(tx_hash);
    }
    let status: String = row.get("transaction_status");
    if status == "success" {
        aggregate.success_count += 1;
    } else if status == "failure" {
        aggregate.failure_count += 1;
        aggregate.failed_count += 1;
    }
    let gas = row
        .get::<Option<String>, _>("gas_cost_raw")
        .as_deref()
        .and_then(decimal_bigint)
        .unwrap_or_default();
    aggregate.gas_cost += &gas;
    if status == "failure" {
        aggregate.failed_gas += gas;
    }
    let execution_type: String = row.get("execution_type");
    *aggregate
        .strategies
        .entry(execution_type.clone())
        .or_default() += 1;
    if let Some(worker) = row.get::<Option<String>, _>("worker_address") {
        *aggregate.workers.entry(worker).or_default() += 1;
    }
    if let Some(executor) = row.get::<Option<String>, _>("executor_address") {
        *aggregate.executors.entry(executor).or_default() += 1;
    }
    for protocol in json_array(&row.get::<Value, _>("protocols")) {
        if let Some(protocol) = protocol.as_str() {
            *aggregate.protocols.entry(protocol.to_owned()).or_default() += 1;
        }
    }
    for pool in json_array(&row.get::<Value, _>("pools")) {
        if let Some(pool) = pool.as_str() {
            aggregate.pools.insert(pool.to_owned());
        }
    }
    add_amount_array(
        &mut aggregate.settlement_inflows,
        &row.get::<Value, _>("settlement_transfers"),
    );
    if execution_type == "funding" {
        add_amount_array(
            &mut aggregate.capital_sent,
            &row.get::<Value, _>("input_assets"),
        );
        add_amount_array(
            &mut aggregate.capital_returned,
            &row.get::<Value, _>("output_assets"),
        );
    }
    let economic_status: String = row.get("economic_verification_status");
    if economic_status == "verified" {
        aggregate.included += 1;
        aggregate.verified_count += 1;
        add_amount_array(
            &mut aggregate.verified_gross,
            &row.get::<Value, _>("verified_gross_profit"),
        );
        add_amount_array(
            &mut aggregate.verified_net,
            &row.get::<Value, _>("verified_net_result"),
        );
        aggregate.verified_gas += row
            .get::<Option<String>, _>("verified_gas_cost_raw")
            .as_deref()
            .and_then(decimal_bigint)
            .unwrap_or_default();
    } else if economic_status == "excluded" && is_economic_candidate(&execution_type) {
        aggregate.excluded += 1;
        let reason = row
            .get::<Option<String>, _>("exclusion_reason")
            .unwrap_or_else(|| "economic verification unavailable".to_owned());
        *aggregate.exclusion_reasons.entry(reason).or_default() += 1;
    } else if economic_status == "unknown" && is_economic_candidate(&execution_type) {
        aggregate.excluded += 1;
        let reason = if row.get::<String, _>("inputs_digest").starts_with("basic:") {
            "pending deep execution reconstruction"
        } else {
            "pending deterministic economic verification"
        };
        *aggregate
            .exclusion_reasons
            .entry(reason.to_owned())
            .or_default() += 1;
    }
}

fn is_economic_candidate(execution_type: &str) -> bool {
    matches!(
        execution_type,
        "dex_swap"
            | "multi_pool_swap"
            | "closed_cycle_arbitrage"
            | "liquidation"
            | "liquidation_attempt"
            | "failed_execution"
            | "unknown_interaction"
    )
}

fn add_amount_array(target: &mut BTreeMap<String, BigInt>, value: &Value) {
    for item in json_array(value) {
        let Some(asset) = item
            .get("asset")
            .or_else(|| item.get("token"))
            .and_then(Value::as_str)
        else {
            continue;
        };
        let Some(amount) = item
            .get("amount_raw")
            .and_then(Value::as_str)
            .and_then(decimal_bigint)
        else {
            continue;
        };
        *target.entry(asset.to_ascii_lowercase()).or_default() += amount;
    }
}

fn rank_counts(values: &HashMap<String, u64>, key: &str) -> Value {
    let mut values = values.iter().collect::<Vec<_>>();
    values.sort_by(|left, right| right.1.cmp(left.1).then_with(|| left.0.cmp(right.0)));
    Value::Array(
        values
            .into_iter()
            .take(20)
            .map(|(value, count)| {
                let mut item = serde_json::Map::new();
                item.insert(key.to_owned(), Value::String(value.clone()));
                item.insert("count".to_owned(), Value::from(*count));
                Value::Object(item)
            })
            .collect(),
    )
}

fn cluster_member_json(row: &sqlx::postgres::PgRow) -> Value {
    let supporting_transactions = row.get::<Vec<String>, _>("supporting_tx_hashes");
    let supporting_transaction_count = supporting_transactions.len();
    json!({
        "address":row.get::<String,_>("address"),
        "candidate_role":row.get::<String,_>("candidate_role"),
        "membership_state":row.get::<String,_>("membership_state"),
        "evidence_state":row.get::<String,_>("evidence_state"),
        "confidence":row.get::<f64,_>("membership_confidence"),
        "evidence":evidence_preview(&row.get::<Value,_>("evidence"),12),
        "supporting_transactions":supporting_transactions.into_iter().take(12).collect::<Vec<_>>(),
        "supporting_transaction_count":supporting_transaction_count,
        "include_in_economic_scope":row.get::<bool,_>("include_in_economic_scope"),
        "include_in_behavior_scope":row.get::<bool,_>("include_in_behavior_scope"),
        "discovery_depth":row.get::<i16,_>("discovery_depth"),
        "first_seen":row.get::<Option<DateTime<Utc>>,_>("first_seen_at"),
        "last_seen":row.get::<Option<DateTime<Utc>>,_>("last_seen_at"),
        "user_overridden":row.get::<bool,_>("user_overridden"),
        "override_note":row.get::<Option<String>,_>("override_note")
    })
}

fn cluster_relationship_json(row: &sqlx::postgres::PgRow) -> Value {
    let supporting_transactions = row.get::<Vec<String>, _>("supporting_tx_hashes");
    let supporting_transaction_count = supporting_transactions.len();
    json!({
        "relationship_id":row.get::<Uuid,_>("relationship_id"),
        "source_address":row.get::<String,_>("source_address"),
        "related_address":row.get::<String,_>("related_address"),
        "relationship_type":row.get::<String,_>("relationship_type"),
        "evidence_state":row.get::<String,_>("evidence_state"),
        "interaction_count":row.get::<i64,_>("interaction_count"),
        "value_transferred":row.get::<Value,_>("value_transferred"),
        "first_seen":row.get::<Option<DateTime<Utc>>,_>("first_seen_at"),
        "last_seen":row.get::<Option<DateTime<Utc>>,_>("last_seen_at"),
        "supporting_transactions":supporting_transactions.into_iter().take(12).collect::<Vec<_>>(),
        "supporting_transaction_count":supporting_transaction_count,
        "confidence":row.get::<f64,_>("confidence"),
        "evidence_reason":row.get::<String,_>("evidence_reason"),
        "evidence":evidence_preview(&row.get::<Value,_>("evidence"),12),
        "discovery_depth":row.get::<i16,_>("discovery_depth")
    })
}

fn cluster_window_json(row: &sqlx::postgres::PgRow) -> Value {
    json!({
        "window":row.get::<String,_>("window_key"),
        "window_start":row.get::<DateTime<Utc>,_>("window_start"),
        "window_end":row.get::<DateTime<Utc>,_>("window_end"),
        "executions":row.get::<i64,_>("execution_count"),
        "successes":row.get::<i64,_>("success_count"),
        "failures":row.get::<i64,_>("failure_count"),
        "protocol_count":row.get::<i64,_>("protocol_count"),
        "pool_count":row.get::<i64,_>("pool_count"),
        "gas_cost_raw":row.get::<Option<String>,_>("gas_cost_raw"),
        "verified_gross_profit":row.get::<Value,_>("verified_gross_profit"),
        "verified_gas_cost_raw":row.get::<Option<String>,_>("verified_gas_cost_raw"),
        "verified_net_result":row.get::<Value,_>("verified_net_result"),
        "verified_execution_count":row.get::<i64,_>("verified_execution_count"),
        "observed_settlement_inflows":row.get::<Value,_>("observed_settlement_inflows"),
        "capital_sent_to_workers":row.get::<Value,_>("capital_sent_to_workers"),
        "capital_returned":row.get::<Value,_>("capital_returned"),
        "net_funding_flow":row.get::<Value,_>("net_funding_flow"),
        "failed_tx_count":row.get::<i64,_>("failed_tx_count"),
        "failed_gas_cost_raw":row.get::<Option<String>,_>("failed_gas_cost_raw"),
        "strategy_distribution":row.get::<Value,_>("strategy_distribution"),
        "top_workers":row.get::<Value,_>("top_workers"),
        "top_executors":row.get::<Value,_>("top_executors"),
        "top_protocols":row.get::<Value,_>("top_protocols"),
        "new_relationship_count":row.get::<i64,_>("new_relationship_count"),
        "last_active":row.get::<Option<DateTime<Utc>>,_>("last_active_at"),
        "included_transactions":row.get::<i64,_>("included_transaction_count"),
        "excluded_transactions":row.get::<i64,_>("excluded_transaction_count"),
        "exclusion_reasons":row.get::<Value,_>("exclusion_reasons"),
        "coverage_ratio":row.get::<f64,_>("coverage_ratio"),
        "coverage":row.get::<Value,_>("coverage"),
        "inputs_digest":row.get::<String,_>("inputs_digest")
    })
}

fn cluster_execution_json(row: &sqlx::postgres::PgRow) -> Value {
    let evidence_refs = row.get::<Value, _>("evidence_refs");
    let evidence_ref_count = evidence_refs.as_array().map_or(0, Vec::len);
    json!({
        "tx_hash":row.get::<String,_>("tx_hash"),
        "block_number":row.get::<i64,_>("block_number"),
        "timestamp":row.get::<Option<DateTime<Utc>>,_>("block_timestamp"),
        "worker":row.get::<Option<String>,_>("worker_address"),
        "executor":row.get::<Option<String>,_>("executor_address"),
        "execution_type":row.get::<String,_>("execution_type"),
        "status":row.get::<String,_>("transaction_status"),
        "protocols":row.get::<Value,_>("protocols"),
        "pools":row.get::<Value,_>("pools"),
        "route":row.get::<Value,_>("route"),
        "input_assets":row.get::<Value,_>("input_assets"),
        "output_assets":row.get::<Value,_>("output_assets"),
        "gas_cost_raw":row.get::<Option<String>,_>("gas_cost_raw"),
        "settlement_transfers":row.get::<Value,_>("settlement_transfers"),
        "candidate_profit_recipient":row.get::<Option<String>,_>("candidate_profit_recipient"),
        "economic_verification_status":row.get::<String,_>("economic_verification_status"),
        "verified_gross_profit":row.get::<Value,_>("verified_gross_profit"),
        "verified_gas_cost_raw":row.get::<Option<String>,_>("verified_gas_cost_raw"),
        "verified_net_result":row.get::<Value,_>("verified_net_result"),
        "exclusion_reason":row.get::<Option<String>,_>("exclusion_reason"),
        "evidence_refs":evidence_preview(&evidence_refs,16),
        "evidence_ref_count":evidence_ref_count,
        "inputs_digest":row.get::<String,_>("inputs_digest")
    })
}

fn evidence_preview(value: &Value, limit: usize) -> Value {
    match value {
        Value::Array(values) => Value::Array(
            values
                .iter()
                .take(limit)
                .map(|value| evidence_preview(value, limit))
                .collect(),
        ),
        Value::Object(values) => {
            let mut preview = serde_json::Map::new();
            for (key, value) in values {
                preview.insert(key.clone(), evidence_preview(value, limit));
                if let Value::Array(items) = value {
                    if items.len() > limit {
                        preview.insert(format!("{key}_total"), Value::from(items.len()));
                    }
                }
            }
            Value::Object(preview)
        }
        _ => value.clone(),
    }
}

fn is_seed_activity_pending(error: &LedgerError) -> bool {
    matches!(
        error,
        LedgerError::TransientIngestion(message)
            if message.contains("seed deterministic activity is not ready")
    )
}

fn cluster_change_json(row: &sqlx::postgres::PgRow) -> Value {
    json!({
        "signal_id":row.get::<Uuid,_>("signal_id"),
        "signal_type":row.get::<String,_>("signal_type"),
        "status":row.get::<String,_>("status"),
        "severity":row.get::<String,_>("severity"),
        "conclusion":row.get::<String,_>("conclusion"),
        "current_value":row.get::<Option<Value>,_>("current_value"),
        "baseline_value":row.get::<Option<Value>,_>("baseline_value"),
        "supporting_transactions":row.get::<Vec<String>,_>("supporting_tx_hashes"),
        "confidence":row.get::<f64,_>("confidence"),
        "inputs_digest":row.get::<String,_>("inputs_digest"),
        "detected_at":row.get::<DateTime<Utc>,_>("detected_at")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pool_and_router_are_not_valid_control_member_roles() {
        assert!(!valid_member_role("pool"));
        assert!(!valid_member_role("router"));
        assert!(!valid_member_role("protocol"));
        assert!(valid_member_role("worker_candidate"));
        assert!(valid_member_role("executor_candidate"));
    }

    #[test]
    fn funding_and_settlement_are_not_economic_profit_candidates() {
        assert!(!is_economic_candidate("funding"));
        assert!(!is_economic_candidate("settlement"));
        assert!(is_economic_candidate("closed_cycle_arbitrage"));
        assert!(is_economic_candidate("failed_execution"));
    }

    #[test]
    fn receipt_gas_includes_base_l1_fee() {
        let receipt = json!({
            "gasUsed":"0x5208",
            "effectiveGasPrice":"0x3b9aca00",
            "l1Fee":"0x64"
        });
        assert_eq!(
            receipt_gas_cost(&receipt).as_deref(),
            Some("21000000000100")
        );
    }

    #[test]
    fn relationship_evidence_keeps_unique_transactions_and_exact_value() {
        let mut relations = BTreeMap::new();
        for _ in 0..2 {
            observe_relationship(
                &mut relations,
                "0x0000000000000000000000000000000000000001",
                "0x0000000000000000000000000000000000000002",
                "funded_address",
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                None,
                0.8,
                "observed",
                Some(("native".to_owned(), BigInt::from(7))),
                "observed transfer",
                1,
            );
        }
        let relationship = relations.values().next().expect("relationship");
        assert_eq!(relationship.interaction_count, 2);
        assert_eq!(relationship.tx_hashes.len(), 1);
        assert_eq!(relationship.values["native"], BigInt::from(14));
    }

    #[test]
    fn evidence_preview_is_bounded_and_reports_original_array_size() {
        let value = json!({"supporting_tx_hashes":["a","b","c"],"nested":{"edges":[1,2,3]}});
        let preview = evidence_preview(&value, 2);
        assert_eq!(preview["supporting_tx_hashes"], json!(["a", "b"]));
        assert_eq!(preview["supporting_tx_hashes_total"], 3);
        assert_eq!(preview["nested"]["edges"], json!([1, 2]));
        assert_eq!(preview["nested"]["edges_total"], 3);
    }

    #[test]
    fn pending_seed_activity_is_deferred_instead_of_failed() {
        assert!(is_seed_activity_pending(&LedgerError::TransientIngestion(
            "seed deterministic activity is not ready; address backfill must complete first"
                .to_owned(),
        )));
        assert!(!is_seed_activity_pending(&LedgerError::TransientIngestion(
            "rpc unavailable".to_owned(),
        )));
    }

    #[test]
    fn reconciled_multi_asset_surplus_stays_a_vector() {
        let result = reconciled_cluster_surpluses(&json!({
            "status":"unavailable",
            "reason":"scope does not have one unambiguous token surplus with no principal outflow",
            "details":{
                "token_nets":{"0x0000000000000000000000000000000000000001":"7","0x0000000000000000000000000000000000000002":"11"},
                "negative_token_nets":{},
                "trace_native_net_raw":"0"
            }
        }))
        .expect("reconciled vector");
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn reconciled_surplus_rejects_principal_outflow() {
        assert!(
            reconciled_cluster_surpluses(&json!({
                "status":"unavailable",
                "reason":"scope does not have one unambiguous token surplus with no principal outflow",
                "details":{
                    "token_nets":{"0x0000000000000000000000000000000000000001":"7","0x0000000000000000000000000000000000000002":"-1"},
                    "negative_token_nets":{"0x0000000000000000000000000000000000000002":"-1"},
                    "trace_native_net_raw":"0"
                }
            }))
            .is_none()
        );
    }
}
