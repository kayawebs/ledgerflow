use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{PgPool, Row, postgres::PgPoolOptions, types::Json};
use uuid::Uuid;

use crate::core::errors::{LedgerError, LedgerResult};
use crate::domain::{
    entities::{
        AddressCluster, ClassifiedTransaction, ClusterAddress, ClusterAlert, ClusterBackfillRun,
        ClusterCoverage, ClusterDiscoveredTx, ClusterMetricsSnapshot, ClusterTrackingState,
        EvidencePack, TransactionAnalysis,
    },
    enums::{
        AlertSeverity, AlertType, BehaviorTag, ClusterBackfillStatus, ClusterTrackingStatus,
        ClusterTxStatus, MetricsWindow,
    },
    interfaces::AnalysisRepository,
};
use crate::ingestion::models::RawTransactionBundle;

#[derive(Clone)]
pub struct SqlxAnalysisRepository {
    pool: PgPool,
}

impl SqlxAnalysisRepository {
    pub async fn connect(database_url: &str) -> LedgerResult<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(database_url)
            .await
            .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        Ok(Self { pool })
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

#[async_trait]
impl AnalysisRepository for SqlxAnalysisRepository {
    async fn save_transaction_bundle(&self, bundle: &RawTransactionBundle) -> LedgerResult<()> {
        let tx_payload = serde_json::to_value(&bundle.tx)
            .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        sqlx::query(
            r#"
            insert into transactions (chain_id, tx_hash, block_number, from_address, to_address, tx_status, value_raw, input_data, payload)
            values ($1,$2,$3,$4,$5,$6,$7,$8,$9)
            on conflict (chain_id, tx_hash) do update set
              block_number = excluded.block_number,
              from_address = excluded.from_address,
              to_address = excluded.to_address,
              tx_status = excluded.tx_status,
              value_raw = excluded.value_raw,
              input_data = excluded.input_data,
              payload = excluded.payload,
              updated_at = now()
            "#,
        )
        .bind(bundle.tx.chain_id as i64)
        .bind(&bundle.tx.tx_hash)
        .bind(bundle.tx.block_number.map(|v| v as i64))
        .bind(&bundle.tx.from_address)
        .bind(&bundle.tx.to_address)
        .bind(if bundle.receipt.status == 1 { "success" } else { "failure" })
        .bind(&bundle.tx.value)
        .bind(&bundle.tx.input_data)
        .bind(Json(tx_payload))
        .execute(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;

        sqlx::query("delete from transaction_evidence where chain_id = $1 and tx_hash = $2")
            .bind(bundle.tx.chain_id as i64)
            .bind(&bundle.tx.tx_hash)
            .execute(&self.pool)
            .await
            .map_err(|err| LedgerError::Persistence(err.to_string()))?;

        let mut evidence_rows = vec![(
            "receipt".to_owned(),
            format!("{}:receipt", bundle.tx.tx_hash),
            serde_json::to_value(&bundle.receipt)
                .map_err(|err| LedgerError::Persistence(err.to_string()))?,
        )];
        if let Some(trace) = bundle.trace.as_ref() {
            evidence_rows.push((
                "trace".to_owned(),
                format!("{}:trace", bundle.tx.tx_hash),
                serde_json::to_value(trace)
                    .map_err(|err| LedgerError::Persistence(err.to_string()))?,
            ));
        }
        if let Some(code) = bundle.target_code.as_ref() {
            evidence_rows.push((
                "code".to_owned(),
                format!("{}:{}", code.address, code.block_number.unwrap_or_default()),
                serde_json::to_value(code)
                    .map_err(|err| LedgerError::Persistence(err.to_string()))?,
            ));
        }
        for (kind, reference, payload) in evidence_rows {
            sqlx::query(
                "insert into transaction_evidence (chain_id, tx_hash, kind, reference, payload) values ($1,$2,$3,$4,$5)",
            )
            .bind(bundle.tx.chain_id as i64)
            .bind(&bundle.tx.tx_hash)
            .bind(kind)
            .bind(reference)
            .bind(Json(payload))
            .execute(&self.pool)
            .await
            .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        }
        Ok(())
    }

    async fn save_analysis(&self, analysis: &TransactionAnalysis) -> LedgerResult<()> {
        let payload = serde_json::to_value(analysis)
            .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        let normalized = serde_json::to_value(&analysis.normalized_execution_facts)
            .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        let unresolved = serde_json::to_value(&analysis.unresolved_items)
            .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        let validations = serde_json::to_value(&analysis.validation_results)
            .map_err(|err| LedgerError::Persistence(err.to_string()))?;

        sqlx::query(
            r#"
            insert into transaction_analysis (
              analysis_id, chain_id, tx_hash, analysis_status, ruleset_id, pipeline_version, flow_derivation_version,
              config_version, input_digest, block_timestamp, from_address, to_address, tx_status,
              normalized_execution_facts, scenario_outputs, accounting_outputs,
              unresolved_items, validation_results, payload
            )
            values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)
            on conflict (chain_id, tx_hash) do update set
              analysis_id = excluded.analysis_id,
              analysis_status = excluded.analysis_status,
              ruleset_id = excluded.ruleset_id,
              pipeline_version = excluded.pipeline_version,
              flow_derivation_version = excluded.flow_derivation_version,
              config_version = excluded.config_version,
              input_digest = excluded.input_digest,
              block_timestamp = excluded.block_timestamp,
              from_address = excluded.from_address,
              to_address = excluded.to_address,
              tx_status = excluded.tx_status,
              normalized_execution_facts = excluded.normalized_execution_facts,
              scenario_outputs = excluded.scenario_outputs,
              accounting_outputs = excluded.accounting_outputs,
              unresolved_items = excluded.unresolved_items,
              validation_results = excluded.validation_results,
              payload = excluded.payload,
              updated_at = now()
            "#,
        )
        .bind(analysis.analysis_id)
        .bind(analysis.chain_id as i64)
        .bind(&analysis.tx_hash)
        .bind(serde_json::to_string(&analysis.analysis_status).unwrap_or_else(|_| "\"failed\"".to_owned()))
        .bind(&analysis.analysis_version_metadata.ruleset_id)
        .bind(&analysis.analysis_version_metadata.pipeline_version)
        .bind(&analysis.analysis_version_metadata.flow_derivation_version)
        .bind(&analysis.analysis_version_metadata.config_version)
        .bind(&analysis.input_digest)
        .bind(analysis.block_timestamp)
        .bind(&analysis.from_address)
        .bind(&analysis.to_address)
        .bind(serde_json::to_string(&analysis.status).unwrap_or_else(|_| "\"pending\"".to_owned()))
        .bind(Json(normalized))
        .bind(Json(analysis.scenario_outputs.clone()))
        .bind(Json(analysis.accounting_outputs.clone()))
        .bind(Json(unresolved))
        .bind(Json(validations))
        .bind(Json(payload))
        .execute(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;

        sqlx::query("delete from recognizer_results where chain_id = $1 and tx_hash = $2")
            .bind(analysis.chain_id as i64)
            .bind(&analysis.tx_hash)
            .execute(&self.pool)
            .await
            .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        sqlx::query("delete from flows where chain_id = $1 and tx_hash = $2")
            .bind(analysis.chain_id as i64)
            .bind(&analysis.tx_hash)
            .execute(&self.pool)
            .await
            .map_err(|err| LedgerError::Persistence(err.to_string()))?;

        for result in &analysis.recognizer_results {
            sqlx::query(
                r#"
                insert into recognizer_results (chain_id, tx_hash, recognizer_id, recognizer_version, label, confidence, payload)
                values ($1,$2,$3,$4,$5,$6,$7)
                "#,
            )
            .bind(analysis.chain_id as i64)
            .bind(&analysis.tx_hash)
            .bind(&result.recognizer_id)
            .bind(&result.recognizer_version)
            .bind(&result.label)
            .bind(result.confidence as f64)
            .bind(Json(serde_json::to_value(result).map_err(|err| LedgerError::Persistence(err.to_string()))?))
            .execute(&self.pool)
            .await
            .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        }

        for flow in &analysis.derived_flows {
            sqlx::query(
                r#"
                insert into flows (chain_id, tx_hash, flow_id, asset_type, token_address, from_address, to_address, amount_raw, derivation_method, confidence, payload)
                values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
                "#,
            )
            .bind(analysis.chain_id as i64)
            .bind(&analysis.tx_hash)
            .bind(&flow.flow_id)
            .bind(serde_json::to_string(&flow.asset_type).unwrap_or_else(|_| "\"native\"".to_owned()))
            .bind(&flow.token_address)
            .bind(&flow.from_address)
            .bind(&flow.to_address)
            .bind(&flow.amount_raw)
            .bind(&flow.derivation_method)
            .bind(flow.confidence as f64)
            .bind(Json(serde_json::to_value(flow).map_err(|err| LedgerError::Persistence(err.to_string()))?))
            .execute(&self.pool)
            .await
            .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        }
        Ok(())
    }

    async fn save_evidence_pack(&self, pack: &EvidencePack) -> LedgerResult<()> {
        sqlx::query(
            r#"
            insert into evidence_packs (
              pack_id, tx_hash, attempt_id, ruleset_id, input_digest, recognizer_set_versions,
              facts, labels, flows, unresolved, validation_results, metadata
            )
            values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
            on conflict (pack_id) do update set
              tx_hash = excluded.tx_hash,
              attempt_id = excluded.attempt_id,
              ruleset_id = excluded.ruleset_id,
              input_digest = excluded.input_digest,
              recognizer_set_versions = excluded.recognizer_set_versions,
              facts = excluded.facts,
              labels = excluded.labels,
              flows = excluded.flows,
              unresolved = excluded.unresolved,
              validation_results = excluded.validation_results,
              metadata = excluded.metadata,
              updated_at = now()
            "#,
        )
        .bind(pack.pack_id)
        .bind(&pack.tx_hash)
        .bind(pack.attempt_id)
        .bind(&pack.ruleset_id)
        .bind(&pack.input_digest)
        .bind(Json(
            serde_json::to_value(&pack.recognizer_set_versions).unwrap_or_default(),
        ))
        .bind(Json(pack.facts.clone()))
        .bind(Json(Value::Array(pack.labels.clone())))
        .bind(Json(Value::Array(pack.flows.clone())))
        .bind(Json(Value::Array(pack.unresolved.clone())))
        .bind(Json(Value::Array(pack.validation_results.clone())))
        .bind(Json(pack.metadata.clone()))
        .execute(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        Ok(())
    }

    async fn get_analysis(
        &self,
        chain_id: u64,
        tx_hash: &str,
    ) -> LedgerResult<Option<TransactionAnalysis>> {
        let row = sqlx::query(
            "select payload from transaction_analysis where chain_id = $1 and tx_hash = $2",
        )
        .bind(chain_id as i64)
        .bind(tx_hash)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        let Some(row) = row else { return Ok(None) };
        let Json(payload): Json<Value> = row
            .try_get("payload")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        serde_json::from_value(payload)
            .map(Some)
            .map_err(|err| LedgerError::Persistence(err.to_string()))
    }

    async fn get_evidence_pack(&self, pack_id: &str) -> LedgerResult<Option<EvidencePack>> {
        let row = sqlx::query(
            "select pack_id, tx_hash, attempt_id, ruleset_id, input_digest, recognizer_set_versions, facts, labels, flows, unresolved, validation_results, metadata from evidence_packs where pack_id = $1"
        )
        .bind(pack_id.parse::<uuid::Uuid>().map_err(|err| LedgerError::Persistence(err.to_string()))?)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        row.map(row_to_pack).transpose()
    }

    async fn get_evidence_pack_by_tx(
        &self,
        _chain_id: u64,
        tx_hash: &str,
    ) -> LedgerResult<Option<EvidencePack>> {
        let row = sqlx::query(
            "select pack_id, tx_hash, attempt_id, ruleset_id, input_digest, recognizer_set_versions, facts, labels, flows, unresolved, validation_results, metadata from evidence_packs where tx_hash = $1 order by created_at desc limit 1"
        )
        .bind(tx_hash)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        row.map(row_to_pack).transpose()
    }

    async fn create_cluster(&self, cluster: &AddressCluster) -> LedgerResult<()> {
        sqlx::query(
            r#"
            insert into address_clusters (cluster_id, chain_id, owner_label, name, description, metadata)
            values ($1,$2,$3,$4,$5,$6)
            on conflict (cluster_id) do update set
              chain_id = excluded.chain_id,
              owner_label = excluded.owner_label,
              name = excluded.name,
              description = excluded.description,
              metadata = excluded.metadata,
              updated_at = now()
            "#,
        )
        .bind(cluster.cluster_id)
        .bind(cluster.chain_id as i64)
        .bind(&cluster.owner_label)
        .bind(&cluster.name)
        .bind(&cluster.description)
        .bind(Json(cluster.metadata.clone()))
        .execute(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        Ok(())
    }

    async fn replace_cluster_addresses(
        &self,
        cluster_id: Uuid,
        addresses: &[ClusterAddress],
    ) -> LedgerResult<()> {
        sqlx::query("delete from cluster_addresses where cluster_id = $1")
            .bind(cluster_id)
            .execute(&self.pool)
            .await
            .map_err(|err| LedgerError::Persistence(err.to_string()))?;

        for address in addresses {
            sqlx::query(
                r#"
                insert into cluster_addresses (cluster_id, address, label, role, metadata)
                values ($1,$2,$3,$4,$5)
                "#,
            )
            .bind(cluster_id)
            .bind(&address.address)
            .bind(&address.label)
            .bind(&address.role)
            .bind(Json(address.metadata.clone()))
            .execute(&self.pool)
            .await
            .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        }
        Ok(())
    }

    async fn get_cluster(&self, cluster_id: Uuid) -> LedgerResult<Option<AddressCluster>> {
        let cluster_row = sqlx::query(
            "select cluster_id, chain_id, owner_label, name, description, metadata from address_clusters where cluster_id = $1",
        )
        .bind(cluster_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        let Some(cluster_row) = cluster_row else {
            return Ok(None);
        };

        let address_rows = sqlx::query(
            "select address, label, role, metadata from cluster_addresses where cluster_id = $1 order by address asc",
        )
        .bind(cluster_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        let addresses = address_rows
            .into_iter()
            .map(|row| {
                let Json(metadata): Json<Value> = row
                    .try_get("metadata")
                    .map_err(|err| LedgerError::Persistence(err.to_string()))?;
                Ok(ClusterAddress {
                    address: row
                        .try_get("address")
                        .map_err(|err| LedgerError::Persistence(err.to_string()))?,
                    label: row
                        .try_get("label")
                        .map_err(|err| LedgerError::Persistence(err.to_string()))?,
                    role: row
                        .try_get("role")
                        .map_err(|err| LedgerError::Persistence(err.to_string()))?,
                    metadata,
                })
            })
            .collect::<LedgerResult<Vec<_>>>()?;

        let Json(metadata): Json<Value> = cluster_row
            .try_get("metadata")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?;

        Ok(Some(AddressCluster {
            cluster_id: cluster_row
                .try_get("cluster_id")
                .map_err(|err| LedgerError::Persistence(err.to_string()))?,
            chain_id: cluster_row
                .try_get::<i64, _>("chain_id")
                .map_err(|err| LedgerError::Persistence(err.to_string()))?
                as u64,
            owner_label: cluster_row
                .try_get("owner_label")
                .map_err(|err| LedgerError::Persistence(err.to_string()))?,
            name: cluster_row
                .try_get("name")
                .map_err(|err| LedgerError::Persistence(err.to_string()))?,
            description: cluster_row
                .try_get("description")
                .map_err(|err| LedgerError::Persistence(err.to_string()))?,
            addresses,
            metadata,
        }))
    }

    async fn list_clusters(&self) -> LedgerResult<Vec<AddressCluster>> {
        let rows = sqlx::query(
            r#"
            select
              c.cluster_id,
              c.chain_id,
              c.owner_label,
              c.name,
              c.description,
              c.metadata,
              coalesce(
                json_agg(
                  json_build_object(
                    'address', a.address,
                    'label', a.label,
                    'role', a.role,
                    'metadata', a.metadata
                  )
                  order by a.address asc
                ) filter (where a.address is not null),
                '[]'::json
              ) as addresses
            from address_clusters c
            left join cluster_addresses a on a.cluster_id = c.cluster_id
            group by c.cluster_id, c.chain_id, c.owner_label, c.name, c.description, c.metadata
            order by c.created_at asc
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;

        rows.into_iter()
            .map(|row| {
                let Json(metadata): Json<Value> = row
                    .try_get("metadata")
                    .map_err(|err| LedgerError::Persistence(err.to_string()))?;
                let Json(addresses): Json<Vec<ClusterAddress>> = row
                    .try_get("addresses")
                    .map_err(|err| LedgerError::Persistence(err.to_string()))?;
                Ok(AddressCluster {
                    cluster_id: row
                        .try_get("cluster_id")
                        .map_err(|err| LedgerError::Persistence(err.to_string()))?,
                    chain_id: row
                        .try_get::<i64, _>("chain_id")
                        .map_err(|err| LedgerError::Persistence(err.to_string()))?
                        as u64,
                    owner_label: row
                        .try_get("owner_label")
                        .map_err(|err| LedgerError::Persistence(err.to_string()))?,
                    name: row
                        .try_get("name")
                        .map_err(|err| LedgerError::Persistence(err.to_string()))?,
                    description: row
                        .try_get("description")
                        .map_err(|err| LedgerError::Persistence(err.to_string()))?,
                    addresses,
                    metadata,
                })
            })
            .collect()
    }

    async fn list_analyses_for_cluster(
        &self,
        chain_id: u64,
        addresses: &[String],
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
    ) -> LedgerResult<Vec<TransactionAnalysis>> {
        let rows = sqlx::query(
            r#"
            select payload
            from transaction_analysis
            where chain_id = $1
              and (from_address = any($2) or to_address = any($2))
              and ($3::timestamptz is null or block_timestamp >= $3)
              and ($4::timestamptz is null or block_timestamp <= $4)
            order by block_timestamp asc nulls last, created_at asc
            "#,
        )
        .bind(chain_id as i64)
        .bind(addresses)
        .bind(start)
        .bind(end)
        .fetch_all(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        rows.into_iter()
            .map(|row| {
                let Json(payload): Json<Value> = row
                    .try_get("payload")
                    .map_err(|err| LedgerError::Persistence(err.to_string()))?;
                serde_json::from_value(payload)
                    .map_err(|err| LedgerError::Persistence(err.to_string()))
            })
            .collect()
    }

    async fn create_cluster_backfill_run(&self, run: &ClusterBackfillRun) -> LedgerResult<()> {
        sqlx::query(
            r#"
            insert into cluster_backfill_runs (
              run_id, cluster_id, chain_id, start_block, end_block, status, requested_at,
              started_at, finished_at, address_count, discovered_tx_count, analyzed_tx_count,
              failed_tx_count, metadata
            )
            values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
            on conflict (run_id) do update set
              cluster_id = excluded.cluster_id,
              chain_id = excluded.chain_id,
              start_block = excluded.start_block,
              end_block = excluded.end_block,
              status = excluded.status,
              requested_at = excluded.requested_at,
              started_at = excluded.started_at,
              finished_at = excluded.finished_at,
              address_count = excluded.address_count,
              discovered_tx_count = excluded.discovered_tx_count,
              analyzed_tx_count = excluded.analyzed_tx_count,
              failed_tx_count = excluded.failed_tx_count,
              metadata = excluded.metadata
            "#,
        )
        .bind(run.run_id)
        .bind(run.cluster_id)
        .bind(run.chain_id as i64)
        .bind(run.start_block.map(|value| value as i64))
        .bind(run.end_block.map(|value| value as i64))
        .bind(cluster_backfill_status_to_str(&run.status))
        .bind(run.requested_at)
        .bind(run.started_at)
        .bind(run.finished_at)
        .bind(run.address_count as i32)
        .bind(run.discovered_tx_count as i32)
        .bind(run.analyzed_tx_count as i32)
        .bind(run.failed_tx_count as i32)
        .bind(Json(run.metadata.clone()))
        .execute(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        Ok(())
    }

    async fn update_cluster_backfill_run(&self, run: &ClusterBackfillRun) -> LedgerResult<()> {
        self.create_cluster_backfill_run(run).await
    }

    async fn list_cluster_backfill_runs(
        &self,
        cluster_id: Uuid,
    ) -> LedgerResult<Vec<ClusterBackfillRun>> {
        let rows = sqlx::query(
            r#"
            select run_id, cluster_id, chain_id, start_block, end_block, status, requested_at,
                   started_at, finished_at, address_count, discovered_tx_count, analyzed_tx_count,
                   failed_tx_count, metadata
            from cluster_backfill_runs
            where cluster_id = $1
            order by requested_at desc
            "#,
        )
        .bind(cluster_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        rows.into_iter().map(row_to_backfill_run).collect()
    }

    async fn claim_next_pending_cluster_backfill_run(
        &self,
    ) -> LedgerResult<Option<ClusterBackfillRun>> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|err| LedgerError::Persistence(err.to_string()))?;

        let row = sqlx::query(
            r#"
            with candidate as (
              select run_id
              from cluster_backfill_runs
              where status = 'pending'
              order by requested_at asc
              limit 1
              for update skip locked
            )
            update cluster_backfill_runs runs
            set status = 'running',
                started_at = coalesce(runs.started_at, now())
            from candidate
            where runs.run_id = candidate.run_id
            returning runs.run_id, runs.cluster_id, runs.chain_id, runs.start_block, runs.end_block,
                      runs.status, runs.requested_at, runs.started_at, runs.finished_at,
                      runs.address_count, runs.discovered_tx_count, runs.analyzed_tx_count,
                      runs.failed_tx_count, runs.metadata
            "#,
        )
        .fetch_optional(&mut *tx)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;

        tx.commit()
            .await
            .map_err(|err| LedgerError::Persistence(err.to_string()))?;

        row.map(row_to_backfill_run).transpose()
    }

    async fn upsert_cluster_discovered_txs(&self, txs: &[ClusterDiscoveredTx]) -> LedgerResult<()> {
        for tx in txs {
            sqlx::query(
                r#"
                insert into cluster_discovered_txs (
                  cluster_id, chain_id, tx_hash, block_number, processing_status, latest_run_id,
                  first_seen_at, last_seen_at, analysis_attempted_at, analyzed_at, last_error, metadata
                )
                values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
                on conflict (cluster_id, tx_hash) do update set
                  chain_id = excluded.chain_id,
                  block_number = coalesce(excluded.block_number, cluster_discovered_txs.block_number),
                  latest_run_id = excluded.latest_run_id,
                  last_seen_at = excluded.last_seen_at,
                  metadata = excluded.metadata
                "#,
            )
            .bind(tx.cluster_id)
            .bind(tx.chain_id as i64)
            .bind(&tx.tx_hash)
            .bind(tx.block_number.map(|value| value as i64))
            .bind(cluster_tx_status_to_str(&tx.status))
            .bind(tx.latest_run_id)
            .bind(tx.first_seen_at)
            .bind(tx.last_seen_at)
            .bind(tx.analysis_attempted_at)
            .bind(tx.analyzed_at)
            .bind(&tx.last_error)
            .bind(Json(tx.metadata.clone()))
            .execute(&self.pool)
            .await
            .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        }
        Ok(())
    }

    async fn update_cluster_discovered_tx_status(
        &self,
        cluster_id: Uuid,
        tx_hash: &str,
        status: ClusterTxStatus,
        latest_run_id: Uuid,
        error: Option<String>,
        analyzed_at: Option<DateTime<Utc>>,
    ) -> LedgerResult<()> {
        sqlx::query(
            r#"
            update cluster_discovered_txs
            set processing_status = $3,
                latest_run_id = $4,
                last_error = $5,
                analysis_attempted_at = now(),
                analyzed_at = $6,
                last_seen_at = now()
            where cluster_id = $1 and tx_hash = $2
            "#,
        )
        .bind(cluster_id)
        .bind(tx_hash)
        .bind(cluster_tx_status_to_str(&status))
        .bind(latest_run_id)
        .bind(error)
        .bind(analyzed_at)
        .execute(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        Ok(())
    }

    async fn get_cluster_coverage(
        &self,
        cluster_id: Uuid,
    ) -> LedgerResult<Option<ClusterCoverage>> {
        let cluster_row =
            sqlx::query("select cluster_id, chain_id from address_clusters where cluster_id = $1")
                .bind(cluster_id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        let Some(cluster_row) = cluster_row else {
            return Ok(None);
        };

        let counts_row = sqlx::query(
            r#"
            select
              count(*)::bigint as discovered_tx_count,
              count(*) filter (where processing_status = '"analyzed"')::bigint as analyzed_tx_count,
              count(*) filter (where processing_status = '"failed"')::bigint as failed_tx_count,
              count(*) filter (where processing_status = '"pending"')::bigint as pending_tx_count
            from cluster_discovered_txs
            where cluster_id = $1
            "#,
        )
        .bind(cluster_id)
        .fetch_one(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;

        let latest_run = sqlx::query(
            r#"
            select run_id, status, started_at, finished_at
            from cluster_backfill_runs
            where cluster_id = $1
            order by requested_at desc
            limit 1
            "#,
        )
        .bind(cluster_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;

        Ok(Some(ClusterCoverage {
            cluster_id,
            chain_id: cluster_row
                .try_get::<i64, _>("chain_id")
                .map_err(|err| LedgerError::Persistence(err.to_string()))?
                as u64,
            discovered_tx_count: counts_row
                .try_get::<i64, _>("discovered_tx_count")
                .map_err(|err| LedgerError::Persistence(err.to_string()))?
                as u32,
            analyzed_tx_count: counts_row
                .try_get::<i64, _>("analyzed_tx_count")
                .map_err(|err| LedgerError::Persistence(err.to_string()))?
                as u32,
            failed_tx_count: counts_row
                .try_get::<i64, _>("failed_tx_count")
                .map_err(|err| LedgerError::Persistence(err.to_string()))?
                as u32,
            pending_tx_count: counts_row
                .try_get::<i64, _>("pending_tx_count")
                .map_err(|err| LedgerError::Persistence(err.to_string()))?
                as u32,
            latest_run_id: latest_run
                .as_ref()
                .and_then(|row| row.try_get("run_id").ok()),
            latest_run_status: latest_run
                .as_ref()
                .and_then(|row| row.try_get::<String, _>("status").ok())
                .map(|value| parse_cluster_backfill_status(&value))
                .transpose()?,
            latest_run_started_at: latest_run
                .as_ref()
                .and_then(|row| row.try_get("started_at").ok()),
            latest_run_finished_at: latest_run
                .as_ref()
                .and_then(|row| row.try_get("finished_at").ok()),
            metadata: serde_json::json!({
                "coverage_mode": "cluster_discovered_txs"
            }),
        }))
    }

    async fn upsert_cluster_tracking_state(
        &self,
        state: &ClusterTrackingState,
    ) -> LedgerResult<()> {
        sqlx::query(
            r#"
            insert into cluster_tracking_state (
              cluster_id, chain_id, tracking_status, last_processed_block, last_processed_at,
              last_updated_at, error_message, tracked_address_count, metadata, created_at, updated_at
            )
            values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
            on conflict (cluster_id) do update set
              chain_id = excluded.chain_id,
              tracking_status = excluded.tracking_status,
              last_processed_block = excluded.last_processed_block,
              last_processed_at = excluded.last_processed_at,
              last_updated_at = excluded.last_updated_at,
              error_message = excluded.error_message,
              tracked_address_count = excluded.tracked_address_count,
              metadata = excluded.metadata,
              updated_at = excluded.updated_at
            "#,
        )
        .bind(state.cluster_id)
        .bind(state.chain_id as i64)
        .bind(cluster_tracking_status_to_str(&state.status))
        .bind(state.last_processed_block.map(|value| value as i64))
        .bind(state.last_processed_at)
        .bind(state.last_updated_at)
        .bind(&state.error_message)
        .bind(state.tracked_address_count as i32)
        .bind(Json(state.metadata.clone()))
        .bind(state.created_at)
        .bind(state.updated_at)
        .execute(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        Ok(())
    }

    async fn get_cluster_tracking_state(
        &self,
        cluster_id: Uuid,
    ) -> LedgerResult<Option<ClusterTrackingState>> {
        let row = sqlx::query("select * from cluster_tracking_state where cluster_id = $1")
            .bind(cluster_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        row.map(row_to_tracking_state).transpose()
    }

    async fn list_active_cluster_tracking_states(&self) -> LedgerResult<Vec<ClusterTrackingState>> {
        let rows = sqlx::query(
            "select * from cluster_tracking_state where tracking_status = 'active' order by updated_at asc",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        rows.into_iter().map(row_to_tracking_state).collect()
    }

    async fn upsert_classified_transactions(
        &self,
        txs: &[ClassifiedTransaction],
    ) -> LedgerResult<()> {
        for tx in txs {
            sqlx::query(
                r#"
                insert into classified_transactions (
                  cluster_id, chain_id, tx_hash, address, block_number, block_timestamp, behavior_tag,
                  protocol, token_in, token_out, amount_in, amount_out, value_usd, gas_cost_usd,
                  estimated_pnl_usd, raw_json, created_at, updated_at
                )
                values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)
                on conflict (cluster_id, tx_hash) do update set
                  chain_id = excluded.chain_id,
                  address = excluded.address,
                  block_number = excluded.block_number,
                  block_timestamp = excluded.block_timestamp,
                  behavior_tag = excluded.behavior_tag,
                  protocol = excluded.protocol,
                  token_in = excluded.token_in,
                  token_out = excluded.token_out,
                  amount_in = excluded.amount_in,
                  amount_out = excluded.amount_out,
                  value_usd = excluded.value_usd,
                  gas_cost_usd = excluded.gas_cost_usd,
                  estimated_pnl_usd = excluded.estimated_pnl_usd,
                  raw_json = excluded.raw_json,
                  updated_at = excluded.updated_at
                "#,
            )
            .bind(tx.cluster_id)
            .bind(tx.chain_id as i64)
            .bind(&tx.tx_hash)
            .bind(&tx.address)
            .bind(tx.block_number as i64)
            .bind(tx.timestamp)
            .bind(behavior_tag_to_str(&tx.behavior_tag))
            .bind(&tx.protocol)
            .bind(&tx.token_in)
            .bind(&tx.token_out)
            .bind(&tx.amount_in)
            .bind(&tx.amount_out)
            .bind(tx.value_usd)
            .bind(tx.gas_cost_usd)
            .bind(tx.estimated_pnl_usd)
            .bind(Json(tx.raw_json.clone()))
            .bind(tx.created_at)
            .bind(tx.updated_at)
            .execute(&self.pool)
            .await
            .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        }
        Ok(())
    }

    async fn list_recent_classified_transactions(
        &self,
        cluster_id: Uuid,
        limit: u32,
    ) -> LedgerResult<Vec<ClassifiedTransaction>> {
        let rows = sqlx::query(
            r#"
            select *
            from classified_transactions
            where cluster_id = $1
            order by block_timestamp desc nulls last, block_number desc, updated_at desc
            limit $2
            "#,
        )
        .bind(cluster_id)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        rows.into_iter().map(row_to_classified_transaction).collect()
    }

    async fn replace_cluster_metrics_snapshots(
        &self,
        cluster_id: Uuid,
        snapshots: &[ClusterMetricsSnapshot],
    ) -> LedgerResult<()> {
        sqlx::query("delete from cluster_metrics_snapshots where cluster_id = $1")
            .bind(cluster_id)
            .execute(&self.pool)
            .await
            .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        for snapshot in snapshots {
            sqlx::query(
                r#"
                insert into cluster_metrics_snapshots (
                  snapshot_id, cluster_id, chain_id, window, snapshot_timestamp, tx_count, volume_usd,
                  gas_cost_usd, pnl_usd, roi, active_addresses_count, behavior_breakdown_json, metadata
                )
                values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
                "#,
            )
            .bind(snapshot.snapshot_id)
            .bind(snapshot.cluster_id)
            .bind(snapshot.chain_id as i64)
            .bind(metrics_window_to_str(&snapshot.window))
            .bind(snapshot.timestamp)
            .bind(snapshot.tx_count as i32)
            .bind(snapshot.volume_usd)
            .bind(snapshot.gas_cost_usd)
            .bind(snapshot.realized_pnl_usd)
            .bind(snapshot.roi)
            .bind(snapshot.active_addresses_count as i32)
            .bind(Json(snapshot.behavior_breakdown.clone()))
            .bind(Json(snapshot.metadata.clone()))
            .execute(&self.pool)
            .await
            .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        }
        Ok(())
    }

    async fn list_cluster_metrics_snapshots(
        &self,
        cluster_id: Uuid,
    ) -> LedgerResult<Vec<ClusterMetricsSnapshot>> {
        let rows = sqlx::query(
            "select * from cluster_metrics_snapshots where cluster_id = $1 order by snapshot_timestamp desc",
        )
        .bind(cluster_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        rows.into_iter().map(row_to_metrics_snapshot).collect()
    }

    async fn insert_cluster_alerts(&self, alerts: &[ClusterAlert]) -> LedgerResult<()> {
        for alert in alerts {
            sqlx::query(
                r#"
                insert into cluster_alerts (
                  alert_id, cluster_id, chain_id, alert_type, severity, title, message,
                  related_metric_json, triggered_at, is_read, read_at, metadata
                )
                values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
                on conflict (alert_id) do nothing
                "#,
            )
            .bind(alert.alert_id)
            .bind(alert.cluster_id)
            .bind(alert.chain_id as i64)
            .bind(alert_type_to_str(&alert.alert_type))
            .bind(alert_severity_to_str(&alert.severity))
            .bind(&alert.title)
            .bind(&alert.message)
            .bind(Json(alert.related_metric.clone()))
            .bind(alert.triggered_at)
            .bind(alert.is_read)
            .bind(alert.read_at)
            .bind(Json(alert.metadata.clone()))
            .execute(&self.pool)
            .await
            .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        }
        Ok(())
    }

    async fn list_cluster_alerts(
        &self,
        cluster_id: Uuid,
        include_read: bool,
        limit: u32,
    ) -> LedgerResult<Vec<ClusterAlert>> {
        let rows = if include_read {
            sqlx::query(
                "select * from cluster_alerts where cluster_id = $1 order by triggered_at desc limit $2",
            )
            .bind(cluster_id)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
        } else {
            sqlx::query(
                "select * from cluster_alerts where cluster_id = $1 and is_read = false order by triggered_at desc limit $2",
            )
            .bind(cluster_id)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
        }
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        rows.into_iter().map(row_to_cluster_alert).collect()
    }

    async fn mark_cluster_alert_read(
        &self,
        cluster_id: Uuid,
        alert_id: Uuid,
    ) -> LedgerResult<Option<ClusterAlert>> {
        let row = sqlx::query(
            r#"
            update cluster_alerts
            set is_read = true, read_at = now()
            where cluster_id = $1 and alert_id = $2
            returning *
            "#,
        )
        .bind(cluster_id)
        .bind(alert_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        row.map(row_to_cluster_alert).transpose()
    }
}

fn row_to_pack(row: sqlx::postgres::PgRow) -> LedgerResult<EvidencePack> {
    let Json(recognizer_set_versions): Json<Value> = row
        .try_get("recognizer_set_versions")
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
    let Json(facts): Json<Value> = row
        .try_get("facts")
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
    let Json(labels): Json<Value> = row
        .try_get("labels")
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
    let Json(flows): Json<Value> = row
        .try_get("flows")
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
    let Json(unresolved): Json<Value> = row
        .try_get("unresolved")
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
    let Json(validation_results): Json<Value> = row
        .try_get("validation_results")
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
    let Json(metadata): Json<Value> = row
        .try_get("metadata")
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
    Ok(EvidencePack {
        pack_id: row
            .try_get("pack_id")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?,
        tx_hash: row
            .try_get("tx_hash")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?,
        attempt_id: row
            .try_get("attempt_id")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?,
        ruleset_id: row
            .try_get("ruleset_id")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?,
        input_digest: row
            .try_get("input_digest")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?,
        recognizer_set_versions: serde_json::from_value(recognizer_set_versions)
            .map_err(|err| LedgerError::Persistence(err.to_string()))?,
        created_at: chrono::Utc::now(),
        facts,
        labels: labels.as_array().cloned().unwrap_or_default(),
        flows: flows.as_array().cloned().unwrap_or_default(),
        unresolved: unresolved.as_array().cloned().unwrap_or_default(),
        validation_results: validation_results.as_array().cloned().unwrap_or_default(),
        metadata,
    })
}

fn row_to_backfill_run(row: sqlx::postgres::PgRow) -> LedgerResult<ClusterBackfillRun> {
    let Json(metadata): Json<Value> = row
        .try_get("metadata")
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
    let status_raw = row
        .try_get::<String, _>("status")
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
    Ok(ClusterBackfillRun {
        run_id: row
            .try_get("run_id")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?,
        cluster_id: row
            .try_get("cluster_id")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?,
        chain_id: row
            .try_get::<i64, _>("chain_id")
            .map_err(|err| LedgerError::Persistence(err.to_string()))? as u64,
        start_block: row
            .try_get::<Option<i64>, _>("start_block")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?
            .map(|value| value as u64),
        end_block: row
            .try_get::<Option<i64>, _>("end_block")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?
            .map(|value| value as u64),
        status: parse_cluster_backfill_status(&status_raw)?,
        requested_at: row
            .try_get("requested_at")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?,
        started_at: row
            .try_get("started_at")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?,
        finished_at: row
            .try_get("finished_at")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?,
        address_count: row
            .try_get::<i32, _>("address_count")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?
            as u32,
        discovered_tx_count: row
            .try_get::<i32, _>("discovered_tx_count")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?
            as u32,
        analyzed_tx_count: row
            .try_get::<i32, _>("analyzed_tx_count")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?
            as u32,
        failed_tx_count: row
            .try_get::<i32, _>("failed_tx_count")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?
            as u32,
        metadata,
    })
}

fn row_to_tracking_state(row: sqlx::postgres::PgRow) -> LedgerResult<ClusterTrackingState> {
    let Json(metadata): Json<Value> = row
        .try_get("metadata")
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
    let status_raw = row
        .try_get::<String, _>("tracking_status")
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
    Ok(ClusterTrackingState {
        cluster_id: row
            .try_get("cluster_id")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?,
        chain_id: row
            .try_get::<i64, _>("chain_id")
            .map_err(|err| LedgerError::Persistence(err.to_string()))? as u64,
        status: parse_cluster_tracking_status(&status_raw)?,
        last_processed_block: row
            .try_get::<Option<i64>, _>("last_processed_block")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?
            .map(|value| value as u64),
        last_processed_at: row
            .try_get("last_processed_at")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?,
        last_updated_at: row
            .try_get("last_updated_at")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?,
        error_message: row
            .try_get("error_message")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?,
        tracked_address_count: row
            .try_get::<i32, _>("tracked_address_count")
            .map_err(|err| LedgerError::Persistence(err.to_string()))? as u32,
        created_at: row
            .try_get("created_at")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?,
        updated_at: row
            .try_get("updated_at")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?,
        metadata,
    })
}

fn row_to_classified_transaction(
    row: sqlx::postgres::PgRow,
) -> LedgerResult<ClassifiedTransaction> {
    let Json(raw_json): Json<Value> = row
        .try_get("raw_json")
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
    let behavior_tag_raw = row
        .try_get::<String, _>("behavior_tag")
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
    Ok(ClassifiedTransaction {
        cluster_id: row
            .try_get("cluster_id")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?,
        chain_id: row
            .try_get::<i64, _>("chain_id")
            .map_err(|err| LedgerError::Persistence(err.to_string()))? as u64,
        tx_hash: row
            .try_get("tx_hash")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?,
        address: row
            .try_get("address")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?,
        block_number: row
            .try_get::<i64, _>("block_number")
            .map_err(|err| LedgerError::Persistence(err.to_string()))? as u64,
        timestamp: row
            .try_get("block_timestamp")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?,
        behavior_tag: parse_behavior_tag(&behavior_tag_raw)?,
        protocol: row
            .try_get("protocol")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?,
        token_in: row
            .try_get("token_in")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?,
        token_out: row
            .try_get("token_out")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?,
        amount_in: row
            .try_get("amount_in")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?,
        amount_out: row
            .try_get("amount_out")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?,
        value_usd: row
            .try_get("value_usd")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?,
        gas_cost_usd: row
            .try_get("gas_cost_usd")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?,
        estimated_pnl_usd: row
            .try_get("estimated_pnl_usd")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?,
        raw_json,
        created_at: row
            .try_get("created_at")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?,
        updated_at: row
            .try_get("updated_at")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?,
    })
}

fn row_to_metrics_snapshot(row: sqlx::postgres::PgRow) -> LedgerResult<ClusterMetricsSnapshot> {
    let Json(behavior_breakdown): Json<Value> = row
        .try_get("behavior_breakdown_json")
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
    let Json(metadata): Json<Value> = row
        .try_get("metadata")
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
    let window_raw = row
        .try_get::<String, _>("window")
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
    Ok(ClusterMetricsSnapshot {
        snapshot_id: row
            .try_get("snapshot_id")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?,
        cluster_id: row
            .try_get("cluster_id")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?,
        chain_id: row
            .try_get::<i64, _>("chain_id")
            .map_err(|err| LedgerError::Persistence(err.to_string()))? as u64,
        window: parse_metrics_window(&window_raw)?,
        timestamp: row
            .try_get("snapshot_timestamp")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?,
        tx_count: row
            .try_get::<i32, _>("tx_count")
            .map_err(|err| LedgerError::Persistence(err.to_string()))? as u32,
        volume_usd: row
            .try_get("volume_usd")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?,
        gas_cost_usd: row
            .try_get("gas_cost_usd")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?,
        realized_pnl_usd: row
            .try_get("pnl_usd")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?,
        roi: row
            .try_get("roi")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?,
        active_addresses_count: row
            .try_get::<i32, _>("active_addresses_count")
            .map_err(|err| LedgerError::Persistence(err.to_string()))? as u32,
        behavior_breakdown,
        metadata,
    })
}

fn row_to_cluster_alert(row: sqlx::postgres::PgRow) -> LedgerResult<ClusterAlert> {
    let Json(related_metric): Json<Value> = row
        .try_get("related_metric_json")
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
    let Json(metadata): Json<Value> = row
        .try_get("metadata")
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
    let alert_type_raw = row
        .try_get::<String, _>("alert_type")
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
    let severity_raw = row
        .try_get::<String, _>("severity")
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
    Ok(ClusterAlert {
        alert_id: row
            .try_get("alert_id")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?,
        cluster_id: row
            .try_get("cluster_id")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?,
        chain_id: row
            .try_get::<i64, _>("chain_id")
            .map_err(|err| LedgerError::Persistence(err.to_string()))? as u64,
        alert_type: parse_alert_type(&alert_type_raw)?,
        severity: parse_alert_severity(&severity_raw)?,
        title: row
            .try_get("title")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?,
        message: row
            .try_get("message")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?,
        related_metric,
        triggered_at: row
            .try_get("triggered_at")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?,
        is_read: row
            .try_get("is_read")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?,
        read_at: row
            .try_get("read_at")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?,
        metadata,
    })
}

fn parse_cluster_backfill_status(value: &str) -> LedgerResult<ClusterBackfillStatus> {
    match value {
        "pending" => Ok(ClusterBackfillStatus::Pending),
        "running" => Ok(ClusterBackfillStatus::Running),
        "completed" => Ok(ClusterBackfillStatus::Completed),
        "partial" => Ok(ClusterBackfillStatus::Partial),
        "failed" => Ok(ClusterBackfillStatus::Failed),
        other => Err(LedgerError::Persistence(format!(
            "unknown cluster backfill status: {other}"
        ))),
    }
}

fn parse_cluster_tracking_status(value: &str) -> LedgerResult<ClusterTrackingStatus> {
    match value {
        "active" => Ok(ClusterTrackingStatus::Active),
        "paused" => Ok(ClusterTrackingStatus::Paused),
        "error" => Ok(ClusterTrackingStatus::Error),
        other => Err(LedgerError::Persistence(format!(
            "unknown cluster tracking status: {other}"
        ))),
    }
}

fn parse_behavior_tag(value: &str) -> LedgerResult<BehaviorTag> {
    match value {
        "arbitrage" => Ok(BehaviorTag::Arbitrage),
        "position" => Ok(BehaviorTag::Position),
        "lp" => Ok(BehaviorTag::Lp),
        "liquidation" => Ok(BehaviorTag::Liquidation),
        "unknown" => Ok(BehaviorTag::Unknown),
        other => Err(LedgerError::Persistence(format!(
            "unknown behavior tag: {other}"
        ))),
    }
}

fn parse_metrics_window(value: &str) -> LedgerResult<MetricsWindow> {
    match value {
        "1h" => Ok(MetricsWindow::OneHour),
        "24h" => Ok(MetricsWindow::TwentyFourHours),
        "7d" => Ok(MetricsWindow::SevenDays),
        other => Err(LedgerError::Persistence(format!(
            "unknown metrics window: {other}"
        ))),
    }
}

fn parse_alert_type(value: &str) -> LedgerResult<AlertType> {
    match value {
        "roi_anomaly" => Ok(AlertType::RoiAnomaly),
        "tx_count_spike" => Ok(AlertType::TxCountSpike),
        "volume_spike" => Ok(AlertType::VolumeSpike),
        "behavior_shift" => Ok(AlertType::BehaviorShift),
        "external_inflow" => Ok(AlertType::ExternalInflow),
        other => Err(LedgerError::Persistence(format!(
            "unknown alert type: {other}"
        ))),
    }
}

fn parse_alert_severity(value: &str) -> LedgerResult<AlertSeverity> {
    match value {
        "low" => Ok(AlertSeverity::Low),
        "medium" => Ok(AlertSeverity::Medium),
        "high" => Ok(AlertSeverity::High),
        other => Err(LedgerError::Persistence(format!(
            "unknown alert severity: {other}"
        ))),
    }
}

fn cluster_backfill_status_to_str(value: &ClusterBackfillStatus) -> &'static str {
    match value {
        ClusterBackfillStatus::Pending => "pending",
        ClusterBackfillStatus::Running => "running",
        ClusterBackfillStatus::Completed => "completed",
        ClusterBackfillStatus::Partial => "partial",
        ClusterBackfillStatus::Failed => "failed",
    }
}

fn cluster_tx_status_to_str(value: &ClusterTxStatus) -> &'static str {
    match value {
        ClusterTxStatus::Pending => "pending",
        ClusterTxStatus::Analyzed => "analyzed",
        ClusterTxStatus::Failed => "failed",
    }
}

fn cluster_tracking_status_to_str(value: &ClusterTrackingStatus) -> &'static str {
    match value {
        ClusterTrackingStatus::Active => "active",
        ClusterTrackingStatus::Paused => "paused",
        ClusterTrackingStatus::Error => "error",
    }
}

fn behavior_tag_to_str(value: &BehaviorTag) -> &'static str {
    match value {
        BehaviorTag::Arbitrage => "arbitrage",
        BehaviorTag::Position => "position",
        BehaviorTag::Lp => "lp",
        BehaviorTag::Liquidation => "liquidation",
        BehaviorTag::Unknown => "unknown",
    }
}

fn metrics_window_to_str(value: &MetricsWindow) -> &'static str {
    match value {
        MetricsWindow::OneHour => "1h",
        MetricsWindow::TwentyFourHours => "24h",
        MetricsWindow::SevenDays => "7d",
    }
}

fn alert_type_to_str(value: &AlertType) -> &'static str {
    match value {
        AlertType::RoiAnomaly => "roi_anomaly",
        AlertType::TxCountSpike => "tx_count_spike",
        AlertType::VolumeSpike => "volume_spike",
        AlertType::BehaviorShift => "behavior_shift",
        AlertType::ExternalInflow => "external_inflow",
    }
}

fn alert_severity_to_str(value: &AlertSeverity) -> &'static str {
    match value {
        AlertSeverity::Low => "low",
        AlertSeverity::Medium => "medium",
        AlertSeverity::High => "high",
    }
}
