use std::{collections::BTreeMap, sync::Arc};

use chrono::Utc;
use serde::Serialize;
use serde_json::{Value, json};
use uuid::Uuid;

use crate::{
    core::errors::{LedgerError, LedgerResult},
    domain::{
        entities::{ClusterBackfillRun, ClusterCoverage, ClusterDiscoveredTx},
        enums::{ClusterBackfillStatus, ClusterTxStatus},
        interfaces::ClusterTxSource,
        value_objects::AddressTxRef,
    },
    services::analysis::AnalysisService,
};

#[derive(Clone, Debug)]
pub struct ClusterBackfillRequest {
    pub start_block: Option<u64>,
    pub end_block: Option<u64>,
    pub force_reanalyze: bool,
    pub query_limit: Option<u32>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ClusterBackfillResult {
    pub run: ClusterBackfillRun,
    pub coverage: ClusterCoverage,
}

#[derive(Clone)]
pub struct ClusterBackfillService {
    analysis_service: Arc<AnalysisService>,
    tx_source: Option<Arc<dyn ClusterTxSource>>,
    default_query_limit: u32,
}

impl ClusterBackfillService {
    pub fn new(
        analysis_service: Arc<AnalysisService>,
        tx_source: Option<Arc<dyn ClusterTxSource>>,
        default_query_limit: u32,
    ) -> Self {
        Self {
            analysis_service,
            tx_source,
            default_query_limit,
        }
    }

    pub async fn backfill_cluster(
        &self,
        cluster_id: Uuid,
        request: ClusterBackfillRequest,
    ) -> LedgerResult<ClusterBackfillResult> {
        let repository = &self.analysis_service.repository;
        let cluster = repository
            .get_cluster(cluster_id)
            .await?
            .ok_or_else(|| LedgerError::NotFound(format!("cluster {cluster_id} not found")))?;
        let tx_source = self.tx_source.as_ref().ok_or_else(|| {
            LedgerError::AnalysisInvariant("indexer tx source is not configured".to_owned())
        })?;

        let run = ClusterBackfillRun {
            run_id: Uuid::new_v4(),
            cluster_id,
            chain_id: cluster.chain_id,
            start_block: request.start_block,
            end_block: request.end_block,
            status: ClusterBackfillStatus::Running,
            requested_at: Utc::now(),
            started_at: Some(Utc::now()),
            finished_at: None,
            address_count: cluster.addresses.len() as u32,
            discovered_tx_count: 0,
            analyzed_tx_count: 0,
            failed_tx_count: 0,
            metadata: build_request_metadata(&request),
        };
        repository.create_cluster_backfill_run(&run).await?;
        self.execute_run(run, request, tx_source.as_ref(), cluster.chain_id)
            .await
    }

    pub async fn enqueue_cluster(
        &self,
        cluster_id: Uuid,
        request: ClusterBackfillRequest,
    ) -> LedgerResult<ClusterBackfillRun> {
        let repository = &self.analysis_service.repository;
        let cluster = repository
            .get_cluster(cluster_id)
            .await?
            .ok_or_else(|| LedgerError::NotFound(format!("cluster {cluster_id} not found")))?;
        if self.tx_source.is_none() {
            return Err(LedgerError::AnalysisInvariant(
                "indexer tx source is not configured".to_owned(),
            ));
        }

        let run = ClusterBackfillRun {
            run_id: Uuid::new_v4(),
            cluster_id,
            chain_id: cluster.chain_id,
            start_block: request.start_block,
            end_block: request.end_block,
            status: ClusterBackfillStatus::Pending,
            requested_at: Utc::now(),
            started_at: None,
            finished_at: None,
            address_count: cluster.addresses.len() as u32,
            discovered_tx_count: 0,
            analyzed_tx_count: 0,
            failed_tx_count: 0,
            metadata: build_request_metadata(&request),
        };
        repository.create_cluster_backfill_run(&run).await?;
        Ok(run)
    }

    pub async fn process_next_pending_run(&self) -> LedgerResult<Option<ClusterBackfillResult>> {
        let tx_source = self.tx_source.as_ref().ok_or_else(|| {
            LedgerError::AnalysisInvariant("indexer tx source is not configured".to_owned())
        })?;
        let repository = &self.analysis_service.repository;
        let Some(run) = repository.claim_next_pending_cluster_backfill_run().await? else {
            return Ok(None);
        };
        let chain_id = run.chain_id;
        let request = request_from_metadata(
            run.start_block,
            run.end_block,
            &run.metadata,
            self.default_query_limit,
        )?;
        Ok(Some(
            self.execute_run(run, request, tx_source.as_ref(), chain_id)
                .await?,
        ))
    }

    pub async fn get_cluster_coverage(&self, cluster_id: Uuid) -> LedgerResult<ClusterCoverage> {
        let repository = &self.analysis_service.repository;
        let cluster = repository
            .get_cluster(cluster_id)
            .await?
            .ok_or_else(|| LedgerError::NotFound(format!("cluster {cluster_id} not found")))?;
        if let Some(coverage) = repository.get_cluster_coverage(cluster_id).await? {
            return Ok(coverage);
        }

        Ok(ClusterCoverage {
            cluster_id,
            chain_id: cluster.chain_id,
            discovered_tx_count: 0,
            analyzed_tx_count: 0,
            failed_tx_count: 0,
            pending_tx_count: 0,
            latest_run_id: None,
            latest_run_status: None,
            latest_run_started_at: None,
            latest_run_finished_at: None,
            metadata: json!({
                "coverage_mode": "cluster_discovered_txs",
                "has_backfill_runs": false,
            }),
        })
    }

    pub async fn list_cluster_backfill_runs(
        &self,
        cluster_id: Uuid,
    ) -> LedgerResult<Vec<ClusterBackfillRun>> {
        let repository = &self.analysis_service.repository;
        repository
            .get_cluster(cluster_id)
            .await?
            .ok_or_else(|| LedgerError::NotFound(format!("cluster {cluster_id} not found")))?;
        repository.list_cluster_backfill_runs(cluster_id).await
    }

    pub fn is_configured(&self) -> bool {
        self.tx_source.is_some()
    }

    async fn execute_run(
        &self,
        mut run: ClusterBackfillRun,
        request: ClusterBackfillRequest,
        tx_source: &dyn ClusterTxSource,
        chain_id: u64,
    ) -> LedgerResult<ClusterBackfillResult> {
        let result = self
            .execute_run_inner(&mut run, request, tx_source, chain_id)
            .await;
        if let Err(err) = result {
            run.status = ClusterBackfillStatus::Failed;
            run.finished_at = Some(Utc::now());
            run.metadata = merge_run_metadata(
                &run.metadata,
                json!({
                    "coverage_mode": "cluster_discovered_txs",
                    "last_error": err.to_string(),
                }),
            );
            self.analysis_service
                .repository
                .update_cluster_backfill_run(&run)
                .await?;
            return Err(err);
        }
        result
    }

    async fn execute_run_inner(
        &self,
        run: &mut ClusterBackfillRun,
        request: ClusterBackfillRequest,
        tx_source: &dyn ClusterTxSource,
        chain_id: u64,
    ) -> LedgerResult<ClusterBackfillResult> {
        let repository = &self.analysis_service.repository;
        let cluster = repository
            .get_cluster(run.cluster_id)
            .await?
            .ok_or_else(|| LedgerError::NotFound(format!("cluster {} not found", run.cluster_id)))?;

        let start_block = request.start_block.unwrap_or(0);
        let end_block = match request.end_block {
            Some(value) => value,
            None => tx_source.get_last_indexed_block(chain_id).await?,
        };
        if end_block < start_block {
            return Err(LedgerError::AnalysisInvariant(format!(
                "invalid backfill block range: start_block {start_block} > end_block {end_block}"
            )));
        }
        run.start_block = Some(start_block);
        run.end_block = Some(end_block);
        if run.started_at.is_none() {
            run.started_at = Some(Utc::now());
        }
        run.status = ClusterBackfillStatus::Running;
        repository.update_cluster_backfill_run(run).await?;

        let query_limit = request.query_limit.unwrap_or(self.default_query_limit).max(1);
        let mut tx_map = BTreeMap::<String, AddressTxRef>::new();
        let mut warnings = Vec::new();

        for address in &cluster.addresses {
            self.collect_refs_for_range(
                tx_source,
                chain_id,
                &address.address,
                start_block,
                end_block,
                query_limit,
                &mut tx_map,
                &mut warnings,
            )
            .await?;
        }

        let now = Utc::now();
        let discovered = tx_map
            .values()
            .map(|item| ClusterDiscoveredTx {
                cluster_id: run.cluster_id,
                chain_id,
                tx_hash: item.tx_hash.clone(),
                block_number: Some(item.block_number),
                status: ClusterTxStatus::Pending,
                latest_run_id: Some(run.run_id),
                first_seen_at: now,
                last_seen_at: now,
                analysis_attempted_at: None,
                analyzed_at: None,
                last_error: None,
                metadata: json!({
                    "source": "indexer_api",
                    "address": item.address,
                    "tx_index": item.tx_index,
                    "direction": item.direction,
                }),
            })
            .collect::<Vec<_>>();
        repository.upsert_cluster_discovered_txs(&discovered).await?;

        let mut analyzed_count = 0u32;
        let mut failed_count = 0u32;
        let mut txs = tx_map.into_values().collect::<Vec<_>>();
        txs.sort_by(|left, right| {
            (left.block_number, left.tx_index, left.tx_hash.as_str()).cmp(&(
                right.block_number,
                right.tx_index,
                right.tx_hash.as_str(),
            ))
        });

        for tx_ref in &txs {
            if !request.force_reanalyze
                && repository.get_analysis(chain_id, &tx_ref.tx_hash).await?.is_some()
            {
                repository
                    .update_cluster_discovered_tx_status(
                        run.cluster_id,
                        &tx_ref.tx_hash,
                        ClusterTxStatus::Analyzed,
                        run.run_id,
                        None,
                        Some(Utc::now()),
                    )
                    .await?;
                analyzed_count += 1;
                continue;
            }

            match self
                .analysis_service
                .analyze_transaction(chain_id, &tx_ref.tx_hash)
                .await
            {
                Ok(_) => {
                    repository
                        .update_cluster_discovered_tx_status(
                            run.cluster_id,
                            &tx_ref.tx_hash,
                            ClusterTxStatus::Analyzed,
                            run.run_id,
                            None,
                            Some(Utc::now()),
                        )
                        .await?;
                    analyzed_count += 1;
                }
                Err(err) => {
                    repository
                        .update_cluster_discovered_tx_status(
                            run.cluster_id,
                            &tx_ref.tx_hash,
                            ClusterTxStatus::Failed,
                            run.run_id,
                            Some(err.to_string()),
                            None,
                        )
                        .await?;
                    failed_count += 1;
                }
            }
        }

        run.discovered_tx_count = txs.len() as u32;
        run.analyzed_tx_count = analyzed_count;
        run.failed_tx_count = failed_count;
        run.finished_at = Some(Utc::now());
        run.status = if failed_count == 0 {
            ClusterBackfillStatus::Completed
        } else if analyzed_count > 0 {
            ClusterBackfillStatus::Partial
        } else {
            ClusterBackfillStatus::Failed
        };
        run.metadata = merge_run_metadata(
            &run.metadata,
            json!({
                "coverage_mode": "cluster_discovered_txs",
                "warnings": warnings,
                "query_limit": query_limit,
                "force_reanalyze": request.force_reanalyze,
            }),
        );
        repository.update_cluster_backfill_run(run).await?;

        let coverage = repository
            .get_cluster_coverage(run.cluster_id)
            .await?
            .ok_or_else(|| {
                LedgerError::AnalysisInvariant(format!(
                    "coverage missing for cluster {} after backfill",
                    run.cluster_id
                ))
            })?;

        Ok(ClusterBackfillResult {
            run: run.clone(),
            coverage,
        })
    }

    async fn collect_refs_for_range(
        &self,
        tx_source: &dyn ClusterTxSource,
        chain_id: u64,
        address: &str,
        start_block: u64,
        end_block: u64,
        query_limit: u32,
        tx_map: &mut BTreeMap<String, AddressTxRef>,
        warnings: &mut Vec<serde_json::Value>,
    ) -> LedgerResult<()> {
        let mut ranges = vec![(start_block, end_block)];
        while let Some((range_start, range_end)) = ranges.pop() {
            let refs = tx_source
                .query_address_refs(
                    chain_id,
                    address,
                    query_limit,
                    Some(range_start),
                    Some(range_end),
                )
                .await?;

            if refs.len() as u32 >= query_limit && range_start < range_end {
                let mid = range_start + ((range_end - range_start) / 2);
                ranges.push((mid.saturating_add(1), range_end));
                ranges.push((range_start, mid));
                continue;
            }

            if refs.len() as u32 >= query_limit && range_start == range_end {
                warnings.push(json!({
                    "type": "single_block_limit_reached",
                    "address": address,
                    "block_number": range_start,
                    "query_limit": query_limit,
                }));
            }

            for item in refs {
                tx_map
                    .entry(item.tx_hash.clone())
                    .and_modify(|existing| {
                        if (item.block_number, item.tx_index)
                            < (existing.block_number, existing.tx_index)
                        {
                            *existing = item.clone();
                        }
                    })
                    .or_insert(item);
            }
        }

        Ok(())
    }
}

fn build_request_metadata(request: &ClusterBackfillRequest) -> Value {
    json!({
        "request": {
            "force_reanalyze": request.force_reanalyze,
            "query_limit": request.query_limit,
        }
    })
}

fn request_from_metadata(
    start_block: Option<u64>,
    end_block: Option<u64>,
    metadata: &Value,
    default_query_limit: u32,
) -> LedgerResult<ClusterBackfillRequest> {
    let request = metadata.get("request");
    let force_reanalyze = request
        .and_then(|value| value.get("force_reanalyze"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let query_limit = request
        .and_then(|value| value.get("query_limit"))
        .and_then(Value::as_u64)
        .map(|value| u32::try_from(value))
        .transpose()
        .map_err(|_| {
            LedgerError::AnalysisInvariant("cluster backfill query_limit is too large".to_owned())
        })?
        .or(Some(default_query_limit));
    Ok(ClusterBackfillRequest {
        start_block,
        end_block,
        force_reanalyze,
        query_limit,
    })
}

fn merge_run_metadata(existing: &Value, extra: Value) -> Value {
    let mut merged = existing.as_object().cloned().unwrap_or_default();
    if let Some(extra_object) = extra.as_object() {
        for (key, value) in extra_object {
            merged.insert(key.clone(), value.clone());
        }
    }
    Value::Object(merged)
}
