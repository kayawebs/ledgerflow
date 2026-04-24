use std::{collections::BTreeMap, sync::Arc};

use chrono::Utc;
use serde::Serialize;
use serde_json::json;
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

        let start_block = request.start_block.unwrap_or(0);
        let end_block = match request.end_block {
            Some(value) => value,
            None => tx_source.get_last_indexed_block(cluster.chain_id).await?,
        };
        if end_block < start_block {
            return Err(LedgerError::AnalysisInvariant(format!(
                "invalid backfill block range: start_block {start_block} > end_block {end_block}"
            )));
        }

        let mut run = ClusterBackfillRun {
            run_id: Uuid::new_v4(),
            cluster_id,
            chain_id: cluster.chain_id,
            start_block: Some(start_block),
            end_block: Some(end_block),
            status: ClusterBackfillStatus::Running,
            requested_at: Utc::now(),
            started_at: Some(Utc::now()),
            finished_at: None,
            address_count: cluster.addresses.len() as u32,
            discovered_tx_count: 0,
            analyzed_tx_count: 0,
            failed_tx_count: 0,
            metadata: json!({}),
        };
        repository.create_cluster_backfill_run(&run).await?;

        let query_limit = request
            .query_limit
            .unwrap_or(self.default_query_limit)
            .max(1);
        let mut tx_map = BTreeMap::<String, AddressTxRef>::new();
        let mut warnings = Vec::new();

        for address in &cluster.addresses {
            self.collect_refs_for_range(
                tx_source.as_ref(),
                cluster.chain_id,
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
                cluster_id,
                chain_id: cluster.chain_id,
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
        repository
            .upsert_cluster_discovered_txs(&discovered)
            .await?;

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
                && repository
                    .get_analysis(cluster.chain_id, &tx_ref.tx_hash)
                    .await?
                    .is_some()
            {
                repository
                    .update_cluster_discovered_tx_status(
                        cluster_id,
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
                .analyze_transaction(cluster.chain_id, &tx_ref.tx_hash)
                .await
            {
                Ok(_) => {
                    repository
                        .update_cluster_discovered_tx_status(
                            cluster_id,
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
                            cluster_id,
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
        run.metadata = json!({
            "coverage_mode": "cluster_discovered_txs",
            "warnings": warnings,
            "query_limit": query_limit,
            "force_reanalyze": request.force_reanalyze,
        });
        repository.update_cluster_backfill_run(&run).await?;

        let coverage = repository
            .get_cluster_coverage(cluster_id)
            .await?
            .ok_or_else(|| {
                LedgerError::AnalysisInvariant(format!(
                    "coverage missing for cluster {cluster_id} after backfill"
                ))
            })?;

        Ok(ClusterBackfillResult { run, coverage })
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
