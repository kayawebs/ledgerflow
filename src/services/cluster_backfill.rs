use std::{collections::BTreeMap, sync::Arc};

use chrono::Utc;
use serde::Serialize;
use serde_json::{Value, json};
use uuid::Uuid;

use crate::{
    core::errors::{LedgerError, LedgerResult},
    domain::{
        entities::{
            AddressCluster, ClusterAddress, ClusterBackfillRun, ClusterCoverage,
            ClusterDiscoveredTx, ClusterTrackingState,
        },
        enums::{ClusterBackfillStatus, ClusterTrackingStatus, ClusterTxStatus},
        interfaces::ClusterTxSource,
        value_objects::AddressTxRef,
    },
    services::{
        alerts::ClusterAlertService,
        analysis::AnalysisService,
        behavior::BehaviorClassifier,
        discovery::{ClusterDiscoveryRequest, ClusterDiscoveryService, DiscoveredAddressType},
        metrics::ClusterMetricsService,
        strategy_analysis::ClusterStrategyAnalysisService,
    },
};

const AUTO_DISCOVERY_MAX_HOPS: u8 = 1;
const AUTO_DISCOVERY_MAX_NODES: u32 = 40;
const AUTO_DISCOVERY_MAX_WINDOWS: usize = 3;
const AUTO_DISCOVERY_MAX_AUTO_ADD: usize = 12;
const AUTO_DISCOVERY_MIN_AUTO_ADD_CONFIDENCE: f32 = 0.75;

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
    continuous_watch_lookback_blocks: u64,
    behavior_classifier: BehaviorClassifier,
    metrics_service: ClusterMetricsService,
    alert_service: ClusterAlertService,
    strategy_analysis_service: ClusterStrategyAnalysisService,
    discovery_service: Option<Arc<ClusterDiscoveryService>>,
}

impl ClusterBackfillService {
    pub fn new(
        analysis_service: Arc<AnalysisService>,
        tx_source: Option<Arc<dyn ClusterTxSource>>,
        default_query_limit: u32,
        continuous_watch_lookback_blocks: u64,
    ) -> Self {
        let repository = analysis_service.repository.clone();
        Self {
            analysis_service,
            tx_source,
            default_query_limit,
            continuous_watch_lookback_blocks,
            behavior_classifier: BehaviorClassifier::default(),
            metrics_service: ClusterMetricsService::new(repository.clone()),
            alert_service: ClusterAlertService::new(repository.clone()),
            strategy_analysis_service: ClusterStrategyAnalysisService::new(repository),
            discovery_service: None,
        }
    }

    pub fn with_discovery_service(
        mut self,
        discovery_service: Arc<ClusterDiscoveryService>,
    ) -> Self {
        self.discovery_service = Some(discovery_service);
        self
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

    pub async fn enqueue_incremental_runs(&self) -> LedgerResult<u32> {
        let tx_source = self.tx_source.as_ref().ok_or_else(|| {
            LedgerError::AnalysisInvariant("indexer tx source is not configured".to_owned())
        })?;
        let repository = &self.analysis_service.repository;
        let tracking_states = repository.list_active_cluster_tracking_states().await?;
        let mut enqueued = 0u32;

        for tracking in tracking_states {
            let Some(cluster) = repository.get_cluster(tracking.cluster_id).await? else {
                continue;
            };
            let runs = repository
                .list_cluster_backfill_runs(cluster.cluster_id)
                .await
                .unwrap_or_default();
            if runs.iter().any(|run| {
                matches!(
                    run.status,
                    ClusterBackfillStatus::Pending | ClusterBackfillStatus::Running
                )
            }) {
                continue;
            }

            let last_indexed_block = tx_source.get_last_indexed_block(cluster.chain_id).await?;
            let latest_completed_end = tracking.last_processed_block.or_else(|| {
                runs.iter()
                    .filter(|run| {
                        matches!(
                            run.status,
                            ClusterBackfillStatus::Completed
                                | ClusterBackfillStatus::Partial
                                | ClusterBackfillStatus::Failed
                        )
                    })
                    .filter_map(|run| run.end_block)
                    .max()
            });

            let start_block = latest_completed_end
                .map(|value| {
                    value
                        .saturating_add(1)
                        .saturating_sub(self.continuous_watch_lookback_blocks)
                })
                .unwrap_or(0);

            if let Some(last_end) = latest_completed_end
                && last_end >= last_indexed_block
            {
                continue;
            }

            let mut tx_map = BTreeMap::<String, AddressTxRef>::new();
            let mut warnings = Vec::new();
            for address in &cluster.addresses {
                self.collect_refs_for_range(
                    tx_source.as_ref(),
                    cluster.chain_id,
                    &address.address,
                    start_block,
                    last_indexed_block,
                    self.default_query_limit.max(1),
                    &mut tx_map,
                    &mut warnings,
                )
                .await?;
            }

            if tx_map.is_empty() {
                self.sync_tracking_state_no_hits(&tracking, &cluster, last_indexed_block)
                    .await?;
                continue;
            }

            self.enqueue_cluster(
                cluster.cluster_id,
                ClusterBackfillRequest {
                    start_block: Some(start_block),
                    end_block: Some(last_indexed_block),
                    force_reanalyze: false,
                    query_limit: Some(self.default_query_limit),
                },
            )
            .await?;
            enqueued += 1;
        }

        Ok(enqueued)
    }

    async fn sync_tracking_state_no_hits(
        &self,
        tracking: &ClusterTrackingState,
        cluster: &crate::domain::entities::AddressCluster,
        last_processed_block: u64,
    ) -> LedgerResult<()> {
        let repository = &self.analysis_service.repository;
        let now = Utc::now();
        let state = ClusterTrackingState {
            cluster_id: tracking.cluster_id,
            chain_id: cluster.chain_id,
            status: tracking.status.clone(),
            last_processed_block: Some(last_processed_block),
            last_processed_at: Some(now),
            last_updated_at: Some(now),
            error_message: None,
            tracked_address_count: cluster.addresses.len() as u32,
            created_at: tracking.created_at,
            updated_at: now,
            metadata: json!({
                "source": "cluster_watch_probe",
                "result": "no_new_transactions",
            }),
        };
        repository.upsert_cluster_tracking_state(&state).await
    }

    async fn sync_tracking_state_success(
        &self,
        run: &ClusterBackfillRun,
        cluster: &crate::domain::entities::AddressCluster,
    ) -> LedgerResult<()> {
        let repository = &self.analysis_service.repository;
        let now = Utc::now();
        let existing = repository
            .get_cluster_tracking_state(run.cluster_id)
            .await?;
        let state = ClusterTrackingState {
            cluster_id: run.cluster_id,
            chain_id: cluster.chain_id,
            status: existing
                .as_ref()
                .map(|item| item.status.clone())
                .unwrap_or(ClusterTrackingStatus::Paused),
            last_processed_block: run.end_block,
            last_processed_at: Some(now),
            last_updated_at: Some(now),
            error_message: None,
            tracked_address_count: cluster.addresses.len() as u32,
            created_at: existing.as_ref().map(|item| item.created_at).unwrap_or(now),
            updated_at: now,
            metadata: json!({
                "source": "cluster_backfill_worker",
                "run_id": run.run_id,
            }),
        };
        repository.upsert_cluster_tracking_state(&state).await
    }

    async fn sync_tracking_state_failure(
        &self,
        run: &ClusterBackfillRun,
        error: &LedgerError,
    ) -> LedgerResult<()> {
        let repository = &self.analysis_service.repository;
        let Some(existing) = repository
            .get_cluster_tracking_state(run.cluster_id)
            .await?
        else {
            return Ok(());
        };
        let now = Utc::now();
        let state = ClusterTrackingState {
            status: ClusterTrackingStatus::Error,
            error_message: Some(error.to_string()),
            last_updated_at: Some(now),
            updated_at: now,
            metadata: json!({
                "source": "cluster_backfill_worker",
                "last_failed_run_id": run.run_id,
            }),
            ..existing
        };
        repository.upsert_cluster_tracking_state(&state).await
    }

    async fn expand_cluster_from_seeds(
        &self,
        run: &ClusterBackfillRun,
        cluster: AddressCluster,
        _tx_source: &dyn ClusterTxSource,
    ) -> LedgerResult<AddressCluster> {
        let Some(discovery_service) = self.discovery_service.as_ref() else {
            return Ok(cluster);
        };
        let seed_addresses = seed_addresses_for_cluster(&cluster);
        if seed_addresses.is_empty() || auto_discovery_is_current(&cluster, &seed_addresses) {
            return Ok(cluster);
        }

        let mut responses = Vec::new();
        let mut warnings = Vec::new();
        for seed_address in &seed_addresses {
            match discovery_service
                .discover(ClusterDiscoveryRequest {
                    chain_id: cluster.chain_id,
                    seed_address: seed_address.clone(),
                    max_hops: AUTO_DISCOVERY_MAX_HOPS,
                    max_nodes: Some(AUTO_DISCOVERY_MAX_NODES),
                    max_windows: Some(AUTO_DISCOVERY_MAX_WINDOWS),
                })
                .await
            {
                Ok(response) => responses.push(response),
                Err(err) => warnings.push(format!("{seed_address}: {err}")),
            }
        }

        if responses.is_empty() {
            return Ok(cluster);
        }

        let mut existing = cluster
            .addresses
            .iter()
            .map(|item| (item.address.to_ascii_lowercase(), item.clone()))
            .collect::<BTreeMap<_, _>>();
        let mut suggestions = Vec::new();
        let mut auto_added = Vec::new();

        for response in &responses {
            warnings.extend(response.warnings.clone());
            let mut candidates = response
                .nodes
                .iter()
                .filter(|node| node.first_seen_hop > 0)
                .filter(|node| !seed_addresses.iter().any(|seed| seed == &node.address))
                .collect::<Vec<_>>();
            candidates.sort_by(|left, right| {
                right
                    .confidence
                    .partial_cmp(&left.confidence)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| right.outbound_edge_count.cmp(&left.outbound_edge_count))
                    .then_with(|| right.inbound_edge_count.cmp(&left.inbound_edge_count))
            });

            for node in candidates {
                let address = node.address.to_ascii_lowercase();
                let already_in_cluster = existing.contains_key(&address);
                let should_auto_add = auto_added.len() < AUTO_DISCOVERY_MAX_AUTO_ADD
                    && !already_in_cluster
                    && node.confidence >= AUTO_DISCOVERY_MIN_AUTO_ADD_CONFIDENCE
                    && matches!(
                        node.address_type,
                        DiscoveredAddressType::Eoa | DiscoveredAddressType::Unknown
                    )
                    && node.relation_type == "inbound_to_seed";
                let suggestion = json!({
                    "address": address,
                    "role": node.role,
                    "relation_type": node.relation_type,
                    "confidence": node.confidence,
                    "address_type": node.address_type,
                    "evidence_tx_hashes": node.evidence_tx_hashes,
                    "reason": node.reason,
                    "seed_address": response.seed_address,
                    "auto_add": should_auto_add,
                    "already_in_cluster": already_in_cluster,
                });
                suggestions.push(suggestion);

                if should_auto_add {
                    let role = suggested_cluster_role(&node.role, &node.relation_type);
                    let cluster_address = ClusterAddress {
                        address: address.clone(),
                        label: Some(format!("auto-{}", role)),
                        role: Some(role),
                        metadata: json!({
                            "source": "auto_seed_discovery",
                            "seed_address": response.seed_address,
                            "relation_type": node.relation_type,
                            "confidence": node.confidence,
                            "address_type": node.address_type,
                            "evidence_tx_hashes": node.evidence_tx_hashes,
                            "reason": node.reason,
                        }),
                    };
                    existing.insert(address.clone(), cluster_address);
                    auto_added.push(address);
                }
            }
        }

        let mut addresses = existing.into_values().collect::<Vec<_>>();
        addresses.sort_by(|left, right| {
            address_sort_key(left)
                .cmp(&address_sort_key(right))
                .then_with(|| left.address.cmp(&right.address))
        });

        let mut metadata = cluster.metadata.as_object().cloned().unwrap_or_default();
        metadata.insert("seed_addresses".to_owned(), json!(seed_addresses));
        metadata.insert(
            "auto_discovery".to_owned(),
            json!({
                "status": "completed",
                "source": "cluster_backfill_worker",
                "run_id": run.run_id,
                "max_hops": AUTO_DISCOVERY_MAX_HOPS,
                "max_nodes": AUTO_DISCOVERY_MAX_NODES,
                "max_windows": AUTO_DISCOVERY_MAX_WINDOWS,
                "auto_added_addresses": auto_added,
                "suggested_addresses": suggestions,
                "warnings": warnings,
                "updated_at": Utc::now(),
            }),
        );

        let updated = AddressCluster {
            addresses,
            metadata: Value::Object(metadata),
            ..cluster
        };
        self.analysis_service
            .repository
            .create_cluster(&updated)
            .await?;
        self.analysis_service
            .repository
            .replace_cluster_addresses(updated.cluster_id, &updated.addresses)
            .await?;
        Ok(updated)
    }

    fn tx_address_for_cluster(
        &self,
        tx_ref: &AddressTxRef,
        cluster: &crate::domain::entities::AddressCluster,
    ) -> String {
        if cluster
            .addresses
            .iter()
            .any(|item| item.address.eq_ignore_ascii_case(&tx_ref.address))
        {
            return tx_ref.address.clone();
        }
        cluster
            .addresses
            .first()
            .map(|item| item.address.clone())
            .unwrap_or_else(|| tx_ref.address.clone())
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
            self.sync_tracking_state_failure(&run, &err).await?;
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
            .ok_or_else(|| {
                LedgerError::NotFound(format!("cluster {} not found", run.cluster_id))
            })?;
        let cluster = self
            .expand_cluster_from_seeds(run, cluster, tx_source)
            .await?;
        run.address_count = cluster.addresses.len() as u32;

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

        let query_limit = request
            .query_limit
            .unwrap_or(self.default_query_limit)
            .max(1);
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
        repository
            .upsert_cluster_discovered_txs(&discovered)
            .await?;

        let mut analyzed_count = 0u32;
        let mut failed_count = 0u32;
        let mut classified = Vec::new();
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
                    .get_analysis(chain_id, &tx_ref.tx_hash)
                    .await?
                    .is_some()
            {
                if let Some(existing_analysis) =
                    repository.get_analysis(chain_id, &tx_ref.tx_hash).await?
                {
                    classified.push(self.behavior_classifier.classify(
                        &cluster,
                        &self.tx_address_for_cluster(tx_ref, &cluster),
                        &existing_analysis,
                    ));
                }
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
                Ok((analysis, _)) => {
                    classified.push(self.behavior_classifier.classify(
                        &cluster,
                        &self.tx_address_for_cluster(tx_ref, &cluster),
                        &analysis,
                    ));
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

        if !classified.is_empty() {
            repository
                .upsert_classified_transactions(&classified)
                .await?;
        }
        self.metrics_service
            .refresh_cluster_metrics(run.cluster_id)
            .await?;
        self.alert_service
            .refresh_cluster_alerts(run.cluster_id)
            .await?;
        let _ = self
            .strategy_analysis_service
            .refresh_cluster_report_snapshot(run.cluster_id, None, None)
            .await;

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
        self.sync_tracking_state_success(run, &cluster).await?;

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

fn seed_addresses_for_cluster(cluster: &AddressCluster) -> Vec<String> {
    let mut seeds = cluster
        .addresses
        .iter()
        .filter(|address| {
            address
                .metadata
                .get("auto_discovery_seed")
                .and_then(Value::as_bool)
                .unwrap_or(false)
                || address.role.as_deref() == Some("seed")
                || address.role.as_deref() == Some("collector")
        })
        .map(|address| address.address.to_ascii_lowercase())
        .collect::<Vec<_>>();
    if seeds.is_empty() {
        seeds = cluster
            .addresses
            .iter()
            .map(|address| address.address.to_ascii_lowercase())
            .collect();
    }
    seeds.sort();
    seeds.dedup();
    seeds
}

fn auto_discovery_is_current(cluster: &AddressCluster, seed_addresses: &[String]) -> bool {
    let Some(discovery) = cluster.metadata.get("auto_discovery") else {
        return false;
    };
    if discovery.get("status").and_then(Value::as_str) != Some("completed") {
        return false;
    }
    let Some(stored_seeds) = cluster
        .metadata
        .get("seed_addresses")
        .and_then(Value::as_array)
    else {
        return false;
    };
    let mut stored = stored_seeds
        .iter()
        .filter_map(Value::as_str)
        .map(str::to_ascii_lowercase)
        .collect::<Vec<_>>();
    stored.sort();
    stored.dedup();
    stored == seed_addresses
}

fn suggested_cluster_role(role: &str, relation_type: &str) -> String {
    match (role, relation_type) {
        (_, "inbound_to_seed") => "executor".to_owned(),
        (_, "outbound_from_seed") => "related_wallet".to_owned(),
        ("source_or_executor", _) => "executor".to_owned(),
        _ => "related_wallet".to_owned(),
    }
}

fn address_sort_key(address: &ClusterAddress) -> u8 {
    if address
        .metadata
        .get("auto_discovery_seed")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        0
    } else if address.metadata.get("source").and_then(Value::as_str) == Some("auto_seed_discovery")
    {
        2
    } else {
        1
    }
}
