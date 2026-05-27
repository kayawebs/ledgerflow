use std::collections::HashMap;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::core::errors::LedgerResult;
use crate::domain::{
    entities::{
        AddressAnalysisCandidate, AddressAnalysisJob, AddressCluster, AddressLabel, AddressProfile,
        ClassifiedTransaction, ClusterAddress, ClusterAlert, ClusterBackfillRun, ClusterCoverage,
        ClusterDiscoveredTx, ClusterMetricsSnapshot, ClusterTrackingState, EvidencePack,
        ProtocolAddressMetadata, TransactionAnalysis,
    },
    enums::{ClusterBackfillStatus, ClusterTrackingStatus, ClusterTxStatus},
    interfaces::AnalysisRepository,
};
use crate::ingestion::models::RawTransactionBundle;

#[derive(Default)]
pub struct InMemoryAnalysisRepository {
    bundles: RwLock<HashMap<(u64, String), RawTransactionBundle>>,
    analyses: RwLock<HashMap<(u64, String), TransactionAnalysis>>,
    packs: RwLock<HashMap<String, EvidencePack>>,
    clusters: RwLock<HashMap<Uuid, AddressCluster>>,
    cluster_backfill_runs: RwLock<HashMap<Uuid, Vec<ClusterBackfillRun>>>,
    cluster_discovered_txs: RwLock<HashMap<(Uuid, String), ClusterDiscoveredTx>>,
    cluster_tracking_states: RwLock<HashMap<Uuid, ClusterTrackingState>>,
    classified_transactions: RwLock<HashMap<(Uuid, String), ClassifiedTransaction>>,
    cluster_metrics_snapshots: RwLock<HashMap<Uuid, Vec<ClusterMetricsSnapshot>>>,
    cluster_alerts: RwLock<HashMap<Uuid, Vec<ClusterAlert>>>,
    address_profiles: RwLock<HashMap<(u64, String), AddressProfile>>,
    address_labels: RwLock<HashMap<(u64, String, String, String), AddressLabel>>,
    protocol_metadata:
        RwLock<HashMap<(u64, String, String, String, String), ProtocolAddressMetadata>>,
    address_jobs: RwLock<HashMap<(u64, String), AddressAnalysisJob>>,
}

#[async_trait]
impl AnalysisRepository for InMemoryAnalysisRepository {
    async fn save_transaction_bundle(&self, bundle: &RawTransactionBundle) -> LedgerResult<()> {
        self.bundles.write().await.insert(
            (bundle.tx.chain_id, bundle.tx.tx_hash.clone()),
            bundle.clone(),
        );
        Ok(())
    }

    async fn save_analysis(&self, analysis: &TransactionAnalysis) -> LedgerResult<()> {
        self.analyses.write().await.insert(
            (analysis.chain_id, analysis.tx_hash.clone()),
            analysis.clone(),
        );
        Ok(())
    }

    async fn save_evidence_pack(&self, pack: &EvidencePack) -> LedgerResult<()> {
        self.packs
            .write()
            .await
            .insert(pack.pack_id.to_string(), pack.clone());
        Ok(())
    }

    async fn get_analysis(
        &self,
        chain_id: u64,
        tx_hash: &str,
    ) -> LedgerResult<Option<TransactionAnalysis>> {
        Ok(self
            .analyses
            .read()
            .await
            .get(&(chain_id, tx_hash.to_owned()))
            .cloned())
    }

    async fn get_evidence_pack(&self, pack_id: &str) -> LedgerResult<Option<EvidencePack>> {
        Ok(self.packs.read().await.get(pack_id).cloned())
    }

    async fn get_evidence_pack_by_tx(
        &self,
        _chain_id: u64,
        tx_hash: &str,
    ) -> LedgerResult<Option<EvidencePack>> {
        Ok(self
            .packs
            .read()
            .await
            .values()
            .find(|pack| pack.tx_hash.as_deref() == Some(tx_hash))
            .cloned())
    }

    async fn create_cluster(&self, cluster: &AddressCluster) -> LedgerResult<()> {
        self.clusters
            .write()
            .await
            .insert(cluster.cluster_id, cluster.clone());
        Ok(())
    }

    async fn replace_cluster_addresses(
        &self,
        cluster_id: Uuid,
        addresses: &[ClusterAddress],
    ) -> LedgerResult<()> {
        if let Some(cluster) = self.clusters.write().await.get_mut(&cluster_id) {
            cluster.addresses = addresses.to_vec();
        }
        Ok(())
    }

    async fn delete_cluster(&self, cluster_id: Uuid) -> LedgerResult<()> {
        self.clusters.write().await.remove(&cluster_id);
        self.cluster_backfill_runs.write().await.remove(&cluster_id);
        self.cluster_tracking_states
            .write()
            .await
            .remove(&cluster_id);
        self.cluster_metrics_snapshots
            .write()
            .await
            .remove(&cluster_id);
        self.cluster_alerts.write().await.remove(&cluster_id);
        self.cluster_discovered_txs
            .write()
            .await
            .retain(|(stored_cluster_id, _), _| *stored_cluster_id != cluster_id);
        self.classified_transactions
            .write()
            .await
            .retain(|(stored_cluster_id, _), _| *stored_cluster_id != cluster_id);
        Ok(())
    }

    async fn get_cluster(&self, cluster_id: Uuid) -> LedgerResult<Option<AddressCluster>> {
        Ok(self.clusters.read().await.get(&cluster_id).cloned())
    }

    async fn list_clusters(&self) -> LedgerResult<Vec<AddressCluster>> {
        Ok(self.clusters.read().await.values().cloned().collect())
    }

    async fn list_analyses_for_cluster(
        &self,
        chain_id: u64,
        addresses: &[String],
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
    ) -> LedgerResult<Vec<TransactionAnalysis>> {
        let address_set = addresses
            .iter()
            .map(|item| item.to_lowercase())
            .collect::<Vec<_>>();
        let analyses = self
            .analyses
            .read()
            .await
            .values()
            .filter(|analysis| analysis.chain_id == chain_id)
            .filter(|analysis| {
                address_set.contains(&analysis.from_address.to_lowercase())
                    || analysis
                        .to_address
                        .as_ref()
                        .map(|item| address_set.contains(&item.to_lowercase()))
                        .unwrap_or(false)
            })
            .filter(|analysis| within_range(analysis.block_timestamp, start, end))
            .cloned()
            .collect::<Vec<_>>();
        Ok(analyses)
    }

    async fn create_cluster_backfill_run(&self, run: &ClusterBackfillRun) -> LedgerResult<()> {
        let mut runs = self.cluster_backfill_runs.write().await;
        runs.entry(run.cluster_id).or_default().push(run.clone());
        Ok(())
    }

    async fn update_cluster_backfill_run(&self, run: &ClusterBackfillRun) -> LedgerResult<()> {
        let mut runs = self.cluster_backfill_runs.write().await;
        let entry = runs.entry(run.cluster_id).or_default();
        if let Some(existing) = entry.iter_mut().find(|item| item.run_id == run.run_id) {
            *existing = run.clone();
        } else {
            entry.push(run.clone());
        }
        entry.sort_by(|left, right| left.requested_at.cmp(&right.requested_at));
        Ok(())
    }

    async fn list_cluster_backfill_runs(
        &self,
        cluster_id: Uuid,
    ) -> LedgerResult<Vec<ClusterBackfillRun>> {
        let mut runs = self
            .cluster_backfill_runs
            .read()
            .await
            .get(&cluster_id)
            .cloned()
            .unwrap_or_default();
        runs.sort_by(|left, right| right.requested_at.cmp(&left.requested_at));
        Ok(runs)
    }

    async fn claim_next_pending_cluster_backfill_run(
        &self,
    ) -> LedgerResult<Option<ClusterBackfillRun>> {
        let mut runs = self.cluster_backfill_runs.write().await;
        let mut selected: Option<(Uuid, usize, chrono::DateTime<Utc>)> = None;

        for (cluster_id, cluster_runs) in runs.iter() {
            for (index, run) in cluster_runs.iter().enumerate() {
                if run.status != ClusterBackfillStatus::Pending {
                    continue;
                }
                match selected {
                    Some((_, _, requested_at)) if run.requested_at >= requested_at => {}
                    _ => selected = Some((*cluster_id, index, run.requested_at)),
                }
            }
        }

        let Some((cluster_id, index, _)) = selected else {
            return Ok(None);
        };

        let run = runs
            .get_mut(&cluster_id)
            .and_then(|cluster_runs| cluster_runs.get_mut(index))
            .map(|run| {
                run.status = ClusterBackfillStatus::Running;
                if run.started_at.is_none() {
                    run.started_at = Some(Utc::now());
                }
                run.clone()
            });
        Ok(run)
    }

    async fn upsert_cluster_discovered_txs(&self, txs: &[ClusterDiscoveredTx]) -> LedgerResult<()> {
        let mut store = self.cluster_discovered_txs.write().await;
        for tx in txs {
            let key = (tx.cluster_id, tx.tx_hash.clone());
            store
                .entry(key)
                .and_modify(|existing| {
                    existing.last_seen_at = tx.last_seen_at;
                    existing.block_number = tx.block_number.or(existing.block_number);
                    existing.latest_run_id = tx.latest_run_id;
                    existing.metadata = tx.metadata.clone();
                })
                .or_insert_with(|| tx.clone());
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
        let mut store = self.cluster_discovered_txs.write().await;
        if let Some(existing) = store.get_mut(&(cluster_id, tx_hash.to_owned())) {
            existing.status = status;
            existing.latest_run_id = Some(latest_run_id);
            existing.analysis_attempted_at = Some(Utc::now());
            existing.analyzed_at = analyzed_at;
            existing.last_error = error;
            existing.last_seen_at = Utc::now();
        }
        Ok(())
    }

    async fn get_cluster_coverage(
        &self,
        cluster_id: Uuid,
    ) -> LedgerResult<Option<ClusterCoverage>> {
        let cluster = self.clusters.read().await.get(&cluster_id).cloned();
        let Some(cluster) = cluster else {
            return Ok(None);
        };
        let store = self.cluster_discovered_txs.read().await;
        let relevant = store
            .values()
            .filter(|tx| tx.cluster_id == cluster_id)
            .cloned()
            .collect::<Vec<_>>();
        let discovered_tx_count = relevant.len() as u32;
        let analyzed_tx_count = relevant
            .iter()
            .filter(|tx| tx.status == ClusterTxStatus::Analyzed)
            .count() as u32;
        let failed_tx_count = relevant
            .iter()
            .filter(|tx| tx.status == ClusterTxStatus::Failed)
            .count() as u32;
        let pending_tx_count = relevant
            .iter()
            .filter(|tx| tx.status == ClusterTxStatus::Pending)
            .count() as u32;

        let latest_run = self
            .cluster_backfill_runs
            .read()
            .await
            .get(&cluster_id)
            .and_then(|runs| runs.iter().max_by_key(|run| run.requested_at).cloned());

        Ok(Some(ClusterCoverage {
            cluster_id,
            chain_id: cluster.chain_id,
            discovered_tx_count,
            analyzed_tx_count,
            failed_tx_count,
            pending_tx_count,
            latest_run_id: latest_run.as_ref().map(|run| run.run_id),
            latest_run_status: latest_run.as_ref().map(|run| run.status.clone()),
            latest_run_started_at: latest_run.as_ref().and_then(|run| run.started_at),
            latest_run_finished_at: latest_run.as_ref().and_then(|run| run.finished_at),
            metadata: json!({
                "coverage_mode": "cluster_discovered_txs",
                "has_backfill_runs": latest_run.is_some(),
            }),
        }))
    }

    async fn upsert_cluster_tracking_state(
        &self,
        state: &ClusterTrackingState,
    ) -> LedgerResult<()> {
        self.cluster_tracking_states
            .write()
            .await
            .insert(state.cluster_id, state.clone());
        Ok(())
    }

    async fn get_cluster_tracking_state(
        &self,
        cluster_id: Uuid,
    ) -> LedgerResult<Option<ClusterTrackingState>> {
        Ok(self
            .cluster_tracking_states
            .read()
            .await
            .get(&cluster_id)
            .cloned())
    }

    async fn list_active_cluster_tracking_states(&self) -> LedgerResult<Vec<ClusterTrackingState>> {
        Ok(self
            .cluster_tracking_states
            .read()
            .await
            .values()
            .filter(|state| state.status == ClusterTrackingStatus::Active)
            .cloned()
            .collect())
    }

    async fn upsert_classified_transactions(
        &self,
        txs: &[ClassifiedTransaction],
    ) -> LedgerResult<()> {
        let mut store = self.classified_transactions.write().await;
        for tx in txs {
            store.insert((tx.cluster_id, tx.tx_hash.clone()), tx.clone());
        }
        Ok(())
    }

    async fn list_recent_classified_transactions(
        &self,
        cluster_id: Uuid,
        limit: u32,
    ) -> LedgerResult<Vec<ClassifiedTransaction>> {
        let mut items = self
            .classified_transactions
            .read()
            .await
            .values()
            .filter(|item| item.cluster_id == cluster_id)
            .cloned()
            .collect::<Vec<_>>();
        items.sort_by(|left, right| {
            right
                .timestamp
                .cmp(&left.timestamp)
                .then_with(|| right.block_number.cmp(&left.block_number))
        });
        items.truncate(limit as usize);
        Ok(items)
    }

    async fn list_classified_transactions_page(
        &self,
        cluster_id: Uuid,
        limit: u32,
        offset: u32,
        behavior_tag: Option<crate::domain::enums::BehaviorTag>,
        protocol_query: Option<String>,
        sort_by: String,
        descending: bool,
    ) -> LedgerResult<(Vec<ClassifiedTransaction>, u64)> {
        let mut items = self
            .classified_transactions
            .read()
            .await
            .values()
            .filter(|item| item.cluster_id == cluster_id)
            .filter(|item| {
                behavior_tag
                    .as_ref()
                    .map(|value| &item.behavior_tag == value)
                    .unwrap_or(true)
            })
            .filter(|item| {
                protocol_query
                    .as_ref()
                    .map(|value| {
                        item.protocol
                            .as_deref()
                            .unwrap_or_default()
                            .to_ascii_lowercase()
                            .contains(&value.to_ascii_lowercase())
                    })
                    .unwrap_or(true)
            })
            .cloned()
            .collect::<Vec<_>>();
        items.sort_by(|left, right| compare_classified_tx(left, right, &sort_by, descending));
        let total = items.len() as u64;
        let page_items = items
            .into_iter()
            .skip(offset as usize)
            .take(limit as usize)
            .collect::<Vec<_>>();
        Ok((page_items, total))
    }

    async fn list_classified_transactions_for_cluster(
        &self,
        cluster_id: Uuid,
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
    ) -> LedgerResult<Vec<ClassifiedTransaction>> {
        let mut items = self
            .classified_transactions
            .read()
            .await
            .values()
            .filter(|item| item.cluster_id == cluster_id)
            .filter(|item| within_range(item.timestamp, start, end))
            .cloned()
            .collect::<Vec<_>>();
        items.sort_by(|left, right| {
            right
                .timestamp
                .cmp(&left.timestamp)
                .then_with(|| right.block_number.cmp(&left.block_number))
        });
        Ok(items)
    }

    async fn replace_cluster_metrics_snapshots(
        &self,
        cluster_id: Uuid,
        snapshots: &[ClusterMetricsSnapshot],
    ) -> LedgerResult<()> {
        self.cluster_metrics_snapshots
            .write()
            .await
            .insert(cluster_id, snapshots.to_vec());
        Ok(())
    }

    async fn list_cluster_metrics_snapshots(
        &self,
        cluster_id: Uuid,
    ) -> LedgerResult<Vec<ClusterMetricsSnapshot>> {
        Ok(self
            .cluster_metrics_snapshots
            .read()
            .await
            .get(&cluster_id)
            .cloned()
            .unwrap_or_default())
    }

    async fn insert_cluster_alerts(&self, alerts: &[ClusterAlert]) -> LedgerResult<()> {
        let mut store = self.cluster_alerts.write().await;
        for alert in alerts {
            let entry = store.entry(alert.cluster_id).or_default();
            if let Some(existing) = entry
                .iter_mut()
                .find(|item| item.alert_id == alert.alert_id)
            {
                *existing = alert.clone();
            } else {
                entry.push(alert.clone());
            }
        }
        Ok(())
    }

    async fn list_cluster_alerts(
        &self,
        cluster_id: Uuid,
        include_read: bool,
        limit: u32,
    ) -> LedgerResult<Vec<ClusterAlert>> {
        let mut alerts = self
            .cluster_alerts
            .read()
            .await
            .get(&cluster_id)
            .cloned()
            .unwrap_or_default();
        alerts.retain(|alert| include_read || !alert.is_read);
        alerts.sort_by(|left, right| right.triggered_at.cmp(&left.triggered_at));
        alerts.truncate(limit as usize);
        Ok(alerts)
    }

    async fn mark_cluster_alert_read(
        &self,
        cluster_id: Uuid,
        alert_id: Uuid,
    ) -> LedgerResult<Option<ClusterAlert>> {
        let mut store = self.cluster_alerts.write().await;
        let Some(alerts) = store.get_mut(&cluster_id) else {
            return Ok(None);
        };
        let Some(alert) = alerts.iter_mut().find(|item| item.alert_id == alert_id) else {
            return Ok(None);
        };
        alert.is_read = true;
        alert.read_at = Some(Utc::now());
        Ok(Some(alert.clone()))
    }

    async fn enqueue_address_analysis_candidates(
        &self,
        chain_id: u64,
        candidates: &[AddressAnalysisCandidate],
    ) -> LedgerResult<u64> {
        let mut jobs = self.address_jobs.write().await;
        let now = Utc::now();
        let mut changed = 0;
        for candidate in candidates {
            let address = candidate.address.to_ascii_lowercase();
            let key = (chain_id, address.clone());
            let manual_lookup = candidate
                .metadata
                .get("source")
                .and_then(|value| value.as_str())
                .map(|value| matches!(value, "address_summary" | "address_analysis_page"))
                .unwrap_or(false);
            let portfolio_refresh = candidate
                .metadata
                .get("roles")
                .and_then(Value::as_array)
                .map(|roles| {
                    roles
                        .iter()
                        .any(|role| role.as_str() == Some("portfolio_refresh"))
                })
                .unwrap_or(false);
            match jobs.get_mut(&key) {
                Some(existing)
                    if existing.status != "completed" || manual_lookup || portfolio_refresh =>
                {
                    existing.last_seen_at = now;
                    existing.updated_at = now;
                    existing.priority = existing.priority.min(candidate.priority);
                    existing.metadata =
                        merge_json(existing.metadata.clone(), candidate.metadata.clone());
                    if existing.status == "failed"
                        || (existing.status == "completed" && (manual_lookup || portfolio_refresh))
                    {
                        existing.status = "pending".to_owned();
                        existing.available_at = now;
                        existing.last_error = None;
                    }
                    changed += 1;
                }
                Some(_) => {}
                None => {
                    jobs.insert(
                        key,
                        AddressAnalysisJob {
                            chain_id,
                            address,
                            status: "pending".to_owned(),
                            attempts: 0,
                            last_error: None,
                            priority: candidate.priority,
                            first_seen_at: now,
                            last_seen_at: now,
                            available_at: now,
                            claimed_at: None,
                            completed_at: None,
                            updated_at: now,
                            metadata: candidate.metadata.clone(),
                        },
                    );
                    changed += 1;
                }
            }
        }
        Ok(changed)
    }

    async fn claim_next_address_analysis_job(&self) -> LedgerResult<Option<AddressAnalysisJob>> {
        let mut jobs = self.address_jobs.write().await;
        let now = Utc::now();
        let selected = jobs
            .iter()
            .filter(|(_, job)| job.status == "pending" && job.available_at <= now)
            .min_by(|(_, left), (_, right)| {
                left.priority
                    .cmp(&right.priority)
                    .then_with(|| left.first_seen_at.cmp(&right.first_seen_at))
            })
            .map(|(key, _)| key.clone());
        let Some(key) = selected else {
            return Ok(None);
        };
        let Some(job) = jobs.get_mut(&key) else {
            return Ok(None);
        };
        job.status = "running".to_owned();
        job.attempts += 1;
        job.claimed_at = Some(now);
        job.updated_at = now;
        Ok(Some(job.clone()))
    }

    async fn upsert_address_profile(&self, profile: &AddressProfile) -> LedgerResult<()> {
        self.address_profiles.write().await.insert(
            (profile.chain_id, profile.address.to_ascii_lowercase()),
            profile.clone(),
        );
        Ok(())
    }

    async fn upsert_address_labels(&self, labels: &[AddressLabel]) -> LedgerResult<()> {
        let mut store = self.address_labels.write().await;
        for label in labels {
            store.insert(
                (
                    label.chain_id,
                    label.address.to_ascii_lowercase(),
                    label.label.clone(),
                    label.source.clone(),
                ),
                label.clone(),
            );
        }
        Ok(())
    }

    async fn upsert_protocol_address_metadata(
        &self,
        metadata: &[ProtocolAddressMetadata],
    ) -> LedgerResult<()> {
        let mut store = self.protocol_metadata.write().await;
        for item in metadata {
            store.insert(
                (
                    item.chain_id,
                    item.address.to_ascii_lowercase(),
                    item.protocol_family.clone(),
                    item.protocol_name.clone(),
                    item.contract_role.clone(),
                ),
                item.clone(),
            );
        }
        Ok(())
    }

    async fn complete_address_analysis_job(
        &self,
        chain_id: u64,
        address: &str,
    ) -> LedgerResult<()> {
        let mut jobs = self.address_jobs.write().await;
        if let Some(job) = jobs.get_mut(&(chain_id, address.to_ascii_lowercase())) {
            let now = Utc::now();
            job.status = "completed".to_owned();
            job.completed_at = Some(now);
            job.updated_at = now;
            job.last_error = None;
        }
        Ok(())
    }

    async fn fail_address_analysis_job(
        &self,
        chain_id: u64,
        address: &str,
        error: &str,
    ) -> LedgerResult<()> {
        let mut jobs = self.address_jobs.write().await;
        if let Some(job) = jobs.get_mut(&(chain_id, address.to_ascii_lowercase())) {
            let now = Utc::now();
            job.status = "failed".to_owned();
            job.last_error = Some(error.to_owned());
            job.available_at = now + chrono::Duration::minutes(10);
            job.updated_at = now;
        }
        Ok(())
    }

    async fn list_address_profiles(
        &self,
        chain_id: u64,
        addresses: &[String],
    ) -> LedgerResult<Vec<AddressProfile>> {
        let store = self.address_profiles.read().await;
        Ok(addresses
            .iter()
            .filter_map(|address| {
                store
                    .get(&(chain_id, address.to_ascii_lowercase()))
                    .cloned()
            })
            .collect())
    }

    async fn list_address_labels(
        &self,
        chain_id: u64,
        addresses: &[String],
    ) -> LedgerResult<Vec<AddressLabel>> {
        let address_set = addresses
            .iter()
            .map(|address| address.to_ascii_lowercase())
            .collect::<Vec<_>>();
        Ok(self
            .address_labels
            .read()
            .await
            .values()
            .filter(|label| label.chain_id == chain_id)
            .filter(|label| address_set.contains(&label.address.to_ascii_lowercase()))
            .cloned()
            .collect())
    }
}

fn within_range(
    timestamp: Option<DateTime<Utc>>,
    start: Option<DateTime<Utc>>,
    end: Option<DateTime<Utc>>,
) -> bool {
    let Some(timestamp) = timestamp else {
        return true;
    };
    if let Some(start) = start {
        if timestamp < start {
            return false;
        }
    }
    if let Some(end) = end {
        if timestamp > end {
            return false;
        }
    }
    true
}

fn compare_classified_tx(
    left: &ClassifiedTransaction,
    right: &ClassifiedTransaction,
    sort_by: &str,
    descending: bool,
) -> std::cmp::Ordering {
    let ordering = match sort_by {
        "value_usd" => partial_cmp_option_f64(left.value_usd, right.value_usd),
        "gas_cost_usd" => partial_cmp_option_f64(left.gas_cost_usd, right.gas_cost_usd),
        "estimated_pnl_usd" => {
            partial_cmp_option_f64(left.estimated_pnl_usd, right.estimated_pnl_usd)
        }
        "block_number" => left.block_number.cmp(&right.block_number),
        _ => left
            .timestamp
            .cmp(&right.timestamp)
            .then(left.block_number.cmp(&right.block_number)),
    };
    if descending {
        ordering.reverse()
    } else {
        ordering
    }
}

fn partial_cmp_option_f64(left: Option<f64>, right: Option<f64>) -> std::cmp::Ordering {
    match (left, right) {
        (Some(left), Some(right)) => left
            .partial_cmp(&right)
            .unwrap_or(std::cmp::Ordering::Equal),
        (Some(_), None) => std::cmp::Ordering::Greater,
        (None, Some(_)) => std::cmp::Ordering::Less,
        (None, None) => std::cmp::Ordering::Equal,
    }
}

fn merge_json(left: serde_json::Value, right: serde_json::Value) -> serde_json::Value {
    match (left, right) {
        (serde_json::Value::Object(mut left), serde_json::Value::Object(right)) => {
            for (key, value) in right {
                left.insert(key, value);
            }
            serde_json::Value::Object(left)
        }
        (_, right) => right,
    }
}
