use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde_json::json;
use uuid::Uuid;

use crate::core::errors::{LedgerError, LedgerResult};
use crate::domain::entities::{
    AddressCluster, ClusterAddress, ClusterSpendBreakdown, ClusterSpendSummary,
    ClusterTransactionSummary, TransactionAnalysis,
};
use crate::domain::enums::{AssetType, SpendCategory};
use crate::domain::interfaces::AnalysisRepository;

#[derive(Clone)]
pub struct ClusterService {
    repository: Arc<dyn AnalysisRepository>,
}

impl ClusterService {
    pub fn new(repository: Arc<dyn AnalysisRepository>) -> Self {
        Self { repository }
    }

    pub async fn create_cluster(
        &self,
        chain_id: u64,
        owner_label: &str,
        name: &str,
        description: Option<String>,
        addresses: Vec<ClusterAddress>,
    ) -> LedgerResult<AddressCluster> {
        if addresses.is_empty() {
            return Err(LedgerError::AnalysisInvariant(
                "cluster must contain at least one address".to_owned(),
            ));
        }
        let cluster = AddressCluster {
            cluster_id: Uuid::new_v4(),
            chain_id,
            owner_label: owner_label.to_owned(),
            name: name.to_owned(),
            description,
            addresses: normalize_addresses(addresses),
            metadata: json!({}),
        };
        self.repository.create_cluster(&cluster).await?;
        self.repository
            .replace_cluster_addresses(cluster.cluster_id, &cluster.addresses)
            .await?;
        Ok(cluster)
    }

    pub async fn get_cluster_summary(
        &self,
        cluster_id: Uuid,
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
    ) -> LedgerResult<ClusterSpendSummary> {
        let cluster = self
            .repository
            .get_cluster(cluster_id)
            .await?
            .ok_or_else(|| LedgerError::NotFound(format!("cluster {cluster_id} not found")))?;
        let address_set = cluster
            .addresses
            .iter()
            .map(|item| item.address.clone())
            .collect::<Vec<_>>();
        let analyses = self
            .repository
            .list_analyses_for_cluster(cluster.chain_id, &address_set, start, end)
            .await?;
        let coverage = self.repository.get_cluster_coverage(cluster_id).await?;
        Ok(self.summarize_cluster(&cluster, &analyses, start, end, coverage))
    }

    pub async fn get_cluster(&self, cluster_id: Uuid) -> LedgerResult<AddressCluster> {
        self.repository
            .get_cluster(cluster_id)
            .await?
            .ok_or_else(|| LedgerError::NotFound(format!("cluster {cluster_id} not found")))
    }

    pub async fn update_cluster(
        &self,
        cluster_id: Uuid,
        chain_id: u64,
        owner_label: &str,
        name: &str,
        description: Option<String>,
        addresses: Vec<ClusterAddress>,
    ) -> LedgerResult<AddressCluster> {
        let existing = self
            .repository
            .get_cluster(cluster_id)
            .await?
            .ok_or_else(|| LedgerError::NotFound(format!("cluster {cluster_id} not found")))?;
        if addresses.is_empty() {
            return Err(LedgerError::AnalysisInvariant(
                "cluster must contain at least one address".to_owned(),
            ));
        }
        let cluster = AddressCluster {
            cluster_id,
            chain_id,
            owner_label: owner_label.to_owned(),
            name: name.to_owned(),
            description,
            addresses: normalize_addresses(addresses),
            metadata: existing.metadata,
        };
        self.repository.create_cluster(&cluster).await?;
        self.repository
            .replace_cluster_addresses(cluster.cluster_id, &cluster.addresses)
            .await?;
        Ok(cluster)
    }

    pub async fn delete_cluster(&self, cluster_id: Uuid) -> LedgerResult<()> {
        self.repository
            .get_cluster(cluster_id)
            .await?
            .ok_or_else(|| LedgerError::NotFound(format!("cluster {cluster_id} not found")))?;
        self.repository.delete_cluster(cluster_id).await
    }

    fn summarize_cluster(
        &self,
        cluster: &AddressCluster,
        analyses: &[TransactionAnalysis],
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
        coverage: Option<crate::domain::entities::ClusterCoverage>,
    ) -> ClusterSpendSummary {
        let cluster_set = cluster
            .addresses
            .iter()
            .map(|item| item.address.to_lowercase())
            .collect::<BTreeSet<_>>();

        let mut total_gas_cost = 0u128;
        let mut total_external_native_outflow = 0u128;
        let mut total_internal_native_transfer = 0u128;
        let mut total_incoming_native = 0u128;
        let mut unresolved_tx_count = 0u32;
        let mut trace_missing_tx_count = 0u32;
        let mut breakdown_map = BTreeMap::<
            (SpendCategory, AssetType, Option<String>, Option<String>),
            (u128, BTreeSet<String>, u32),
        >::new();
        let mut tx_summaries = Vec::new();

        for analysis in analyses {
            let gas_cost = parse_raw_amount(
                analysis
                    .accounting_outputs
                    .get("gas_cost_raw")
                    .and_then(|value| value.as_str())
                    .unwrap_or("0"),
            );
            if cluster_set.contains(&analysis.from_address.to_lowercase()) {
                total_gas_cost += gas_cost;
                increment_breakdown(
                    &mut breakdown_map,
                    SpendCategory::GasBurn,
                    AssetType::Native,
                    None,
                    Some("network_gas".to_owned()),
                    gas_cost,
                    &analysis.tx_hash,
                );
            }

            let mut tx_external_native = 0u128;
            let mut tx_external_token_count = 0u32;
            let mut tx_internal_transfer_count = 0u32;
            let mut counterparties = BTreeSet::new();

            for flow in &analysis.derived_flows {
                let from_in_cluster = cluster_set.contains(&flow.from_address.to_lowercase());
                let to_in_cluster = cluster_set.contains(&flow.to_address.to_lowercase());
                let amount = parse_raw_amount(&flow.amount_raw);

                if from_in_cluster && to_in_cluster {
                    if flow.asset_type == AssetType::Native {
                        total_internal_native_transfer += amount;
                    }
                    tx_internal_transfer_count += 1;
                    counterparties.insert(flow.to_address.clone());
                    increment_breakdown(
                        &mut breakdown_map,
                        SpendCategory::InternalTransfer,
                        flow.asset_type.clone(),
                        flow.token_address.clone(),
                        Some(flow.to_address.clone()),
                        amount,
                        &analysis.tx_hash,
                    );
                    continue;
                }

                if from_in_cluster && !to_in_cluster {
                    if flow.asset_type == AssetType::Native {
                        total_external_native_outflow += amount;
                        tx_external_native += amount;
                    } else {
                        tx_external_token_count += 1;
                    }
                    counterparties.insert(flow.to_address.clone());
                    increment_breakdown(
                        &mut breakdown_map,
                        SpendCategory::ExternalOutflow,
                        flow.asset_type.clone(),
                        flow.token_address.clone(),
                        Some(flow.to_address.clone()),
                        amount,
                        &analysis.tx_hash,
                    );
                    continue;
                }

                if !from_in_cluster && to_in_cluster {
                    if flow.asset_type == AssetType::Native {
                        total_incoming_native += amount;
                    }
                    counterparties.insert(flow.from_address.clone());
                    increment_breakdown(
                        &mut breakdown_map,
                        SpendCategory::IncomingTransfer,
                        flow.asset_type.clone(),
                        flow.token_address.clone(),
                        Some(flow.from_address.clone()),
                        amount,
                        &analysis.tx_hash,
                    );
                }
            }

            let unresolved_count = analysis.unresolved_items.len() as u32;
            if unresolved_count > 0 {
                unresolved_tx_count += 1;
            }
            let trace_available = analysis
                .normalized_execution_facts
                .derived_facts
                .get("trace_available")
                .and_then(|value| value.as_bool())
                .unwrap_or(false);
            if !trace_available {
                trace_missing_tx_count += 1;
            }

            tx_summaries.push(ClusterTransactionSummary {
                tx_hash: analysis.tx_hash.clone(),
                block_timestamp: analysis.block_timestamp,
                from_address: analysis.from_address.clone(),
                to_address: analysis.to_address.clone(),
                status: analysis.status.clone(),
                analysis_status: analysis.analysis_status.clone(),
                recognizer_labels: analysis
                    .recognizer_results
                    .iter()
                    .map(|item| item.label.clone())
                    .collect(),
                gas_cost_raw: gas_cost.to_string(),
                external_native_outflow_raw: tx_external_native.to_string(),
                external_token_outflow_count: tx_external_token_count,
                internal_transfer_count: tx_internal_transfer_count,
                unresolved_count,
                trace_available,
                counterparties: counterparties.into_iter().collect(),
            });
        }

        tx_summaries.sort_by(|left, right| left.block_timestamp.cmp(&right.block_timestamp));

        let breakdowns = breakdown_map
            .into_iter()
            .map(
                |(
                    (category, asset_type, token_address, counterparty),
                    (amount, tx_hashes, flow_count),
                )| {
                    ClusterSpendBreakdown {
                        category,
                        asset_type,
                        token_address,
                        counterparty,
                        amount_raw: amount.to_string(),
                        tx_count: tx_hashes.len() as u32,
                        flow_count,
                    }
                },
            )
            .collect::<Vec<_>>();

        let mut warnings = Vec::new();
        if let Some(coverage) = coverage.as_ref() {
            if coverage.pending_tx_count > 0 {
                warnings.push(format!(
                    "coverage is partial: {} discovered txs are still pending analysis",
                    coverage.pending_tx_count
                ));
            }
            if coverage.failed_tx_count > 0 {
                warnings.push(format!(
                    "{} discovered txs failed analysis and may affect totals",
                    coverage.failed_tx_count
                ));
            }
            if coverage.discovered_tx_count > coverage.analyzed_tx_count {
                warnings.push("summary only reflects analyzed transactions, not all discovered transactions".to_owned());
            }
        }
        if trace_missing_tx_count > 0 {
            warnings.push(format!(
                "{} analyzed txs were processed without trace data",
                trace_missing_tx_count
            ));
        }
        if unresolved_tx_count > 0 {
            warnings.push(format!(
                "{} analyzed txs still contain unresolved items",
                unresolved_tx_count
            ));
        }

        ClusterSpendSummary {
            cluster_id: cluster.cluster_id,
            chain_id: cluster.chain_id,
            owner_label: cluster.owner_label.clone(),
            cluster_name: cluster.name.clone(),
            window_start: start,
            window_end: end,
            generated_at: Utc::now(),
            address_count: cluster.addresses.len() as u32,
            analyzed_tx_count: analyses.len() as u32,
            total_gas_cost_raw: total_gas_cost.to_string(),
            total_external_native_outflow_raw: total_external_native_outflow.to_string(),
            total_internal_native_transfer_raw: total_internal_native_transfer.to_string(),
            total_incoming_native_raw: total_incoming_native.to_string(),
            breakdowns,
            transactions: tx_summaries,
            metadata: json!({
                "note": "summary derived from already analyzed transactions for the cluster address set",
                "coverage_mode": "persisted_analyses_only",
                "coverage": coverage,
                "warnings": warnings,
                "unresolved_tx_count": unresolved_tx_count,
                "trace_missing_tx_count": trace_missing_tx_count,
            }),
        }
    }
}

fn normalize_addresses(addresses: Vec<ClusterAddress>) -> Vec<ClusterAddress> {
    let mut dedup = BTreeMap::<String, ClusterAddress>::new();
    for mut item in addresses {
        item.address = item.address.to_lowercase();
        dedup.insert(item.address.clone(), item);
    }
    dedup.into_values().collect()
}

fn parse_raw_amount(value: &str) -> u128 {
    value.parse::<u128>().unwrap_or_default()
}

fn increment_breakdown(
    breakdown_map: &mut BTreeMap<
        (SpendCategory, AssetType, Option<String>, Option<String>),
        (u128, BTreeSet<String>, u32),
    >,
    category: SpendCategory,
    asset_type: AssetType,
    token_address: Option<String>,
    counterparty: Option<String>,
    amount: u128,
    tx_hash: &str,
) {
    let entry = breakdown_map
        .entry((category, asset_type, token_address, counterparty))
        .or_insert_with(|| (0u128, BTreeSet::new(), 0u32));
    entry.0 += amount;
    entry.1.insert(tx_hash.to_owned());
    entry.2 += 1;
}
