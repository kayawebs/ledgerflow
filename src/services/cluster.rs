use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde_json::json;
use uuid::Uuid;

use crate::core::errors::{LedgerError, LedgerResult};
use crate::domain::entities::{
    AddressCluster, ClusterAddress, ClusterAssetChangeReport, ClusterAssetPurposeBreakdown,
    ClusterDailyAssetChange, ClusterSpendBreakdown, ClusterSpendSummary, ClusterTransactionSummary,
    TransactionAnalysis,
};
use crate::domain::enums::{AssetType, BehaviorTag, SpendCategory};
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

    pub async fn get_cluster_asset_changes(
        &self,
        cluster_id: Uuid,
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
    ) -> LedgerResult<ClusterAssetChangeReport> {
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
        let classified = self
            .repository
            .list_classified_transactions_for_cluster(cluster_id, start, end)
            .await?;
        let classified_by_tx = classified
            .into_iter()
            .map(|item| (item.tx_hash.clone(), item.behavior_tag))
            .collect::<BTreeMap<_, _>>();
        let coverage = self.repository.get_cluster_coverage(cluster_id).await?;
        Ok(self.summarize_cluster_asset_changes(
            &cluster,
            &analyses,
            &classified_by_tx,
            start,
            end,
            coverage,
        ))
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
                warnings.push(
                    "summary only reflects analyzed transactions, not all discovered transactions"
                        .to_owned(),
                );
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

    fn summarize_cluster_asset_changes(
        &self,
        cluster: &AddressCluster,
        analyses: &[TransactionAnalysis],
        classified_by_tx: &BTreeMap<String, BehaviorTag>,
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
        coverage: Option<crate::domain::entities::ClusterCoverage>,
    ) -> ClusterAssetChangeReport {
        let cluster_set = cluster
            .addresses
            .iter()
            .map(|item| item.address.to_lowercase())
            .collect::<BTreeSet<_>>();
        let mut daily =
            BTreeMap::<(String, AssetType, Option<String>), DailyAssetAccumulator>::new();
        let mut purpose = BTreeMap::<
            (String, AssetType, Option<String>, String, Option<String>),
            PurposeAssetAccumulator,
        >::new();
        let mut unresolved_tx_count = 0u32;
        let mut trace_missing_tx_count = 0u32;

        for analysis in analyses {
            let day = analysis_day(analysis);
            let behavior = classified_by_tx.get(&analysis.tx_hash);
            let gas_cost = parse_raw_amount(
                analysis
                    .accounting_outputs
                    .get("gas_cost_raw")
                    .and_then(|value| value.as_str())
                    .unwrap_or("0"),
            );
            if cluster_set.contains(&analysis.from_address.to_lowercase()) && gas_cost > 0 {
                let amount = u128_to_i128(gas_cost);
                let key = (day.clone(), AssetType::Native, None);
                let entry = daily.entry(key.clone()).or_default();
                entry.gas += amount;
                entry.net -= amount;
                entry.tx_hashes.insert(analysis.tx_hash.clone());
                push_sample(&mut entry.sample_tx_hashes, &analysis.tx_hash, 10);
                increment_purpose(
                    &mut purpose,
                    key,
                    "gas_cost",
                    Some("network_gas".to_owned()),
                    amount,
                    &analysis.tx_hash,
                );
            }

            for flow in &analysis.derived_flows {
                let from_in_cluster = cluster_set.contains(&flow.from_address.to_lowercase());
                let to_in_cluster = cluster_set.contains(&flow.to_address.to_lowercase());
                if !from_in_cluster && !to_in_cluster {
                    continue;
                }
                let amount = u128_to_i128(parse_raw_amount(&flow.amount_raw));
                if amount == 0 {
                    continue;
                }
                let token_address = flow
                    .token_address
                    .as_ref()
                    .map(|value| value.to_lowercase());
                let key = (day.clone(), flow.asset_type.clone(), token_address);
                let entry = daily.entry(key.clone()).or_default();
                entry.flow_count += 1;
                entry.tx_hashes.insert(analysis.tx_hash.clone());
                push_sample(&mut entry.sample_tx_hashes, &analysis.tx_hash, 10);

                if from_in_cluster && to_in_cluster {
                    entry.internal += amount;
                    increment_purpose(
                        &mut purpose,
                        key,
                        "internal_transfer",
                        Some(flow.to_address.clone()),
                        amount,
                        &analysis.tx_hash,
                    );
                } else if from_in_cluster {
                    entry.outgoing += amount;
                    entry.net -= amount;
                    increment_purpose(
                        &mut purpose,
                        key,
                        outflow_purpose(behavior),
                        Some(flow.to_address.clone()),
                        amount,
                        &analysis.tx_hash,
                    );
                } else {
                    entry.incoming += amount;
                    entry.net += amount;
                    increment_purpose(
                        &mut purpose,
                        key,
                        inflow_purpose(behavior),
                        Some(flow.from_address.clone()),
                        amount,
                        &analysis.tx_hash,
                    );
                }
            }

            if !analysis.unresolved_items.is_empty() {
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
        }

        let mut daily_changes = daily
            .into_iter()
            .map(
                |((day, asset_type, token_address), value)| ClusterDailyAssetChange {
                    day,
                    asset_type,
                    token_address,
                    incoming_raw: value.incoming.to_string(),
                    outgoing_raw: value.outgoing.to_string(),
                    gas_raw: value.gas.to_string(),
                    internal_raw: value.internal.to_string(),
                    net_raw: value.net.to_string(),
                    tx_count: value.tx_hashes.len() as u32,
                    flow_count: value.flow_count,
                    sample_tx_hashes: value.sample_tx_hashes,
                },
            )
            .collect::<Vec<_>>();
        daily_changes.sort_by(|left, right| {
            (
                left.day.as_str(),
                asset_sort_key(&left.asset_type),
                left.token_address.as_deref(),
            )
                .cmp(&(
                    right.day.as_str(),
                    asset_sort_key(&right.asset_type),
                    right.token_address.as_deref(),
                ))
        });

        let purpose_breakdowns = purpose
            .into_iter()
            .map(
                |((day, asset_type, token_address, purpose, counterparty), value)| {
                    ClusterAssetPurposeBreakdown {
                        day,
                        asset_type,
                        token_address,
                        purpose,
                        counterparty,
                        amount_raw: value.amount.to_string(),
                        tx_count: value.tx_hashes.len() as u32,
                        flow_count: value.flow_count,
                        sample_tx_hashes: value.sample_tx_hashes,
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
                warnings.push("asset changes only reflect analyzed transactions, not all discovered transactions".to_owned());
            }
        }
        if trace_missing_tx_count > 0 {
            warnings.push(format!(
                "{} analyzed txs were processed without trace data",
                trace_missing_tx_count
            ));
        }

        ClusterAssetChangeReport {
            cluster_id: cluster.cluster_id,
            chain_id: cluster.chain_id,
            owner_label: cluster.owner_label.clone(),
            cluster_name: cluster.name.clone(),
            window_start: start,
            window_end: end,
            generated_at: Utc::now(),
            address_count: cluster.addresses.len() as u32,
            analyzed_tx_count: analyses.len() as u32,
            daily_changes,
            purpose_breakdowns,
            metadata: json!({
                "note": "daily asset changes are derived from already analyzed cluster transactions",
                "coverage_mode": "persisted_analyses_only",
                "coverage": coverage,
                "warnings": warnings,
                "unresolved_tx_count": unresolved_tx_count,
                "trace_missing_tx_count": trace_missing_tx_count,
                "net_formula": "incoming_raw - outgoing_raw - gas_raw; internal transfers are tracked separately and excluded from net_raw",
            }),
        }
    }
}

#[derive(Default)]
struct DailyAssetAccumulator {
    incoming: i128,
    outgoing: i128,
    gas: i128,
    internal: i128,
    net: i128,
    tx_hashes: BTreeSet<String>,
    flow_count: u32,
    sample_tx_hashes: Vec<String>,
}

#[derive(Default)]
struct PurposeAssetAccumulator {
    amount: i128,
    tx_hashes: BTreeSet<String>,
    flow_count: u32,
    sample_tx_hashes: Vec<String>,
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

fn u128_to_i128(value: u128) -> i128 {
    i128::try_from(value).unwrap_or(i128::MAX)
}

fn analysis_day(analysis: &TransactionAnalysis) -> String {
    analysis
        .block_timestamp
        .map(|value| value.format("%Y-%m-%d").to_string())
        .unwrap_or_else(|| "unknown".to_owned())
}

fn asset_sort_key(asset_type: &AssetType) -> u8 {
    match asset_type {
        AssetType::Native => 0,
        AssetType::Erc20 => 1,
    }
}

fn inflow_purpose(behavior: Option<&BehaviorTag>) -> &'static str {
    match behavior {
        Some(BehaviorTag::Arbitrage) => "arbitrage_gain",
        Some(BehaviorTag::Liquidation) => "liquidation_gain",
        Some(BehaviorTag::Position) => "position_inflow",
        Some(BehaviorTag::Lp) => "lp_inflow",
        Some(BehaviorTag::Unknown) | None => "external_inflow",
    }
}

fn outflow_purpose(behavior: Option<&BehaviorTag>) -> &'static str {
    match behavior {
        Some(BehaviorTag::Arbitrage) => "arbitrage_spend",
        Some(BehaviorTag::Liquidation) => "liquidation_spend",
        Some(BehaviorTag::Position) => "position_spend",
        Some(BehaviorTag::Lp) => "lp_spend",
        Some(BehaviorTag::Unknown) | None => "external_outflow",
    }
}

fn push_sample(items: &mut Vec<String>, tx_hash: &str, limit: usize) {
    if items.len() < limit && !items.iter().any(|item| item == tx_hash) {
        items.push(tx_hash.to_owned());
    }
}

fn increment_purpose(
    purpose_map: &mut BTreeMap<
        (String, AssetType, Option<String>, String, Option<String>),
        PurposeAssetAccumulator,
    >,
    key: (String, AssetType, Option<String>),
    purpose: &str,
    counterparty: Option<String>,
    amount: i128,
    tx_hash: &str,
) {
    let entry = purpose_map
        .entry((key.0, key.1, key.2, purpose.to_owned(), counterparty))
        .or_default();
    entry.amount += amount;
    entry.tx_hashes.insert(tx_hash.to_owned());
    entry.flow_count += 1;
    push_sample(&mut entry.sample_tx_hashes, tx_hash, 10);
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
