use std::{
    collections::BTreeMap,
    hash::{Hash, Hasher},
};

use chrono::Utc;
use serde_json::json;
use uuid::Uuid;

use crate::domain::{
    entities::{AddressCluster, ClassifiedTransaction, TransactionAnalysis},
    enums::{AssetType, BehaviorTag},
    value_objects::AssetFlow,
};

const DEFAULT_NATIVE_TOKEN_PRICE_USD: f64 = 2_500.0;
const ERC20_DECIMALS_ASSUMPTION: f64 = 1_000_000_000_000_000_000.0;

#[derive(Clone)]
pub struct BehaviorClassifier {
    native_token_price_usd: f64,
}

impl Default for BehaviorClassifier {
    fn default() -> Self {
        Self {
            native_token_price_usd: DEFAULT_NATIVE_TOKEN_PRICE_USD,
        }
    }
}

impl BehaviorClassifier {
    pub fn classify(
        &self,
        cluster: &AddressCluster,
        tx_ref_address: &str,
        analysis: &TransactionAnalysis,
    ) -> ClassifiedTransaction {
        let address = self.resolve_cluster_address(cluster, tx_ref_address);
        let selector = analysis
            .normalized_execution_facts
            .selector
            .clone()
            .unwrap_or_default()
            .to_lowercase();
        let labels = analysis
            .recognizer_results
            .iter()
            .map(|item| item.label.clone())
            .collect::<Vec<_>>();
        let protocol = analysis
            .recognizer_results
            .iter()
            .find_map(|item| {
                item.attributes
                    .get("protocol")
                    .and_then(|value| value.as_str())
                    .map(str::to_owned)
            })
            .or_else(|| {
                labels
                    .iter()
                    .find(|label| {
                        !matches!(label.as_str(), "selector_present" | "failed_execution")
                    })
                    .cloned()
            })
            .or_else(|| analysis.to_address.clone())
            .or_else(|| labels.first().cloned());

        let (token_in, amount_in, token_out, amount_out) =
            classify_token_direction(&address, &analysis.normalized_execution_facts.derived_facts);
        let (volume_usd, external_inflow_usd, external_outflow_usd) =
            self.estimate_native_volume(&address, &analysis.derived_flows);
        let gas_cost_usd = parse_native_usd(
            analysis
                .accounting_outputs
                .get("gas_cost_raw")
                .and_then(|value| value.as_str()),
            self.native_token_price_usd,
        );
        let estimated_pnl_usd = match (external_inflow_usd, external_outflow_usd, gas_cost_usd) {
            (Some(inflow), Some(outflow), Some(gas)) => Some(inflow - outflow - gas),
            (Some(inflow), None, Some(gas)) => Some(inflow - gas),
            (None, Some(outflow), Some(gas)) => Some(-outflow - gas),
            _ => None,
        };

        let behavior_tag = classify_behavior_tag(
            &selector,
            &labels,
            token_in.as_deref(),
            token_out.as_deref(),
            estimated_pnl_usd,
            gas_cost_usd,
        );

        ClassifiedTransaction {
            cluster_id: cluster.cluster_id,
            chain_id: cluster.chain_id,
            tx_hash: analysis.tx_hash.clone(),
            address,
            block_number: analysis.block_number.unwrap_or_default(),
            timestamp: analysis.block_timestamp,
            behavior_tag,
            protocol,
            token_in,
            token_out,
            amount_in,
            amount_out,
            value_usd: volume_usd,
            gas_cost_usd,
            estimated_pnl_usd,
            raw_json: json!({
                "analysis_status": analysis.analysis_status,
                "recognizer_labels": labels,
                "selector": selector,
                "external_inflow_usd": external_inflow_usd,
                "external_outflow_usd": external_outflow_usd,
            }),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    fn resolve_cluster_address(&self, cluster: &AddressCluster, tx_ref_address: &str) -> String {
        if cluster
            .addresses
            .iter()
            .any(|item| item.address.eq_ignore_ascii_case(tx_ref_address))
        {
            return tx_ref_address.to_lowercase();
        }
        cluster
            .addresses
            .first()
            .map(|item| item.address.to_lowercase())
            .unwrap_or_else(|| tx_ref_address.to_lowercase())
    }

    fn estimate_native_volume(
        &self,
        address: &str,
        flows: &[AssetFlow],
    ) -> (Option<f64>, Option<f64>, Option<f64>) {
        let mut inflow = 0.0f64;
        let mut outflow = 0.0f64;
        for flow in flows {
            if flow.asset_type != AssetType::Native {
                continue;
            }
            let Some(amount_usd) =
                parse_native_usd(Some(&flow.amount_raw), self.native_token_price_usd)
            else {
                continue;
            };
            if flow.to_address.eq_ignore_ascii_case(address) {
                inflow += amount_usd;
            }
            if flow.from_address.eq_ignore_ascii_case(address) {
                outflow += amount_usd;
            }
        }

        let volume = inflow + outflow;
        (
            if volume > 0.0 { Some(volume) } else { None },
            if inflow > 0.0 { Some(inflow) } else { None },
            if outflow > 0.0 { Some(outflow) } else { None },
        )
    }
}

fn classify_token_direction(
    address: &str,
    derived_facts: &serde_json::Value,
) -> (
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
) {
    let mut incoming: BTreeMap<String, i128> = BTreeMap::new();
    let mut outgoing: BTreeMap<String, i128> = BTreeMap::new();

    for entry in derived_facts
        .get("token_net_deltas")
        .and_then(|value| value.as_array())
        .into_iter()
        .flatten()
    {
        let entry_address = entry
            .get("address")
            .and_then(|value| value.as_str())
            .unwrap_or_default()
            .to_lowercase();
        if entry_address != address.to_lowercase() {
            continue;
        }
        let token_address = entry
            .get("token_address")
            .and_then(|value| value.as_str())
            .unwrap_or_default()
            .to_lowercase();
        let delta = entry
            .get("delta_raw")
            .and_then(|value| value.as_str())
            .and_then(|value| value.parse::<i128>().ok())
            .unwrap_or_default();
        if delta > 0 {
            incoming.insert(token_address, delta);
        } else if delta < 0 {
            outgoing.insert(token_address, delta.abs());
        }
    }

    let token_in = incoming.iter().max_by_key(|(_, value)| *value);
    let token_out = outgoing.iter().max_by_key(|(_, value)| *value);
    (
        token_in.map(|(token, _)| token.clone()),
        token_in.map(|(_, amount)| amount.to_string()),
        token_out.map(|(token, _)| token.clone()),
        token_out.map(|(_, amount)| amount.to_string()),
    )
}

fn classify_behavior_tag(
    selector: &str,
    labels: &[String],
    token_in: Option<&str>,
    token_out: Option<&str>,
    estimated_pnl_usd: Option<f64>,
    gas_cost_usd: Option<f64>,
) -> BehaviorTag {
    if labels
        .iter()
        .any(|label| label == "liquidation_like_activity")
    {
        return BehaviorTag::Liquidation;
    }
    if labels
        .iter()
        .any(|label| label == "arbitrage_like_activity")
        || (token_in.is_some()
            && token_out.is_some()
            && estimated_pnl_usd.unwrap_or_default() > gas_cost_usd.unwrap_or_default())
    {
        return BehaviorTag::Arbitrage;
    }
    if matches!(
        selector,
        "0xe8e33700" | "0xf305d719" | "0xbaa2abde" | "0x219f5d17" | "0x5b0d5984"
    ) {
        return BehaviorTag::Lp;
    }
    if labels.iter().any(|label| {
        matches!(
            label.as_str(),
            "swap_like_activity"
                | "approval_then_swap_like"
                | "bridge_like_asset_exit"
                | "deposit_like_activity"
                | "withdraw_like_activity"
        )
    }) {
        return BehaviorTag::Position;
    }
    BehaviorTag::Unknown
}

fn parse_native_usd(raw: Option<&str>, native_token_price_usd: f64) -> Option<f64> {
    let value = raw?.parse::<f64>().ok()?;
    Some((value / ERC20_DECIMALS_ASSUMPTION) * native_token_price_usd)
}

pub fn alert_bucket_key(
    cluster_id: Uuid,
    scope: &str,
    triggered_at: chrono::DateTime<Utc>,
) -> Uuid {
    let key = format!(
        "{cluster_id}:{scope}:{}",
        triggered_at.format("%Y-%m-%dT%H")
    );
    stable_uuid_from_key(&key)
}

pub fn stable_uuid_from_key(key: &str) -> Uuid {
    let mut left = std::collections::hash_map::DefaultHasher::new();
    "ledgerflow-left".hash(&mut left);
    key.hash(&mut left);

    let mut right = std::collections::hash_map::DefaultHasher::new();
    "ledgerflow-right".hash(&mut right);
    key.hash(&mut right);

    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&left.finish().to_be_bytes());
    bytes[8..].copy_from_slice(&right.finish().to_be_bytes());
    Uuid::from_bytes(bytes)
}
