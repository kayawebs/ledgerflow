use serde_json::json;

use crate::domain::entities::TransactionAnalysis;
use crate::domain::enums::{AssetType, EvidenceKind};
use crate::domain::interfaces::FlowDeriver;
use crate::domain::value_objects::{AssetFlow, EvidenceRef};
use crate::ingestion::models::{RawTraceFrame, RawTransactionBundle};
use crate::normalization::service::TRANSFER_TOPIC;

fn topic_to_address(topic: &str) -> String {
    let trimmed = topic.trim_start_matches("0x");
    format!("0x{}", &trimmed[trimmed.len().saturating_sub(40)..]).to_lowercase()
}

fn hex_amount_to_decimal(value: &str) -> String {
    u128::from_str_radix(value.trim_start_matches("0x"), 16)
        .unwrap_or_default()
        .to_string()
}

fn trace_native_flows(tx_hash: &str, frame: Option<&RawTraceFrame>) -> Vec<AssetFlow> {
    let Some(frame) = frame else {
        return Vec::new();
    };
    let mut flows = Vec::new();
    for (index, child) in frame.children.iter().enumerate() {
        if child.to_address.is_some()
            && u128::from_str_radix(child.value.trim_start_matches("0x"), 16).unwrap_or(0) > 0
        {
            flows.push(AssetFlow {
                flow_id: format!("{tx_hash}:native:{index}"),
                asset_type: AssetType::Native,
                token_address: None,
                from_address: child.from_address.to_lowercase(),
                to_address: child.to_address.clone().unwrap_or_default().to_lowercase(),
                amount_raw: hex_amount_to_decimal(&child.value),
                direction_hint: None,
                evidence_refs: vec![EvidenceRef {
                    kind: EvidenceKind::CallFrame,
                    reference: format!("{tx_hash}:trace:{index}"),
                    tx_hash: Some(tx_hash.to_owned()),
                    metadata: json!({}),
                }],
                derivation_method: "trace_native_transfer".to_owned(),
                confidence: 0.95,
                metadata: json!({}),
            });
        }
        flows.extend(trace_native_flows(tx_hash, Some(child)));
    }
    flows
}

#[derive(Default)]
pub struct BasicFlowDeriver;

impl FlowDeriver for BasicFlowDeriver {
    fn derive(
        &self,
        bundle: &RawTransactionBundle,
        _analysis: &TransactionAnalysis,
    ) -> Vec<AssetFlow> {
        let mut flows = Vec::new();
        let tx_hash = &bundle.tx.tx_hash;
        if bundle.tx.to_address.is_some()
            && u128::from_str_radix(bundle.tx.value.trim_start_matches("0x"), 16).unwrap_or(0) > 0
        {
            flows.push(AssetFlow {
                flow_id: format!("{tx_hash}:native:root"),
                asset_type: AssetType::Native,
                token_address: None,
                from_address: bundle.tx.from_address.to_lowercase(),
                to_address: bundle
                    .tx
                    .to_address
                    .clone()
                    .unwrap_or_default()
                    .to_lowercase(),
                amount_raw: hex_amount_to_decimal(&bundle.tx.value),
                direction_hint: None,
                evidence_refs: vec![EvidenceRef {
                    kind: EvidenceKind::Tx,
                    reference: tx_hash.clone(),
                    tx_hash: Some(tx_hash.clone()),
                    metadata: json!({}),
                }],
                derivation_method: "tx_value".to_owned(),
                confidence: 1.0,
                metadata: json!({}),
            });
        }
        flows.extend(trace_native_flows(tx_hash, bundle.trace.as_ref()));

        for log in &bundle.receipt.logs {
            if log.topics.len() < 3 || !log.topics[0].eq_ignore_ascii_case(TRANSFER_TOPIC) {
                continue;
            }
            flows.push(AssetFlow {
                flow_id: format!("{tx_hash}:erc20:{}", log.log_index),
                asset_type: AssetType::Erc20,
                token_address: Some(log.address.to_lowercase()),
                from_address: topic_to_address(&log.topics[1]),
                to_address: topic_to_address(&log.topics[2]),
                amount_raw: hex_amount_to_decimal(&log.data),
                direction_hint: None,
                evidence_refs: vec![EvidenceRef {
                    kind: EvidenceKind::Log,
                    reference: format!("{tx_hash}:log:{}", log.log_index),
                    tx_hash: Some(tx_hash.clone()),
                    metadata: json!({}),
                }],
                derivation_method: "erc20_transfer_log".to_owned(),
                confidence: 0.98,
                metadata: json!({}),
            });
        }

        flows
    }
}
