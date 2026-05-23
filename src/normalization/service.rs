use chrono::Utc;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

use crate::core::versioning::VersionBundle;
use crate::domain::entities::{NormalizedExecutionFacts, TransactionAnalysis};
use crate::domain::enums::{AnalysisStatus, EvidenceKind, TransactionStatus};
use crate::domain::value_objects::{CodeIdentitySummary, EvidenceRef};
use crate::ingestion::models::{RawTraceFrame, RawTransactionBundle};

pub const TRANSFER_TOPIC: &str =
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
pub const APPROVAL_TOPIC: &str =
    "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925";

fn count_trace(frame: Option<&RawTraceFrame>) -> (u32, u32) {
    match frame {
        None => (0, 0),
        Some(frame) => {
            let mut total = 1u32;
            let mut internal = frame.children.len() as u32;
            for child in &frame.children {
                let (child_total, child_internal) = count_trace(Some(child));
                total += child_total;
                internal += child_internal;
            }
            (total, internal)
        }
    }
}

fn hex_to_u128(value: Option<&str>) -> u128 {
    u128::from_str_radix(value.unwrap_or("0x0").trim_start_matches("0x"), 16).unwrap_or_default()
}

fn topic_to_address(topic: &str) -> String {
    let trimmed = topic.trim_start_matches("0x");
    format!("0x{}", &trimmed[trimmed.len().saturating_sub(40)..]).to_lowercase()
}

fn collect_trace_details(
    frame: Option<&RawTraceFrame>,
    touched_contracts: &mut BTreeSet<String>,
    call_type_counts: &mut BTreeMap<String, u32>,
    nonzero_native_call_count: &mut u32,
    selector_set: &mut BTreeSet<String>,
) {
    let Some(frame) = frame else {
        return;
    };
    *call_type_counts
        .entry(frame.call_type.to_lowercase())
        .or_default() += 1;
    if let Some(to_address) = frame.to_address.as_ref() {
        touched_contracts.insert(to_address.to_lowercase());
    }
    if hex_to_u128(Some(&frame.value)) > 0 {
        *nonzero_native_call_count += 1;
    }
    if frame.input_data.len() >= 10 {
        selector_set.insert(frame.input_data[..10].to_lowercase());
    }
    for child in &frame.children {
        collect_trace_details(
            Some(child),
            touched_contracts,
            call_type_counts,
            nonzero_native_call_count,
            selector_set,
        );
    }
}

fn selector_from_input(input: &str) -> Option<String> {
    if input.len() >= 10 {
        Some(input[..10].to_lowercase())
    } else {
        None
    }
}

fn trace_address_label(trace_address: &[u64]) -> String {
    if trace_address.is_empty() {
        "root".to_owned()
    } else {
        trace_address
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(".")
    }
}

fn collect_execution_timeline(
    tx_hash: &str,
    frame: &RawTraceFrame,
    trace_address: &[u64],
    depth: u32,
    rows: &mut Vec<Value>,
) {
    let parent_trace_address = if trace_address.is_empty() {
        Value::Null
    } else {
        json!(trace_address[..trace_address.len().saturating_sub(1)].to_vec())
    };
    rows.push(json!({
        "order": rows.len() + 1,
        "kind": if trace_address.is_empty() { "tx_entry" } else { "internal_call" },
        "trace_address": trace_address,
        "trace_address_label": trace_address_label(trace_address),
        "parent_trace_address": parent_trace_address,
        "depth": depth,
        "from_address": frame.from_address.to_lowercase(),
        "to_address": frame.to_address.as_ref().map(|value| value.to_lowercase()),
        "call_type": frame.call_type.to_lowercase(),
        "selector": selector_from_input(&frame.input_data),
        "value_raw": hex_to_u128(Some(&frame.value)).to_string(),
        "evidence_ref": format!("{}:trace:{}", tx_hash, trace_address_label(trace_address)),
    }));
    for (index, child) in frame.children.iter().enumerate() {
        let mut child_trace_address = trace_address.to_vec();
        child_trace_address.push(index as u64);
        collect_execution_timeline(tx_hash, child, &child_trace_address, depth + 1, rows);
    }
}

fn execution_timeline(bundle: &RawTransactionBundle) -> Vec<Value> {
    if let Some(trace) = bundle.trace.as_ref() {
        let mut rows = Vec::new();
        collect_execution_timeline(&bundle.tx.tx_hash, trace, &[], 0, &mut rows);
        return rows;
    }
    vec![json!({
        "order": 1,
        "kind": "tx_entry",
        "trace_address": [],
        "trace_address_label": "root",
        "parent_trace_address": Value::Null,
        "depth": 0,
        "from_address": bundle.tx.from_address.to_lowercase(),
        "to_address": bundle.tx.to_address.as_ref().map(|value| value.to_lowercase()),
        "call_type": "transaction",
        "selector": selector_from_input(&bundle.tx.input_data),
        "value_raw": hex_to_u128(Some(&bundle.tx.value)).to_string(),
        "evidence_ref": bundle.tx.tx_hash,
        "trace_limited": true,
    })]
}

#[derive(Default)]
pub struct EvidenceNormalizer;

impl EvidenceNormalizer {
    pub fn normalize(&self, bundle: &RawTransactionBundle) -> TransactionAnalysis {
        let selector = selector_from_input(&bundle.tx.input_data);
        let (call_count, internal_call_count) = count_trace(bundle.trace.as_ref());
        let mut participants = vec![bundle.tx.from_address.to_lowercase()];
        if let Some(target) = bundle.tx.to_address.as_ref() {
            participants.push(target.to_lowercase());
        }
        let mut touched_contracts = BTreeSet::new();
        if let Some(target) = bundle.tx.to_address.as_ref() {
            touched_contracts.insert(target.to_lowercase());
        }
        let mut call_type_counts = BTreeMap::<String, u32>::new();
        let mut nonzero_native_call_count = 0u32;
        let mut trace_selectors = BTreeSet::new();
        let mut log_summaries = Vec::new();
        let mut token_net_deltas = BTreeMap::<(String, String), i128>::new();
        let mut approval_edges = BTreeSet::<(String, String, String)>::new();
        let mut evidence_refs = vec![
            EvidenceRef {
                kind: EvidenceKind::Tx,
                reference: bundle.tx.tx_hash.clone(),
                tx_hash: Some(bundle.tx.tx_hash.clone()),
                metadata: json!({}),
            },
            EvidenceRef {
                kind: EvidenceKind::Receipt,
                reference: format!("{}:receipt", bundle.tx.tx_hash),
                tx_hash: Some(bundle.tx.tx_hash.clone()),
                metadata: json!({}),
            },
        ];
        for log in &bundle.receipt.logs {
            participants.push(log.address.to_lowercase());
            touched_contracts.insert(log.address.to_lowercase());
            evidence_refs.push(EvidenceRef {
                kind: EvidenceKind::Log,
                reference: format!("{}:log:{}", bundle.tx.tx_hash, log.log_index),
                tx_hash: Some(bundle.tx.tx_hash.clone()),
                metadata: json!({ "address": log.address }),
            });
            log_summaries.push(json!({
                "log_index": log.log_index,
                "address": log.address,
                "topics": log.topics,
                "topic_0": log.topics.first(),
                "is_transfer": log.topics.first().map(|v| v.eq_ignore_ascii_case(TRANSFER_TOPIC)).unwrap_or(false),
                "is_approval": log.topics.first().map(|v| v.eq_ignore_ascii_case(APPROVAL_TOPIC)).unwrap_or(false),
            }));

            if log.topics.len() >= 3 && log.topics[0].eq_ignore_ascii_case(TRANSFER_TOPIC) {
                let token = log.address.to_lowercase();
                let from_address = topic_to_address(&log.topics[1]);
                let to_address = topic_to_address(&log.topics[2]);
                let amount = i128::try_from(hex_to_u128(Some(&log.data))).unwrap_or_default();
                *token_net_deltas
                    .entry((from_address.clone(), token.clone()))
                    .or_default() -= amount;
                *token_net_deltas.entry((to_address, token)).or_default() += amount;
            }

            if log.topics.len() >= 3 && log.topics[0].eq_ignore_ascii_case(APPROVAL_TOPIC) {
                approval_edges.insert((
                    topic_to_address(&log.topics[1]),
                    topic_to_address(&log.topics[2]),
                    log.address.to_lowercase(),
                ));
            }
        }
        participants.sort();
        participants.dedup();

        if bundle.trace.is_some() {
            evidence_refs.push(EvidenceRef {
                kind: EvidenceKind::Trace,
                reference: format!("{}:trace", bundle.tx.tx_hash),
                tx_hash: Some(bundle.tx.tx_hash.clone()),
                metadata: json!({}),
            });
        }
        collect_trace_details(
            bundle.trace.as_ref(),
            &mut touched_contracts,
            &mut call_type_counts,
            &mut nonzero_native_call_count,
            &mut trace_selectors,
        );

        let code_identity_summary = bundle.target_code.as_ref().map(|code| {
            let mut hasher = Sha256::new();
            hasher.update(code.bytecode.as_bytes());
            CodeIdentitySummary {
                address: Some(code.address.clone()),
                code_hash: Some(hex::encode(hasher.finalize())),
                bytecode_length: code.bytecode.strip_prefix("0x").map(|v| v.len() / 2),
                confidence: 1.0,
            }
        });

        let normalized = NormalizedExecutionFacts {
            selector,
            call_count: if call_count == 0 { 1 } else { call_count },
            internal_call_count,
            event_count: bundle.receipt.logs.len() as u32,
            call_tree: serde_json::to_value(&bundle.trace).unwrap_or(Value::Null),
            participants,
            log_summaries,
            derived_facts: json!({
                "receipt_status": bundle.receipt.status,
                "trace_available": bundle.trace.is_some(),
                "execution_timeline": execution_timeline(bundle),
                "has_token_transfer_logs": bundle.receipt.logs.iter().any(|log| log.topics.first().map(|v| v.eq_ignore_ascii_case(TRANSFER_TOPIC)).unwrap_or(false)),
                "has_approval_logs": bundle.receipt.logs.iter().any(|log| log.topics.first().map(|v| v.eq_ignore_ascii_case(APPROVAL_TOPIC)).unwrap_or(false)),
                "touched_contracts": touched_contracts.into_iter().collect::<Vec<_>>(),
                "call_type_counts": call_type_counts,
                "nonzero_native_call_count": nonzero_native_call_count,
                "trace_selectors": trace_selectors.into_iter().collect::<Vec<_>>(),
                "token_net_deltas": token_net_deltas.into_iter().map(|((address, token_address), delta_raw)| json!({
                    "address": address,
                    "token_address": token_address,
                    "delta_raw": delta_raw.to_string(),
                })).collect::<Vec<_>>(),
                "approval_edges": approval_edges.into_iter().map(|(owner, spender, token_address)| json!({
                    "owner": owner,
                    "spender": spender,
                    "token_address": token_address,
                })).collect::<Vec<_>>(),
                "nonce": bundle.tx.nonce,
                "tx_value_raw": hex_to_u128(Some(&bundle.tx.value)).to_string(),
                "gas_used_raw": hex_to_u128(bundle.receipt.gas_used.as_deref()).to_string(),
                "effective_gas_price_raw": hex_to_u128(bundle.receipt.effective_gas_price.as_deref()).to_string(),
                "gas_cost_raw": (
                    hex_to_u128(bundle.receipt.gas_used.as_deref())
                        * hex_to_u128(bundle.receipt.effective_gas_price.as_deref())
                ).to_string(),
            }),
            evidence_refs: evidence_refs.clone(),
        };

        TransactionAnalysis {
            analysis_id: Uuid::new_v4(),
            chain_id: bundle.tx.chain_id,
            tx_hash: bundle.tx.tx_hash.clone(),
            block_number: bundle.tx.block_number,
            block_timestamp: bundle.tx.block_timestamp.or(Some(Utc::now())),
            from_address: bundle.tx.from_address.to_lowercase(),
            to_address: bundle
                .tx
                .to_address
                .as_ref()
                .map(|value| value.to_lowercase()),
            status: if bundle.receipt.status == 1 {
                TransactionStatus::Success
            } else {
                TransactionStatus::Failure
            },
            value: u128::from_str_radix(bundle.tx.value.trim_start_matches("0x"), 16)
                .unwrap_or_default()
                .to_string(),
            input_data: bundle.tx.input_data.clone(),
            code_identity_summary,
            raw_evidence_refs: evidence_refs,
            normalized_execution_facts: normalized,
            derived_flows: Vec::new(),
            recognizer_results: Vec::new(),
            scenario_outputs: json!({}),
            accounting_outputs: json!({}),
            analysis_status: if bundle.trace.is_some() {
                AnalysisStatus::Pending
            } else {
                AnalysisStatus::Partial
            },
            analysis_version_metadata: VersionBundle::default(),
            unresolved_items: Vec::new(),
            validation_results: Vec::new(),
            input_digest: String::new(),
            attempt_id: None,
        }
    }
}
