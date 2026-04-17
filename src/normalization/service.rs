use chrono::Utc;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::core::versioning::VersionBundle;
use crate::domain::entities::{NormalizedExecutionFacts, TransactionAnalysis};
use crate::domain::enums::{AnalysisStatus, EvidenceKind, TransactionStatus};
use crate::domain::value_objects::{CodeIdentitySummary, EvidenceRef};
use crate::ingestion::models::{RawTraceFrame, RawTransactionBundle};

pub const TRANSFER_TOPIC: &str =
    "0xddf252ad00000000000000000000000000000000000000000000000000000000";
pub const APPROVAL_TOPIC: &str =
    "0x8c5be1e5ebec7d5bd14f714f7c0d1e84f3dd0314c0f7b2291e5f1f0ee4f6f7c1";

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

#[derive(Default)]
pub struct EvidenceNormalizer;

impl EvidenceNormalizer {
    pub fn normalize(&self, bundle: &RawTransactionBundle) -> TransactionAnalysis {
        let selector = if bundle.tx.input_data.len() >= 10 {
            Some(bundle.tx.input_data[..10].to_owned())
        } else {
            None
        };
        let (call_count, internal_call_count) = count_trace(bundle.trace.as_ref());
        let mut participants = vec![bundle.tx.from_address.to_lowercase()];
        if let Some(target) = bundle.tx.to_address.as_ref() {
            participants.push(target.to_lowercase());
        }
        let mut log_summaries = Vec::new();
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
                "has_token_transfer_logs": bundle.receipt.logs.iter().any(|log| log.topics.first().map(|v| v.eq_ignore_ascii_case(TRANSFER_TOPIC)).unwrap_or(false)),
                "has_approval_logs": bundle.receipt.logs.iter().any(|log| log.topics.first().map(|v| v.eq_ignore_ascii_case(APPROVAL_TOPIC)).unwrap_or(false)),
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
