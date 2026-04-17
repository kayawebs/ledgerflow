use chrono::Utc;
use serde_json::json;
use uuid::Uuid;

use crate::domain::entities::{EvidencePack, TransactionAnalysis};

#[derive(Default, Clone)]
pub struct EvidencePackBuilder;

impl EvidencePackBuilder {
    pub fn build(&self, analysis: &TransactionAnalysis) -> EvidencePack {
        EvidencePack {
            pack_id: Uuid::new_v4(),
            tx_hash: Some(analysis.tx_hash.clone()),
            attempt_id: analysis.attempt_id,
            ruleset_id: analysis.analysis_version_metadata.ruleset_id.clone(),
            recognizer_set_versions: analysis
                .analysis_version_metadata
                .recognizer_versions
                .clone(),
            input_digest: analysis.input_digest.clone(),
            created_at: Utc::now(),
            facts: serde_json::to_value(&analysis.normalized_execution_facts).unwrap_or_default(),
            labels: analysis
                .recognizer_results
                .iter()
                .map(|item| serde_json::to_value(item).unwrap_or_default())
                .collect(),
            flows: analysis
                .derived_flows
                .iter()
                .map(|item| serde_json::to_value(item).unwrap_or_default())
                .collect(),
            unresolved: analysis
                .unresolved_items
                .iter()
                .map(|item| serde_json::to_value(item).unwrap_or_default())
                .collect(),
            validation_results: analysis
                .validation_results
                .iter()
                .map(|item| serde_json::to_value(item).unwrap_or_default())
                .collect(),
            metadata: json!({
                "analysis_id": analysis.analysis_id,
                "chain_id": analysis.chain_id,
                "analysis_status": analysis.analysis_status,
            }),
        }
    }
}
