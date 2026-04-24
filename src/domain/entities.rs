use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::core::versioning::VersionBundle;
use crate::domain::enums::{
    AnalysisStatus, AssetType, ClusterBackfillStatus, ClusterTxStatus, SpendCategory,
    TransactionStatus,
};
use crate::domain::value_objects::{
    AssetFlow, CodeIdentitySummary, EvidenceRef, UnresolvedItem, ValidationMessage,
};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct NormalizedExecutionFacts {
    pub selector: Option<String>,
    pub call_count: u32,
    pub internal_call_count: u32,
    pub event_count: u32,
    pub call_tree: Value,
    pub participants: Vec<String>,
    pub log_summaries: Vec<Value>,
    pub derived_facts: Value,
    pub evidence_refs: Vec<EvidenceRef>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RecognizerResult {
    pub recognizer_id: String,
    pub recognizer_version: String,
    pub label: String,
    pub confidence: f32,
    pub evidence_refs: Vec<EvidenceRef>,
    pub attributes: Value,
    pub unresolved_points: Vec<UnresolvedItem>,
    pub warnings: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct TransactionAnalysis {
    pub analysis_id: Uuid,
    pub chain_id: u64,
    pub tx_hash: String,
    pub block_number: Option<u64>,
    pub block_timestamp: Option<DateTime<Utc>>,
    pub from_address: String,
    pub to_address: Option<String>,
    pub status: TransactionStatus,
    pub value: String,
    pub input_data: String,
    pub code_identity_summary: Option<CodeIdentitySummary>,
    pub raw_evidence_refs: Vec<EvidenceRef>,
    pub normalized_execution_facts: NormalizedExecutionFacts,
    pub derived_flows: Vec<AssetFlow>,
    pub recognizer_results: Vec<RecognizerResult>,
    pub scenario_outputs: Value,
    pub accounting_outputs: Value,
    pub analysis_status: AnalysisStatus,
    pub analysis_version_metadata: VersionBundle,
    pub unresolved_items: Vec<UnresolvedItem>,
    pub validation_results: Vec<ValidationMessage>,
    pub input_digest: String,
    pub attempt_id: Option<Uuid>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct EvidencePack {
    pub pack_id: Uuid,
    pub tx_hash: Option<String>,
    pub attempt_id: Option<Uuid>,
    pub ruleset_id: String,
    pub recognizer_set_versions: BTreeMap<String, String>,
    pub input_digest: String,
    pub created_at: DateTime<Utc>,
    pub facts: Value,
    pub labels: Vec<Value>,
    pub flows: Vec<Value>,
    pub unresolved: Vec<Value>,
    pub validation_results: Vec<Value>,
    pub metadata: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Task {
    pub task_id: Uuid,
    pub external_ref: Option<String>,
    pub metadata: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Opportunity {
    pub opportunity_id: Uuid,
    pub task_id: Option<Uuid>,
    pub label: Option<String>,
    pub metadata: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Attempt {
    pub attempt_id: Uuid,
    pub opportunity_id: Option<Uuid>,
    pub tx_hashes: Vec<String>,
    pub metadata: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ClusterAddress {
    pub address: String,
    pub label: Option<String>,
    pub role: Option<String>,
    pub metadata: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AddressCluster {
    pub cluster_id: Uuid,
    pub chain_id: u64,
    pub owner_label: String,
    pub name: String,
    pub description: Option<String>,
    pub addresses: Vec<ClusterAddress>,
    pub metadata: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ClusterSpendBreakdown {
    pub category: SpendCategory,
    pub asset_type: AssetType,
    pub token_address: Option<String>,
    pub counterparty: Option<String>,
    pub amount_raw: String,
    pub tx_count: u32,
    pub flow_count: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ClusterTransactionSummary {
    pub tx_hash: String,
    pub block_timestamp: Option<DateTime<Utc>>,
    pub from_address: String,
    pub to_address: Option<String>,
    pub status: TransactionStatus,
    pub recognizer_labels: Vec<String>,
    pub gas_cost_raw: String,
    pub external_native_outflow_raw: String,
    pub external_token_outflow_count: u32,
    pub internal_transfer_count: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ClusterSpendSummary {
    pub cluster_id: Uuid,
    pub chain_id: u64,
    pub owner_label: String,
    pub cluster_name: String,
    pub window_start: Option<DateTime<Utc>>,
    pub window_end: Option<DateTime<Utc>>,
    pub generated_at: DateTime<Utc>,
    pub address_count: u32,
    pub analyzed_tx_count: u32,
    pub total_gas_cost_raw: String,
    pub total_external_native_outflow_raw: String,
    pub total_internal_native_transfer_raw: String,
    pub total_incoming_native_raw: String,
    pub breakdowns: Vec<ClusterSpendBreakdown>,
    pub transactions: Vec<ClusterTransactionSummary>,
    pub metadata: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ClusterBackfillRun {
    pub run_id: Uuid,
    pub cluster_id: Uuid,
    pub chain_id: u64,
    pub start_block: Option<u64>,
    pub end_block: Option<u64>,
    pub status: ClusterBackfillStatus,
    pub requested_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub address_count: u32,
    pub discovered_tx_count: u32,
    pub analyzed_tx_count: u32,
    pub failed_tx_count: u32,
    pub metadata: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ClusterDiscoveredTx {
    pub cluster_id: Uuid,
    pub chain_id: u64,
    pub tx_hash: String,
    pub block_number: Option<u64>,
    pub status: ClusterTxStatus,
    pub latest_run_id: Option<Uuid>,
    pub first_seen_at: DateTime<Utc>,
    pub last_seen_at: DateTime<Utc>,
    pub analysis_attempted_at: Option<DateTime<Utc>>,
    pub analyzed_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub metadata: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ClusterCoverage {
    pub cluster_id: Uuid,
    pub chain_id: u64,
    pub discovered_tx_count: u32,
    pub analyzed_tx_count: u32,
    pub failed_tx_count: u32,
    pub pending_tx_count: u32,
    pub latest_run_id: Option<Uuid>,
    pub latest_run_status: Option<ClusterBackfillStatus>,
    pub latest_run_started_at: Option<DateTime<Utc>>,
    pub latest_run_finished_at: Option<DateTime<Utc>>,
    pub metadata: Value,
}
