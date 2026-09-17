use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::core::versioning::VersionBundle;
use crate::domain::enums::{
    AlertSeverity, AlertType, AnalysisStatus, AssetType, BehaviorTag, ClusterBackfillStatus,
    ClusterTrackingStatus, ClusterTxStatus, MetricsWindow, SpendCategory, TransactionStatus,
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
pub struct AddressAnalysisCandidate {
    pub address: String,
    pub priority: i32,
    pub metadata: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AddressProfile {
    pub chain_id: u64,
    pub address: String,
    pub address_type: String,
    pub code_hash: Option<String>,
    pub bytecode_length: Option<u64>,
    pub first_seen_at: DateTime<Utc>,
    pub last_seen_at: DateTime<Utc>,
    pub last_analyzed_at: Option<DateTime<Utc>>,
    pub confidence: f32,
    pub metadata: Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AddressLabel {
    pub chain_id: u64,
    pub address: String,
    pub label: String,
    pub source: String,
    pub confidence: f32,
    pub evidence_json: Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// A provenance-preserving claim about an address. Unlike `AddressLabel`, which
/// is a read projection, assertions keep the producer, method version,
/// confidence, validity, and evidence needed to recompute effective labels.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AddressLabelAssertion {
    pub assertion_id: Uuid,
    pub claim_fingerprint: String,
    pub chain_id: u64,
    pub address: String,
    pub label: String,
    pub dimension: String,
    pub exclusive_group: Option<String>,
    pub source: String,
    pub source_kind: String,
    pub source_trust: f32,
    pub origin_kind: String,
    pub method_version: String,
    pub dataset_key: Option<String>,
    pub dataset_version: Option<String>,
    pub assertion_value: bool,
    pub confidence: f32,
    pub status: String,
    pub valid_from_block: Option<u64>,
    pub valid_to_block: Option<u64>,
    pub observed_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
    pub evidence_json: Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ProtocolAddressMetadata {
    pub chain_id: u64,
    pub address: String,
    pub protocol_family: String,
    pub protocol_name: String,
    pub contract_role: String,
    pub source: String,
    pub adapter_version: String,
    pub confidence: f32,
    pub metadata: Value,
    pub first_seen_at: DateTime<Utc>,
    pub last_seen_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AddressAnalysisJob {
    pub chain_id: u64,
    pub address: String,
    pub status: String,
    pub attempts: u32,
    pub last_error: Option<String>,
    pub priority: i32,
    pub first_seen_at: DateTime<Utc>,
    pub last_seen_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub claimed_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
    pub metadata: Value,
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
pub struct AppUser {
    pub user_id: Uuid,
    pub email: String,
    pub workspace_plan: String,
    pub email_verified_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct UserSession {
    pub session_id: Uuid,
    pub user_id: Uuid,
    pub expires_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub last_seen_at: DateTime<Utc>,
    pub revoked_at: Option<DateTime<Utc>>,
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
    pub analysis_status: AnalysisStatus,
    pub recognizer_labels: Vec<String>,
    pub gas_cost_raw: String,
    pub external_native_outflow_raw: String,
    pub external_token_outflow_count: u32,
    pub internal_transfer_count: u32,
    pub unresolved_count: u32,
    pub trace_available: bool,
    pub counterparties: Vec<String>,
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
pub struct ClusterDailyAssetChange {
    pub day: String,
    pub asset_type: AssetType,
    pub token_address: Option<String>,
    pub token_symbol: Option<String>,
    pub token_name: Option<String>,
    pub token_decimals: Option<u8>,
    pub incoming_raw: String,
    pub outgoing_raw: String,
    pub gas_raw: String,
    pub internal_raw: String,
    pub net_raw: String,
    pub incoming_decimal: Option<String>,
    pub outgoing_decimal: Option<String>,
    pub gas_decimal: Option<String>,
    pub internal_decimal: Option<String>,
    pub net_decimal: Option<String>,
    pub incoming_usd: Option<String>,
    pub outgoing_usd: Option<String>,
    pub internal_usd: Option<String>,
    pub net_usd: Option<String>,
    pub price_sources: Vec<String>,
    pub tx_count: u32,
    pub flow_count: u32,
    pub sample_tx_hashes: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ClusterAssetPurposeBreakdown {
    pub day: String,
    pub asset_type: AssetType,
    pub token_address: Option<String>,
    pub token_symbol: Option<String>,
    pub token_name: Option<String>,
    pub token_decimals: Option<u8>,
    pub purpose: String,
    pub counterparty: Option<String>,
    pub amount_raw: String,
    pub amount_decimal: Option<String>,
    pub amount_usd: Option<String>,
    pub price_sources: Vec<String>,
    pub tx_count: u32,
    pub flow_count: u32,
    pub sample_tx_hashes: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ClusterAssetChangeReport {
    pub cluster_id: Uuid,
    pub chain_id: u64,
    pub owner_label: String,
    pub cluster_name: String,
    pub window_start: Option<DateTime<Utc>>,
    pub window_end: Option<DateTime<Utc>>,
    pub generated_at: DateTime<Utc>,
    pub address_count: u32,
    pub analyzed_tx_count: u32,
    pub daily_changes: Vec<ClusterDailyAssetChange>,
    pub purpose_breakdowns: Vec<ClusterAssetPurposeBreakdown>,
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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ClusterTrackingState {
    pub cluster_id: Uuid,
    pub chain_id: u64,
    pub status: ClusterTrackingStatus,
    pub last_processed_block: Option<u64>,
    pub last_processed_at: Option<DateTime<Utc>>,
    pub last_updated_at: Option<DateTime<Utc>>,
    pub error_message: Option<String>,
    pub tracked_address_count: u32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub metadata: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ClusterMetricsSnapshot {
    pub snapshot_id: Uuid,
    pub cluster_id: Uuid,
    pub chain_id: u64,
    pub window: MetricsWindow,
    pub timestamp: DateTime<Utc>,
    pub tx_count: u32,
    pub volume_usd: Option<f64>,
    pub gas_cost_usd: Option<f64>,
    pub realized_pnl_usd: Option<f64>,
    pub roi: Option<f64>,
    pub active_addresses_count: u32,
    pub behavior_breakdown: Value,
    pub metadata: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ClusterStrategyReportSnapshot {
    pub snapshot_id: Uuid,
    pub cluster_id: Uuid,
    pub chain_id: u64,
    pub window_start: Option<DateTime<Utc>>,
    pub window_end: Option<DateTime<Utc>>,
    pub window_key: String,
    pub generated_at: DateTime<Utc>,
    pub report_json: Value,
    pub metadata: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ClassifiedTransaction {
    pub cluster_id: Uuid,
    pub chain_id: u64,
    pub tx_hash: String,
    pub address: String,
    pub block_number: u64,
    pub timestamp: Option<DateTime<Utc>>,
    pub behavior_tag: BehaviorTag,
    pub protocol: Option<String>,
    pub token_in: Option<String>,
    pub token_out: Option<String>,
    pub amount_in: Option<String>,
    pub amount_out: Option<String>,
    pub value_usd: Option<f64>,
    pub gas_cost_usd: Option<f64>,
    pub estimated_pnl_usd: Option<f64>,
    pub raw_json: Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ClusterAlert {
    pub alert_id: Uuid,
    pub cluster_id: Uuid,
    pub chain_id: u64,
    pub alert_type: AlertType,
    pub severity: AlertSeverity,
    pub title: String,
    pub message: String,
    pub related_metric: Value,
    pub triggered_at: DateTime<Utc>,
    pub is_read: bool,
    pub read_at: Option<DateTime<Utc>>,
    pub metadata: Value,
}
