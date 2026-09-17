use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::domain::enums::{AddressHitDirection, AssetType, EvidenceKind, ValidationStatus};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct EvidenceRef {
    pub kind: EvidenceKind,
    pub reference: String,
    pub tx_hash: Option<String>,
    pub metadata: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct CodeIdentitySummary {
    pub address: Option<String>,
    pub code_hash: Option<String>,
    pub bytecode_length: Option<usize>,
    pub confidence: f32,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct UnresolvedItem {
    pub code: String,
    pub message: String,
    pub evidence_refs: Vec<EvidenceRef>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AssetFlow {
    pub flow_id: String,
    pub asset_type: AssetType,
    pub token_address: Option<String>,
    pub from_address: String,
    pub to_address: String,
    pub amount_raw: String,
    pub direction_hint: Option<String>,
    pub evidence_refs: Vec<EvidenceRef>,
    pub derivation_method: String,
    pub confidence: f32,
    pub metadata: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ValidationMessage {
    pub validator_id: String,
    pub status: ValidationStatus,
    pub message: String,
    pub details: Value,
    pub evidence_refs: Vec<EvidenceRef>,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AddressTxRef {
    pub address: String,
    pub tx_hash: String,
    pub block_number: u64,
    pub tx_index: u32,
    pub direction: AddressHitDirection,
    pub from_address: Option<String>,
    pub to_address: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AddressTxDiscovery {
    pub refs: Vec<AddressTxRef>,
    /// `None` means the source cannot prove block-range completeness. It must
    /// never be interpreted as complete by an intelligence consumer.
    pub coverage_complete: Option<bool>,
    pub indexed_block_count: Option<u64>,
    pub requested_block_count: Option<u64>,
}

/// Auditable, URL-free runtime provenance for a chain data plane. Optional
/// fields remain null when a source cannot prove them; callers must not infer
/// full-history coverage from a live indexed range.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ChainDataSourceStatus {
    pub chain_id: u64,
    pub data_source: String,
    pub indexed_from_block: Option<u64>,
    pub indexed_to_block: Option<u64>,
    pub chain_head: Option<u64>,
    pub ingestion_lag: Option<u64>,
    pub last_updated_at: Option<DateTime<Utc>>,
    pub history_mode: String,
    pub full_history_available: bool,
    pub available: bool,
    pub limitation: Option<String>,
}
