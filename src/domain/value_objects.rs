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
