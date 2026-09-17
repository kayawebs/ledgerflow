use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RawLog {
    pub log_index: u64,
    pub address: String,
    pub topics: Vec<String>,
    pub data: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RawTransaction {
    pub chain_id: u64,
    pub tx_hash: String,
    pub block_number: Option<u64>,
    pub block_timestamp: Option<DateTime<Utc>>,
    pub from_address: String,
    pub to_address: Option<String>,
    pub value: String,
    pub input_data: String,
    pub nonce: Option<u64>,
    pub tx_index: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RawReceipt {
    pub tx_hash: String,
    pub status: u64,
    pub gas_used: Option<String>,
    pub effective_gas_price: Option<String>,
    #[serde(default)]
    pub l1_fee: Option<String>,
    pub logs: Vec<RawLog>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RawTraceFrame {
    pub trace_address: Vec<u64>,
    pub from_address: String,
    pub to_address: Option<String>,
    pub value: String,
    pub input_data: String,
    pub call_type: String,
    pub children: Vec<RawTraceFrame>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RawCode {
    pub address: String,
    pub bytecode: String,
    pub block_number: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RawTransactionBundle {
    pub tx: RawTransaction,
    pub receipt: RawReceipt,
    pub trace: Option<RawTraceFrame>,
    pub target_code: Option<RawCode>,
    pub snapshots: Vec<Value>,
    pub metadata: Value,
}
