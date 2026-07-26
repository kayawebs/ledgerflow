use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::core::errors::{LedgerError, LedgerResult};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AddressTxDirection {
    From,
    To,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct AddressTxRef {
    pub chain_id: u32,
    pub address: String,
    pub tx_hash: String,
    pub block_number: u32,
    pub tx_index: u32,
    pub direction: AddressTxDirection,
    pub from_address: Option<String>,
    pub to_address: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct AddressTxQueryResponse {
    pub address: String,
    pub refs: Vec<AddressTxRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub coverage: Option<LazyIndexCoverage>,
}

#[derive(Clone, Debug, Serialize)]
pub struct AddressTxHashesResponse {
    pub address: String,
    pub tx_hashes: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub coverage: Option<LazyIndexCoverage>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct LazyIndexCoverage {
    pub from_block: u32,
    pub to_block: u32,
    pub requested_block_count: u64,
    pub indexed_block_count: u64,
    pub complete: bool,
    pub request_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct LazyIndexingRequest {
    pub request_id: String,
    pub chain_id: u32,
    pub requested_address: String,
    pub from_block: u32,
    pub to_block: u32,
    pub next_block: u32,
    pub status: String,
    pub last_error: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct IndexedBlock {
    pub chain_id: u32,
    pub block_number: u32,
    pub block_hash: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IndexerApiKeyStatus {
    Active,
    Revoked,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct IndexerApiKeyRecord {
    pub key_id: String,
    #[serde(skip_serializing, default)]
    pub token_hash: Option<String>,
    pub owner_user_id: String,
    pub owner_email: String,
    pub label: String,
    pub token_prefix: String,
    pub status: IndexerApiKeyStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub last_used_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CreateIndexerApiKeyRequest {
    pub owner_user_id: String,
    pub owner_email: String,
    pub label: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RevokeIndexerApiKeyRequest {
    pub owner_user_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CreateIndexerApiKeyResponse {
    pub record: IndexerApiKeyRecord,
    pub api_key: String,
}

pub fn normalize_address(address: &str) -> LedgerResult<String> {
    let address = address.trim().to_lowercase();
    if address.len() != 42 || !address.starts_with("0x") {
        return Err(LedgerError::MalformedEvidence(format!(
            "invalid address: {address}"
        )));
    }
    let hex_part = &address[2..];
    if !hex_part.chars().all(|char| char.is_ascii_hexdigit()) {
        return Err(LedgerError::MalformedEvidence(format!(
            "invalid address hex: {address}"
        )));
    }
    Ok(address)
}

pub fn extract_block_refs(chain_id: u32, block_payload: &Value) -> LedgerResult<Vec<AddressTxRef>> {
    let block_number = parse_hex_u32(block_payload.get("number").and_then(Value::as_str))?;
    let transactions = block_payload
        .get("transactions")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            LedgerError::MalformedEvidence("block payload missing transactions array".to_owned())
        })?;

    let mut refs = Vec::with_capacity(transactions.len() * 2);
    for transaction in transactions {
        let tx_hash = transaction
            .get("hash")
            .and_then(Value::as_str)
            .ok_or_else(|| LedgerError::MalformedEvidence("transaction missing hash".to_owned()))?
            .to_lowercase();
        let from_address =
            normalize_address(transaction.get("from").and_then(Value::as_str).ok_or_else(
                || LedgerError::MalformedEvidence("transaction missing from".to_owned()),
            )?)?;
        let to_address = transaction
            .get("to")
            .and_then(Value::as_str)
            .map(normalize_address)
            .transpose()?;
        let tx_index = parse_hex_u32(transaction.get("transactionIndex").and_then(Value::as_str))?;
        refs.push(AddressTxRef {
            chain_id,
            address: from_address.clone(),
            tx_hash: tx_hash.clone(),
            block_number,
            tx_index,
            direction: AddressTxDirection::From,
            from_address: Some(from_address.clone()),
            to_address: to_address.clone(),
        });

        if let Some(to_address) = to_address {
            refs.push(AddressTxRef {
                chain_id,
                address: to_address.clone(),
                tx_hash,
                block_number,
                tx_index,
                direction: AddressTxDirection::To,
                from_address: Some(from_address),
                to_address: Some(to_address),
            });
        }
    }
    Ok(refs)
}

fn parse_hex_u32(value: Option<&str>) -> LedgerResult<u32> {
    let value =
        value.ok_or_else(|| LedgerError::MalformedEvidence("missing hex field".to_owned()))?;
    u32::from_str_radix(value.trim_start_matches("0x"), 16)
        .map_err(|err| LedgerError::MalformedEvidence(err.to_string()))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{AddressTxDirection, extract_block_refs};

    #[test]
    fn extract_block_refs_emits_from_and_to_rows() {
        let refs = extract_block_refs(
            8453,
            &json!({
                "number": "0x10",
                "transactions": [
                    {
                        "hash": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                        "from": "0x1111111111111111111111111111111111111111",
                        "to": "0x2222222222222222222222222222222222222222",
                        "transactionIndex": "0x0"
                    }
                ]
            }),
        )
        .unwrap();
        assert_eq!(refs.len(), 2);
        assert_eq!(refs[0].direction, AddressTxDirection::From);
        assert_eq!(refs[1].direction, AddressTxDirection::To);
    }

    #[test]
    fn extract_block_refs_supports_contract_creation() {
        let refs = extract_block_refs(
            8453,
            &json!({
                "number": "0x10",
                "transactions": [
                    {
                        "hash": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                        "from": "0x1111111111111111111111111111111111111111",
                        "to": null,
                        "transactionIndex": "0x1"
                    }
                ]
            }),
        )
        .unwrap();
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].direction, AddressTxDirection::From);
    }

    #[test]
    fn extract_block_refs_keeps_both_directions_for_self_send() {
        let refs = extract_block_refs(
            8453,
            &json!({
                "number": "0x10",
                "transactions": [
                    {
                        "hash": "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
                        "from": "0x1111111111111111111111111111111111111111",
                        "to": "0x1111111111111111111111111111111111111111",
                        "transactionIndex": "0x2"
                    }
                ]
            }),
        )
        .unwrap();
        assert_eq!(refs.len(), 2);
    }
}
