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
}

#[derive(Clone, Debug, Serialize)]
pub struct AddressTxQueryResponse {
    pub address: String,
    pub refs: Vec<AddressTxRef>,
}

#[derive(Clone, Debug, Serialize)]
pub struct AddressTxHashesResponse {
    pub address: String,
    pub tx_hashes: Vec<String>,
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
        let tx_index = parse_hex_u32(transaction.get("transactionIndex").and_then(Value::as_str))?;
        refs.push(AddressTxRef {
            chain_id,
            address: from_address,
            tx_hash: tx_hash.clone(),
            block_number,
            tx_index,
            direction: AddressTxDirection::From,
        });

        if let Some(to_address) = transaction.get("to").and_then(Value::as_str) {
            refs.push(AddressTxRef {
                chain_id,
                address: normalize_address(to_address)?,
                tx_hash,
                block_number,
                tx_index,
                direction: AddressTxDirection::To,
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
