use std::collections::BTreeMap;

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
    #[serde(default)]
    pub block_hash: String,
    pub tx_index: u32,
    pub direction: AddressTxDirection,
    pub from_address: Option<String>,
    pub to_address: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CanonicalBlock {
    pub chain_id: u32,
    pub block_number: u32,
    pub block_hash: String,
    pub parent_hash: String,
    pub timestamp: u64,
    pub l1_block_number: Option<u64>,
    pub finality_state: String,
    pub is_canonical: u8,
    pub worker_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RawTransaction {
    pub chain_id: u32,
    pub block_number: u32,
    pub block_hash: String,
    pub tx_index: u32,
    pub tx_hash: String,
    pub from_address: String,
    pub to_address: Option<String>,
    pub nonce: u64,
    pub value: String,
    pub input: String,
    pub status: Option<u8>,
    pub gas_used: Option<u64>,
    pub effective_gas_price: Option<String>,
    pub contract_address: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RawLog {
    pub chain_id: u32,
    pub block_number: u32,
    pub block_hash: String,
    pub tx_hash: String,
    pub tx_index: u32,
    pub log_index: u32,
    pub address: String,
    pub topics: Vec<String>,
    pub data: String,
    pub is_removed: u8,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CanonicalAddressObservation {
    pub chain_id: u32,
    pub address: String,
    pub observation_role: String,
    pub first_seen_block: u32,
    pub last_seen_block: u32,
    pub observation_count: u64,
    pub sample_block_hash: String,
    pub sample_tx_hash: String,
    pub sample_log_index: Option<u32>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexedBlockFacts {
    pub block: CanonicalBlock,
    pub transactions: Vec<RawTransaction>,
    pub logs: Vec<RawLog>,
    pub refs: Vec<AddressTxRef>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct IndexerWorkerProgress {
    pub chain_id: u32,
    pub worker_id: String,
    pub worker_kind: String,
    pub last_indexed_block: u32,
    pub last_indexed_hash: String,
    pub range_start: u32,
    pub range_end: Option<u32>,
    pub rpc_url: String,
    pub retry_count: u32,
    pub last_error: String,
    pub last_success_at: Option<String>,
    pub head_block: Option<u32>,
    pub lag_blocks: Option<u32>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChainReorganization {
    pub event_id: String,
    pub chain_id: u32,
    pub worker_id: String,
    pub previous_tip: u32,
    pub common_ancestor: u32,
    pub new_head: u32,
    pub depth: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct IndexerStorageStats {
    pub canonical_block_count: u64,
    pub raw_transaction_count: u64,
    pub raw_log_count: u64,
    pub raw_trace_count: u64,
    pub orphaned_block_count: u64,
    pub reorg_count: u64,
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
    Ok(extract_block_facts(chain_id, "legacy", block_payload, None)?.refs)
}

pub fn extract_block_facts(
    chain_id: u32,
    worker_id: &str,
    block_payload: &Value,
    receipts_payload: Option<&Value>,
) -> LedgerResult<IndexedBlockFacts> {
    let block_number = parse_hex_u32(block_payload.get("number").and_then(Value::as_str))?;
    let block_hash = required_lower_hex(block_payload, "hash", "block")?;
    let parent_hash = required_lower_hex(block_payload, "parentHash", "block")?;
    let timestamp = parse_hex_u64(block_payload.get("timestamp").and_then(Value::as_str))?;
    let l1_block_number =
        parse_optional_hex_u64(block_payload.get("l1BlockNumber").and_then(Value::as_str))?;
    let transactions = block_payload
        .get("transactions")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            LedgerError::MalformedEvidence("block payload missing transactions array".to_owned())
        })?;

    let receipts = match receipts_payload {
        Some(payload) => {
            let items = payload.as_array().ok_or_else(|| {
                LedgerError::MalformedEvidence(
                    "eth_getBlockReceipts result is not an array".to_owned(),
                )
            })?;
            let mut by_hash = BTreeMap::new();
            for receipt in items {
                let tx_hash = required_lower_hex(receipt, "transactionHash", "receipt")?;
                by_hash.insert(tx_hash, receipt);
            }
            Some(by_hash)
        }
        None => None,
    };

    let mut refs = Vec::with_capacity(transactions.len() * 2);
    let mut raw_transactions = Vec::with_capacity(transactions.len());
    let mut raw_logs = Vec::new();
    for transaction in transactions {
        let tx_hash = required_lower_hex(transaction, "hash", "transaction")?;
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
        let receipt = receipts
            .as_ref()
            .map(|items| {
                items.get(&tx_hash).copied().ok_or_else(|| {
                    LedgerError::MalformedEvidence(format!(
                        "block {block_number} receipt missing for transaction {tx_hash}"
                    ))
                })
            })
            .transpose()?;
        if let Some(receipt) = receipt {
            let receipt_block_hash = required_lower_hex(receipt, "blockHash", "receipt")?;
            if receipt_block_hash != block_hash {
                return Err(LedgerError::MalformedEvidence(format!(
                    "transaction {tx_hash} receipt block hash does not match block {block_number}"
                )));
            }
        }
        let status = receipt
            .and_then(|item| item.get("status"))
            .and_then(Value::as_str)
            .map(|value| parse_hex_u64(Some(value)))
            .transpose()?
            .map(|value| u8::try_from(value).unwrap_or(u8::MAX));
        let gas_used = receipt
            .and_then(|item| item.get("gasUsed"))
            .and_then(Value::as_str)
            .map(|value| parse_hex_u64(Some(value)))
            .transpose()?;
        let effective_gas_price = receipt
            .and_then(|item| item.get("effectiveGasPrice"))
            .and_then(Value::as_str)
            .map(str::to_ascii_lowercase);
        let contract_address = receipt
            .and_then(|item| item.get("contractAddress"))
            .and_then(Value::as_str)
            .map(normalize_address)
            .transpose()?;

        raw_transactions.push(RawTransaction {
            chain_id,
            block_number,
            block_hash: block_hash.clone(),
            tx_index,
            tx_hash: tx_hash.clone(),
            from_address: from_address.clone(),
            to_address: to_address.clone(),
            nonce: parse_hex_u64(transaction.get("nonce").and_then(Value::as_str))?,
            value: transaction
                .get("value")
                .and_then(Value::as_str)
                .unwrap_or("0x0")
                .to_ascii_lowercase(),
            input: transaction
                .get("input")
                .and_then(Value::as_str)
                .unwrap_or("0x")
                .to_ascii_lowercase(),
            status,
            gas_used,
            effective_gas_price,
            contract_address,
        });

        if let Some(logs) = receipt
            .and_then(|item| item.get("logs"))
            .and_then(Value::as_array)
        {
            for log in logs {
                let log_block_hash = required_lower_hex(log, "blockHash", "log")?;
                if log_block_hash != block_hash {
                    return Err(LedgerError::MalformedEvidence(format!(
                        "transaction {tx_hash} log block hash does not match block {block_number}"
                    )));
                }
                let log_tx_hash = required_lower_hex(log, "transactionHash", "log")?;
                if log_tx_hash != tx_hash {
                    return Err(LedgerError::MalformedEvidence(format!(
                        "receipt log transaction hash does not match {tx_hash}"
                    )));
                }
                let topics = log
                    .get("topics")
                    .and_then(Value::as_array)
                    .ok_or_else(|| {
                        LedgerError::MalformedEvidence("log missing topics array".to_owned())
                    })?
                    .iter()
                    .map(|topic| {
                        topic.as_str().map(str::to_ascii_lowercase).ok_or_else(|| {
                            LedgerError::MalformedEvidence("log topic is not a string".to_owned())
                        })
                    })
                    .collect::<LedgerResult<Vec<_>>>()?;
                raw_logs.push(RawLog {
                    chain_id,
                    block_number,
                    block_hash: block_hash.clone(),
                    tx_hash: tx_hash.clone(),
                    tx_index,
                    log_index: parse_hex_u32(log.get("logIndex").and_then(Value::as_str))?,
                    address: normalize_address(
                        log.get("address").and_then(Value::as_str).ok_or_else(|| {
                            LedgerError::MalformedEvidence("log missing address".to_owned())
                        })?,
                    )?,
                    topics,
                    data: log
                        .get("data")
                        .and_then(Value::as_str)
                        .unwrap_or("0x")
                        .to_ascii_lowercase(),
                    is_removed: u8::from(
                        log.get("removed").and_then(Value::as_bool).unwrap_or(false),
                    ),
                });
            }
        }

        refs.push(AddressTxRef {
            chain_id,
            address: from_address.clone(),
            tx_hash: tx_hash.clone(),
            block_number,
            block_hash: block_hash.clone(),
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
                block_hash: block_hash.clone(),
                tx_index,
                direction: AddressTxDirection::To,
                from_address: Some(from_address),
                to_address: Some(to_address),
            });
        }
    }
    Ok(IndexedBlockFacts {
        block: CanonicalBlock {
            chain_id,
            block_number,
            block_hash,
            parent_hash,
            timestamp,
            l1_block_number,
            finality_state: "soft_confirmed".to_owned(),
            is_canonical: 1,
            worker_id: worker_id.to_owned(),
        },
        transactions: raw_transactions,
        logs: raw_logs,
        refs,
    })
}

fn parse_hex_u32(value: Option<&str>) -> LedgerResult<u32> {
    let value =
        value.ok_or_else(|| LedgerError::MalformedEvidence("missing hex field".to_owned()))?;
    u32::from_str_radix(value.trim_start_matches("0x"), 16)
        .map_err(|err| LedgerError::MalformedEvidence(err.to_string()))
}

fn parse_hex_u64(value: Option<&str>) -> LedgerResult<u64> {
    let value =
        value.ok_or_else(|| LedgerError::MalformedEvidence("missing hex field".to_owned()))?;
    u64::from_str_radix(value.trim_start_matches("0x"), 16)
        .map_err(|err| LedgerError::MalformedEvidence(err.to_string()))
}

fn parse_optional_hex_u64(value: Option<&str>) -> LedgerResult<Option<u64>> {
    value.map(|value| parse_hex_u64(Some(value))).transpose()
}

fn required_lower_hex(payload: &Value, field: &str, kind: &str) -> LedgerResult<String> {
    payload
        .get(field)
        .and_then(Value::as_str)
        .map(str::to_ascii_lowercase)
        .ok_or_else(|| LedgerError::MalformedEvidence(format!("{kind} missing {field}")))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{AddressTxDirection, extract_block_facts, extract_block_refs};

    #[test]
    fn extract_block_refs_emits_from_and_to_rows() {
        let refs = extract_block_refs(
            8453,
            &json!({
                "number": "0x10",
                "hash": "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
                "parentHash": "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
                "timestamp": "0x1",
                "transactions": [
                    {
                        "hash": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                        "from": "0x1111111111111111111111111111111111111111",
                        "to": "0x2222222222222222222222222222222222222222",
                        "transactionIndex": "0x0",
                        "nonce": "0x1"
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
                "hash": "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
                "parentHash": "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
                "timestamp": "0x1",
                "transactions": [
                    {
                        "hash": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                        "from": "0x1111111111111111111111111111111111111111",
                        "to": null,
                        "transactionIndex": "0x1",
                        "nonce": "0x2"
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
                "hash": "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
                "parentHash": "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
                "timestamp": "0x1",
                "transactions": [
                    {
                        "hash": "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
                        "from": "0x1111111111111111111111111111111111111111",
                        "to": "0x1111111111111111111111111111111111111111",
                        "transactionIndex": "0x2",
                        "nonce": "0x3"
                    }
                ]
            }),
        )
        .unwrap();
        assert_eq!(refs.len(), 2);
    }

    #[test]
    fn extract_block_facts_joins_receipt_status_and_logs() {
        let block_hash = "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
        let tx_hash = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let facts = extract_block_facts(
            4663,
            "robinhood_live",
            &json!({
                "number": "0x10",
                "hash": block_hash,
                "parentHash": "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
                "timestamp": "0x2",
                "l1BlockNumber": "0x8",
                "transactions": [{
                    "hash": tx_hash,
                    "from": "0x1111111111111111111111111111111111111111",
                    "to": "0x2222222222222222222222222222222222222222",
                    "transactionIndex": "0x0",
                    "nonce": "0x1",
                    "value": "0x2a",
                    "input": "0x1234"
                }]
            }),
            Some(&json!([{
                "transactionHash": tx_hash,
                "blockHash": block_hash,
                "status": "0x1",
                "gasUsed": "0x5208",
                "effectiveGasPrice": "0x3b9aca00",
                "contractAddress": null,
                "logs": [{
                    "blockHash": block_hash,
                    "transactionHash": tx_hash,
                    "transactionIndex": "0x0",
                    "logIndex": "0x0",
                    "address": "0x3333333333333333333333333333333333333333",
                    "topics": ["0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"],
                    "data": "0x01",
                    "removed": false
                }]
            }])),
        )
        .unwrap();

        assert_eq!(facts.block.l1_block_number, Some(8));
        assert_eq!(facts.transactions[0].status, Some(1));
        assert_eq!(facts.transactions[0].gas_used, Some(21_000));
        assert_eq!(facts.logs.len(), 1);
        assert_eq!(facts.refs[0].block_hash, block_hash);
    }
}
