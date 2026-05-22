use std::sync::Arc;

use chrono::{DateTime, TimeZone, Utc};
use serde_json::Value;

use crate::core::errors::{LedgerError, LedgerResult};
use crate::ingestion::models::{
    RawCode, RawLog, RawReceipt, RawTraceFrame, RawTransaction, RawTransactionBundle,
};
use crate::ingestion::rpc::RpcClient;

fn hex_to_u64(value: Option<&str>) -> Option<u64> {
    value
        .and_then(|raw| raw.strip_prefix("0x"))
        .and_then(|raw| u64::from_str_radix(raw, 16).ok())
}

fn parse_trace(payload: &Value) -> RawTraceFrame {
    let children = payload
        .get("calls")
        .and_then(Value::as_array)
        .map(|items| items.iter().map(parse_trace).collect())
        .unwrap_or_default();
    RawTraceFrame {
        trace_address: Vec::new(),
        from_address: payload
            .get("from")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned(),
        to_address: payload
            .get("to")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        value: payload
            .get("value")
            .and_then(Value::as_str)
            .unwrap_or("0x0")
            .to_owned(),
        input_data: payload
            .get("input")
            .and_then(Value::as_str)
            .unwrap_or("0x")
            .to_owned(),
        call_type: payload
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("call")
            .to_owned(),
        children,
    }
}

#[derive(Clone)]
pub struct TransactionBundleFetcher {
    rpc_client: Arc<dyn RpcClient>,
    trace_enabled: bool,
}

impl TransactionBundleFetcher {
    pub fn new(rpc_client: Arc<dyn RpcClient>, trace_enabled: bool) -> Self {
        Self {
            rpc_client,
            trace_enabled,
        }
    }

    pub async fn fetch(&self, chain_id: u64, tx_hash: &str) -> LedgerResult<RawTransactionBundle> {
        self.fetch_inner(chain_id, tx_hash, true, None).await
    }

    pub async fn fetch_without_code(
        &self,
        chain_id: u64,
        tx_hash: &str,
    ) -> LedgerResult<RawTransactionBundle> {
        self.fetch_inner(chain_id, tx_hash, false, None).await
    }

    pub async fn fetch_without_code_with_timestamp(
        &self,
        chain_id: u64,
        tx_hash: &str,
        block_timestamp: Option<DateTime<Utc>>,
    ) -> LedgerResult<RawTransactionBundle> {
        self.fetch_inner(chain_id, tx_hash, false, block_timestamp)
            .await
    }

    pub async fn fetch_without_code_from_tx_payload_with_timestamp(
        &self,
        chain_id: u64,
        tx_payload: Value,
        block_timestamp: Option<DateTime<Utc>>,
    ) -> LedgerResult<RawTransactionBundle> {
        self.fetch_inner_from_tx_payload(chain_id, tx_payload, None, false, block_timestamp)
            .await
    }

    pub async fn fetch_without_code_from_payloads_with_timestamp(
        &self,
        chain_id: u64,
        tx_payload: Value,
        receipt_payload: Value,
        block_timestamp: Option<DateTime<Utc>>,
    ) -> LedgerResult<RawTransactionBundle> {
        self.fetch_inner_from_tx_payload(
            chain_id,
            tx_payload,
            Some(receipt_payload),
            false,
            block_timestamp,
        )
        .await
    }

    async fn fetch_inner(
        &self,
        chain_id: u64,
        tx_hash: &str,
        include_target_code: bool,
        block_timestamp_override: Option<DateTime<Utc>>,
    ) -> LedgerResult<RawTransactionBundle> {
        let tx_payload = self.rpc_client.get_transaction(chain_id, tx_hash).await?;
        if tx_payload.is_null() {
            return Err(LedgerError::MalformedEvidence(format!(
                "missing tx payload for {tx_hash}"
            )));
        }
        self.fetch_inner_from_tx_payload(
            chain_id,
            tx_payload,
            None,
            include_target_code,
            block_timestamp_override,
        )
        .await
    }

    async fn fetch_inner_from_tx_payload(
        &self,
        chain_id: u64,
        tx_payload: Value,
        receipt_payload: Option<Value>,
        include_target_code: bool,
        block_timestamp_override: Option<DateTime<Utc>>,
    ) -> LedgerResult<RawTransactionBundle> {
        let tx_hash = tx_payload
            .get("hash")
            .and_then(Value::as_str)
            .ok_or_else(|| LedgerError::MalformedEvidence("missing tx hash".to_owned()))?
            .to_owned();
        let receipt_payload = match receipt_payload {
            Some(payload) => payload,
            None => self.rpc_client.get_receipt(chain_id, &tx_hash).await?,
        };
        let logs = receipt_payload
            .get("logs")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|log| RawLog {
                log_index: hex_to_u64(log.get("logIndex").and_then(Value::as_str))
                    .unwrap_or_default(),
                address: log
                    .get("address")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                topics: log
                    .get("topics")
                    .and_then(Value::as_array)
                    .map(|topics| {
                        topics
                            .iter()
                            .filter_map(Value::as_str)
                            .map(ToOwned::to_owned)
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default(),
                data: log
                    .get("data")
                    .and_then(Value::as_str)
                    .unwrap_or("0x")
                    .to_owned(),
            })
            .collect::<Vec<_>>();

        let receipt = RawReceipt {
            tx_hash: tx_hash.clone(),
            status: hex_to_u64(receipt_payload.get("status").and_then(Value::as_str))
                .unwrap_or_default(),
            gas_used: receipt_payload
                .get("gasUsed")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
            effective_gas_price: receipt_payload
                .get("effectiveGasPrice")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
            logs,
        };

        let tx = RawTransaction {
            chain_id,
            tx_hash,
            block_number: hex_to_u64(tx_payload.get("blockNumber").and_then(Value::as_str)),
            block_timestamp: None,
            from_address: tx_payload
                .get("from")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            to_address: tx_payload
                .get("to")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
            value: tx_payload
                .get("value")
                .and_then(Value::as_str)
                .unwrap_or("0x0")
                .to_owned(),
            input_data: tx_payload
                .get("input")
                .and_then(Value::as_str)
                .unwrap_or("0x")
                .to_owned(),
            nonce: hex_to_u64(tx_payload.get("nonce").and_then(Value::as_str)),
        };
        let block_timestamp = if block_timestamp_override.is_some() {
            block_timestamp_override
        } else if let Some(block_number) = tx.block_number {
            let block_payload = self.rpc_client.get_block(chain_id, block_number).await?;
            hex_to_u64(block_payload.get("timestamp").and_then(Value::as_str))
                .and_then(|value| Utc.timestamp_opt(value as i64, 0).single())
        } else {
            None
        };
        let tx = RawTransaction {
            block_timestamp,
            ..tx
        };

        let target_code = if include_target_code {
            if let Some(address) = tx.to_address.as_deref() {
                Some(RawCode {
                    address: address.to_owned(),
                    bytecode: self
                        .rpc_client
                        .get_code(chain_id, address, tx.block_number)
                        .await?,
                    block_number: tx.block_number,
                })
            } else {
                None
            }
        } else {
            None
        };

        let trace = if self.trace_enabled {
            match self.rpc_client.get_trace(chain_id, &tx.tx_hash).await {
                Ok(Some(value)) => Some(parse_trace(&value)),
                Ok(None) | Err(LedgerError::UnsupportedTrace(_)) => None,
                Err(error) => return Err(error),
            }
        } else {
            None
        };

        Ok(RawTransactionBundle {
            tx,
            receipt,
            trace,
            target_code,
            snapshots: Vec::new(),
            metadata: Value::Object(Default::default()),
        })
    }
}
