use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{Value, json};

use ledgerflow::{
    core::errors::{LedgerError, LedgerResult},
    ingestion::rpc::RpcClient,
    normalization::service::APPROVAL_TOPIC,
};

fn pad_address(address: &str) -> String {
    format!("0x{:0>64}", address.trim_start_matches("0x").to_lowercase())
}

#[derive(Clone)]
pub struct MockRpcClient {
    pub include_trace: bool,
    pub raise_trace_unsupported: bool,
}

impl MockRpcClient {
    pub fn boxed(self) -> Arc<dyn RpcClient> {
        Arc::new(self)
    }
}

impl Default for MockRpcClient {
    fn default() -> Self {
        Self {
            include_trace: true,
            raise_trace_unsupported: false,
        }
    }
}

#[async_trait]
impl RpcClient for MockRpcClient {
    async fn get_transaction(&self, chain_id: u64, tx_hash: &str) -> LedgerResult<Value> {
        let input = if tx_hash.contains("liquidation") {
            format!("{}{}", "0x12345678", "00".repeat(32))
        } else if tx_hash.contains("approve") {
            format!("{}{}", "0x095ea7b3", "00".repeat(32))
        } else if tx_hash.contains("transfer") {
            format!("{}{}", "0xa9059cbb", "00".repeat(32))
        } else {
            format!("{}{}", "0x38ed1739", "00".repeat(32))
        };
        Ok(json!({
            "hash": tx_hash,
            "blockNumber": format!("0x{:x}", 123456u64),
            "from": "0x1111111111111111111111111111111111111111",
            "to": "0x2222222222222222222222222222222222222222",
            "value": format!("0x{:x}", 1000u64),
            "input": input,
            "nonce": "0x7",
            "chainId": chain_id,
        }))
    }

    async fn get_receipt(&self, _chain_id: u64, tx_hash: &str) -> LedgerResult<Value> {
        let status = if tx_hash.contains("fail") { "0x0" } else { "0x1" };
        Ok(json!({
            "status": status,
            "gasUsed": format!("0x{:x}", 21000u64),
            "effectiveGasPrice": format!("0x{:x}", 1_000_000_000u64),
            "logs": [
                {
                    "logIndex": "0x0",
                    "address": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "topics": [
                        APPROVAL_TOPIC,
                        pad_address("0x1111111111111111111111111111111111111111"),
                        pad_address("0x2222222222222222222222222222222222222222")
                    ],
                    "data": format!("0x{:x}", 5000u64)
                },
                {
                    "logIndex": "0x1",
                    "address": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "topics": [
                        "0xddf252ad00000000000000000000000000000000000000000000000000000000",
                        pad_address("0x1111111111111111111111111111111111111111"),
                        pad_address("0x3333333333333333333333333333333333333333")
                    ],
                    "data": format!("0x{:x}", 100u64)
                },
                {
                    "logIndex": "0x2",
                    "address": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                    "topics": [
                        "0xddf252ad00000000000000000000000000000000000000000000000000000000",
                        pad_address("0x4444444444444444444444444444444444444444"),
                        pad_address("0x1111111111111111111111111111111111111111")
                    ],
                    "data": format!("0x{:x}", 95u64)
                }
            ]
        }))
    }

    async fn get_block(&self, _chain_id: u64, _block_number: u64) -> LedgerResult<Value> {
        Ok(json!({
            "timestamp": "0x661e4f80"
        }))
    }

    async fn get_trace(&self, _chain_id: u64, _tx_hash: &str) -> LedgerResult<Option<Value>> {
        let tx_hash = _tx_hash;
        if self.raise_trace_unsupported {
            return Err(LedgerError::UnsupportedTrace(
                "trace unavailable".to_owned(),
            ));
        }
        if !self.include_trace {
            return Ok(None);
        }
        if tx_hash.contains("liquidation") {
            return Ok(Some(json!({
                "from": "0x1111111111111111111111111111111111111111",
                "to": "0x2222222222222222222222222222222222222222",
                "value": "0x0",
                "input": "0x12345678",
                "type": "call",
                "calls": [
                    {
                        "from": "0x2222222222222222222222222222222222222222",
                        "to": "0x5555555555555555555555555555555555555555",
                        "value": "0x0",
                        "input": "0x00a718a9",
                        "type": "call",
                        "calls": []
                    },
                    {
                        "from": "0x2222222222222222222222222222222222222222",
                        "to": "0x3333333333333333333333333333333333333333",
                        "value": "0x0",
                        "input": "0xa9059cbb",
                        "type": "call",
                        "calls": []
                    }
                ]
            })));
        }
        Ok(Some(json!({
            "from": "0x1111111111111111111111111111111111111111",
            "to": "0x2222222222222222222222222222222222222222",
            "value": format!("0x{:x}", 1000u64),
            "input": "0x38ed1739",
            "type": "call",
            "calls": [
                {
                    "from": "0x2222222222222222222222222222222222222222",
                    "to": "0x3333333333333333333333333333333333333333",
                    "value": "0x0",
                    "input": "0xa9059cbb",
                    "type": "call",
                    "calls": []
                },
                {
                    "from": "0x2222222222222222222222222222222222222222",
                    "to": "0x4444444444444444444444444444444444444444",
                    "value": "0xa",
                    "input": "0x",
                    "type": "call",
                    "calls": []
                }
            ]
        })))
    }

    async fn get_code(
        &self,
        _chain_id: u64,
        _address: &str,
        _block_number: Option<u64>,
    ) -> LedgerResult<String> {
        Ok("0x6001600055".to_owned())
    }
}
