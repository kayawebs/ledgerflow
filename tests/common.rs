use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{Value, json};

use ledgerflow::{
    core::errors::{LedgerError, LedgerResult},
    ingestion::rpc::RpcClient,
    normalization::service::{APPROVAL_TOPIC, TRANSFER_TOPIC},
};

fn pad_address(address: &str) -> String {
    format!("0x{:0>64}", address.trim_start_matches("0x").to_lowercase())
}

fn pad_u256(value: u64) -> String {
    format!("0x{value:064x}")
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
        let (from_address, to_address, input) = if tx_hash.contains("flashloan-liquidation") {
            (
                "0x1111111111111111111111111111111111111111",
                "0x2222222222222222222222222222222222222222",
                format!("{}{}", "0x12345678", "00".repeat(32)),
            )
        } else if tx_hash.contains("executor-swap") {
            (
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "0x2222222222222222222222222222222222222222",
                format!("{}{}", "0x181a8b99", "00".repeat(32)),
            )
        } else if tx_hash.contains("executor-arbitrage") {
            (
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "0x2222222222222222222222222222222222222222",
                format!("{}{}", "0x181a8b99", "00".repeat(32)),
            )
        } else if tx_hash.contains("morpho-liquidation") {
            (
                "0x1111111111111111111111111111111111111111",
                "0x2222222222222222222222222222222222222222",
                format!("{}{}", "0x12345678", "00".repeat(32)),
            )
        } else if tx_hash.contains("liquidation") {
            (
                "0x1111111111111111111111111111111111111111",
                "0x2222222222222222222222222222222222222222",
                format!("{}{}", "0x12345678", "00".repeat(32)),
            )
        } else if tx_hash.contains("approve") {
            (
                "0x1111111111111111111111111111111111111111",
                "0x2222222222222222222222222222222222222222",
                format!("{}{}", "0x095ea7b3", "00".repeat(32)),
            )
        } else if tx_hash.contains("transfer") {
            (
                "0x1111111111111111111111111111111111111111",
                "0x2222222222222222222222222222222222222222",
                format!("{}{}", "0xa9059cbb", "00".repeat(32)),
            )
        } else {
            (
                "0x1111111111111111111111111111111111111111",
                "0x2222222222222222222222222222222222222222",
                format!("{}{}", "0x38ed1739", "00".repeat(32)),
            )
        };
        Ok(json!({
            "hash": tx_hash,
            "blockNumber": format!("0x{:x}", 123456u64),
            "from": from_address,
            "to": to_address,
            "value": format!("0x{:x}", 1000u64),
            "input": input,
            "nonce": "0x7",
            "chainId": chain_id,
        }))
    }

    async fn get_receipt(&self, _chain_id: u64, tx_hash: &str) -> LedgerResult<Value> {
        let status = if tx_hash.contains("fail") {
            "0x0"
        } else {
            "0x1"
        };
        if tx_hash.contains("flashloan-liquidation") {
            return Ok(json!({
                "status": status,
                "gasUsed": format!("0x{:x}", 21000u64),
                "effectiveGasPrice": format!("0x{:x}", 1_000_000_000u64),
                "logs": [
                    {
                        "logIndex": "0x0",
                        "address": "0x4200000000000000000000000000000000000006",
                        "topics": [
                            TRANSFER_TOPIC,
                            pad_address("0x9999999999999999999999999999999999999999"),
                            pad_address("0x2222222222222222222222222222222222222222")
                        ],
                        "data": pad_u256(43)
                    },
                    {
                        "logIndex": "0x1",
                        "address": "0xbbbbbbbbbb9cc5e90e3b3af64bdaf62c37eeffcb",
                        "topics": [
                            "0xa4946ede45d0c6f06a0f5ce92c9ad3b4751452d2fe0e25010783bcab57a67e41",
                            "0x090bb600c50dd50358b2b513eba567070b4f6f874a1cac2d7af7ff4da17be5a9",
                            pad_address("0x2222222222222222222222222222222222222222"),
                            pad_address("0xe7bc1ed115b368b946d97e45ee79f47a14ebf179")
                        ],
                        "data": "0x0"
                    },
                    {
                        "logIndex": "0x2",
                        "address": "0x4200000000000000000000000000000000000006",
                        "topics": [
                            TRANSFER_TOPIC,
                            pad_address("0x2222222222222222222222222222222222222222"),
                            pad_address("0xbbbbbbbbbb9cc5e90e3b3af64bdaf62c37eeffcb")
                        ],
                        "data": pad_u256(2)
                    },
                    {
                        "logIndex": "0x3",
                        "address": "0x091a5abe6616e26268e5eecff256c2212fce2707",
                        "topics": [
                            TRANSFER_TOPIC,
                            pad_address("0xbbbbbbbbbb9cc5e90e3b3af64bdaf62c37eeffcb"),
                            pad_address("0x2222222222222222222222222222222222222222")
                        ],
                        "data": pad_u256(1394)
                    }
                ]
            }));
        }
        if tx_hash.contains("executor-swap") {
            return Ok(json!({
                "status": status,
                "gasUsed": format!("0x{:x}", 21000u64),
                "effectiveGasPrice": format!("0x{:x}", 1_000_000_000u64),
                "logs": [
                    {
                        "logIndex": "0x0",
                        "address": "0x4200000000000000000000000000000000000006",
                        "topics": [
                            APPROVAL_TOPIC,
                            pad_address("0x2222222222222222222222222222222222222222"),
                            pad_address("0x4752ba5dbc23f44d87826276bf6fd6b1c372ad24")
                        ],
                        "data": pad_u256(5000)
                    },
                    {
                        "logIndex": "0x1",
                        "address": "0x4200000000000000000000000000000000000006",
                        "topics": [
                            TRANSFER_TOPIC,
                            pad_address("0x2222222222222222222222222222222222222222"),
                            pad_address("0x3333333333333333333333333333333333333333")
                        ],
                        "data": pad_u256(100)
                    },
                    {
                        "logIndex": "0x2",
                        "address": "0xe9cb5734ece50867d9c7549313ae86ea6bfe35ef",
                        "topics": [
                            TRANSFER_TOPIC,
                            pad_address("0x3333333333333333333333333333333333333333"),
                            pad_address("0x2222222222222222222222222222222222222222")
                        ],
                        "data": pad_u256(250)
                    }
                ]
            }));
        }
        if tx_hash.contains("executor-arbitrage") {
            return Ok(json!({
                "status": status,
                "gasUsed": format!("0x{:x}", 21000u64),
                "effectiveGasPrice": format!("0x{:x}", 1_000_000_000u64),
                "logs": [
                    {
                        "logIndex": "0x0",
                        "address": "0x4200000000000000000000000000000000000006",
                        "topics": [
                            TRANSFER_TOPIC,
                            pad_address("0x2222222222222222222222222222222222222222"),
                            pad_address("0x3333333333333333333333333333333333333333")
                        ],
                        "data": pad_u256(100)
                    },
                    {
                        "logIndex": "0x1",
                        "address": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                        "topics": [
                            TRANSFER_TOPIC,
                            pad_address("0x3333333333333333333333333333333333333333"),
                            pad_address("0x2222222222222222222222222222222222222222")
                        ],
                        "data": pad_u256(90)
                    },
                    {
                        "logIndex": "0x2",
                        "address": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                        "topics": [
                            TRANSFER_TOPIC,
                            pad_address("0x2222222222222222222222222222222222222222"),
                            pad_address("0x4444444444444444444444444444444444444444")
                        ],
                        "data": pad_u256(90)
                    },
                    {
                        "logIndex": "0x3",
                        "address": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                        "topics": [
                            TRANSFER_TOPIC,
                            pad_address("0x4444444444444444444444444444444444444444"),
                            pad_address("0x2222222222222222222222222222222222222222")
                        ],
                        "data": pad_u256(105)
                    }
                ]
            }));
        }
        if tx_hash.contains("morpho-liquidation") {
            return Ok(json!({
                "status": status,
                "gasUsed": format!("0x{:x}", 21000u64),
                "effectiveGasPrice": format!("0x{:x}", 1_000_000_000u64),
                "logs": [
                    {
                        "logIndex": "0x0",
                        "address": "0xbbbbbbbbbb9cc5e90e3b3af64bdaf62c37eeffcb",
                        "topics": [
                            "0xa4946ede45d0c6f06a0f5ce92c9ad3b4751452d2fe0e25010783bcab57a67e41",
                            "0x090bb600c50dd50358b2b513eba567070b4f6f874a1cac2d7af7ff4da17be5a9",
                            pad_address("0x2222222222222222222222222222222222222222"),
                            pad_address("0xe7bc1ed115b368b946d97e45ee79f47a14ebf179")
                        ],
                        "data": "0x0"
                    },
                    {
                        "logIndex": "0x1",
                        "address": "0x4200000000000000000000000000000000000006",
                        "topics": [
                            TRANSFER_TOPIC,
                            pad_address("0x2222222222222222222222222222222222222222"),
                            pad_address("0xbbbbbbbbbb9cc5e90e3b3af64bdaf62c37eeffcb")
                        ],
                        "data": pad_u256(42)
                    },
                    {
                        "logIndex": "0x2",
                        "address": "0x091a5abe6616e26268e5eecff256c2212fce2707",
                        "topics": [
                            TRANSFER_TOPIC,
                            pad_address("0xbbbbbbbbbb9cc5e90e3b3af64bdaf62c37eeffcb"),
                            pad_address("0x2222222222222222222222222222222222222222")
                        ],
                        "data": pad_u256(1394)
                    },
                    {
                        "logIndex": "0x3",
                        "address": "0x4200000000000000000000000000000000000006",
                        "topics": [
                            APPROVAL_TOPIC,
                            pad_address("0x2222222222222222222222222222222222222222"),
                            pad_address("0xbbbbbbbbbb9cc5e90e3b3af64bdaf62c37eeffcb")
                        ],
                        "data": pad_u256(42)
                    }
                ]
            }));
        }
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
                    "data": pad_u256(5000)
                },
                {
                    "logIndex": "0x1",
                    "address": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "topics": [
                        TRANSFER_TOPIC,
                        pad_address("0x1111111111111111111111111111111111111111"),
                        pad_address("0x3333333333333333333333333333333333333333")
                    ],
                    "data": pad_u256(100)
                },
                {
                    "logIndex": "0x2",
                    "address": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                    "topics": [
                        TRANSFER_TOPIC,
                        pad_address("0x4444444444444444444444444444444444444444"),
                        pad_address("0x1111111111111111111111111111111111111111")
                    ],
                    "data": pad_u256(95)
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
        if tx_hash.contains("flashloan-liquidation") {
            return Ok(Some(json!({
                "from": "0x1111111111111111111111111111111111111111",
                "to": "0x2222222222222222222222222222222222222222",
                "value": "0x0",
                "input": "0x12345678",
                "type": "call",
                "calls": [
                    {
                        "from": "0x2222222222222222222222222222222222222222",
                        "to": "0x9999999999999999999999999999999999999999",
                        "value": "0x0",
                        "input": "0x42b0b77c",
                        "type": "call",
                        "calls": []
                    },
                    {
                        "from": "0x2222222222222222222222222222222222222222",
                        "to": "0xbbbbbbbbbb9cc5e90e3b3af64bdaf62c37eeffcb",
                        "value": "0x0",
                        "input": "0x267a47ca",
                        "type": "call",
                        "calls": []
                    }
                ]
            })));
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
