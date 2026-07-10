use async_trait::async_trait;
use reqwest::Client;
use serde_json::{Value, json};

use crate::core::errors::{LedgerError, LedgerResult};

#[async_trait]
pub trait RpcClient: Send + Sync {
    async fn get_transaction(&self, chain_id: u64, tx_hash: &str) -> LedgerResult<Value>;
    async fn get_receipt(&self, chain_id: u64, tx_hash: &str) -> LedgerResult<Value>;
    async fn get_block(&self, chain_id: u64, block_number: u64) -> LedgerResult<Value>;
    async fn get_trace(&self, chain_id: u64, tx_hash: &str) -> LedgerResult<Option<Value>>;
    async fn get_code(
        &self,
        chain_id: u64,
        address: &str,
        block_number: Option<u64>,
    ) -> LedgerResult<String>;
    async fn get_balance(
        &self,
        _chain_id: u64,
        _address: &str,
        _block_number: Option<u64>,
    ) -> LedgerResult<String> {
        Err(LedgerError::TransientIngestion(
            "eth_getBalance is not implemented for this rpc client".to_owned(),
        ))
    }
    async fn eth_call(
        &self,
        _chain_id: u64,
        _to: &str,
        _data: &str,
        _block_number: Option<u64>,
    ) -> LedgerResult<String> {
        Err(LedgerError::UnsupportedTrace(
            "eth_call is not implemented for this rpc client".to_owned(),
        ))
    }
}

#[derive(Clone)]
pub struct HttpRpcClient {
    rpc_url: String,
    client: Client,
}

impl HttpRpcClient {
    pub fn new(rpc_url: String) -> Self {
        Self {
            rpc_url,
            client: Client::new(),
        }
    }

    pub async fn get_block_receipts(&self, block_number: u64) -> LedgerResult<Vec<Value>> {
        let result = self
            .call(
                "eth_getBlockReceipts",
                json!([format!("0x{block_number:x}")]),
            )
            .await?;
        result.as_array().cloned().ok_or_else(|| {
            LedgerError::MalformedEvidence(
                "eth_getBlockReceipts response was not an array".to_owned(),
            )
        })
    }

    pub async fn get_logs_by_topics(
        &self,
        from_block: u64,
        to_block: u64,
        topic0s: &[&str],
    ) -> LedgerResult<Vec<Value>> {
        let topics = topic0s
            .iter()
            .map(|topic| Value::String((*topic).to_owned()))
            .collect::<Vec<_>>();
        let result = self
            .call(
                "eth_getLogs",
                json!([{
                    "fromBlock": format!("0x{from_block:x}"),
                    "toBlock": format!("0x{to_block:x}"),
                    "topics": [topics],
                }]),
            )
            .await?;
        result.as_array().cloned().ok_or_else(|| {
            LedgerError::MalformedEvidence("eth_getLogs response was not an array".to_owned())
        })
    }

    async fn call(&self, method: &str, params: Value) -> LedgerResult<Value> {
        let response = self
            .client
            .post(&self.rpc_url)
            .json(&json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": method,
                "params": params
            }))
            .send()
            .await
            .map_err(|err| LedgerError::TransientIngestion(err.to_string()))?;
        let payload = response
            .json::<Value>()
            .await
            .map_err(|err| LedgerError::TransientIngestion(err.to_string()))?;
        if payload.get("error").is_some() {
            return Err(LedgerError::TransientIngestion(
                payload["error"].to_string(),
            ));
        }
        Ok(payload["result"].clone())
    }
}

#[async_trait]
impl RpcClient for HttpRpcClient {
    async fn get_transaction(&self, _chain_id: u64, tx_hash: &str) -> LedgerResult<Value> {
        self.call("eth_getTransactionByHash", json!([tx_hash]))
            .await
    }

    async fn get_receipt(&self, _chain_id: u64, tx_hash: &str) -> LedgerResult<Value> {
        self.call("eth_getTransactionReceipt", json!([tx_hash]))
            .await
    }

    async fn get_block(&self, _chain_id: u64, block_number: u64) -> LedgerResult<Value> {
        self.call(
            "eth_getBlockByNumber",
            json!([format!("0x{block_number:x}"), false]),
        )
        .await
    }

    async fn get_trace(&self, _chain_id: u64, tx_hash: &str) -> LedgerResult<Option<Value>> {
        match self
            .call(
                "debug_traceTransaction",
                json!([tx_hash, { "tracer": "callTracer" }]),
            )
            .await
        {
            Ok(result) => Ok(Some(result)),
            Err(LedgerError::TransientIngestion(message)) => {
                Err(LedgerError::UnsupportedTrace(message))
            }
            Err(other) => Err(other),
        }
    }

    async fn get_code(
        &self,
        _chain_id: u64,
        address: &str,
        block_number: Option<u64>,
    ) -> LedgerResult<String> {
        let block_ref = block_number
            .map(|value| format!("0x{value:x}"))
            .unwrap_or_else(|| "latest".to_owned());
        let result = self
            .call("eth_getCode", json!([address, block_ref]))
            .await?;
        result.as_str().map(ToOwned::to_owned).ok_or_else(|| {
            LedgerError::MalformedEvidence("eth_getCode response was not a string".to_owned())
        })
    }

    async fn get_balance(
        &self,
        _chain_id: u64,
        address: &str,
        block_number: Option<u64>,
    ) -> LedgerResult<String> {
        let block_ref = block_number
            .map(|value| format!("0x{value:x}"))
            .unwrap_or_else(|| "latest".to_owned());
        let result = self
            .call("eth_getBalance", json!([address, block_ref]))
            .await?;
        result.as_str().map(ToOwned::to_owned).ok_or_else(|| {
            LedgerError::MalformedEvidence("eth_getBalance response was not a string".to_owned())
        })
    }

    async fn eth_call(
        &self,
        _chain_id: u64,
        to: &str,
        data: &str,
        block_number: Option<u64>,
    ) -> LedgerResult<String> {
        let block_ref = block_number
            .map(|value| format!("0x{value:x}"))
            .unwrap_or_else(|| "latest".to_owned());
        let result = self
            .call("eth_call", json!([{ "to": to, "data": data }, block_ref]))
            .await?;
        result.as_str().map(ToOwned::to_owned).ok_or_else(|| {
            LedgerError::MalformedEvidence("eth_call response was not a string".to_owned())
        })
    }
}
