use reqwest::Client;
use serde_json::{Value, json};

use crate::core::errors::{LedgerError, LedgerResult};

#[derive(Clone)]
pub struct IndexerRpcClient {
    rpc_url: String,
    client: Client,
}

impl IndexerRpcClient {
    pub fn new(rpc_url: String) -> Self {
        Self {
            rpc_url,
            client: Client::new(),
        }
    }

    pub async fn get_latest_block_number(&self) -> LedgerResult<u64> {
        let result = self.call("eth_blockNumber", json!([])).await?;
        parse_hex_u64(result.as_str())
    }

    pub async fn get_block_with_transactions(&self, block_number: u64) -> LedgerResult<Value> {
        self.call(
            "eth_getBlockByNumber",
            json!([format!("0x{block_number:x}"), true]),
        )
        .await
    }

    pub async fn get_transaction_by_hash(&self, tx_hash: &str) -> LedgerResult<Value> {
        self.call("eth_getTransactionByHash", json!([tx_hash])).await
    }

    async fn call(&self, method: &str, params: Value) -> LedgerResult<Value> {
        let response = self
            .client
            .post(&self.rpc_url)
            .json(&json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": method,
                "params": params,
            }))
            .send()
            .await
            .map_err(|err| LedgerError::TransientIngestion(err.to_string()))?;
        let status = response.status();
        let body_bytes = response
            .bytes()
            .await
            .map_err(|err| LedgerError::TransientIngestion(err.to_string()))?;
        let body = String::from_utf8_lossy(&body_bytes).into_owned();
        if !status.is_success() {
            return Err(LedgerError::TransientIngestion(format!(
                "rpc http status {status}: {}",
                truncate_for_log(&body)
            )));
        }
        let payload = serde_json::from_str::<Value>(&body).map_err(|err| {
            LedgerError::TransientIngestion(format!(
                "rpc non-json response ({err}): {}",
                truncate_for_log(&body)
            ))
        })?;
        if payload.get("error").is_some() {
            return Err(LedgerError::TransientIngestion(
                payload["error"].to_string(),
            ));
        }
        Ok(payload["result"].clone())
    }
}

fn parse_hex_u64(value: Option<&str>) -> LedgerResult<u64> {
    let value = value
        .ok_or_else(|| LedgerError::MalformedEvidence("missing latest block hex".to_owned()))?;
    u64::from_str_radix(value.trim_start_matches("0x"), 16)
        .map_err(|err| LedgerError::MalformedEvidence(err.to_string()))
}

fn truncate_for_log(value: &str) -> String {
    let value = value.replace('\n', " ").replace('\r', " ");
    if value.len() > 240 {
        format!("{}...", &value[..240])
    } else {
        value
    }
}
