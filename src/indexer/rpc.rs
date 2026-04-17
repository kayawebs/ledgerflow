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

fn parse_hex_u64(value: Option<&str>) -> LedgerResult<u64> {
    let value = value
        .ok_or_else(|| LedgerError::MalformedEvidence("missing latest block hex".to_owned()))?;
    u64::from_str_radix(value.trim_start_matches("0x"), 16)
        .map_err(|err| LedgerError::MalformedEvidence(err.to_string()))
}
