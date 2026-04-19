use serde_json::{Value, json};

use crate::core::errors::{LedgerError, LedgerResult};

#[derive(Clone)]
pub struct IndexerRpcClient {
    url: String,
    client: reqwest::Client,
}

impl IndexerRpcClient {
    pub fn new(url: String) -> Self {
        Self {
            url,
            client: reqwest::Client::new(),
        }
    }

    pub async fn get_latest_block_number(&self) -> LedgerResult<u64> {
        let value = self.call("eth_blockNumber", json!([])).await?;
        parse_hex_u64(
            value
                .as_str()
                .ok_or_else(|| LedgerError::MalformedEvidence("invalid block number".to_owned()))?,
        )
    }

    pub async fn get_block_with_transactions(&self, block_number: u64) -> LedgerResult<Value> {
        self.call(
            "eth_getBlockByNumber",
            json!([format!("0x{block_number:x}"), true]),
        )
        .await
    }

    pub async fn get_transaction_by_hash(&self, tx_hash: &str) -> LedgerResult<Value> {
        self.call("eth_getTransactionByHash", json!([tx_hash]))
            .await
    }

    pub async fn get_transaction_receipt(&self, tx_hash: &str) -> LedgerResult<Value> {
        self.call("eth_getTransactionReceipt", json!([tx_hash]))
            .await
    }

    async fn call(&self, method: &str, params: Value) -> LedgerResult<Value> {
        let response = self
            .client
            .post(&self.url)
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
        let bytes = response
            .bytes()
            .await
            .map_err(|err| LedgerError::TransientIngestion(err.to_string()))?;
        if !status.is_success() {
            let body = String::from_utf8_lossy(&bytes).replace('\n', " ");
            return Err(LedgerError::TransientIngestion(format!(
                "rpc http status {status}: {body}"
            )));
        }

        let payload: Value = serde_json::from_slice(&bytes)
            .map_err(|err| LedgerError::TransientIngestion(err.to_string()))?;
        if let Some(error) = payload.get("error") {
            return Err(LedgerError::TransientIngestion(error.to_string()));
        }
        payload
            .get("result")
            .cloned()
            .ok_or_else(|| LedgerError::MalformedEvidence("rpc response missing result".to_owned()))
    }
}

fn parse_hex_u64(value: &str) -> LedgerResult<u64> {
    u64::from_str_radix(value.trim_start_matches("0x"), 16)
        .map_err(|err| LedgerError::MalformedEvidence(err.to_string()))
}
