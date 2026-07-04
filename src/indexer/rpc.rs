use std::collections::BTreeMap;

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

    pub async fn get_blocks_with_transactions(
        &self,
        block_numbers: &[u64],
    ) -> LedgerResult<Vec<(u64, Value)>> {
        if block_numbers.is_empty() {
            return Ok(Vec::new());
        }
        if block_numbers.len() == 1 {
            let block_number = block_numbers[0];
            return self
                .get_block_with_transactions(block_number)
                .await
                .map(|payload| vec![(block_number, payload)]);
        }

        let requests = block_numbers
            .iter()
            .map(|block_number| {
                json!({
                    "jsonrpc": "2.0",
                    "id": block_number,
                    "method": "eth_getBlockByNumber",
                    "params": [format!("0x{block_number:x}"), true],
                })
            })
            .collect::<Vec<_>>();
        let responses = self.call_batch(requests).await?;
        let mut by_block = BTreeMap::new();
        for response in responses {
            let block_number = response.get("id").and_then(Value::as_u64).ok_or_else(|| {
                LedgerError::TransientIngestion("rpc batch response missing numeric id".to_owned())
            })?;
            if let Some(error) = response.get("error") {
                return Err(LedgerError::TransientIngestion(error.to_string()));
            }
            by_block.insert(
                block_number,
                response.get("result").cloned().unwrap_or(Value::Null),
            );
        }

        block_numbers
            .iter()
            .map(|block_number| {
                by_block
                    .remove(block_number)
                    .map(|payload| (*block_number, payload))
                    .ok_or_else(|| {
                        LedgerError::TransientIngestion(format!(
                            "rpc batch response missing block {block_number}"
                        ))
                    })
            })
            .collect()
    }

    pub async fn get_transaction_by_hash(&self, tx_hash: &str) -> LedgerResult<Value> {
        self.call("eth_getTransactionByHash", json!([tx_hash]))
            .await
    }

    async fn call(&self, method: &str, params: Value) -> LedgerResult<Value> {
        let payload = self
            .send_json(json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": method,
                "params": params,
            }))
            .await?;
        if payload.get("error").is_some() {
            return Err(LedgerError::TransientIngestion(
                payload["error"].to_string(),
            ));
        }
        Ok(payload["result"].clone())
    }

    async fn call_batch(&self, requests: Vec<Value>) -> LedgerResult<Vec<Value>> {
        let payload = self.send_json(Value::Array(requests)).await?;
        match payload {
            Value::Array(items) => Ok(items),
            other => Err(LedgerError::TransientIngestion(format!(
                "rpc batch non-array response: {}",
                truncate_for_log(&other.to_string())
            ))),
        }
    }

    async fn send_json(&self, payload: Value) -> LedgerResult<Value> {
        let response = self
            .client
            .post(&self.rpc_url)
            .json(&payload)
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
        Ok(payload)
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
