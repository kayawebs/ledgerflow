use std::{collections::BTreeMap, time::Duration};

use reqwest::Client;
use serde_json::{Value, json};
use tokio::time::sleep;

use crate::core::errors::{LedgerError, LedgerResult};

#[derive(Clone)]
pub struct IndexerRpcClient {
    rpc_url: String,
    client: Client,
}

impl IndexerRpcClient {
    pub fn new(rpc_url: String) -> Self {
        Self::with_timeout(rpc_url, Duration::from_secs(30))
    }

    pub fn with_timeout(rpc_url: String, request_timeout: Duration) -> Self {
        let client = Client::builder()
            .timeout(request_timeout)
            .build()
            .unwrap_or_else(|_| Client::new());
        Self { rpc_url, client }
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

    pub async fn get_block_header(&self, block_number: u64) -> LedgerResult<Value> {
        self.call(
            "eth_getBlockByNumber",
            json!([format!("0x{block_number:x}"), false]),
        )
        .await
    }

    pub async fn get_block_headers(
        &self,
        block_numbers: &[u64],
    ) -> LedgerResult<Vec<(u64, Value)>> {
        if block_numbers.is_empty() {
            return Ok(Vec::new());
        }
        if block_numbers.len() == 1 {
            let block_number = block_numbers[0];
            return self
                .get_block_header(block_number)
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
                    "params": [format!("0x{block_number:x}"), false],
                })
            })
            .collect::<Vec<_>>();
        let responses = self.call_batch(requests).await?;
        batch_results_by_numeric_id(block_numbers, responses, "block header")
    }

    pub async fn get_logs(&self, filter: Value) -> LedgerResult<Vec<Value>> {
        let result = self.call("eth_getLogs", json!([filter])).await?;
        result.as_array().cloned().ok_or_else(|| {
            LedgerError::MalformedEvidence("eth_getLogs result is not an array".to_owned())
        })
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

    pub async fn get_blocks_with_transactions_and_receipts(
        &self,
        block_numbers: &[u64],
    ) -> LedgerResult<Vec<(u64, Value, Value)>> {
        let mut results = Vec::with_capacity(block_numbers.len());
        for block_number in block_numbers {
            let (block, receipts) = tokio::try_join!(
                self.get_block_with_transactions(*block_number),
                self.get_block_receipts(*block_number),
            )?;
            results.push((*block_number, block, receipts));
        }
        Ok(results)
    }

    pub async fn get_blocks_with_transactions_and_transaction_receipts(
        &self,
        block_numbers: &[u64],
        request_delay_ms: u64,
    ) -> LedgerResult<Vec<(u64, Value, Value)>> {
        let mut results = Vec::with_capacity(block_numbers.len());
        for block_number in block_numbers {
            let mut block = self.get_block_header(*block_number).await?;
            let transaction_hashes = block
                .get("transactions")
                .and_then(Value::as_array)
                .ok_or_else(|| {
                    LedgerError::MalformedEvidence(format!(
                        "block {block_number} payload missing transactions array"
                    ))
                })?
                .iter()
                .map(|transaction_hash| {
                    transaction_hash.as_str().map(str::to_owned).ok_or_else(|| {
                        LedgerError::MalformedEvidence(format!(
                            "block {block_number} header transaction is not a hash"
                        ))
                    })
                })
                .collect::<LedgerResult<Vec<_>>>()?;
            let mut transactions = Vec::with_capacity(transaction_hashes.len());
            let mut receipts = Vec::with_capacity(transaction_hashes.len());
            for tx_hash in transaction_hashes {
                if request_delay_ms > 0 {
                    sleep(Duration::from_millis(request_delay_ms)).await;
                }
                let (transaction, receipt) = tokio::try_join!(
                    self.get_transaction_by_hash(&tx_hash),
                    self.get_transaction_receipt(&tx_hash),
                )?;
                if transaction.is_null() {
                    return Err(LedgerError::TransientIngestion(format!(
                        "transaction {tx_hash} returned null payload"
                    )));
                }
                if receipt.is_null() {
                    return Err(LedgerError::TransientIngestion(format!(
                        "transaction {tx_hash} returned null receipt"
                    )));
                }
                transactions.push(transaction);
                receipts.push(receipt);
            }
            block["transactions"] = Value::Array(transactions);
            results.push((*block_number, block, Value::Array(receipts)));
        }
        Ok(results)
    }

    pub async fn get_block_receipts(&self, block_number: u64) -> LedgerResult<Value> {
        self.call(
            "eth_getBlockReceipts",
            json!([format!("0x{block_number:x}")]),
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

    /// Fetches receipts for many transactions in one JSON-RPC batch. Missing
    /// receipts are returned as `Value::Null` in input order.
    pub async fn get_transaction_receipts(&self, tx_hashes: &[String]) -> LedgerResult<Vec<Value>> {
        if tx_hashes.is_empty() {
            return Ok(Vec::new());
        }
        let requests = tx_hashes
            .iter()
            .enumerate()
            .map(|(index, hash)| {
                json!({
                    "jsonrpc": "2.0",
                    "id": index,
                    "method": "eth_getTransactionReceipt",
                    "params": [hash],
                })
            })
            .collect::<Vec<_>>();
        let responses = self.call_batch(requests).await?;
        let ids = (0..tx_hashes.len() as u64).collect::<Vec<_>>();
        Ok(
            batch_results_by_numeric_id(&ids, responses, "transaction receipt")?
                .into_iter()
                .map(|(_, receipt)| receipt)
                .collect(),
        )
    }

    /// Senders of many transactions in one JSON-RPC batch, in input order.
    /// An entry is `None` when the node does not know the transaction.
    pub async fn get_transaction_senders(
        &self,
        tx_hashes: &[String],
    ) -> LedgerResult<Vec<Option<String>>> {
        if tx_hashes.is_empty() {
            return Ok(Vec::new());
        }
        let requests = tx_hashes
            .iter()
            .enumerate()
            .map(|(index, hash)| {
                json!({
                    "jsonrpc": "2.0",
                    "id": index,
                    "method": "eth_getTransactionByHash",
                    "params": [hash],
                })
            })
            .collect::<Vec<_>>();
        let responses = self.call_batch(requests).await?;
        let ids = (0..tx_hashes.len() as u64).collect::<Vec<_>>();
        Ok(batch_results_by_numeric_id(&ids, responses, "transaction")?
            .into_iter()
            .map(|(_, transaction)| {
                transaction
                    .get("from")
                    .and_then(Value::as_str)
                    .map(|sender| sender.to_ascii_lowercase())
            })
            .collect())
    }

    /// Executes many read-only `eth_call`s in one JSON-RPC batch. Each entry is
    /// `Ok(hex)` or the per-call error message, in input order.
    pub async fn eth_call_batch(
        &self,
        calls: &[(String, String)],
    ) -> LedgerResult<Vec<Result<String, String>>> {
        if calls.is_empty() {
            return Ok(Vec::new());
        }
        let requests = calls
            .iter()
            .enumerate()
            .map(|(index, (to, data))| {
                json!({
                    "jsonrpc": "2.0",
                    "id": index,
                    "method": "eth_call",
                    "params": [{ "to": to, "data": data }, "latest"],
                })
            })
            .collect::<Vec<_>>();
        let responses = self.call_batch(requests).await?;
        let mut by_id = BTreeMap::new();
        for response in responses {
            let Some(id) = response.get("id").and_then(Value::as_u64) else {
                continue;
            };
            let outcome = match (
                response.get("result").and_then(Value::as_str),
                response.get("error"),
            ) {
                (Some(result), None) => Ok(result.to_owned()),
                (_, Some(error)) => Err(error.to_string()),
                _ => Err("missing eth_call result".to_owned()),
            };
            by_id.insert(id, outcome);
        }
        Ok((0..calls.len() as u64)
            .map(|id| {
                by_id
                    .remove(&id)
                    .unwrap_or_else(|| Err("missing batch response".to_owned()))
            })
            .collect())
    }

    /// Executes a read-only `eth_call` at `latest` and returns the raw hex result.
    pub async fn eth_call(&self, to: &str, data: &str) -> LedgerResult<String> {
        let result = self
            .call("eth_call", json!([{ "to": to, "data": data }, "latest"]))
            .await?;
        result.as_str().map(str::to_owned).ok_or_else(|| {
            LedgerError::MalformedEvidence("eth_call result is not a hex string".to_owned())
        })
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

fn batch_results_by_numeric_id(
    ids: &[u64],
    responses: Vec<Value>,
    label: &str,
) -> LedgerResult<Vec<(u64, Value)>> {
    let mut by_id = BTreeMap::new();
    for response in responses {
        let id = response.get("id").and_then(Value::as_u64).ok_or_else(|| {
            LedgerError::TransientIngestion("rpc batch response missing numeric id".to_owned())
        })?;
        if let Some(error) = response.get("error") {
            return Err(LedgerError::TransientIngestion(error.to_string()));
        }
        by_id.insert(id, response.get("result").cloned().unwrap_or(Value::Null));
    }
    ids.iter()
        .map(|id| {
            by_id.remove(id).map(|value| (*id, value)).ok_or_else(|| {
                LedgerError::TransientIngestion(format!("rpc batch response missing {label} {id}"))
            })
        })
        .collect()
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
