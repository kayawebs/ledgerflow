use async_trait::async_trait;
use reqwest::Client;
use serde::Deserialize;
use serde_json::Value;

use crate::{
    core::errors::{LedgerError, LedgerResult},
    domain::{enums::AddressHitDirection, value_objects::AddressTxRef},
};

/// External address-history providers are discovery aids only. Every returned
/// transaction is subsequently fetched from the configured chain RPC and is
/// subject to the normal receipt/log/trace verification rules.
#[async_trait]
pub trait AddressHistorySource: Send + Sync {
    fn source_name(&self) -> &'static str;

    async fn fetch_iteration(
        &self,
        chain_id: u64,
        address: &str,
        state: &AddressHistoryCursor,
    ) -> LedgerResult<AddressHistoryDiscovery>;
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AddressHistoryCursor {
    pub transaction_page: u64,
    pub token_transfer_page: u64,
    pub internal_transaction_page: u64,
    pub transaction_start_block: u64,
    pub token_transfer_start_block: u64,
    pub internal_transaction_start_block: u64,
    pub transaction_cursor: Option<Value>,
    pub token_transfer_cursor: Option<Value>,
    pub internal_transaction_cursor: Option<Value>,
    pub transactions_complete: bool,
    pub token_transfers_complete: bool,
    pub internal_transactions_complete: bool,
    pub earliest_activity_block: Option<u64>,
}

impl AddressHistoryCursor {
    pub fn normalized(mut self) -> Self {
        self.transaction_page = self.transaction_page.max(1);
        self.token_transfer_page = self.token_transfer_page.max(1);
        self.internal_transaction_page = self.internal_transaction_page.max(1);
        self
    }

    pub fn complete(&self) -> bool {
        self.transactions_complete
            && self.token_transfers_complete
            && self.internal_transactions_complete
    }
}

#[derive(Clone, Debug, Default)]
pub struct AddressHistoryDiscovery {
    pub refs: Vec<AddressTxRef>,
    pub cursor: AddressHistoryCursor,
    pub fetched_rows: usize,
}

#[derive(Clone)]
pub struct BlockscoutAddressHistorySource {
    base_url: String,
    client: Client,
    pages_per_iteration: usize,
    include_internal_transactions: bool,
}

impl BlockscoutAddressHistorySource {
    pub fn new(
        base_url: String,
        _page_size: usize,
        pages_per_iteration: usize,
        include_internal_transactions: bool,
    ) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_owned(),
            client: Client::new(),
            pages_per_iteration: pages_per_iteration.clamp(1, 100),
            include_internal_transactions,
        }
    }

    async fn fetch_lane(
        &self,
        chain_id: u64,
        address: &str,
        endpoint: &'static str,
        mut cursor: Option<Value>,
        already_complete: bool,
    ) -> LedgerResult<LaneDiscovery> {
        if already_complete {
            return Ok(LaneDiscovery {
                next_cursor: cursor,
                complete: true,
                ..LaneDiscovery::default()
            });
        }
        let mut result = LaneDiscovery::default();
        for _ in 0..self.pages_per_iteration {
            let mut request = self.client.get(format!(
                "{}/api/v2/addresses/{address}/{endpoint}",
                self.base_url
            ));
            if let Some(Value::Object(params)) = cursor.as_ref() {
                let query = params
                    .iter()
                    .filter_map(|(key, value)| query_value(value).map(|value| (key.clone(), value)))
                    .collect::<Vec<_>>();
                request = request.query(&query);
            }
            let response = request.send().await.map_err(|error| {
                LedgerError::TransientIngestion(format!(
                    "address history discovery request failed: {error}"
                ))
            })?;
            let status = response.status();
            let body = response.text().await.map_err(|error| {
                LedgerError::TransientIngestion(format!(
                    "address history discovery response failed: {error}"
                ))
            })?;
            if !status.is_success() {
                return Err(LedgerError::TransientIngestion(format!(
                    "address history provider returned HTTP {status}"
                )));
            }
            let payload: BlockscoutV2Envelope = serde_json::from_str(&body).map_err(|error| {
                LedgerError::TransientIngestion(format!(
                    "address history provider returned malformed JSON: {error}"
                ))
            })?;
            let rows = payload.items;
            let row_count = rows.len();
            for row in rows {
                if let Some(reference) = blockscout_ref(chain_id, address, &row) {
                    result.earliest_activity_block =
                        min_block(result.earliest_activity_block, Some(reference.block_number));
                    result.refs.push(reference);
                }
            }
            result.fetched_rows = result.fetched_rows.saturating_add(row_count);
            cursor = payload.next_page_params;
            if cursor.is_none() {
                result.complete = true;
                break;
            }
        }
        result.next_cursor = cursor;
        Ok(result)
    }
}

#[async_trait]
impl AddressHistorySource for BlockscoutAddressHistorySource {
    fn source_name(&self) -> &'static str {
        "blockscout_v2_address_history"
    }

    async fn fetch_iteration(
        &self,
        chain_id: u64,
        address: &str,
        state: &AddressHistoryCursor,
    ) -> LedgerResult<AddressHistoryDiscovery> {
        let state = state.clone().normalized();
        let transactions = self
            .fetch_lane(
                chain_id,
                address,
                "transactions",
                state.transaction_cursor.clone(),
                state.transactions_complete,
            )
            .await?;
        let token_transfers = self
            .fetch_lane(
                chain_id,
                address,
                "token-transfers",
                state.token_transfer_cursor.clone(),
                state.token_transfers_complete,
            )
            .await?;
        let internal_transactions = self
            .fetch_lane(
                chain_id,
                address,
                "internal-transactions",
                state.internal_transaction_cursor.clone(),
                state.internal_transactions_complete || !self.include_internal_transactions,
            )
            .await?;
        let mut refs = transactions.refs;
        refs.extend(token_transfers.refs);
        refs.extend(internal_transactions.refs);
        Ok(AddressHistoryDiscovery {
            refs,
            cursor: AddressHistoryCursor {
                transaction_page: state.transaction_page,
                token_transfer_page: state.token_transfer_page,
                internal_transaction_page: state.internal_transaction_page,
                transaction_start_block: state.transaction_start_block,
                token_transfer_start_block: state.token_transfer_start_block,
                internal_transaction_start_block: state.internal_transaction_start_block,
                transaction_cursor: transactions.next_cursor,
                token_transfer_cursor: token_transfers.next_cursor,
                internal_transaction_cursor: internal_transactions.next_cursor,
                transactions_complete: transactions.complete,
                token_transfers_complete: token_transfers.complete,
                internal_transactions_complete: internal_transactions.complete,
                earliest_activity_block: min_block(
                    state.earliest_activity_block,
                    min_block(
                        transactions.earliest_activity_block,
                        min_block(
                            token_transfers.earliest_activity_block,
                            internal_transactions.earliest_activity_block,
                        ),
                    ),
                ),
            },
            fetched_rows: transactions
                .fetched_rows
                .saturating_add(token_transfers.fetched_rows)
                .saturating_add(internal_transactions.fetched_rows),
        })
    }
}

#[derive(Debug, Deserialize)]
struct BlockscoutV2Envelope {
    items: Vec<Value>,
    next_page_params: Option<Value>,
}

#[derive(Clone, Debug, Default)]
struct LaneDiscovery {
    refs: Vec<AddressTxRef>,
    next_cursor: Option<Value>,
    complete: bool,
    earliest_activity_block: Option<u64>,
    fetched_rows: usize,
}

fn blockscout_ref(_chain_id: u64, address: &str, row: &Value) -> Option<AddressTxRef> {
    let tx_hash = row
        .get("hash")
        .or_else(|| row.get("transactionHash"))
        .or_else(|| row.get("transaction_hash"))?
        .as_str()?
        .to_ascii_lowercase();
    if !valid_hash(&tx_hash) {
        return None;
    }
    let block_number = value_u64(row.get("blockNumber").or_else(|| row.get("block_number")))?;
    let tx_index = value_u64(
        row.get("transactionIndex")
            .or_else(|| row.get("transaction_index")),
    )
    .unwrap_or(0) as u32;
    let from = address_value(row.get("from"));
    let to = address_value(row.get("to"));
    let direction = if from.as_deref().is_some_and(|value| value == address) {
        AddressHitDirection::From
    } else {
        AddressHitDirection::To
    };
    Some(AddressTxRef {
        address: address.to_owned(),
        tx_hash,
        block_number,
        tx_index,
        direction,
        from_address: from,
        to_address: to,
    })
}

fn address_value(value: Option<&Value>) -> Option<String> {
    value
        .and_then(|value| {
            value
                .as_str()
                .or_else(|| value.get("hash").and_then(Value::as_str))
        })
        .map(str::to_ascii_lowercase)
        .filter(|value| value.len() == 42 && value.starts_with("0x"))
}

fn value_u64(value: Option<&Value>) -> Option<u64> {
    value.and_then(|value| {
        value
            .as_u64()
            .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
    })
}

fn valid_hash(value: &str) -> bool {
    value.len() == 66
        && value.starts_with("0x")
        && value[2..].chars().all(|value| value.is_ascii_hexdigit())
}

fn min_block(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn query_value(value: &Value) -> Option<String> {
    value
        .as_str()
        .map(str::to_owned)
        .or_else(|| value.as_i64().map(|value| value.to_string()))
        .or_else(|| value.as_u64().map(|value| value.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_provider_transaction_rows_as_discovery_evidence() {
        let row = serde_json::json!({
            "hash":"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "blockNumber":"123",
            "transactionIndex":"7",
            "from":"0x1111111111111111111111111111111111111111",
            "to":"0x2222222222222222222222222222222222222222"
        });
        let reference = blockscout_ref(8453, "0x1111111111111111111111111111111111111111", &row)
            .expect("valid reference");
        assert_eq!(reference.block_number, 123);
        assert_eq!(reference.tx_index, 7);
        assert_eq!(reference.direction, AddressHitDirection::From);
    }

    #[test]
    fn cursor_is_complete_only_when_every_activity_lane_is_complete() {
        assert!(
            !AddressHistoryCursor {
                transactions_complete: true,
                token_transfers_complete: true,
                ..AddressHistoryCursor::default()
            }
            .complete()
        );
        assert!(
            AddressHistoryCursor {
                transactions_complete: true,
                token_transfers_complete: true,
                internal_transactions_complete: true,
                ..AddressHistoryCursor::default()
            }
            .complete()
        );
    }
}
