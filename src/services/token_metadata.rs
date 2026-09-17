use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use serde_json::{Value, json};
use sqlx::{PgPool, Row};
use tracing::warn;

use crate::{
    core::errors::{LedgerError, LedgerResult},
    domain::{enums::AssetType, value_objects::AssetFlow},
    ingestion::rpc::RpcClient,
};

pub(crate) const ERC20_SYMBOL_SELECTOR: &str = "0x95d89b41";
pub(crate) const ERC20_NAME_SELECTOR: &str = "0x06fdde03";
pub(crate) const ERC20_DECIMALS_SELECTOR: &str = "0x313ce567";

#[derive(Clone, Debug)]
pub struct Erc20TokenMetadata {
    pub chain_id: u64,
    pub token_address: String,
    pub symbol: Option<String>,
    pub name: Option<String>,
    pub decimals: Option<u8>,
    pub fetch_status: String,
}

#[derive(Clone)]
pub struct TokenMetadataService {
    pool: PgPool,
    rpc_client: Arc<dyn RpcClient>,
}

impl TokenMetadataService {
    pub fn new(pool: PgPool, rpc_client: Arc<dyn RpcClient>) -> Self {
        Self { pool, rpc_client }
    }

    pub async fn enrich_flows(
        &self,
        chain_id: u64,
        block_number: Option<u64>,
        flows: &mut [AssetFlow],
    ) -> LedgerResult<()> {
        let token_addresses = flows
            .iter()
            .filter(|flow| flow.asset_type == AssetType::Erc20)
            .filter_map(|flow| flow.token_address.as_deref())
            .map(|address| address.to_ascii_lowercase())
            .collect::<BTreeSet<_>>();
        if token_addresses.is_empty() {
            return Ok(());
        }

        let mut metadata_by_address = BTreeMap::new();
        for address in token_addresses {
            match self.get_or_fetch(chain_id, &address, block_number).await {
                Ok(metadata) => {
                    metadata_by_address.insert(address, metadata);
                }
                Err(err) => {
                    warn!(token_address = address, error = %err, "token metadata fetch failed");
                }
            }
        }

        for flow in flows {
            let Some(address) = flow.token_address.as_deref().map(str::to_ascii_lowercase) else {
                continue;
            };
            let Some(metadata) = metadata_by_address.get(&address) else {
                continue;
            };
            let token_json = metadata.to_json();
            match flow.metadata.as_object_mut() {
                Some(object) => {
                    object.insert("token".to_owned(), token_json);
                }
                None => {
                    flow.metadata = json!({ "token": token_json });
                }
            }
        }
        Ok(())
    }

    pub async fn get_or_fetch(
        &self,
        chain_id: u64,
        token_address: &str,
        block_number: Option<u64>,
    ) -> LedgerResult<Erc20TokenMetadata> {
        if let Some(metadata) = self.load(chain_id, token_address).await? {
            if metadata.fetch_status == "ok" {
                return Ok(metadata);
            }
        }
        let fetched = self
            .fetch_from_chain(chain_id, token_address, block_number)
            .await;
        let metadata = match fetched {
            Ok(metadata) => metadata,
            Err(err) => Erc20TokenMetadata {
                chain_id,
                token_address: token_address.to_ascii_lowercase(),
                symbol: None,
                name: None,
                decimals: None,
                fetch_status: "error".to_owned(),
            }
            .with_error(err.to_string()),
        };
        self.upsert(&metadata).await?;
        Ok(metadata)
    }

    async fn load(
        &self,
        chain_id: u64,
        token_address: &str,
    ) -> LedgerResult<Option<Erc20TokenMetadata>> {
        let row = sqlx::query(
            r#"
            select chain_id, token_address, symbol, name, decimals, fetch_status
            from erc20_token_metadata
            where chain_id = $1 and token_address = $2
            "#,
        )
        .bind(chain_id as i64)
        .bind(token_address.to_ascii_lowercase())
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        row.map(row_to_metadata).transpose()
    }

    async fn fetch_from_chain(
        &self,
        chain_id: u64,
        token_address: &str,
        block_number: Option<u64>,
    ) -> LedgerResult<Erc20TokenMetadata> {
        let symbol_call =
            self.fetch_string(chain_id, token_address, ERC20_SYMBOL_SELECTOR, block_number);
        let name_call =
            self.fetch_string(chain_id, token_address, ERC20_NAME_SELECTOR, block_number);
        let decimals_call = self.rpc_client.eth_call(
            chain_id,
            token_address,
            ERC20_DECIMALS_SELECTOR,
            block_number,
        );
        let (symbol, name, decimals) = tokio::join!(symbol_call, name_call, decimals_call);
        let symbol = symbol.ok().flatten();
        let name = name.ok().flatten();
        let decimals = decimals
            .ok()
            .and_then(|value| parse_uint_hex(&value))
            .and_then(|value| u8::try_from(value).ok());
        if symbol.is_none() && name.is_none() && decimals.is_none() {
            return Err(LedgerError::MalformedEvidence(format!(
                "no ERC20 metadata returned for {token_address}"
            )));
        }
        Ok(Erc20TokenMetadata {
            chain_id,
            token_address: token_address.to_ascii_lowercase(),
            symbol,
            name,
            decimals,
            fetch_status: "ok".to_owned(),
        })
    }

    async fn fetch_string(
        &self,
        chain_id: u64,
        token_address: &str,
        selector: &str,
        block_number: Option<u64>,
    ) -> LedgerResult<Option<String>> {
        let raw = self
            .rpc_client
            .eth_call(chain_id, token_address, selector, block_number)
            .await?;
        Ok(decode_erc20_string(&raw))
    }

    async fn upsert(&self, metadata: &Erc20TokenMetadata) -> LedgerResult<()> {
        sqlx::query(
            r#"
            insert into erc20_token_metadata (
              chain_id, token_address, symbol, name, decimals, fetch_status, error_message, fetched_at, created_at, updated_at
            )
            values ($1,$2,$3,$4,$5,$6,$7,now(),now(),now())
            on conflict (chain_id, token_address) do update set
              symbol = excluded.symbol,
              name = excluded.name,
              decimals = excluded.decimals,
              fetch_status = excluded.fetch_status,
              error_message = excluded.error_message,
              fetched_at = excluded.fetched_at,
              updated_at = now()
            "#,
        )
        .bind(metadata.chain_id as i64)
        .bind(&metadata.token_address)
        .bind(&metadata.symbol)
        .bind(&metadata.name)
        .bind(metadata.decimals.map(i32::from))
        .bind(&metadata.fetch_status)
        .bind(metadata.error_message())
        .execute(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        Ok(())
    }
}

impl Erc20TokenMetadata {
    pub fn to_json(&self) -> Value {
        json!({
            "symbol": self.symbol.as_deref().unwrap_or("Unknown ERC20"),
            "name": self.name.as_deref().unwrap_or("Unlabeled token"),
            "address": self.token_address,
            "decimals": self.decimals,
            "fetch_status": self.fetch_status,
        })
    }

    fn with_error(self, _error: String) -> Self {
        self
    }

    fn error_message(&self) -> Option<&str> {
        if self.fetch_status == "error" {
            Some("metadata fetch failed")
        } else {
            None
        }
    }
}

fn row_to_metadata(row: sqlx::postgres::PgRow) -> LedgerResult<Erc20TokenMetadata> {
    Ok(Erc20TokenMetadata {
        chain_id: row
            .try_get::<i64, _>("chain_id")
            .map(|value| value as u64)
            .map_err(|err| LedgerError::Persistence(err.to_string()))?,
        token_address: row
            .try_get("token_address")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?,
        symbol: row
            .try_get("symbol")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?,
        name: row
            .try_get("name")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?,
        decimals: row
            .try_get::<Option<i32>, _>("decimals")
            .map(|value| value.and_then(|value| u8::try_from(value).ok()))
            .map_err(|err| LedgerError::Persistence(err.to_string()))?,
        fetch_status: row
            .try_get("fetch_status")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?,
    })
}

pub(crate) fn parse_uint_hex(raw: &str) -> Option<u128> {
    u128::from_str_radix(raw.trim_start_matches("0x"), 16).ok()
}

pub(crate) fn decode_erc20_string(raw: &str) -> Option<String> {
    let hex = raw.trim_start_matches("0x");
    if hex.is_empty() {
        return None;
    }
    if let Some(dynamic) = decode_abi_string(hex) {
        return Some(dynamic);
    }
    decode_bytes32_string(hex)
}

fn decode_abi_string(hex: &str) -> Option<String> {
    if hex.len() < 128 {
        return None;
    }
    let offset = usize::from_str_radix(hex.get(0..64)?, 16).ok()?;
    let length_start = offset.checked_mul(2)?;
    let length_end = length_start.checked_add(64)?;
    let length = usize::from_str_radix(hex.get(length_start..length_end)?, 16).ok()?;
    let data_start = length_end;
    let data_end = data_start.checked_add(length.checked_mul(2)?)?;
    decode_hex_utf8(hex.get(data_start..data_end)?)
}

fn decode_bytes32_string(hex: &str) -> Option<String> {
    let data = hex.get(0..64)?;
    decode_hex_utf8(data)
}

fn decode_hex_utf8(hex: &str) -> Option<String> {
    let mut bytes = Vec::new();
    let mut chars = hex.as_bytes().chunks_exact(2);
    for chunk in &mut chars {
        let pair = std::str::from_utf8(chunk).ok()?;
        let byte = u8::from_str_radix(pair, 16).ok()?;
        if byte != 0 {
            bytes.push(byte);
        }
    }
    String::from_utf8(bytes)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_dynamic_abi_string() {
        let raw = concat!(
            "0x",
            "0000000000000000000000000000000000000000000000000000000000000020",
            "0000000000000000000000000000000000000000000000000000000000000004",
            "5553444300000000000000000000000000000000000000000000000000000000"
        );
        assert_eq!(decode_erc20_string(raw), Some("USDC".to_owned()));
    }

    #[test]
    fn decodes_bytes32_string() {
        let raw = "0x5553444300000000000000000000000000000000000000000000000000000000";
        assert_eq!(decode_erc20_string(raw), Some("USDC".to_owned()));
    }
}
