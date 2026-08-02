use std::{env, sync::Arc};

use serde_json::Value;
use sqlx::{PgPool, Row};

use crate::core::errors::{LedgerError, LedgerResult};

#[derive(Clone, Debug)]
pub struct ChainRoute {
    pub chain_id: u64,
    pub slug: String,
    pub display_name: String,
    pub native_symbol: String,
    pub explorer_base_url: Option<String>,
    pub api_base_url: String,
    pub indexer_api_base_url: Option<String>,
    pub api_bearer_token: Option<String>,
    pub indexer_api_bearer_token: Option<String>,
    pub capabilities: Value,
    pub config_version: u64,
}

#[derive(Clone, Debug)]
pub struct ChainRegistry {
    routes: Arc<[ChainRoute]>,
    default_chain_id: u64,
}

impl ChainRegistry {
    pub async fn load(
        pool: &PgPool,
        requested_default_chain_id: u64,
        legacy_api_base_url: String,
        legacy_indexer_api_base_url: Option<String>,
        legacy_indexer_api_bearer_token: Option<String>,
    ) -> LedgerResult<Self> {
        let rows = sqlx::query(
            r#"
            select
              chain_id,
              slug,
              display_name,
              native_symbol,
              explorer_base_url,
              api_base_url,
              indexer_api_base_url,
              capabilities,
              config_version
            from chain_registry
            where enabled = true
            order by sort_order asc, chain_id asc
            "#,
        )
        .fetch_all(pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;

        let mut routes = rows
            .into_iter()
            .map(|row| {
                let chain_id = row
                    .try_get::<i64, _>("chain_id")
                    .map_err(|err| LedgerError::Persistence(err.to_string()))?
                    as u64;
                Ok(ChainRoute {
                    chain_id,
                    slug: row
                        .try_get("slug")
                        .map_err(|err| LedgerError::Persistence(err.to_string()))?,
                    display_name: row
                        .try_get("display_name")
                        .map_err(|err| LedgerError::Persistence(err.to_string()))?,
                    native_symbol: row
                        .try_get("native_symbol")
                        .map_err(|err| LedgerError::Persistence(err.to_string()))?,
                    explorer_base_url: row
                        .try_get("explorer_base_url")
                        .map_err(|err| LedgerError::Persistence(err.to_string()))?,
                    api_base_url: row
                        .try_get("api_base_url")
                        .map_err(|err| LedgerError::Persistence(err.to_string()))?,
                    indexer_api_base_url: row
                        .try_get("indexer_api_base_url")
                        .map_err(|err| LedgerError::Persistence(err.to_string()))?,
                    api_bearer_token: chain_secret("LEDGERFLOW_CHAIN_API_BEARER_TOKEN", chain_id),
                    indexer_api_bearer_token: chain_secret(
                        "LEDGERFLOW_CHAIN_INDEXER_BEARER_TOKEN",
                        chain_id,
                    ),
                    capabilities: row
                        .try_get::<Value, _>("capabilities")
                        .map_err(|err| LedgerError::Persistence(err.to_string()))?,
                    config_version: row
                        .try_get::<i64, _>("config_version")
                        .map_err(|err| LedgerError::Persistence(err.to_string()))?
                        as u64,
                })
            })
            .collect::<LedgerResult<Vec<_>>>()?;

        if routes.is_empty() {
            routes.push(ChainRoute {
                chain_id: requested_default_chain_id,
                slug: fallback_slug(requested_default_chain_id),
                display_name: fallback_display_name(requested_default_chain_id),
                native_symbol: "ETH".to_owned(),
                explorer_base_url: fallback_explorer(requested_default_chain_id),
                api_base_url: legacy_api_base_url,
                indexer_api_base_url: legacy_indexer_api_base_url,
                api_bearer_token: chain_secret(
                    "LEDGERFLOW_CHAIN_API_BEARER_TOKEN",
                    requested_default_chain_id,
                ),
                indexer_api_bearer_token: chain_secret(
                    "LEDGERFLOW_CHAIN_INDEXER_BEARER_TOKEN",
                    requested_default_chain_id,
                )
                .or(legacy_indexer_api_bearer_token),
                capabilities: serde_json::json!({
                    "address_index": true,
                    "cluster_analysis": true,
                    "asset_changes": true,
                    "arbitrage_monitor": true,
                    "trace_analysis": true
                }),
                config_version: 1,
            });
        }

        let default_chain_id = if routes
            .iter()
            .any(|route| route.chain_id == requested_default_chain_id)
        {
            requested_default_chain_id
        } else {
            routes[0].chain_id
        };

        Ok(Self {
            routes: routes.into(),
            default_chain_id,
        })
    }

    pub fn default_chain_id(&self) -> u64 {
        self.default_chain_id
    }

    pub fn routes(&self) -> &[ChainRoute] {
        &self.routes
    }

    pub fn resolve(&self, chain_id: u64) -> Option<&ChainRoute> {
        self.routes.iter().find(|route| route.chain_id == chain_id)
    }
}

fn chain_secret(prefix: &str, chain_id: u64) -> Option<String> {
    env::var(format!("{prefix}_{chain_id}"))
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn fallback_slug(chain_id: u64) -> String {
    match chain_id {
        1 => "ethereum".to_owned(),
        8453 => "base".to_owned(),
        4663 => "robinhood".to_owned(),
        46630 => "robinhood-testnet".to_owned(),
        42161 => "arbitrum".to_owned(),
        10 => "optimism".to_owned(),
        _ => format!("chain-{chain_id}"),
    }
}

fn fallback_display_name(chain_id: u64) -> String {
    match chain_id {
        1 => "Ethereum".to_owned(),
        8453 => "Base".to_owned(),
        4663 => "Robinhood Chain".to_owned(),
        46630 => "Robinhood Chain Testnet".to_owned(),
        42161 => "Arbitrum".to_owned(),
        10 => "Optimism".to_owned(),
        _ => format!("Chain {chain_id}"),
    }
}

fn fallback_explorer(chain_id: u64) -> Option<String> {
    match chain_id {
        1 => Some("https://etherscan.io".to_owned()),
        8453 => Some("https://basescan.org".to_owned()),
        4663 => Some("https://robinhoodchain.blockscout.com".to_owned()),
        46630 => Some("https://explorer.testnet.chain.robinhood.com".to_owned()),
        42161 => Some("https://arbiscan.io".to_owned()),
        10 => Some("https://optimistic.etherscan.io".to_owned()),
        _ => None,
    }
}
