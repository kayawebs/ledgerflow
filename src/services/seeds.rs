use serde_json::json;
use sqlx::{PgPool, types::Json};

use crate::core::errors::{LedgerError, LedgerResult};

#[derive(Clone, Debug, Default)]
pub struct SeedSummary {
    pub token_rows: u64,
    pub label_rows: u64,
    pub protocol_rows: u64,
}

struct TokenSeed {
    address: &'static str,
    symbol: &'static str,
    name: &'static str,
    decimals: i32,
    labels: &'static [&'static str],
}

struct ProtocolSeed {
    address: &'static str,
    family: &'static str,
    name: &'static str,
    role: &'static str,
    labels: &'static [&'static str],
}

const BASE_CHAIN_ID: u64 = 8453;

const BASE_TOKEN_SEEDS: &[TokenSeed] = &[
    TokenSeed {
        address: "0x4200000000000000000000000000000000000006",
        symbol: "WETH",
        name: "Wrapped Ether",
        decimals: 18,
        labels: &["token:weth", "canonical:weth"],
    },
    TokenSeed {
        address: "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",
        symbol: "USDC",
        name: "USD Coin",
        decimals: 6,
        labels: &["token:usdc", "stablecoin"],
    },
    TokenSeed {
        address: "0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca",
        symbol: "USDbC",
        name: "USD Base Coin",
        decimals: 6,
        labels: &["token:usdbc", "stablecoin"],
    },
    TokenSeed {
        address: "0xfde4c96c8593536e31f229ea8f37b2ada2699bb2",
        symbol: "USDT",
        name: "Tether USD",
        decimals: 6,
        labels: &["token:usdt", "stablecoin"],
    },
];

const BASE_PROTOCOL_SEEDS: &[ProtocolSeed] = &[ProtocolSeed {
    address: "0xbbbbbbbbbb9cc5e90e3b3af64bdaf62c37eeffcb",
    family: "lending",
    name: "morpho_blue",
    role: "protocol",
    labels: &["protocol:morpho_blue", "lending:protocol"],
}];

pub async fn seed_known_metadata(pool: &PgPool, chain_id: u64) -> LedgerResult<SeedSummary> {
    if chain_id != BASE_CHAIN_ID {
        return Ok(SeedSummary::default());
    }
    let mut summary = SeedSummary::default();
    for token in BASE_TOKEN_SEEDS {
        summary.token_rows += upsert_token(pool, chain_id, token).await?;
        for label in token.labels {
            summary.label_rows += upsert_label(
                pool,
                chain_id,
                token.address,
                label,
                json!({
                    "seed_kind": "erc20_token",
                    "symbol": token.symbol,
                    "name": token.name,
                }),
            )
            .await?;
        }
    }
    for protocol in BASE_PROTOCOL_SEEDS {
        summary.protocol_rows += upsert_protocol(pool, chain_id, protocol).await?;
        for label in protocol.labels {
            summary.label_rows += upsert_label(
                pool,
                chain_id,
                protocol.address,
                label,
                json!({
                    "seed_kind": "protocol_address",
                    "protocol_family": protocol.family,
                    "protocol_name": protocol.name,
                    "contract_role": protocol.role,
                }),
            )
            .await?;
        }
    }
    Ok(summary)
}

async fn upsert_token(pool: &PgPool, chain_id: u64, token: &TokenSeed) -> LedgerResult<u64> {
    let result = sqlx::query(
        r#"
        insert into erc20_token_metadata (
          chain_id, token_address, symbol, name, decimals, fetch_status, fetched_at, created_at, updated_at
        )
        values ($1,$2,$3,$4,$5,'ok',now(),now(),now())
        on conflict (chain_id, token_address) do update set
          symbol = coalesce(erc20_token_metadata.symbol, excluded.symbol),
          name = coalesce(erc20_token_metadata.name, excluded.name),
          decimals = coalesce(erc20_token_metadata.decimals, excluded.decimals),
          fetch_status = case
            when erc20_token_metadata.fetch_status = 'ok' then erc20_token_metadata.fetch_status
            else excluded.fetch_status
          end,
          updated_at = now()
        "#,
    )
    .bind(chain_id as i64)
    .bind(token.address)
    .bind(token.symbol)
    .bind(token.name)
    .bind(token.decimals)
    .execute(pool)
    .await
    .map_err(|err| LedgerError::Persistence(err.to_string()))?;
    Ok(result.rows_affected())
}

async fn upsert_label(
    pool: &PgPool,
    chain_id: u64,
    address: &str,
    label: &str,
    evidence: serde_json::Value,
) -> LedgerResult<u64> {
    let result = sqlx::query(
        r#"
        insert into address_labels (
          chain_id, address, label, source, confidence, evidence_json, created_at, updated_at
        )
        values ($1,$2,$3,'ledgerflow_seed',0.99,$4,now(),now())
        on conflict (chain_id, address, label, source) do update set
          confidence = greatest(address_labels.confidence, excluded.confidence),
          evidence_json = excluded.evidence_json,
          updated_at = now()
        "#,
    )
    .bind(chain_id as i64)
    .bind(address)
    .bind(label)
    .bind(Json(evidence))
    .execute(pool)
    .await
    .map_err(|err| LedgerError::Persistence(err.to_string()))?;
    Ok(result.rows_affected())
}

async fn upsert_protocol(
    pool: &PgPool,
    chain_id: u64,
    protocol: &ProtocolSeed,
) -> LedgerResult<u64> {
    let result = sqlx::query(
        r#"
        insert into address_protocol_metadata (
          chain_id, address, protocol_family, protocol_name, contract_role, source,
          adapter_version, confidence, metadata, first_seen_at, last_seen_at, updated_at
        )
        values ($1,$2,$3,$4,$5,'ledgerflow_seed','1.0.0',0.99,$6,now(),now(),now())
        on conflict (chain_id, address, protocol_family, protocol_name, contract_role) do update set
          source = excluded.source,
          adapter_version = excluded.adapter_version,
          confidence = greatest(address_protocol_metadata.confidence, excluded.confidence),
          metadata = excluded.metadata,
          last_seen_at = now(),
          updated_at = now()
        "#,
    )
    .bind(chain_id as i64)
    .bind(protocol.address)
    .bind(protocol.family)
    .bind(protocol.name)
    .bind(protocol.role)
    .bind(Json(json!({
        "seed_kind": "protocol_address",
        "protocol_family": protocol.family,
        "protocol_name": protocol.name,
        "contract_role": protocol.role,
    })))
    .execute(pool)
    .await
    .map_err(|err| LedgerError::Persistence(err.to_string()))?;
    Ok(result.rows_affected())
}
