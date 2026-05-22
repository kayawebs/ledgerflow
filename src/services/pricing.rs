use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::{PgPool, Row, types::Json};
use uuid::Uuid;

use crate::{
    core::errors::{LedgerError, LedgerResult},
    domain::{
        entities::TransactionAnalysis,
        enums::AssetType,
        value_objects::AssetFlow,
    },
};

const PRICE_BLOCK_BUCKET_SIZE: u64 = 150;
const DEFAULT_QUOTE_SYMBOL: &str = "USD";
const SOURCE_TX_EXECUTION: &str = "tx_execution";

#[derive(Clone)]
pub struct PriceService {
    pool: PgPool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct PriceObservation {
    pub chain_id: u64,
    pub token_address: String,
    pub quote_token: String,
    pub quote_symbol: String,
    pub block_number: Option<u64>,
    pub block_bucket: Option<u64>,
    pub price_decimal: String,
    pub source: String,
    pub confidence: f32,
    pub tx_hash: Option<String>,
    pub metadata: Value,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct PriceAnalysisSummary {
    pub observations_written: usize,
    pub requests_enqueued: usize,
    pub observations: Vec<PriceObservation>,
    pub queued_tokens: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct PriceQuote {
    pub token_address: String,
    pub quote_symbol: String,
    pub amount_raw: String,
    pub amount_decimal: String,
    pub value_decimal: String,
    pub price_decimal: String,
    pub confidence: f32,
    pub source: String,
    pub observed_block: Option<u64>,
    pub block_bucket: Option<u64>,
}

#[derive(Clone, Debug)]
struct FlowAmount {
    flow: AssetFlow,
    token_address: String,
    amount_decimal: f64,
    symbol: Option<String>,
    decimals: Option<u8>,
    stable: Option<StableToken>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct StableToken {
    address: &'static str,
    symbol: &'static str,
}

const STABLE_TOKENS: &[StableToken] = &[
    StableToken {
        address: "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",
        symbol: "USDC",
    },
    StableToken {
        address: "0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca",
        symbol: "USDbC",
    },
    StableToken {
        address: "0xfde4c96c8593536e31f229ea8f37b2ada2699bb2",
        symbol: "USDT",
    },
];

impl PriceService {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn observe_analysis(
        &self,
        analysis: &mut TransactionAnalysis,
    ) -> LedgerResult<PriceAnalysisSummary> {
        let observations = derive_tx_execution_observations(analysis);
        let mut observations_written = 0usize;
        for observation in &observations {
            self.insert_observation(analysis, observation).await?;
            self.upsert_cache(analysis, observation).await?;
            observations_written += 1;
        }

        let priced_tokens = observations
            .iter()
            .map(|observation| observation.token_address.clone())
            .collect::<BTreeSet<_>>();
        let requested_tokens = collect_price_request_tokens(analysis, &priced_tokens);
        let mut requests_enqueued = 0usize;
        for token_address in &requested_tokens {
            requests_enqueued += self.enqueue_request(analysis, token_address).await? as usize;
        }

        let summary = PriceAnalysisSummary {
            observations_written,
            requests_enqueued,
            observations,
            queued_tokens: requested_tokens.into_iter().collect(),
        };
        attach_pricing_summary(analysis, &summary);
        Ok(summary)
    }

    pub async fn quote_amount(
        &self,
        chain_id: u64,
        token_address: &str,
        amount_raw: &str,
        decimals: u8,
        block_number: Option<u64>,
    ) -> LedgerResult<Option<PriceQuote>> {
        let Some(block_bucket) = block_number.map(block_bucket) else {
            return Ok(None);
        };
        let token_address = token_address.to_ascii_lowercase();
        let row = sqlx::query(
            r#"
            select
              token_address,
              quote_symbol,
              price_decimal::text as price_decimal,
              confidence,
              source,
              observed_block,
              block_bucket
            from token_price_cache
            where chain_id = $1
              and token_address = $2
              and quote_symbol = $3
              and block_bucket between $4 - 2 and $4 + 2
              and (expires_at is null or expires_at > now())
            order by confidence desc, abs(block_bucket - $4) asc, updated_at desc
            limit 1
            "#,
        )
        .bind(chain_id as i64)
        .bind(&token_address)
        .bind(DEFAULT_QUOTE_SYMBOL)
        .bind(block_bucket as i64)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        let Some(row) = row else {
            return Ok(None);
        };
        let amount_decimal_f64 = parse_amount_decimal(amount_raw, decimals)?;
        let price_decimal = row
            .try_get::<String, _>("price_decimal")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        let price_f64 = price_decimal
            .parse::<f64>()
            .map_err(|err| LedgerError::MalformedEvidence(err.to_string()))?;
        Ok(Some(PriceQuote {
            token_address: row
                .try_get("token_address")
                .map_err(|err| LedgerError::Persistence(err.to_string()))?,
            quote_symbol: row
                .try_get("quote_symbol")
                .map_err(|err| LedgerError::Persistence(err.to_string()))?,
            amount_raw: amount_raw.to_owned(),
            amount_decimal: format_decimal(amount_decimal_f64),
            value_decimal: format_decimal(amount_decimal_f64 * price_f64),
            price_decimal,
            confidence: row
                .try_get::<f32, _>("confidence")
                .map_err(|err| LedgerError::Persistence(err.to_string()))?,
            source: row
                .try_get("source")
                .map_err(|err| LedgerError::Persistence(err.to_string()))?,
            observed_block: row
                .try_get::<Option<i64>, _>("observed_block")
                .map_err(|err| LedgerError::Persistence(err.to_string()))?
                .map(|value| value as u64),
            block_bucket: row
                .try_get::<i64, _>("block_bucket")
                .map_err(|err| LedgerError::Persistence(err.to_string()))?
                .try_into()
                .ok(),
        }))
    }

    async fn insert_observation(
        &self,
        analysis: &TransactionAnalysis,
        observation: &PriceObservation,
    ) -> LedgerResult<()> {
        sqlx::query(
            r#"
            insert into price_observations (
              observation_id, chain_id, token_address, quote_token, quote_symbol,
              block_number, block_timestamp, block_bucket, price_decimal, source,
              confidence, pool_address, tx_hash, metadata
            )
            values ($1,$2,$3,$4,$5,$6,$7,$8,$9::numeric,$10,$11,$12,$13,$14)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(observation.chain_id as i64)
        .bind(&observation.token_address)
        .bind(&observation.quote_token)
        .bind(&observation.quote_symbol)
        .bind(observation.block_number.map(|value| value as i64))
        .bind(analysis.block_timestamp)
        .bind(observation.block_bucket.map(|value| value as i64))
        .bind(&observation.price_decimal)
        .bind(&observation.source)
        .bind(observation.confidence as f64)
        .bind(None::<String>)
        .bind(&observation.tx_hash)
        .bind(Json(observation.metadata.clone()))
        .execute(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        Ok(())
    }

    async fn upsert_cache(
        &self,
        analysis: &TransactionAnalysis,
        observation: &PriceObservation,
    ) -> LedgerResult<()> {
        let Some(block_bucket) = observation.block_bucket else {
            return Ok(());
        };
        sqlx::query(
            r#"
            insert into token_price_cache (
              chain_id, token_address, quote_symbol, block_bucket, price_decimal,
              confidence, source, observed_block, observed_at, expires_at, metadata
            )
            values ($1,$2,$3,$4,$5::numeric,$6,$7,$8,$9,now() + interval '7 days',$10)
            on conflict (chain_id, token_address, quote_symbol, block_bucket) do update set
              price_decimal = case
                when excluded.confidence >= token_price_cache.confidence then excluded.price_decimal
                else token_price_cache.price_decimal
              end,
              confidence = greatest(token_price_cache.confidence, excluded.confidence),
              source = case
                when excluded.confidence >= token_price_cache.confidence then excluded.source
                else token_price_cache.source
              end,
              observed_block = case
                when excluded.confidence >= token_price_cache.confidence then excluded.observed_block
                else token_price_cache.observed_block
              end,
              observed_at = case
                when excluded.confidence >= token_price_cache.confidence then excluded.observed_at
                else token_price_cache.observed_at
              end,
              expires_at = greatest(token_price_cache.expires_at, excluded.expires_at),
              metadata = case
                when excluded.confidence >= token_price_cache.confidence then excluded.metadata
                else token_price_cache.metadata
              end,
              updated_at = now()
            "#,
        )
        .bind(observation.chain_id as i64)
        .bind(&observation.token_address)
        .bind(&observation.quote_symbol)
        .bind(block_bucket as i64)
        .bind(&observation.price_decimal)
        .bind(observation.confidence as f64)
        .bind(&observation.source)
        .bind(observation.block_number.map(|value| value as i64))
        .bind(analysis.block_timestamp)
        .bind(Json(observation.metadata.clone()))
        .execute(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        Ok(())
    }

    async fn enqueue_request(
        &self,
        analysis: &TransactionAnalysis,
        token_address: &str,
    ) -> LedgerResult<u64> {
        let block_bucket = analysis.block_number.map(block_bucket);
        let result = sqlx::query(
            r#"
            insert into price_requests (
              request_id, chain_id, token_address, quote_symbol, target_block,
              block_bucket, target_timestamp, priority, reason, metadata
            )
            values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
            on conflict do nothing
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(analysis.chain_id as i64)
        .bind(token_address)
        .bind(DEFAULT_QUOTE_SYMBOL)
        .bind(analysis.block_number.map(|value| value as i64))
        .bind(block_bucket.map(|value| value as i64))
        .bind(analysis.block_timestamp)
        .bind(30i32)
        .bind("tx_analysis_missing_price")
        .bind(Json(json!({
            "source": "tx_analysis",
            "tx_hash": analysis.tx_hash,
            "block_number": analysis.block_number,
        })))
        .execute(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        Ok(result.rows_affected())
    }
}

pub fn derive_tx_execution_observations(
    analysis: &TransactionAnalysis,
) -> Vec<PriceObservation> {
    let flows = analysis
        .derived_flows
        .iter()
        .filter_map(flow_amount)
        .collect::<Vec<_>>();
    let block_bucket = analysis.block_number.map(block_bucket);
    let mut observations = BTreeMap::<String, PriceObservation>::new();

    for stable in flows.iter().filter(|item| item.stable.is_some()) {
        let Some(stable_token) = stable.stable else {
            continue;
        };
        observations
            .entry(stable.token_address.clone())
            .or_insert_with(|| PriceObservation {
                chain_id: analysis.chain_id,
                token_address: stable.token_address.clone(),
                quote_token: stable.token_address.clone(),
                quote_symbol: DEFAULT_QUOTE_SYMBOL.to_owned(),
                block_number: analysis.block_number,
                block_bucket,
                price_decimal: "1".to_owned(),
                source: SOURCE_TX_EXECUTION.to_owned(),
                confidence: 0.99,
                tx_hash: Some(analysis.tx_hash.clone()),
                metadata: json!({
                    "quote_stable": stable_token.symbol,
                    "method": "stable_token_anchor",
                    "flow_id": stable.flow.flow_id,
                }),
            });

        for token in flows
            .iter()
            .filter(|item| item.stable.is_none() && item.amount_decimal > 0.0)
        {
            let Some(confidence) = pair_confidence(stable, token) else {
                continue;
            };
            if stable.amount_decimal <= 0.0 {
                continue;
            }
            let price = stable.amount_decimal / token.amount_decimal;
            if !price.is_finite() || price <= 0.0 {
                continue;
            }
            observations
                .entry(token.token_address.clone())
                .or_insert_with(|| PriceObservation {
                    chain_id: analysis.chain_id,
                    token_address: token.token_address.clone(),
                    quote_token: stable.token_address.clone(),
                    quote_symbol: DEFAULT_QUOTE_SYMBOL.to_owned(),
                    block_number: analysis.block_number,
                    block_bucket,
                    price_decimal: format_decimal(price),
                    source: SOURCE_TX_EXECUTION.to_owned(),
                    confidence,
                    tx_hash: Some(analysis.tx_hash.clone()),
                    metadata: json!({
                        "method": "paired_erc20_transfer",
                        "quote_stable": stable_token.symbol,
                        "quote_flow_id": stable.flow.flow_id,
                        "token_flow_id": token.flow.flow_id,
                        "token_symbol": token.symbol,
                        "token_decimals": token.decimals,
                        "quote_amount_decimal": stable.amount_decimal,
                        "token_amount_decimal": token.amount_decimal,
                    }),
                });
        }
    }

    observations.into_values().collect()
}

fn collect_price_request_tokens(
    analysis: &TransactionAnalysis,
    priced_tokens: &BTreeSet<String>,
) -> BTreeSet<String> {
    analysis
        .derived_flows
        .iter()
        .filter(|flow| flow.asset_type == AssetType::Erc20)
        .filter_map(|flow| flow.token_address.as_deref())
        .map(str::to_ascii_lowercase)
        .filter(|address| !priced_tokens.contains(address))
        .collect()
}

fn attach_pricing_summary(analysis: &mut TransactionAnalysis, summary: &PriceAnalysisSummary) {
    let summary_json = serde_json::to_value(summary).unwrap_or_else(|_| json!({}));
    match analysis.scenario_outputs.as_object_mut() {
        Some(object) => {
            object.insert("pricing".to_owned(), summary_json);
        }
        None => {
            analysis.scenario_outputs = json!({ "pricing": summary_json });
        }
    }
}

fn flow_amount(flow: &AssetFlow) -> Option<FlowAmount> {
    if flow.asset_type != AssetType::Erc20 {
        return None;
    }
    let token_address = flow.token_address.as_deref()?.to_ascii_lowercase();
    let decimals = token_decimals(flow);
    let amount_decimal = parse_amount_decimal(&flow.amount_raw, decimals).ok()?;
    if !amount_decimal.is_finite() || amount_decimal <= 0.0 {
        return None;
    }
    Some(FlowAmount {
        flow: flow.clone(),
        stable: stable_token(&token_address),
        token_address,
        amount_decimal,
        symbol: token_symbol(flow),
        decimals: Some(decimals),
    })
}

fn token_decimals(flow: &AssetFlow) -> u8 {
    flow.metadata
        .pointer("/token/decimals")
        .and_then(Value::as_u64)
        .and_then(|value| u8::try_from(value).ok())
        .unwrap_or(18)
}

fn token_symbol(flow: &AssetFlow) -> Option<String> {
    flow.metadata
        .pointer("/token/symbol")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

fn stable_token(address: &str) -> Option<StableToken> {
    STABLE_TOKENS
        .iter()
        .copied()
        .find(|token| token.address == address)
}

fn pair_confidence(stable: &FlowAmount, token: &FlowAmount) -> Option<f32> {
    let stable_from = stable.flow.from_address.to_ascii_lowercase();
    let stable_to = stable.flow.to_address.to_ascii_lowercase();
    let token_from = token.flow.from_address.to_ascii_lowercase();
    let token_to = token.flow.to_address.to_ascii_lowercase();
    if stable_from == token_to && stable_to == token_from {
        Some(0.95)
    } else if stable_from == token_from
        || stable_from == token_to
        || stable_to == token_from
        || stable_to == token_to
    {
        Some(0.65)
    } else {
        None
    }
}

fn block_bucket(block_number: u64) -> u64 {
    block_number / PRICE_BLOCK_BUCKET_SIZE
}

fn parse_amount_decimal(amount_raw: &str, decimals: u8) -> LedgerResult<f64> {
    let raw = amount_raw
        .parse::<f64>()
        .map_err(|err| LedgerError::MalformedEvidence(err.to_string()))?;
    Ok(raw / 10f64.powi(i32::from(decimals)))
}

fn format_decimal(value: f64) -> String {
    let formatted = format!("{value:.18}");
    formatted
        .trim_end_matches('0')
        .trim_end_matches('.')
        .to_owned()
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::domain::{enums::AssetType, value_objects::EvidenceRef};

    use super::*;

    #[test]
    fn derives_token_price_from_stable_swap_flows() {
        let analysis = test_analysis(vec![
            flow(
                "stable",
                "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",
                "0xuser",
                "0xpool",
                "2500000",
                6,
                "USDC",
            ),
            flow(
                "token",
                "0xtoken000000000000000000000000000000000001",
                "0xpool",
                "0xuser",
                "1000000000000000000",
                18,
                "ABC",
            ),
        ]);

        let observations = derive_tx_execution_observations(&analysis);
        let token_price = observations
            .iter()
            .find(|item| item.token_address == "0xtoken000000000000000000000000000000000001")
            .expect("token price should be observed");
        assert_eq!(token_price.price_decimal, "2.5");
        assert_eq!(token_price.confidence, 0.95);
    }

    #[test]
    fn does_not_guess_price_without_stable_leg() {
        let analysis = test_analysis(vec![
            flow(
                "weth",
                "0x4200000000000000000000000000000000000006",
                "0xuser",
                "0xpool",
                "1000000000000000000",
                18,
                "WETH",
            ),
            flow(
                "token",
                "0xtoken000000000000000000000000000000000001",
                "0xpool",
                "0xuser",
                "1000000000000000000",
                18,
                "ABC",
            ),
        ]);

        assert!(derive_tx_execution_observations(&analysis).is_empty());
    }

    fn test_analysis(flows: Vec<AssetFlow>) -> TransactionAnalysis {
        TransactionAnalysis {
            analysis_id: Uuid::new_v4(),
            chain_id: 8453,
            tx_hash: "0xtx".to_owned(),
            block_number: Some(1000),
            block_timestamp: None,
            from_address: "0xuser".to_owned(),
            to_address: Some("0xrouter".to_owned()),
            status: crate::domain::enums::TransactionStatus::Success,
            value: "0x0".to_owned(),
            input_data: "0x".to_owned(),
            code_identity_summary: None,
            raw_evidence_refs: Vec::new(),
            normalized_execution_facts: crate::domain::entities::NormalizedExecutionFacts {
                selector: None,
                call_count: 0,
                internal_call_count: 0,
                event_count: 0,
                call_tree: json!({}),
                participants: Vec::new(),
                log_summaries: Vec::new(),
                derived_facts: json!({}),
                evidence_refs: Vec::new(),
            },
            derived_flows: flows,
            recognizer_results: Vec::new(),
            scenario_outputs: json!({}),
            accounting_outputs: json!({}),
            analysis_status: crate::domain::enums::AnalysisStatus::Completed,
            analysis_version_metadata: crate::core::versioning::VersionBundle::default(),
            unresolved_items: Vec::new(),
            validation_results: Vec::new(),
            input_digest: String::new(),
            attempt_id: None,
        }
    }

    fn flow(
        id: &str,
        token: &str,
        from: &str,
        to: &str,
        amount_raw: &str,
        decimals: u8,
        symbol: &str,
    ) -> AssetFlow {
        AssetFlow {
            flow_id: id.to_owned(),
            asset_type: AssetType::Erc20,
            token_address: Some(token.to_ascii_lowercase()),
            from_address: from.to_owned(),
            to_address: to.to_owned(),
            amount_raw: amount_raw.to_owned(),
            direction_hint: None,
            evidence_refs: Vec::<EvidenceRef>::new(),
            derivation_method: "test".to_owned(),
            confidence: 1.0,
            metadata: json!({
                "token": {
                    "symbol": symbol,
                    "decimals": decimals,
                }
            }),
        }
    }
}
