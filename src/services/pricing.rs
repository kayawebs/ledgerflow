use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::{PgPool, Row, types::Json};
use uuid::Uuid;

use crate::{
    core::errors::{LedgerError, LedgerResult},
    domain::{entities::TransactionAnalysis, enums::AssetType, value_objects::AssetFlow},
    ingestion::rpc::RpcClient,
};

const PRICE_BLOCK_BUCKET_SIZE: u64 = 150;
const DEFAULT_QUOTE_SYMBOL: &str = "USD";
const SOURCE_TX_EXECUTION: &str = "tx_execution";
const SOURCE_STABLE_ANCHOR: &str = "stable_anchor";
const SOURCE_COMPOSED_CACHE: &str = "composed_cache";
const SOURCE_DEX_POOL_SNAPSHOT: &str = "dex_pool_snapshot";
const GET_RESERVES_SELECTOR: &str = "0x0902f1ac";
const SLOT0_SELECTOR: &str = "0x3850c7bd";

#[derive(Clone)]
pub struct PriceService {
    pool: PgPool,
    rpc_client: Option<Arc<dyn RpcClient>>,
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
    pub flow_quotes: Vec<FlowPriceQuote>,
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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct FlowPriceQuote {
    pub flow_id: String,
    pub quote: PriceQuote,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct PriceWorkerResult {
    pub processed: bool,
    pub completed: bool,
    pub request_id: Option<Uuid>,
    pub token_address: Option<String>,
    pub method: Option<String>,
}

#[derive(Clone, Debug)]
struct PriceRequest {
    request_id: Uuid,
    chain_id: u64,
    token_address: String,
    quote_symbol: String,
    target_block: Option<u64>,
    block_bucket: Option<u64>,
    target_timestamp: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug)]
struct PoolPriceCandidate {
    chain_id: u64,
    token_address: String,
    quote_token: String,
    pool_address: String,
    protocol_name: String,
    token0: String,
    token1: String,
    token0_decimals: u8,
    token1_decimals: u8,
    slot0_supported: bool,
    get_reserves_supported: bool,
    confidence: f32,
}

impl PoolPriceCandidate {
    fn relative_price_from_token1_per_token0(&self, price_token1_per_token0: f64) -> Option<f64> {
        if !price_token1_per_token0.is_finite() || price_token1_per_token0 <= 0.0 {
            return None;
        }
        if self.token_address == self.token0 && self.quote_token == self.token1 {
            Some(price_token1_per_token0)
        } else if self.token_address == self.token1 && self.quote_token == self.token0 {
            Some(1.0 / price_token1_per_token0)
        } else {
            None
        }
    }

    fn pool_method(&self) -> &'static str {
        if self.slot0_supported {
            "slot0"
        } else if self.get_reserves_supported {
            "getReserves"
        } else {
            "unknown"
        }
    }
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
        Self {
            pool,
            rpc_client: None,
        }
    }

    pub fn with_rpc_client(mut self, rpc_client: Arc<dyn RpcClient>) -> Self {
        self.rpc_client = Some(rpc_client);
        self
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

        let usd_priced_tokens = observations
            .iter()
            .filter(|observation| observation.quote_symbol == DEFAULT_QUOTE_SYMBOL)
            .map(|observation| observation.token_address.clone())
            .collect::<BTreeSet<_>>();
        let requested_tokens = collect_price_request_tokens(analysis, &usd_priced_tokens);
        let mut requests_enqueued = 0usize;
        for token_address in &requested_tokens {
            requests_enqueued += self.enqueue_request(analysis, token_address).await? as usize;
        }

        let flow_quotes = self.attach_flow_quotes(analysis).await?;
        let summary = PriceAnalysisSummary {
            observations_written,
            requests_enqueued,
            observations,
            queued_tokens: requested_tokens.into_iter().collect(),
            flow_quotes,
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
        let token_address = token_address.to_ascii_lowercase();
        let amount_decimal_f64 = parse_amount_decimal(amount_raw, decimals)?;
        if let Some(stable) = stable_token(&token_address) {
            return Ok(Some(PriceQuote {
                token_address,
                quote_symbol: DEFAULT_QUOTE_SYMBOL.to_owned(),
                amount_raw: amount_raw.to_owned(),
                amount_decimal: format_decimal(amount_decimal_f64),
                value_decimal: format_decimal(amount_decimal_f64),
                price_decimal: "1".to_owned(),
                confidence: 1.0,
                source: format!("{SOURCE_STABLE_ANCHOR}:{}", stable.symbol),
                observed_block: block_number,
                block_bucket: block_number.map(block_bucket),
            }));
        }
        let Some(block_bucket) = block_number.map(block_bucket) else {
            return Ok(None);
        };
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
              and (
                metadata->>'historical_state' = 'true'
                or source = $5
                or source = $6
              )
            order by confidence desc, abs(block_bucket - $4) asc, updated_at desc
            limit 1
            "#,
        )
        .bind(chain_id as i64)
        .bind(&token_address)
        .bind(DEFAULT_QUOTE_SYMBOL)
        .bind(block_bucket as i64)
        .bind(SOURCE_TX_EXECUTION)
        .bind(SOURCE_STABLE_ANCHOR)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        let Some(row) = row else {
            return Ok(None);
        };
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

    pub async fn quote_current_amount(
        &self,
        chain_id: u64,
        token_address: &str,
        amount_raw: &str,
        decimals: u8,
    ) -> LedgerResult<Option<PriceQuote>> {
        let token_address = token_address.to_ascii_lowercase();
        let amount_decimal_f64 = parse_amount_decimal(amount_raw, decimals)?;
        if let Some(stable) = stable_token(&token_address) {
            return Ok(Some(PriceQuote {
                token_address,
                quote_symbol: DEFAULT_QUOTE_SYMBOL.to_owned(),
                amount_raw: amount_raw.to_owned(),
                amount_decimal: format_decimal(amount_decimal_f64),
                value_decimal: format_decimal(amount_decimal_f64),
                price_decimal: "1".to_owned(),
                confidence: 1.0,
                source: format!("{SOURCE_STABLE_ANCHOR}:{}", stable.symbol),
                observed_block: None,
                block_bucket: Some(0),
            }));
        }
        let mut row = self
            .load_current_usd_cache(chain_id, &token_address)
            .await?;
        if row.is_none() {
            let request = PriceRequest {
                request_id: Uuid::new_v4(),
                chain_id,
                token_address: token_address.clone(),
                quote_symbol: DEFAULT_QUOTE_SYMBOL.to_owned(),
                target_block: None,
                block_bucket: Some(0),
                target_timestamp: None,
            };
            if self.resolve_price_request(&request).await?.is_some() {
                row = self
                    .load_current_usd_cache(chain_id, &token_address)
                    .await?;
            }
        }
        let Some(row) = row else {
            self.enqueue_current_request(chain_id, &token_address)
                .await?;
            return Ok(None);
        };
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

    async fn load_current_usd_cache(
        &self,
        chain_id: u64,
        token_address: &str,
    ) -> LedgerResult<Option<sqlx::postgres::PgRow>> {
        sqlx::query(
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
              and block_bucket = 0
              and (expires_at is null or expires_at > now())
            order by confidence desc, updated_at desc
            limit 1
            "#,
        )
        .bind(chain_id as i64)
        .bind(token_address.to_ascii_lowercase())
        .bind(DEFAULT_QUOTE_SYMBOL)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))
    }

    async fn attach_flow_quotes(
        &self,
        analysis: &mut TransactionAnalysis,
    ) -> LedgerResult<Vec<FlowPriceQuote>> {
        let mut quotes = Vec::new();
        for index in 0..analysis.derived_flows.len() {
            let candidate = {
                let flow = &analysis.derived_flows[index];
                if flow.asset_type != AssetType::Erc20 {
                    None
                } else {
                    flow.token_address.as_ref().map(|token_address| {
                        (
                            flow.flow_id.clone(),
                            token_address.clone(),
                            flow.amount_raw.clone(),
                            token_decimals(flow),
                        )
                    })
                }
            };
            let Some((flow_id, token_address, amount_raw, decimals)) = candidate else {
                continue;
            };
            let Some(quote) = self
                .quote_amount(
                    analysis.chain_id,
                    &token_address,
                    &amount_raw,
                    decimals,
                    analysis.block_number,
                )
                .await?
            else {
                continue;
            };
            let quote_json = serde_json::to_value(&quote).unwrap_or_else(|_| json!({}));
            let flow = &mut analysis.derived_flows[index];
            if !flow.metadata.is_object() {
                flow.metadata = json!({});
            }
            if let Some(object) = flow.metadata.as_object_mut() {
                object.insert("estimated_usd".to_owned(), quote_json);
            }
            quotes.push(FlowPriceQuote { flow_id, quote });
        }
        Ok(quotes)
    }

    pub async fn process_next_price_request(&self) -> LedgerResult<PriceWorkerResult> {
        let Some(request) = self.claim_next_price_request().await? else {
            return Ok(PriceWorkerResult::default());
        };
        let request_id = request.request_id;
        let token_address = request.token_address.clone();
        match self.resolve_price_request(&request).await {
            Ok(Some(method)) => {
                self.complete_request(request_id, json!({ "method": method }))
                    .await?;
                Ok(PriceWorkerResult {
                    processed: true,
                    completed: true,
                    request_id: Some(request_id),
                    token_address: Some(token_address),
                    method: Some(method),
                })
            }
            Ok(None) => {
                self.release_request(request_id, "price source not available yet")
                    .await?;
                Ok(PriceWorkerResult {
                    processed: true,
                    completed: false,
                    request_id: Some(request_id),
                    token_address: Some(token_address),
                    method: None,
                })
            }
            Err(err) => {
                self.release_request(request_id, &err.to_string()).await?;
                Err(err)
            }
        }
    }

    async fn resolve_price_request(&self, request: &PriceRequest) -> LedgerResult<Option<String>> {
        if request.quote_symbol != DEFAULT_QUOTE_SYMBOL {
            return Ok(None);
        }
        if stable_token(&request.token_address).is_some() {
            let observation = PriceObservation {
                chain_id: request.chain_id,
                token_address: request.token_address.clone(),
                quote_token: request.token_address.clone(),
                quote_symbol: DEFAULT_QUOTE_SYMBOL.to_owned(),
                block_number: request.target_block,
                block_bucket: request.block_bucket,
                price_decimal: "1".to_owned(),
                source: SOURCE_STABLE_ANCHOR.to_owned(),
                confidence: 0.99,
                tx_hash: None,
                metadata: json!({
                    "method": "known_stable_anchor",
                    "request_id": request.request_id,
                    "state_block": request.target_block,
                    "historical_state": request.target_block.is_some(),
                    "target_timestamp": request.target_timestamp,
                }),
            };
            self.insert_observation_parts(&observation, request.target_timestamp)
                .await?;
            self.upsert_cache_parts(&observation, request.target_timestamp)
                .await?;
            return Ok(Some("stable_anchor".to_owned()));
        }
        if self.has_usd_cache(request).await? {
            return Ok(Some("existing_cache".to_owned()));
        }
        if self.refresh_from_known_dex_pool(request).await? {
            return Ok(Some("dex_pool_snapshot".to_owned()));
        }
        if self.compose_cached_price(request).await? {
            return Ok(Some("composed_cache".to_owned()));
        }
        Ok(None)
    }

    async fn refresh_from_known_dex_pool(&self, request: &PriceRequest) -> LedgerResult<bool> {
        let Some(rpc_client) = self.rpc_client.as_ref() else {
            return Ok(false);
        };
        let candidates = self.load_pool_candidates(request).await?;
        for candidate in candidates {
            let Some(relative_price) = self
                .fetch_pool_relative_price(rpc_client.as_ref(), &candidate, request.target_block)
                .await?
            else {
                continue;
            };
            let (quote_usd_price, quote_confidence, quote_source) =
                match stable_token(&candidate.quote_token) {
                    Some(stable) => (
                        1.0,
                        0.99,
                        format!("{SOURCE_STABLE_ANCHOR}:{}", stable.symbol),
                    ),
                    None => {
                        let Some(cache) = self
                            .usd_cache_for_request(
                                request.chain_id,
                                &candidate.quote_token,
                                request.block_bucket,
                                request.target_block.is_some(),
                            )
                            .await?
                        else {
                            continue;
                        };
                        cache
                    }
                };
            let price = relative_price * quote_usd_price;
            if !price.is_finite() || price <= 0.0 {
                continue;
            }
            let observation = PriceObservation {
                chain_id: request.chain_id,
                token_address: request.token_address.clone(),
                quote_token: candidate.quote_token.clone(),
                quote_symbol: DEFAULT_QUOTE_SYMBOL.to_owned(),
                block_number: request.target_block,
                block_bucket: request.block_bucket,
                price_decimal: format_decimal(price),
                source: SOURCE_DEX_POOL_SNAPSHOT.to_owned(),
                confidence: (candidate.confidence * quote_confidence * 0.92).min(0.95),
                tx_hash: None,
                metadata: json!({
                    "method": "known_dex_pool_onchain_snapshot",
                    "pool_address": candidate.pool_address,
                    "protocol_name": candidate.protocol_name,
                    "pool_method": candidate.pool_method(),
                    "token0": candidate.token0,
                    "token1": candidate.token1,
                    "quote_token": candidate.quote_token,
                    "relative_price": format_decimal(relative_price),
                    "quote_usd_source": quote_source,
                    "state_block": request.target_block,
                    "historical_state": request.target_block.is_some(),
                    "request_id": request.request_id,
                    "target_timestamp": request.target_timestamp,
                }),
            };
            self.insert_observation_parts(&observation, request.target_timestamp)
                .await?;
            self.upsert_cache_parts(&observation, request.target_timestamp)
                .await?;
            return Ok(true);
        }
        Ok(false)
    }

    async fn load_pool_candidates(
        &self,
        request: &PriceRequest,
    ) -> LedgerResult<Vec<PoolPriceCandidate>> {
        let rows = sqlx::query(
            r#"
            select address, protocol_name, confidence, metadata
            from address_protocol_metadata
            where chain_id = $1
              and protocol_family = 'dex'
              and contract_role = 'dex_pool'
              and (
                lower(metadata->>'token0') = $2
                or lower(metadata->>'token1') = $2
              )
            order by
              case
                when lower(metadata->>'token0') in (
                  '0x833589fcd6edb6e08f4c7c32d4f71b54bda02913',
                  '0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca',
                  '0xfde4c96c8593536e31f229ea8f37b2ada2699bb2'
                ) then 0
                when lower(metadata->>'token1') in (
                  '0x833589fcd6edb6e08f4c7c32d4f71b54bda02913',
                  '0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca',
                  '0xfde4c96c8593536e31f229ea8f37b2ada2699bb2'
                ) then 0
                else 1
              end,
              confidence desc,
              updated_at desc
            limit 20
            "#,
        )
        .bind(request.chain_id as i64)
        .bind(&request.token_address)
        .fetch_all(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;

        let mut candidates = Vec::new();
        for row in rows {
            let metadata = row
                .try_get::<Json<Value>, _>("metadata")
                .map_err(|err| LedgerError::Persistence(err.to_string()))?
                .0;
            let Some(token0) = metadata
                .get("token0")
                .and_then(Value::as_str)
                .map(str::to_ascii_lowercase)
            else {
                continue;
            };
            let Some(token1) = metadata
                .get("token1")
                .and_then(Value::as_str)
                .map(str::to_ascii_lowercase)
            else {
                continue;
            };
            let quote_token = if request.token_address == token0 {
                token1.clone()
            } else {
                token0.clone()
            };
            candidates.push(PoolPriceCandidate {
                chain_id: request.chain_id,
                token_address: request.token_address.clone(),
                quote_token,
                pool_address: row
                    .try_get::<String, _>("address")
                    .map_err(|err| LedgerError::Persistence(err.to_string()))?,
                protocol_name: row
                    .try_get::<String, _>("protocol_name")
                    .map_err(|err| LedgerError::Persistence(err.to_string()))?,
                token0,
                token1,
                token0_decimals: self
                    .token_decimals(
                        request.chain_id,
                        metadata
                            .get("token0")
                            .and_then(Value::as_str)
                            .unwrap_or_default(),
                    )
                    .await?,
                token1_decimals: self
                    .token_decimals(
                        request.chain_id,
                        metadata
                            .get("token1")
                            .and_then(Value::as_str)
                            .unwrap_or_default(),
                    )
                    .await?,
                slot0_supported: metadata
                    .get("slot0_supported")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                get_reserves_supported: metadata
                    .get("get_reserves_supported")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                confidence: row
                    .try_get::<f32, _>("confidence")
                    .map_err(|err| LedgerError::Persistence(err.to_string()))?,
            });
        }
        Ok(candidates)
    }

    async fn fetch_pool_relative_price(
        &self,
        rpc_client: &dyn RpcClient,
        candidate: &PoolPriceCandidate,
        block_number: Option<u64>,
    ) -> LedgerResult<Option<f64>> {
        if candidate.slot0_supported {
            if let Ok(raw) = rpc_client
                .eth_call(
                    candidate.chain_id,
                    &candidate.pool_address,
                    SLOT0_SELECTOR,
                    block_number,
                )
                .await
            {
                if let Some(price_token1_per_token0) = parse_slot0_price_token1_per_token0(
                    &raw,
                    candidate.token0_decimals,
                    candidate.token1_decimals,
                ) {
                    return Ok(
                        candidate.relative_price_from_token1_per_token0(price_token1_per_token0)
                    );
                }
            }
        }
        if candidate.get_reserves_supported {
            if let Ok(raw) = rpc_client
                .eth_call(
                    candidate.chain_id,
                    &candidate.pool_address,
                    GET_RESERVES_SELECTOR,
                    block_number,
                )
                .await
            {
                if let Some(price_token1_per_token0) = parse_reserves_price_token1_per_token0(
                    &raw,
                    candidate.token0_decimals,
                    candidate.token1_decimals,
                ) {
                    return Ok(
                        candidate.relative_price_from_token1_per_token0(price_token1_per_token0)
                    );
                }
            }
        }
        Ok(None)
    }

    async fn usd_cache_for_request(
        &self,
        chain_id: u64,
        token_address: &str,
        block_bucket: Option<u64>,
        historical: bool,
    ) -> LedgerResult<Option<(f64, f32, String)>> {
        let (bucket_filter, freshness_filter) = if historical {
            (
                "and block_bucket between $4 - 2 and $4 + 2",
                "and (metadata->>'historical_state' = 'true' or source = 'tx_execution' or source = 'stable_anchor')",
            )
        } else {
            (
                "",
                "and (expires_at is null or expires_at > now()) and updated_at > now() - interval '10 minutes'",
            )
        };
        let sql = format!(
            r#"
            select price_decimal::text as price_decimal, confidence, source
            from token_price_cache
            where chain_id = $1
              and token_address = $2
              and quote_symbol = $3
              {bucket_filter}
              {freshness_filter}
            order by confidence desc, updated_at desc
            limit 1
            "#
        );
        let mut query = sqlx::query(&sql)
            .bind(chain_id as i64)
            .bind(token_address.to_ascii_lowercase())
            .bind(DEFAULT_QUOTE_SYMBOL);
        if historical {
            let Some(block_bucket) = block_bucket else {
                return Ok(None);
            };
            query = query.bind(block_bucket as i64);
        }
        let row = query
            .fetch_optional(&self.pool)
            .await
            .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        row.map(|row| {
            let price = row
                .try_get::<String, _>("price_decimal")
                .map_err(|err| LedgerError::Persistence(err.to_string()))?
                .parse::<f64>()
                .map_err(|err| LedgerError::MalformedEvidence(err.to_string()))?;
            let confidence = row
                .try_get::<f32, _>("confidence")
                .map_err(|err| LedgerError::Persistence(err.to_string()))?;
            let source = row
                .try_get::<String, _>("source")
                .map_err(|err| LedgerError::Persistence(err.to_string()))?;
            Ok((price, confidence, source))
        })
        .transpose()
    }

    async fn token_decimals(&self, chain_id: u64, token_address: &str) -> LedgerResult<u8> {
        if let Some(stable) = stable_token(&token_address.to_ascii_lowercase()) {
            return Ok(match stable.symbol {
                "USDC" | "USDbC" | "USDT" => 6,
                _ => 18,
            });
        }
        let row = sqlx::query(
            r#"
            select decimals
            from erc20_token_metadata
            where chain_id = $1 and token_address = $2 and fetch_status = 'ok'
            "#,
        )
        .bind(chain_id as i64)
        .bind(token_address.to_ascii_lowercase())
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        Ok(row
            .and_then(|row| row.try_get::<Option<i32>, _>("decimals").ok().flatten())
            .and_then(|value| u8::try_from(value).ok())
            .unwrap_or(18))
    }

    async fn claim_next_price_request(&self) -> LedgerResult<Option<PriceRequest>> {
        let row = sqlx::query(
            r#"
            update price_requests
            set status = 'processing',
                attempts = attempts + 1,
                updated_at = now()
            where request_id = (
                select request_id
                from price_requests
                where status = 'pending'
                  and quote_symbol = $1
                  and (attempts = 0 or updated_at < now() - interval '5 minutes')
                order by priority asc, created_at asc
                for update skip locked
                limit 1
            )
            returning request_id, chain_id, token_address, quote_symbol, target_block,
                      block_bucket, target_timestamp
            "#,
        )
        .bind(DEFAULT_QUOTE_SYMBOL)
        .fetch_optional(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        row.map(|row| {
            Ok(PriceRequest {
                request_id: row
                    .try_get("request_id")
                    .map_err(|err| LedgerError::Persistence(err.to_string()))?,
                chain_id: row
                    .try_get::<i64, _>("chain_id")
                    .map_err(|err| LedgerError::Persistence(err.to_string()))?
                    as u64,
                token_address: row
                    .try_get::<String, _>("token_address")
                    .map_err(|err| LedgerError::Persistence(err.to_string()))?
                    .to_ascii_lowercase(),
                quote_symbol: row
                    .try_get("quote_symbol")
                    .map_err(|err| LedgerError::Persistence(err.to_string()))?,
                target_block: row
                    .try_get::<Option<i64>, _>("target_block")
                    .map_err(|err| LedgerError::Persistence(err.to_string()))?
                    .map(|value| value as u64),
                block_bucket: row
                    .try_get::<Option<i64>, _>("block_bucket")
                    .map_err(|err| LedgerError::Persistence(err.to_string()))?
                    .map(|value| value as u64),
                target_timestamp: row
                    .try_get("target_timestamp")
                    .map_err(|err| LedgerError::Persistence(err.to_string()))?,
            })
        })
        .transpose()
    }

    async fn has_usd_cache(&self, request: &PriceRequest) -> LedgerResult<bool> {
        let Some(block_bucket) = request.block_bucket else {
            return Ok(false);
        };
        let freshness_filter = if request.target_block.is_some() {
            "and (metadata->>'historical_state' = 'true' or source = 'tx_execution' or source = 'stable_anchor')"
        } else {
            "and (expires_at is null or expires_at > now()) and updated_at > now() - interval '10 minutes'"
        };
        let sql = format!(
            r#"
            select 1
            from token_price_cache
            where chain_id = $1
              and token_address = $2
              and quote_symbol = $3
              and block_bucket between $4 - 2 and $4 + 2
              {freshness_filter}
            limit 1
            "#
        );
        let row = sqlx::query(&sql)
            .bind(request.chain_id as i64)
            .bind(&request.token_address)
            .bind(DEFAULT_QUOTE_SYMBOL)
            .bind(block_bucket as i64)
            .fetch_optional(&self.pool)
            .await
            .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        Ok(row.is_some())
    }

    async fn compose_cached_price(&self, request: &PriceRequest) -> LedgerResult<bool> {
        let Some(block_bucket) = request.block_bucket else {
            return Ok(false);
        };
        let quote_cache_filter = if request.target_block.is_some() {
            "and (usd.metadata->>'historical_state' = 'true' or usd.source = 'tx_execution' or usd.source = 'stable_anchor')"
        } else {
            "and (usd.expires_at is null or usd.expires_at > now()) and usd.updated_at > now() - interval '10 minutes'"
        };
        let sql = format!(
            r#"
            select
              rel.quote_token,
              rel.quote_symbol,
              rel.price_decimal::text as relative_price_decimal,
              rel.confidence as relative_confidence,
              rel.source as relative_source,
              rel.block_number as relative_block_number,
              rel.metadata as relative_metadata,
              usd.price_decimal::text as quote_usd_price_decimal,
              usd.confidence as quote_usd_confidence,
              usd.source as quote_usd_source,
              usd.observed_block as quote_usd_observed_block
            from price_observations rel
            join token_price_cache usd
              on usd.chain_id = rel.chain_id
             and usd.token_address = rel.quote_token
             and usd.quote_symbol = $3
             and usd.block_bucket between $4 - 2 and $4 + 2
             {quote_cache_filter}
            where rel.chain_id = $1
              and rel.token_address = $2
              and rel.quote_symbol <> $3
              and rel.block_bucket between $4 - 10 and $4 + 10
            order by
              (rel.confidence * usd.confidence) desc,
              abs(rel.block_bucket - $4) asc,
              rel.created_at desc
            limit 1
            "#
        );
        let row = sqlx::query(&sql)
            .bind(request.chain_id as i64)
            .bind(&request.token_address)
            .bind(DEFAULT_QUOTE_SYMBOL)
            .bind(block_bucket as i64)
            .fetch_optional(&self.pool)
            .await
            .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        let Some(row) = row else {
            return Ok(false);
        };
        let relative_price = row
            .try_get::<String, _>("relative_price_decimal")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?
            .parse::<f64>()
            .map_err(|err| LedgerError::MalformedEvidence(err.to_string()))?;
        let quote_usd_price = row
            .try_get::<String, _>("quote_usd_price_decimal")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?
            .parse::<f64>()
            .map_err(|err| LedgerError::MalformedEvidence(err.to_string()))?;
        let composed_price = relative_price * quote_usd_price;
        if !composed_price.is_finite() || composed_price <= 0.0 {
            return Ok(false);
        }
        let relative_confidence = row
            .try_get::<f32, _>("relative_confidence")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        let quote_usd_confidence = row
            .try_get::<f32, _>("quote_usd_confidence")
            .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        let observation = PriceObservation {
            chain_id: request.chain_id,
            token_address: request.token_address.clone(),
            quote_token: row
                .try_get("quote_token")
                .map_err(|err| LedgerError::Persistence(err.to_string()))?,
            quote_symbol: DEFAULT_QUOTE_SYMBOL.to_owned(),
            block_number: request.target_block,
            block_bucket: request.block_bucket,
            price_decimal: format_decimal(composed_price),
            source: SOURCE_COMPOSED_CACHE.to_owned(),
            confidence: (relative_confidence * quote_usd_confidence * 0.9).min(0.95),
            tx_hash: None,
            metadata: json!({
                "method": "relative_observation_times_quote_usd_cache",
                "request_id": request.request_id,
                "relative_quote_symbol": row.try_get::<String, _>("quote_symbol").unwrap_or_default(),
                "relative_source": row.try_get::<String, _>("relative_source").unwrap_or_default(),
                "quote_usd_source": row.try_get::<String, _>("quote_usd_source").unwrap_or_default(),
                "relative_block_number": row.try_get::<Option<i64>, _>("relative_block_number").ok().flatten(),
                "quote_usd_observed_block": row.try_get::<Option<i64>, _>("quote_usd_observed_block").ok().flatten(),
                "state_block": request.target_block,
                "historical_state": request.target_block.is_some(),
                "relative_metadata": row.try_get::<Json<Value>, _>("relative_metadata").ok().map(|json| json.0),
            }),
        };
        self.insert_observation_parts(&observation, request.target_timestamp)
            .await?;
        self.upsert_cache_parts(&observation, request.target_timestamp)
            .await?;
        Ok(true)
    }

    async fn complete_request(&self, request_id: Uuid, metadata: Value) -> LedgerResult<()> {
        sqlx::query(
            r#"
            update price_requests
            set status = 'completed',
                metadata = metadata || $2::jsonb,
                updated_at = now()
            where request_id = $1
            "#,
        )
        .bind(request_id)
        .bind(Json(metadata))
        .execute(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        Ok(())
    }

    async fn release_request(&self, request_id: Uuid, error: &str) -> LedgerResult<()> {
        sqlx::query(
            r#"
            update price_requests
            set status = case when attempts >= 5 then 'error' else 'pending' end,
                last_error = $2,
                updated_at = now()
            where request_id = $1
            "#,
        )
        .bind(request_id)
        .bind(error)
        .execute(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        Ok(())
    }

    async fn insert_observation(
        &self,
        analysis: &TransactionAnalysis,
        observation: &PriceObservation,
    ) -> LedgerResult<()> {
        self.insert_observation_parts(observation, analysis.block_timestamp)
            .await
    }

    async fn insert_observation_parts(
        &self,
        observation: &PriceObservation,
        block_timestamp: Option<DateTime<Utc>>,
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
        .bind(block_timestamp)
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
        self.upsert_cache_parts(observation, analysis.block_timestamp)
            .await
    }

    async fn upsert_cache_parts(
        &self,
        observation: &PriceObservation,
        observed_at: Option<DateTime<Utc>>,
    ) -> LedgerResult<()> {
        let Some(block_bucket) = observation.block_bucket else {
            return Ok(());
        };
        let historical = observation.block_number.is_some();
        sqlx::query(
            r#"
            insert into token_price_cache (
              chain_id, token_address, quote_symbol, block_bucket, price_decimal,
              confidence, source, observed_block, observed_at, expires_at, metadata
            )
            values (
              $1,$2,$3,$4,$5::numeric,$6,$7,$8,$9,
              case when $11 then null else now() + interval '10 minutes' end,
              $10
            )
            on conflict (chain_id, token_address, quote_symbol, block_bucket) do update set
              price_decimal = case
                when $11 and coalesce(token_price_cache.metadata->>'historical_state', 'false') <> 'true'
                then excluded.price_decimal
                when excluded.confidence >= token_price_cache.confidence
                  or (not $11 and token_price_cache.updated_at <= now() - interval '10 minutes')
                then excluded.price_decimal
                else token_price_cache.price_decimal
              end,
              confidence = case
                when $11 and coalesce(token_price_cache.metadata->>'historical_state', 'false') <> 'true'
                then excluded.confidence
                when not $11 and token_price_cache.updated_at <= now() - interval '10 minutes'
                then excluded.confidence
                else greatest(token_price_cache.confidence, excluded.confidence)
              end,
              source = case
                when $11 and coalesce(token_price_cache.metadata->>'historical_state', 'false') <> 'true'
                then excluded.source
                when excluded.confidence >= token_price_cache.confidence
                  or (not $11 and token_price_cache.updated_at <= now() - interval '10 minutes')
                then excluded.source
                else token_price_cache.source
              end,
              observed_block = case
                when $11 and coalesce(token_price_cache.metadata->>'historical_state', 'false') <> 'true'
                then excluded.observed_block
                when excluded.confidence >= token_price_cache.confidence
                  or (not $11 and token_price_cache.updated_at <= now() - interval '10 minutes')
                then excluded.observed_block
                else token_price_cache.observed_block
              end,
              observed_at = case
                when $11 and coalesce(token_price_cache.metadata->>'historical_state', 'false') <> 'true'
                then excluded.observed_at
                when excluded.confidence >= token_price_cache.confidence
                  or (not $11 and token_price_cache.updated_at <= now() - interval '10 minutes')
                then excluded.observed_at
                else token_price_cache.observed_at
              end,
              expires_at = excluded.expires_at,
              metadata = case
                when $11 and coalesce(token_price_cache.metadata->>'historical_state', 'false') <> 'true'
                then excluded.metadata
                when excluded.confidence >= token_price_cache.confidence
                  or (not $11 and token_price_cache.updated_at <= now() - interval '10 minutes')
                then excluded.metadata
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
        .bind(observed_at)
        .bind(Json(observation.metadata.clone()))
        .bind(historical)
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

    async fn enqueue_current_request(
        &self,
        chain_id: u64,
        token_address: &str,
    ) -> LedgerResult<u64> {
        let result = sqlx::query(
            r#"
            insert into price_requests (
              request_id, chain_id, token_address, quote_symbol, target_block,
              block_bucket, target_timestamp, priority, reason, metadata
            )
            values ($1,$2,$3,$4,null,0,null,$5,$6,$7)
            on conflict do nothing
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(chain_id as i64)
        .bind(token_address.to_ascii_lowercase())
        .bind(DEFAULT_QUOTE_SYMBOL)
        .bind(20i32)
        .bind("address_portfolio_current_price")
        .bind(Json(json!({
            "source": "address_portfolio",
            "current_snapshot": true,
        })))
        .execute(&self.pool)
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        Ok(result.rows_affected())
    }
}

pub fn derive_tx_execution_observations(analysis: &TransactionAnalysis) -> Vec<PriceObservation> {
    let flows = analysis
        .derived_flows
        .iter()
        .filter_map(flow_amount)
        .collect::<Vec<_>>();
    let block_bucket = analysis.block_number.map(block_bucket);
    let mut observations = BTreeMap::<String, PriceObservation>::new();

    for token in &flows {
        for quote in &flows {
            if token.token_address == quote.token_address || token.amount_decimal <= 0.0 {
                continue;
            }
            let Some(confidence) = pair_confidence(quote, token) else {
                continue;
            };
            let price = quote.amount_decimal / token.amount_decimal;
            if !price.is_finite() || price <= 0.0 {
                continue;
            }
            let quote_symbol = quote
                .symbol
                .clone()
                .unwrap_or_else(|| truncate_middle(&quote.token_address, 6));
            observations
                .entry(observation_key(
                    &token.token_address,
                    &quote.token_address,
                    &quote_symbol,
                ))
                .or_insert_with(|| PriceObservation {
                    chain_id: analysis.chain_id,
                    token_address: token.token_address.clone(),
                    quote_token: quote.token_address.clone(),
                    quote_symbol,
                    block_number: analysis.block_number,
                    block_bucket,
                    price_decimal: format_decimal(price),
                    source: SOURCE_TX_EXECUTION.to_owned(),
                    confidence,
                    tx_hash: Some(analysis.tx_hash.clone()),
                    metadata: json!({
                        "method": "paired_erc20_relative_transfer",
                        "quote_flow_id": quote.flow.flow_id,
                        "token_flow_id": token.flow.flow_id,
                        "token_symbol": token.symbol,
                        "token_decimals": token.decimals,
                        "quote_symbol": quote.symbol,
                        "quote_decimals": quote.decimals,
                        "quote_amount_decimal": quote.amount_decimal,
                        "token_amount_decimal": token.amount_decimal,
                    }),
                });
        }
    }

    for stable in flows.iter().filter(|item| item.stable.is_some()) {
        let Some(stable_token) = stable.stable else {
            continue;
        };
        observations
            .entry(observation_key(
                &stable.token_address,
                &stable.token_address,
                DEFAULT_QUOTE_SYMBOL,
            ))
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
                .entry(observation_key(
                    &token.token_address,
                    &stable.token_address,
                    DEFAULT_QUOTE_SYMBOL,
                ))
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

fn observation_key(token_address: &str, quote_token: &str, quote_symbol: &str) -> String {
    format!("{token_address}:{quote_token}:{quote_symbol}")
}

fn parse_amount_decimal(amount_raw: &str, decimals: u8) -> LedgerResult<f64> {
    let raw = amount_raw
        .parse::<f64>()
        .map_err(|err| LedgerError::MalformedEvidence(err.to_string()))?;
    Ok(raw / 10f64.powi(i32::from(decimals)))
}

fn parse_reserves_price_token1_per_token0(
    raw: &str,
    token0_decimals: u8,
    token1_decimals: u8,
) -> Option<f64> {
    let words = hex_words(raw);
    let reserve0 = words.first().and_then(|word| parse_hex_word_f64(word))?;
    let reserve1 = words.get(1).and_then(|word| parse_hex_word_f64(word))?;
    if reserve0 <= 0.0 || reserve1 <= 0.0 {
        return None;
    }
    let token0_amount = reserve0 / 10f64.powi(i32::from(token0_decimals));
    let token1_amount = reserve1 / 10f64.powi(i32::from(token1_decimals));
    if token0_amount <= 0.0 || token1_amount <= 0.0 {
        return None;
    }
    Some(token1_amount / token0_amount)
}

fn parse_slot0_price_token1_per_token0(
    raw: &str,
    token0_decimals: u8,
    token1_decimals: u8,
) -> Option<f64> {
    let words = hex_words(raw);
    let sqrt_price_x96 = words.first().and_then(|word| parse_hex_word_f64(word))?;
    if sqrt_price_x96 <= 0.0 {
        return None;
    }
    let raw_price = (sqrt_price_x96 / 2f64.powi(96)).powi(2);
    let decimal_adjustment = 10f64.powi(i32::from(token0_decimals) - i32::from(token1_decimals));
    let price = raw_price * decimal_adjustment;
    if price.is_finite() && price > 0.0 {
        Some(price)
    } else {
        None
    }
}

fn hex_words(raw: &str) -> Vec<String> {
    let clean = raw.trim().strip_prefix("0x").unwrap_or(raw.trim());
    clean
        .as_bytes()
        .chunks(64)
        .filter_map(|chunk| std::str::from_utf8(chunk).ok())
        .filter(|word| word.len() == 64)
        .map(ToOwned::to_owned)
        .collect()
}

fn parse_hex_word_f64(word: &str) -> Option<f64> {
    u128::from_str_radix(word.trim_start_matches('0'), 16)
        .ok()
        .map(|value| value as f64)
}

fn truncate_middle(value: &str, keep_each_side: usize) -> String {
    if value.len() <= keep_each_side.saturating_mul(2).saturating_add(3) {
        return value.to_owned();
    }
    format!(
        "{}...{}",
        &value[..keep_each_side],
        &value[value.len().saturating_sub(keep_each_side)..]
    )
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

        let observations = derive_tx_execution_observations(&analysis);
        assert!(
            observations
                .iter()
                .all(|observation| observation.quote_symbol != DEFAULT_QUOTE_SYMBOL)
        );
        assert!(
            observations
                .iter()
                .any(|observation| observation.quote_symbol == "WETH")
        );
    }

    #[test]
    fn derives_v2_pool_price_from_reserves() {
        let raw = format!(
            "0x{:064x}{:064x}{:064x}",
            1_000_000_000_000_000_000u128, 3_500_000_000u128, 0u128
        );
        let price = parse_reserves_price_token1_per_token0(&raw, 18, 6)
            .expect("reserve price should parse");
        assert_eq!(format_decimal(price), "3500");
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
