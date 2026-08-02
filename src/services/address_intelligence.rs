use std::{collections::BTreeMap, sync::Arc};

use chrono::Utc;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{
    core::errors::LedgerResult,
    domain::{
        entities::{
            AddressAnalysisJob, AddressLabel, AddressLabelAssertion, AddressProfile,
            ProtocolAddressMetadata, TransactionAnalysis,
        },
        enums::AssetType,
        interfaces::{ClusterTxSource, DynRepository},
        value_objects::AddressTxRef,
    },
    ingestion::rpc::RpcClient,
    services::{analysis::AnalysisService, pricing::PriceService},
};

const BALANCE_OF_SELECTOR: &str = "0x70a08231";
const PORTFOLIO_TOKEN_LIMIT: usize = 64;
const TOKEN0_SELECTOR: &str = "0x0dfe1681";
const TOKEN1_SELECTOR: &str = "0xd21220a7";
const FEE_SELECTOR: &str = "0xddca3f43";
const FACTORY_SELECTOR: &str = "0xc45a0155";
const STABLE_SELECTOR: &str = "0x5f6e283f";
const GET_RESERVES_SELECTOR: &str = "0x0902f1ac";
const SLOT0_SELECTOR: &str = "0x3850c7bd";

#[derive(Clone)]
pub struct AddressIntelligenceService {
    repository: DynRepository,
    rpc_client: Arc<dyn RpcClient>,
    analysis_service: Option<Arc<AnalysisService>>,
    tx_source: Option<Arc<dyn ClusterTxSource>>,
    history_query_limit: u32,
    price_service: Option<Arc<PriceService>>,
    whale_threshold_usd: f64,
    wrapped_native_token_address: Option<String>,
    native_token_symbol: String,
}

impl AddressIntelligenceService {
    pub fn new(repository: DynRepository, rpc_client: Arc<dyn RpcClient>) -> Self {
        Self {
            repository,
            rpc_client,
            analysis_service: None,
            tx_source: None,
            history_query_limit: 1000,
            price_service: None,
            whale_threshold_usd: 100_000.0,
            wrapped_native_token_address: None,
            native_token_symbol: "ETH".to_owned(),
        }
    }

    pub fn with_portfolio_classification(
        mut self,
        price_service: Arc<PriceService>,
        whale_threshold_usd: f64,
    ) -> Self {
        self.price_service = Some(price_service);
        self.whale_threshold_usd = whale_threshold_usd.max(0.0);
        self
    }

    pub fn with_native_asset(
        mut self,
        wrapped_token_address: Option<String>,
        symbol: impl Into<String>,
    ) -> Self {
        self.wrapped_native_token_address = wrapped_token_address
            .map(|address| address.trim().to_ascii_lowercase())
            .filter(|address| !address.is_empty());
        self.native_token_symbol = symbol.into();
        self
    }

    pub fn with_history_backfill(
        mut self,
        analysis_service: Arc<AnalysisService>,
        tx_source: Arc<dyn ClusterTxSource>,
        query_limit: u32,
    ) -> Self {
        self.analysis_service = Some(analysis_service);
        self.tx_source = Some(tx_source);
        self.history_query_limit = query_limit.max(1);
        self
    }

    pub async fn process_next_job(&self) -> LedgerResult<Option<AddressProfile>> {
        let Some(job) = self.repository.claim_next_address_analysis_job().await? else {
            return Ok(None);
        };

        match self.analyze_job(&job).await {
            Ok((mut profile, mut assertions, protocol_metadata)) => {
                if self.backfill_address_history(&job).await? {
                    profile.metadata = merge_metadata(
                        profile.metadata,
                        json!({
                            "address_history_backfilled_at": Utc::now(),
                            "address_history_backfill_source": "indexer",
                        }),
                    );
                }
                if let Some(valuation) = self.evaluate_portfolio(&job, &profile).await? {
                    profile.metadata = merge_metadata(
                        profile.metadata,
                        json!({ "portfolio_estimate": valuation.to_json() }),
                    );
                    if valuation.is_whale() {
                        assertions.push(assertion_for(
                            &job,
                            "actor:whale",
                            "balance_classifier",
                            valuation.label_confidence(),
                            valuation.to_json(),
                            Utc::now(),
                        ));
                    }
                }
                self.repository.upsert_address_profile(&profile).await?;
                self.repository
                    .upsert_address_label_assertions(&assertions)
                    .await?;
                if !protocol_metadata.is_empty() {
                    self.repository
                        .upsert_protocol_address_metadata(&protocol_metadata)
                        .await?;
                }
                self.repository
                    .complete_address_analysis_job(job.chain_id, &job.address)
                    .await?;
                Ok(Some(profile))
            }
            Err(err) => {
                self.repository
                    .fail_address_analysis_job(job.chain_id, &job.address, &err.to_string())
                    .await?;
                Err(err)
            }
        }
    }

    async fn analyze_job(
        &self,
        job: &AddressAnalysisJob,
    ) -> LedgerResult<(
        AddressProfile,
        Vec<AddressLabelAssertion>,
        Vec<ProtocolAddressMetadata>,
    )> {
        let code = self
            .rpc_client
            .get_code(job.chain_id, &job.address, None)
            .await?;
        let now = Utc::now();
        let code_bytes = decode_hex_code(&code);
        let bytecode_length = code_bytes.as_ref().map(|bytes| bytes.len() as u64);
        let code_hash = code_bytes.as_ref().map(|bytes| {
            let mut hasher = Sha256::new();
            hasher.update(bytes);
            format!("0x{}", hex::encode(hasher.finalize()))
        });
        let has_code = bytecode_length.unwrap_or_default() > 0;
        let existing_labels = self
            .repository
            .list_address_labels(job.chain_id, &[job.address.to_ascii_lowercase()])
            .await?;
        let known_address_type = effective_address_type(&existing_labels);
        let protocol_probe = if has_code {
            self.probe_protocol_address(job.chain_id, &job.address)
                .await
        } else {
            None
        };
        let role_labels = role_labels_from_metadata(&job.metadata);
        let address_type = if let Some(known_address_type) = known_address_type {
            known_address_type
        } else if let Some(probe) = protocol_probe.as_ref() {
            probe.contract_role.clone()
        } else if role_labels.iter().any(|label| label == "erc20_token") {
            "token".to_owned()
        } else if has_code
            && role_labels.iter().any(|label| {
                matches!(
                    label.as_str(),
                    "approval_spender" | "suspected_router" | "suspected_router_or_protocol"
                )
            })
        {
            "router_candidate".to_owned()
        } else if has_code
            && role_labels
                .iter()
                .any(|label| matches!(label.as_str(), "suspected_protocol" | "touched_contract"))
        {
            "protocol_candidate".to_owned()
        } else if has_code {
            "contract".to_owned()
        } else {
            "eoa".to_owned()
        };
        let confidence = match address_type.as_str() {
            "token" | "router" | "protocol" => 0.9,
            "router_candidate" | "protocol_candidate" => 0.68,
            "contract" => 0.7,
            "eoa" => 0.8,
            _ => 0.4,
        };
        let profile = AddressProfile {
            chain_id: job.chain_id,
            address: job.address.to_ascii_lowercase(),
            address_type: address_type.clone(),
            code_hash,
            bytecode_length,
            first_seen_at: job.first_seen_at,
            last_seen_at: now,
            last_analyzed_at: Some(now),
            confidence,
            metadata: json!({
                "analyzed_by": "ledgerflow_address_worker",
                "job_metadata": job.metadata,
                "protocol_intelligence": protocol_probe.as_ref().map(ProtocolProbe::to_json),
            }),
            created_at: now,
            updated_at: now,
        };

        let mut assertions = vec![assertion_for(
            job,
            &address_type,
            "address_worker",
            confidence,
            json!({ "reason": "address_type" }),
            now,
        )];
        for role_label in role_labels {
            assertions.push(assertion_for(
                job,
                &role_label,
                "tx_analysis",
                0.75,
                json!({ "reason": "observed_role", "metadata": job.metadata }),
                now,
            ));
        }
        let protocol_metadata = protocol_probe
            .as_ref()
            .map(|probe| probe.to_metadata(job, now))
            .into_iter()
            .collect::<Vec<_>>();
        if let Some(probe) = protocol_probe {
            assertions.push(assertion_for(
                job,
                &probe.protocol_family,
                "protocol_probe",
                probe.confidence,
                probe.to_json(),
                now,
            ));
            assertions.push(assertion_for(
                job,
                &probe.protocol_name,
                "protocol_probe",
                probe.confidence,
                probe.to_json(),
                now,
            ));
            assertions.push(assertion_for(
                job,
                &probe.contract_role,
                "protocol_probe",
                probe.confidence,
                probe.to_json(),
                now,
            ));
        }
        Ok((profile, assertions, protocol_metadata))
    }

    async fn probe_protocol_address(&self, chain_id: u64, address: &str) -> Option<ProtocolProbe> {
        let token0 = self
            .fetch_address(chain_id, address, TOKEN0_SELECTOR)
            .await?;
        let token1 = self
            .fetch_address(chain_id, address, TOKEN1_SELECTOR)
            .await?;
        let factory = self
            .fetch_address(chain_id, address, FACTORY_SELECTOR)
            .await;
        let fee = self.fetch_uint(chain_id, address, FEE_SELECTOR).await;
        let stable = self.fetch_bool(chain_id, address, STABLE_SELECTOR).await;
        let get_reserves_supported = self
            .rpc_client
            .eth_call(chain_id, address, GET_RESERVES_SELECTOR, None)
            .await
            .ok()
            .filter(|value| value.trim_start_matches("0x").len() >= 192)
            .is_some();
        let slot0_supported = self
            .rpc_client
            .eth_call(chain_id, address, SLOT0_SELECTOR, None)
            .await
            .ok()
            .filter(|value| value.trim_start_matches("0x").len() >= 224)
            .is_some();
        let protocol_name = if slot0_supported || fee.is_some() {
            "uniswap_v3_compatible_pool"
        } else if get_reserves_supported {
            "uniswap_v2_compatible_pool"
        } else if stable.is_some() {
            "solidly_compatible_pool"
        } else {
            "uniswap_v2_compatible_pool"
        }
        .to_owned();
        Some(ProtocolProbe {
            protocol_family: "dex".to_owned(),
            protocol_name,
            contract_role: "dex_pool".to_owned(),
            token0,
            token1,
            factory,
            fee,
            stable,
            get_reserves_supported,
            slot0_supported,
            confidence: if slot0_supported || get_reserves_supported {
                0.9
            } else {
                0.82
            },
        })
    }

    async fn fetch_address(&self, chain_id: u64, address: &str, selector: &str) -> Option<String> {
        self.rpc_client
            .eth_call(chain_id, address, selector, None)
            .await
            .ok()
            .and_then(|value| decode_address_word(&value))
    }

    async fn fetch_uint(&self, chain_id: u64, address: &str, selector: &str) -> Option<u128> {
        self.rpc_client
            .eth_call(chain_id, address, selector, None)
            .await
            .ok()
            .and_then(|value| parse_uint_hex(&value))
    }

    async fn fetch_bool(&self, chain_id: u64, address: &str, selector: &str) -> Option<bool> {
        self.fetch_uint(chain_id, address, selector)
            .await
            .map(|value| value != 0)
    }

    async fn backfill_address_history(&self, job: &AddressAnalysisJob) -> LedgerResult<bool> {
        if !should_backfill_history(&job.metadata) {
            return Ok(false);
        }
        let (Some(analysis_service), Some(tx_source)) =
            (self.analysis_service.as_ref(), self.tx_source.as_ref())
        else {
            return Ok(false);
        };
        let end_block = tx_source.get_last_indexed_block(job.chain_id).await?;
        let mut tx_map = BTreeMap::<String, AddressTxRef>::new();
        collect_refs_for_range(
            tx_source.as_ref(),
            job.chain_id,
            &job.address,
            0,
            end_block,
            self.history_query_limit,
            &mut tx_map,
        )
        .await?;

        let mut txs = tx_map.into_values().collect::<Vec<_>>();
        txs.sort_by(|left, right| {
            (left.block_number, left.tx_index, left.tx_hash.as_str()).cmp(&(
                right.block_number,
                right.tx_index,
                right.tx_hash.as_str(),
            ))
        });
        for tx_ref in txs {
            if self
                .repository
                .get_analysis(job.chain_id, &tx_ref.tx_hash)
                .await?
                .is_some()
            {
                continue;
            }
            if analysis_service
                .analyze_transaction(job.chain_id, &tx_ref.tx_hash)
                .await
                .is_err()
            {
                continue;
            }
        }
        Ok(true)
    }

    async fn evaluate_portfolio(
        &self,
        job: &AddressAnalysisJob,
        profile: &AddressProfile,
    ) -> LedgerResult<Option<PortfolioValuation>> {
        let Some(price_service) = self.price_service.as_ref() else {
            return Ok(None);
        };
        if !portfolio_eligible_address_type(&profile.address_type) {
            return Ok(None);
        }
        let analyses = self
            .repository
            .list_analyses_for_cluster(job.chain_id, std::slice::from_ref(&job.address), None, None)
            .await?;
        let tokens = collect_portfolio_tokens(&job.address, &analyses, PORTFOLIO_TOKEN_LIMIT);
        let observed_token_count = tokens.len();
        let mut assets = Vec::new();
        let mut missing_prices = Vec::new();

        for token in tokens {
            let Some(raw_balance) = self
                .erc20_balance(job.chain_id, &token.address, &job.address)
                .await
            else {
                continue;
            };
            if raw_balance == "0" {
                continue;
            }
            match price_service
                .quote_current_amount(job.chain_id, &token.address, &raw_balance, token.decimals)
                .await?
            {
                Some(quote) => assets.push(json!({
                    "asset_type": "erc20",
                    "token_address": token.address,
                    "symbol": token.symbol,
                    "balance_raw_approx": raw_balance,
                    "amount_decimal": quote.amount_decimal,
                    "price_decimal": quote.price_decimal,
                    "estimated_usd": quote.value_decimal,
                    "price_source": quote.source,
                })),
                None => missing_prices.push(token.address),
            }
        }

        if let Ok(raw_balance) = self
            .rpc_client
            .get_balance(job.chain_id, &job.address, None)
            .await
            && let Some(raw_balance) = hex_quantity_to_decimal_approx(&raw_balance)
            && raw_balance != "0"
        {
            if let Some(wrapped_native_token_address) = &self.wrapped_native_token_address {
                match price_service
                    .quote_current_amount(
                        job.chain_id,
                        wrapped_native_token_address,
                        &raw_balance,
                        18,
                    )
                    .await?
                {
                    Some(quote) => assets.push(json!({
                        "asset_type": "native",
                        "token_address": Value::Null,
                        "wrapped_token_address": wrapped_native_token_address,
                        "symbol": self.native_token_symbol,
                        "balance_raw_approx": raw_balance,
                        "amount_decimal": quote.amount_decimal,
                        "price_decimal": quote.price_decimal,
                        "estimated_usd": quote.value_decimal,
                        "price_source": quote.source,
                    })),
                    None => missing_prices.push("native".to_owned()),
                }
            } else {
                missing_prices.push("native".to_owned());
            }
        }

        let estimated_usd = assets
            .iter()
            .filter_map(|asset| asset.get("estimated_usd").and_then(Value::as_str))
            .filter_map(|value| value.parse::<f64>().ok())
            .sum::<f64>();
        Ok(Some(PortfolioValuation {
            estimated_usd,
            threshold_usd: self.whale_threshold_usd,
            assets,
            missing_prices,
            observed_token_count,
            evaluated_at: Utc::now(),
        }))
    }

    async fn erc20_balance(&self, chain_id: u64, token: &str, address: &str) -> Option<String> {
        let data = format!(
            "{BALANCE_OF_SELECTOR}{:0>64}",
            address.trim_start_matches("0x").to_ascii_lowercase()
        );
        self.rpc_client
            .eth_call(chain_id, token, &data, None)
            .await
            .ok()
            .and_then(|value| hex_quantity_to_decimal_approx(&value))
    }
}

#[derive(Clone, Debug)]
struct PortfolioToken {
    address: String,
    decimals: u8,
    symbol: Option<String>,
}

#[derive(Clone, Debug)]
struct PortfolioValuation {
    estimated_usd: f64,
    threshold_usd: f64,
    assets: Vec<Value>,
    missing_prices: Vec<String>,
    observed_token_count: usize,
    evaluated_at: chrono::DateTime<Utc>,
}

impl PortfolioValuation {
    fn to_json(&self) -> Value {
        json!({
            "method": "analyzed_token_balance_lower_bound",
            "estimated_usd": self.estimated_usd,
            "threshold_usd": self.threshold_usd,
            "is_whale": self.estimated_usd >= self.threshold_usd,
            "assets": self.assets,
            "missing_prices": self.missing_prices,
            "observed_token_count": self.observed_token_count,
            "evaluated_at": self.evaluated_at,
        })
    }

    fn label_confidence(&self) -> f32 {
        if self.missing_prices.is_empty() {
            0.92
        } else {
            0.82
        }
    }

    fn is_whale(&self) -> bool {
        self.estimated_usd >= self.threshold_usd
    }
}

fn portfolio_eligible_address_type(address_type: &str) -> bool {
    address_type == "eoa"
}

fn collect_portfolio_tokens(
    address: &str,
    analyses: &[TransactionAnalysis],
    limit: usize,
) -> Vec<PortfolioToken> {
    let address = address.to_ascii_lowercase();
    let mut tokens = BTreeMap::<String, PortfolioToken>::new();
    for analysis in analyses.iter().rev() {
        for flow in &analysis.derived_flows {
            if flow.asset_type != AssetType::Erc20
                || (flow.from_address.to_ascii_lowercase() != address
                    && flow.to_address.to_ascii_lowercase() != address)
            {
                continue;
            }
            let Some(token_address) = flow.token_address.as_deref() else {
                continue;
            };
            let token_address = token_address.to_ascii_lowercase();
            tokens
                .entry(token_address.clone())
                .or_insert_with(|| PortfolioToken {
                    address: token_address,
                    decimals: flow
                        .metadata
                        .pointer("/token/decimals")
                        .and_then(Value::as_u64)
                        .and_then(|value| u8::try_from(value).ok())
                        .unwrap_or(18),
                    symbol: flow
                        .metadata
                        .pointer("/token/symbol")
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned),
                });
            if tokens.len() >= limit {
                return tokens.into_values().collect();
            }
        }
    }
    tokens.into_values().collect()
}

fn hex_quantity_to_decimal_approx(value: &str) -> Option<String> {
    let raw = value.trim().strip_prefix("0x").unwrap_or(value.trim());
    if raw.is_empty() || raw.chars().all(|ch| ch == '0') {
        return Some("0".to_owned());
    }
    let number = raw.chars().try_fold(0f64, |acc, digit| {
        let digit = digit.to_digit(16)? as f64;
        Some(acc * 16.0 + digit)
    })?;
    number.is_finite().then(|| format!("{number:.0}"))
}

struct ProtocolProbe {
    protocol_family: String,
    protocol_name: String,
    contract_role: String,
    token0: String,
    token1: String,
    factory: Option<String>,
    fee: Option<u128>,
    stable: Option<bool>,
    get_reserves_supported: bool,
    slot0_supported: bool,
    confidence: f32,
}

impl ProtocolProbe {
    fn to_json(&self) -> Value {
        json!({
            "protocol_family": self.protocol_family,
            "protocol_name": self.protocol_name,
            "contract_role": self.contract_role,
            "token0": self.token0,
            "token1": self.token1,
            "factory": self.factory,
            "fee": self.fee.map(|value| value.to_string()),
            "stable": self.stable,
            "get_reserves_supported": self.get_reserves_supported,
            "slot0_supported": self.slot0_supported,
            "confidence": self.confidence,
        })
    }

    fn to_metadata(
        &self,
        job: &AddressAnalysisJob,
        now: chrono::DateTime<Utc>,
    ) -> ProtocolAddressMetadata {
        ProtocolAddressMetadata {
            chain_id: job.chain_id,
            address: job.address.to_ascii_lowercase(),
            protocol_family: self.protocol_family.clone(),
            protocol_name: self.protocol_name.clone(),
            contract_role: self.contract_role.clone(),
            source: "protocol_probe".to_owned(),
            adapter_version: "dex_probe_v1".to_owned(),
            confidence: self.confidence,
            metadata: self.to_json(),
            first_seen_at: job.first_seen_at,
            last_seen_at: now,
            updated_at: now,
        }
    }
}

fn decode_address_word(value: &str) -> Option<String> {
    let raw = value.trim().strip_prefix("0x").unwrap_or(value.trim());
    if raw.len() < 64 {
        return None;
    }
    let address = &raw[raw.len().saturating_sub(40)..];
    if address.chars().all(|ch| ch == '0') {
        return None;
    }
    Some(format!("0x{}", address.to_ascii_lowercase()))
}

fn parse_uint_hex(value: &str) -> Option<u128> {
    u128::from_str_radix(value.trim().strip_prefix("0x").unwrap_or(value.trim()), 16).ok()
}

async fn collect_refs_for_range(
    tx_source: &dyn ClusterTxSource,
    chain_id: u64,
    address: &str,
    start_block: u64,
    end_block: u64,
    query_limit: u32,
    tx_map: &mut BTreeMap<String, AddressTxRef>,
) -> LedgerResult<()> {
    let mut ranges = vec![(start_block, end_block)];
    while let Some((range_start, range_end)) = ranges.pop() {
        let refs = tx_source
            .query_address_refs(
                chain_id,
                address,
                query_limit,
                Some(range_start),
                Some(range_end),
            )
            .await?;

        if refs.len() as u32 >= query_limit && range_start < range_end {
            let mid = range_start + ((range_end - range_start) / 2);
            ranges.push((mid.saturating_add(1), range_end));
            ranges.push((range_start, mid));
            continue;
        }

        for item in refs {
            tx_map
                .entry(item.tx_hash.clone())
                .and_modify(|existing| {
                    if (item.block_number, item.tx_index)
                        < (existing.block_number, existing.tx_index)
                    {
                        *existing = item.clone();
                    }
                })
                .or_insert(item);
        }
    }
    Ok(())
}

fn should_backfill_history(metadata: &Value) -> bool {
    metadata
        .get("source")
        .and_then(|value| value.as_str())
        .map(|value| matches!(value, "address_summary" | "address_analysis_page"))
        .unwrap_or(false)
        || role_labels_from_metadata(metadata)
            .iter()
            .any(|role| role == "manual_lookup")
}

fn merge_metadata(mut left: Value, right: Value) -> Value {
    if let (Some(left_obj), Some(right_obj)) = (left.as_object_mut(), right.as_object()) {
        for (key, value) in right_obj {
            left_obj.insert(key.clone(), value.clone());
        }
        left
    } else {
        right
    }
}

fn effective_address_type(labels: &[AddressLabel]) -> Option<String> {
    const PRECEDENCE: &[&str] = &[
        "token",
        "router",
        "protocol",
        "dex_pool",
        "router_candidate",
        "protocol_candidate",
        "eoa",
        "contract",
    ];
    PRECEDENCE.iter().find_map(|candidate| {
        labels
            .iter()
            .filter(|label| label.confidence >= 0.70)
            .any(|label| {
                label
                    .label
                    .strip_prefix("address_type:")
                    .unwrap_or(&label.label)
                    == *candidate
            })
            .then(|| (*candidate).to_owned())
    })
}

fn role_labels_from_metadata(metadata: &Value) -> Vec<String> {
    metadata
        .get("roles")
        .and_then(|value| value.as_array())
        .into_iter()
        .flatten()
        .filter_map(|value| value.as_str())
        .filter(|value| !value.trim().is_empty())
        .map(|value| value.trim().to_ascii_lowercase())
        .collect()
}

fn assertion_for(
    job: &AddressAnalysisJob,
    label: &str,
    source: &str,
    confidence: f32,
    evidence_json: Value,
    now: chrono::DateTime<Utc>,
) -> AddressLabelAssertion {
    let (dimension, exclusive_group) = label_taxonomy(label);
    let (source_kind, source_trust, origin_kind, method_version) = source_provenance(source);
    let address = job.address.to_ascii_lowercase();
    let mut hasher = Sha256::new();
    hasher.update(format!(
        "{}|{}|{}|{}|{}",
        job.chain_id, address, label, source, method_version
    ));
    AddressLabelAssertion {
        assertion_id: Uuid::new_v4(),
        claim_fingerprint: hex::encode(hasher.finalize()),
        chain_id: job.chain_id,
        address,
        label: label.to_owned(),
        dimension: dimension.to_owned(),
        exclusive_group: exclusive_group.map(str::to_owned),
        source: source.to_owned(),
        source_kind: source_kind.to_owned(),
        source_trust,
        origin_kind: origin_kind.to_owned(),
        method_version: method_version.to_owned(),
        dataset_key: None,
        dataset_version: None,
        assertion_value: true,
        confidence,
        status: "active".to_owned(),
        valid_from_block: None,
        valid_to_block: None,
        observed_at: now,
        expires_at: None,
        evidence_json,
        created_at: now,
        updated_at: now,
    }
}

fn label_taxonomy(label: &str) -> (&'static str, Option<&'static str>) {
    if label.starts_with("actor:") {
        ("actor_type", None)
    } else if label.starts_with("token:")
        || label.starts_with("canonical:")
        || label == "stablecoin"
    {
        ("asset_identity", None)
    } else if label.starts_with("protocol:") {
        ("protocol", None)
    } else if matches!(label, "eoa" | "contract") {
        ("address_kind", Some("address_kind"))
    } else if label.contains("router")
        || label.contains("pool")
        || matches!(
            label,
            "token"
                | "protocol"
                | "router_candidate"
                | "protocol_candidate"
                | "approval_spender"
                | "touched_contract"
        )
    {
        ("contract_role", None)
    } else {
        ("behavior_or_role", None)
    }
}

fn source_provenance(source: &str) -> (&'static str, f32, &'static str, &'static str) {
    match source {
        "address_worker" => ("onchain", 0.90, "onchain", "address_worker_v2"),
        "protocol_probe" => ("onchain", 0.92, "onchain", "dex_probe_v1"),
        "tx_analysis" => ("model", 0.78, "model", "transaction_roles_v1"),
        "balance_classifier" => ("model", 0.82, "model", "portfolio_classifier_v1"),
        _ => ("model", 0.50, "model", "unknown_v1"),
    }
}

fn decode_hex_code(code: &str) -> Option<Vec<u8>> {
    let code = code.trim();
    if code == "0x" || code.is_empty() {
        return Some(Vec::new());
    }
    let without_prefix = code.strip_prefix("0x").unwrap_or(code);
    hex::decode(without_prefix).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_user_like_addresses_are_portfolio_candidates() {
        assert!(portfolio_eligible_address_type("eoa"));
        assert!(!portfolio_eligible_address_type("contract"));
        assert!(!portfolio_eligible_address_type("dex_pool"));
        assert!(!portfolio_eligible_address_type("router"));
        assert!(!portfolio_eligible_address_type("protocol"));
    }

    #[test]
    fn partial_portfolio_can_promote_whale_without_full_coverage() {
        let valuation = PortfolioValuation {
            estimated_usd: 125_000.0,
            threshold_usd: 100_000.0,
            assets: Vec::new(),
            missing_prices: vec!["0xunknown".to_owned()],
            observed_token_count: 2,
            evaluated_at: Utc::now(),
        };
        assert!(valuation.is_whale());
        assert_eq!(valuation.label_confidence(), 0.82);
    }

    #[test]
    fn parses_large_rpc_quantities_for_valuation() {
        assert_eq!(hex_quantity_to_decimal_approx("0x0").as_deref(), Some("0"));
        assert_eq!(
            hex_quantity_to_decimal_approx("0xde0b6b3a7640000").as_deref(),
            Some("1000000000000000000")
        );
    }
}
