use std::{collections::BTreeMap, sync::Arc};

use chrono::Utc;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{
    core::errors::{LedgerError, LedgerResult},
    domain::{
        entities::{
            AddressAnalysisJob, AddressLabel, AddressLabelAssertion, AddressProfile,
            ProtocolAddressMetadata, TransactionAnalysis,
        },
        enums::{AddressHitDirection, AssetType},
        interfaces::{ClusterTxSource, DynRepository},
        value_objects::AddressTxRef,
    },
    ingestion::rpc::RpcClient,
    services::{
        analysis::AnalysisService, pricing::PriceService, token_metadata::TokenMetadataService,
    },
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
const ERC20_TRANSFER_TOPIC: &str =
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
const ADDRESS_ACTIVITY_LOG_CHUNK_BLOCKS: u64 = 5_000;

#[derive(Debug, Default)]
struct AddressHistoryResult {
    start_block: Option<u64>,
    end_block: Option<u64>,
    direct_transaction_count: usize,
    transfer_log_count: usize,
    unique_transaction_count: usize,
    analyzed_transaction_count: usize,
    limitations: Vec<String>,
}

impl AddressHistoryResult {
    fn to_json(&self) -> Value {
        json!({
            "start_block": self.start_block,
            "end_block": self.end_block,
            "direct_transaction_count": self.direct_transaction_count,
            "transfer_log_count": self.transfer_log_count,
            "unique_transaction_count": self.unique_transaction_count,
            "analyzed_transaction_count": self.analyzed_transaction_count,
            "limitations": self.limitations,
        })
    }
}

#[derive(Clone)]
pub struct AddressIntelligenceService {
    repository: DynRepository,
    rpc_client: Arc<dyn RpcClient>,
    analysis_service: Option<Arc<AnalysisService>>,
    token_metadata_service: Option<Arc<TokenMetadataService>>,
    tx_source: Option<Arc<dyn ClusterTxSource>>,
    history_query_limit: u32,
    price_service: Option<Arc<PriceService>>,
    whale_threshold_usd: f64,
    native_token_symbol: String,
    maximum_job_priority: i32,
    worker_chain_id: Option<u64>,
    history_lookback_blocks: u64,
    history_analysis_limit: usize,
}

impl AddressIntelligenceService {
    pub fn new(repository: DynRepository, rpc_client: Arc<dyn RpcClient>) -> Self {
        Self {
            repository,
            rpc_client,
            analysis_service: None,
            token_metadata_service: None,
            tx_source: None,
            history_query_limit: 1000,
            price_service: None,
            whale_threshold_usd: 100_000.0,
            native_token_symbol: "ETH".to_owned(),
            maximum_job_priority: 100,
            worker_chain_id: None,
            history_lookback_blocks: 50_000,
            history_analysis_limit: 32,
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

    pub fn with_token_metadata_service(
        mut self,
        token_metadata_service: Arc<TokenMetadataService>,
    ) -> Self {
        self.token_metadata_service = Some(token_metadata_service);
        self
    }

    pub fn with_native_asset(mut self, symbol: impl Into<String>) -> Self {
        self.native_token_symbol = symbol.into();
        self
    }

    pub fn with_maximum_job_priority(mut self, maximum_job_priority: i32) -> Self {
        self.maximum_job_priority = maximum_job_priority.clamp(0, 100);
        self
    }

    pub fn with_worker_chain_id(mut self, chain_id: u64) -> Self {
        self.worker_chain_id = Some(chain_id);
        self
    }

    pub fn with_history_limits(
        mut self,
        history_lookback_blocks: u64,
        history_analysis_limit: usize,
    ) -> Self {
        self.history_lookback_blocks = history_lookback_blocks.max(1);
        self.history_analysis_limit = history_analysis_limit.clamp(1, 256);
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

    pub fn with_transaction_analysis(mut self, analysis_service: Arc<AnalysisService>) -> Self {
        self.analysis_service = Some(analysis_service);
        self
    }

    pub async fn process_next_job(&self) -> LedgerResult<Option<AddressProfile>> {
        let job = match self.worker_chain_id {
            Some(chain_id) => {
                self.repository
                    .claim_next_address_analysis_job_for_chain_up_to(
                        chain_id,
                        self.maximum_job_priority,
                    )
                    .await?
            }
            None => {
                self.repository
                    .claim_next_address_analysis_job_up_to(self.maximum_job_priority)
                    .await?
            }
        };
        let Some(job) = job else {
            return Ok(None);
        };

        match self.analyze_job(&job).await {
            Ok((mut profile, mut assertions, protocol_metadata)) => {
                if should_backfill_address_history(&profile.address_type) {
                    if let Some(history) = self.backfill_address_history(&job).await? {
                        profile.metadata = merge_metadata(
                            profile.metadata,
                            json!({
                                "address_history_backfilled_at": Utc::now(),
                                "address_history_backfill_source": "indexer_and_rpc_activity_locator",
                                "address_history_backfill_mode": "bounded_recent_activity",
                                "address_history_lookback_blocks": self.history_lookback_blocks,
                                "address_history_analysis_limit": self.history_analysis_limit,
                                "address_history": history.to_json(),
                            }),
                        );
                    }
                } else {
                    profile.metadata = merge_metadata(
                        profile.metadata,
                        json!({
                            "address_history_backfill_skipped": true,
                            "address_history_backfill_skip_reason":
                                "high-volume protocol address uses its scoped protocol projector",
                        }),
                    );
                }
                if should_evaluate_portfolio(&job.metadata)
                    && let Some(valuation) = self.evaluate_portfolio(&job, &profile).await?
                {
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
        let role_labels = role_labels_from_metadata(&job.metadata);
        let token_metadata = if has_code
            && role_labels
                .iter()
                .any(|label| matches!(label.as_str(), "suspected_token" | "erc20_token"))
        {
            match self.token_metadata_service.as_ref() {
                Some(service) => Some(
                    service
                        .get_or_fetch(job.chain_id, &job.address, None)
                        .await?,
                ),
                None => None,
            }
        } else {
            None
        };
        let token_metadata_verified = token_metadata
            .as_ref()
            .is_some_and(|metadata| metadata.fetch_status == "ok");
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
        let address_type = if token_metadata_verified {
            "token".to_owned()
        } else if let Some(known_address_type) = known_address_type {
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
                "token_metadata": token_metadata.as_ref().map(|metadata| metadata.to_json()),
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
        if let Some(metadata) = token_metadata
            .as_ref()
            .filter(|metadata| metadata.fetch_status == "ok")
        {
            assertions.push(assertion_for(
                job,
                "erc20_token",
                "token_metadata_probe",
                0.95,
                metadata.to_json(),
                now,
            ));
        }
        let role_source = role_assertion_source(&job.metadata);
        for role_label in role_labels {
            assertions.push(assertion_for(
                job,
                &role_label,
                role_source,
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

    async fn backfill_address_history(
        &self,
        job: &AddressAnalysisJob,
    ) -> LedgerResult<Option<AddressHistoryResult>> {
        let evidence_hashes =
            evidence_transaction_hashes(&job.metadata, self.history_query_limit.min(16) as usize);
        if !evidence_hashes.is_empty() {
            let Some(analysis_service) = self.analysis_service.as_ref() else {
                return Err(LedgerError::AnalysisInvariant(
                    "bounded evidence history was requested but transaction analysis is not configured"
                        .to_owned(),
                ));
            };
            let mut failures = Vec::new();
            let mut analyzed_transaction_count = 0;
            for tx_hash in evidence_hashes {
                if self
                    .repository
                    .get_analysis(job.chain_id, &tx_hash)
                    .await?
                    .is_some()
                {
                    analyzed_transaction_count += 1;
                    continue;
                }
                if let Err(error) = analysis_service
                    .analyze_transaction(job.chain_id, &tx_hash)
                    .await
                {
                    failures.push(format!("{tx_hash}: {error}"));
                } else {
                    analyzed_transaction_count += 1;
                }
            }
            if !failures.is_empty() {
                return Err(LedgerError::TransientIngestion(format!(
                    "{} bounded seed evidence transaction(s) could not be analyzed: {}",
                    failures.len(),
                    failures.join("; ").chars().take(4_000).collect::<String>()
                )));
            }
            return Ok(Some(AddressHistoryResult {
                unique_transaction_count: analyzed_transaction_count,
                analyzed_transaction_count,
                ..AddressHistoryResult::default()
            }));
        }
        if !should_backfill_history(&job.metadata) {
            return Ok(None);
        }
        let (Some(analysis_service), Some(tx_source)) =
            (self.analysis_service.as_ref(), self.tx_source.as_ref())
        else {
            return Ok(None);
        };
        let end_block = tx_source.get_last_indexed_block(job.chain_id).await?;
        let start_block = end_block.saturating_sub(self.history_lookback_blocks.saturating_sub(1));
        let mut tx_map = BTreeMap::<String, AddressTxRef>::new();
        collect_refs_for_range(
            tx_source.as_ref(),
            job.chain_id,
            &job.address,
            start_block,
            end_block,
            self.history_query_limit,
            &mut tx_map,
        )
        .await?;
        let direct_transaction_count = tx_map.len();
        let (transfer_log_count, activity_scan_start, transfer_limitation) = self
            .collect_transfer_activity_refs(job, start_block, end_block, &mut tx_map)
            .await;

        let mut txs = tx_map.into_values().collect::<Vec<_>>();
        txs.sort_by(|left, right| {
            (right.block_number, right.tx_index, right.tx_hash.as_str()).cmp(&(
                left.block_number,
                left.tx_index,
                left.tx_hash.as_str(),
            ))
        });
        let unique_transaction_count = txs.len();
        txs.truncate(self.history_analysis_limit);
        let mut analyzed_transaction_count = 0;
        for tx_ref in txs {
            if self
                .repository
                .get_analysis(job.chain_id, &tx_ref.tx_hash)
                .await?
                .is_some()
            {
                analyzed_transaction_count += 1;
                continue;
            }
            if analysis_service
                .analyze_transaction(job.chain_id, &tx_ref.tx_hash)
                .await
                .is_err()
            {
                continue;
            }
            analyzed_transaction_count += 1;
        }
        let mut limitations = Vec::new();
        if let Some(limitation) = transfer_limitation {
            limitations.push(limitation);
        }
        if unique_transaction_count > self.history_analysis_limit {
            limitations.push(format!(
                "Only the newest {} of {} discovered transactions were analyzed.",
                self.history_analysis_limit, unique_transaction_count
            ));
        }
        Ok(Some(AddressHistoryResult {
            start_block: Some(activity_scan_start.unwrap_or(start_block)),
            end_block: Some(end_block),
            direct_transaction_count,
            transfer_log_count,
            unique_transaction_count,
            analyzed_transaction_count,
            limitations,
        }))
    }

    async fn collect_transfer_activity_refs(
        &self,
        job: &AddressAnalysisJob,
        start_block: u64,
        end_block: u64,
        tx_map: &mut BTreeMap<String, AddressTxRef>,
    ) -> (usize, Option<u64>, Option<String>) {
        let address_topic = padded_address_topic(&job.address);
        let discovery_limit = self.history_analysis_limit.saturating_mul(8).clamp(64, 512);
        let mut cursor_end = end_block;
        let mut transfer_log_count = 0usize;
        let mut earliest_scanned;
        loop {
            let chunk_start = cursor_end
                .saturating_sub(ADDRESS_ACTIVITY_LOG_CHUNK_BLOCKS.saturating_sub(1))
                .max(start_block);
            earliest_scanned = chunk_start;
            for (direction, topics) in [
                (
                    AddressHitDirection::To,
                    json!([ERC20_TRANSFER_TOPIC, null, address_topic]),
                ),
                (
                    AddressHitDirection::From,
                    json!([ERC20_TRANSFER_TOPIC, address_topic]),
                ),
            ] {
                let filter = json!({
                    "fromBlock": format!("0x{chunk_start:x}"),
                    "toBlock": format!("0x{cursor_end:x}"),
                    "topics": topics,
                });
                let logs = match self.rpc_client.get_logs(job.chain_id, filter).await {
                    Ok(logs) => logs,
                    Err(error) => {
                        return (
                            transfer_log_count,
                            Some(earliest_scanned),
                            Some(format!(
                                "Token-transfer activity lookup stopped at block {cursor_end}: {error}"
                            )),
                        );
                    }
                };
                transfer_log_count = transfer_log_count.saturating_add(logs.len());
                for log in logs {
                    if let Some(reference) = transfer_log_address_ref(
                        job.chain_id,
                        &job.address,
                        direction.clone(),
                        &log,
                    ) {
                        tx_map.entry(reference.tx_hash.clone()).or_insert(reference);
                    }
                }
            }
            if tx_map.len() >= discovery_limit || chunk_start == start_block {
                break;
            }
            cursor_end = chunk_start.saturating_sub(1);
        }
        (transfer_log_count, Some(earliest_scanned), None)
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
            .list_analyses_for_observed_addresses(
                job.chain_id,
                std::slice::from_ref(&job.address),
                None,
                None,
            )
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
            if let Some(wrapped_native_token_address) = price_service
                .wrapped_native_token_address(job.chain_id)
                .await?
            {
                match price_service
                    .quote_current_amount(
                        job.chain_id,
                        &wrapped_native_token_address,
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

fn padded_address_topic(address: &str) -> String {
    format!(
        "0x{:0>64}",
        address.trim_start_matches("0x").to_ascii_lowercase()
    )
}

fn address_from_topic(topic: &str) -> Option<String> {
    let body = topic.trim().trim_start_matches("0x");
    if body.len() != 64 || !body.chars().all(|character| character.is_ascii_hexdigit()) {
        return None;
    }
    Some(format!("0x{}", &body[24..]).to_ascii_lowercase())
}

fn transfer_log_address_ref(
    _chain_id: u64,
    address: &str,
    direction: AddressHitDirection,
    log: &Value,
) -> Option<AddressTxRef> {
    if log.get("removed").and_then(Value::as_bool).unwrap_or(false) {
        return None;
    }
    let topics = log.get("topics")?.as_array()?;
    if topics.len() < 3
        || !topics[0]
            .as_str()?
            .eq_ignore_ascii_case(ERC20_TRANSFER_TOPIC)
    {
        return None;
    }
    let from_address = address_from_topic(topics[1].as_str()?)?;
    let to_address = address_from_topic(topics[2].as_str()?)?;
    let tx_hash = log.get("transactionHash")?.as_str()?.to_ascii_lowercase();
    if tx_hash.len() != 66 {
        return None;
    }
    let block_number = parse_uint_hex(log.get("blockNumber")?.as_str()?)? as u64;
    let tx_index = parse_uint_hex(log.get("transactionIndex")?.as_str()?)? as u32;
    Some(AddressTxRef {
        address: address.to_ascii_lowercase(),
        tx_hash,
        block_number,
        tx_index,
        direction,
        from_address: Some(from_address),
        to_address: Some(to_address),
    })
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

fn evidence_transaction_hashes(metadata: &Value, limit: usize) -> Vec<String> {
    if metadata.get("history_mode").and_then(Value::as_str) != Some("evidence_transactions") {
        return Vec::new();
    }
    metadata
        .get("evidence_transaction_hashes")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_ascii_lowercase)
        .filter(|hash| {
            hash.len() == 66
                && hash.starts_with("0x")
                && hash[2..]
                    .chars()
                    .all(|character| character.is_ascii_hexdigit())
        })
        .take(limit.max(1))
        .collect()
}

fn should_evaluate_portfolio(metadata: &Value) -> bool {
    if metadata.get("source").and_then(Value::as_str) != Some("canonical_address_facts") {
        return true;
    }
    role_labels_from_metadata(metadata)
        .iter()
        .any(|role| matches!(role.as_str(), "portfolio_refresh" | "manual_lookup"))
}

fn should_backfill_address_history(address_type: &str) -> bool {
    !matches!(
        address_type,
        "token" | "router" | "protocol" | "dex_pool" | "router_candidate" | "protocol_candidate"
    )
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

fn role_assertion_source(metadata: &Value) -> &'static str {
    match metadata.get("source").and_then(Value::as_str) {
        Some("canonical_address_facts") => "canonical_address_facts",
        Some("onchain_factory_event" | "validated_dex_pool_inventory") => {
            "onchain_contract_discovery"
        }
        _ => "tx_analysis",
    }
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
                | "suspected_token"
                | "erc20_token"
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
        "token_metadata_probe" => ("onchain", 0.95, "onchain", "erc20_metadata_probe_v1"),
        "canonical_address_facts" => ("onchain", 0.95, "onchain", "address_fact_producer_v1"),
        "onchain_contract_discovery" => ("onchain", 0.90, "onchain", "canonical_factory_events_v1"),
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
    fn canonical_fact_jobs_skip_portfolio_work_without_an_explicit_refresh() {
        assert!(!should_evaluate_portfolio(&json!({
            "source": "canonical_address_facts",
            "roles": ["transaction_sender"]
        })));
        assert!(should_evaluate_portfolio(&json!({
            "source": "canonical_address_facts",
            "roles": ["transaction_sender", "portfolio_refresh"]
        })));
        assert!(should_evaluate_portfolio(&json!({
            "source": "manual_lookup",
            "roles": []
        })));
    }

    #[test]
    fn high_volume_protocol_addresses_use_scoped_projectors() {
        assert!(!should_backfill_address_history("token"));
        assert!(!should_backfill_address_history("router"));
        assert!(!should_backfill_address_history("protocol"));
        assert!(!should_backfill_address_history("dex_pool"));
        assert!(!should_backfill_address_history("router_candidate"));
        assert!(!should_backfill_address_history("protocol_candidate"));
        assert!(should_backfill_address_history("eoa"));
        assert!(should_backfill_address_history("contract"));
    }

    #[test]
    fn seed_history_is_bounded_to_explicit_valid_evidence_hashes() {
        let first = format!("0x{}", "1".repeat(64));
        let second = format!("0x{}", "a".repeat(64));
        let metadata = json!({
            "history_mode": "evidence_transactions",
            "evidence_transaction_hashes": [first, "0xnot-a-hash", second]
        });
        let hashes = evidence_transaction_hashes(&metadata, 1);
        assert_eq!(hashes, vec![format!("0x{}", "1".repeat(64))]);
        assert!(
            evidence_transaction_hashes(
                &json!({
                    "history_mode": "full",
                    "evidence_transaction_hashes": [format!("0x{}", "2".repeat(64))]
                }),
                8
            )
            .is_empty()
        );
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

    #[test]
    fn transfer_log_activity_recovers_a_recipient_that_is_not_a_tx_endpoint() {
        let observed = "0x0629da86af5a4ae1ba5e1589b13702558d0fb056";
        let sender = "0x8407699e359ae158bd7ec0668600cc19a79f17c3";
        let tx_hash = format!("0x{}", "a".repeat(64));
        let log = json!({
            "transactionHash": tx_hash,
            "blockNumber": "0x2fd8f6f",
            "transactionIndex": "0x57",
            "removed": false,
            "topics": [
                ERC20_TRANSFER_TOPIC,
                padded_address_topic(sender),
                padded_address_topic(observed),
            ]
        });
        let reference = transfer_log_address_ref(8453, observed, AddressHitDirection::To, &log)
            .expect("valid Transfer log should produce an activity reference");
        assert_eq!(reference.address, observed);
        assert_eq!(reference.from_address.as_deref(), Some(sender));
        assert_eq!(reference.to_address.as_deref(), Some(observed));
        assert_eq!(reference.block_number, 0x2fd8f6f);
    }
}
