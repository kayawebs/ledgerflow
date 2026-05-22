use std::collections::BTreeMap;
use std::sync::Arc;

use serde_json::json;
use tracing::{info, warn};

use crate::accounting::rollups::AccountingService;
use crate::core::errors::{LedgerError, LedgerResult};
use crate::core::versioning::{RULESET_ID, VersionBundle};
use crate::domain::entities::{AddressAnalysisCandidate, EvidencePack, TransactionAnalysis};
use crate::domain::enums::{AnalysisStatus, AssetType, ValidationStatus};
use crate::domain::interfaces::{AnalysisRepository, FlowDeriver};
use crate::evidence_pack::builder::EvidencePackBuilder;
use crate::evidence_pack::hashing::build_input_digest;
use crate::flows::derivation::BasicFlowDeriver;
use crate::ingestion::fetchers::TransactionBundleFetcher;
use crate::normalization::service::EvidenceNormalizer;
use crate::plugins::loader::PluginContext;
use crate::recognition::registry::RecognizerRegistry;
use crate::scenarios::adapters::ScenarioAdapterService;
use crate::services::pricing::PriceService;
use crate::services::token_metadata::TokenMetadataService;

#[derive(Clone)]
pub struct AnalysisService {
    pub fetcher: TransactionBundleFetcher,
    pub normalizer: Arc<EvidenceNormalizer>,
    pub flow_deriver: Arc<dyn FlowDeriver>,
    pub recognizer_registry: Arc<RecognizerRegistry>,
    pub plugin_context: PluginContext,
    pub evidence_pack_builder: Arc<EvidencePackBuilder>,
    pub scenario_service: Arc<ScenarioAdapterService>,
    pub accounting_service: Arc<AccountingService>,
    pub repository: Arc<dyn AnalysisRepository>,
    pub token_metadata_service: Option<Arc<TokenMetadataService>>,
    pub price_service: Option<Arc<PriceService>>,
    pub enabled_recognizers: Vec<String>,
}

impl AnalysisService {
    pub fn new(
        fetcher: TransactionBundleFetcher,
        recognizer_registry: RecognizerRegistry,
        plugin_context: PluginContext,
        repository: Arc<dyn AnalysisRepository>,
        enabled_recognizers: Vec<String>,
    ) -> Self {
        Self {
            fetcher,
            normalizer: Arc::new(EvidenceNormalizer),
            flow_deriver: Arc::new(BasicFlowDeriver),
            recognizer_registry: Arc::new(recognizer_registry),
            plugin_context,
            evidence_pack_builder: Arc::new(EvidencePackBuilder),
            scenario_service: Arc::new(ScenarioAdapterService),
            accounting_service: Arc::new(AccountingService),
            repository,
            token_metadata_service: None,
            price_service: None,
            enabled_recognizers,
        }
    }

    pub fn with_token_metadata_service(
        mut self,
        token_metadata_service: Arc<TokenMetadataService>,
    ) -> Self {
        self.token_metadata_service = Some(token_metadata_service);
        self
    }

    pub fn with_price_service(mut self, price_service: Arc<PriceService>) -> Self {
        self.price_service = Some(price_service);
        self
    }

    pub async fn analyze_transaction(
        &self,
        chain_id: u64,
        tx_hash: &str,
    ) -> LedgerResult<(TransactionAnalysis, EvidencePack)> {
        self.analyze_transaction_with_recognizers(chain_id, tx_hash, None)
            .await
    }

    pub async fn analyze_transaction_with_recognizers(
        &self,
        chain_id: u64,
        tx_hash: &str,
        recognizer_override: Option<Vec<String>>,
    ) -> LedgerResult<(TransactionAnalysis, EvidencePack)> {
        info!(stage = "fetch", %tx_hash, "fetch_bundle");
        let bundle = self.fetcher.fetch(chain_id, tx_hash).await?;
        self.repository.save_transaction_bundle(&bundle).await?;

        let mut analysis = self.normalizer.normalize(&bundle);
        let selected_recognizers =
            recognizer_override.unwrap_or_else(|| self.enabled_recognizers.clone());
        let recognizer_versions = self
            .recognizer_registry
            .list_metadata()
            .into_iter()
            .filter(|meta| {
                selected_recognizers
                    .iter()
                    .any(|id| id == &meta.recognizer_id)
            })
            .map(|meta| (meta.recognizer_id, meta.version))
            .collect::<BTreeMap<_, _>>();
        analysis.analysis_version_metadata = VersionBundle {
            recognizer_versions,
            ..VersionBundle::default()
        };
        analysis.input_digest = build_input_digest(&bundle, &analysis.analysis_version_metadata);

        info!(stage = "flows", %tx_hash, "derive_flows");
        analysis.derived_flows = self.flow_deriver.derive(&bundle, &analysis);
        if let Some(token_metadata_service) = self.token_metadata_service.as_ref() {
            if let Err(err) = token_metadata_service
                .enrich_flows(chain_id, analysis.block_number, &mut analysis.derived_flows)
                .await
            {
                warn!(%tx_hash, error = %err, "token metadata enrichment failed");
            }
        }

        info!(stage = "recognition", %tx_hash, "run_recognizers");
        analysis.recognizer_results = self
            .recognizer_registry
            .execute(&mut analysis, &selected_recognizers)?;
        analysis.scenario_outputs = self.scenario_service.adapt(&analysis);
        analysis.accounting_outputs = self.accounting_service.summarize(&analysis);
        if let Some(price_service) = self.price_service.as_ref() {
            if let Err(err) = price_service.observe_analysis(&mut analysis).await {
                warn!(%tx_hash, error = %err, "price observation failed");
            }
        }

        let mut pack = self.evidence_pack_builder.build(&analysis);
        let mut validations = Vec::new();
        for validator in &self.plugin_context.validators {
            validations.extend(validator.validate(&bundle, &analysis, Some(&pack)));
        }
        analysis.validation_results = validations.clone();
        pack.validation_results = validations
            .iter()
            .map(|item| serde_json::to_value(item).unwrap_or_default())
            .collect();

        analysis.analysis_status = if validations
            .iter()
            .any(|item| item.status == ValidationStatus::Fail)
        {
            AnalysisStatus::Failed
        } else if validations
            .iter()
            .any(|item| item.status == ValidationStatus::Warn)
        {
            AnalysisStatus::Partial
        } else {
            AnalysisStatus::Completed
        };
        pack.metadata = json!({
            "analysis_id": analysis.analysis_id,
            "chain_id": analysis.chain_id,
            "analysis_status": analysis.analysis_status,
        });

        self.repository.save_analysis(&analysis).await?;
        self.repository.save_evidence_pack(&pack).await?;
        if let Err(err) = self.enqueue_address_intelligence(&analysis).await {
            warn!(%tx_hash, error = %err, "address intelligence enqueue failed");
        }
        Ok((analysis, pack))
    }

    async fn enqueue_address_intelligence(
        &self,
        analysis: &TransactionAnalysis,
    ) -> LedgerResult<()> {
        let candidates = collect_address_candidates(analysis);
        if candidates.is_empty() {
            return Ok(());
        }
        self.repository
            .enqueue_address_analysis_candidates(analysis.chain_id, &candidates)
            .await?;
        Ok(())
    }

    pub async fn get_analysis(
        &self,
        chain_id: u64,
        tx_hash: &str,
    ) -> LedgerResult<TransactionAnalysis> {
        self.repository
            .get_analysis(chain_id, tx_hash)
            .await?
            .ok_or_else(|| LedgerError::NotFound(format!("analysis not found for {tx_hash}")))
    }

    pub async fn get_evidence_pack(&self, pack_id: &str) -> LedgerResult<EvidencePack> {
        self.repository
            .get_evidence_pack(pack_id)
            .await?
            .ok_or_else(|| LedgerError::NotFound(format!("evidence pack {pack_id} not found")))
    }

    pub async fn get_evidence_pack_by_tx(
        &self,
        chain_id: u64,
        tx_hash: &str,
    ) -> LedgerResult<EvidencePack> {
        self.repository
            .get_evidence_pack_by_tx(chain_id, tx_hash)
            .await?
            .ok_or_else(|| LedgerError::NotFound(format!("evidence pack for {tx_hash} not found")))
    }

    pub fn ensure_ruleset_available(&self, ruleset_id: &str) -> LedgerResult<()> {
        if ruleset_id == RULESET_ID {
            Ok(())
        } else {
            Err(LedgerError::NotFound(format!(
                "ruleset {ruleset_id} is not installed"
            )))
        }
    }
}

fn collect_address_candidates(analysis: &TransactionAnalysis) -> Vec<AddressAnalysisCandidate> {
    let mut candidates = BTreeMap::<String, AddressAnalysisCandidate>::new();
    insert_candidate(
        &mut candidates,
        &analysis.from_address,
        20,
        json!({ "source": "tx_analysis", "tx_hash": analysis.tx_hash, "roles": ["tx_from"] }),
    );
    if let Some(to_address) = analysis.to_address.as_deref() {
        insert_candidate(
            &mut candidates,
            to_address,
            25,
            json!({ "source": "tx_analysis", "tx_hash": analysis.tx_hash, "roles": ["tx_to"] }),
        );
    }
    for participant in &analysis.normalized_execution_facts.participants {
        insert_candidate(
            &mut candidates,
            participant,
            60,
            json!({ "source": "tx_analysis", "tx_hash": analysis.tx_hash, "roles": ["participant"] }),
        );
    }
    for log in &analysis.normalized_execution_facts.log_summaries {
        if let Some(address) = log.get("address").and_then(|value| value.as_str()) {
            insert_candidate(
                &mut candidates,
                address,
                55,
                json!({ "source": "tx_analysis", "tx_hash": analysis.tx_hash, "roles": ["log_emitter"] }),
            );
        }
    }
    for flow in &analysis.derived_flows {
        insert_candidate(
            &mut candidates,
            &flow.from_address,
            40,
            json!({ "source": "tx_analysis", "tx_hash": analysis.tx_hash, "roles": ["flow_from"] }),
        );
        insert_candidate(
            &mut candidates,
            &flow.to_address,
            40,
            json!({ "source": "tx_analysis", "tx_hash": analysis.tx_hash, "roles": ["flow_to"] }),
        );
        if flow.asset_type == AssetType::Erc20 {
            if let Some(token_address) = flow.token_address.as_deref() {
                insert_candidate(
                    &mut candidates,
                    token_address,
                    15,
                    json!({ "source": "tx_analysis", "tx_hash": analysis.tx_hash, "roles": ["erc20_token"] }),
                );
            }
        }
    }
    candidates.into_values().collect()
}

fn insert_candidate(
    candidates: &mut BTreeMap<String, AddressAnalysisCandidate>,
    address: &str,
    priority: i32,
    metadata: serde_json::Value,
) {
    let Some(address) = normalize_address_candidate(address) else {
        return;
    };
    candidates
        .entry(address.clone())
        .and_modify(|candidate| {
            candidate.priority = candidate.priority.min(priority);
            candidate.metadata =
                merge_candidate_metadata(candidate.metadata.clone(), metadata.clone());
        })
        .or_insert(AddressAnalysisCandidate {
            address,
            priority,
            metadata,
        });
}

fn normalize_address_candidate(address: &str) -> Option<String> {
    let address = address.trim().to_ascii_lowercase();
    if address.len() == 42 && address.starts_with("0x") {
        Some(address)
    } else {
        None
    }
}

fn merge_candidate_metadata(
    left: serde_json::Value,
    right: serde_json::Value,
) -> serde_json::Value {
    match (left, right) {
        (serde_json::Value::Object(mut left), serde_json::Value::Object(right)) => {
            for (key, value) in right {
                left.insert(key, value);
            }
            serde_json::Value::Object(left)
        }
        (_, right) => right,
    }
}
