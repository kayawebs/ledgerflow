use std::{collections::BTreeMap, sync::Arc};

use chrono::Utc;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::{
    core::errors::LedgerResult,
    domain::{
        entities::{AddressAnalysisJob, AddressLabel, AddressProfile},
        interfaces::{ClusterTxSource, DynRepository},
        value_objects::AddressTxRef,
    },
    ingestion::rpc::RpcClient,
    services::analysis::AnalysisService,
};

#[derive(Clone)]
pub struct AddressIntelligenceService {
    repository: DynRepository,
    rpc_client: Arc<dyn RpcClient>,
    analysis_service: Option<Arc<AnalysisService>>,
    tx_source: Option<Arc<dyn ClusterTxSource>>,
    history_query_limit: u32,
}

impl AddressIntelligenceService {
    pub fn new(repository: DynRepository, rpc_client: Arc<dyn RpcClient>) -> Self {
        Self {
            repository,
            rpc_client,
            analysis_service: None,
            tx_source: None,
            history_query_limit: 1000,
        }
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
            Ok((profile, labels)) => {
                self.repository.upsert_address_profile(&profile).await?;
                self.repository.upsert_address_labels(&labels).await?;
                self.backfill_address_history(&job).await?;
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
    ) -> LedgerResult<(AddressProfile, Vec<AddressLabel>)> {
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
        let known = known_address_label(job.chain_id, &job.address);
        let role_labels = role_labels_from_metadata(&job.metadata);
        let address_type = if known
            .as_ref()
            .map(|label| matches!(label.address_type.as_str(), "token" | "router" | "protocol"))
            .unwrap_or(false)
        {
            known.as_ref().unwrap().address_type.clone()
        } else if role_labels.iter().any(|label| label == "erc20_token") {
            "token".to_owned()
        } else if has_code {
            "contract".to_owned()
        } else {
            "eoa".to_owned()
        };
        let confidence = match address_type.as_str() {
            "token" | "router" | "protocol" => 0.9,
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
            }),
            created_at: now,
            updated_at: now,
        };

        let mut labels = vec![label_for(
            job,
            &address_type,
            "address_worker",
            confidence,
            json!({ "reason": "address_type" }),
            now,
        )];
        for role_label in role_labels {
            labels.push(label_for(
                job,
                &role_label,
                "tx_analysis",
                0.75,
                json!({ "reason": "observed_role", "metadata": job.metadata }),
                now,
            ));
        }
        if let Some(known) = known {
            labels.push(label_for(
                job,
                &known.label,
                "builtin",
                known.confidence,
                json!({ "reason": "known_address" }),
                now,
            ));
        }
        Ok((profile, labels))
    }

    async fn backfill_address_history(&self, job: &AddressAnalysisJob) -> LedgerResult<()> {
        if !should_backfill_history(&job.metadata) {
            return Ok(());
        }
        let (Some(analysis_service), Some(tx_source)) =
            (self.analysis_service.as_ref(), self.tx_source.as_ref())
        else {
            return Ok(());
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
        Ok(())
    }
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

struct KnownAddressLabel {
    label: String,
    address_type: String,
    confidence: f32,
}

fn known_address_label(chain_id: u64, address: &str) -> Option<KnownAddressLabel> {
    if chain_id != 8453 {
        return None;
    }
    match address.to_ascii_lowercase().as_str() {
        "0x4200000000000000000000000000000000000006" => Some(KnownAddressLabel {
            label: "weth_token".to_owned(),
            address_type: "token".to_owned(),
            confidence: 0.99,
        }),
        "0x4752ba5dbc23f44d87826276bf6fd6b1c372ad24" => Some(KnownAddressLabel {
            label: "uniswap_v2_router".to_owned(),
            address_type: "router".to_owned(),
            confidence: 0.95,
        }),
        "0xbbbbbbbbbb9cc5e90e3b3af64bdaf62c37eeffcb" => Some(KnownAddressLabel {
            label: "morpho_blue".to_owned(),
            address_type: "protocol".to_owned(),
            confidence: 0.95,
        }),
        _ => None,
    }
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

fn label_for(
    job: &AddressAnalysisJob,
    label: &str,
    source: &str,
    confidence: f32,
    evidence_json: Value,
    now: chrono::DateTime<Utc>,
) -> AddressLabel {
    AddressLabel {
        chain_id: job.chain_id,
        address: job.address.to_ascii_lowercase(),
        label: label.to_owned(),
        source: source.to_owned(),
        confidence,
        evidence_json,
        created_at: now,
        updated_at: now,
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
