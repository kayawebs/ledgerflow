use std::{collections::BTreeMap, sync::Arc};

use chrono::Utc;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::{
    core::errors::LedgerResult,
    domain::{
        entities::{AddressAnalysisJob, AddressLabel, AddressProfile, ProtocolAddressMetadata},
        interfaces::{ClusterTxSource, DynRepository},
        value_objects::AddressTxRef,
    },
    ingestion::rpc::RpcClient,
    services::analysis::AnalysisService,
};

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
            Ok((mut profile, labels, protocol_metadata)) => {
                if self.backfill_address_history(&job).await? {
                    profile.metadata = merge_metadata(
                        profile.metadata,
                        json!({
                            "address_history_backfilled_at": Utc::now(),
                            "address_history_backfill_source": "indexer",
                        }),
                    );
                }
                self.repository.upsert_address_profile(&profile).await?;
                self.repository.upsert_address_labels(&labels).await?;
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
        Vec<AddressLabel>,
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
        let known = known_address_label(job.chain_id, &job.address);
        let protocol_probe = if known.is_none() && has_code {
            self.probe_protocol_address(job.chain_id, &job.address)
                .await
        } else {
            None
        };
        let role_labels = role_labels_from_metadata(&job.metadata);
        let address_type = if known
            .as_ref()
            .map(|label| matches!(label.address_type.as_str(), "token" | "router" | "protocol"))
            .unwrap_or(false)
        {
            known.as_ref().unwrap().address_type.clone()
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
        if let Some(known) = known.as_ref() {
            labels.push(label_for(
                job,
                &known.label,
                "builtin",
                known.confidence,
                json!({ "reason": "known_address" }),
                now,
            ));
        }
        let mut protocol_metadata = protocol_probe
            .as_ref()
            .map(|probe| probe.to_metadata(job, now))
            .into_iter()
            .collect::<Vec<_>>();
        if let Some(known_metadata) = known
            .as_ref()
            .and_then(|known| known.to_protocol_metadata(job, now))
        {
            protocol_metadata.push(known_metadata);
        }
        if let Some(probe) = protocol_probe {
            labels.push(label_for(
                job,
                &probe.protocol_family,
                "protocol_probe",
                probe.confidence,
                probe.to_json(),
                now,
            ));
            labels.push(label_for(
                job,
                &probe.protocol_name,
                "protocol_probe",
                probe.confidence,
                probe.to_json(),
                now,
            ));
            labels.push(label_for(
                job,
                &probe.contract_role,
                "protocol_probe",
                probe.confidence,
                probe.to_json(),
                now,
            ));
        }
        Ok((profile, labels, protocol_metadata))
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

struct KnownAddressLabel {
    label: String,
    address_type: String,
    confidence: f32,
}

impl KnownAddressLabel {
    fn to_protocol_metadata(
        &self,
        job: &AddressAnalysisJob,
        now: chrono::DateTime<Utc>,
    ) -> Option<ProtocolAddressMetadata> {
        let (protocol_family, protocol_name, contract_role) = if self.label.starts_with("uniswap_")
        {
            ("dex", "uniswap", self.address_type.as_str())
        } else if self.label.starts_with("aerodrome_") {
            ("dex", "aerodrome", self.address_type.as_str())
        } else if self.label.starts_with("zeroex_") {
            ("dex", "zeroex", self.address_type.as_str())
        } else if self.label == "morpho_blue" {
            ("lending", "morpho_blue", self.address_type.as_str())
        } else {
            return None;
        };
        Some(ProtocolAddressMetadata {
            chain_id: job.chain_id,
            address: job.address.to_ascii_lowercase(),
            protocol_family: protocol_family.to_owned(),
            protocol_name: protocol_name.to_owned(),
            contract_role: contract_role.to_owned(),
            source: "builtin".to_owned(),
            adapter_version: "known_address_v1".to_owned(),
            confidence: self.confidence,
            metadata: json!({
                "label": self.label,
                "address_type": self.address_type,
            }),
            first_seen_at: job.first_seen_at,
            last_seen_at: now,
            updated_at: now,
        })
    }
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
        "0x2626664c2603336e57b271c5c0b26f421741e481" => Some(KnownAddressLabel {
            label: "uniswap_v3_swaprouter02".to_owned(),
            address_type: "router".to_owned(),
            confidence: 0.95,
        }),
        "0x6ff5693b99212da76ad316178a184ab56d299b43" => Some(KnownAddressLabel {
            label: "uniswap_universal_router".to_owned(),
            address_type: "router".to_owned(),
            confidence: 0.9,
        }),
        "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf" => Some(KnownAddressLabel {
            label: "aerodrome_router".to_owned(),
            address_type: "router".to_owned(),
            confidence: 0.9,
        }),
        "0x7747f8d2a76bd6345cc29622a946a929647f2359" => Some(KnownAddressLabel {
            label: "zeroex_base_settler".to_owned(),
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
