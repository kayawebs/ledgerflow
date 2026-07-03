use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::Arc,
};

use serde::{Deserialize, Serialize};

use crate::{
    core::errors::{LedgerError, LedgerResult},
    domain::{interfaces::ClusterTxSource, value_objects::AddressTxRef},
    ingestion::rpc::RpcClient,
};

const MAX_HOPS: u8 = 3;
const DISCOVERY_BATCH_LIMIT: u32 = 10_000;
const MAX_DISCOVERY_WINDOWS: usize = 5_000;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DiscoveredAddressType {
    Eoa,
    Contract,
    Unknown,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterDiscoveryRequest {
    pub chain_id: u64,
    pub seed_address: String,
    pub max_hops: u8,
    pub max_nodes: Option<u32>,
    pub max_windows: Option<usize>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterDiscoveryNode {
    pub address: String,
    pub address_type: DiscoveredAddressType,
    pub first_seen_hop: u8,
    pub inbound_edge_count: u32,
    pub outbound_edge_count: u32,
    pub role: String,
    pub relation_type: String,
    pub confidence: f32,
    pub evidence_tx_hashes: Vec<String>,
    pub reason: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterDiscoveryEdge {
    pub from_address: String,
    pub to_address: String,
    pub tx_count: u32,
    pub first_seen_hop: u8,
    pub sample_tx_hashes: Vec<String>,
    pub min_block_number: Option<u64>,
    pub max_block_number: Option<u64>,
    pub relation_type: String,
    pub confidence: f32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterDiscoveryResponse {
    pub seed_address: String,
    pub max_hops: u8,
    pub total_nodes: u32,
    pub total_edges: u32,
    pub nodes: Vec<ClusterDiscoveryNode>,
    pub edges: Vec<ClusterDiscoveryEdge>,
    pub warnings: Vec<String>,
}

#[derive(Clone)]
pub struct ClusterDiscoveryService {
    tx_source: Arc<dyn ClusterTxSource>,
    rpc_client: Arc<dyn RpcClient>,
}

impl ClusterDiscoveryService {
    pub fn new(tx_source: Arc<dyn ClusterTxSource>, rpc_client: Arc<dyn RpcClient>) -> Self {
        Self {
            tx_source,
            rpc_client,
        }
    }

    pub async fn discover(
        &self,
        request: ClusterDiscoveryRequest,
    ) -> LedgerResult<ClusterDiscoveryResponse> {
        let seed_address = normalize_address(&request.seed_address)?;
        let max_hops = request.max_hops.clamp(1, MAX_HOPS);
        let max_nodes = request.max_nodes.unwrap_or(u32::MAX).max(1);
        let max_windows = request.max_windows.unwrap_or(MAX_DISCOVERY_WINDOWS).max(1);

        let mut warnings = Vec::new();
        let mut frontier = VecDeque::from([(seed_address.clone(), 0u8)]);
        let mut expanded = BTreeSet::new();
        let mut nodes = BTreeMap::<String, ClusterDiscoveryNode>::new();
        let mut edges = BTreeMap::<(String, String), ClusterDiscoveryEdge>::new();
        let mut code_cache = BTreeMap::<String, DiscoveredAddressType>::new();

        let seed_type = self
            .address_type(request.chain_id, &seed_address, &mut code_cache)
            .await;
        nodes.insert(
            seed_address.clone(),
            ClusterDiscoveryNode {
                address: seed_address.clone(),
                address_type: seed_type,
                first_seen_hop: 0,
                inbound_edge_count: 0,
                outbound_edge_count: 0,
                role: "seed".to_owned(),
                relation_type: "seed".to_owned(),
                confidence: 1.0,
                evidence_tx_hashes: Vec::new(),
                reason: "user supplied seed address".to_owned(),
            },
        );

        while let Some((current_address, hop)) = frontier.pop_front() {
            if hop >= max_hops || !expanded.insert(current_address.clone()) {
                continue;
            }

            let refs = self
                .collect_full_history_refs(
                    request.chain_id,
                    &current_address,
                    max_windows,
                    &mut warnings,
                )
                .await?;

            let mut unique_refs = BTreeMap::<String, AddressTxRef>::new();
            for tx_ref in refs {
                unique_refs.entry(tx_ref.tx_hash.clone()).or_insert(tx_ref);
            }

            for tx_ref in unique_refs.into_values() {
                let Some(from) = tx_ref.from_address.as_deref().map(normalize_address_lossy) else {
                    warnings.push(format!(
                        "tx {} is missing top-level from/to fields in the indexer result",
                        tx_ref.tx_hash
                    ));
                    continue;
                };
                let to = tx_ref.to_address.as_deref().map(normalize_address_lossy);
                let Some(counterparty) =
                    resolve_counterparty(&current_address, &from, to.as_deref())
                else {
                    continue;
                };
                if counterparty == current_address {
                    continue;
                }
                if !nodes.contains_key(&counterparty) && nodes.len() as u32 >= max_nodes {
                    warnings.push(format!(
                        "discovery reached the max_nodes cap ({max_nodes}), so additional related addresses were skipped"
                    ));
                    continue;
                }

                let counterparty_type = self
                    .address_type(request.chain_id, &counterparty, &mut code_cache)
                    .await;
                let next_hop = hop.saturating_add(1);
                let relation = infer_relation(
                    &seed_address,
                    &current_address,
                    &counterparty,
                    &from,
                    to.as_deref(),
                    &counterparty_type,
                    next_hop,
                );
                nodes
                    .entry(counterparty.clone())
                    .and_modify(|node| {
                        node.first_seen_hop = node.first_seen_hop.min(next_hop);
                        node.confidence = node.confidence.max(relation.confidence);
                        if relation.confidence >= node.confidence {
                            node.role = relation.role.clone();
                            node.relation_type = relation.relation_type.clone();
                            node.reason = relation.reason.clone();
                        }
                        push_unique_sample(&mut node.evidence_tx_hashes, &tx_ref.tx_hash, 5);
                    })
                    .or_insert_with(|| ClusterDiscoveryNode {
                        address: counterparty.clone(),
                        address_type: counterparty_type.clone(),
                        first_seen_hop: next_hop,
                        inbound_edge_count: 0,
                        outbound_edge_count: 0,
                        role: relation.role.clone(),
                        relation_type: relation.relation_type.clone(),
                        confidence: relation.confidence,
                        evidence_tx_hashes: vec![tx_ref.tx_hash.clone()],
                        reason: relation.reason.clone(),
                    });

                let edge_key = (from.clone(), to.clone().unwrap_or_default());
                let edge = edges.entry(edge_key.clone()).or_insert_with(|| {
                    let edge_relation =
                        infer_edge_relation(&seed_address, &edge_key.0, &edge_key.1);
                    ClusterDiscoveryEdge {
                        from_address: edge_key.0.clone(),
                        to_address: edge_key.1.clone(),
                        tx_count: 0,
                        first_seen_hop: next_hop,
                        sample_tx_hashes: Vec::new(),
                        min_block_number: Some(tx_ref.block_number),
                        max_block_number: Some(tx_ref.block_number),
                        relation_type: edge_relation.0,
                        confidence: edge_relation.1,
                    }
                });
                edge.tx_count += 1;
                edge.first_seen_hop = edge.first_seen_hop.min(next_hop);
                push_unique_sample(&mut edge.sample_tx_hashes, &tx_ref.tx_hash, 5);
                edge.min_block_number = Some(
                    edge.min_block_number
                        .map(|value| value.min(tx_ref.block_number))
                        .unwrap_or(tx_ref.block_number),
                );
                edge.max_block_number = Some(
                    edge.max_block_number
                        .map(|value| value.max(tx_ref.block_number))
                        .unwrap_or(tx_ref.block_number),
                );
                edge.confidence = edge
                    .confidence
                    .max(edge_confidence(edge.tx_count, next_hop));

                if let Some(node) = nodes.get_mut(&from) {
                    node.outbound_edge_count += 1;
                }
                if let Some(destination) = to.as_ref().and_then(|address| nodes.get_mut(address)) {
                    destination.inbound_edge_count += 1;
                }

                if next_hop < max_hops && !expanded.contains(&counterparty) {
                    frontier.push_back((counterparty, next_hop));
                }
            }
        }

        Ok(ClusterDiscoveryResponse {
            seed_address,
            max_hops,
            total_nodes: nodes.len() as u32,
            total_edges: edges.len() as u32,
            nodes: nodes.into_values().collect(),
            edges: edges.into_values().collect(),
            warnings,
        })
    }

    async fn address_type(
        &self,
        chain_id: u64,
        address: &str,
        cache: &mut BTreeMap<String, DiscoveredAddressType>,
    ) -> DiscoveredAddressType {
        if let Some(existing) = cache.get(address) {
            return existing.clone();
        }
        let result = match self.rpc_client.get_code(chain_id, address, None).await {
            Ok(code) if is_empty_code(&code) => DiscoveredAddressType::Eoa,
            Ok(_) => DiscoveredAddressType::Contract,
            Err(_) => DiscoveredAddressType::Unknown,
        };
        cache.insert(address.to_owned(), result.clone());
        result
    }

    async fn collect_full_history_refs(
        &self,
        chain_id: u64,
        address: &str,
        max_windows: usize,
        warnings: &mut Vec<String>,
    ) -> LedgerResult<Vec<AddressTxRef>> {
        let mut collected = Vec::new();
        let mut seen_hashes = BTreeSet::<String>::new();
        let mut to_block = None;
        let mut windows = 0usize;

        loop {
            windows += 1;
            if windows > max_windows {
                warnings.push(format!(
                    "address {} exceeded the discovery pagination safety cap, so results may be incomplete",
                    address
                ));
                break;
            }

            let refs = self
                .tx_source
                .query_address_refs(chain_id, address, DISCOVERY_BATCH_LIMIT, None, to_block)
                .await?;
            if refs.is_empty() {
                break;
            }

            let batch_len = refs.len();
            let min_block = refs.iter().map(|tx_ref| tx_ref.block_number).min();
            for tx_ref in refs {
                if seen_hashes.insert(tx_ref.tx_hash.clone()) {
                    collected.push(tx_ref);
                }
            }

            if batch_len < DISCOVERY_BATCH_LIMIT as usize {
                break;
            }

            let Some(next_to_block) = min_block.and_then(|value| value.checked_sub(1)) else {
                break;
            };
            to_block = Some(next_to_block);
        }

        Ok(collected)
    }
}

#[derive(Clone)]
struct InferredRelation {
    role: String,
    relation_type: String,
    confidence: f32,
    reason: String,
}

fn infer_relation(
    seed_address: &str,
    current_address: &str,
    counterparty: &str,
    from: &str,
    to: Option<&str>,
    counterparty_type: &DiscoveredAddressType,
    hop: u8,
) -> InferredRelation {
    let counterparty_is_from = from.eq_ignore_ascii_case(counterparty);
    let counterparty_is_to = to
        .map(|value| value.eq_ignore_ascii_case(counterparty))
        .unwrap_or(false);
    let seed_is_to = to
        .map(|value| value.eq_ignore_ascii_case(seed_address))
        .unwrap_or(false);
    let seed_is_from = from.eq_ignore_ascii_case(seed_address);
    let current_is_seed = current_address.eq_ignore_ascii_case(seed_address);
    let base_confidence = (0.9 - (hop.saturating_sub(1) as f32 * 0.12)).max(0.45);

    if current_is_seed && counterparty_is_from && seed_is_to {
        return InferredRelation {
            role: "source_or_executor".to_owned(),
            relation_type: "inbound_to_seed".to_owned(),
            confidence: base_confidence,
            reason: "address sent funds or proceeds directly to the seed address".to_owned(),
        };
    }
    if current_is_seed && counterparty_is_to && seed_is_from {
        return InferredRelation {
            role: match counterparty_type {
                DiscoveredAddressType::Contract => "protocol_or_executor".to_owned(),
                _ => "recipient_or_funded_address".to_owned(),
            },
            relation_type: "outbound_from_seed".to_owned(),
            confidence: (base_confidence - 0.05).max(0.4),
            reason: "seed address sent funds or interacted with this address".to_owned(),
        };
    }
    if matches!(counterparty_type, DiscoveredAddressType::Contract) {
        return InferredRelation {
            role: "contract_counterparty".to_owned(),
            relation_type: "shared_contract_interaction".to_owned(),
            confidence: (base_confidence - 0.2).max(0.35),
            reason: "address appears as a contract counterparty in the discovered graph".to_owned(),
        };
    }
    InferredRelation {
        role: "counterparty".to_owned(),
        relation_type: "transaction_counterparty".to_owned(),
        confidence: (base_confidence - 0.15).max(0.35),
        reason: "address is connected by top-level transactions in the discovered graph".to_owned(),
    }
}

fn infer_edge_relation(seed_address: &str, from: &str, to: &str) -> (String, f32) {
    if to.eq_ignore_ascii_case(seed_address) {
        ("inbound_to_seed".to_owned(), 0.9)
    } else if from.eq_ignore_ascii_case(seed_address) {
        ("outbound_from_seed".to_owned(), 0.85)
    } else {
        ("transaction_edge".to_owned(), 0.65)
    }
}

fn edge_confidence(tx_count: u32, hop: u8) -> f32 {
    let count_bonus = (tx_count.min(10) as f32) * 0.02;
    (0.55 + count_bonus - (hop.saturating_sub(1) as f32 * 0.08)).clamp(0.35, 0.95)
}

fn push_unique_sample(items: &mut Vec<String>, value: &str, limit: usize) {
    if items.len() < limit && !items.iter().any(|item| item == value) {
        items.push(value.to_owned());
    }
}

fn resolve_counterparty(current_address: &str, from: &str, to: Option<&str>) -> Option<String> {
    if from.eq_ignore_ascii_case(current_address) {
        return to.map(normalize_address_lossy);
    }
    if let Some(to_address) = to
        && to_address.eq_ignore_ascii_case(current_address)
    {
        return Some(normalize_address_lossy(from));
    }
    None
}

fn normalize_address(address: &str) -> LedgerResult<String> {
    let normalized = normalize_address_lossy(address);
    if !normalized.starts_with("0x") || normalized.len() != 42 {
        return Err(LedgerError::AnalysisInvariant(format!(
            "invalid address: {address}"
        )));
    }
    Ok(normalized)
}

fn normalize_address_lossy(address: &str) -> String {
    address.trim().to_ascii_lowercase()
}

fn is_empty_code(code: &str) -> bool {
    let trimmed = code.trim();
    trimmed.eq_ignore_ascii_case("0x") || trimmed.eq_ignore_ascii_case("0x0")
}
