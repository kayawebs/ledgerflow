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

const DEFAULT_PER_ADDRESS_LIMIT: u32 = 100;
const MAX_PER_ADDRESS_LIMIT: u32 = 250;
const MAX_HOPS: u8 = 3;

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
    pub per_address_limit: Option<u32>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterDiscoveryNode {
    pub address: String,
    pub address_type: DiscoveredAddressType,
    pub first_seen_hop: u8,
    pub inbound_edge_count: u32,
    pub outbound_edge_count: u32,
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

    pub async fn discover(&self, request: ClusterDiscoveryRequest) -> LedgerResult<ClusterDiscoveryResponse> {
        let seed_address = normalize_address(&request.seed_address)?;
        let max_hops = request.max_hops.clamp(1, MAX_HOPS);
        let per_address_limit = request
            .per_address_limit
            .unwrap_or(DEFAULT_PER_ADDRESS_LIMIT)
            .clamp(1, MAX_PER_ADDRESS_LIMIT);

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
            },
        );

        while let Some((current_address, hop)) = frontier.pop_front() {
            if hop >= max_hops || !expanded.insert(current_address.clone()) {
                continue;
            }

            let refs = self
                .tx_source
                .query_address_refs(
                    request.chain_id,
                    &current_address,
                    per_address_limit,
                    None,
                    None,
                )
                .await?;
            if refs.len() as u32 >= per_address_limit {
                warnings.push(format!(
                    "address {} hit the per-address tx ref limit {}, so the graph is truncated",
                    current_address, per_address_limit
                ));
            }

            let mut unique_refs = BTreeMap::<String, AddressTxRef>::new();
            for tx_ref in refs {
                unique_refs.entry(tx_ref.tx_hash.clone()).or_insert(tx_ref);
            }

            for tx_ref in unique_refs.into_values() {
                let Some(from) = tx_ref
                    .from_address
                    .as_deref()
                    .map(normalize_address_lossy)
                else {
                    warnings.push(format!(
                        "tx {} is missing top-level from/to fields in the indexer result",
                        tx_ref.tx_hash
                    ));
                    continue;
                };
                let to = tx_ref.to_address.as_deref().map(normalize_address_lossy);
                let Some(counterparty) = resolve_counterparty(&current_address, &from, to.as_deref())
                else {
                    continue;
                };
                if counterparty == current_address {
                    continue;
                }

                let counterparty_type = self
                    .address_type(request.chain_id, &counterparty, &mut code_cache)
                    .await;
                let next_hop = hop.saturating_add(1);
                nodes
                    .entry(counterparty.clone())
                    .and_modify(|node| {
                        node.first_seen_hop = node.first_seen_hop.min(next_hop);
                    })
                    .or_insert_with(|| ClusterDiscoveryNode {
                        address: counterparty.clone(),
                        address_type: counterparty_type,
                        first_seen_hop: next_hop,
                        inbound_edge_count: 0,
                        outbound_edge_count: 0,
                    });

                let edge_key = (from.clone(), to.clone().unwrap_or_default());
                let edge = edges.entry(edge_key.clone()).or_insert_with(|| ClusterDiscoveryEdge {
                    from_address: edge_key.0.clone(),
                    to_address: edge_key.1.clone(),
                    tx_count: 0,
                    first_seen_hop: next_hop,
                    sample_tx_hashes: Vec::new(),
                    min_block_number: Some(tx_ref.block_number),
                    max_block_number: Some(tx_ref.block_number),
                });
                edge.tx_count += 1;
                edge.first_seen_hop = edge.first_seen_hop.min(next_hop);
                if edge.sample_tx_hashes.len() < 5
                    && !edge.sample_tx_hashes.iter().any(|item| item == &tx_ref.tx_hash)
                {
                    edge.sample_tx_hashes.push(tx_ref.tx_hash.clone());
                }
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
