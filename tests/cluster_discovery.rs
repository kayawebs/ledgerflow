use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use serde_json::{Value, json};

use ledgerflow::{
    core::errors::LedgerResult,
    domain::{enums::AddressHitDirection, interfaces::ClusterTxSource, value_objects::AddressTxRef},
    ingestion::rpc::RpcClient,
    services::discovery::{ClusterDiscoveryRequest, ClusterDiscoveryService, DiscoveredAddressType},
};

#[derive(Clone)]
struct MockDiscoveryTxSource {
    refs: HashMap<String, Vec<AddressTxRef>>,
}

#[async_trait]
impl ClusterTxSource for MockDiscoveryTxSource {
    async fn get_last_indexed_block(&self, _chain_id: u64) -> LedgerResult<u64> {
        Ok(200)
    }

    async fn query_address_refs(
        &self,
        _chain_id: u64,
        address: &str,
        limit: u32,
        _from_block: Option<u64>,
        _to_block: Option<u64>,
    ) -> LedgerResult<Vec<AddressTxRef>> {
        Ok(self
            .refs
            .get(&address.to_ascii_lowercase())
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .take(limit as usize)
            .collect())
    }
}

#[derive(Clone)]
struct MockDiscoveryRpc {
    txs: HashMap<String, Value>,
    codes: HashMap<String, String>,
}

#[async_trait]
impl RpcClient for MockDiscoveryRpc {
    async fn get_transaction(&self, _chain_id: u64, tx_hash: &str) -> LedgerResult<Value> {
        Ok(self.txs.get(tx_hash).cloned().unwrap_or(Value::Null))
    }

    async fn get_receipt(&self, _chain_id: u64, _tx_hash: &str) -> LedgerResult<Value> {
        Ok(Value::Null)
    }

    async fn get_block(&self, _chain_id: u64, _block_number: u64) -> LedgerResult<Value> {
        Ok(Value::Null)
    }

    async fn get_trace(&self, _chain_id: u64, _tx_hash: &str) -> LedgerResult<Option<Value>> {
        Ok(None)
    }

    async fn get_code(
        &self,
        _chain_id: u64,
        address: &str,
        _block_number: Option<u64>,
    ) -> LedgerResult<String> {
        Ok(self
            .codes
            .get(&address.to_ascii_lowercase())
            .cloned()
            .unwrap_or_else(|| "0x".to_owned()))
    }
}

#[tokio::test]
async fn cluster_discovery_finds_neighbors_and_types_across_two_hops() {
    let service = ClusterDiscoveryService::new(
        Arc::new(MockDiscoveryTxSource {
            refs: HashMap::from([
                (
                    "0x1111111111111111111111111111111111111111".to_owned(),
                    vec![AddressTxRef {
                        address: "0x1111111111111111111111111111111111111111".to_owned(),
                        tx_hash: "0xtx-a".to_owned(),
                        block_number: 10,
                        tx_index: 0,
                        direction: AddressHitDirection::From,
                    }],
                ),
                (
                    "0x2222222222222222222222222222222222222222".to_owned(),
                    vec![AddressTxRef {
                        address: "0x2222222222222222222222222222222222222222".to_owned(),
                        tx_hash: "0xtx-b".to_owned(),
                        block_number: 12,
                        tx_index: 0,
                        direction: AddressHitDirection::From,
                    }],
                ),
            ]),
        }),
        Arc::new(MockDiscoveryRpc {
            txs: HashMap::from([
                (
                    "0xtx-a".to_owned(),
                    json!({
                        "from": "0x1111111111111111111111111111111111111111",
                        "to": "0x2222222222222222222222222222222222222222"
                    }),
                ),
                (
                    "0xtx-b".to_owned(),
                    json!({
                        "from": "0x2222222222222222222222222222222222222222",
                        "to": "0x3333333333333333333333333333333333333333"
                    }),
                ),
            ]),
            codes: HashMap::from([
                (
                    "0x1111111111111111111111111111111111111111".to_owned(),
                    "0x".to_owned(),
                ),
                (
                    "0x2222222222222222222222222222222222222222".to_owned(),
                    "0x60016000".to_owned(),
                ),
                (
                    "0x3333333333333333333333333333333333333333".to_owned(),
                    "0x".to_owned(),
                ),
            ]),
        }),
    );

    let result = service
        .discover(ClusterDiscoveryRequest {
            chain_id: 8453,
            seed_address: "0x1111111111111111111111111111111111111111".to_owned(),
            max_hops: 2,
            per_address_limit: Some(50),
        })
        .await
        .unwrap();

    assert_eq!(result.total_nodes, 3);
    assert_eq!(result.total_edges, 2);
    assert!(result
        .nodes
        .iter()
        .any(|node| node.address == "0x2222222222222222222222222222222222222222"
            && node.address_type == DiscoveredAddressType::Contract
            && node.first_seen_hop == 1));
    assert!(result
        .nodes
        .iter()
        .any(|node| node.address == "0x3333333333333333333333333333333333333333"
            && node.address_type == DiscoveredAddressType::Eoa
            && node.first_seen_hop == 2));
}
