use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use ledgerflow::{
    core::errors::{LedgerError, LedgerResult},
    domain::{
        entities::{AddressAnalysisCandidate, AddressLabelAssertion},
        interfaces::DynRepository,
    },
    ingestion::rpc::RpcClient,
    repositories::memory::InMemoryAnalysisRepository,
    services::address_intelligence::AddressIntelligenceService,
};
use serde_json::{Value, json};
use uuid::Uuid;

struct MockRpcClient;

#[async_trait]
impl RpcClient for MockRpcClient {
    async fn get_transaction(&self, _chain_id: u64, _tx_hash: &str) -> LedgerResult<Value> {
        Err(LedgerError::TransientIngestion("not used".to_owned()))
    }

    async fn get_receipt(&self, _chain_id: u64, _tx_hash: &str) -> LedgerResult<Value> {
        Err(LedgerError::TransientIngestion("not used".to_owned()))
    }

    async fn get_block(&self, _chain_id: u64, _block_number: u64) -> LedgerResult<Value> {
        Err(LedgerError::TransientIngestion("not used".to_owned()))
    }

    async fn get_trace(&self, _chain_id: u64, _tx_hash: &str) -> LedgerResult<Option<Value>> {
        Ok(None)
    }

    async fn get_code(
        &self,
        _chain_id: u64,
        _address: &str,
        _block_number: Option<u64>,
    ) -> LedgerResult<String> {
        Ok("0x60006000".to_owned())
    }

    async fn eth_call(
        &self,
        _chain_id: u64,
        _to: &str,
        data: &str,
        _block_number: Option<u64>,
    ) -> LedgerResult<String> {
        match data {
            "0x0dfe1681" => Ok(format!(
                "0x{:0>64}",
                "4200000000000000000000000000000000000006"
            )),
            "0xd21220a7" => Ok(format!(
                "0x{:0>64}",
                "1111111111111111111111111111111111111111"
            )),
            "0xddca3f43" => {
                Ok("0x00000000000000000000000000000000000000000000000000000000000001f4".to_owned())
            }
            "0xc45a0155" => Ok(format!(
                "0x{:0>64}",
                "2222222222222222222222222222222222222222"
            )),
            _ => Err(LedgerError::UnsupportedTrace("unsupported call".to_owned())),
        }
    }
}

#[tokio::test]
async fn address_worker_only_claims_jobs_for_its_configured_chain() {
    let repo: DynRepository = Arc::new(InMemoryAnalysisRepository::default());
    let base = "0x1111111111111111111111111111111111111111".to_owned();
    let robinhood = "0x2222222222222222222222222222222222222222".to_owned();
    repo.enqueue_address_analysis_candidates(
        8453,
        &[AddressAnalysisCandidate {
            address: base.clone(),
            priority: 1,
            metadata: json!({"source":"canonical_address_facts","roles":["transaction_sender"]}),
        }],
    )
    .await
    .unwrap();
    repo.enqueue_address_analysis_candidates(
        4663,
        &[AddressAnalysisCandidate {
            address: robinhood.clone(),
            priority: 10,
            metadata: json!({"source":"canonical_address_facts","roles":["transaction_sender"]}),
        }],
    )
    .await
    .unwrap();

    let service = AddressIntelligenceService::new(Arc::clone(&repo), Arc::new(MockRpcClient))
        .with_worker_chain_id(4663);
    let profile = service.process_next_job().await.unwrap().unwrap();
    assert_eq!(profile.chain_id, 4663);
    assert_eq!(profile.address, robinhood);

    let remaining = repo
        .claim_next_address_analysis_job()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(remaining.chain_id, 8453);
    assert_eq!(remaining.address, base);
}

#[tokio::test]
async fn address_worker_uses_curated_assertions_for_known_token_candidates() {
    let repo: DynRepository = Arc::new(InMemoryAnalysisRepository::default());
    let weth = "0x4200000000000000000000000000000000000006".to_owned();
    repo.upsert_address_label_assertions(&[
        curated_assertion(8453, &weth, "token", "contract_role"),
        curated_assertion(8453, &weth, "token:weth", "asset_identity"),
    ])
    .await
    .unwrap();
    repo.enqueue_address_analysis_candidates(
        8453,
        &[AddressAnalysisCandidate {
            address: weth.clone(),
            priority: 15,
            metadata: json!({
                "source": "tx_analysis",
                "tx_hash": "0xabc",
                "roles": ["erc20_token"]
            }),
        }],
    )
    .await
    .unwrap();

    let service = AddressIntelligenceService::new(Arc::clone(&repo), Arc::new(MockRpcClient));
    let profile = service.process_next_job().await.unwrap().unwrap();
    assert_eq!(profile.address, weth);
    assert_eq!(profile.address_type, "token");

    let labels = repo
        .list_address_labels(8453, &[weth.clone()])
        .await
        .unwrap();
    assert!(labels.iter().any(|label| label.label == "token:weth"));
    assert!(labels.iter().any(|label| label.label == "erc20_token"));
    assert!(
        repo.claim_next_address_analysis_job()
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn address_worker_defers_generic_jobs_above_the_seed_priority_ceiling() {
    let repo: DynRepository = Arc::new(InMemoryAnalysisRepository::default());
    let seed = "0x1111111111111111111111111111111111111111".to_owned();
    let generic = "0x2222222222222222222222222222222222222222".to_owned();
    repo.enqueue_address_analysis_candidates(
        4663,
        &[
            AddressAnalysisCandidate {
                address: generic.clone(),
                priority: 80,
                metadata: json!({"source":"canonical_address_facts","roles":["transaction_sender"]}),
            },
            AddressAnalysisCandidate {
                address: seed.clone(),
                priority: 10,
                metadata: json!({"source":"seed_intelligence","roles":["arbitrage_executor_candidate"]}),
            },
        ],
    )
    .await
    .unwrap();

    let service = AddressIntelligenceService::new(repo.clone(), Arc::new(MockRpcClient))
        .with_maximum_job_priority(39);
    let profile = service.process_next_job().await.unwrap().unwrap();
    assert_eq!(profile.address, seed);
    assert!(service.process_next_job().await.unwrap().is_none());

    let deferred = repo
        .claim_next_address_analysis_job()
        .await
        .unwrap()
        .expect("generic job remains available for an explicit future backfill");
    assert_eq!(deferred.address, generic);
    assert_eq!(deferred.priority, 80);
}

fn curated_assertion(
    chain_id: u64,
    address: &str,
    label: &str,
    dimension: &str,
) -> AddressLabelAssertion {
    let now = Utc::now();
    AddressLabelAssertion {
        assertion_id: Uuid::new_v4(),
        claim_fingerprint: format!("test:{chain_id}:{address}:{label}"),
        chain_id,
        address: address.to_owned(),
        label: label.to_owned(),
        dimension: dimension.to_owned(),
        exclusive_group: None,
        source: "test_curated_registry".to_owned(),
        source_kind: "curated".to_owned(),
        source_trust: 1.0,
        origin_kind: "curated_seed".to_owned(),
        method_version: "test_v1".to_owned(),
        dataset_key: Some("test-core".to_owned()),
        dataset_version: Some("1".to_owned()),
        assertion_value: true,
        confidence: 1.0,
        status: "active".to_owned(),
        valid_from_block: None,
        valid_to_block: None,
        observed_at: now,
        expires_at: None,
        evidence_json: json!({ "verification": "test" }),
        created_at: now,
        updated_at: now,
    }
}

#[tokio::test]
async fn address_worker_detects_dex_pool_shape() {
    let repo: DynRepository = Arc::new(InMemoryAnalysisRepository::default());
    let pool = "0x3333333333333333333333333333333333333333".to_owned();
    repo.enqueue_address_analysis_candidates(
        8453,
        &[AddressAnalysisCandidate {
            address: pool.clone(),
            priority: 15,
            metadata: json!({
                "source": "tx_analysis",
                "tx_hash": "0xabc",
                "roles": ["log_emitter"]
            }),
        }],
    )
    .await
    .unwrap();

    let service = AddressIntelligenceService::new(repo.clone(), Arc::new(MockRpcClient));
    let profile = service
        .process_next_job()
        .await
        .unwrap()
        .expect("profile should be produced");
    assert_eq!(profile.address_type, "dex_pool");
    assert_eq!(
        profile.metadata["protocol_intelligence"]["protocol_family"],
        "dex"
    );
    assert_eq!(
        profile.metadata["protocol_intelligence"]["protocol_name"],
        "uniswap_v3_compatible_pool"
    );
    let labels = repo.list_address_labels(8453, &[pool]).await.unwrap();
    assert!(labels.iter().any(|label| label.label == "dex_pool"));
}

#[tokio::test]
async fn suspected_token_role_requeues_a_completed_job_only_once() {
    let repo: DynRepository = Arc::new(InMemoryAnalysisRepository::default());
    let token = "0x4444444444444444444444444444444444444444".to_owned();
    repo.enqueue_address_analysis_candidates(
        4663,
        &[AddressAnalysisCandidate {
            address: token.clone(),
            priority: 30,
            metadata: json!({"source": "legacy_discovery", "roles": ["touched_contract"]}),
        }],
    )
    .await
    .unwrap();
    let first = repo
        .claim_next_address_analysis_job()
        .await
        .unwrap()
        .unwrap();
    repo.complete_address_analysis_job(first.chain_id, &first.address)
        .await
        .unwrap();

    let token_candidate = AddressAnalysisCandidate {
        address: token.clone(),
        priority: 30,
        metadata: json!({
            "source": "validated_dex_pool_inventory",
            "roles": ["suspected_token"]
        }),
    };
    assert_eq!(
        repo.enqueue_address_analysis_candidates(4663, &[token_candidate.clone()])
            .await
            .unwrap(),
        1
    );
    let second = repo
        .claim_next_address_analysis_job()
        .await
        .unwrap()
        .unwrap();
    repo.complete_address_analysis_job(second.chain_id, &second.address)
        .await
        .unwrap();

    assert_eq!(
        repo.enqueue_address_analysis_candidates(4663, &[token_candidate])
            .await
            .unwrap(),
        0
    );
    assert!(
        repo.claim_next_address_analysis_job()
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn canonical_fact_roles_are_merged_and_each_new_role_requeues_once() {
    let repo: DynRepository = Arc::new(InMemoryAnalysisRepository::default());
    let address = "0x5555555555555555555555555555555555555555".to_owned();
    let initial = AddressAnalysisCandidate {
        address: address.clone(),
        priority: 90,
        metadata: json!({
            "source": "canonical_address_facts",
            "roles": ["transaction_recipient"]
        }),
    };
    repo.enqueue_address_analysis_candidates(4663, &[initial.clone()])
        .await
        .unwrap();
    let first = repo
        .claim_next_address_analysis_job()
        .await
        .unwrap()
        .unwrap();
    repo.complete_address_analysis_job(first.chain_id, &first.address)
        .await
        .unwrap();
    assert_eq!(
        repo.enqueue_address_analysis_candidates(4663, &[initial])
            .await
            .unwrap(),
        0
    );

    let sender = AddressAnalysisCandidate {
        address: address.clone(),
        priority: 80,
        metadata: json!({
            "source": "canonical_address_facts",
            "roles": ["transaction_sender"]
        }),
    };
    assert_eq!(
        repo.enqueue_address_analysis_candidates(4663, &[sender.clone()])
            .await
            .unwrap(),
        1
    );
    let second = repo
        .claim_next_address_analysis_job()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        second.metadata["roles"],
        json!(["transaction_recipient", "transaction_sender"])
    );
    repo.complete_address_analysis_job(second.chain_id, &second.address)
        .await
        .unwrap();
    assert_eq!(
        repo.enqueue_address_analysis_candidates(4663, &[sender])
            .await
            .unwrap(),
        0
    );
}
