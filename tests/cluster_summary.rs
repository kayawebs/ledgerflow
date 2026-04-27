mod common;

use std::sync::Arc;

use serde_json::json;

use common::MockRpcClient;
use ledgerflow::{
    domain::{
        entities::ClusterAddress,
        enums::{AssetType, SpendCategory},
        interfaces::AnalysisRepository,
        value_objects::{AssetFlow, EvidenceRef},
    },
    ingestion::fetchers::TransactionBundleFetcher,
    plugins::loader::load_plugins,
    recognition::registry::RecognizerRegistry,
    repositories::memory::InMemoryAnalysisRepository,
    services::{analysis::AnalysisService, cluster::ClusterService},
};

#[tokio::test]
async fn cluster_summary_rolls_up_gas_and_external_flows() {
    let repository = Arc::new(InMemoryAnalysisRepository::default());
    let fetcher = TransactionBundleFetcher::new(MockRpcClient::default().boxed(), true);
    let mut registry = RecognizerRegistry::new();
    let plugin_context = load_plugins(&["sample_builtin_pack".to_owned()], &mut registry);
    let analysis_service = AnalysisService::new(
        fetcher,
        registry,
        plugin_context,
        repository.clone(),
        vec![
            "selector_presence".to_owned(),
            "erc20_approval".to_owned(),
            "erc20_transfer_flow".to_owned(),
            "native_transfer".to_owned(),
            "swap_flow".to_owned(),
        ],
    );
    let cluster_service = ClusterService::new(repository);

    let (analysis, _) = analysis_service
        .analyze_transaction(8453, "0xcluster1")
        .await
        .unwrap();
    assert_eq!(
        analysis.block_timestamp.unwrap().to_rfc3339(),
        "2024-04-16T10:14:24+00:00"
    );
    assert_eq!(
        analysis.accounting_outputs["gas_cost_raw"],
        json!("21000000000000")
    );

    let cluster = cluster_service
        .create_cluster(
            8453,
            "ops_team",
            "primary_searcher",
            Some("bot + operator".to_owned()),
            vec![ClusterAddress {
                address: "0x1111111111111111111111111111111111111111".to_owned(),
                label: Some("bot-1".to_owned()),
                role: Some("bot".to_owned()),
                metadata: json!({}),
            }],
        )
        .await
        .unwrap();

    let summary = cluster_service
        .get_cluster_summary(cluster.cluster_id, None, None)
        .await
        .unwrap();

    assert_eq!(summary.analyzed_tx_count, 1);
    assert_eq!(summary.total_gas_cost_raw, "21000000000000");
    assert_eq!(summary.total_external_native_outflow_raw, "1000");
    assert_eq!(summary.total_incoming_native_raw, "0");
    assert_eq!(summary.transactions[0].analysis_status, analysis.analysis_status);
    assert!(summary.transactions[0].trace_available);
    assert!(!summary.transactions[0].counterparties.is_empty());
    assert!(summary.breakdowns.iter().any(|item| {
        item.category == SpendCategory::GasBurn
            && item.asset_type == AssetType::Native
            && item.amount_raw == "21000000000000"
    }));
    assert!(summary.breakdowns.iter().any(|item| {
        item.category == SpendCategory::ExternalOutflow
            && item.asset_type == AssetType::Erc20
            && item.amount_raw == "100"
    }));
    assert!(summary.metadata["warnings"].is_array());
}

#[tokio::test]
async fn cluster_summary_tracks_internal_native_transfers() {
    let repository = Arc::new(InMemoryAnalysisRepository::default());
    let fetcher = TransactionBundleFetcher::new(MockRpcClient::default().boxed(), true);
    let mut registry = RecognizerRegistry::new();
    let plugin_context = load_plugins(&["sample_builtin_pack".to_owned()], &mut registry);
    let analysis_service = AnalysisService::new(
        fetcher,
        registry,
        plugin_context,
        repository.clone(),
        vec!["native_transfer".to_owned()],
    );
    let cluster_service = ClusterService::new(repository.clone());

    let (mut analysis, _) = analysis_service
        .analyze_transaction(8453, "0xcluster2")
        .await
        .unwrap();
    analysis.derived_flows.push(AssetFlow {
        flow_id: "0xcluster2:native:internal".to_owned(),
        asset_type: AssetType::Native,
        token_address: None,
        from_address: "0x1111111111111111111111111111111111111111".to_owned(),
        to_address: "0x5555555555555555555555555555555555555555".to_owned(),
        amount_raw: "50".to_owned(),
        direction_hint: None,
        evidence_refs: vec![EvidenceRef {
            kind: ledgerflow::domain::enums::EvidenceKind::Flow,
            reference: "synthetic".to_owned(),
            tx_hash: Some("0xcluster2".to_owned()),
            metadata: json!({}),
        }],
        derivation_method: "test_internal".to_owned(),
        confidence: 1.0,
        metadata: json!({}),
    });
    repository.save_analysis(&analysis).await.unwrap();

    let cluster = cluster_service
        .create_cluster(
            8453,
            "ops_team",
            "with_internal",
            None,
            vec![
                ClusterAddress {
                    address: "0x1111111111111111111111111111111111111111".to_owned(),
                    label: Some("bot".to_owned()),
                    role: Some("bot".to_owned()),
                    metadata: json!({}),
                },
                ClusterAddress {
                    address: "0x5555555555555555555555555555555555555555".to_owned(),
                    label: Some("treasury".to_owned()),
                    role: Some("eoa".to_owned()),
                    metadata: json!({}),
                },
            ],
        )
        .await
        .unwrap();

    let summary = cluster_service
        .get_cluster_summary(cluster.cluster_id, None, None)
        .await
        .unwrap();

    assert_eq!(summary.total_internal_native_transfer_raw, "50");
    assert!(summary.breakdowns.iter().any(|item| {
        item.category == SpendCategory::InternalTransfer
            && item.asset_type == AssetType::Native
            && item.amount_raw == "50"
    }));
}
