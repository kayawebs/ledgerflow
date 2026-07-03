mod common;

use std::sync::Arc;

use chrono::Utc;
use serde_json::json;

use common::MockRpcClient;
use ledgerflow::{
    domain::{
        entities::{ClassifiedTransaction, ClusterAddress},
        enums::{AssetType, BehaviorTag, SpendCategory},
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
    assert_eq!(
        summary.transactions[0].analysis_status,
        analysis.analysis_status
    );
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

#[tokio::test]
async fn cluster_asset_changes_roll_up_daily_token_net_and_purposes() {
    let repository = Arc::new(InMemoryAnalysisRepository::default());
    let fetcher = TransactionBundleFetcher::new(MockRpcClient::default().boxed(), true);
    let mut registry = RecognizerRegistry::new();
    let plugin_context = load_plugins(&["sample_builtin_pack".to_owned()], &mut registry);
    let analysis_service = AnalysisService::new(
        fetcher,
        registry,
        plugin_context,
        repository.clone(),
        vec!["selector_presence".to_owned()],
    );
    let cluster_service = ClusterService::new(repository.clone());

    let (mut analysis, _) = analysis_service
        .analyze_transaction(8453, "0xasset-change")
        .await
        .unwrap();
    let token = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned();
    analysis.accounting_outputs = json!({ "gas_cost_raw": "10" });
    analysis.derived_flows = vec![
        AssetFlow {
            flow_id: "0xasset-change:token:out".to_owned(),
            asset_type: AssetType::Erc20,
            token_address: Some(token.clone()),
            from_address: "0x1111111111111111111111111111111111111111".to_owned(),
            to_address: "0x2222222222222222222222222222222222222222".to_owned(),
            amount_raw: "100".to_owned(),
            direction_hint: None,
            evidence_refs: Vec::new(),
            derivation_method: "test_asset_change".to_owned(),
            confidence: 1.0,
            metadata: json!({
                "token": {
                    "symbol": "ABC",
                    "name": "Alpha Bot Coin",
                    "address": token,
                    "decimals": 2,
                    "fetch_status": "ok"
                },
                "estimated_usd": {
                    "amount_decimal": "1",
                    "value_decimal": "2",
                    "price_decimal": "2",
                    "source": "tx_execution"
                }
            }),
        },
        AssetFlow {
            flow_id: "0xasset-change:token:in".to_owned(),
            asset_type: AssetType::Erc20,
            token_address: Some(token.clone()),
            from_address: "0x3333333333333333333333333333333333333333".to_owned(),
            to_address: "0x1111111111111111111111111111111111111111".to_owned(),
            amount_raw: "150".to_owned(),
            direction_hint: None,
            evidence_refs: Vec::new(),
            derivation_method: "test_asset_change".to_owned(),
            confidence: 1.0,
            metadata: json!({
                "token": {
                    "symbol": "ABC",
                    "name": "Alpha Bot Coin",
                    "address": token,
                    "decimals": 2,
                    "fetch_status": "ok"
                },
                "estimated_usd": {
                    "amount_decimal": "1.5",
                    "value_decimal": "3",
                    "price_decimal": "2",
                    "source": "tx_execution"
                }
            }),
        },
    ];
    repository.save_analysis(&analysis).await.unwrap();

    let cluster = cluster_service
        .create_cluster(
            8453,
            "ops_team",
            "asset_changes",
            None,
            vec![ClusterAddress {
                address: "0x1111111111111111111111111111111111111111".to_owned(),
                label: Some("bot".to_owned()),
                role: Some("bot".to_owned()),
                metadata: json!({}),
            }],
        )
        .await
        .unwrap();
    let now = Utc::now();
    repository
        .upsert_classified_transactions(&[ClassifiedTransaction {
            cluster_id: cluster.cluster_id,
            chain_id: 8453,
            tx_hash: analysis.tx_hash.clone(),
            address: analysis.from_address.clone(),
            block_number: analysis.block_number.unwrap_or_default(),
            timestamp: analysis.block_timestamp,
            behavior_tag: BehaviorTag::Arbitrage,
            protocol: Some("test_protocol".to_owned()),
            token_in: Some(token.clone()),
            token_out: Some(token.clone()),
            amount_in: Some("100".to_owned()),
            amount_out: Some("150".to_owned()),
            value_usd: None,
            gas_cost_usd: None,
            estimated_pnl_usd: None,
            raw_json: json!({}),
            created_at: now,
            updated_at: now,
        }])
        .await
        .unwrap();

    let report = cluster_service
        .get_cluster_asset_changes(cluster.cluster_id, None, None)
        .await
        .unwrap();

    let token_day = report
        .daily_changes
        .iter()
        .find(|row| {
            row.asset_type == AssetType::Erc20 && row.token_address.as_deref() == Some(&token)
        })
        .expect("expected token daily row");
    assert_eq!(token_day.incoming_raw, "150");
    assert_eq!(token_day.outgoing_raw, "100");
    assert_eq!(token_day.net_raw, "50");
    assert_eq!(token_day.token_symbol.as_deref(), Some("ABC"));
    assert_eq!(token_day.token_decimals, Some(2));
    assert_eq!(token_day.incoming_decimal.as_deref(), Some("1.5"));
    assert_eq!(token_day.outgoing_decimal.as_deref(), Some("1"));
    assert_eq!(token_day.net_decimal.as_deref(), Some("0.5"));
    assert_eq!(token_day.net_usd.as_deref(), Some("1"));
    assert_eq!(token_day.price_sources, vec!["tx_execution".to_owned()]);

    let native_day = report
        .daily_changes
        .iter()
        .find(|row| row.asset_type == AssetType::Native)
        .expect("expected native gas row");
    assert_eq!(native_day.gas_raw, "10");
    assert_eq!(native_day.net_raw, "-10");
    assert_eq!(
        native_day.gas_decimal.as_deref(),
        Some("0.00000000000000001")
    );

    assert!(report.purpose_breakdowns.iter().any(|row| {
        row.asset_type == AssetType::Erc20
            && row.purpose == "arbitrage_gain"
            && row.amount_raw == "150"
            && row.amount_decimal.as_deref() == Some("1.5")
            && row.amount_usd.as_deref() == Some("3")
    }));
    assert!(report.purpose_breakdowns.iter().any(|row| {
        row.asset_type == AssetType::Erc20
            && row.purpose == "arbitrage_spend"
            && row.amount_raw == "100"
            && row.amount_decimal.as_deref() == Some("1")
            && row.amount_usd.as_deref() == Some("2")
    }));
    assert!(report.purpose_breakdowns.iter().any(|row| {
        row.asset_type == AssetType::Native && row.purpose == "gas_cost" && row.amount_raw == "10"
    }));
}
