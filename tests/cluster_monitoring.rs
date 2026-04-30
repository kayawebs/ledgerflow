mod common;

use std::sync::Arc;

use chrono::{Duration, Utc};
use serde_json::json;

use common::MockRpcClient;
use ledgerflow::{
    domain::{
        entities::{ClassifiedTransaction, ClusterAddress},
        enums::{AlertType, BehaviorTag},
        interfaces::AnalysisRepository,
    },
    ingestion::fetchers::TransactionBundleFetcher,
    plugins::loader::load_plugins,
    recognition::registry::RecognizerRegistry,
    repositories::memory::InMemoryAnalysisRepository,
    services::{
        alerts::ClusterAlertService, analysis::AnalysisService, behavior::BehaviorClassifier,
        cluster::ClusterService, metrics::ClusterMetricsService,
    },
};

#[tokio::test]
async fn behavior_classifier_marks_swap_like_activity_as_position() {
    let repository = Arc::new(InMemoryAnalysisRepository::default());
    let fetcher = TransactionBundleFetcher::new(MockRpcClient::default().boxed(), true);
    let mut registry = RecognizerRegistry::new();
    let plugin_context = load_plugins(&["sample_builtin_pack".to_owned()], &mut registry);
    let analysis_service = Arc::new(AnalysisService::new(
        fetcher,
        registry,
        plugin_context,
        repository.clone(),
        vec!["selector_presence".to_owned(), "swap_flow".to_owned()],
    ));
    let cluster_service = ClusterService::new(repository);

    let cluster = cluster_service
        .create_cluster(
            8453,
            "ops_team",
            "position_cluster",
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

    let (analysis, _) = analysis_service
        .analyze_transaction(8453, "0xswap-position")
        .await
        .unwrap();
    let classified = BehaviorClassifier::default().classify(
        &cluster,
        "0x1111111111111111111111111111111111111111",
        &analysis,
    );

    assert_eq!(classified.behavior_tag, BehaviorTag::Position);
    assert!(classified.token_in.is_some() || classified.token_out.is_some());
}

#[tokio::test]
async fn behavior_classifier_marks_liquidation_when_trace_hits_known_selector() {
    let repository = Arc::new(InMemoryAnalysisRepository::default());
    let fetcher = TransactionBundleFetcher::new(MockRpcClient::default().boxed(), true);
    let mut registry = RecognizerRegistry::new();
    let plugin_context = load_plugins(&["sample_builtin_pack".to_owned()], &mut registry);
    let analysis_service = Arc::new(AnalysisService::new(
        fetcher,
        registry,
        plugin_context,
        repository.clone(),
        vec!["selector_presence".to_owned(), "liquidation_like_scenario".to_owned()],
    ));
    let cluster_service = ClusterService::new(repository);

    let cluster = cluster_service
        .create_cluster(
            8453,
            "ops_team",
            "liquidator_cluster",
            None,
            vec![ClusterAddress {
                address: "0x1111111111111111111111111111111111111111".to_owned(),
                label: Some("liquidator".to_owned()),
                role: Some("bot".to_owned()),
                metadata: json!({}),
            }],
        )
        .await
        .unwrap();

    let (analysis, _) = analysis_service
        .analyze_transaction(8453, "0xliquidation-case")
        .await
        .unwrap();
    let classified = BehaviorClassifier::default().classify(
        &cluster,
        "0x1111111111111111111111111111111111111111",
        &analysis,
    );

    assert_eq!(classified.behavior_tag, BehaviorTag::Liquidation);
    assert_eq!(classified.protocol.as_deref(), Some("aave"));
}

#[tokio::test]
async fn behavior_classifier_marks_morpho_liquidation_without_trace_when_event_matches() {
    let repository = Arc::new(InMemoryAnalysisRepository::default());
    let fetcher = TransactionBundleFetcher::new(
        MockRpcClient {
            include_trace: false,
            ..Default::default()
        }
        .boxed(),
        false,
    );
    let mut registry = RecognizerRegistry::new();
    let plugin_context = load_plugins(&["sample_builtin_pack".to_owned()], &mut registry);
    let analysis_service = Arc::new(AnalysisService::new(
        fetcher,
        registry,
        plugin_context,
        repository.clone(),
        vec!["selector_presence".to_owned(), "liquidation_like_scenario".to_owned()],
    ));
    let cluster_service = ClusterService::new(repository);

    let cluster = cluster_service
        .create_cluster(
            8453,
            "ops_team",
            "morpho_liquidator_cluster",
            None,
            vec![ClusterAddress {
                address: "0x1111111111111111111111111111111111111111".to_owned(),
                label: Some("liquidator".to_owned()),
                role: Some("bot".to_owned()),
                metadata: json!({}),
            }],
        )
        .await
        .unwrap();

    let (analysis, _) = analysis_service
        .analyze_transaction(8453, "0xmorpho-liquidation-case")
        .await
        .unwrap();
    let classified = BehaviorClassifier::default().classify(
        &cluster,
        "0x1111111111111111111111111111111111111111",
        &analysis,
    );

    assert_eq!(classified.behavior_tag, BehaviorTag::Liquidation);
    assert_eq!(classified.protocol.as_deref(), Some("morpho_blue"));
}

#[tokio::test]
async fn metrics_and_alerts_are_computed_from_recent_classified_transactions() {
    let repository = Arc::new(InMemoryAnalysisRepository::default());
    let cluster_service = ClusterService::new(repository.clone());
    let metrics_service = ClusterMetricsService::new(repository.clone());
    let alert_service = ClusterAlertService::new(repository.clone());

    let cluster = cluster_service
        .create_cluster(
            8453,
            "ops_team",
            "monitored_cluster",
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
    let txs = vec![
        ClassifiedTransaction {
            cluster_id: cluster.cluster_id,
            chain_id: 8453,
            tx_hash: "0xmetric-a".to_owned(),
            address: "0x1111111111111111111111111111111111111111".to_owned(),
            block_number: 100,
            timestamp: Some(now - Duration::minutes(10)),
            behavior_tag: BehaviorTag::Arbitrage,
            protocol: Some("swap_like_activity".to_owned()),
            token_in: Some("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned()),
            token_out: Some("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned()),
            amount_in: Some("1000000000000000000".to_owned()),
            amount_out: Some("1010000000000000000".to_owned()),
            value_usd: Some(250_000.0),
            gas_cost_usd: Some(30.0),
            estimated_pnl_usd: Some(25_000.0),
            raw_json: json!({
                "external_inflow_usd": 140000.0,
                "external_outflow_usd": 90000.0,
            }),
            created_at: now,
            updated_at: now,
        },
        ClassifiedTransaction {
            cluster_id: cluster.cluster_id,
            chain_id: 8453,
            tx_hash: "0xmetric-b".to_owned(),
            address: "0x1111111111111111111111111111111111111111".to_owned(),
            block_number: 101,
            timestamp: Some(now - Duration::minutes(5)),
            behavior_tag: BehaviorTag::Arbitrage,
            protocol: Some("swap_like_activity".to_owned()),
            token_in: Some("0xcccccccccccccccccccccccccccccccccccccccc".to_owned()),
            token_out: Some("0xdddddddddddddddddddddddddddddddddddddddd".to_owned()),
            amount_in: Some("500000000000000000".to_owned()),
            amount_out: Some("530000000000000000".to_owned()),
            value_usd: Some(180_000.0),
            gas_cost_usd: Some(25.0),
            estimated_pnl_usd: Some(18_000.0),
            raw_json: json!({
                "external_inflow_usd": 120000.0,
                "external_outflow_usd": 70000.0,
            }),
            created_at: now,
            updated_at: now,
        },
    ];

    repository.upsert_classified_transactions(&txs).await.unwrap();

    let snapshots = metrics_service
        .refresh_cluster_metrics(cluster.cluster_id)
        .await
        .unwrap();
    assert_eq!(snapshots.len(), 3);
    let one_hour = snapshots
        .iter()
        .find(|item| item.window == ledgerflow::domain::enums::MetricsWindow::OneHour)
        .unwrap();
    assert_eq!(one_hour.tx_count, 2);
    assert!(one_hour.roi.is_some());
    assert_eq!(one_hour.active_addresses_count, 1);

    let alerts = alert_service
        .refresh_cluster_alerts(cluster.cluster_id)
        .await
        .unwrap();
    assert!(alerts.iter().any(|item| item.alert_type == AlertType::RoiAnomaly));
    assert!(alerts.iter().any(|item| item.alert_type == AlertType::ExternalInflow));
}
