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
        alerts::ClusterAlertService,
        analysis::AnalysisService,
        behavior::BehaviorClassifier,
        cluster::ClusterService,
        metrics::ClusterMetricsService,
        report_agent::{
            ReportAgentFilters, ReportAgentQuery, ReportAgentSection, ReportAgentService,
        },
        strategy_analysis::ClusterStrategyAnalysisService,
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
    assert_action_kind(&analysis.scenario_outputs, "swap");
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
        vec![
            "selector_presence".to_owned(),
            "liquidation_like_scenario".to_owned(),
        ],
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
async fn liquidation_recognizer_does_not_label_plain_swap_flow() {
    let repository = Arc::new(InMemoryAnalysisRepository::default());
    let fetcher = TransactionBundleFetcher::new(MockRpcClient::default().boxed(), true);
    let mut registry = RecognizerRegistry::new();
    let plugin_context = load_plugins(&["sample_builtin_pack".to_owned()], &mut registry);
    let analysis_service = Arc::new(AnalysisService::new(
        fetcher,
        registry,
        plugin_context,
        repository,
        vec![
            "selector_presence".to_owned(),
            "liquidation_like_scenario".to_owned(),
        ],
    ));

    let (analysis, _) = analysis_service
        .analyze_transaction(8453, "0xswap-position")
        .await
        .unwrap();

    assert!(
        !analysis
            .recognizer_results
            .iter()
            .any(|result| result.label == "liquidation_like_activity")
    );
    assert!(
        analysis
            .scenario_outputs
            .get("storyline")
            .map(|value| value.is_null())
            .unwrap_or(true)
    );
}

#[tokio::test]
async fn executor_mediated_swap_produces_swap_action_and_storyline() {
    let repository = Arc::new(InMemoryAnalysisRepository::default());
    let fetcher = TransactionBundleFetcher::new(MockRpcClient::default().boxed(), true);
    let mut registry = RecognizerRegistry::new();
    let plugin_context = load_plugins(&["sample_builtin_pack".to_owned()], &mut registry);
    let analysis_service = Arc::new(AnalysisService::new(
        fetcher,
        registry,
        plugin_context,
        repository,
        vec![
            "selector_presence".to_owned(),
            "erc20_transfer_flow".to_owned(),
            "approval_swap_scenario".to_owned(),
        ],
    ));

    let (analysis, _) = analysis_service
        .analyze_transaction(8453, "0xexecutor-swap-case")
        .await
        .unwrap();

    assert_action_kind(&analysis.scenario_outputs, "swap");
    let storyline = analysis
        .scenario_outputs
        .get("storyline")
        .expect("swap storyline should exist");
    assert_eq!(
        storyline.get("kind").and_then(|value| value.as_str()),
        Some("swap")
    );
    assert!(
        storyline
            .get("steps")
            .and_then(|value| value.as_array())
            .map(|items| items.len() >= 2)
            .unwrap_or(false)
    );
}

#[tokio::test]
async fn executor_mediated_multi_leg_cycle_is_arbitrage_not_transfer() {
    let repository = Arc::new(InMemoryAnalysisRepository::default());
    let fetcher = TransactionBundleFetcher::new(MockRpcClient::default().boxed(), true);
    let mut registry = RecognizerRegistry::new();
    let plugin_context = load_plugins(&["sample_builtin_pack".to_owned()], &mut registry);
    let analysis_service = Arc::new(AnalysisService::new(
        fetcher,
        registry,
        plugin_context,
        repository,
        vec![
            "selector_presence".to_owned(),
            "erc20_transfer_flow".to_owned(),
            "arbitrage_like_scenario".to_owned(),
        ],
    ));

    let (analysis, _) = analysis_service
        .analyze_transaction(8453, "0xexecutor-arbitrage-case")
        .await
        .unwrap();

    assert!(
        analysis
            .recognizer_results
            .iter()
            .any(|result| result.label == "arbitrage_like_activity")
    );
    let storyline = analysis
        .scenario_outputs
        .get("storyline")
        .expect("arbitrage storyline should exist");
    assert_eq!(
        storyline.get("kind").and_then(|value| value.as_str()),
        Some("arbitrage")
    );
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
        vec![
            "selector_presence".to_owned(),
            "liquidation_like_scenario".to_owned(),
        ],
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
    assert_action_kind(&analysis.scenario_outputs, "liquidation");
}

#[tokio::test]
async fn scenario_outputs_include_liquidation_storyline_for_morpho_case() {
    let repository = Arc::new(InMemoryAnalysisRepository::default());
    let fetcher = TransactionBundleFetcher::new(MockRpcClient::default().boxed(), true);
    let mut registry = RecognizerRegistry::new();
    let plugin_context = load_plugins(&["sample_builtin_pack".to_owned()], &mut registry);
    let analysis_service = Arc::new(AnalysisService::new(
        fetcher,
        registry,
        plugin_context,
        repository,
        vec![
            "selector_presence".to_owned(),
            "liquidation_like_scenario".to_owned(),
        ],
    ));

    let (analysis, _) = analysis_service
        .analyze_transaction(8453, "0xmorpho-liquidation-case")
        .await
        .unwrap();

    let storyline = analysis
        .scenario_outputs
        .get("storyline")
        .expect("storyline should exist for morpho liquidation");

    assert_eq!(
        storyline.get("kind").and_then(|value| value.as_str()),
        Some("liquidation")
    );
    assert!(
        storyline
            .get("headline")
            .and_then(|value| value.as_str())
            .map(|value| value.contains("morpho_blue liquidation"))
            .unwrap_or(false)
    );
    assert_eq!(
        analysis
            .scenario_outputs
            .get("protocol")
            .and_then(|value| value.as_str()),
        Some("morpho_blue")
    );
    assert!(
        analysis
            .scenario_outputs
            .get("behavior_fingerprint")
            .and_then(|value| value.as_str())
            .is_some()
    );
    assert!(
        storyline
            .get("call_path")
            .and_then(|value| value.as_array())
            .map(|items| !items.is_empty())
            .unwrap_or(false)
    );
    assert!(
        storyline
            .get("funding_sources")
            .and_then(|value| value.as_array())
            .map(|items| !items.is_empty())
            .unwrap_or(false)
    );
    assert!(
        storyline
            .get("net_effect")
            .and_then(|value| value.get("gained"))
            .and_then(|value| value.as_array())
            .map(|items| !items.is_empty())
            .unwrap_or(false)
    );
    assert!(
        storyline
            .get("net_effect")
            .and_then(|value| value.get("spent"))
            .and_then(|value| value.as_array())
            .map(|items| !items.is_empty())
            .unwrap_or(false)
    );
    assert!(
        storyline
            .get("steps")
            .and_then(|value| value.as_array())
            .map(|items| !items.is_empty())
            .unwrap_or(false)
    );
    assert!(
        storyline
            .get("repay_legs")
            .and_then(|value| value.as_array())
            .map(|items| !items.is_empty())
            .unwrap_or(false)
    );
    assert!(
        storyline
            .get("collateral_receipts")
            .and_then(|value| value.as_array())
            .map(|items| !items.is_empty())
            .unwrap_or(false)
    );
}

#[tokio::test]
async fn morpho_liquidation_case_derives_erc20_flows() {
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
        repository,
        vec![
            "selector_presence".to_owned(),
            "liquidation_like_scenario".to_owned(),
        ],
    ));

    let (analysis, _) = analysis_service
        .analyze_transaction(8453, "0xmorpho-liquidation-case")
        .await
        .unwrap();

    assert!(
        !analysis.derived_flows.is_empty(),
        "expected ERC20 transfer logs to produce derived flows"
    );
    assert!(
        analysis
            .derived_flows
            .iter()
            .any(|flow| flow.token_address.as_deref()
                == Some("0x4200000000000000000000000000000000000006")),
        "expected WETH-like transfer flow to be derived"
    );
}

#[tokio::test]
async fn flashloan_funding_source_is_labeled_in_storyline() {
    let repository = Arc::new(InMemoryAnalysisRepository::default());
    let fetcher = TransactionBundleFetcher::new(MockRpcClient::default().boxed(), true);
    let mut registry = RecognizerRegistry::new();
    let plugin_context = load_plugins(&["sample_builtin_pack".to_owned()], &mut registry);
    let analysis_service = Arc::new(AnalysisService::new(
        fetcher,
        registry,
        plugin_context,
        repository,
        vec![
            "selector_presence".to_owned(),
            "liquidation_like_scenario".to_owned(),
        ],
    ));

    let (analysis, _) = analysis_service
        .analyze_transaction(8453, "0xflashloan-liquidation-case")
        .await
        .unwrap();

    let storyline = analysis
        .scenario_outputs
        .get("storyline")
        .expect("storyline should exist");
    let hinted_funding = storyline
        .get("funding_sources")
        .and_then(|value| value.as_array())
        .and_then(|items| {
            items.iter().find(|item| {
                item.get("source_hint")
                    .and_then(|value| value.get("protocol"))
                    .and_then(|value| value.as_str())
                    .is_some()
            })
        })
        .expect("funding source should exist");
    assert_eq!(
        hinted_funding
            .get("source_hint")
            .and_then(|value| value.get("protocol"))
            .and_then(|value| value.as_str()),
        Some("aave_v3")
    );
    assert_eq!(
        hinted_funding
            .get("source_hint")
            .and_then(|value| value.get("label"))
            .and_then(|value| value.as_str()),
        Some("Aave flash loan")
    );
    assert_action_kind(&analysis.scenario_outputs, "flashloan");
}

fn assert_action_kind(scenario_outputs: &serde_json::Value, expected_kind: &str) {
    let actions = scenario_outputs
        .get("actions")
        .and_then(|value| value.as_array())
        .expect("actions should be present");
    assert!(
        actions.iter().any(|action| {
            action.get("kind").and_then(|value| value.as_str()) == Some(expected_kind)
        }),
        "expected decoded action kind {expected_kind}, got {actions:?}"
    );
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

    repository
        .upsert_classified_transactions(&txs)
        .await
        .unwrap();

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
    assert!(
        alerts
            .iter()
            .any(|item| item.alert_type == AlertType::RoiAnomaly)
    );
    assert!(
        alerts
            .iter()
            .any(|item| item.alert_type == AlertType::ExternalInflow)
    );
}

#[tokio::test]
async fn strategy_analysis_rolls_up_cluster_arbitrage_behavior() {
    let repository = Arc::new(InMemoryAnalysisRepository::default());
    let cluster_service = ClusterService::new(repository.clone());
    let strategy_service = ClusterStrategyAnalysisService::new(repository.clone());

    let cluster = cluster_service
        .create_cluster(
            8453,
            "ops_team",
            "known_searcher",
            None,
            vec![
                ClusterAddress {
                    address: "0x1111111111111111111111111111111111111111".to_owned(),
                    label: Some("bot".to_owned()),
                    role: Some("executor".to_owned()),
                    metadata: json!({}),
                },
                ClusterAddress {
                    address: "0x2222222222222222222222222222222222222222".to_owned(),
                    label: Some("collector".to_owned()),
                    role: Some("collector".to_owned()),
                    metadata: json!({}),
                },
            ],
        )
        .await
        .unwrap();

    let now = Utc::now();
    let txs = vec![
        ClassifiedTransaction {
            cluster_id: cluster.cluster_id,
            chain_id: 8453,
            tx_hash: "0xarb-success".to_owned(),
            address: "0x1111111111111111111111111111111111111111".to_owned(),
            block_number: 200,
            timestamp: Some(now - Duration::minutes(3)),
            behavior_tag: BehaviorTag::Arbitrage,
            protocol: Some("uniswap_v3".to_owned()),
            token_in: Some("0x4200000000000000000000000000000000000006".to_owned()),
            token_out: Some("0x833589fcd6edb6e08f4c7c32d4f71b54bda02913".to_owned()),
            amount_in: Some("1000000000000000000".to_owned()),
            amount_out: Some("2500000000".to_owned()),
            value_usd: Some(2500.0),
            gas_cost_usd: Some(3.0),
            estimated_pnl_usd: Some(12.0),
            raw_json: json!({}),
            created_at: now,
            updated_at: now,
        },
        ClassifiedTransaction {
            cluster_id: cluster.cluster_id,
            chain_id: 8453,
            tx_hash: "0xarb-failed".to_owned(),
            address: "0x1111111111111111111111111111111111111111".to_owned(),
            block_number: 201,
            timestamp: Some(now - Duration::minutes(2)),
            behavior_tag: BehaviorTag::Arbitrage,
            protocol: Some("uniswap_v3".to_owned()),
            token_in: Some("0x4200000000000000000000000000000000000006".to_owned()),
            token_out: Some("0x833589fcd6edb6e08f4c7c32d4f71b54bda02913".to_owned()),
            amount_in: Some("1000000000000000000".to_owned()),
            amount_out: Some("2490000000".to_owned()),
            value_usd: Some(2490.0),
            gas_cost_usd: Some(4.0),
            estimated_pnl_usd: Some(-2.0),
            raw_json: json!({}),
            created_at: now,
            updated_at: now,
        },
        ClassifiedTransaction {
            cluster_id: cluster.cluster_id,
            chain_id: 8453,
            tx_hash: "0xliquidation".to_owned(),
            address: "0x2222222222222222222222222222222222222222".to_owned(),
            block_number: 202,
            timestamp: Some(now - Duration::minutes(1)),
            behavior_tag: BehaviorTag::Liquidation,
            protocol: Some("aave".to_owned()),
            token_in: None,
            token_out: None,
            amount_in: None,
            amount_out: None,
            value_usd: Some(5000.0),
            gas_cost_usd: Some(5.0),
            estimated_pnl_usd: Some(40.0),
            raw_json: json!({}),
            created_at: now,
            updated_at: now,
        },
    ];

    repository
        .upsert_classified_transactions(&txs)
        .await
        .unwrap();

    let analysis = strategy_service
        .analyze_cluster(cluster.cluster_id, None, None)
        .await
        .unwrap();

    assert_eq!(analysis.tx_count, 3);
    assert_eq!(analysis.arbitrage_tx_count, 2);
    assert_eq!(analysis.successful_arbitrage_tx_count, 1);
    assert_eq!(analysis.failed_arbitrage_tx_count, 1);
    assert_eq!(analysis.arbitrage_success_rate, Some(0.5));
    assert_eq!(analysis.total_estimated_pnl_usd, Some(50.0));
    assert_eq!(analysis.total_gas_cost_usd, Some(12.0));

    let route = analysis.top_routes.first().unwrap();
    // Token symbols are resolved from persisted metadata by the application layer.
    // This repository-only fixture intentionally has no token registry.
    assert_eq!(route.route, "0x833589...2913 -> 0x420000...0006");
    assert_eq!(route.tx_count, 2);
    assert_eq!(route.success_count, 1);
    assert_eq!(route.success_rate, Some(0.5));

    let uniswap = analysis
        .top_protocols
        .iter()
        .find(|item| item.protocol == "uniswap_v3")
        .unwrap();
    assert_eq!(uniswap.tx_count, 2);
    assert_eq!(uniswap.arbitrage_tx_count, 2);
    assert_eq!(uniswap.success_count, 1);

    let executor = analysis
        .address_roles
        .iter()
        .find(|item| item.address == "0x1111111111111111111111111111111111111111")
        .unwrap();
    assert_eq!(executor.configured_role.as_deref(), Some("executor"));
    assert_eq!(executor.inferred_role, "arbitrage_executor");
    assert_eq!(executor.tx_count, 2);

    let arbitrage = analysis
        .strategy_breakdown
        .iter()
        .find(|item| item.strategy_type == "arbitrage")
        .unwrap();
    assert_eq!(arbitrage.tx_count, 2);
    assert_eq!(arbitrage.success_count, 1);

    assert_eq!(analysis.behavior_profile.primary_behavior, "arbitrage");
    assert_eq!(analysis.behavior_profile.primary_behavior_tx_count, 2);
    assert_eq!(
        analysis.behavior_profile.primary_behavior_tx_share,
        Some(2.0 / 3.0)
    );
    assert_eq!(analysis.behavior_profile.active_addresses_count, 2);

    assert_eq!(analysis.performance.tx_count, 3);
    assert_eq!(analysis.performance.success_count, 2);
    assert_eq!(analysis.performance.failed_count, 1);
    assert_eq!(analysis.performance.success_rate, Some(2.0 / 3.0));
    assert_eq!(analysis.performance.estimated_pnl_usd, Some(50.0));
    assert_eq!(analysis.performance.gas_cost_usd, Some(12.0));
    assert_eq!(analysis.performance.pnl_to_gas_ratio, Some(50.0 / 12.0));

    let arbitrage_report = analysis.arbitrage_report.as_ref().unwrap();
    assert_eq!(arbitrage_report.strategy_type, "arbitrage");
    assert_eq!(arbitrage_report.tx_count, 2);
    assert_eq!(arbitrage_report.success_count, 1);
    assert_eq!(arbitrage_report.failed_count, 1);
    assert_eq!(arbitrage_report.success_rate, Some(0.5));
    assert_eq!(arbitrage_report.estimated_pnl_usd, Some(10.0));
    assert_eq!(arbitrage_report.gas_cost_usd, Some(7.0));
    assert_eq!(
        arbitrage_report.top_routes[0].route,
        "0x833589...2913 -> 0x420000...0006"
    );
    assert_eq!(arbitrage_report.top_protocols[0].protocol, "uniswap_v3");
    assert_eq!(
        arbitrage_report.representative_transactions[0].tx_hash,
        "0xarb-success"
    );

    let liquidation_report = analysis.liquidation_report.as_ref().unwrap();
    assert_eq!(liquidation_report.strategy_type, "liquidation");
    assert_eq!(liquidation_report.tx_count, 1);
    assert_eq!(liquidation_report.success_count, 1);
    assert_eq!(liquidation_report.estimated_pnl_usd, Some(40.0));
    assert_eq!(liquidation_report.gas_cost_usd, Some(5.0));
    assert_eq!(liquidation_report.top_protocols[0].protocol, "aave");

    assert_eq!(
        analysis.representative_transactions[0].tx_hash,
        "0xliquidation"
    );

    let snapshot = strategy_service
        .refresh_cluster_report_snapshot(cluster.cluster_id, None, None)
        .await
        .unwrap();
    assert_eq!(snapshot.cluster_id, cluster.cluster_id);
    assert_eq!(snapshot.chain_id, 8453);
    assert_eq!(snapshot.window_key, "*..*");
    assert_eq!(
        snapshot.report_json["behavior_profile"]["primary_behavior"],
        json!("arbitrage")
    );

    let cached = strategy_service
        .get_cached_cluster_report_snapshot(cluster.cluster_id, None, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cached.snapshot_id, snapshot.snapshot_id);
    assert_eq!(cached.report_json["tx_count"], json!(3));
}

#[tokio::test]
async fn report_agent_reads_bounded_cached_cluster_report() {
    let (strategy_service, cluster_id) = seeded_strategy_report_snapshot().await;
    let agent_service = ReportAgentService::new(strategy_service);

    let response = agent_service
        .query_cluster_report(ReportAgentQuery {
            cluster_id,
            window_start: None,
            window_end: None,
            all_time: true,
            sections: vec![
                ReportAgentSection::Overview,
                ReportAgentSection::Arbitrage,
                ReportAgentSection::Samples,
            ],
            filters: ReportAgentFilters {
                strategy: Some("arbitrage".to_owned()),
                ..Default::default()
            },
            row_limit: 2,
            cost_budget: 50,
        })
        .await
        .unwrap();

    assert_eq!(
        response.plan.allowed_sources,
        vec!["cluster_strategy_report_snapshots".to_owned()]
    );
    assert!(response.plan.estimated_cost <= 50);
    assert!(response.answer.contains("Arbitrage"));
    assert!(
        response
            .rows
            .iter()
            .any(|row| row.item_type == "strategy_summary")
    );
    assert!(
        response
            .citations
            .iter()
            .any(|citation| citation.source == "cluster_strategy_report_snapshots")
    );
    assert!(
        response
            .rows
            .iter()
            .filter(|row| row.section == "samples")
            .count()
            <= 2
    );
}

#[tokio::test]
async fn report_agent_requires_explicit_window_scope() {
    let (strategy_service, cluster_id) = seeded_strategy_report_snapshot().await;
    let agent_service = ReportAgentService::new(strategy_service);

    let err = agent_service
        .query_cluster_report(ReportAgentQuery {
            cluster_id,
            window_start: None,
            window_end: None,
            all_time: false,
            sections: vec![ReportAgentSection::Overview],
            filters: ReportAgentFilters::default(),
            row_limit: 5,
            cost_budget: 20,
        })
        .await
        .unwrap_err();

    assert!(err.to_string().contains("bounded window"));
}

#[tokio::test]
async fn report_agent_rejects_queries_over_cost_budget() {
    let (strategy_service, cluster_id) = seeded_strategy_report_snapshot().await;
    let agent_service = ReportAgentService::new(strategy_service);

    let err = agent_service
        .query_cluster_report(ReportAgentQuery {
            cluster_id,
            window_start: None,
            window_end: None,
            all_time: true,
            sections: vec![
                ReportAgentSection::Overview,
                ReportAgentSection::Performance,
                ReportAgentSection::Arbitrage,
                ReportAgentSection::Routes,
                ReportAgentSection::Samples,
            ],
            filters: ReportAgentFilters::default(),
            row_limit: 10,
            cost_budget: 1,
        })
        .await
        .unwrap_err();

    assert!(err.to_string().contains("exceeds budget"));
}

async fn seeded_strategy_report_snapshot() -> (Arc<ClusterStrategyAnalysisService>, uuid::Uuid) {
    let repository = Arc::new(InMemoryAnalysisRepository::default());
    let cluster_service = ClusterService::new(repository.clone());
    let strategy_service = Arc::new(ClusterStrategyAnalysisService::new(repository.clone()));

    let cluster = cluster_service
        .create_cluster(
            8453,
            "ops_team",
            "agent_cluster",
            None,
            vec![ClusterAddress {
                address: "0x1111111111111111111111111111111111111111".to_owned(),
                label: Some("bot".to_owned()),
                role: Some("executor".to_owned()),
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
            tx_hash: "0xagent-arb-success".to_owned(),
            address: "0x1111111111111111111111111111111111111111".to_owned(),
            block_number: 300,
            timestamp: Some(now - Duration::minutes(2)),
            behavior_tag: BehaviorTag::Arbitrage,
            protocol: Some("uniswap_v3".to_owned()),
            token_in: Some("0x4200000000000000000000000000000000000006".to_owned()),
            token_out: Some("0x833589fcd6edb6e08f4c7c32d4f71b54bda02913".to_owned()),
            amount_in: Some("1000000000000000000".to_owned()),
            amount_out: Some("2500000000".to_owned()),
            value_usd: Some(2500.0),
            gas_cost_usd: Some(3.0),
            estimated_pnl_usd: Some(12.0),
            raw_json: json!({}),
            created_at: now,
            updated_at: now,
        },
        ClassifiedTransaction {
            cluster_id: cluster.cluster_id,
            chain_id: 8453,
            tx_hash: "0xagent-arb-loss".to_owned(),
            address: "0x1111111111111111111111111111111111111111".to_owned(),
            block_number: 301,
            timestamp: Some(now - Duration::minutes(1)),
            behavior_tag: BehaviorTag::Arbitrage,
            protocol: Some("uniswap_v3".to_owned()),
            token_in: Some("0x4200000000000000000000000000000000000006".to_owned()),
            token_out: Some("0x833589fcd6edb6e08f4c7c32d4f71b54bda02913".to_owned()),
            amount_in: Some("1000000000000000000".to_owned()),
            amount_out: Some("2490000000".to_owned()),
            value_usd: Some(2490.0),
            gas_cost_usd: Some(4.0),
            estimated_pnl_usd: Some(-2.0),
            raw_json: json!({}),
            created_at: now,
            updated_at: now,
        },
    ];

    repository
        .upsert_classified_transactions(&txs)
        .await
        .unwrap();
    strategy_service
        .refresh_cluster_report_snapshot(cluster.cluster_id, None, None)
        .await
        .unwrap();

    (strategy_service, cluster.cluster_id)
}
