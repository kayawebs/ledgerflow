mod common;

use std::sync::Arc;

use ledgerflow::{
    ingestion::fetchers::TransactionBundleFetcher, plugins::loader::load_plugins,
    recognition::registry::RecognizerRegistry, repositories::memory::InMemoryAnalysisRepository,
    services::analysis::AnalysisService,
};

use common::MockRpcClient;

#[tokio::test]
async fn end_to_end_analysis_happy_path() {
    let mut registry = RecognizerRegistry::new();
    let plugin_context = load_plugins(&["sample_builtin_pack".to_owned()], &mut registry);
    let service = AnalysisService::new(
        TransactionBundleFetcher::new(MockRpcClient::default().boxed(), true),
        registry,
        plugin_context,
        Arc::new(InMemoryAnalysisRepository::default()),
        vec![
            "selector_presence".to_owned(),
            "erc20_transfer_flow".to_owned(),
            "native_transfer".to_owned(),
            "batch_router_heuristic".to_owned(),
            "approval_swap_scenario".to_owned(),
        ],
    );

    let (analysis, pack) = service.analyze_transaction(8453, "0xe2e").await.unwrap();
    let labels = analysis
        .recognizer_results
        .iter()
        .map(|item| item.label.clone())
        .collect::<Vec<_>>();
    assert!(!analysis.input_digest.is_empty());
    assert!(labels.contains(&"selector_present".to_owned()));
    assert!(labels.contains(&"erc20_transfer_activity".to_owned()));
    assert_eq!(
        pack.metadata["analysis_status"],
        serde_json::json!(analysis.analysis_status)
    );
}

#[tokio::test]
async fn graceful_handling_of_unsupported_trace() {
    let mut registry = RecognizerRegistry::new();
    let plugin_context = load_plugins(&["sample_builtin_pack".to_owned()], &mut registry);
    let service = AnalysisService::new(
        TransactionBundleFetcher::new(
            MockRpcClient {
                include_trace: true,
                raise_trace_unsupported: true,
            }
            .boxed(),
            true,
        ),
        registry,
        plugin_context,
        Arc::new(InMemoryAnalysisRepository::default()),
        vec![
            "selector_presence".to_owned(),
            "erc20_transfer_flow".to_owned(),
            "native_transfer".to_owned(),
        ],
    );

    let (analysis, _) = service
        .analyze_transaction(8453, "0xnotrace")
        .await
        .unwrap();
    assert_eq!(
        analysis.normalized_execution_facts.derived_facts["trace_available"],
        false
    );
    assert_eq!(
        analysis.analysis_status,
        ledgerflow::domain::enums::AnalysisStatus::Partial
    );
    assert!(
        analysis
            .validation_results
            .iter()
            .any(|item| item.validator_id == "trace_availability"
                && item.status == ledgerflow::domain::enums::ValidationStatus::Warn)
    );
}
