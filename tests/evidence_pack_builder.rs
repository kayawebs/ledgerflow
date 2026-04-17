mod common;

use ledgerflow::{
    core::versioning::VersionBundle,
    domain::interfaces::FlowDeriver,
    evidence_pack::{builder::EvidencePackBuilder, hashing::build_input_digest},
    flows::derivation::BasicFlowDeriver,
    ingestion::fetchers::TransactionBundleFetcher,
    normalization::service::EvidenceNormalizer,
    recognition::{builtins::builtin_recognizers, registry::RecognizerRegistry},
};

use common::MockRpcClient;

#[tokio::test]
async fn evidence_pack_builder_test() {
    let fetcher = TransactionBundleFetcher::new(MockRpcClient::default().boxed(), true);
    let bundle = fetcher.fetch(8453, "0xpack").await.unwrap();
    let mut analysis = EvidenceNormalizer.normalize(&bundle);

    let mut registry = RecognizerRegistry::new();
    for recognizer in builtin_recognizers() {
        registry.register(recognizer);
    }
    let recognizer_versions = registry
        .list_metadata()
        .into_iter()
        .map(|meta| (meta.recognizer_id, meta.version))
        .collect();
    analysis.analysis_version_metadata = VersionBundle {
        recognizer_versions,
        ..VersionBundle::default()
    };
    analysis.input_digest = build_input_digest(&bundle, &analysis.analysis_version_metadata);
    analysis.derived_flows = BasicFlowDeriver.derive(&bundle, &analysis);
    analysis.recognizer_results = builtin_recognizers()
        .into_iter()
        .filter(|recognizer| {
            [
                "selector_presence",
                "erc20_transfer_flow",
                "native_transfer",
                "approval_swap_scenario",
            ]
            .contains(&recognizer.recognizer_id())
        })
        .filter_map(|recognizer| recognizer.recognize(&analysis).unwrap())
        .collect();

    let pack = EvidencePackBuilder.build(&analysis);
    assert_eq!(pack.tx_hash.as_deref(), Some("0xpack"));
    assert_eq!(pack.input_digest, analysis.input_digest);
    assert!(pack.labels.len() >= 2);
}
