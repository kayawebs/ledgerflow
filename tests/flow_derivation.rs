mod common;

use ledgerflow::{
    domain::interfaces::FlowDeriver, flows::derivation::BasicFlowDeriver,
    ingestion::fetchers::TransactionBundleFetcher, normalization::service::EvidenceNormalizer,
};

use common::MockRpcClient;

#[tokio::test]
async fn flow_normalization() {
    let fetcher = TransactionBundleFetcher::new(MockRpcClient::default().boxed(), true);
    let bundle = fetcher.fetch(8453, "0xflow").await.unwrap();
    let analysis = EvidenceNormalizer.normalize(&bundle);
    let flows = BasicFlowDeriver.derive(&bundle, &analysis);
    assert_eq!(flows.len(), 4);
    assert!(
        flows
            .iter()
            .any(|flow| flow.asset_type == ledgerflow::domain::enums::AssetType::Native)
    );
    assert!(
        flows.iter().any(|flow| flow.token_address.as_deref()
            == Some("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
    );
}
