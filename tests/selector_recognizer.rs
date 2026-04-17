mod common;

use ledgerflow::{
    domain::interfaces::Recognizer, ingestion::fetchers::TransactionBundleFetcher,
    normalization::service::EvidenceNormalizer, recognition::builtins::SelectorPresenceRecognizer,
};

use common::MockRpcClient;

#[tokio::test]
async fn selector_extraction() {
    let fetcher = TransactionBundleFetcher::new(MockRpcClient::default().boxed(), false);
    let bundle = fetcher.fetch(8453, "0xtest").await.unwrap();
    let analysis = EvidenceNormalizer.normalize(&bundle);
    let result = SelectorPresenceRecognizer
        .recognize(&analysis)
        .unwrap()
        .unwrap();
    assert_eq!(result.attributes["selector"], "0x38ed1739");
    assert!(
        result.attributes["candidate_signatures"][0]
            .as_str()
            .unwrap()
            .contains("swapExactTokensForTokens")
    );
}
