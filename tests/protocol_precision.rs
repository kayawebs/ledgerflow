use std::collections::BTreeMap;

use ledgerflow::{
    core::versioning::VersionBundle,
    domain::{
        entities::{NormalizedExecutionFacts, TransactionAnalysis},
        enums::{AnalysisStatus, TransactionStatus},
        interfaces::Recognizer,
    },
    recognition::builtins::ProtocolDeploymentEvidenceRecognizer,
};
use serde_json::{Value, json};
use uuid::Uuid;

fn deployment_map() -> BTreeMap<String, Value> {
    let datasets = [
        include_str!("../config/intelligence/base-core.v1.json"),
        include_str!("../config/intelligence/base-phase1-protocols.v1.json"),
    ];
    let mut deployments = BTreeMap::new();
    for raw in datasets {
        let dataset: Value = serde_json::from_str(raw).unwrap();
        for entry in dataset["addresses"].as_array().unwrap() {
            let address = entry["address"].as_str().unwrap().to_ascii_lowercase();
            for deployment in entry["protocol_deployments"].as_array().unwrap() {
                deployments.insert(
                    address.clone(),
                    json!({
                        "chain_id": 8453,
                        "address": address,
                        "protocol_family": deployment["protocol_family"],
                        "protocol_name": deployment["protocol_key"],
                        "contract_role": deployment["contract_role"],
                        "source": dataset["source"]["source_key"],
                        "adapter_version": dataset["dataset_version"],
                        "confidence": deployment["confidence"],
                    }),
                );
            }
        }
    }
    deployments
}

fn analysis(row: &Value, evidence: Option<&Value>) -> TransactionAnalysis {
    TransactionAnalysis {
        analysis_id: Uuid::nil(),
        chain_id: 8453,
        tx_hash: row["tx_hash"].as_str().unwrap().to_owned(),
        block_number: row["block_number"].as_u64(),
        block_timestamp: None,
        from_address: "0x0000000000000000000000000000000000000000".to_owned(),
        to_address: Some(row["evidence_address"].as_str().unwrap().to_owned()),
        status: TransactionStatus::Success,
        value: "0".to_owned(),
        input_data: "0x".to_owned(),
        code_identity_summary: None,
        raw_evidence_refs: Vec::new(),
        normalized_execution_facts: NormalizedExecutionFacts {
            selector: None,
            call_count: 1,
            internal_call_count: 0,
            event_count: 0,
            call_tree: json!({}),
            participants: vec![row["evidence_address"].as_str().unwrap().to_owned()],
            log_summaries: Vec::new(),
            derived_facts: json!({
                "protocol_evidence": evidence.into_iter().cloned().collect::<Vec<_>>(),
            }),
            evidence_refs: Vec::new(),
        },
        derived_flows: Vec::new(),
        recognizer_results: Vec::new(),
        scenario_outputs: json!({}),
        accounting_outputs: json!({}),
        analysis_status: AnalysisStatus::Completed,
        analysis_version_metadata: VersionBundle::default(),
        unresolved_items: Vec::new(),
        validation_results: Vec::new(),
        input_digest: "fixture".to_owned(),
        attempt_id: None,
    }
}

#[test]
fn reviewed_base_protocol_precision_is_exact_and_fail_closed() {
    let deployments = deployment_map();
    let rows = include_str!("fixtures/base_protocol_ground_truth.jsonl")
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(rows.len(), 95);

    let recognizer = ProtocolDeploymentEvidenceRecognizer;
    let mut true_positive = 0_u32;
    let mut false_positive = 0_u32;
    let mut false_negative = 0_u32;
    let mut reviewed_negative = 0_u32;
    for row in rows {
        assert_eq!(row["independent_from"], "recognizer_output");
        let address = row["evidence_address"]
            .as_str()
            .unwrap()
            .to_ascii_lowercase();
        let result = recognizer
            .recognize(&analysis(&row, deployments.get(&address)))
            .unwrap();
        let expected = row["expected_protocol"].as_str();
        let predicted = result
            .as_ref()
            .and_then(|result| result.attributes["protocol"].as_str());
        match (expected, predicted) {
            (Some(expected), Some(predicted)) if expected == predicted => true_positive += 1,
            (None, None) => reviewed_negative += 1,
            (None, Some(_)) | (Some(_), Some(_)) => false_positive += 1,
            (Some(_), None) => false_negative += 1,
        }
    }
    assert_eq!(true_positive, 86);
    assert_eq!(reviewed_negative, 9);
    assert_eq!(false_positive, 0);
    assert_eq!(false_negative, 0);
}
