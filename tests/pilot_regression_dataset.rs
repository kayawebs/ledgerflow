use std::collections::{BTreeSet, HashMap};

use num_bigint::BigUint;
use serde_json::Value;

#[test]
fn base_regression_dataset_has_100_rpc_verified_transactions() {
    let raw = include_str!("fixtures/base_regression_dataset.jsonl");
    let rows = raw
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(rows.len(), 100);
    let hashes = rows
        .iter()
        .map(|row| row["tx_hash"].as_str().unwrap())
        .collect::<BTreeSet<_>>();
    assert_eq!(hashes.len(), 100);
    for row in &rows {
        assert_eq!(row["schema_version"], "ledgerflow.base-regression.v3");
        assert_eq!(row["chain_id"], 8453);
        assert_eq!(
            row["evidence_sources"],
            serde_json::json!(["eth_getTransactionByHash", "eth_getTransactionReceipt"])
        );
        let used = row["gas_used_raw"]
            .as_str()
            .unwrap()
            .parse::<BigUint>()
            .unwrap();
        let price = row["effective_gas_price_raw"]
            .as_str()
            .unwrap()
            .parse::<BigUint>()
            .unwrap();
        let execution_cost = row["execution_gas_cost_raw"]
            .as_str()
            .unwrap()
            .parse::<BigUint>()
            .unwrap();
        let l1_fee = row["l1_fee_raw"]
            .as_str()
            .unwrap()
            .parse::<BigUint>()
            .unwrap();
        let cost = row["gas_cost_raw"]
            .as_str()
            .unwrap()
            .parse::<BigUint>()
            .unwrap();
        assert_eq!(used * price, execution_cost);
        assert_eq!(execution_cost + l1_fee, cost);
        let transfer_logs = row["transfer_logs"].as_array().unwrap();
        assert!(transfer_logs.len() <= row["log_count"].as_u64().unwrap() as usize);
        for transfer in transfer_logs {
            assert!(
                transfer["token_address"]
                    .as_str()
                    .unwrap()
                    .starts_with("0x")
            );
            assert_eq!(transfer["token_address"].as_str().unwrap().len(), 42);
            assert_eq!(transfer["from_address"].as_str().unwrap().len(), 42);
            assert_eq!(transfer["to_address"].as_str().unwrap().len(), 42);
            transfer["value_raw"]
                .as_str()
                .unwrap()
                .parse::<BigUint>()
                .unwrap();
        }
    }
    let protocols = rows
        .iter()
        .filter_map(|row| row["expected_protocol"].as_str())
        .collect::<BTreeSet<_>>();
    for required in [
        "uniswap_v2",
        "uniswap_v3",
        "uniswap_v4",
        "aerodrome",
        "pancakeswap",
        "balancer",
        "morpho",
        "aave",
        "moonwell",
        "transfer",
    ] {
        assert!(protocols.contains(required), "missing {required}");
    }
    assert!(rows.iter().any(|v| v["expected_case"] == "dex_arbitrage"));
    assert!(rows.iter().any(|v| v["expected_case"] == "liquidation"));
    assert!(
        rows.iter()
            .any(|v| v["expected_case"] == "complex_asset_flow")
    );
    assert!(rows.iter().any(|v| v["status"] == "failure"));
    let complex = rows
        .iter()
        .find(|row| row["expected_case"] == "complex_asset_flow")
        .unwrap();
    assert!(complex["transfer_logs"].as_array().unwrap().len() >= 6);
    assert!(
        complex["transfer_logs"]
            .as_array()
            .unwrap()
            .iter()
            .any(|log| log["value_raw"] == "299088934")
    );
}

#[test]
fn demo_protocol_paths_agree_with_evaluable_fixture_labels() {
    let fixtures = include_str!("fixtures/base_regression_dataset.jsonl")
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .map(|row| (row["tx_hash"].as_str().unwrap().to_owned(), row))
        .collect::<HashMap<_, _>>();
    let events: Vec<Value> =
        serde_json::from_str(include_str!("../demo/base-public/all_events.json")).unwrap();
    let aliases = HashMap::from([
        ("uniswap_v2", "uniswap v2"),
        ("uniswap_v3", "uniswap v3"),
        ("uniswap_v4", "uniswap v4"),
        ("aerodrome", "aerodrome"),
        ("pancakeswap", "pancakeswap"),
        ("balancer", "balancer"),
        ("morpho", "morpho"),
        ("aave", "aave"),
        ("moonwell", "moonwell"),
    ]);
    let mut evaluated = 0;
    let mut matched = 0;
    for event in &events {
        let fixture = &fixtures[event["tx_hash"].as_str().unwrap()];
        let Some(alias) = fixture["expected_protocol"]
            .as_str()
            .and_then(|protocol| aliases.get(protocol))
        else {
            continue;
        };
        let path = event["protocol_path"].as_array().unwrap();
        if path.is_empty() {
            continue;
        }
        evaluated += 1;
        if path.iter().any(|item| {
            item["protocol"]
                .as_str()
                .is_some_and(|value| value.to_ascii_lowercase().contains(alias))
        }) {
            matched += 1;
        }
    }
    assert!(evaluated >= 30, "insufficient evaluable protocol paths");
    assert_eq!(matched, evaluated);
    for (event_type, hash) in [
        (
            "dex_arbitrage",
            "0x0a0309ba7b2d9268a30a7022f3ea33b6db3998628364dad56ddeb7ddc25887d1",
        ),
        (
            "liquidation",
            "0x6f4c472b846a2d5530ece4596a39c5cb9f088f0ad2e3804f80f95c8b2289d40a",
        ),
    ] {
        assert!(
            events
                .iter()
                .any(|event| { event["event_type"] == event_type && event["tx_hash"] == hash }),
            "missing {event_type} Demo case"
        );
    }
}
