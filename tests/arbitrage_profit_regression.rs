use std::collections::BTreeSet;

use num_bigint::BigUint;
use serde_json::Value;

#[test]
fn deterministic_profit_cases_are_fail_closed_and_gas_exact() {
    let rows = include_str!("fixtures/base_arbitrage_profit_cases.jsonl")
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(rows.len(), 6);
    assert_eq!(
        rows.iter()
            .map(|row| row["tx_hash"].as_str().unwrap())
            .collect::<BTreeSet<_>>()
            .len(),
        rows.len()
    );

    let mut verified = 0;
    let mut unavailable = 0;
    for row in rows {
        assert_eq!(
            row["schema_version"],
            "ledgerflow.arbitrage-profit-regression.v1"
        );
        assert_eq!(row["chain_id"], 8453);
        assert_eq!(row["accounting_scope"].as_array().unwrap().len(), 2);
        let execution = row["execution_gas_cost_raw"]
            .as_str()
            .unwrap()
            .parse::<BigUint>()
            .unwrap();
        let l1 = row["l1_fee_raw"]
            .as_str()
            .unwrap()
            .parse::<BigUint>()
            .unwrap();
        let total = row["gas_cost_raw"]
            .as_str()
            .unwrap()
            .parse::<BigUint>()
            .unwrap();
        assert_eq!(execution + l1, total);

        match row["expected_status"].as_str().unwrap() {
            "verified" => {
                verified += 1;
                assert!(
                    row["profit_raw"]
                        .as_str()
                        .unwrap()
                        .parse::<BigUint>()
                        .unwrap()
                        > BigUint::from(0_u8)
                );
                let token = row["profit_token_address"].as_str().unwrap();
                assert!(token.starts_with("0x") && token.len() == 42);
                assert!(row["expected_reason"].is_null());
            }
            "unavailable" => {
                unavailable += 1;
                assert!(row["profit_raw"].is_null());
                assert!(row["profit_token_address"].is_null());
                assert!(!row["expected_reason"].as_str().unwrap().is_empty());
            }
            status => panic!("unexpected status {status}"),
        }
    }
    assert_eq!(verified, 3);
    assert_eq!(unavailable, 3);
}
