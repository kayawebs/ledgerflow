use chrono::Utc;
use ledgerflow::{
    domain::{entities::ClassifiedTransaction, enums::BehaviorTag},
    services::strategy_facts::build_strategy_fact,
};
use serde_json::json;
use uuid::Uuid;

#[test]
fn strategy_fact_normalizes_arbitrage_route_and_protocol_family() {
    let tx = classified_tx(
        BehaviorTag::Arbitrage,
        Some("Uniswap_V3"),
        Some("0x4200000000000000000000000000000000000006"),
        Some("0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"),
        Some(12.0),
        Some(3.0),
    );

    let fact = build_strategy_fact(&tx);

    assert_eq!(fact.strategy_type, "arbitrage");
    assert_eq!(fact.strategy_subtype, "token_pair_arbitrage");
    assert_eq!(fact.protocol_name.as_deref(), Some("uniswap_v3"));
    assert_eq!(fact.protocol_family.as_deref(), Some("uniswap"));
    assert_eq!(fact.route.route_key, "0x833589...2913 -> 0x420000...0006");
    assert_eq!(fact.route.token_in.unwrap().symbol, None);
    assert_eq!(fact.route.token_out.unwrap().symbol, None);
    assert_eq!(fact.outcome.report_success, true);
    assert_eq!(fact.outcome.failure_reason, None);
    assert_eq!(fact.pnl_to_gas_ratio, Some(4.0));
}

#[test]
fn strategy_fact_labels_liquidation_protocol_and_negative_pnl_failure() {
    let tx = classified_tx(
        BehaviorTag::Liquidation,
        Some("aave_v3"),
        None,
        None,
        Some(-5.0),
        Some(2.5),
    );

    let fact = build_strategy_fact(&tx);

    assert_eq!(fact.strategy_type, "liquidation");
    assert_eq!(fact.strategy_subtype, "aave_liquidation");
    assert_eq!(fact.protocol_family.as_deref(), Some("aave"));
    assert_eq!(fact.route.route_key, "unknown_route");
    assert_eq!(fact.outcome.profitable, Some(false));
    assert_eq!(fact.outcome.report_success, false);
    assert_eq!(
        fact.outcome.failure_reason.as_deref(),
        Some("negative_estimated_pnl")
    );
}

fn classified_tx(
    behavior_tag: BehaviorTag,
    protocol: Option<&str>,
    token_in: Option<&str>,
    token_out: Option<&str>,
    estimated_pnl_usd: Option<f64>,
    gas_cost_usd: Option<f64>,
) -> ClassifiedTransaction {
    let now = Utc::now();
    ClassifiedTransaction {
        cluster_id: Uuid::nil(),
        chain_id: 8453,
        tx_hash: "0xfact".to_owned(),
        address: "0x1111111111111111111111111111111111111111".to_owned(),
        block_number: 1,
        timestamp: Some(now),
        behavior_tag,
        protocol: protocol.map(str::to_owned),
        token_in: token_in.map(str::to_owned),
        token_out: token_out.map(str::to_owned),
        amount_in: None,
        amount_out: None,
        value_usd: Some(100.0),
        gas_cost_usd,
        estimated_pnl_usd,
        raw_json: json!({}),
        created_at: now,
        updated_at: now,
    }
}
