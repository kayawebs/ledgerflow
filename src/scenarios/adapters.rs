use std::collections::BTreeSet;

use serde_json::{Value, json};

use crate::domain::{
    entities::{RecognizerResult, TransactionAnalysis},
    enums::AssetType,
};

#[derive(Default, Clone)]
pub struct ScenarioAdapterService;

impl ScenarioAdapterService {
    pub fn adapt(&self, analysis: &TransactionAnalysis) -> Value {
        let labels = analysis
            .recognizer_results
            .iter()
            .map(|item| item.label.clone())
            .collect::<Vec<_>>();
        let primary = primary_recognizer(analysis);
        let protocol = primary
            .and_then(protocol_hint)
            .map(str::to_owned)
            .or_else(|| analysis.to_address.clone());
        let storyline = if labels.iter().any(|label| label == "liquidation_like_activity") {
            build_liquidation_story(analysis, protocol.as_deref())
        } else if labels.iter().any(|label| label == "arbitrage_like_activity") {
            build_arbitrage_story(analysis, protocol.as_deref())
        } else {
            None
        };

        json!({
            "attempt_hint": analysis.tx_hash,
            "opportunity_hint": labels.first(),
            "labels": labels,
            "primary_activity": primary.map(|item| item.label.clone()),
            "protocol": protocol,
            "behavior_fingerprint": build_behavior_fingerprint(analysis, primary),
            "storyline": storyline,
        })
    }
}

fn primary_recognizer(analysis: &TransactionAnalysis) -> Option<&RecognizerResult> {
    analysis
        .recognizer_results
        .iter()
        .filter(|item| item.label != "selector_present")
        .max_by(|left, right| left.confidence.total_cmp(&right.confidence))
}

fn protocol_hint(result: &RecognizerResult) -> Option<&str> {
    result
        .attributes
        .get("protocol")
        .and_then(|value| value.as_str())
}

fn build_behavior_fingerprint(
    analysis: &TransactionAnalysis,
    primary: Option<&RecognizerResult>,
) -> String {
    let executor = analysis.to_address.as_deref().unwrap_or("contract_creation");
    let selector = analysis
        .normalized_execution_facts
        .selector
        .as_deref()
        .unwrap_or("no_selector");
    let label = primary
        .map(|item| item.label.as_str())
        .unwrap_or("unknown_activity");
    let protocol = primary
        .and_then(protocol_hint)
        .or_else(|| analysis.to_address.as_deref())
        .unwrap_or("unknown_protocol");
    format!(
        "{}:{}:{}:{}",
        executor.to_lowercase(),
        selector.to_lowercase(),
        label,
        protocol.to_lowercase()
    )
}

fn build_liquidation_story(analysis: &TransactionAnalysis, protocol: Option<&str>) -> Option<Value> {
    let executor = analysis
        .to_address
        .as_ref()
        .map(|value| value.to_lowercase())
        .unwrap_or_else(|| analysis.from_address.to_lowercase());
    let sender = analysis.from_address.to_lowercase();
    let call_path = call_path_steps(&analysis.normalized_execution_facts.call_tree, 8);
    let adapter_attributes = liquidation_adapter_attributes(analysis);
    let protocol_contract = adapter_attributes
        .as_ref()
        .and_then(|value| value.get("protocol_contract"))
        .and_then(|value| value.as_str())
        .map(str::to_owned)
        .or_else(|| protocol_contract_from_recognizers(analysis));
    let liquidator = adapter_attributes
        .as_ref()
        .and_then(|value| value.get("liquidator"))
        .and_then(|value| value.as_str())
        .map(|value| value.to_lowercase())
        .unwrap_or_else(|| executor.clone());
    let borrower = adapter_attributes
        .as_ref()
        .and_then(|value| value.get("borrower"))
        .and_then(|value| value.as_str())
        .map(|value| value.to_lowercase());
    let protocol_neighborhood = liquidation_protocol_neighborhood(
        &call_path,
        protocol_contract.as_deref(),
        &sender,
        &executor,
        borrower.as_deref(),
    );
    let actor_addresses = vec![executor.clone(), liquidator.clone()];

    let funding_sources = related_flows(analysis, |flow| {
        address_in_set(&flow.to_address, &actor_addresses)
            && !address_in_set(&flow.from_address, &actor_addresses)
            && !address_in_set(&flow.from_address, &protocol_neighborhood)
    });
    let repay_legs = related_flows(analysis, |flow| {
        address_in_set(&flow.from_address, &actor_addresses)
            && address_in_set(&flow.to_address, &protocol_neighborhood)
    });
    let collateral_receipts = related_flows(analysis, |flow| {
        address_in_set(&flow.to_address, &actor_addresses)
            && address_in_set(&flow.from_address, &protocol_neighborhood)
    });
    let disposition_legs = related_flows(analysis, |flow| {
        address_in_set(&flow.from_address, &actor_addresses)
            && !address_in_set(&flow.to_address, &actor_addresses)
            && !address_in_set(&flow.to_address, &protocol_neighborhood)
    });

    if funding_sources.is_empty()
        && repay_legs.is_empty()
        && collateral_receipts.is_empty()
        && disposition_legs.is_empty()
        && call_path.is_empty()
    {
        return None;
    }

    Some(json!({
        "kind": "liquidation",
        "summary": format!(
            "EOA {} called executor {}. LedgerFlow matched a liquidation path against {} and summarized how funds entered the executor, where debt repayment flowed, what collateral came back, and where the proceeds were routed next.",
            short_address(&sender),
            short_address(&executor),
            protocol.unwrap_or("an unresolved liquidation protocol")
        ),
        "executor_address": executor,
        "liquidator_address": liquidator,
        "initiator_address": sender,
        "borrower_address": borrower,
        "protocol": protocol,
        "protocol_contract": protocol_contract,
        "call_path": call_path,
        "funding_sources": funding_sources,
        "repay_legs": repay_legs,
        "collateral_receipts": collateral_receipts,
        "disposition_legs": disposition_legs,
    }))
}

fn build_arbitrage_story(analysis: &TransactionAnalysis, protocol: Option<&str>) -> Option<Value> {
    let executor = analysis
        .to_address
        .as_ref()
        .map(|value| value.to_lowercase())
        .unwrap_or_else(|| analysis.from_address.to_lowercase());
    let sender = analysis.from_address.to_lowercase();
    let call_path = call_path_steps(&analysis.normalized_execution_facts.call_tree, 8);
    let funding_sources = related_flows(analysis, |flow| {
        flow.to_address.eq_ignore_ascii_case(&executor) && !flow.from_address.eq_ignore_ascii_case(&executor)
    });
    let exit_legs = related_flows(analysis, |flow| {
        flow.from_address.eq_ignore_ascii_case(&executor)
            && !flow.to_address.eq_ignore_ascii_case(&executor)
    });
    let cycle_legs = related_flows(analysis, |flow| {
        flow.from_address.eq_ignore_ascii_case(&sender) || flow.to_address.eq_ignore_ascii_case(&sender)
    });

    if funding_sources.is_empty() && exit_legs.is_empty() && call_path.is_empty() {
        return None;
    }

    Some(json!({
        "kind": "arbitrage",
        "summary": format!(
            "EOA {} triggered executor {}. LedgerFlow summarized the funding leg, the internal route path, and the exit legs that look like arbitrage settlement for {}.",
            short_address(&sender),
            short_address(&executor),
            protocol.unwrap_or("an unresolved venue set")
        ),
        "executor_address": executor,
        "initiator_address": sender,
        "protocol": protocol,
        "call_path": call_path,
        "funding_sources": funding_sources,
        "cycle_legs": cycle_legs,
        "exit_legs": exit_legs,
    }))
}

fn protocol_contract_from_recognizers(analysis: &TransactionAnalysis) -> Option<String> {
    analysis
        .recognizer_results
        .iter()
        .find_map(|item| {
            item.attributes
                .get("adapter_attributes")
                .and_then(|value| value.get("protocol_contract"))
                .and_then(|value| value.as_str())
                .map(str::to_owned)
        })
}

fn liquidation_adapter_attributes(analysis: &TransactionAnalysis) -> Option<Value> {
    analysis
        .recognizer_results
        .iter()
        .find(|item| item.label == "liquidation_like_activity")
        .and_then(|item| item.attributes.get("adapter_attributes"))
        .cloned()
}

fn liquidation_protocol_neighborhood(
    call_path: &[Value],
    protocol_contract: Option<&str>,
    sender: &str,
    executor: &str,
    borrower: Option<&str>,
) -> Vec<String> {
    let mut addresses = BTreeSet::new();
    if let Some(protocol_contract) = protocol_contract {
        addresses.insert(protocol_contract.to_lowercase());
    }
    for step in call_path {
        for key in ["from_address", "to_address"] {
            if let Some(address) = step.get(key).and_then(|value| value.as_str()) {
                let address = address.to_lowercase();
                if address != sender
                    && address != executor
                    && borrower.map(|value| address != value).unwrap_or(true)
                {
                    addresses.insert(address);
                }
            }
        }
    }
    addresses.into_iter().collect()
}

fn address_in_set(address: &str, values: &[String]) -> bool {
    values.iter().any(|value| address.eq_ignore_ascii_case(value))
}

fn call_path_steps(call_tree: &Value, limit: usize) -> Vec<Value> {
    let mut steps = Vec::new();
    collect_call_path_steps(call_tree, 0, limit, &mut steps);
    steps
}

fn collect_call_path_steps(call_tree: &Value, depth: usize, limit: usize, steps: &mut Vec<Value>) {
    if steps.len() >= limit {
        return;
    }
    let Some(object) = call_tree.as_object() else {
        return;
    };
    let selector = object
        .get("input_data")
        .and_then(|value| value.as_str())
        .filter(|value| value.len() >= 10)
        .map(|value| value[..10].to_lowercase());
    steps.push(json!({
        "depth": depth,
        "from_address": object.get("from_address").and_then(|value| value.as_str()),
        "to_address": object.get("to_address").and_then(|value| value.as_str()),
        "call_type": object.get("call_type").and_then(|value| value.as_str()),
        "selector": selector,
        "value": object.get("value").and_then(|value| value.as_str()),
    }));
    if steps.len() >= limit {
        return;
    }
    if let Some(children) = object.get("children").and_then(|value| value.as_array()) {
        for child in children {
            collect_call_path_steps(child, depth + 1, limit, steps);
            if steps.len() >= limit {
                break;
            }
        }
    }
}

fn related_flows<F>(analysis: &TransactionAnalysis, predicate: F) -> Vec<Value>
where
    F: Fn(&crate::domain::value_objects::AssetFlow) -> bool,
{
    analysis
        .derived_flows
        .iter()
        .filter(|flow| predicate(flow))
        .map(|flow| {
            json!({
                "asset_type": match flow.asset_type {
                    AssetType::Native => "native",
                    AssetType::Erc20 => "erc20",
                },
                "token_address": flow.token_address,
                "from_address": flow.from_address,
                "to_address": flow.to_address,
                "amount_raw": flow.amount_raw,
                "derivation_method": flow.derivation_method,
                "confidence": flow.confidence,
            })
        })
        .collect()
}

fn short_address(address: &str) -> String {
    if address.len() <= 12 {
        return address.to_owned();
    }
    format!("{}...{}", &address[..8], &address[address.len() - 4..])
}
