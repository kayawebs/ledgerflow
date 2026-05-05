use std::collections::{BTreeMap, BTreeSet};

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

    let subject_scope = build_subject_scope(&sender, &actor_addresses);
    let net_effect = build_net_effect(analysis, &subject_scope.addresses);
    let steps = build_liquidation_steps(
        &funding_sources,
        &repay_legs,
        &collateral_receipts,
        &disposition_legs,
    );
    let headline = build_liquidation_headline(
        protocol.unwrap_or("unknown protocol"),
        &net_effect,
    );

    Some(json!({
        "kind": "liquidation",
        "headline": headline,
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
        "subject_scope": {
            "mode": subject_scope.mode,
            "label": subject_scope.label,
            "addresses": subject_scope.addresses,
        },
        "net_effect": net_effect,
        "steps": steps,
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

    let subject_scope = build_subject_scope(&sender, std::slice::from_ref(&executor));
    let net_effect = build_net_effect(analysis, &subject_scope.addresses);
    let steps = build_arbitrage_steps(&funding_sources, &cycle_legs, &exit_legs);
    let headline = build_arbitrage_headline(protocol.unwrap_or("unknown venues"), &net_effect);

    Some(json!({
        "kind": "arbitrage",
        "headline": headline,
        "summary": format!(
            "EOA {} triggered executor {}. LedgerFlow summarized the funding leg, the internal route path, and the exit legs that look like arbitrage settlement for {}.",
            short_address(&sender),
            short_address(&executor),
            protocol.unwrap_or("an unresolved venue set")
        ),
        "executor_address": executor,
        "initiator_address": sender,
        "protocol": protocol,
        "subject_scope": {
            "mode": subject_scope.mode,
            "label": subject_scope.label,
            "addresses": subject_scope.addresses,
        },
        "net_effect": net_effect,
        "steps": steps,
        "call_path": call_path,
        "funding_sources": funding_sources,
        "cycle_legs": cycle_legs,
        "exit_legs": exit_legs,
    }))
}

#[derive(Clone)]
struct SubjectScope {
    mode: &'static str,
    label: String,
    addresses: Vec<String>,
}

fn build_subject_scope(initiator: &str, actor_addresses: &[String]) -> SubjectScope {
    let mut addresses = BTreeSet::new();
    addresses.insert(initiator.to_owned());
    for address in actor_addresses {
        addresses.insert(address.to_lowercase());
    }
    let addresses = addresses.into_iter().collect::<Vec<_>>();
    SubjectScope {
        mode: "strategy",
        label: "initiator + executor scope".to_owned(),
        addresses,
    }
}

fn build_net_effect(analysis: &TransactionAnalysis, subject_addresses: &[String]) -> Value {
    let mut deltas = BTreeMap::<(AssetType, Option<String>), i128>::new();
    for flow in &analysis.derived_flows {
        let from_in_scope = address_in_set(&flow.from_address, subject_addresses);
        let to_in_scope = address_in_set(&flow.to_address, subject_addresses);
        if from_in_scope == to_in_scope {
            continue;
        }
        let amount = flow.amount_raw.parse::<i128>().unwrap_or_default();
        let entry = deltas
            .entry((flow.asset_type.clone(), flow.token_address.clone()))
            .or_default();
        if to_in_scope {
            *entry += amount;
        } else {
            *entry -= amount;
        }
    }
    let gas_cost_raw = analysis
        .normalized_execution_facts
        .derived_facts
        .get("gas_cost_raw")
        .and_then(|value| value.as_str())
        .and_then(|value| value.parse::<i128>().ok())
        .unwrap_or_default();
    let gained = deltas
        .iter()
        .filter(|(_, delta)| **delta > 0)
        .map(|((asset_type, token_address), delta)| render_effect_entry(*delta, asset_type, token_address.as_deref(), "gained"))
        .collect::<Vec<_>>();
    let spent = deltas
        .iter()
        .filter(|(_, delta)| **delta < 0)
        .map(|((asset_type, token_address), delta)| render_effect_entry(delta.abs(), asset_type, token_address.as_deref(), "spent"))
        .collect::<Vec<_>>();
    json!({
        "gained": gained,
        "spent": spent,
        "gas": if gas_cost_raw > 0 {
            Some(render_effect_entry(gas_cost_raw, &AssetType::Native, None, "gas"))
        } else {
            None
        },
    })
}

fn render_effect_entry(
    amount_raw: i128,
    asset_type: &AssetType,
    token_address: Option<&str>,
    direction: &str,
) -> Value {
    let token = token_descriptor(asset_type, token_address);
    json!({
        "direction": direction,
        "amount_raw": amount_raw.to_string(),
        "asset_type": match asset_type {
            AssetType::Native => "native",
            AssetType::Erc20 => "erc20",
        },
        "token": token,
    })
}

fn build_liquidation_steps(
    funding_sources: &[Value],
    repay_legs: &[Value],
    collateral_receipts: &[Value],
    disposition_legs: &[Value],
) -> Vec<Value> {
    let mut steps = Vec::new();
    let mut next_order = 1usize;
    next_order = extend_steps(&mut steps, next_order, "Funding executor", funding_sources, "Executor received");
    next_order = extend_steps(&mut steps, next_order, "Repay protocol debt", repay_legs, "Executor sent");
    next_order = extend_steps(&mut steps, next_order, "Receive collateral", collateral_receipts, "Executor received");
    let _ = extend_steps(&mut steps, next_order, "Route proceeds", disposition_legs, "Executor sent");
    steps
}

fn build_arbitrage_steps(
    funding_sources: &[Value],
    cycle_legs: &[Value],
    exit_legs: &[Value],
) -> Vec<Value> {
    let mut steps = Vec::new();
    let mut next_order = 1usize;
    next_order = extend_steps(&mut steps, next_order, "Funding executor", funding_sources, "Executor received");
    next_order = extend_steps(&mut steps, next_order, "Run route", cycle_legs, "Route leg");
    let _ = extend_steps(&mut steps, next_order, "Exit route", exit_legs, "Executor sent");
    steps
}

fn extend_steps(
    steps: &mut Vec<Value>,
    start_order: usize,
    title: &str,
    flows: &[Value],
    prefix: &str,
) -> usize {
    for (index, flow) in flows.iter().enumerate() {
        let token = flow
            .get("token")
            .cloned()
            .unwrap_or_else(|| token_descriptor(&AssetType::Native, None));
        let symbol = token
            .get("symbol")
            .and_then(|value| value.as_str())
            .unwrap_or("asset");
        let amount_raw = flow.get("amount_raw").and_then(|value| value.as_str()).unwrap_or("n/a");
        let from_address = flow.get("from_address").and_then(|value| value.as_str()).unwrap_or("n/a");
        let to_address = flow.get("to_address").and_then(|value| value.as_str()).unwrap_or("n/a");
        steps.push(json!({
            "order": start_order + index,
            "title": title,
            "summary": format!(
                "{} {} {} from {} to {}.",
                prefix,
                amount_raw,
                symbol,
                short_address(from_address),
                short_address(to_address)
            ),
            "token": token,
            "amount_raw": amount_raw,
            "from_address": from_address,
            "to_address": to_address,
        }));
    }
    start_order + flows.len()
}

fn build_liquidation_headline(protocol: &str, net_effect: &Value) -> String {
    let gained = net_effect
        .get("gained")
        .and_then(|value| value.as_array())
        .and_then(|items| items.first());
    let spent = net_effect
        .get("spent")
        .and_then(|value| value.as_array())
        .and_then(|items| items.first());
    match (spent, gained) {
        (Some(spent), Some(gained)) => format!(
            "{} liquidation: strategy spent {} {} and received {} {}.",
            protocol,
            spent.get("amount_raw").and_then(|value| value.as_str()).unwrap_or("n/a"),
            spent
                .get("token")
                .and_then(|value| value.get("symbol"))
                .and_then(|value| value.as_str())
                .unwrap_or("asset"),
            gained.get("amount_raw").and_then(|value| value.as_str()).unwrap_or("n/a"),
            gained
                .get("token")
                .and_then(|value| value.get("symbol"))
                .and_then(|value| value.as_str())
                .unwrap_or("asset"),
        ),
        _ => format!("{protocol} liquidation detected for the strategy scope."),
    }
}

fn build_arbitrage_headline(protocol: &str, net_effect: &Value) -> String {
    let gained_count = net_effect
        .get("gained")
        .and_then(|value| value.as_array())
        .map(|items| items.len())
        .unwrap_or(0);
    format!("{protocol} arbitrage-like execution detected with {gained_count} incoming asset legs.")
}

fn token_descriptor(asset_type: &AssetType, token_address: Option<&str>) -> Value {
    match asset_type {
        AssetType::Native => json!({
            "symbol": "ETH",
            "name": "Ether",
            "address": null,
        }),
        AssetType::Erc20 => {
            let address = token_address.unwrap_or_default().to_lowercase();
            let (symbol, name) = match address.as_str() {
                "0x4200000000000000000000000000000000000006" => ("WETH", "Wrapped Ether"),
                "0x091a5abe6616e26268e5eecff256c2212fce2707" => ("BondETH", "Bond ETH"),
                _ => ("ERC20", "Unlabeled token"),
            };
            json!({
                "symbol": symbol,
                "name": name,
                "address": token_address,
            })
        }
    }
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
            let token = token_descriptor(&flow.asset_type, flow.token_address.as_deref());
            json!({
                "asset_type": match flow.asset_type {
                    AssetType::Native => "native",
                    AssetType::Erc20 => "erc20",
                },
                "token": token,
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
