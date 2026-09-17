use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::domain::{
    entities::{RecognizerResult, TransactionAnalysis},
    enums::AssetType,
    value_objects::{AssetFlow, EvidenceRef},
};

const AAVE_V3_FLASH_LOAN_SIMPLE_SELECTOR: &str = "0x42b0b77c";
const AAVE_V3_FLASH_LOAN_SELECTOR: &str = "0xab9c4b5d";
const BALANCER_VAULT_FLASH_LOAN_SELECTOR: &str = "0x5c38449e";
const UNISWAP_V3_FLASH_SELECTOR: &str = "0x490e6cbc";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum DecodedActionKind {
    Liquidation,
    Swap,
    Flashloan,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct DecodedAction {
    pub action_id: String,
    pub kind: DecodedActionKind,
    pub protocol: Option<String>,
    pub confidence: f32,
    pub roles: Value,
    pub assets: Value,
    pub flow_ids: Vec<String>,
    pub evidence_refs: Vec<EvidenceRef>,
    pub source: String,
    pub notes: Vec<String>,
}

#[derive(Default, Clone)]
pub struct ActionDecoderService;

impl ActionDecoderService {
    pub fn decode(&self, analysis: &TransactionAnalysis) -> Vec<DecodedAction> {
        let mut actions = Vec::new();
        actions.extend(decode_flashloan_actions(analysis));
        actions.extend(decode_liquidation_actions(analysis));
        actions.extend(decode_swap_actions(analysis));
        actions
    }
}

fn decode_liquidation_actions(analysis: &TransactionAnalysis) -> Vec<DecodedAction> {
    analysis
        .recognizer_results
        .iter()
        .filter(|result| result.label == "liquidation_like_activity")
        .filter_map(|result| {
            let adapter = result.attributes.get("adapter_attributes")?;
            let protocol = result
                .attributes
                .get("protocol")
                .and_then(|value| value.as_str())
                .map(str::to_owned);
            let protocol_contract = adapter
                .get("protocol_contract")
                .and_then(|value| value.as_str())
                .map(str::to_owned);
            let flow_ids = protocol_contract
                .as_deref()
                .map(|contract| flows_touching_address(analysis, contract))
                .unwrap_or_default();
            Some(DecodedAction {
                action_id: "liquidation:0".to_owned(),
                kind: DecodedActionKind::Liquidation,
                protocol,
                confidence: result.confidence,
                roles: json!({
                    "sender": result.attributes.get("sender").cloned(),
                    "target": result.attributes.get("target").cloned(),
                    "protocol_contract": protocol_contract,
                    "liquidator": adapter.get("liquidator").cloned(),
                    "borrower": adapter.get("borrower").cloned(),
                    "market_id": adapter.get("market_id").cloned(),
                }),
                assets: json!({
                    "matched_liquidation_selectors": result.attributes.get("matched_liquidation_selectors").cloned(),
                    "matched_event_topic": result.attributes.get("matched_event_topic").cloned(),
                }),
                flow_ids,
                evidence_refs: result.evidence_refs.clone(),
                source: result.recognizer_id.clone(),
                notes: result
                    .warnings
                    .iter()
                    .chain(result.unresolved_points.iter().map(|item| &item.message))
                    .cloned()
                    .collect(),
            })
        })
        .collect()
}

fn decode_swap_actions(analysis: &TransactionAnalysis) -> Vec<DecodedAction> {
    let swap_signal = best_recognizer(analysis, &["approval_then_swap_like", "swap_like_activity"]);
    let Some(subject) = find_swap_subject(analysis) else {
        return Vec::new();
    };

    let flow_ids = subject
        .outgoing
        .iter()
        .chain(subject.incoming.iter())
        .map(|flow| flow.flow_id.clone())
        .collect::<Vec<_>>();
    let evidence_refs = subject
        .outgoing
        .iter()
        .chain(subject.incoming.iter())
        .flat_map(|flow| flow.evidence_refs.clone())
        .collect::<Vec<_>>();
    let source = swap_signal
        .map(|result| result.recognizer_id.clone())
        .unwrap_or_else(|| "asset_exchange_heuristic".to_owned());
    let confidence = swap_signal.map(|result| result.confidence).unwrap_or(0.55);
    let protocol = swap_protocol_hint(analysis).or_else(|| analysis.to_address.clone());

    vec![DecodedAction {
        action_id: "swap:0".to_owned(),
        kind: DecodedActionKind::Swap,
        protocol,
        confidence,
        roles: json!({
            "subject": subject.address,
            "subject_role": subject.role,
            "initiator": analysis.from_address,
            "executor": analysis.to_address,
            "router_or_counterparty": swap_protocol_hint(analysis).or_else(|| analysis.to_address.clone()),
        }),
        assets: json!({
            "token_in": subject.outgoing.first().map(|flow| flow_to_json(flow)),
            "token_out": subject.incoming.first().map(|flow| flow_to_json(flow)),
            "outgoing_count": subject.outgoing.len(),
            "incoming_count": subject.incoming.len(),
        }),
        flow_ids,
        evidence_refs,
        source,
        notes: vec!["Swap action is inferred from subject-scope token in/out; protocol-specific swap decoding is not attached yet.".to_owned()],
    }]
}

struct SwapSubject<'a> {
    address: String,
    role: &'static str,
    outgoing: Vec<&'a AssetFlow>,
    incoming: Vec<&'a AssetFlow>,
}

fn find_swap_subject(analysis: &TransactionAnalysis) -> Option<SwapSubject<'_>> {
    let mut candidates = vec![("initiator", analysis.from_address.to_lowercase())];
    if let Some(executor) = analysis
        .to_address
        .as_ref()
        .map(|value| value.to_lowercase())
    {
        if !candidates
            .iter()
            .any(|(_, address)| address.eq_ignore_ascii_case(&executor))
        {
            candidates.push(("executor", executor));
        }
    }

    candidates.into_iter().find_map(|(role, address)| {
        let outgoing = analysis
            .derived_flows
            .iter()
            .filter(|flow| flow.from_address.eq_ignore_ascii_case(&address))
            .collect::<Vec<_>>();
        let incoming = analysis
            .derived_flows
            .iter()
            .filter(|flow| flow.to_address.eq_ignore_ascii_case(&address))
            .collect::<Vec<_>>();
        if outgoing.is_empty() || incoming.is_empty() {
            None
        } else {
            Some(SwapSubject {
                address,
                role,
                outgoing,
                incoming,
            })
        }
    })
}

fn decode_flashloan_actions(analysis: &TransactionAnalysis) -> Vec<DecodedAction> {
    let mut actions = Vec::new();
    collect_flashloan_actions(
        analysis,
        &analysis.normalized_execution_facts.call_tree,
        0,
        &mut actions,
    );
    actions
}

fn collect_flashloan_actions(
    analysis: &TransactionAnalysis,
    node: &Value,
    depth: usize,
    actions: &mut Vec<DecodedAction>,
) {
    let Some(object) = node.as_object() else {
        return;
    };
    let selector = object
        .get("input_data")
        .and_then(|value| value.as_str())
        .filter(|value| value.len() >= 10)
        .map(|value| value[..10].to_lowercase());
    if let Some(selector) = selector.as_deref() {
        if let Some((protocol, label)) = flashloan_protocol_hint(selector) {
            let lender = object
                .get("to_address")
                .and_then(|value| value.as_str())
                .map(str::to_owned);
            let flow_ids = lender
                .as_deref()
                .map(|address| flows_touching_address(analysis, address))
                .unwrap_or_default();
            actions.push(DecodedAction {
                action_id: format!("flashloan:{}", actions.len()),
                kind: DecodedActionKind::Flashloan,
                protocol: Some(protocol.to_owned()),
                confidence: 0.72,
                roles: json!({
                    "lender": lender,
                    "caller": object.get("from_address").and_then(|value| value.as_str()),
                    "depth": depth,
                }),
                assets: json!({
                    "selector": selector,
                    "label": label,
                }),
                flow_ids,
                evidence_refs: analysis.raw_evidence_refs.clone(),
                source: "call_selector_decoder".to_owned(),
                notes: vec!["Flashloan action is selector-based; exact principal and fee attribution requires protocol-specific decoding.".to_owned()],
            });
        }
    }

    if let Some(children) = object.get("children").and_then(|value| value.as_array()) {
        for child in children {
            collect_flashloan_actions(analysis, child, depth + 1, actions);
        }
    }
}

fn best_recognizer<'a>(
    analysis: &'a TransactionAnalysis,
    labels: &[&str],
) -> Option<&'a RecognizerResult> {
    analysis
        .recognizer_results
        .iter()
        .filter(|result| labels.iter().any(|label| result.label == *label))
        .max_by(|left, right| left.confidence.total_cmp(&right.confidence))
}

fn swap_protocol_hint(analysis: &TransactionAnalysis) -> Option<String> {
    analysis.recognizer_results.iter().find_map(|result| {
        result
            .attributes
            .get("approvals")
            .and_then(|value| value.as_array())
            .and_then(|items| items.first())
            .and_then(|item| item.get("spender"))
            .and_then(|value| value.as_str())
            .map(str::to_owned)
    })
}

fn flows_touching_address(analysis: &TransactionAnalysis, address: &str) -> Vec<String> {
    analysis
        .derived_flows
        .iter()
        .filter(|flow| {
            flow.from_address.eq_ignore_ascii_case(address)
                || flow.to_address.eq_ignore_ascii_case(address)
        })
        .map(|flow| flow.flow_id.clone())
        .collect()
}

fn flow_to_json(flow: &AssetFlow) -> Value {
    json!({
        "flow_id": flow.flow_id,
        "asset_type": match flow.asset_type {
            AssetType::Native => "native",
            AssetType::Erc20 => "erc20",
        },
        "token_address": flow.token_address,
        "from_address": flow.from_address,
        "to_address": flow.to_address,
        "amount_raw": flow.amount_raw,
        "metadata": flow.metadata,
    })
}

fn flashloan_protocol_hint(selector: &str) -> Option<(&'static str, &'static str)> {
    match selector {
        AAVE_V3_FLASH_LOAN_SIMPLE_SELECTOR | AAVE_V3_FLASH_LOAN_SELECTOR => {
            Some(("aave_v3", "Aave flash loan"))
        }
        BALANCER_VAULT_FLASH_LOAN_SELECTOR => Some(("balancer_vault", "Balancer flash loan")),
        UNISWAP_V3_FLASH_SELECTOR => Some(("uniswap_v3", "Uniswap V3 flash")),
        _ => None,
    }
}
