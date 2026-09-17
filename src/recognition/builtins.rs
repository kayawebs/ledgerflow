use std::collections::BTreeSet;
use std::sync::Arc;

use serde_json::{Value, json};

use crate::core::errors::{LedgerError, LedgerResult};
use crate::domain::entities::{RecognizerResult, TransactionAnalysis};
use crate::domain::enums::{AssetType, EvidenceKind, TransactionStatus};
use crate::domain::interfaces::Recognizer;
use crate::domain::value_objects::{EvidenceRef, UnresolvedItem};

fn selector_candidates(selector: &str) -> Vec<String> {
    match selector {
        "0xa9059cbb" => vec!["transfer(address,uint256)".to_owned()],
        "0x095ea7b3" => vec!["approve(address,uint256)".to_owned()],
        "0x38ed1739" => {
            vec!["swapExactTokensForTokens(uint256,uint256,address[],address,uint256)".to_owned()]
        }
        _ => Vec::new(),
    }
}

fn token_net_delta_entries(analysis: &TransactionAnalysis) -> Vec<(String, String, i128)> {
    analysis
        .normalized_execution_facts
        .derived_facts
        .get("token_net_deltas")
        .and_then(|value| value.as_array())
        .into_iter()
        .flatten()
        .filter_map(|item| {
            Some((
                item.get("address")?.as_str()?.to_lowercase(),
                item.get("token_address")?.as_str()?.to_lowercase(),
                item.get("delta_raw")?.as_str()?.parse::<i128>().ok()?,
            ))
        })
        .collect()
}

fn approval_edge_entries(analysis: &TransactionAnalysis) -> Vec<(String, String, String)> {
    analysis
        .normalized_execution_facts
        .derived_facts
        .get("approval_edges")
        .and_then(|value| value.as_array())
        .into_iter()
        .flatten()
        .filter_map(|item| {
            Some((
                item.get("owner")?.as_str()?.to_lowercase(),
                item.get("spender")?.as_str()?.to_lowercase(),
                item.get("token_address")?.as_str()?.to_lowercase(),
            ))
        })
        .collect()
}

fn trace_selectors(analysis: &TransactionAnalysis) -> BTreeSet<String> {
    analysis
        .normalized_execution_facts
        .derived_facts
        .get("trace_selectors")
        .and_then(|value| value.as_array())
        .into_iter()
        .flatten()
        .filter_map(|item| item.as_str().map(|value| value.to_lowercase()))
        .collect()
}

fn log_topic_zeroes(analysis: &TransactionAnalysis) -> BTreeSet<String> {
    analysis
        .normalized_execution_facts
        .log_summaries
        .iter()
        .filter_map(|item| {
            item.get("topic_0")
                .and_then(|value| value.as_str())
                .map(|value| value.to_lowercase())
        })
        .collect()
}

#[derive(Clone, Debug)]
struct LogSignal {
    address: String,
    topics: Vec<String>,
}

fn log_signals(analysis: &TransactionAnalysis) -> Vec<LogSignal> {
    analysis
        .normalized_execution_facts
        .log_summaries
        .iter()
        .filter_map(|item| {
            Some(LogSignal {
                address: item.get("address")?.as_str()?.to_lowercase(),
                topics: item
                    .get("topics")?
                    .as_array()?
                    .iter()
                    .filter_map(|value| value.as_str().map(|value| value.to_lowercase()))
                    .collect(),
            })
        })
        .collect()
}

pub struct ProtocolDeploymentEvidenceRecognizer;

impl Recognizer for ProtocolDeploymentEvidenceRecognizer {
    fn recognizer_id(&self) -> &str {
        "protocol_deployment_evidence"
    }
    fn version(&self) -> &str {
        "1.0.0"
    }
    fn order(&self) -> u32 {
        15
    }
    fn description(&self) -> &str {
        "Recognizes protocols only from versioned database deployment evidence for contracts touched by the transaction."
    }

    fn recognize(&self, analysis: &TransactionAnalysis) -> LedgerResult<Option<RecognizerResult>> {
        let mut matches = analysis
            .normalized_execution_facts
            .derived_facts
            .get("protocol_evidence")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter(|item| {
                item.get("confidence")
                    .and_then(Value::as_f64)
                    .is_some_and(|confidence| confidence >= 0.5)
            })
            .cloned()
            .collect::<Vec<_>>();
        if matches.is_empty() {
            return Ok(None);
        }
        matches.sort_by(|a, b| {
            b.get("confidence")
                .and_then(Value::as_f64)
                .unwrap_or_default()
                .total_cmp(
                    &a.get("confidence")
                        .and_then(Value::as_f64)
                        .unwrap_or_default(),
                )
                .then_with(|| {
                    a.get("protocol_name")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .cmp(
                            b.get("protocol_name")
                                .and_then(Value::as_str)
                                .unwrap_or_default(),
                        )
                })
        });
        let primary = &matches[0];
        let protocol = primary
            .get("protocol_name")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                LedgerError::MalformedEvidence(
                    "protocol deployment evidence missing protocol_name".to_owned(),
                )
            })?;
        let protocols = matches
            .iter()
            .filter_map(|item| item.get("protocol_name").and_then(Value::as_str))
            .map(str::to_owned)
            .collect::<BTreeSet<_>>();
        let evidence_refs = matches
            .iter()
            .filter_map(|item| {
                let address = item.get("address")?.as_str()?;
                Some(EvidenceRef {
                    kind: EvidenceKind::DerivedFact,
                    reference: format!("{}:protocol-deployment:{address}", analysis.tx_hash),
                    tx_hash: Some(analysis.tx_hash.clone()),
                    metadata: json!({
                        "source": item.get("source"),
                        "adapter_version": item.get("adapter_version"),
                    }),
                })
            })
            .collect();
        Ok(Some(RecognizerResult {
            recognizer_id: self.recognizer_id().to_owned(),
            recognizer_version: self.version().to_owned(),
            label: "database_protocol_interaction".to_owned(),
            confidence: primary
                .get("confidence")
                .and_then(Value::as_f64)
                .unwrap_or_default() as f32,
            evidence_refs,
            attributes: json!({
                "protocol": protocol,
                "protocols": protocols,
                "source": "protocol_deployments",
                "matches": matches,
            }),
            unresolved_points: Vec::new(),
            warnings: Vec::new(),
        }))
    }
}

fn topic_to_address(topic: &str) -> Option<String> {
    let trimmed = topic.trim_start_matches("0x");
    if trimmed.len() < 40 {
        return None;
    }
    Some(format!("0x{}", &trimmed[trimmed.len() - 40..]).to_lowercase())
}

fn is_known_liquidation_selector(selector: &str) -> bool {
    matches!(
        selector,
        "0x00a718a9" // Aave liquidationCall(address,address,address,uint256,bool)
            | "0xf5e3c462" // Compound v2 liquidateBorrow(address,uint256,address)
            | "0xc1342574" // Euler liquidate(address,address,uint256,uint256)
            | "0x267a47ca" // Morpho-style liquidate(address,address,uint256,address,bool)
    )
}

const MORPHO_BLUE_LIQUIDATE_TOPIC: &str =
    "0xa4946ede45d0c6f06a0f5ce92c9ad3b4751452d2fe0e25010783bcab57a67e41";
const AAVE_LIQUIDATION_CALL_TOPIC: &str =
    "0xe413a321e8681d831f4dbccbca790d2952b56f977908e45be37335533e005286";

#[derive(Clone, Debug)]
struct LiquidationSignals {
    sender: String,
    target: Option<String>,
    selector: String,
    trace_selectors: BTreeSet<String>,
    internal_call_count: u32,
    log_topic_zeroes: BTreeSet<String>,
    logs: Vec<LogSignal>,
}

impl LiquidationSignals {
    fn from_analysis(analysis: &TransactionAnalysis) -> Self {
        Self {
            sender: analysis.from_address.to_lowercase(),
            target: analysis
                .to_address
                .as_ref()
                .map(|value| value.to_lowercase()),
            selector: analysis
                .normalized_execution_facts
                .selector
                .clone()
                .unwrap_or_default()
                .to_lowercase(),
            trace_selectors: trace_selectors(analysis),
            internal_call_count: analysis.normalized_execution_facts.internal_call_count,
            log_topic_zeroes: log_topic_zeroes(analysis),
            logs: log_signals(analysis),
        }
    }

    fn matched_liquidation_selectors(&self) -> BTreeSet<String> {
        std::iter::once(self.selector.clone())
            .chain(self.trace_selectors.iter().cloned())
            .filter(|value| is_known_liquidation_selector(value))
            .collect()
    }
}

#[derive(Clone, Debug)]
struct LiquidationDetection {
    protocol: &'static str,
    confidence: f32,
    matched_selectors: BTreeSet<String>,
    matched_event_topic: Option<&'static str>,
    attributes: serde_json::Value,
    warnings: Vec<String>,
    unresolved_code: &'static str,
    unresolved_message: &'static str,
}

trait LiquidationProtocolAdapter {
    fn protocol_id(&self) -> &'static str;
    fn prefilter(&self, signals: &LiquidationSignals) -> bool;
    fn detect(&self, signals: &LiquidationSignals) -> Option<LiquidationDetection>;
}

struct MorphoBlueLiquidationAdapter;

impl LiquidationProtocolAdapter for MorphoBlueLiquidationAdapter {
    fn protocol_id(&self) -> &'static str {
        "morpho_blue"
    }

    fn prefilter(&self, signals: &LiquidationSignals) -> bool {
        signals
            .log_topic_zeroes
            .contains(MORPHO_BLUE_LIQUIDATE_TOPIC)
            || signals
                .matched_liquidation_selectors()
                .contains("0x267a47ca")
    }

    fn detect(&self, signals: &LiquidationSignals) -> Option<LiquidationDetection> {
        let matched_selectors = signals.matched_liquidation_selectors();
        let event_log = signals.logs.iter().find(|log| {
            log.topics
                .first()
                .map(|topic| topic == MORPHO_BLUE_LIQUIDATE_TOPIC)
                .unwrap_or(false)
        });
        if event_log.is_none() && !matched_selectors.contains("0x267a47ca") {
            return None;
        }
        let borrower = event_log
            .and_then(|log| log.topics.get(3))
            .and_then(|value| topic_to_address(value));
        let liquidator = event_log
            .and_then(|log| log.topics.get(2))
            .and_then(|value| topic_to_address(value));
        let market_id = event_log.and_then(|log| log.topics.get(1)).cloned();
        Some(LiquidationDetection {
            protocol: self.protocol_id(),
            confidence: if event_log.is_some() { 0.99 } else { 0.87 },
            matched_selectors,
            matched_event_topic: if event_log.is_some() {
                Some(MORPHO_BLUE_LIQUIDATE_TOPIC)
            } else {
                None
            },
            attributes: json!({
                "borrower": borrower,
                "liquidator": liquidator,
                "market_id": market_id,
                "protocol_contract": event_log.map(|log| log.address.clone()),
                "internal_call_count": signals.internal_call_count,
            }),
            warnings: if event_log.is_some() {
                Vec::new()
            } else {
                vec!["selector_matched_without_receipt_event".to_owned()]
            },
            unresolved_code: "morpho_liquidation_amounts_partial",
            unresolved_message: "Morpho Blue liquidation event matched, but exact repay/seize amounts are not fully normalized yet.",
        })
    }
}

struct AaveLiquidationAdapter;

impl LiquidationProtocolAdapter for AaveLiquidationAdapter {
    fn protocol_id(&self) -> &'static str {
        "aave"
    }

    fn prefilter(&self, signals: &LiquidationSignals) -> bool {
        signals
            .matched_liquidation_selectors()
            .contains("0x00a718a9")
            || signals
                .log_topic_zeroes
                .contains(AAVE_LIQUIDATION_CALL_TOPIC)
    }

    fn detect(&self, signals: &LiquidationSignals) -> Option<LiquidationDetection> {
        let matched_selectors = signals.matched_liquidation_selectors();
        let matched_event = signals
            .log_topic_zeroes
            .contains(AAVE_LIQUIDATION_CALL_TOPIC);
        if !matched_selectors.contains("0x00a718a9") && !matched_event {
            return None;
        }
        Some(LiquidationDetection {
            protocol: self.protocol_id(),
            confidence: if matched_event {
                0.96
            } else if signals.selector == "0x00a718a9" {
                0.94
            } else {
                0.86
            },
            matched_selectors,
            matched_event_topic: if matched_event {
                Some(AAVE_LIQUIDATION_CALL_TOPIC)
            } else {
                None
            },
            attributes: json!({
                "internal_call_count": signals.internal_call_count,
            }),
            warnings: vec!["selector_matched_without_protocol_event_adapter".to_owned()],
            unresolved_code: "aave_liquidation_roles_partial",
            unresolved_message: "Aave liquidation selector matched, but borrower and asset roles are only partially decoded.",
        })
    }
}

struct CompoundLiquidationAdapter;

impl LiquidationProtocolAdapter for CompoundLiquidationAdapter {
    fn protocol_id(&self) -> &'static str {
        "compound_v2"
    }

    fn prefilter(&self, signals: &LiquidationSignals) -> bool {
        signals
            .matched_liquidation_selectors()
            .contains("0xf5e3c462")
    }

    fn detect(&self, signals: &LiquidationSignals) -> Option<LiquidationDetection> {
        let matched_selectors = signals.matched_liquidation_selectors();
        if !matched_selectors.contains("0xf5e3c462") {
            return None;
        }
        Some(LiquidationDetection {
            protocol: self.protocol_id(),
            confidence: if signals.selector == "0xf5e3c462" {
                0.93
            } else {
                0.85
            },
            matched_selectors,
            matched_event_topic: None,
            attributes: json!({
                "internal_call_count": signals.internal_call_count,
            }),
            warnings: vec!["selector_matched_without_protocol_event_adapter".to_owned()],
            unresolved_code: "compound_liquidation_roles_partial",
            unresolved_message: "Compound liquidation selector matched, but borrower and asset roles are only partially decoded.",
        })
    }
}

struct EulerLiquidationAdapter;

impl LiquidationProtocolAdapter for EulerLiquidationAdapter {
    fn protocol_id(&self) -> &'static str {
        "euler"
    }

    fn prefilter(&self, signals: &LiquidationSignals) -> bool {
        signals
            .matched_liquidation_selectors()
            .contains("0xc1342574")
    }

    fn detect(&self, signals: &LiquidationSignals) -> Option<LiquidationDetection> {
        let matched_selectors = signals.matched_liquidation_selectors();
        if !matched_selectors.contains("0xc1342574") {
            return None;
        }
        Some(LiquidationDetection {
            protocol: self.protocol_id(),
            confidence: if signals.selector == "0xc1342574" {
                0.93
            } else {
                0.84
            },
            matched_selectors,
            matched_event_topic: None,
            attributes: json!({
                "internal_call_count": signals.internal_call_count,
            }),
            warnings: vec!["selector_matched_without_protocol_event_adapter".to_owned()],
            unresolved_code: "euler_liquidation_roles_partial",
            unresolved_message: "Euler liquidation selector matched, but account and collateral/debt roles are only partially decoded.",
        })
    }
}

fn liquidation_adapters() -> [Box<dyn LiquidationProtocolAdapter>; 4] {
    [
        Box::new(MorphoBlueLiquidationAdapter),
        Box::new(AaveLiquidationAdapter),
        Box::new(CompoundLiquidationAdapter),
        Box::new(EulerLiquidationAdapter),
    ]
}

pub struct SelectorPresenceRecognizer;

impl Recognizer for SelectorPresenceRecognizer {
    fn recognizer_id(&self) -> &str {
        "selector_presence"
    }
    fn version(&self) -> &str {
        "1.0.0"
    }
    fn order(&self) -> u32 {
        10
    }
    fn description(&self) -> &str {
        "Extracts selector presence without overclaiming intent."
    }

    fn recognize(&self, analysis: &TransactionAnalysis) -> LedgerResult<Option<RecognizerResult>> {
        let Some(selector) = analysis.normalized_execution_facts.selector.clone() else {
            return Ok(None);
        };
        Ok(Some(RecognizerResult {
            recognizer_id: self.recognizer_id().to_owned(),
            recognizer_version: self.version().to_owned(),
            label: "selector_present".to_owned(),
            confidence: 0.99,
            evidence_refs: vec![EvidenceRef {
                kind: EvidenceKind::DerivedFact,
                reference: format!("{}:selector", analysis.tx_hash),
                tx_hash: Some(analysis.tx_hash.clone()),
                metadata: json!({}),
            }],
            attributes: json!({
                "selector": selector,
                "candidate_signatures": selector_candidates(analysis.normalized_execution_facts.selector.as_deref().unwrap_or_default()),
            }),
            unresolved_points: Vec::new(),
            warnings: Vec::new(),
        }))
    }
}

pub struct Erc20TransferFlowRecognizer;

impl Recognizer for Erc20TransferFlowRecognizer {
    fn recognizer_id(&self) -> &str {
        "erc20_transfer_flow"
    }
    fn version(&self) -> &str {
        "1.0.0"
    }
    fn order(&self) -> u32 {
        20
    }
    fn description(&self) -> &str {
        "Recognizes straightforward ERC20 transfer behavior from flows."
    }

    fn recognize(&self, analysis: &TransactionAnalysis) -> LedgerResult<Option<RecognizerResult>> {
        let token_flows = analysis
            .derived_flows
            .iter()
            .filter(|flow| flow.asset_type == AssetType::Erc20)
            .cloned()
            .collect::<Vec<_>>();
        if token_flows.is_empty() {
            return Ok(None);
        }
        let tokens = token_flows
            .iter()
            .filter_map(|flow| flow.token_address.clone())
            .collect::<BTreeSet<_>>();
        Ok(Some(RecognizerResult {
            recognizer_id: self.recognizer_id().to_owned(),
            recognizer_version: self.version().to_owned(),
            label: "erc20_transfer_activity".to_owned(),
            confidence: 0.9,
            evidence_refs: token_flows
                .iter()
                .flat_map(|flow| flow.evidence_refs.clone())
                .collect(),
            attributes: json!({
                "token_addresses": tokens,
                "flow_count": token_flows.len(),
            }),
            unresolved_points: Vec::new(),
            warnings: Vec::new(),
        }))
    }
}

pub struct NativeTransferRecognizer;

impl Recognizer for NativeTransferRecognizer {
    fn recognizer_id(&self) -> &str {
        "native_transfer"
    }
    fn version(&self) -> &str {
        "1.0.0"
    }
    fn order(&self) -> u32 {
        30
    }
    fn description(&self) -> &str {
        "Recognizes plain native transfers from explicit value movement."
    }

    fn recognize(&self, analysis: &TransactionAnalysis) -> LedgerResult<Option<RecognizerResult>> {
        let native_flows = analysis
            .derived_flows
            .iter()
            .filter(|flow| flow.asset_type == AssetType::Native)
            .cloned()
            .collect::<Vec<_>>();
        if native_flows.is_empty() {
            return Ok(None);
        }
        let confidence =
            if native_flows.len() == 1 && analysis.normalized_execution_facts.event_count == 0 {
                0.98
            } else {
                0.72
            };
        Ok(Some(RecognizerResult {
            recognizer_id: self.recognizer_id().to_owned(),
            recognizer_version: self.version().to_owned(),
            label: "native_transfer_activity".to_owned(),
            confidence,
            evidence_refs: native_flows
                .iter()
                .flat_map(|flow| flow.evidence_refs.clone())
                .collect(),
            attributes: json!({ "flow_count": native_flows.len() }),
            unresolved_points: Vec::new(),
            warnings: Vec::new(),
        }))
    }
}

pub struct BatchRouterHeuristicRecognizer;

impl Recognizer for BatchRouterHeuristicRecognizer {
    fn recognizer_id(&self) -> &str {
        "batch_router_heuristic"
    }
    fn version(&self) -> &str {
        "1.0.0"
    }
    fn order(&self) -> u32 {
        40
    }
    fn description(&self) -> &str {
        "Heuristic recognizer for batch-router style execution."
    }

    fn recognize(&self, analysis: &TransactionAnalysis) -> LedgerResult<Option<RecognizerResult>> {
        let token_flow_count = analysis
            .derived_flows
            .iter()
            .filter(|flow| flow.asset_type == AssetType::Erc20)
            .count();
        let internal_call_count = analysis.normalized_execution_facts.internal_call_count;
        if internal_call_count < 2 || token_flow_count < 2 {
            return Ok(None);
        }
        let mut warnings = Vec::new();
        let mut confidence = 0.62;
        if analysis
            .normalized_execution_facts
            .selector
            .as_deref()
            .map(|selector| !selector_candidates(selector).is_empty())
            .unwrap_or(false)
        {
            warnings.push("known selector present; heuristic label only".to_owned());
            confidence -= 0.05;
        }
        Ok(Some(RecognizerResult {
            recognizer_id: self.recognizer_id().to_owned(),
            recognizer_version: self.version().to_owned(),
            label: "batch_router_exec".to_owned(),
            confidence,
            evidence_refs: analysis.raw_evidence_refs.clone(),
            attributes: json!({
                "internal_call_count": internal_call_count,
                "token_flow_count": token_flow_count,
                "selector": analysis.normalized_execution_facts.selector,
            }),
            unresolved_points: vec![UnresolvedItem {
                code: "heuristic_only".to_owned(),
                message: "Pattern suggests aggregation or routing, but protocol classification is unresolved.".to_owned(),
                evidence_refs: Vec::new(),
            }],
            warnings,
        }))
    }
}

pub struct SampleScenarioRecognizer;

impl Recognizer for SampleScenarioRecognizer {
    fn recognizer_id(&self) -> &str {
        "approval_swap_scenario"
    }
    fn version(&self) -> &str {
        "1.0.0"
    }
    fn order(&self) -> u32 {
        50
    }
    fn description(&self) -> &str {
        "Narrow scenario recognizer for approval-plus-swap-like sequences."
    }

    fn recognize(&self, analysis: &TransactionAnalysis) -> LedgerResult<Option<RecognizerResult>> {
        let has_approval = analysis
            .normalized_execution_facts
            .log_summaries
            .iter()
            .any(|item| {
                item.get("is_approval")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
            });
        let token_addresses = analysis
            .derived_flows
            .iter()
            .filter(|flow| flow.asset_type == AssetType::Erc20)
            .filter_map(|flow| flow.token_address.clone())
            .collect::<BTreeSet<_>>();
        if !has_approval || token_addresses.len() < 2 {
            return Ok(None);
        }
        let selector = analysis.normalized_execution_facts.selector.clone();
        let confidence = match selector.as_deref() {
            Some("0x095ea7b3") | Some("0x38ed1739") => 0.74,
            _ => 0.64,
        };
        Ok(Some(RecognizerResult {
            recognizer_id: self.recognizer_id().to_owned(),
            recognizer_version: self.version().to_owned(),
            label: "approval_then_swap_like".to_owned(),
            confidence,
            evidence_refs: analysis.raw_evidence_refs.clone(),
            attributes: json!({
                "token_touch_count": token_addresses.len(),
                "selector": selector,
            }),
            unresolved_points: vec![UnresolvedItem {
                code: "protocol_unresolved".to_owned(),
                message: "Swap-like scenario recognized without protocol-specific adapter."
                    .to_owned(),
                evidence_refs: Vec::new(),
            }],
            warnings: Vec::new(),
        }))
    }
}

pub struct Erc20ApprovalRecognizer;

impl Recognizer for Erc20ApprovalRecognizer {
    fn recognizer_id(&self) -> &str {
        "erc20_approval"
    }
    fn version(&self) -> &str {
        "1.0.0"
    }
    fn order(&self) -> u32 {
        25
    }
    fn description(&self) -> &str {
        "Recognizes explicit ERC20 approval activity."
    }

    fn recognize(&self, analysis: &TransactionAnalysis) -> LedgerResult<Option<RecognizerResult>> {
        let approvals = approval_edge_entries(analysis);
        if approvals.is_empty() {
            return Ok(None);
        }
        Ok(Some(RecognizerResult {
            recognizer_id: self.recognizer_id().to_owned(),
            recognizer_version: self.version().to_owned(),
            label: "erc20_approval_activity".to_owned(),
            confidence: 0.97,
            evidence_refs: analysis
                .raw_evidence_refs
                .iter()
                .filter(|item| item.kind == EvidenceKind::Log)
                .cloned()
                .collect(),
            attributes: json!({
                "approvals": approvals.iter().map(|(owner, spender, token_address)| json!({
                    "owner": owner,
                    "spender": spender,
                    "token_address": token_address,
                })).collect::<Vec<_>>(),
                "approval_count": approvals.len(),
            }),
            unresolved_points: Vec::new(),
            warnings: Vec::new(),
        }))
    }
}

pub struct SwapFlowRecognizer;

impl Recognizer for SwapFlowRecognizer {
    fn recognizer_id(&self) -> &str {
        "swap_flow"
    }
    fn version(&self) -> &str {
        "1.0.0"
    }
    fn order(&self) -> u32 {
        45
    }
    fn description(&self) -> &str {
        "Recognizes token-in/token-out swap-like value exchange for the tx sender."
    }

    fn recognize(&self, analysis: &TransactionAnalysis) -> LedgerResult<Option<RecognizerResult>> {
        let sender = analysis.from_address.to_lowercase();
        let deltas = token_net_delta_entries(analysis)
            .into_iter()
            .filter(|(address, _, _)| address == &sender)
            .collect::<Vec<_>>();
        let outgoing = deltas
            .iter()
            .filter(|(_, _, delta)| *delta < 0)
            .map(|(_, token, delta)| (token.clone(), delta.abs()))
            .collect::<Vec<_>>();
        let incoming = deltas
            .iter()
            .filter(|(_, _, delta)| *delta > 0)
            .map(|(_, token, delta)| (token.clone(), *delta))
            .collect::<Vec<_>>();
        if outgoing.is_empty() || incoming.is_empty() {
            return Ok(None);
        }
        let touched_tokens = deltas
            .iter()
            .map(|(_, token, _)| token.clone())
            .collect::<BTreeSet<_>>();
        if touched_tokens.len() < 2 {
            return Ok(None);
        }

        Ok(Some(RecognizerResult {
            recognizer_id: self.recognizer_id().to_owned(),
            recognizer_version: self.version().to_owned(),
            label: "swap_like_activity".to_owned(),
            confidence: if analysis.normalized_execution_facts.internal_call_count > 0 {
                0.86
            } else {
                0.73
            },
            evidence_refs: analysis.raw_evidence_refs.clone(),
            attributes: json!({
                "sender": sender,
                "outgoing_tokens": outgoing.iter().map(|(token_address, amount_raw)| json!({
                    "token_address": token_address,
                    "amount_raw": amount_raw.to_string(),
                })).collect::<Vec<_>>(),
                "incoming_tokens": incoming.iter().map(|(token_address, amount_raw)| json!({
                    "token_address": token_address,
                    "amount_raw": amount_raw.to_string(),
                })).collect::<Vec<_>>(),
            }),
            unresolved_points: vec![UnresolvedItem {
                code: "protocol_unresolved".to_owned(),
                message:
                    "Swap-like token exchange detected, but protocol classification is unresolved."
                        .to_owned(),
                evidence_refs: Vec::new(),
            }],
            warnings: Vec::new(),
        }))
    }
}

pub struct BridgeLikeTransferRecognizer;

impl Recognizer for BridgeLikeTransferRecognizer {
    fn recognizer_id(&self) -> &str {
        "bridge_like_transfer"
    }
    fn version(&self) -> &str {
        "1.0.0"
    }
    fn order(&self) -> u32 {
        55
    }
    fn description(&self) -> &str {
        "Flags simple bridge-like asset exits that send value into a contract with no offsetting inflow."
    }

    fn recognize(&self, analysis: &TransactionAnalysis) -> LedgerResult<Option<RecognizerResult>> {
        let sender = analysis.from_address.to_lowercase();
        let deltas = token_net_delta_entries(analysis)
            .into_iter()
            .filter(|(address, _, _)| address == &sender)
            .collect::<Vec<_>>();
        let outgoing_count = deltas.iter().filter(|(_, _, delta)| *delta < 0).count();
        let incoming_count = deltas.iter().filter(|(_, _, delta)| *delta > 0).count();
        if outgoing_count == 0 || incoming_count > 0 {
            return Ok(None);
        }
        let target = analysis.to_address.clone().unwrap_or_default();
        if target.is_empty() || target == sender {
            return Ok(None);
        }
        Ok(Some(RecognizerResult {
            recognizer_id: self.recognizer_id().to_owned(),
            recognizer_version: self.version().to_owned(),
            label: "bridge_like_asset_exit".to_owned(),
            confidence: 0.58,
            evidence_refs: analysis.raw_evidence_refs.clone(),
            attributes: json!({
                "sender": sender,
                "target": target,
                "outgoing_asset_count": outgoing_count,
                "incoming_asset_count": incoming_count,
            }),
            unresolved_points: vec![UnresolvedItem {
                code: "bridge_destination_unconfirmed".to_owned(),
                message: "Pattern resembles a bridge or escrow-style asset exit, but destination semantics are unresolved.".to_owned(),
                evidence_refs: Vec::new(),
            }],
            warnings: vec!["heuristic_only".to_owned()],
        }))
    }
}

pub struct DepositWithdrawPatternRecognizer;

impl Recognizer for DepositWithdrawPatternRecognizer {
    fn recognizer_id(&self) -> &str {
        "deposit_withdraw_pattern"
    }
    fn version(&self) -> &str {
        "1.0.0"
    }
    fn order(&self) -> u32 {
        60
    }
    fn description(&self) -> &str {
        "Recognizes simple deposit-like or withdraw-like token movement patterns against a target contract."
    }

    fn recognize(&self, analysis: &TransactionAnalysis) -> LedgerResult<Option<RecognizerResult>> {
        let sender = analysis.from_address.to_lowercase();
        let target = analysis
            .to_address
            .clone()
            .unwrap_or_default()
            .to_lowercase();
        if target.is_empty() {
            return Ok(None);
        }
        let token_flows = analysis
            .derived_flows
            .iter()
            .filter(|flow| flow.asset_type == AssetType::Erc20)
            .collect::<Vec<_>>();
        let deposit_like = token_flows.iter().any(|flow| {
            flow.from_address.eq_ignore_ascii_case(&sender)
                && flow.to_address.eq_ignore_ascii_case(&target)
        });
        let withdraw_like = token_flows.iter().any(|flow| {
            flow.from_address.eq_ignore_ascii_case(&target)
                && flow.to_address.eq_ignore_ascii_case(&sender)
        });
        let (label, confidence) = match (deposit_like, withdraw_like) {
            (true, false) => ("deposit_like_activity", 0.68),
            (false, true) => ("withdraw_like_activity", 0.68),
            _ => return Ok(None),
        };
        Ok(Some(RecognizerResult {
            recognizer_id: self.recognizer_id().to_owned(),
            recognizer_version: self.version().to_owned(),
            label: label.to_owned(),
            confidence,
            evidence_refs: analysis.raw_evidence_refs.clone(),
            attributes: json!({
                "sender": sender,
                "target": target,
                "token_flow_count": token_flows.len(),
            }),
            unresolved_points: vec![UnresolvedItem {
                code: "position_accounting_unresolved".to_owned(),
                message: "Deposit/withdraw-like pattern detected without protocol-specific position accounting."
                    .to_owned(),
                evidence_refs: Vec::new(),
            }],
            warnings: Vec::new(),
        }))
    }
}

pub struct ArbitrageLikeScenarioRecognizer;

fn arbitrage_subject_deltas(
    analysis: &TransactionAnalysis,
) -> Option<(&'static str, String, Vec<(String, String, i128)>)> {
    let all_deltas = token_net_delta_entries(analysis);
    let mut candidates = vec![("sender", analysis.from_address.to_lowercase())];
    if let Some(target) = analysis
        .to_address
        .as_ref()
        .map(|value| value.to_lowercase())
    {
        if !candidates
            .iter()
            .any(|(_, address)| address.eq_ignore_ascii_case(&target))
        {
            candidates.push(("executor", target));
        }
    }

    candidates.into_iter().find_map(|(role, address)| {
        let deltas = all_deltas
            .iter()
            .filter(|(entry_address, _, _)| entry_address.eq_ignore_ascii_case(&address))
            .cloned()
            .collect::<Vec<_>>();
        if deltas.is_empty() {
            None
        } else {
            Some((role, address, deltas))
        }
    })
}

fn distinct_flow_counterparties(analysis: &TransactionAnalysis) -> BTreeSet<String> {
    analysis
        .derived_flows
        .iter()
        .flat_map(|flow| {
            [
                flow.from_address.to_ascii_lowercase(),
                flow.to_address.to_ascii_lowercase(),
            ]
        })
        .collect()
}

impl Recognizer for ArbitrageLikeScenarioRecognizer {
    fn recognizer_id(&self) -> &str {
        "arbitrage_like_scenario"
    }
    fn version(&self) -> &str {
        "1.0.0"
    }
    fn order(&self) -> u32 {
        65
    }
    fn description(&self) -> &str {
        "Recognizes simple arbitrage-like multi-leg token cycling patterns."
    }

    fn recognize(&self, analysis: &TransactionAnalysis) -> LedgerResult<Option<RecognizerResult>> {
        let Some((subject_role, subject, deltas)) = arbitrage_subject_deltas(analysis) else {
            return Ok(None);
        };
        let outgoing_count = deltas.iter().filter(|(_, _, delta)| *delta < 0).count();
        let incoming_count = deltas.iter().filter(|(_, _, delta)| *delta > 0).count();
        let touched_tokens = deltas
            .iter()
            .map(|(_, token, _)| token.clone())
            .collect::<BTreeSet<_>>();
        let subject_flow_count = analysis
            .derived_flows
            .iter()
            .filter(|flow| {
                flow.asset_type == AssetType::Erc20
                    && (flow.from_address.eq_ignore_ascii_case(&subject)
                        || flow.to_address.eq_ignore_ascii_case(&subject))
            })
            .count();
        let counterparty_count = distinct_flow_counterparties(analysis).len();
        let route_like_shape = subject_flow_count >= 2
            && counterparty_count >= 3
            && analysis.normalized_execution_facts.event_count >= 2;
        if analysis.normalized_execution_facts.internal_call_count < 1
            || outgoing_count == 0
            || incoming_count == 0
            || touched_tokens.len() < 2
            || (!route_like_shape && subject_flow_count < 4 && touched_tokens.len() < 3)
        {
            return Ok(None);
        }

        let confidence = if analysis.normalized_execution_facts.internal_call_count >= 2
            && subject_flow_count >= 4
        {
            0.67
        } else {
            0.58
        };

        Ok(Some(RecognizerResult {
            recognizer_id: self.recognizer_id().to_owned(),
            recognizer_version: self.version().to_owned(),
            label: "arbitrage_like_activity".to_owned(),
            confidence,
            evidence_refs: analysis.raw_evidence_refs.clone(),
            attributes: json!({
                "subject": subject,
                "subject_role": subject_role,
                "token_touch_count": touched_tokens.len(),
                "subject_flow_count": subject_flow_count,
                "counterparty_count": counterparty_count,
                "route_like_shape": route_like_shape,
                "internal_call_count": analysis.normalized_execution_facts.internal_call_count,
            }),
            unresolved_points: vec![UnresolvedItem {
                code: "profitability_unresolved".to_owned(),
                message: "Arbitrage-like flow pattern detected, but realized profitability and venue classification are unresolved."
                    .to_owned(),
                evidence_refs: Vec::new(),
            }],
            warnings: vec!["heuristic_only".to_owned()],
        }))
    }
}

pub struct LiquidationLikeScenarioRecognizer;

impl Recognizer for LiquidationLikeScenarioRecognizer {
    fn recognizer_id(&self) -> &str {
        "liquidation_like_scenario"
    }
    fn version(&self) -> &str {
        "1.1.0"
    }
    fn order(&self) -> u32 {
        70
    }
    fn description(&self) -> &str {
        "Recognizes liquidation activity through protocol adapters first, then narrow repay-and-seize heuristics."
    }

    fn recognize(&self, analysis: &TransactionAnalysis) -> LedgerResult<Option<RecognizerResult>> {
        let signals = LiquidationSignals::from_analysis(analysis);
        let matched_liquidation_selectors = signals.matched_liquidation_selectors();
        let has_liquidation_event_signal = signals
            .log_topic_zeroes
            .contains(MORPHO_BLUE_LIQUIDATE_TOPIC)
            || signals
                .log_topic_zeroes
                .contains(AAVE_LIQUIDATION_CALL_TOPIC);
        if matched_liquidation_selectors.is_empty()
            && !has_liquidation_event_signal
            && signals.internal_call_count < 2
        {
            return Ok(None);
        }

        for adapter in liquidation_adapters() {
            if !adapter.prefilter(&signals) {
                continue;
            }
            let Some(detection) = adapter.detect(&signals) else {
                continue;
            };
            return Ok(Some(RecognizerResult {
                recognizer_id: self.recognizer_id().to_owned(),
                recognizer_version: self.version().to_owned(),
                label: "liquidation_like_activity".to_owned(),
                confidence: detection.confidence,
                evidence_refs: analysis.raw_evidence_refs.clone(),
                attributes: json!({
                    "protocol": detection.protocol,
                    "sender": signals.sender.clone(),
                    "target": signals.target.clone(),
                    "selector": signals.selector.clone(),
                    "matched_liquidation_selectors": detection.matched_selectors,
                    "matched_event_topic": detection.matched_event_topic,
                    "internal_call_count": signals.internal_call_count,
                    "adapter_attributes": detection.attributes,
                }),
                unresolved_points: vec![UnresolvedItem {
                    code: detection.unresolved_code.to_owned(),
                    message: detection.unresolved_message.to_owned(),
                    evidence_refs: Vec::new(),
                }],
                warnings: detection.warnings,
            }));
        }

        Ok(None)
    }
}

pub struct FailedExecutionClassifier;

impl Recognizer for FailedExecutionClassifier {
    fn recognizer_id(&self) -> &str {
        "failed_execution_classifier"
    }
    fn version(&self) -> &str {
        "1.0.0"
    }
    fn order(&self) -> u32 {
        15
    }
    fn description(&self) -> &str {
        "Classifies reverted or failed top-level execution."
    }

    fn recognize(&self, analysis: &TransactionAnalysis) -> LedgerResult<Option<RecognizerResult>> {
        if analysis.status != TransactionStatus::Failure {
            return Ok(None);
        }
        Ok(Some(RecognizerResult {
            recognizer_id: self.recognizer_id().to_owned(),
            recognizer_version: self.version().to_owned(),
            label: "failed_execution".to_owned(),
            confidence: 0.99,
            evidence_refs: analysis.raw_evidence_refs.clone(),
            attributes: json!({
                "trace_available": analysis.normalized_execution_facts.derived_facts["trace_available"],
                "selector": analysis.normalized_execution_facts.selector,
            }),
            unresolved_points: vec![UnresolvedItem {
                code: "revert_reason_unresolved".to_owned(),
                message: "Execution failed, but a decoded revert reason is not available."
                    .to_owned(),
                evidence_refs: Vec::new(),
            }],
            warnings: Vec::new(),
        }))
    }
}

pub struct FailingRecognizer;

impl Recognizer for FailingRecognizer {
    fn recognizer_id(&self) -> &str {
        "failing_recognizer"
    }
    fn version(&self) -> &str {
        "0.0.1"
    }
    fn order(&self) -> u32 {
        999
    }
    fn description(&self) -> &str {
        "Test recognizer that fails."
    }

    fn recognize(&self, _analysis: &TransactionAnalysis) -> LedgerResult<Option<RecognizerResult>> {
        Err(LedgerError::RecognizerExecution(
            "intentional test failure".to_owned(),
        ))
    }
}

pub fn builtin_recognizers() -> Vec<Arc<dyn Recognizer>> {
    vec![
        Arc::new(SelectorPresenceRecognizer),
        Arc::new(ProtocolDeploymentEvidenceRecognizer),
        Arc::new(FailedExecutionClassifier),
        Arc::new(Erc20TransferFlowRecognizer),
        Arc::new(Erc20ApprovalRecognizer),
        Arc::new(NativeTransferRecognizer),
        Arc::new(BatchRouterHeuristicRecognizer),
        Arc::new(SwapFlowRecognizer),
        Arc::new(SampleScenarioRecognizer),
        Arc::new(BridgeLikeTransferRecognizer),
        Arc::new(DepositWithdrawPatternRecognizer),
        Arc::new(ArbitrageLikeScenarioRecognizer),
        Arc::new(LiquidationLikeScenarioRecognizer),
    ]
}
