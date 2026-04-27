use std::collections::BTreeSet;
use std::sync::Arc;

use serde_json::json;

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
                message: "Swap-like token exchange detected, but protocol classification is unresolved."
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
        let target = analysis.to_address.clone().unwrap_or_default().to_lowercase();
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
        Arc::new(FailedExecutionClassifier),
        Arc::new(Erc20TransferFlowRecognizer),
        Arc::new(Erc20ApprovalRecognizer),
        Arc::new(NativeTransferRecognizer),
        Arc::new(BatchRouterHeuristicRecognizer),
        Arc::new(SwapFlowRecognizer),
        Arc::new(SampleScenarioRecognizer),
        Arc::new(BridgeLikeTransferRecognizer),
        Arc::new(DepositWithdrawPatternRecognizer),
    ]
}
