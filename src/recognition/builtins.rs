use std::collections::BTreeSet;
use std::sync::Arc;

use serde_json::json;

use crate::core::errors::{LedgerError, LedgerResult};
use crate::domain::entities::{RecognizerResult, TransactionAnalysis};
use crate::domain::enums::{AssetType, EvidenceKind};
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
        Arc::new(Erc20TransferFlowRecognizer),
        Arc::new(NativeTransferRecognizer),
        Arc::new(BatchRouterHeuristicRecognizer),
        Arc::new(SampleScenarioRecognizer),
    ]
}
