use std::collections::BTreeSet;

use chrono::{TimeZone, Utc};
use num_bigint::BigUint;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::domain::{
    entities::TransactionAnalysis, enums::TransactionStatus, value_objects::AssetFlow,
};

use super::{
    EVIDENCE_PACK_SCHEMA_VERSION, PilotConfig, PilotEntity, PilotEventType, PilotEvidencePack,
    PilotRule, RuleFilters, RuleThreshold, RuleThresholds,
};

pub const PILOT_CLASSIFIER_VERSION: &str = "ledgerflow.pilot-classifier.v1.2.0";

#[derive(Clone, Debug, Default)]
pub struct ClassificationContext {
    /// Number of failed transactions from the same subject in the configured window,
    /// including the current transaction.
    pub repeated_failure_count: u32,
}

pub fn classify(
    config: &PilotConfig,
    analysis: &TransactionAnalysis,
    context: &ClassificationContext,
) -> Vec<PilotEvidencePack> {
    if !config.project.enabled || analysis.chain_id != config.project.chain_id {
        return Vec::new();
    }
    let entities = config
        .entities
        .iter()
        .filter(|entity| entity.include)
        .collect::<Vec<_>>();
    let relevant_flows = boundary_flows(analysis, &entities);
    let subject = subject_entity(analysis, &entities, &relevant_flows);
    let protocols = protocol_path(analysis, &entities);
    let known_activity = analysis.recognizer_results.iter().any(|result| {
        !matches!(
            result.label.as_str(),
            "selector_present" | "unknown_activity"
        )
    });
    let labels = analysis
        .recognizer_results
        .iter()
        .map(|r| r.label.to_ascii_lowercase())
        .collect::<Vec<_>>();

    config
        .rules
        .iter()
        .filter(|rule| rule.enabled)
        .filter_map(|rule| {
            if !filters_match(&rule.filters, analysis, &relevant_flows, &protocols) {
                return None;
            }
            let matching_flows = relevant_flows
                .iter()
                .copied()
                .filter(|flow| flow_meets_threshold(flow, rule))
                .collect::<Vec<_>>();
            let gas_cost_raw = exact_gas_cost_raw(analysis);
            let fires = match rule.event_type {
                PilotEventType::LargeAssetFlow => !matching_flows.is_empty(),
                PilotEventType::TreasuryMovement => {
                    subject.as_ref().is_some_and(|entity| {
                        role_is(entity, &["treasury", "reserve", "operations"])
                    }) && !matching_flows.is_empty()
                }
                PilotEventType::ProtocolRevenue => subject.as_ref().is_some_and(|entity| {
                    role_is(entity, &["protocol_revenue", "revenue", "fee_collector"])
                        && matching_flows
                            .iter()
                            .any(|flow| eq_address(&flow.to_address, &entity.address))
                }),
                PilotEventType::DexArbitrage => {
                    labels.iter().any(|label| label.contains("arbitrage"))
                        || subject.as_ref().is_some_and(|entity| {
                            role_is(entity, &["arbitrage_executor", "searcher"])
                        })
                }
                PilotEventType::Liquidation => {
                    labels.iter().any(|label| label.contains("liquidation"))
                }
                PilotEventType::HighCostFailure => {
                    analysis.status == TransactionStatus::Failure
                        && raw_meets(gas_cost_raw.as_deref(), gas_threshold(rule))
                }
                PilotEventType::RepeatedFailedAttempts => {
                    analysis.status == TransactionStatus::Failure
                        && context.repeated_failure_count >= count_threshold(rule).unwrap_or(3)
                }
                PilotEventType::UnknownMaterialInteraction => {
                    !known_activity && !matching_flows.is_empty()
                }
            };
            fires.then(|| {
                build_pack(
                    config,
                    rule,
                    analysis,
                    subject,
                    &matching_flows,
                    &protocols,
                    context,
                )
            })
        })
        .collect()
}

fn build_pack(
    config: &PilotConfig,
    rule: &PilotRule,
    analysis: &TransactionAnalysis,
    subject: Option<&PilotEntity>,
    flows: &[&AssetFlow],
    protocols: &[Value],
    context: &ClassificationContext,
) -> PilotEvidencePack {
    let subject_address = subject
        .map(|v| v.address.as_str())
        .unwrap_or(&analysis.from_address);
    let event_name = super::event_type_name(&rule.event_type);
    let ruleset_id = config_digest(config);
    let event_id = Uuid::new_v5(
        &Uuid::NAMESPACE_URL,
        format!(
            "ledgerflow:{}:{}:{}:{}",
            config.project.project_id,
            analysis.input_digest,
            event_name,
            subject_address.to_ascii_lowercase()
        )
        .as_bytes(),
    );
    let asset_flows = flows
        .iter()
        .filter_map(|flow| serde_json::to_value(flow).ok())
        .collect::<Vec<_>>();
    let key_amount = flows
        .iter()
        .filter_map(|flow| {
            flow.amount_raw
                .parse::<BigUint>()
                .ok()
                .map(|amount| (amount, flow.amount_raw.clone()))
        })
        .max_by(|a, b| a.0.cmp(&b.0))
        .map(|(_, raw)| raw);
    let trace_available = analysis
        .normalized_execution_facts
        .derived_facts
        .get("trace_available")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let gas_cost_raw = exact_gas_cost_raw(analysis);
    let deterministic_profit = verified_profit(analysis)
        .filter(|_| matches!(rule.event_type, PilotEventType::DexArbitrage));
    let mut limitations = Vec::new();
    if !trace_available {
        limitations.push(
            "trace unavailable; internal calls and native internal flows may be incomplete"
                .to_owned(),
        );
    }
    if flows
        .iter()
        .any(|flow| flow.metadata.get("amount_usd").is_none())
    {
        limitations.push(
            "one or more asset flows have no deterministic historical USD valuation".to_owned(),
        );
    }
    if matches!(rule.event_type, PilotEventType::DexArbitrage) && deterministic_profit.is_none() {
        limitations
            .push("profit is null unless deterministic asset accounting proves it".to_owned());
    }
    if let Some(profit) = deterministic_profit {
        limitations.extend(
            profit
                .get("limitations")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .map(ToOwned::to_owned),
        );
    }
    let conclusion = match rule.event_type {
        PilotEventType::LargeAssetFlow => format!("Material asset flow involving {subject_address}; largest raw amount {}.", key_amount.as_deref().unwrap_or("unknown")),
        PilotEventType::TreasuryMovement => format!("Treasury-role entity {subject_address} moved assets."),
        PilotEventType::ProtocolRevenue => format!("Assets flowed to configured revenue entity {subject_address}."),
        PilotEventType::DexArbitrage => if let Some(profit) = deterministic_profit { format!("Deterministic accounting proved a raw token surplus of {} for the configured searcher scope.", profit["profit_raw"].as_str().unwrap_or("unknown")) } else if analysis.recognizer_results.iter().any(|result| result.label.to_ascii_lowercase().contains("arbitrage")) { "Deterministic protocol decoding classified this transaction as arbitrage-like activity; profit remains unconfirmed.".to_owned() } else { format!("Transaction called configured arbitrage executor {subject_address}; profit remains unconfirmed without complete accounting.") },
        PilotEventType::Liquidation => "Deterministic protocol decoding classified this transaction as liquidation-like activity.".to_owned(),
        PilotEventType::HighCostFailure => format!("Transaction failed with gas cost {} raw native units.", gas_cost_raw.as_deref().unwrap_or("unknown")),
        PilotEventType::RepeatedFailedAttempts => format!("Subject produced {} failed attempts in the configured window.", context.repeated_failure_count),
        PilotEventType::UnknownMaterialInteraction => "Material interaction was observed, but no configured deterministic protocol recognizer identified it.".to_owned(),
    };
    let created_at = analysis
        .block_timestamp
        .unwrap_or_else(|| Utc.timestamp_opt(0, 0).single().expect("unix epoch"));
    let confidence = confidence_for(rule, analysis, flows).or_else(|| {
        (matches!(rule.event_type, PilotEventType::DexArbitrage)
            && subject.is_some_and(|entity| role_is(entity, &["arbitrage_executor", "searcher"])))
        .then_some(0.75)
    });
    let mut pack = PilotEvidencePack {
        schema_version: EVIDENCE_PACK_SCHEMA_VERSION.to_owned(), event_id,
        project_id: config.project.project_id.clone(), chain_id: analysis.chain_id,
        block_number: analysis.block_number, tx_hash: analysis.tx_hash.clone(),
        subject: json!({"address": subject_address, "entity_id": subject.map(|v| &v.entity_id), "role": subject.map(|v| &v.role), "label": subject.and_then(|v| v.label.as_ref())}),
        event_type: rule.event_type.clone(), severity: rule.severity.clone(), conclusion,
        entities: config.entities.iter().filter(|entity| entity.include && analysis_involves_entity(analysis, entity)).map(|entity| json!({"entity_id":entity.entity_id,"address":entity.address,"role":entity.role,"label":entity.label})).collect(),
        asset_flows, protocol_path: protocols.to_vec(),
        profit_and_cost: json!({
            "profit_raw": deterministic_profit.and_then(|value| value.get("profit_raw")).cloned().unwrap_or(Value::Null),
            "profit_token_address": deterministic_profit.and_then(|value| value.get("profit_token_address")).cloned().unwrap_or(Value::Null),
            "profit_recipient": deterministic_profit.and_then(|value| value.get("profit_recipient")).cloned().unwrap_or(Value::Null),
            "profit_method": deterministic_profit.and_then(|value| value.get("method")).cloned().unwrap_or(Value::Null),
            "execution_gas_cost_raw": exact_gas_component(analysis, "execution_gas_cost_raw"),
            "l1_fee_raw": exact_gas_component(analysis, "l1_fee_raw"),
            "gas_cost_raw": gas_cost_raw,
        }),
        technical_evidence: json!({"analysis_id":analysis.analysis_id,"analysis_input_digest":analysis.input_digest,"transaction_status":analysis.status,"receipt_status":analysis.normalized_execution_facts.derived_facts.get("receipt_status"),"trace_available":trace_available,"deterministic_profit":analysis.normalized_execution_facts.derived_facts.get("deterministic_profit"),"raw_evidence_refs":analysis.raw_evidence_refs,"recognizer_results":analysis.recognizer_results}),
        confidence, limitations, ruleset_id, inputs_digest: String::new(), created_at,
    };
    pack.inputs_digest = pack.digest_without_digest();
    pack
}

fn boundary_flows<'a>(
    analysis: &'a TransactionAnalysis,
    entities: &[&PilotEntity],
) -> Vec<&'a AssetFlow> {
    analysis
        .derived_flows
        .iter()
        .filter(|flow| {
            let from = entities
                .iter()
                .any(|entity| eq_address(&flow.from_address, &entity.address));
            let to = entities
                .iter()
                .any(|entity| eq_address(&flow.to_address, &entity.address));
            from != to
        })
        .collect()
}

fn subject_entity<'a>(
    analysis: &TransactionAnalysis,
    entities: &[&'a PilotEntity],
    flows: &[&AssetFlow],
) -> Option<&'a PilotEntity> {
    entities
        .iter()
        .copied()
        .find(|entity| eq_address(&analysis.from_address, &entity.address))
        .or_else(|| {
            analysis.to_address.as_deref().and_then(|to| {
                entities
                    .iter()
                    .copied()
                    .find(|entity| eq_address(to, &entity.address))
            })
        })
        .or_else(|| {
            entities.iter().copied().find(|entity| {
                flows.iter().any(|flow| {
                    eq_address(&flow.from_address, &entity.address)
                        || eq_address(&flow.to_address, &entity.address)
                })
            })
        })
}

fn protocol_path(analysis: &TransactionAnalysis, entities: &[&PilotEntity]) -> Vec<Value> {
    let mut seen = BTreeSet::new();
    let mut path = Vec::new();
    for result in &analysis.recognizer_results {
        let protocols = result
            .attributes
            .get("protocols")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .chain(
                result
                    .attributes
                    .get("protocol")
                    .and_then(Value::as_str)
                    .into_iter(),
            );
        for protocol in protocols {
            if seen.insert(protocol.to_ascii_lowercase()) {
                path.push(json!({"protocol":protocol,"source":"recognizer","recognizer_id":result.recognizer_id,"recognizer_version":result.recognizer_version,"confidence":result.confidence}));
            }
        }
    }
    for entity in entities {
        if !role_is(entity, &["protocol", "protocol_revenue", "fee_collector"])
            || !analysis_involves_entity(analysis, entity)
        {
            continue;
        }
        let protocol = entity.label.as_deref().unwrap_or(&entity.entity_id);
        if seen.insert(protocol.to_ascii_lowercase()) {
            path.push(json!({
                "protocol": protocol,
                "source": "configured_entity",
                "entity_id": entity.entity_id,
                "address": entity.address,
                "role": entity.role
            }));
        }
    }
    path
}

fn filters_match(
    filters: &RuleFilters,
    analysis: &TransactionAnalysis,
    flows: &[&AssetFlow],
    protocols: &[Value],
) -> bool {
    let addresses = std::iter::once(analysis.from_address.as_str())
        .chain(analysis.to_address.as_deref())
        .chain(
            flows
                .iter()
                .flat_map(|flow| [flow.from_address.as_str(), flow.to_address.as_str()]),
        );
    let address_set = addresses
        .map(str::to_ascii_lowercase)
        .collect::<BTreeSet<_>>();
    let token_set = flows
        .iter()
        .filter_map(|flow| flow.token_address.as_deref())
        .map(str::to_ascii_lowercase)
        .collect::<BTreeSet<_>>();
    let protocol_set = protocols
        .iter()
        .filter_map(|v| v.get("protocol").and_then(Value::as_str))
        .map(str::to_ascii_lowercase)
        .collect::<BTreeSet<_>>();
    list_allows(
        &filters.include_addresses,
        &filters.exclude_addresses,
        &address_set,
    ) && list_allows(&filters.include_tokens, &filters.exclude_tokens, &token_set)
        && list_allows(
            &filters.include_protocols,
            &filters.exclude_protocols,
            &protocol_set,
        )
}

fn list_allows(include: &[String], exclude: &[String], observed: &BTreeSet<String>) -> bool {
    let included = include.is_empty()
        || include
            .iter()
            .any(|item| observed.contains(&item.to_ascii_lowercase()));
    included
        && !exclude
            .iter()
            .any(|item| observed.contains(&item.to_ascii_lowercase()))
}

fn flow_meets_threshold(flow: &AssetFlow, rule: &PilotRule) -> bool {
    let raw_ok = raw_meets(Some(&flow.amount_raw), amount_threshold(rule));
    let usd_ok = detailed_threshold(rule)
        .and_then(|v| v.amount_usd)
        .is_none_or(|minimum| {
            flow.metadata
                .get("amount_usd")
                .and_then(Value::as_f64)
                .is_some_and(|amount| amount >= minimum)
        });
    raw_ok && usd_ok
}

fn raw_meets(actual: Option<&str>, minimum: Option<&str>) -> bool {
    let Some(minimum) = minimum else {
        return true;
    };
    let (Some(actual), Ok(minimum)) = (actual, minimum.parse::<BigUint>()) else {
        return false;
    };
    actual
        .parse::<BigUint>()
        .is_ok_and(|actual| actual >= minimum)
}

fn amount_threshold(rule: &PilotRule) -> Option<&str> {
    match rule.threshold.as_ref()? {
        RuleThreshold::Raw(v) => Some(v),
        RuleThreshold::Detailed(v) => v.amount_raw.as_deref(),
    }
}
fn gas_threshold(rule: &PilotRule) -> Option<&str> {
    match rule.threshold.as_ref()? {
        RuleThreshold::Raw(v) => Some(v),
        RuleThreshold::Detailed(v) => v.gas_cost_raw.as_deref().or(v.amount_raw.as_deref()),
    }
}
fn count_threshold(rule: &PilotRule) -> Option<u32> {
    detailed_threshold(rule).and_then(|v| v.count)
}
pub fn failure_window_minutes(rule: &PilotRule) -> u32 {
    detailed_threshold(rule)
        .and_then(|v| v.window_minutes)
        .unwrap_or(60)
}
fn detailed_threshold(rule: &PilotRule) -> Option<&RuleThresholds> {
    match rule.threshold.as_ref()? {
        RuleThreshold::Detailed(v) => Some(v),
        RuleThreshold::Raw(_) => None,
    }
}
fn exact_gas_cost_raw(analysis: &TransactionAnalysis) -> Option<String> {
    analysis
        .accounting_outputs
        .get("gas_cost_raw")
        .or_else(|| {
            analysis
                .normalized_execution_facts
                .derived_facts
                .get("gas_cost_raw")
        })
        .and_then(|v| v.as_str())
        .map(str::to_owned)
}
fn exact_gas_component(analysis: &TransactionAnalysis, key: &str) -> Option<String> {
    analysis
        .accounting_outputs
        .get(key)
        .or_else(|| analysis.normalized_execution_facts.derived_facts.get(key))
        .and_then(Value::as_str)
        .map(str::to_owned)
}
fn verified_profit(analysis: &TransactionAnalysis) -> Option<&Value> {
    let profit = analysis
        .normalized_execution_facts
        .derived_facts
        .get("deterministic_profit")?;
    (profit.get("status").and_then(Value::as_str) == Some("verified")
        && profit.get("accounting_complete").and_then(Value::as_bool) == Some(true)
        && profit
            .get("profit_raw")
            .and_then(Value::as_str)
            .and_then(|value| value.parse::<BigUint>().ok())
            .is_some_and(|value| value > BigUint::from(0_u8))
        && profit
            .get("profit_token_address")
            .and_then(Value::as_str)
            .is_some_and(|value| value.starts_with("0x") && value.len() == 42))
    .then_some(profit)
}
fn confidence_for(
    rule: &PilotRule,
    analysis: &TransactionAnalysis,
    flows: &[&AssetFlow],
) -> Option<f64> {
    match rule.event_type {
        PilotEventType::DexArbitrage => analysis
            .recognizer_results
            .iter()
            .filter(|result| result.label.to_ascii_lowercase().contains("arbitrage"))
            .map(|result| result.confidence as f64)
            .max_by(f64::total_cmp),
        PilotEventType::Liquidation => analysis
            .recognizer_results
            .iter()
            .filter(|result| result.label.to_ascii_lowercase().contains("liquidation"))
            .map(|result| result.confidence as f64)
            .max_by(f64::total_cmp),
        PilotEventType::UnknownMaterialInteraction => Some(0.5),
        PilotEventType::HighCostFailure | PilotEventType::RepeatedFailedAttempts => Some(1.0),
        _ => flows
            .iter()
            .map(|flow| flow.confidence as f64)
            .max_by(f64::total_cmp),
    }
}
fn role_is(entity: &PilotEntity, roles: &[&str]) -> bool {
    roles
        .iter()
        .any(|role| entity.role.eq_ignore_ascii_case(role))
}
fn eq_address(a: &str, b: &str) -> bool {
    a.eq_ignore_ascii_case(b)
}
fn analysis_involves_entity(analysis: &TransactionAnalysis, entity: &PilotEntity) -> bool {
    eq_address(&analysis.from_address, &entity.address)
        || analysis
            .to_address
            .as_deref()
            .is_some_and(|to| eq_address(to, &entity.address))
        || analysis.derived_flows.iter().any(|flow| {
            eq_address(&flow.from_address, &entity.address)
                || eq_address(&flow.to_address, &entity.address)
        })
}

pub fn config_digest(config: &PilotConfig) -> String {
    let canonical = serde_json::to_vec(config).expect("pilot config is serializable");
    let mut hasher = Sha256::new();
    hasher.update(PILOT_CLASSIFIER_VERSION.as_bytes());
    hasher.update(canonical);
    hex::encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::versioning::VersionBundle;
    use crate::domain::{
        entities::{NormalizedExecutionFacts, RecognizerResult},
        enums::{AnalysisStatus, AssetType},
        value_objects::AssetFlow,
    };
    use serde_json::json;

    fn analysis() -> TransactionAnalysis {
        TransactionAnalysis {
            analysis_id: Uuid::nil(),
            chain_id: 8453,
            tx_hash: format!("0x{}", "1".repeat(64)),
            block_number: Some(1),
            block_timestamp: Some(Utc.timestamp_opt(1, 0).unwrap()),
            from_address: "0x1111111111111111111111111111111111111111".into(),
            to_address: Some("0x2222222222222222222222222222222222222222".into()),
            status: TransactionStatus::Success,
            value: "0".into(),
            input_data: "0x".into(),
            code_identity_summary: None,
            raw_evidence_refs: vec![],
            normalized_execution_facts: NormalizedExecutionFacts {
                selector: None,
                call_count: 1,
                internal_call_count: 0,
                event_count: 1,
                call_tree: Value::Null,
                participants: vec![],
                log_summaries: vec![],
                derived_facts: json!({"trace_available":true,"gas_cost_raw":"10","receipt_status":1}),
                evidence_refs: vec![],
            },
            derived_flows: vec![AssetFlow {
                flow_id: "f".into(),
                asset_type: AssetType::Erc20,
                token_address: Some("0x3333333333333333333333333333333333333333".into()),
                from_address: "0x1111111111111111111111111111111111111111".into(),
                to_address: "0x2222222222222222222222222222222222222222".into(),
                amount_raw: "100".into(),
                direction_hint: None,
                evidence_refs: vec![],
                derivation_method: "log".into(),
                confidence: 1.0,
                metadata: json!({}),
            }],
            recognizer_results: vec![RecognizerResult {
                recognizer_id: "x".into(),
                recognizer_version: "1".into(),
                label: "transfer".into(),
                confidence: 1.0,
                evidence_refs: vec![],
                attributes: json!({}),
                unresolved_points: vec![],
                warnings: vec![],
            }],
            scenario_outputs: json!({}),
            accounting_outputs: json!({"gas_cost_raw":"10"}),
            analysis_status: AnalysisStatus::Completed,
            analysis_version_metadata: VersionBundle::default(),
            unresolved_items: vec![],
            validation_results: vec![],
            input_digest: "source".into(),
            attempt_id: None,
        }
    }

    fn config_for(event_type: PilotEventType, role: &str) -> PilotConfig {
        PilotConfig {
            project: super::super::PilotProject {
                project_id: "p".into(),
                name: "P".into(),
                chain_id: 8453,
                monitoring_start_block: None,
                continuous_monitoring: false,
                public_publish: false,
                enabled: true,
            },
            entities: vec![PilotEntity {
                entity_id: "e".into(),
                address: "0x1111111111111111111111111111111111111111".into(),
                role: role.into(),
                label: None,
                include: true,
            }],
            rules: vec![PilotRule {
                rule_id: "r".into(),
                event_type,
                severity: super::super::PilotSeverity::High,
                threshold: Some(RuleThreshold::Raw("1".into())),
                filters: RuleFilters::default(),
                enabled: true,
            }],
            telegram: vec![],
        }
    }

    #[test]
    fn large_flow_pack_is_deterministic() {
        let config = config_for(PilotEventType::LargeAssetFlow, "treasury");
        let first = classify(&config, &analysis(), &ClassificationContext::default());
        let second = classify(&config, &analysis(), &ClassificationContext::default());
        assert_eq!(first[0].event_id, second[0].event_id);
        assert_eq!(first[0].inputs_digest, second[0].inputs_digest);
    }

    #[test]
    fn all_eight_event_types_emit_schema_valid_packs() {
        let cases = [
            (PilotEventType::LargeAssetFlow, "treasury"),
            (PilotEventType::TreasuryMovement, "treasury"),
            (PilotEventType::ProtocolRevenue, "protocol_revenue"),
            (PilotEventType::DexArbitrage, "arbitrage_executor"),
            (PilotEventType::Liquidation, "protocol"),
            (PilotEventType::HighCostFailure, "monitored_contract"),
            (PilotEventType::RepeatedFailedAttempts, "monitored_contract"),
            (
                PilotEventType::UnknownMaterialInteraction,
                "monitored_contract",
            ),
        ];
        for (event_type, role) in cases {
            let mut tx = analysis();
            match event_type {
                PilotEventType::ProtocolRevenue => {
                    tx.derived_flows[0].from_address =
                        "0x4444444444444444444444444444444444444444".into();
                    tx.derived_flows[0].to_address =
                        "0x1111111111111111111111111111111111111111".into();
                }
                PilotEventType::Liquidation => {
                    tx.recognizer_results[0].label = "aave_liquidation".into();
                }
                PilotEventType::HighCostFailure | PilotEventType::RepeatedFailedAttempts => {
                    tx.status = TransactionStatus::Failure;
                }
                PilotEventType::UnknownMaterialInteraction => tx.recognizer_results.clear(),
                _ => {}
            }
            let context = ClassificationContext {
                repeated_failure_count: 3,
            };
            let packs = classify(&config_for(event_type.clone(), role), &tx, &context);
            assert_eq!(
                packs.len(),
                1,
                "missing {}",
                super::super::event_type_name(&event_type)
            );
            assert_eq!(packs[0].event_type, event_type);
            super::super::validate_evidence_pack(&packs[0]).unwrap();
            if packs[0].event_type == PilotEventType::DexArbitrage {
                assert!(packs[0].profit_and_cost["profit_raw"].is_null());
                assert!(packs[0].limitations.iter().any(|v| v.contains("profit")));
            }
        }
    }

    #[test]
    fn verified_profit_is_emitted_but_unverified_profit_remains_null() {
        let config = config_for(PilotEventType::DexArbitrage, "searcher");
        let mut tx = analysis();
        tx.normalized_execution_facts.derived_facts["deterministic_profit"] = json!({
            "status": "verified",
            "accounting_complete": true,
            "method": "archive_balance_snapshot_receipt_trace_reconciliation_v1",
            "profit_raw": "42",
            "profit_token_address": "0x3333333333333333333333333333333333333333",
            "profit_recipient": "0x1111111111111111111111111111111111111111",
            "limitations": ["profit is exact only for the explicitly configured accounting scope"]
        });
        let packs = classify(&config, &tx, &ClassificationContext::default());
        assert_eq!(packs[0].profit_and_cost["profit_raw"], "42");
        assert_eq!(
            packs[0].profit_and_cost["profit_token_address"],
            "0x3333333333333333333333333333333333333333"
        );
        assert!(
            !packs[0]
                .limitations
                .iter()
                .any(|value| value.contains("profit is null"))
        );

        tx.normalized_execution_facts.derived_facts["deterministic_profit"]["accounting_complete"] =
            json!(false);
        let packs = classify(&config, &tx, &ClassificationContext::default());
        assert!(packs[0].profit_and_cost["profit_raw"].is_null());
    }
}
