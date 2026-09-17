//! Base Economic Monitoring Pilot primitives.  This module is deliberately
//! deterministic: it contains configuration, event taxonomy, evidence-pack
//! serialization and report formatting, but no LLM-dependent decisions.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use uuid::Uuid;

pub mod profit;

pub const BASE_CHAIN_ID: u64 = 8453;
pub const EVIDENCE_PACK_SCHEMA_VERSION: &str = "ledgerflow.evidence-pack.v1";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PilotConfig {
    pub project: PilotProject,
    #[serde(default)]
    pub entities: Vec<PilotEntity>,
    #[serde(default)]
    pub rules: Vec<PilotRule>,
    #[serde(default)]
    pub telegram: Vec<TelegramDestination>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PilotProject {
    pub project_id: String,
    pub name: String,
    pub chain_id: u64,
    #[serde(default)]
    pub monitoring_start_block: Option<u64>,
    #[serde(default)]
    pub continuous_monitoring: bool,
    /// Explicit opt-in for publishing this project's Evidence Packs on the
    /// unauthenticated public feed. Customer/Pilot projects stay private by
    /// default, including older configs that predate this field.
    #[serde(default)]
    pub public_publish: bool,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PilotEntity {
    pub entity_id: String,
    pub address: String,
    pub role: String,
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default = "default_true")]
    pub include: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PilotRule {
    pub rule_id: String,
    pub event_type: PilotEventType,
    #[serde(default = "default_severity")]
    pub severity: PilotSeverity,
    #[serde(default)]
    pub threshold: Option<RuleThreshold>,
    #[serde(default)]
    pub filters: RuleFilters,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RuleThreshold {
    /// Backward-compatible shorthand interpreted as an exact raw-unit threshold.
    Raw(String),
    Detailed(RuleThresholds),
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RuleThresholds {
    #[serde(default)]
    pub amount_raw: Option<String>,
    #[serde(default)]
    pub amount_usd: Option<f64>,
    #[serde(default)]
    pub gas_cost_raw: Option<String>,
    #[serde(default)]
    pub count: Option<u32>,
    #[serde(default)]
    pub window_minutes: Option<u32>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RuleFilters {
    #[serde(default)]
    pub include_addresses: Vec<String>,
    #[serde(default)]
    pub exclude_addresses: Vec<String>,
    #[serde(default)]
    pub include_tokens: Vec<String>,
    #[serde(default)]
    pub exclude_tokens: Vec<String>,
    #[serde(default)]
    pub include_protocols: Vec<String>,
    #[serde(default)]
    pub exclude_protocols: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelegramDestination {
    pub destination_id: String,
    pub chat_id: String,
    #[serde(default)]
    pub secret_ref: Option<String>,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum PilotEventType {
    LargeAssetFlow,
    TreasuryMovement,
    ProtocolRevenue,
    DexArbitrage,
    Liquidation,
    HighCostFailure,
    RepeatedFailedAttempts,
    UnknownMaterialInteraction,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum PilotSeverity {
    Info,
    Low,
    Medium,
    High,
    Critical,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PilotEvidencePack {
    pub schema_version: String,
    pub event_id: Uuid,
    pub project_id: String,
    pub chain_id: u64,
    pub block_number: Option<u64>,
    pub tx_hash: String,
    pub subject: Value,
    pub event_type: PilotEventType,
    pub severity: PilotSeverity,
    pub conclusion: String,
    #[serde(default)]
    pub entities: Vec<Value>,
    #[serde(default)]
    pub asset_flows: Vec<Value>,
    #[serde(default)]
    pub protocol_path: Vec<Value>,
    #[serde(default)]
    pub profit_and_cost: Value,
    #[serde(default)]
    pub technical_evidence: Value,
    pub confidence: Option<f64>,
    #[serde(default)]
    pub limitations: Vec<String>,
    pub ruleset_id: String,
    pub inputs_digest: String,
    pub created_at: DateTime<Utc>,
}

impl PilotEvidencePack {
    pub fn digest_without_digest(&self) -> String {
        let mut copy = self.clone();
        copy.inputs_digest.clear();
        let bytes = serde_json::to_vec(&copy).expect("evidence pack is serializable");
        let mut h = Sha256::new();
        h.update(bytes);
        hex::encode(h.finalize())
    }
}

pub fn validate_evidence_pack(pack: &PilotEvidencePack) -> Result<(), String> {
    let schema: Value =
        serde_json::from_str(include_str!("../../schemas/evidence-pack-v1.schema.json"))
            .map_err(|e| e.to_string())?;
    let instance = serde_json::to_value(pack).map_err(|e| e.to_string())?;
    let validator = jsonschema::validator_for(&schema).map_err(|e| e.to_string())?;
    let errors = validator
        .iter_errors(&instance)
        .map(|e| e.to_string())
        .collect::<Vec<_>>();
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("; "))
    }
}

pub fn load_yaml(
    path: impl AsRef<std::path::Path>,
) -> Result<PilotConfig, Box<dyn std::error::Error + Send + Sync>> {
    let raw = std::fs::read_to_string(path)?;
    let config: PilotConfig = serde_yaml::from_str(&raw)?;
    validate_config(&config)?;
    Ok(config)
}

pub fn validate_config(config: &PilotConfig) -> Result<(), String> {
    if config.project.chain_id != BASE_CHAIN_ID {
        return Err(format!(
            "Phase 1 supports Base only (chain_id={BASE_CHAIN_ID})"
        ));
    }
    if config.project.project_id.trim().is_empty() {
        return Err("project.project_id is required".into());
    }
    if config.project.name.trim().is_empty() {
        return Err("project.name is required".into());
    }
    if !config
        .project
        .project_id
        .chars()
        .all(|value| value.is_ascii_alphanumeric() || matches!(value, '-' | '_'))
    {
        return Err("project_id may contain only letters, numbers, '-' and '_'".into());
    }
    if !(10..=30).contains(&config.entities.len()) {
        return Err("a Pilot project must configure between 10 and 30 entities".into());
    }
    if config.project.continuous_monitoring && config.project.monitoring_start_block.is_none() {
        return Err("continuous monitoring requires project.monitoring_start_block".into());
    }
    let mut ids = std::collections::BTreeSet::new();
    let mut addresses = std::collections::BTreeSet::new();
    for entity in &config.entities {
        if entity.entity_id.trim().is_empty() || entity.role.trim().is_empty() {
            return Err("entity_id and entity role are required".into());
        }
        let address = entity.address.strip_prefix("0x").unwrap_or(&entity.address);
        if address.len() != 40 || !address.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err(format!("invalid entity address: {}", entity.address));
        }
        if !ids.insert(entity.entity_id.to_ascii_lowercase()) {
            return Err(format!("duplicate entity_id: {}", entity.entity_id));
        }
        if !addresses.insert(entity.address.to_ascii_lowercase()) {
            return Err(format!("duplicate entity address: {}", entity.address));
        }
    }
    if config.rules.is_empty() {
        return Err("at least one rule is required".into());
    }
    if !config.entities.iter().any(|entity| entity.include) {
        return Err("at least one Pilot entity must be included".into());
    }
    let mut rule_ids = std::collections::BTreeSet::new();
    let mut rule_types = std::collections::BTreeSet::new();
    for rule in &config.rules {
        if rule.rule_id.trim().is_empty() {
            return Err("rule_id is required".into());
        }
        if !rule_ids.insert(rule.rule_id.to_ascii_lowercase()) {
            return Err(format!("duplicate rule_id: {}", rule.rule_id));
        }
        if !rule_types.insert(event_type_name(&rule.event_type)) {
            return Err(format!(
                "duplicate event_type rule: {}",
                event_type_name(&rule.event_type)
            ));
        }
        validate_threshold(rule)?;
    }
    let mut destination_ids = std::collections::BTreeSet::new();
    for destination in &config.telegram {
        if destination.destination_id.trim().is_empty() || destination.chat_id.trim().is_empty() {
            return Err("Telegram destination_id and chat_id are required".into());
        }
        if !destination_ids.insert(destination.destination_id.to_ascii_lowercase()) {
            return Err(format!(
                "duplicate Telegram destination_id: {}",
                destination.destination_id
            ));
        }
    }
    Ok(())
}

fn validate_threshold(rule: &PilotRule) -> Result<(), String> {
    let raw_values = match &rule.threshold {
        Some(RuleThreshold::Raw(value)) => vec![value],
        Some(RuleThreshold::Detailed(value)) => {
            [value.amount_raw.as_ref(), value.gas_cost_raw.as_ref()]
                .into_iter()
                .flatten()
                .collect()
        }
        None => Vec::new(),
    };
    for value in raw_values {
        if value.parse::<num_bigint::BigUint>().is_err() {
            return Err(format!(
                "rule {} has invalid raw-unit threshold",
                rule.rule_id
            ));
        }
    }
    if let Some(details) = detailed_threshold_value(rule) {
        if details
            .amount_usd
            .is_some_and(|value| !value.is_finite() || value < 0.0)
        {
            return Err(format!("rule {} has invalid USD threshold", rule.rule_id));
        }
        if details.count == Some(0) || details.window_minutes == Some(0) {
            return Err(format!(
                "rule {} count/window thresholds must be positive",
                rule.rule_id
            ));
        }
    }
    Ok(())
}

fn detailed_threshold_value(rule: &PilotRule) -> Option<&RuleThresholds> {
    match rule.threshold.as_ref()? {
        RuleThreshold::Detailed(value) => Some(value),
        RuleThreshold::Raw(_) => None,
    }
}

fn default_true() -> bool {
    true
}
fn default_severity() -> PilotSeverity {
    PilotSeverity::Medium
}

pub fn event_type_name(event_type: &PilotEventType) -> &'static str {
    match event_type {
        PilotEventType::LargeAssetFlow => "large_asset_flow",
        PilotEventType::TreasuryMovement => "treasury_movement",
        PilotEventType::ProtocolRevenue => "protocol_revenue",
        PilotEventType::DexArbitrage => "dex_arbitrage",
        PilotEventType::Liquidation => "liquidation",
        PilotEventType::HighCostFailure => "high_cost_failure",
        PilotEventType::RepeatedFailedAttempts => "repeated_failed_attempts",
        PilotEventType::UnknownMaterialInteraction => "unknown_material_interaction",
    }
}

pub mod alerts;
pub mod classifier;
pub mod report;
pub mod repository;

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn example_config_is_base_only() {
        let config: PilotConfig = serde_yaml::from_str(include_str!(
            "../../config/pilots/example-base-project.yaml"
        ))
        .unwrap();
        validate_config(&config).unwrap();
        assert_eq!(config.project.chain_id, BASE_CHAIN_ID);
    }
}
