use chrono::{DateTime, Utc};
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionEventStatus {
    Detected,
    Verified,
    Investigating,
    Resolved,
    Dismissed,
}

impl ExecutionEventStatus {
    pub fn key(self) -> &'static str {
        match self {
            Self::Detected => "detected",
            Self::Verified => "verified",
            Self::Investigating => "investigating",
            Self::Resolved => "resolved",
            Self::Dismissed => "dismissed",
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Detected => "Detected · Unverified",
            Self::Verified => "Verified",
            Self::Investigating => "Investigating",
            Self::Resolved => "Resolved",
            Self::Dismissed => "Dismissed",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventSeverity {
    Low,
    Medium,
    High,
    Critical,
}

impl EventSeverity {
    pub fn key(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
            Self::Critical => "critical",
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Low => "LOW",
            Self::Medium => "MEDIUM",
            Self::High => "HIGH",
            Self::Critical => "CRITICAL",
        }
    }

    pub fn from_explicit(value: &str) -> Self {
        match value.trim().to_ascii_lowercase().as_str() {
            "critical" => Self::Critical,
            "high" => Self::High,
            "medium" => Self::Medium,
            _ => Self::Low,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionEventType {
    DexArbitrage,
    Backrun,
    Liquidation,
    LiquidationAttempt,
    RepeatedFailedAttempts,
    LargeAssetFlow,
    ProtocolRevenue,
    TreasuryMovement,
    SolverExecution,
    UnknownMaterialInteraction,
}

impl ExecutionEventType {
    pub fn key(self) -> &'static str {
        match self {
            Self::DexArbitrage => "dex_arbitrage",
            Self::Backrun => "backrun",
            Self::Liquidation => "liquidation",
            Self::LiquidationAttempt => "liquidation_attempt",
            Self::RepeatedFailedAttempts => "repeated_failed_attempts",
            Self::LargeAssetFlow => "large_asset_flow",
            Self::ProtocolRevenue => "protocol_revenue",
            Self::TreasuryMovement => "treasury_movement",
            Self::SolverExecution => "solver_execution",
            Self::UnknownMaterialInteraction => "unknown_material_interaction",
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::DexArbitrage => "DEX Arbitrage",
            Self::Backrun => "Backrun",
            Self::Liquidation => "Liquidation",
            Self::LiquidationAttempt => "Liquidation Attempt",
            Self::RepeatedFailedAttempts => "Repeated Failed Attempts",
            Self::LargeAssetFlow => "Large Asset Flow",
            Self::ProtocolRevenue => "Protocol Revenue",
            Self::TreasuryMovement => "Treasury Movement",
            Self::SolverExecution => "Solver Execution",
            Self::UnknownMaterialInteraction => "Unknown Material Interaction",
        }
    }

    pub fn from_key(value: &str) -> Self {
        match value.trim().to_ascii_lowercase().as_str() {
            "dex_arbitrage" => Self::DexArbitrage,
            "backrun" => Self::Backrun,
            "liquidation" => Self::Liquidation,
            "liquidation_attempt" => Self::LiquidationAttempt,
            "repeated_failed_attempts" => Self::RepeatedFailedAttempts,
            "large_asset_flow" => Self::LargeAssetFlow,
            "protocol_revenue" => Self::ProtocolRevenue,
            "treasury_movement" => Self::TreasuryMovement,
            "solver_execution" => Self::SolverExecution,
            _ => Self::UnknownMaterialInteraction,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfidenceLevel {
    Low,
    Medium,
    High,
}

impl ConfidenceLevel {
    pub fn label(self) -> &'static str {
        match self {
            Self::Low => "Low",
            Self::Medium => "Medium",
            Self::High => "High",
        }
    }
}

#[derive(Debug, Clone)]
pub struct EventConfidence {
    pub score: Option<u8>,
    pub level: Option<ConfidenceLevel>,
}

#[derive(Debug, Clone, Default)]
pub struct EconomicImpact {
    pub trigger_size: Option<String>,
    pub price_impact: Option<String>,
    pub gross_value_captured: Option<String>,
    pub gas_cost: Option<String>,
    pub net_profit: Option<String>,
    pub protocol_fees: Option<String>,
    pub rebate: Option<String>,
    pub asset_moved: Option<String>,
    pub usd_value: Option<f64>,
}

impl EconomicImpact {
    pub fn primary_value(&self) -> Option<&str> {
        self.net_profit
            .as_deref()
            .or(self.asset_moved.as_deref())
            .or(self.gross_value_captured.as_deref())
            .or(self.gas_cost.as_deref())
    }
}

#[derive(Debug, Clone)]
pub struct EntityReference {
    pub role: String,
    pub address: String,
    pub label: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntityEvidenceState {
    Observed,
    Inferred,
    Unverified,
}

impl EntityEvidenceState {
    pub fn label(self) -> &'static str {
        match self {
            Self::Observed => "Observed",
            Self::Inferred => "Inferred",
            Self::Unverified => "Unverified",
        }
    }

    pub fn key(self) -> &'static str {
        match self {
            Self::Observed => "observed",
            Self::Inferred => "inferred",
            Self::Unverified => "unverified",
        }
    }
}

#[derive(Debug, Clone)]
pub struct EntityIdentityClaim {
    pub value: String,
    pub confidence: Option<u8>,
    pub evidence_state: EntityEvidenceState,
    pub source: Option<String>,
}

#[derive(Debug, Clone)]
pub struct EntityBehavior {
    pub name: String,
    pub transaction_count: u64,
    pub share_percent: Option<f64>,
    pub gas_cost: Option<String>,
    pub evidence_state: EntityEvidenceState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntityRelationshipKind {
    Funding,
    Execution,
    Settlement,
    Protocol,
    ObservedInteraction,
}

impl EntityRelationshipKind {
    pub fn label(self) -> &'static str {
        match self {
            Self::Funding => "Funding relationship",
            Self::Execution => "Execution relationship",
            Self::Settlement => "Profit / settlement relationship",
            Self::Protocol => "Protocol relationship",
            Self::ObservedInteraction => "Observed interaction",
        }
    }
}

#[derive(Debug, Clone)]
pub struct EntityRelationship {
    pub kind: EntityRelationshipKind,
    pub counterparty: EntityReference,
    pub evidence_state: EntityEvidenceState,
    pub observation_count: Option<u64>,
    pub last_seen_at: Option<DateTime<Utc>>,
    pub evidence_href: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct EntityEconomicIntelligence {
    pub observed_execution_count: u64,
    pub verified_profitable_execution_count: u64,
    pub verified_usd_impact: Option<f64>,
    pub analyzed_gas_cost: Option<String>,
    pub coverage: String,
}

#[derive(Debug, Clone)]
pub struct EntityTimelineItem {
    pub occurred_at: Option<DateTime<Utc>>,
    pub title: String,
    pub detail: String,
    pub impact: Option<String>,
    pub evidence_state: EntityEvidenceState,
    pub event_id: Option<String>,
}

/// Frontend projection assembled from existing deterministic address analysis,
/// execution events, and persisted flow relationships. It intentionally keeps
/// observation and inference separate and never asserts real-world ownership.
#[derive(Debug, Clone)]
pub struct EntityProfile {
    pub id: String,
    pub chain_id: u64,
    pub entity_type: String,
    pub roles: Vec<EntityIdentityClaim>,
    pub labels: Vec<EntityIdentityClaim>,
    pub confidence: Option<u8>,
    pub first_seen_at: Option<DateTime<Utc>>,
    pub last_seen_at: Option<DateTime<Utc>>,
    pub behaviors: Vec<EntityBehavior>,
    pub protocols: Vec<String>,
    pub relationships: Vec<EntityRelationship>,
    pub economics: EntityEconomicIntelligence,
    pub timeline: Vec<EntityTimelineItem>,
    pub limitations: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObservationMonitoringState {
    Available,
    Enabled,
    Unavailable,
}

impl ObservationMonitoringState {
    pub fn key(self) -> &'static str {
        match self {
            Self::Available => "available",
            Self::Enabled => "enabled",
            Self::Unavailable => "unavailable",
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Available => "Monitoring available",
            Self::Enabled => "Monitoring enabled",
            Self::Unavailable => "Monitoring unavailable",
        }
    }
}

/// User-facing observation projection. The persisted Entity remains the
/// deterministic analysis subject; this wrapper expresses the recurring
/// customer workflow without leaking that internal model into the UI.
#[derive(Debug, Clone)]
pub struct ObservedObject {
    pub id: String,
    pub target: String,
    pub object_type: String,
    pub workspace_id: String,
    pub observed_since: DateTime<Utc>,
    pub monitoring_state: ObservationMonitoringState,
    pub intelligence_profile: EntityProfile,
}

#[derive(Debug, Clone, Default)]
pub struct EvidenceSummary {
    pub receipt_available: bool,
    pub logs_available: bool,
    pub trace_available: bool,
    pub token_deltas_available: bool,
    pub route_fingerprint: Option<String>,
    pub ruleset_id: Option<String>,
    pub inputs_digest: Option<String>,
    pub limitations: Vec<String>,
    pub raw: Option<Value>,
}

#[derive(Debug, Clone)]
pub struct ExecutionTimelineStep {
    pub label: String,
    pub title: String,
    pub detail: String,
    pub status: Option<String>,
    pub occurred_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct ExecutionEvent {
    pub id: String,
    pub source: &'static str,
    pub chain_id: u64,
    pub block_number: Option<i64>,
    pub tx_hash: String,
    pub event_type: ExecutionEventType,
    pub status: ExecutionEventStatus,
    pub severity: EventSeverity,
    pub conclusion: String,
    pub economic_impact: EconomicImpact,
    pub confidence: EventConfidence,
    pub participants: Vec<EntityReference>,
    pub protocols: Vec<String>,
    pub timeline: Vec<ExecutionTimelineStep>,
    pub evidence: EvidenceSummary,
    pub occurred_at: Option<DateTime<Utc>>,
}

impl ExecutionEvent {
    pub fn is_unverified(&self) -> bool {
        self.status == ExecutionEventStatus::Detected
    }
}

#[derive(Debug, Clone)]
pub struct ArbitrageCandidateInput {
    pub chain_id: u64,
    pub tx_hash: String,
    pub block_number: i64,
    pub block_timestamp: Option<DateTime<Utc>>,
    pub detection_source: String,
    pub sender_address: Option<String>,
    pub entry_contract: Option<String>,
    pub detection_score: i32,
    pub swap_log_count: i32,
    pub distinct_pool_count: i32,
    pub route: String,
    pub route_fingerprint: String,
    pub confirmation_status: String,
    pub profit_token_address: Option<String>,
    pub profit_amount_raw: Option<String>,
    pub pool_addresses: Vec<String>,
    pub economic_verification: Option<Value>,
}

#[derive(Debug, Clone)]
pub struct PilotEventInput {
    pub event_id: String,
    pub chain_id: u64,
    pub block_number: Option<i64>,
    pub tx_hash: String,
    pub event_type: String,
    pub severity: String,
    pub conclusion: String,
    pub evidence_pack: Value,
    pub created_at: DateTime<Utc>,
}

pub fn adapt_pilot_event(input: PilotEventInput) -> ExecutionEvent {
    let pack = &input.evidence_pack;
    let confidence_score = pack
        .get("confidence")
        .and_then(Value::as_f64)
        .map(|value| (value.clamp(0.0, 1.0) * 100.0).round() as u8);
    let confidence_level = confidence_score.map(|score| {
        if score >= 85 {
            ConfidenceLevel::High
        } else if score >= 60 {
            ConfidenceLevel::Medium
        } else {
            ConfidenceLevel::Low
        }
    });
    let participants = pack
        .get("entities")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|entity| {
            let address = entity.get("address")?.as_str()?.to_owned();
            Some(EntityReference {
                role: entity
                    .get("role")
                    .and_then(Value::as_str)
                    .unwrap_or("Relevant entity")
                    .to_owned(),
                address,
                label: entity
                    .get("label")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
            })
        })
        .collect::<Vec<_>>();
    let protocols = pack
        .get("protocol_path")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|item| item.get("protocol").and_then(Value::as_str))
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let profit_and_cost = pack.get("profit_and_cost");
    let raw_field = |key: &str| {
        profit_and_cost
            .and_then(|value| value.get(key))
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(|value| format!("{value} raw units"))
    };
    let asset_moved = pack
        .get("asset_flows")
        .and_then(Value::as_array)
        .and_then(|flows| flows.first())
        .and_then(|flow| flow.get("amount_raw"))
        .and_then(Value::as_str)
        .map(|value| format!("{value} raw units"));
    let technical = pack.get("technical_evidence");
    let raw_refs = technical
        .and_then(|value| value.get("raw_evidence_refs"))
        .and_then(Value::as_array);
    let has_evidence_kind = |kind: &str| {
        raw_refs.is_some_and(|refs| {
            refs.iter().any(|item| {
                item.get("kind")
                    .and_then(Value::as_str)
                    .is_some_and(|value| value == kind)
            })
        })
    };
    let limitations = pack
        .get("limitations")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect::<Vec<_>>();

    ExecutionEvent {
        id: format!("pilot-{}", input.event_id),
        source: "pilot_events",
        chain_id: input.chain_id,
        block_number: input.block_number,
        tx_hash: input.tx_hash,
        event_type: ExecutionEventType::from_key(&input.event_type),
        // Pilot events are persisted only after deterministic classification and
        // Evidence Pack validation. This is a source guarantee, not an LLM guess.
        status: ExecutionEventStatus::Verified,
        severity: EventSeverity::from_explicit(&input.severity),
        conclusion: input.conclusion,
        economic_impact: EconomicImpact {
            gas_cost: raw_field("gas_cost_raw"),
            net_profit: raw_field("profit_raw"),
            asset_moved,
            ..EconomicImpact::default()
        },
        confidence: EventConfidence {
            score: confidence_score,
            level: confidence_level,
        },
        participants,
        protocols,
        timeline: vec![ExecutionTimelineStep {
            label: "Execution".to_owned(),
            title: "Deterministic transaction evidence".to_owned(),
            detail: "The event was classified from the persisted transaction, receipt, logs, trace availability, and token flows in its Evidence Pack."
                .to_owned(),
            status: technical
                .and_then(|value| value.get("transaction_status"))
                .and_then(Value::as_str)
                .map(str::to_owned),
            occurred_at: Some(input.created_at),
        }],
        evidence: EvidenceSummary {
            receipt_available: has_evidence_kind("receipt"),
            logs_available: has_evidence_kind("log"),
            trace_available: technical
                .and_then(|value| value.get("trace_available"))
                .and_then(Value::as_bool)
                .unwrap_or(false),
            token_deltas_available: pack
                .get("asset_flows")
                .and_then(Value::as_array)
                .is_some_and(|flows| !flows.is_empty()),
            route_fingerprint: None,
            ruleset_id: pack
                .get("ruleset_id")
                .and_then(Value::as_str)
                .map(str::to_owned),
            inputs_digest: pack
                .get("inputs_digest")
                .and_then(Value::as_str)
                .map(str::to_owned),
            limitations,
            raw: Some(input.evidence_pack),
        },
        occurred_at: Some(input.created_at),
    }
}

/// Fail-closed adapter from the existing detector record to the public event
/// model. A multi-pool candidate is not promoted to a verified arbitrage merely
/// because it was analyzed. Only an explicit economic-verification status may
/// expose the recorded profit field.
pub fn adapt_arbitrage_candidate(input: ArbitrageCandidateInput) -> ExecutionEvent {
    let verification_status = input
        .economic_verification
        .as_ref()
        .and_then(|value| value.get("result_status"))
        .and_then(Value::as_str);
    let explicitly_verified = matches!(
        input
            .confirmation_status
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "verified" | "verified_profit" | "confirmed_economic"
    ) || verification_status == Some("verified");
    let explicitly_dismissed = input.confirmation_status.eq_ignore_ascii_case("dismissed")
        || verification_status == Some("dismissed");
    let status = if explicitly_verified {
        ExecutionEventStatus::Verified
    } else if explicitly_dismissed {
        ExecutionEventStatus::Dismissed
    } else {
        ExecutionEventStatus::Detected
    };
    let mut participants = Vec::new();
    if let Some(address) = input.sender_address.as_ref() {
        participants.push(EntityReference {
            role: "Sender / candidate actor".to_owned(),
            address: address.clone(),
            label: None,
        });
    }
    if let Some(address) = input.entry_contract.as_ref() {
        participants.push(EntityReference {
            role: "Entry contract".to_owned(),
            address: address.clone(),
            label: None,
        });
    }
    participants.extend(input.pool_addresses.iter().map(|address| EntityReference {
        role: "Observed pool".to_owned(),
        address: address.clone(),
        label: None,
    }));
    if let Some(address) = input
        .economic_verification
        .as_ref()
        .and_then(|value| value.get("profit_recipient"))
        .and_then(Value::as_str)
        .filter(|address| {
            input
                .sender_address
                .as_deref()
                .is_none_or(|sender| !sender.eq_ignore_ascii_case(address))
        })
    {
        participants.push(EntityReference {
            role: "Verified surplus recipient".to_owned(),
            address: address.to_owned(),
            label: None,
        });
    }

    // The verifier proves an exact positive token surplus and records native
    // gas separately. It does not convert unlike assets, so presenting this as
    // net profit would overstate the available evidence.
    let verified_surplus = if explicitly_verified {
        input.profit_amount_raw.as_ref().map(|amount| {
            input
                .profit_token_address
                .as_ref()
                .map(|token| format!("{amount} raw units · {token}"))
                .unwrap_or_else(|| format!("{amount} raw units"))
        })
    } else {
        None
    };
    let conclusion = input
        .economic_verification
        .as_ref()
        .and_then(|value| value.get("conclusion"))
        .and_then(Value::as_str)
        .map(str::to_owned)
        .unwrap_or_else(|| if explicitly_verified {
        format!(
            "Verified multi-pool execution with {} swap logs across {} pools.",
            input.swap_log_count, input.distinct_pool_count
        )
    } else if explicitly_dismissed {
        "Deterministic evidence dismissed this multi-pool economic candidate.".to_owned()
    } else {
        format!(
            "Detected a multi-pool execution candidate with {} swap logs across {} pools. Economic profit and intent are not verified.",
            input.swap_log_count, input.distinct_pool_count
        )
    });
    let limitations = if let Some(values) = input
        .economic_verification
        .as_ref()
        .and_then(|value| value.get("limitations"))
        .and_then(Value::as_array)
    {
        values
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect()
    } else if explicitly_verified || explicitly_dismissed {
        Vec::new()
    } else {
        vec![
            "Detector output is a candidate signal, not a confirmed arbitrage conclusion.".to_owned(),
            "Net profit, protocol attribution, and transaction intent are unavailable until deterministic evidence verification completes.".to_owned(),
        ]
    };
    let mut timeline = vec![ExecutionTimelineStep {
        label: "Detection".to_owned(),
        title: "Multi-pool execution signal".to_owned(),
        detail: format!(
            "{} observed {} swap logs across {} distinct pools.",
            input.detection_source, input.swap_log_count, input.distinct_pool_count
        ),
        status: Some(input.confirmation_status.clone()),
        occurred_at: input.block_timestamp,
    }];
    if explicitly_verified || explicitly_dismissed {
        timeline.push(ExecutionTimelineStep {
            label: "Verification".to_owned(),
            title: if explicitly_verified {
                "Economic evidence verified"
            } else {
                "Candidate dismissed"
            }
            .to_owned(),
            detail: conclusion.clone(),
            status: Some(status.key().to_owned()),
            occurred_at: input.block_timestamp,
        });
    }
    let verification_confidence = input
        .economic_verification
        .as_ref()
        .and_then(|value| value.get("confidence"))
        .and_then(Value::as_f64)
        .map(|value| (value.clamp(0.0, 1.0) * 100.0).round() as u8);
    let severity = input
        .economic_verification
        .as_ref()
        .and_then(|value| value.get("severity"))
        .and_then(Value::as_str)
        .map(EventSeverity::from_explicit)
        .unwrap_or(if explicitly_verified {
            EventSeverity::Medium
        } else {
            EventSeverity::Low
        });
    let gas_cost = input
        .economic_verification
        .as_ref()
        .and_then(|value| value.get("gas_cost_raw"))
        .and_then(Value::as_str)
        .map(|value| format!("{value} raw native units"));
    let verification_trace_available = input
        .economic_verification
        .as_ref()
        .and_then(|value| value.pointer("/analysis/trace_available"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let verification_ruleset = input
        .economic_verification
        .as_ref()
        .and_then(|value| value.get("ruleset_id"))
        .and_then(Value::as_str)
        .map(str::to_owned);
    let verification_digest = input
        .economic_verification
        .as_ref()
        .and_then(|value| value.get("inputs_digest"))
        .and_then(Value::as_str)
        .map(str::to_owned);
    let has_economic_verification = input.economic_verification.is_some();
    let raw_evidence = serde_json::json!({
        "candidate": {
            "detection_source": input.detection_source,
            "detection_score": input.detection_score,
            "swap_log_count": input.swap_log_count,
            "distinct_pool_count": input.distinct_pool_count,
            "route": input.route,
            "confirmation_status": input.confirmation_status,
        },
        "economic_verification": input.economic_verification.clone(),
    });

    ExecutionEvent {
        id: format!("arb-{}", input.tx_hash.trim_start_matches("0x")),
        source: "arbitrage_candidates",
        chain_id: input.chain_id,
        block_number: Some(input.block_number),
        tx_hash: input.tx_hash,
        event_type: ExecutionEventType::DexArbitrage,
        status,
        severity,
        conclusion,
        economic_impact: EconomicImpact {
            gas_cost,
            gross_value_captured: verified_surplus,
            ..EconomicImpact::default()
        },
        confidence: EventConfidence {
            score: verification_confidence,
            level: Some(if explicitly_verified {
                ConfidenceLevel::High
            } else if verification_confidence.is_some_and(|score| score >= 60) {
                ConfidenceLevel::Medium
            } else {
                ConfidenceLevel::Low
            }),
        },
        participants,
        protocols: Vec::new(),
        timeline,
        evidence: EvidenceSummary {
            receipt_available: has_economic_verification,
            logs_available: input.swap_log_count > 0,
            trace_available: verification_trace_available,
            token_deltas_available: explicitly_verified,
            route_fingerprint: (!input.route_fingerprint.is_empty())
                .then_some(input.route_fingerprint),
            ruleset_id: verification_ruleset,
            inputs_digest: verification_digest,
            limitations,
            raw: Some(raw_evidence),
            ..EvidenceSummary::default()
        },
        occurred_at: input.block_timestamp,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkspaceKind {
    Named,
    Demo,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkspacePlan {
    Free,
    Pilot,
    Enterprise,
}

impl WorkspacePlan {
    pub fn parse(value: &str) -> Self {
        match value.trim().to_ascii_lowercase().as_str() {
            "pilot" => Self::Pilot,
            "enterprise" => Self::Enterprise,
            _ => Self::Free,
        }
    }

    pub fn key(self) -> &'static str {
        match self {
            Self::Free => "free",
            Self::Pilot => "pilot",
            Self::Enterprise => "enterprise",
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Free => "Free",
            Self::Pilot => "Pilot",
            Self::Enterprise => "Enterprise",
        }
    }

    pub fn allows(self, capability: WorkspaceCapability) -> bool {
        self.capabilities().contains(&capability)
    }

    pub fn capabilities(self) -> &'static [WorkspaceCapability] {
        const FREE: &[WorkspaceCapability] = &[
            WorkspaceCapability::PublicResearch,
            WorkspaceCapability::DemoMonitoring,
            WorkspaceCapability::SampleReports,
        ];
        const PILOT: &[WorkspaceCapability] = &[
            WorkspaceCapability::PublicResearch,
            WorkspaceCapability::DemoMonitoring,
            WorkspaceCapability::SampleReports,
            WorkspaceCapability::CustomMonitoredEntities,
            WorkspaceCapability::TelegramAlerts,
            WorkspaceCapability::CustomRules,
            WorkspaceCapability::PrivateInvestigations,
            WorkspaceCapability::WeeklyReports,
        ];
        const ENTERPRISE: &[WorkspaceCapability] = &[
            WorkspaceCapability::PublicResearch,
            WorkspaceCapability::DemoMonitoring,
            WorkspaceCapability::SampleReports,
            WorkspaceCapability::CustomMonitoredEntities,
            WorkspaceCapability::TelegramAlerts,
            WorkspaceCapability::CustomRules,
            WorkspaceCapability::PrivateInvestigations,
            WorkspaceCapability::WeeklyReports,
            WorkspaceCapability::ScopedApi,
            WorkspaceCapability::ContractedSla,
        ];
        match self {
            Self::Free => FREE,
            Self::Pilot => PILOT,
            Self::Enterprise => ENTERPRISE,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkspaceCapability {
    PublicResearch,
    DemoMonitoring,
    SampleReports,
    CustomMonitoredEntities,
    TelegramAlerts,
    CustomRules,
    PrivateInvestigations,
    WeeklyReports,
    ScopedApi,
    ContractedSla,
}

#[derive(Debug, Clone)]
pub struct WorkspaceRef {
    pub id: String,
    pub name: String,
    pub kind: WorkspaceKind,
    pub workspace_plan: WorkspacePlan,
    pub chain_id: Option<u64>,
    pub monitored_entity_count: usize,
    pub workspace_type: String,
    pub status: String,
    pub legacy_cluster_id: Option<uuid::Uuid>,
    pub onboarding_complete: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkspaceRange {
    Last24Hours,
    Last7Days,
    Last30Days,
}

impl WorkspaceRange {
    pub fn parse(value: Option<&str>) -> Self {
        match value {
            Some("7d") => Self::Last7Days,
            Some("30d") => Self::Last30Days,
            _ => Self::Last24Hours,
        }
    }

    pub fn key(self) -> &'static str {
        match self {
            Self::Last24Hours => "24h",
            Self::Last7Days => "7d",
            Self::Last30Days => "30d",
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Last24Hours => "Last 24h",
            Self::Last7Days => "Last 7d",
            Self::Last30Days => "Last 30d",
        }
    }

    pub fn postgres_interval(self) -> &'static str {
        match self {
            Self::Last24Hours => "24 hours",
            Self::Last7Days => "7 days",
            Self::Last30Days => "30 days",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidate(status: &str) -> ArbitrageCandidateInput {
        ArbitrageCandidateInput {
            chain_id: 8453,
            tx_hash: "0xabc".to_owned(),
            block_number: 10,
            block_timestamp: None,
            detection_source: "rpc_logs".to_owned(),
            sender_address: Some("0xsender".to_owned()),
            entry_contract: None,
            detection_score: 50,
            swap_log_count: 3,
            distinct_pool_count: 2,
            route: "pool-a > pool-b".to_owned(),
            route_fingerprint: "route-1".to_owned(),
            confirmation_status: status.to_owned(),
            profit_token_address: Some("0xtoken".to_owned()),
            profit_amount_raw: Some("123".to_owned()),
            pool_addresses: vec!["0xpool".to_owned()],
            economic_verification: None,
        }
    }

    #[test]
    fn workspace_plans_fail_closed_to_free_and_expand_capabilities_explicitly() {
        assert_eq!(WorkspacePlan::parse("unexpected"), WorkspacePlan::Free);
        assert!(WorkspacePlan::Free.allows(WorkspaceCapability::PublicResearch));
        assert!(!WorkspacePlan::Free.allows(WorkspaceCapability::TelegramAlerts));
        assert!(WorkspacePlan::Pilot.allows(WorkspaceCapability::TelegramAlerts));
        assert!(!WorkspacePlan::Pilot.allows(WorkspaceCapability::ScopedApi));
        assert!(WorkspacePlan::Enterprise.allows(WorkspaceCapability::ScopedApi));
        assert!(!WorkspacePlan::Pilot.allows(WorkspaceCapability::ContractedSla));
        assert!(WorkspacePlan::Enterprise.allows(WorkspaceCapability::ContractedSla));
    }

    #[test]
    fn unconfirmed_candidate_never_exposes_profit() {
        let event = adapt_arbitrage_candidate(candidate("analyzed_multi_pool_unconfirmed"));
        assert_eq!(event.status, ExecutionEventStatus::Detected);
        assert_eq!(event.economic_impact.net_profit, None);
        assert!(event.conclusion.contains("not verified"));
    }

    #[test]
    fn explicit_economic_verification_exposes_surplus_but_not_net_profit() {
        let event = adapt_arbitrage_candidate(candidate("verified_profit"));
        assert_eq!(event.status, ExecutionEventStatus::Verified);
        assert_eq!(
            event.economic_impact.gross_value_captured.as_deref(),
            Some("123 raw units · 0xtoken")
        );
        assert_eq!(event.economic_impact.net_profit, None);
    }

    #[test]
    fn deterministic_dismissal_maps_to_dismissed_event() {
        let mut input = candidate("dismissed");
        input.profit_amount_raw = None;
        input.profit_token_address = None;
        input.economic_verification = Some(serde_json::json!({
            "result_status": "dismissed",
            "conclusion": "Receipt evidence shows that the transaction reverted.",
            "confidence": 0.95,
            "severity": "low",
            "ruleset_id": "ledgerflow.economic-verifier.v1",
            "inputs_digest": "digest",
            "limitations": []
        }));
        let event = adapt_arbitrage_candidate(input);
        assert_eq!(event.status, ExecutionEventStatus::Dismissed);
        assert_eq!(event.economic_impact.net_profit, None);
        assert_eq!(event.evidence.inputs_digest.as_deref(), Some("digest"));
    }

    #[test]
    fn range_parser_is_bounded_to_supported_values() {
        assert_eq!(WorkspaceRange::parse(Some("7d")), WorkspaceRange::Last7Days);
        assert_eq!(
            WorkspaceRange::parse(Some("custom")),
            WorkspaceRange::Last24Hours
        );
    }
}
