use std::collections::BTreeSet;

use num_bigint::BigUint;
use serde_json::Value;

use crate::pilot::{PilotEventType, PilotEvidencePack, validate_evidence_pack};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublicReport {
    pub slug: &'static str,
    pub title: &'static str,
    pub summary: &'static str,
    pub category: &'static str,
    pub published_date: &'static str,
    pub related_events: &'static [&'static str],
    pub author: &'static str,
    pub cover_label: &'static str,
    pub markdown: &'static str,
}

impl PublicReport {
    pub fn reading_time_minutes(&self) -> usize {
        self.markdown
            .split_whitespace()
            .count()
            .div_ceil(220)
            .max(1)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublicTimelineStep {
    pub label: &'static str,
    pub detail: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublicEvidenceRecord {
    pub slug: &'static str,
    pub title: &'static str,
    pub conclusion: &'static str,
    pub event_type: &'static str,
    pub event_type_key: &'static str,
    pub chain: &'static str,
    pub chain_id: u64,
    pub tx_hash: &'static str,
    pub occurred_at: Option<&'static str>,
    pub protocol: Option<&'static str>,
    pub trigger: &'static str,
    pub status: &'static str,
    pub status_key: &'static str,
    pub economic_impact: &'static str,
    pub participants: &'static [&'static str],
    pub execution_timeline: &'static [PublicTimelineStep],
    pub evidence_summary: &'static [&'static str],
    pub related_entities: &'static [&'static str],
    pub confidence: &'static str,
    pub confidence_level: &'static str,
    pub limitations: &'static [&'static str],
}

/// Owned view model used by the continuously published Evidence Explorer.
/// The original static records remain as editorially reviewed examples and
/// are converted into this same shape before rendering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishedEvidenceRecord {
    pub slug: String,
    pub title: String,
    pub conclusion: String,
    pub event_type: String,
    pub event_type_key: String,
    pub chain: String,
    pub chain_id: u64,
    pub tx_hash: String,
    pub occurred_at: Option<String>,
    pub protocol: Option<String>,
    pub trigger: String,
    pub status: String,
    pub status_key: String,
    pub economic_impact: String,
    pub participants: Vec<String>,
    pub execution_timeline: Vec<PublishedTimelineStep>,
    pub evidence_summary: Vec<String>,
    pub related_entities: Vec<String>,
    pub confidence: String,
    pub confidence_level: String,
    pub limitations: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishedTimelineStep {
    pub label: String,
    pub detail: String,
}

const REPORTS: &[PublicReport] = &[
    PublicReport {
        slug: "same-flashblock-backruns-on-base",
        title: "Same-Flashblock Backruns on Base",
        summary: "A methodology and publication-threshold note: the current public corpus does not contain enough canonical Flashblock ordering evidence to publish a backrun count.",
        category: "MEV",
        published_date: "2026-08-16",
        related_events: &["0x0a0309ba7b2d9268a30a7022f3ea33b6db3998628364dad56ddeb7ddc25887d1"],
        author: "LedgerFlow Research",
        cover_label: "MEV / BASE",
        markdown: include_str!("../../reports/public/same-flashblock-backruns-on-base.md"),
    },
    PublicReport {
        slug: "execution-intelligence-overview",
        title: "Execution Evidence Framework",
        summary: "How deterministic transaction facts become execution events, Evidence Packs, monitoring signals, and reproducible reports.",
        category: "Execution Intelligence",
        published_date: "2026-08-16",
        related_events: &[
            "0x57b5c3bddf71686b4b435930c075412e43783a283d6ed894350fa787f4ccba03",
            "0x6f4c472b846a2d5530ece4596a39c5cb9f088f0ad2e3804f80f95c8b2289d40a",
        ],
        author: "LedgerFlow Research",
        cover_label: "EVIDENCE / EXECUTION",
        markdown: include_str!(
            "../../reports/public/ledgerflow-execution-intelligence-overview.md"
        ),
    },
    PublicReport {
        slug: "base-searcher-landscape",
        title: "Base Searcher Landscape",
        summary: "A bounded evidence note based on six reviewed Base accounting cases and the public Pilot corpus; it is not a full actor census.",
        category: "Ecosystem Reports",
        published_date: "2026-08-16",
        related_events: &[
            "0x1ae5d1351545e3f71caa9db955d4b4a98a4e1bafe098949212b3262015fd8737",
            "0x921b70cbfd389ead780ef510f24556c5e3967478a41a2fd7b6f990df6b2f92dc",
            "0xe529680c4cab9f3725fd0a7d0e83addcf7a5dbb28335f27b32719b338202f915",
        ],
        author: "LedgerFlow Research",
        cover_label: "SEARCHERS / BASE",
        markdown: include_str!("../../reports/public/base-searcher-landscape.md"),
    },
];

const EVIDENCE_RECORDS: &[PublicEvidenceRecord] = &[
    PublicEvidenceRecord {
        slug: "base-deterministic-usdc-surplus",
        title: "Verified DEX Arbitrage Accounting",
        conclusion: "Archive balances, receipt transfers, trace-derived native flows, and exact gas reconcile to a positive USDC delta for the configured accounting scope.",
        event_type: "DEX arbitrage accounting",
        event_type_key: "dex_arbitrage",
        chain: "Base",
        chain_id: 8453,
        tx_hash: "0xe529680c4cab9f3725fd0a7d0e83addcf7a5dbb28335f27b32719b338202f915",
        occurred_at: None,
        protocol: None,
        trigger: "Configured accounting-scope transaction",
        status: "Verified",
        status_key: "verified",
        economic_impact: "3,896 raw USDC units (0.003896 USDC) verified; exact total gas cost 1,360,551,043,717 raw native units.",
        participants: &[
            "0xaa3242ec58e5d0f6fe2fbb0ed8b76bd72672968d — configured accounting scope",
            "0x47c553cf1c109c99c2376a3c0c220b19121f4e98 — configured accounting scope",
        ],
        execution_timeline: &[
            PublicTimelineStep {
                label: "Trigger",
                detail: "The configured accounting-scope transaction executed on Base.",
            },
            PublicTimelineStep {
                label: "Execution",
                detail: "Receipt Transfer logs and trace-derived native flows were collected.",
            },
            PublicTimelineStep {
                label: "Reconciliation",
                detail: "Block-before and block-after balances reconciled the scoped token deltas.",
            },
            PublicTimelineStep {
                label: "Settlement",
                detail: "Exact execution gas and Base L1 fee were included; the USDC surplus was verified.",
            },
        ],
        evidence_summary: &[
            "Transaction receipt and ERC-20 Transfer logs",
            "debug_traceTransaction call trace",
            "Block-before and block-after token balance snapshots",
            "Native balance reconciliation with execution gas and Base L1 fee",
        ],
        related_entities: &[
            "0xaa3242ec58e5d0f6fe2fbb0ed8b76bd72672968d",
            "0x47c553cf1c109c99c2376a3c0c220b19121f4e98",
        ],
        confidence: "Verified — deterministic reconciliation",
        confidence_level: "High",
        limitations: &[
            "The result proves a raw USDC surplus for the configured scope, not real-world actor ownership.",
            "Native gas and USDC surplus are not combined into a net USD profit without an accepted historical price input.",
            "The fixed regression artifact records the block number but does not include a publication-safe event timestamp.",
        ],
    },
    PublicEvidenceRecord {
        slug: "base-aave-liquidation-evidence",
        title: "Liquidation-like Interaction with Attribution Conflict",
        conclusion: "A successful Base transaction produced liquidation-like protocol evidence, but the public artifact contains conflicting Aave recognizer and Morpho subject attribution, so this record is not published as a verified liquidation.",
        event_type: "Liquidation",
        event_type_key: "liquidation",
        chain: "Base",
        chain_id: 8453,
        tx_hash: "0x6f4c472b846a2d5530ece4596a39c5cb9f088f0ad2e3804f80f95c8b2289d40a",
        occurred_at: Some("2026-05-12T14:33:51Z"),
        protocol: None,
        trigger: "Successful transaction receipt with 32 logs",
        status: "Detected",
        status_key: "detected",
        economic_impact: "Exact total gas cost 114,088,802,965,348 raw native units; liquidator profit remains unknown.",
        participants: &[
            "0xeeacf719dbbbe9a2a06c076ab6bd5beb08e8c41d — transaction sender",
            "0x583dcfef0d240dc80753f0f0b26513fee27d9b77 — top-level call target",
        ],
        execution_timeline: &[
            PublicTimelineStep {
                label: "Trigger",
                detail: "The sender called the recorded top-level target on Base.",
            },
            PublicTimelineStep {
                label: "Execution",
                detail: "The receipt succeeded and emitted 32 logs.",
            },
            PublicTimelineStep {
                label: "Detection",
                detail: "The liquidation recognizer produced high-confidence Aave protocol evidence.",
            },
            PublicTimelineStep {
                label: "Review required",
                detail: "The configured event subject resolves to Morpho, so protocol attribution remains unresolved.",
            },
        ],
        evidence_summary: &[
            "Successful transaction receipt with 32 logs",
            "Liquidation-like recognizer output with 0.96 confidence",
            "Exact ERC-20 Transfer values from receipt logs",
            "Execution gas and Base L1 fee",
        ],
        related_entities: &[
            "0xeeacf719dbbbe9a2a06c076ab6bd5beb08e8c41d",
            "0x583dcfef0d240dc80753f0f0b26513fee27d9b77",
            "0xbbbbbbbbbb9cc5e90e3b3af64bdaf62c37eeffcb",
        ],
        confidence: "High detector confidence — attribution unresolved",
        confidence_level: "High",
        limitations: &[
            "The public event artifact identifies an Aave recognizer result while its configured subject is Morpho; protocol attribution is unresolved.",
            "The public Evidence Pack does not include a complete trace for this record.",
            "Liquidator profit and USD value are unknown.",
        ],
    },
    PublicEvidenceRecord {
        slug: "base-complex-asset-flow",
        title: "Verified Complex Asset-flow Reconstruction",
        conclusion: "Receipt logs reconstruct a six-leg USDC, WETH, and token flow through the Uniswap Universal Router without inferring intent or profit.",
        event_type: "Large asset flow",
        event_type_key: "large_asset_flow",
        chain: "Base",
        chain_id: 8453,
        tx_hash: "0x57b5c3bddf71686b4b435930c075412e43783a283d6ed894350fa787f4ccba03",
        occurred_at: Some("2025-01-30T20:51:09Z"),
        protocol: Some("Uniswap Universal Router"),
        trigger: "Universal Router transaction",
        status: "Verified",
        status_key: "verified",
        economic_impact: "Largest recorded event flow: 61,391,462,420,555,703 raw WETH units; exact total gas cost 2,500,041,682,590 raw native units; profit and USD value are unknown.",
        participants: &[
            "0xfbe1c203a4ffd3de4670d90eda1e62b4f0928033 — transaction sender",
            "0x3fc91a3afd70395cd496c647d5a6cc9d4b2b7fad — Uniswap Universal Router",
        ],
        execution_timeline: &[
            PublicTimelineStep {
                label: "Trigger",
                detail: "The sender called the Uniswap Universal Router.",
            },
            PublicTimelineStep {
                label: "Execution",
                detail: "The receipt succeeded and emitted 11 logs.",
            },
            PublicTimelineStep {
                label: "Flow reconstruction",
                detail: "Six exact ERC-20 Transfer-derived legs were reconstructed across USDC, WETH, and one additional token.",
            },
            PublicTimelineStep {
                label: "Evidence boundary",
                detail: "The largest raw WETH flow and exact total gas were recorded; intent, profit, and USD value remain unknown.",
            },
        ],
        evidence_summary: &[
            "Successful transaction receipt with 11 logs",
            "Six exact ERC-20 Transfer-derived asset flows",
            "USDC and WETH token metadata",
            "Configured Universal Router protocol evidence",
        ],
        related_entities: &[
            "0xfbe1c203a4ffd3de4670d90eda1e62b4f0928033",
            "0x3fc91a3afd70395cd496c647d5a6cc9d4b2b7fad",
            "0x4200000000000000000000000000000000000006",
            "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",
        ],
        confidence: "High — 0.98 flow confidence",
        confidence_level: "High",
        limitations: &[
            "Trace was unavailable in the public artifact, so internal calls may be incomplete.",
            "The largest raw amount is not a USD value and must not be compared across token decimals.",
            "Verified refers to the receipt-derived flow reconstruction, not transaction intent or profit.",
        ],
    },
];

pub fn reports() -> &'static [PublicReport] {
    REPORTS
}

pub fn report(slug: &str) -> Option<&'static PublicReport> {
    REPORTS.iter().find(|report| report.slug == slug)
}

pub fn evidence_records() -> &'static [PublicEvidenceRecord] {
    EVIDENCE_RECORDS
}

pub fn curated_evidence_records() -> Vec<PublishedEvidenceRecord> {
    EVIDENCE_RECORDS
        .iter()
        .map(PublishedEvidenceRecord::from)
        .collect()
}

pub fn embedded_public_evidence_packs() -> Result<Vec<PilotEvidencePack>, String> {
    let packs = serde_json::from_str::<Vec<PilotEvidencePack>>(include_str!(
        "../../demo/base-public/all_events.json"
    ))
    .map_err(|error| error.to_string())?;
    validate_public_packs(packs)
}

pub fn validate_public_packs(
    packs: Vec<PilotEvidencePack>,
) -> Result<Vec<PilotEvidencePack>, String> {
    let mut validated = Vec::with_capacity(packs.len());
    for pack in packs {
        validate_evidence_pack(&pack)?;
        if !evidence_pack_is_publication_safe(&pack) {
            return Err(format!(
                "event {} contains publication-denied fields",
                pack.event_id
            ));
        }
        validated.push(pack);
    }
    Ok(validated)
}

pub fn published_evidence_records(packs: &[PilotEvidencePack]) -> Vec<PublishedEvidenceRecord> {
    let mut records = curated_evidence_records();
    let mut identities = records
        .iter()
        .map(|record| {
            format!(
                "{}:{}",
                record.tx_hash.to_ascii_lowercase(),
                record.event_type_key
            )
        })
        .collect::<BTreeSet<_>>();
    for pack in packs {
        let record = PublishedEvidenceRecord::from(pack);
        let identity = format!(
            "{}:{}",
            record.tx_hash.to_ascii_lowercase(),
            record.event_type_key
        );
        if identities.insert(identity) {
            records.push(record);
        }
    }
    records.sort_by(|left, right| {
        right
            .occurred_at
            .cmp(&left.occurred_at)
            .then_with(|| left.slug.cmp(&right.slug))
    });
    records
}

impl From<&PublicEvidenceRecord> for PublishedEvidenceRecord {
    fn from(record: &PublicEvidenceRecord) -> Self {
        Self {
            slug: record.slug.to_owned(),
            title: record.title.to_owned(),
            conclusion: record.conclusion.to_owned(),
            event_type: record.event_type.to_owned(),
            event_type_key: record.event_type_key.to_owned(),
            chain: record.chain.to_owned(),
            chain_id: record.chain_id,
            tx_hash: record.tx_hash.to_owned(),
            occurred_at: record.occurred_at.map(str::to_owned),
            protocol: record.protocol.map(str::to_owned),
            trigger: record.trigger.to_owned(),
            status: record.status.to_owned(),
            status_key: record.status_key.to_owned(),
            economic_impact: record.economic_impact.to_owned(),
            participants: record
                .participants
                .iter()
                .map(|item| (*item).to_owned())
                .collect(),
            execution_timeline: record
                .execution_timeline
                .iter()
                .map(|step| PublishedTimelineStep {
                    label: step.label.to_owned(),
                    detail: step.detail.to_owned(),
                })
                .collect(),
            evidence_summary: record
                .evidence_summary
                .iter()
                .map(|item| (*item).to_owned())
                .collect(),
            related_entities: record
                .related_entities
                .iter()
                .map(|item| (*item).to_owned())
                .collect(),
            confidence: record.confidence.to_owned(),
            confidence_level: record.confidence_level.to_owned(),
            limitations: record
                .limitations
                .iter()
                .map(|item| (*item).to_owned())
                .collect(),
        }
    }
}

impl From<&PilotEvidencePack> for PublishedEvidenceRecord {
    fn from(pack: &PilotEvidencePack) -> Self {
        let (status, status_key) = publication_status(pack);
        let event_type_key = pilot_event_type_key(&pack.event_type).to_owned();
        let event_type = pilot_event_type_label(&pack.event_type).to_owned();
        let protocol = pack
            .protocol_path
            .iter()
            .find_map(|entry| value_text(entry, &["protocol", "label", "name"]));
        let trigger = value_text(&pack.subject, &["label", "role", "address"])
            .unwrap_or_else(|| "Published Base transaction evidence".to_owned());
        let mut participants = pack
            .entities
            .iter()
            .filter_map(format_entity)
            .collect::<Vec<_>>();
        if let Some(subject) = format_entity(&pack.subject)
            && !participants.contains(&subject)
        {
            participants.push(subject);
        }
        if participants.is_empty() {
            participants.push("Participants unavailable in the published Evidence Pack".to_owned());
        }
        let related_entities = related_addresses(pack);
        let confidence_score = pack.confidence.map(|score| score.clamp(0.0, 1.0));
        let confidence_level = match confidence_score {
            Some(score) if score >= 0.9 => "High",
            Some(score) if score >= 0.7 => "Medium",
            Some(_) => "Low",
            None => "Unknown",
        }
        .to_owned();
        let confidence = confidence_score
            .map(|score| format!("{} · {:.0}%", confidence_level, score * 100.0))
            .unwrap_or_else(|| "Unknown · score unavailable".to_owned());
        let mut limitations = pack.limitations.clone();
        if status_key == "detected" {
            limitations.push(
                "This record is published as Detected / Unverified; its conclusion must not be treated as a verified economic attribution."
                    .to_owned(),
            );
        }
        limitations.sort();
        limitations.dedup();
        Self {
            slug: format!("event-{}", pack.event_id),
            title: format!("{status} {event_type}"),
            conclusion: pack.conclusion.clone(),
            event_type,
            event_type_key,
            chain: if pack.chain_id == 8453 {
                "Base".to_owned()
            } else {
                format!("Chain {}", pack.chain_id)
            },
            chain_id: pack.chain_id,
            tx_hash: pack.tx_hash.clone(),
            occurred_at: Some(pack.created_at.to_rfc3339()),
            protocol,
            trigger,
            status: status.to_owned(),
            status_key: status_key.to_owned(),
            economic_impact: economic_impact(pack),
            participants,
            execution_timeline: execution_timeline(pack, status_key),
            evidence_summary: evidence_summary(pack),
            related_entities,
            confidence,
            confidence_level,
            limitations,
        }
    }
}

fn publication_status(pack: &PilotEvidencePack) -> (&'static str, &'static str) {
    let receipt_status = pack
        .technical_evidence
        .get("receipt_status")
        .and_then(Value::as_u64);
    let gas_is_exact = pack
        .profit_and_cost
        .get("gas_cost_raw")
        .and_then(Value::as_str)
        .is_some_and(is_unsigned_integer);
    let flows_are_exact = !pack.asset_flows.is_empty()
        && pack.asset_flows.iter().all(|flow| {
            flow.get("amount_raw")
                .and_then(Value::as_str)
                .is_some_and(is_unsigned_integer)
                && flow
                    .get("evidence_refs")
                    .and_then(Value::as_array)
                    .is_some_and(|refs| !refs.is_empty())
        });
    let deterministic_profit = pack
        .technical_evidence
        .get("deterministic_profit")
        .is_some_and(|profit| {
            profit.get("accounting_complete").and_then(Value::as_bool) == Some(true)
                && profit
                    .get("profit_raw")
                    .and_then(Value::as_str)
                    .is_some_and(is_unsigned_integer)
        });
    let verified = match pack.event_type {
        PilotEventType::LargeAssetFlow => receipt_status == Some(1) && flows_are_exact,
        PilotEventType::HighCostFailure => receipt_status == Some(0) && gas_is_exact,
        PilotEventType::DexArbitrage => deterministic_profit && gas_is_exact,
        _ => false,
    };
    if verified {
        ("Verified", "verified")
    } else {
        ("Detected", "detected")
    }
}

fn pilot_event_type_key(event_type: &PilotEventType) -> &'static str {
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

fn pilot_event_type_label(event_type: &PilotEventType) -> &'static str {
    match event_type {
        PilotEventType::LargeAssetFlow => "Large Asset Flow",
        PilotEventType::TreasuryMovement => "Treasury Movement",
        PilotEventType::ProtocolRevenue => "Protocol Revenue",
        PilotEventType::DexArbitrage => "DEX Arbitrage",
        PilotEventType::Liquidation => "Liquidation",
        PilotEventType::HighCostFailure => "High-cost Failure",
        PilotEventType::RepeatedFailedAttempts => "Repeated Failed Attempts",
        PilotEventType::UnknownMaterialInteraction => "Unknown Material Interaction",
    }
}

fn economic_impact(pack: &PilotEvidencePack) -> String {
    let gas = pack
        .profit_and_cost
        .get("gas_cost_raw")
        .and_then(Value::as_str)
        .filter(|value| is_unsigned_integer(value));
    let profit = pack
        .profit_and_cost
        .get("profit_raw")
        .and_then(Value::as_str)
        .filter(|value| is_unsigned_integer(value));
    let profit_token = pack
        .profit_and_cost
        .get("profit_token_address")
        .and_then(Value::as_str);
    let largest_flow = pack
        .asset_flows
        .iter()
        .filter_map(|flow| {
            let raw = flow.get("amount_raw")?.as_str()?;
            let amount = raw.parse::<BigUint>().ok()?;
            Some((amount, raw, flow))
        })
        .max_by(|left, right| left.0.cmp(&right.0));
    let mut facts = Vec::new();
    if let Some(profit) = profit {
        facts.push(format!(
            "Verified profit: {profit} raw units{}",
            profit_token
                .map(|token| format!(" of {token}"))
                .unwrap_or_default()
        ));
    } else if let Some((_, raw, flow)) = largest_flow {
        let asset = flow
            .get("metadata")
            .and_then(|metadata| metadata.get("token"))
            .and_then(|token| value_text(token, &["symbol", "address"]))
            .or_else(|| {
                flow.get("token_address")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            })
            .unwrap_or_else(|| {
                flow.get("asset_type")
                    .and_then(Value::as_str)
                    .unwrap_or("asset")
                    .to_owned()
            });
        facts.push(format!(
            "Largest reconstructed flow: {raw} raw {asset} units"
        ));
    }
    if let Some(gas) = gas {
        facts.push(format!("exact gas cost: {gas} raw native units"));
    }
    if facts.is_empty() {
        "Economic impact unavailable from published evidence.".to_owned()
    } else {
        format!("{}.", facts.join("; "))
    }
}

fn execution_timeline(pack: &PilotEvidencePack, status_key: &str) -> Vec<PublishedTimelineStep> {
    let receipt_status = pack
        .technical_evidence
        .get("receipt_status")
        .and_then(Value::as_u64);
    let mut steps = vec![PublishedTimelineStep {
        label: "Transaction".to_owned(),
        detail: format!("Transaction {} was observed on Base.", pack.tx_hash),
    }];
    if let Some(receipt_status) = receipt_status {
        steps.push(PublishedTimelineStep {
            label: "Receipt".to_owned(),
            detail: if receipt_status == 1 {
                "The transaction receipt succeeded.".to_owned()
            } else {
                "The transaction receipt reverted.".to_owned()
            },
        });
    }
    if !pack.asset_flows.is_empty() {
        steps.push(PublishedTimelineStep {
            label: "Flow reconstruction".to_owned(),
            detail: format!(
                "{} exact raw asset-flow legs were reconstructed from published transaction evidence.",
                pack.asset_flows.len()
            ),
        });
    }
    steps.push(PublishedTimelineStep {
        label: if status_key == "verified" {
            "Verification"
        } else {
            "Review boundary"
        }
        .to_owned(),
        detail: if status_key == "verified" {
            "The event-specific deterministic publication gate passed.".to_owned()
        } else {
            "The signal is public for review, but the event-specific verification gate did not pass."
                .to_owned()
        },
    });
    steps
}

fn evidence_summary(pack: &PilotEvidencePack) -> Vec<String> {
    let mut evidence = Vec::new();
    if let Some(status) = pack
        .technical_evidence
        .get("receipt_status")
        .and_then(Value::as_u64)
    {
        evidence.push(format!("Receipt status: {status}"));
    }
    if let Some(references) = pack
        .technical_evidence
        .get("raw_evidence_refs")
        .and_then(Value::as_array)
    {
        evidence.push(format!(
            "{} transaction evidence references",
            references.len()
        ));
    }
    if let Some(trace_available) = pack
        .technical_evidence
        .get("trace_available")
        .and_then(Value::as_bool)
    {
        evidence.push(format!(
            "Trace {}",
            if trace_available {
                "available"
            } else {
                "unavailable"
            }
        ));
    }
    if !pack.asset_flows.is_empty() {
        evidence.push(format!("{} exact raw asset flows", pack.asset_flows.len()));
    }
    evidence.push(format!("Ruleset: {}", pack.ruleset_id));
    evidence.push(format!("Inputs digest: {}", pack.inputs_digest));
    evidence
}

fn related_addresses(pack: &PilotEvidencePack) -> Vec<String> {
    let mut addresses = BTreeSet::new();
    for value in std::iter::once(&pack.subject)
        .chain(pack.entities.iter())
        .chain(pack.asset_flows.iter())
    {
        for key in ["address", "from_address", "to_address", "token_address"] {
            if let Some(address) = value.get(key).and_then(Value::as_str)
                && address.starts_with("0x")
            {
                addresses.insert(address.to_ascii_lowercase());
            }
        }
    }
    addresses.into_iter().collect()
}

fn format_entity(value: &Value) -> Option<String> {
    let address = value.get("address").and_then(Value::as_str)?;
    let label = value_text(value, &["label", "entity_id", "role"]);
    Some(match label {
        Some(label) => format!("{address} — {label}"),
        None => address.to_owned(),
    })
}

fn value_text(value: &Value, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(Value::as_str))
        .map(str::to_owned)
}

fn is_unsigned_integer(value: &str) -> bool {
    !value.is_empty() && value.chars().all(|character| character.is_ascii_digit())
}

pub fn evidence_pack_is_publication_safe(pack: &PilotEvidencePack) -> bool {
    const DENIED_KEYS: &[&str] = &[
        "private_key",
        "rpc_url",
        "websocket_url",
        "secret_ref",
        "chat_id",
        "api_key",
        "authorization",
        "bearer_token",
        "customer_id",
    ];
    fn walk(value: &Value) -> bool {
        match value {
            Value::Object(object) => object.iter().all(|(key, value)| {
                !DENIED_KEYS
                    .iter()
                    .any(|denied| key.eq_ignore_ascii_case(denied))
                    && walk(value)
            }),
            Value::Array(values) => values.iter().all(walk),
            _ => true,
        }
    }
    serde_json::to_value(pack).is_ok_and(|value| walk(&value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pilot::PilotProject;

    #[test]
    fn public_project_flag_defaults_closed_and_demo_opts_in() {
        let private: PilotProject =
            serde_yaml::from_str("project_id: private\nname: Private\nchain_id: 8453\n")
                .expect("legacy project config");
        assert!(!private.public_publish);

        let demo = crate::pilot::load_yaml("config/pilots/demo-base-public.yaml")
            .expect("public demo config");
        assert!(demo.project.public_publish);
        assert!(demo.project.continuous_monitoring);
    }

    #[test]
    fn embedded_corpus_is_valid_safe_and_fully_adapted() {
        let packs = embedded_public_evidence_packs().expect("public corpus");
        assert_eq!(packs.len(), 39);
        assert!(packs.iter().all(evidence_pack_is_publication_safe));
        let records = published_evidence_records(&packs);
        assert!(records.len() >= 39);
        assert!(records.iter().any(|record| record.status_key == "verified"));
        assert!(records.iter().any(|record| record.status_key == "detected"));
        assert!(
            records
                .iter()
                .all(|record| !record.economic_impact.is_empty())
        );
    }

    #[test]
    fn publication_gate_does_not_verify_detector_only_liquidation_or_arbitrage() {
        let packs = embedded_public_evidence_packs().expect("public corpus");
        for pack in packs.iter().filter(|pack| {
            matches!(
                pack.event_type,
                PilotEventType::Liquidation | PilotEventType::DexArbitrage
            )
        }) {
            assert_eq!(publication_status(pack), ("Detected", "detected"));
            let record = PublishedEvidenceRecord::from(pack);
            assert!(
                record
                    .limitations
                    .iter()
                    .any(|item| item.contains("Detected / Unverified"))
            );
        }
    }

    #[test]
    fn deterministic_flow_and_failed_receipt_can_pass_narrow_gates() {
        let packs = embedded_public_evidence_packs().expect("public corpus");
        let flow = packs
            .iter()
            .find(|pack| pack.event_type == PilotEventType::LargeAssetFlow)
            .expect("flow event");
        assert_eq!(publication_status(flow), ("Verified", "verified"));
        let failure = packs
            .iter()
            .find(|pack| pack.event_type == PilotEventType::HighCostFailure)
            .expect("failed event");
        assert_eq!(publication_status(failure), ("Verified", "verified"));
        let failure_record = PublishedEvidenceRecord::from(failure);
        let exact_gas = failure.profit_and_cost["gas_cost_raw"]
            .as_str()
            .expect("exact gas");
        assert!(failure_record.economic_impact.contains(exact_gas));
    }

    #[test]
    fn public_safety_filter_fails_closed_on_secret_fields() {
        let mut pack = embedded_public_evidence_packs()
            .expect("public corpus")
            .remove(0);
        pack.technical_evidence["rpc_url"] = Value::String("http://private-rpc".to_owned());
        assert!(!evidence_pack_is_publication_safe(&pack));
    }
}
