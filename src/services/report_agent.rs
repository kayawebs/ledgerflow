use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::{
    core::errors::{LedgerError, LedgerResult},
    services::strategy_analysis::{
        AddressRoleStat, ClusterStrategyAnalysis, ClusterStrategyAnalysisService,
        RepresentativeTransaction, RouteStat, StrategyDetailReport, StrategyProtocolStat,
    },
};

pub const REPORT_AGENT_MAX_ROW_LIMIT: u32 = 50;
pub const REPORT_AGENT_MAX_COST_BUDGET: u32 = 250;

#[derive(Clone)]
pub struct ReportAgentService {
    strategy_analysis_service: Arc<ClusterStrategyAnalysisService>,
}

#[derive(Clone, Debug)]
pub struct ReportAgentQuery {
    pub cluster_id: Uuid,
    pub window_start: Option<DateTime<Utc>>,
    pub window_end: Option<DateTime<Utc>>,
    pub all_time: bool,
    pub sections: Vec<ReportAgentSection>,
    pub filters: ReportAgentFilters,
    pub row_limit: u32,
    pub cost_budget: u32,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReportAgentSection {
    Overview,
    BehaviorProfile,
    Performance,
    Arbitrage,
    Liquidation,
    Routes,
    Protocols,
    Addresses,
    Samples,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ReportAgentFilters {
    pub strategy: Option<String>,
    pub protocol: Option<String>,
    pub route: Option<String>,
    pub address: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ReportAgentResponse {
    pub query: ReportAgentQueryEcho,
    pub plan: ReportAgentPlan,
    pub answer: String,
    pub rows: Vec<ReportAgentRow>,
    pub citations: Vec<ReportAgentCitation>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ReportAgentQueryEcho {
    pub cluster_id: Uuid,
    pub window_start: Option<DateTime<Utc>>,
    pub window_end: Option<DateTime<Utc>>,
    pub all_time: bool,
    pub sections: Vec<ReportAgentSection>,
    pub filters: ReportAgentFilters,
    pub row_limit: u32,
    pub cost_budget: u32,
}

#[derive(Clone, Debug, Serialize)]
pub struct ReportAgentPlan {
    pub allowed_sources: Vec<String>,
    pub estimated_cost: u32,
    pub row_limit: u32,
    pub sections: Vec<ReportAgentSection>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ReportAgentRow {
    pub section: String,
    pub item_type: String,
    pub label: String,
    pub metrics: Value,
    pub sample_tx_hashes: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ReportAgentCitation {
    pub source: String,
    pub section: String,
    pub row_ref: String,
    pub tx_hash: Option<String>,
}

impl ReportAgentService {
    pub fn new(strategy_analysis_service: Arc<ClusterStrategyAnalysisService>) -> Self {
        Self {
            strategy_analysis_service,
        }
    }

    pub async fn query_cluster_report(
        &self,
        query: ReportAgentQuery,
    ) -> LedgerResult<ReportAgentResponse> {
        validate_query(&query)?;
        let sections = normalize_sections(&query.sections);
        let snapshot = self
            .strategy_analysis_service
            .get_cached_cluster_report_snapshot(
                query.cluster_id,
                query.window_start,
                query.window_end,
            )
            .await?
            .ok_or_else(|| {
                LedgerError::NotFound(format!(
                    "cluster strategy report snapshot for {} not found",
                    query.cluster_id
                ))
            })?;
        let report: ClusterStrategyAnalysis = serde_json::from_value(snapshot.report_json.clone())
            .map_err(|err| LedgerError::Persistence(err.to_string()))?;

        let row_limit = query.row_limit.min(REPORT_AGENT_MAX_ROW_LIMIT);
        let mut rows = Vec::new();
        let mut citations = vec![ReportAgentCitation {
            source: "cluster_strategy_report_snapshots".to_owned(),
            section: "snapshot".to_owned(),
            row_ref: snapshot.window_key.clone(),
            tx_hash: None,
        }];
        let mut answer_lines = Vec::new();

        for section in &sections {
            match section {
                ReportAgentSection::Overview => {
                    push_overview(&report, &mut rows, &mut answer_lines);
                }
                ReportAgentSection::BehaviorProfile => {
                    push_behavior_profile(&report, row_limit, &mut rows, &mut answer_lines);
                }
                ReportAgentSection::Performance => {
                    push_performance(&report, &mut rows, &mut answer_lines);
                }
                ReportAgentSection::Arbitrage => {
                    push_strategy_report(
                        "arbitrage",
                        report.arbitrage_report.as_ref(),
                        &query.filters,
                        row_limit,
                        &mut rows,
                        &mut citations,
                        &mut answer_lines,
                    );
                }
                ReportAgentSection::Liquidation => {
                    push_strategy_report(
                        "liquidation",
                        report.liquidation_report.as_ref(),
                        &query.filters,
                        row_limit,
                        &mut rows,
                        &mut citations,
                        &mut answer_lines,
                    );
                }
                ReportAgentSection::Routes => {
                    push_routes(&report.top_routes, &query.filters, row_limit, &mut rows);
                }
                ReportAgentSection::Protocols => {
                    push_protocols(&report.top_protocols, &query.filters, row_limit, &mut rows);
                }
                ReportAgentSection::Addresses => {
                    push_addresses(&report.address_roles, &query.filters, row_limit, &mut rows);
                }
                ReportAgentSection::Samples => {
                    push_samples(
                        &report.representative_transactions,
                        &query.filters,
                        row_limit,
                        &mut rows,
                        &mut citations,
                    );
                }
            }
        }

        let estimated_cost = estimate_cost(&sections, &rows);
        if estimated_cost > query.cost_budget {
            return Err(LedgerError::AnalysisInvariant(format!(
                "report query estimated cost {estimated_cost} exceeds budget {}",
                query.cost_budget
            )));
        }

        if answer_lines.is_empty() {
            answer_lines.push(
                "No matching report summary was available for the requested sections and filters."
                    .to_owned(),
            );
        }

        Ok(ReportAgentResponse {
            query: ReportAgentQueryEcho {
                cluster_id: query.cluster_id,
                window_start: query.window_start,
                window_end: query.window_end,
                all_time: query.all_time,
                sections,
                filters: query.filters,
                row_limit,
                cost_budget: query.cost_budget,
            },
            plan: ReportAgentPlan {
                allowed_sources: vec!["cluster_strategy_report_snapshots".to_owned()],
                estimated_cost,
                row_limit,
                sections: normalize_sections(&query.sections),
            },
            answer: answer_lines.join("\n"),
            rows,
            citations,
        })
    }
}

fn validate_query(query: &ReportAgentQuery) -> LedgerResult<()> {
    if query.cluster_id.is_nil() {
        return Err(LedgerError::AnalysisInvariant(
            "cluster scope is required".to_owned(),
        ));
    }
    if !query.all_time && query.window_start.is_none() && query.window_end.is_none() {
        return Err(LedgerError::AnalysisInvariant(
            "report query requires a bounded window or all_time=true".to_owned(),
        ));
    }
    if let (Some(start), Some(end)) = (query.window_start, query.window_end) {
        if start > end {
            return Err(LedgerError::AnalysisInvariant(
                "window_start must be before window_end".to_owned(),
            ));
        }
    }
    if query.row_limit == 0 || query.row_limit > REPORT_AGENT_MAX_ROW_LIMIT {
        return Err(LedgerError::AnalysisInvariant(format!(
            "row_limit must be between 1 and {REPORT_AGENT_MAX_ROW_LIMIT}"
        )));
    }
    if query.cost_budget == 0 || query.cost_budget > REPORT_AGENT_MAX_COST_BUDGET {
        return Err(LedgerError::AnalysisInvariant(format!(
            "cost_budget must be between 1 and {REPORT_AGENT_MAX_COST_BUDGET}"
        )));
    }
    Ok(())
}

fn normalize_sections(sections: &[ReportAgentSection]) -> Vec<ReportAgentSection> {
    let mut sections = if sections.is_empty() {
        vec![
            ReportAgentSection::Overview,
            ReportAgentSection::Performance,
            ReportAgentSection::Arbitrage,
            ReportAgentSection::Liquidation,
        ]
    } else {
        sections.to_vec()
    };
    sections.sort();
    sections.dedup();
    sections
}

fn push_overview(
    report: &ClusterStrategyAnalysis,
    rows: &mut Vec<ReportAgentRow>,
    answer_lines: &mut Vec<String>,
) {
    rows.push(ReportAgentRow {
        section: "overview".to_owned(),
        item_type: "cluster".to_owned(),
        label: report.cluster_id.to_string(),
        metrics: json!({
            "chain_id": report.chain_id,
            "tx_count": report.tx_count,
            "primary_behavior": report.behavior_profile.primary_behavior,
            "active_addresses_count": report.behavior_profile.active_addresses_count,
        }),
        sample_tx_hashes: Vec::new(),
    });
    answer_lines.push(format!(
        "Cluster {} is primarily {}: {} of {} classified tx ({}) across {} active addresses.",
        report.cluster_id,
        report.behavior_profile.primary_behavior,
        report.behavior_profile.primary_behavior_tx_count,
        report.tx_count,
        format_percent(report.behavior_profile.primary_behavior_tx_share),
        report.behavior_profile.active_addresses_count,
    ));
}

fn push_behavior_profile(
    report: &ClusterStrategyAnalysis,
    row_limit: u32,
    rows: &mut Vec<ReportAgentRow>,
    answer_lines: &mut Vec<String>,
) {
    for item in report
        .behavior_profile
        .behavior_breakdown
        .iter()
        .take(row_limit as usize)
    {
        rows.push(ReportAgentRow {
            section: "behavior_profile".to_owned(),
            item_type: "behavior".to_owned(),
            label: item.strategy_type.clone(),
            metrics: json!({
                "tx_count": item.tx_count,
                "success_count": item.success_count,
                "estimated_pnl_usd": item.estimated_pnl_usd,
            }),
            sample_tx_hashes: Vec::new(),
        });
    }
    answer_lines.push(format!(
        "Behavior mix includes {} classified behavior buckets.",
        report.behavior_profile.behavior_breakdown.len()
    ));
}

fn push_performance(
    report: &ClusterStrategyAnalysis,
    rows: &mut Vec<ReportAgentRow>,
    answer_lines: &mut Vec<String>,
) {
    let performance = &report.performance;
    rows.push(ReportAgentRow {
        section: "performance".to_owned(),
        item_type: "summary".to_owned(),
        label: "bot_performance".to_owned(),
        metrics: json!({
            "tx_count": performance.tx_count,
            "success_count": performance.success_count,
            "failed_count": performance.failed_count,
            "success_rate": performance.success_rate,
            "estimated_pnl_usd": performance.estimated_pnl_usd,
            "gas_cost_usd": performance.gas_cost_usd,
            "avg_estimated_pnl_usd": performance.avg_estimated_pnl_usd,
            "avg_gas_cost_usd": performance.avg_gas_cost_usd,
            "pnl_to_gas_ratio": performance.pnl_to_gas_ratio,
        }),
        sample_tx_hashes: Vec::new(),
    });
    answer_lines.push(format!(
        "Performance: {} success / {} failed, success rate {}, estimated PnL {}, gas {}, PnL/gas {}.",
        performance.success_count,
        performance.failed_count,
        format_percent(performance.success_rate),
        format_usd(performance.estimated_pnl_usd),
        format_usd(performance.gas_cost_usd),
        format_ratio(performance.pnl_to_gas_ratio),
    ));
}

fn push_strategy_report(
    strategy: &str,
    report: Option<&StrategyDetailReport>,
    filters: &ReportAgentFilters,
    row_limit: u32,
    rows: &mut Vec<ReportAgentRow>,
    citations: &mut Vec<ReportAgentCitation>,
    answer_lines: &mut Vec<String>,
) {
    if !matches_filter(filters.strategy.as_deref(), strategy) {
        return;
    }
    let Some(report) = report else {
        answer_lines.push(format!(
            "No {strategy} report is available for this cluster window."
        ));
        return;
    };
    rows.push(ReportAgentRow {
        section: strategy.to_owned(),
        item_type: "strategy_summary".to_owned(),
        label: report.strategy_type.clone(),
        metrics: json!({
            "tx_count": report.tx_count,
            "success_count": report.success_count,
            "failed_count": report.failed_count,
            "success_rate": report.success_rate,
            "estimated_pnl_usd": report.estimated_pnl_usd,
            "gas_cost_usd": report.gas_cost_usd,
            "pnl_to_gas_ratio": report.pnl_to_gas_ratio,
        }),
        sample_tx_hashes: Vec::new(),
    });
    push_routes(&report.top_routes, filters, row_limit, rows);
    push_strategy_protocols(&report.top_protocols, filters, row_limit, rows);
    push_samples(
        &report.representative_transactions,
        filters,
        row_limit,
        rows,
        citations,
    );
    answer_lines.push(format!(
        "{}: {} tx, {} success / {} failed, success rate {}, estimated PnL {}, gas {}, PnL/gas {}.",
        title_case(strategy),
        report.tx_count,
        report.success_count,
        report.failed_count,
        format_percent(report.success_rate),
        format_usd(report.estimated_pnl_usd),
        format_usd(report.gas_cost_usd),
        format_ratio(report.pnl_to_gas_ratio),
    ));
}

fn push_routes(
    routes: &[RouteStat],
    filters: &ReportAgentFilters,
    row_limit: u32,
    rows: &mut Vec<ReportAgentRow>,
) {
    for route in routes
        .iter()
        .filter(|item| matches_filter(filters.route.as_deref(), &item.route))
        .take(row_limit as usize)
    {
        rows.push(ReportAgentRow {
            section: "routes".to_owned(),
            item_type: "route".to_owned(),
            label: route.route.clone(),
            metrics: json!({
                "tx_count": route.tx_count,
                "success_count": route.success_count,
                "success_rate": route.success_rate,
                "estimated_pnl_usd": route.estimated_pnl_usd,
                "gas_cost_usd": route.gas_cost_usd,
            }),
            sample_tx_hashes: route.sample_tx_hashes.clone(),
        });
    }
}

fn push_protocols(
    protocols: &[crate::services::strategy_analysis::ProtocolStat],
    filters: &ReportAgentFilters,
    row_limit: u32,
    rows: &mut Vec<ReportAgentRow>,
) {
    for protocol in protocols
        .iter()
        .filter(|item| matches_filter(filters.protocol.as_deref(), &item.protocol))
        .take(row_limit as usize)
    {
        rows.push(ReportAgentRow {
            section: "protocols".to_owned(),
            item_type: "protocol".to_owned(),
            label: protocol.protocol.clone(),
            metrics: json!({
                "tx_count": protocol.tx_count,
                "arbitrage_tx_count": protocol.arbitrage_tx_count,
                "success_count": protocol.success_count,
                "success_rate": protocol.success_rate,
                "estimated_pnl_usd": protocol.estimated_pnl_usd,
                "gas_cost_usd": protocol.gas_cost_usd,
            }),
            sample_tx_hashes: protocol.sample_tx_hashes.clone(),
        });
    }
}

fn push_strategy_protocols(
    protocols: &[StrategyProtocolStat],
    filters: &ReportAgentFilters,
    row_limit: u32,
    rows: &mut Vec<ReportAgentRow>,
) {
    for protocol in protocols
        .iter()
        .filter(|item| matches_filter(filters.protocol.as_deref(), &item.protocol))
        .take(row_limit as usize)
    {
        rows.push(ReportAgentRow {
            section: "protocols".to_owned(),
            item_type: "protocol".to_owned(),
            label: protocol.protocol.clone(),
            metrics: json!({
                "tx_count": protocol.tx_count,
                "success_count": protocol.success_count,
                "success_rate": protocol.success_rate,
                "estimated_pnl_usd": protocol.estimated_pnl_usd,
                "gas_cost_usd": protocol.gas_cost_usd,
            }),
            sample_tx_hashes: protocol.sample_tx_hashes.clone(),
        });
    }
}

fn push_addresses(
    addresses: &[AddressRoleStat],
    filters: &ReportAgentFilters,
    row_limit: u32,
    rows: &mut Vec<ReportAgentRow>,
) {
    for address in addresses
        .iter()
        .filter(|item| matches_filter(filters.address.as_deref(), &item.address))
        .take(row_limit as usize)
    {
        rows.push(ReportAgentRow {
            section: "addresses".to_owned(),
            item_type: "address".to_owned(),
            label: address.address.clone(),
            metrics: json!({
                "configured_role": address.configured_role,
                "inferred_role": address.inferred_role,
                "tx_count": address.tx_count,
                "arbitrage_tx_count": address.arbitrage_tx_count,
                "success_count": address.success_count,
                "estimated_pnl_usd": address.estimated_pnl_usd,
            }),
            sample_tx_hashes: address.sample_tx_hashes.clone(),
        });
    }
}

fn push_samples(
    transactions: &[RepresentativeTransaction],
    filters: &ReportAgentFilters,
    row_limit: u32,
    rows: &mut Vec<ReportAgentRow>,
    citations: &mut Vec<ReportAgentCitation>,
) {
    for tx in transactions
        .iter()
        .filter(|tx| sample_matches_filters(tx, filters))
        .take(row_limit as usize)
    {
        if rows.iter().any(|row| {
            row.item_type == "transaction" && row.label.eq_ignore_ascii_case(&tx.tx_hash)
        }) {
            continue;
        }
        rows.push(ReportAgentRow {
            section: "samples".to_owned(),
            item_type: "transaction".to_owned(),
            label: tx.tx_hash.clone(),
            metrics: json!({
                "behavior_tag": tx.behavior_tag,
                "strategy_subtype": tx.strategy_subtype,
                "protocol": tx.protocol,
                "protocol_family": tx.protocol_family,
                "address": tx.address,
                "block_number": tx.block_number,
                "timestamp": tx.timestamp,
                "estimated_pnl_usd": tx.estimated_pnl_usd,
                "gas_cost_usd": tx.gas_cost_usd,
                "route": tx.route,
            }),
            sample_tx_hashes: vec![tx.tx_hash.clone()],
        });
        if !citations
            .iter()
            .any(|citation| citation.tx_hash.as_deref() == Some(tx.tx_hash.as_str()))
        {
            citations.push(ReportAgentCitation {
                source: "cluster_strategy_report_snapshots".to_owned(),
                section: "samples".to_owned(),
                row_ref: tx.tx_hash.clone(),
                tx_hash: Some(tx.tx_hash.clone()),
            });
        }
    }
}

fn sample_matches_filters(tx: &RepresentativeTransaction, filters: &ReportAgentFilters) -> bool {
    matches_filter(filters.strategy.as_deref(), &tx.behavior_tag)
        && matches_filter(
            filters.protocol.as_deref(),
            tx.protocol.as_deref().unwrap_or("n/a"),
        )
        && matches_filter(filters.route.as_deref(), &tx.route)
        && matches_filter(filters.address.as_deref(), &tx.address)
}

fn matches_filter(filter: Option<&str>, value: &str) -> bool {
    filter
        .map(|filter| value.eq_ignore_ascii_case(filter))
        .unwrap_or(true)
}

fn estimate_cost(sections: &[ReportAgentSection], rows: &[ReportAgentRow]) -> u32 {
    1 + sections.len() as u32 + rows.len() as u32
}

fn format_usd(value: Option<f64>) -> String {
    value
        .map(|value| format!("${value:.2}"))
        .unwrap_or_else(|| "n/a".to_owned())
}

fn format_percent(value: Option<f64>) -> String {
    value
        .map(|value| format!("{:.2}%", value * 100.0))
        .unwrap_or_else(|| "n/a".to_owned())
}

fn format_ratio(value: Option<f64>) -> String {
    value
        .map(|value| format!("{value:.2}x"))
        .unwrap_or_else(|| "n/a".to_owned())
}

fn title_case(value: &str) -> String {
    let mut chars = value.chars();
    match chars.next() {
        Some(first) => format!("{}{}", first.to_uppercase(), chars.as_str()),
        None => String::new(),
    }
}
