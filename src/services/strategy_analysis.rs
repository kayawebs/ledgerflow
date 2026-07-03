use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use chrono::{DateTime, Utc};
use serde::Serialize;
use uuid::Uuid;

use crate::{
    core::errors::{LedgerError, LedgerResult},
    domain::{entities::ClassifiedTransaction, enums::BehaviorTag, interfaces::AnalysisRepository},
};

#[derive(Clone)]
pub struct ClusterStrategyAnalysisService {
    repository: Arc<dyn AnalysisRepository>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ClusterStrategyAnalysis {
    pub cluster_id: Uuid,
    pub chain_id: u64,
    pub generated_at: DateTime<Utc>,
    pub window_start: Option<DateTime<Utc>>,
    pub window_end: Option<DateTime<Utc>>,
    pub tx_count: u32,
    pub arbitrage_tx_count: u32,
    pub successful_arbitrage_tx_count: u32,
    pub failed_arbitrage_tx_count: u32,
    pub arbitrage_success_rate: Option<f64>,
    pub total_estimated_pnl_usd: Option<f64>,
    pub total_gas_cost_usd: Option<f64>,
    pub top_routes: Vec<RouteStat>,
    pub top_protocols: Vec<ProtocolStat>,
    pub address_roles: Vec<AddressRoleStat>,
    pub strategy_breakdown: Vec<StrategyBreakdownRow>,
    pub behavior_profile: ClusterBehaviorProfile,
    pub performance: BotPerformanceSummary,
    pub arbitrage_report: Option<StrategyDetailReport>,
    pub liquidation_report: Option<StrategyDetailReport>,
    pub representative_transactions: Vec<RepresentativeTransaction>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ClusterBehaviorProfile {
    pub primary_behavior: String,
    pub primary_behavior_tx_count: u32,
    pub primary_behavior_tx_share: Option<f64>,
    pub active_addresses_count: u32,
    pub behavior_breakdown: Vec<StrategyBreakdownRow>,
}

#[derive(Clone, Debug, Serialize)]
pub struct BotPerformanceSummary {
    pub tx_count: u32,
    pub success_count: u32,
    pub failed_count: u32,
    pub success_rate: Option<f64>,
    pub estimated_pnl_usd: Option<f64>,
    pub gas_cost_usd: Option<f64>,
    pub avg_estimated_pnl_usd: Option<f64>,
    pub avg_gas_cost_usd: Option<f64>,
    pub pnl_to_gas_ratio: Option<f64>,
}

#[derive(Clone, Debug, Serialize)]
pub struct StrategyDetailReport {
    pub strategy_type: String,
    pub tx_count: u32,
    pub success_count: u32,
    pub failed_count: u32,
    pub success_rate: Option<f64>,
    pub estimated_pnl_usd: Option<f64>,
    pub gas_cost_usd: Option<f64>,
    pub avg_estimated_pnl_usd: Option<f64>,
    pub avg_gas_cost_usd: Option<f64>,
    pub pnl_to_gas_ratio: Option<f64>,
    pub top_routes: Vec<RouteStat>,
    pub top_protocols: Vec<StrategyProtocolStat>,
    pub representative_transactions: Vec<RepresentativeTransaction>,
}

#[derive(Clone, Debug, Serialize)]
pub struct StrategyProtocolStat {
    pub protocol: String,
    pub tx_count: u32,
    pub success_count: u32,
    pub success_rate: Option<f64>,
    pub estimated_pnl_usd: Option<f64>,
    pub gas_cost_usd: Option<f64>,
    pub sample_tx_hashes: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct RepresentativeTransaction {
    pub tx_hash: String,
    pub behavior_tag: String,
    pub protocol: Option<String>,
    pub address: String,
    pub block_number: u64,
    pub timestamp: Option<DateTime<Utc>>,
    pub estimated_pnl_usd: Option<f64>,
    pub gas_cost_usd: Option<f64>,
    pub route: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct RouteStat {
    pub route: String,
    pub tx_count: u32,
    pub success_count: u32,
    pub success_rate: Option<f64>,
    pub estimated_pnl_usd: Option<f64>,
    pub gas_cost_usd: Option<f64>,
    pub sample_tx_hashes: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ProtocolStat {
    pub protocol: String,
    pub tx_count: u32,
    pub arbitrage_tx_count: u32,
    pub success_count: u32,
    pub success_rate: Option<f64>,
    pub estimated_pnl_usd: Option<f64>,
    pub gas_cost_usd: Option<f64>,
    pub sample_tx_hashes: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct AddressRoleStat {
    pub address: String,
    pub configured_role: Option<String>,
    pub inferred_role: String,
    pub tx_count: u32,
    pub arbitrage_tx_count: u32,
    pub success_count: u32,
    pub estimated_pnl_usd: Option<f64>,
    pub sample_tx_hashes: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct StrategyBreakdownRow {
    pub strategy_type: String,
    pub tx_count: u32,
    pub success_count: u32,
    pub estimated_pnl_usd: Option<f64>,
}

impl ClusterStrategyAnalysisService {
    pub fn new(repository: Arc<dyn AnalysisRepository>) -> Self {
        Self { repository }
    }

    pub async fn analyze_cluster(
        &self,
        cluster_id: Uuid,
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
    ) -> LedgerResult<ClusterStrategyAnalysis> {
        let cluster = self
            .repository
            .get_cluster(cluster_id)
            .await?
            .ok_or_else(|| LedgerError::NotFound(format!("cluster {cluster_id} not found")))?;
        let txs = self
            .repository
            .list_classified_transactions_for_cluster(cluster_id, start, end)
            .await?;
        let role_by_address = cluster
            .addresses
            .iter()
            .map(|item| (item.address.to_ascii_lowercase(), item.role.clone()))
            .collect::<BTreeMap<_, _>>();

        let mut route_stats = BTreeMap::<String, Accumulator>::new();
        let mut protocol_stats = BTreeMap::<String, ProtocolAccumulator>::new();
        let mut address_stats = BTreeMap::<String, AddressAccumulator>::new();
        let mut strategy_stats = BTreeMap::<String, Accumulator>::new();
        let mut arbitrage_total = Accumulator::default();
        let mut arbitrage_route_stats = BTreeMap::<String, Accumulator>::new();
        let mut arbitrage_protocol_stats = BTreeMap::<String, Accumulator>::new();
        let mut liquidation_total = Accumulator::default();
        let mut liquidation_route_stats = BTreeMap::<String, Accumulator>::new();
        let mut liquidation_protocol_stats = BTreeMap::<String, Accumulator>::new();
        let mut active_addresses = BTreeSet::<String>::new();
        let mut total_pnl = Sum::default();
        let mut total_gas = Sum::default();
        let mut successful_tx_count = 0u32;
        let mut failed_tx_count = 0u32;
        let mut arbitrage_tx_count = 0u32;
        let mut successful_arbitrage_tx_count = 0u32;
        let mut failed_arbitrage_tx_count = 0u32;

        for tx in &txs {
            total_pnl.add(tx.estimated_pnl_usd);
            total_gas.add(tx.gas_cost_usd);
            let strategy_type = strategy_type(tx);
            let is_success = is_successful(tx);
            if is_success {
                successful_tx_count += 1;
            } else {
                failed_tx_count += 1;
            }
            strategy_stats
                .entry(strategy_type.clone())
                .or_default()
                .add(tx, is_success);

            let address_key = tx.address.to_ascii_lowercase();
            active_addresses.insert(address_key.clone());
            address_stats
                .entry(address_key.clone())
                .or_insert_with(|| AddressAccumulator {
                    configured_role: role_by_address.get(&address_key).cloned().flatten(),
                    inferred_role: "participant".to_owned(),
                    arbitrage_tx_count: 0,
                    inner: Accumulator::default(),
                })
                .add(tx, is_success);

            let protocol = tx.protocol.clone().unwrap_or_else(|| "unknown".to_owned());
            protocol_stats
                .entry(protocol.clone())
                .or_insert_with(|| ProtocolAccumulator {
                    protocol: protocol.clone(),
                    inner: Accumulator::default(),
                    arbitrage_tx_count: 0,
                })
                .add(tx, is_success);

            if tx.behavior_tag == BehaviorTag::Arbitrage {
                arbitrage_tx_count += 1;
                arbitrage_total.add(tx, is_success);
                if is_success {
                    successful_arbitrage_tx_count += 1;
                } else {
                    failed_arbitrage_tx_count += 1;
                }
                let route = route_key(tx);
                route_stats.entry(route).or_default().add(tx, is_success);
                let route = route_key(tx);
                arbitrage_route_stats
                    .entry(route)
                    .or_default()
                    .add(tx, is_success);
                arbitrage_protocol_stats
                    .entry(protocol.clone())
                    .or_default()
                    .add(tx, is_success);
                if let Some(item) = protocol_stats.get_mut(&protocol) {
                    item.arbitrage_tx_count += 1;
                }
                if let Some(item) = address_stats.get_mut(&address_key) {
                    item.inferred_role = infer_address_role(tx);
                    item.arbitrage_tx_count += 1;
                }
            } else if tx.behavior_tag == BehaviorTag::Liquidation {
                liquidation_total.add(tx, is_success);
                let route = route_key(tx);
                liquidation_route_stats
                    .entry(route)
                    .or_default()
                    .add(tx, is_success);
                liquidation_protocol_stats
                    .entry(protocol.clone())
                    .or_default()
                    .add(tx, is_success);
            }
        }

        let tx_count = txs.len() as u32;
        let top_routes = route_stats
            .into_iter()
            .map(|(route, stat)| stat.into_route(route))
            .collect::<Vec<_>>()
            .sorted_by_tx_count();
        let top_protocols = protocol_stats
            .into_values()
            .map(ProtocolAccumulator::into_stat)
            .collect::<Vec<_>>()
            .sorted_by_tx_count();
        let address_roles = address_stats
            .into_iter()
            .map(|(address, stat)| stat.into_stat(address))
            .collect::<Vec<_>>()
            .sorted_by_tx_count();
        let strategy_breakdown = strategy_stats
            .into_iter()
            .map(|(strategy_type, stat)| stat.into_strategy(strategy_type))
            .collect::<Vec<_>>()
            .sorted_by_tx_count();

        Ok(ClusterStrategyAnalysis {
            cluster_id,
            chain_id: cluster.chain_id,
            generated_at: Utc::now(),
            window_start: start,
            window_end: end,
            tx_count,
            arbitrage_tx_count,
            successful_arbitrage_tx_count,
            failed_arbitrage_tx_count,
            arbitrage_success_rate: ratio(successful_arbitrage_tx_count, arbitrage_tx_count),
            total_estimated_pnl_usd: total_pnl.value(),
            total_gas_cost_usd: total_gas.value(),
            top_routes,
            top_protocols,
            address_roles,
            behavior_profile: behavior_profile(
                &strategy_breakdown,
                tx_count,
                active_addresses.len() as u32,
            ),
            performance: BotPerformanceSummary {
                tx_count,
                success_count: successful_tx_count,
                failed_count: failed_tx_count,
                success_rate: ratio(successful_tx_count, tx_count),
                estimated_pnl_usd: total_pnl.value(),
                gas_cost_usd: total_gas.value(),
                avg_estimated_pnl_usd: total_pnl.average(),
                avg_gas_cost_usd: total_gas.average(),
                pnl_to_gas_ratio: ratio_f64(total_pnl.value(), total_gas.value()),
            },
            arbitrage_report: strategy_detail_report(
                "arbitrage",
                arbitrage_total,
                arbitrage_route_stats,
                arbitrage_protocol_stats,
                representative_transactions_for_behavior(&txs, &BehaviorTag::Arbitrage, 8),
            ),
            liquidation_report: strategy_detail_report(
                "liquidation",
                liquidation_total,
                liquidation_route_stats,
                liquidation_protocol_stats,
                representative_transactions_for_behavior(&txs, &BehaviorTag::Liquidation, 8),
            ),
            representative_transactions: representative_transactions(&txs, 12),
            strategy_breakdown,
        })
    }
}

#[derive(Default)]
struct Sum {
    value: f64,
    count: u32,
}

impl Sum {
    fn add(&mut self, value: Option<f64>) {
        if let Some(value) = value.filter(|value| value.is_finite()) {
            self.value += value;
            self.count += 1;
        }
    }

    fn value(&self) -> Option<f64> {
        (self.count > 0).then_some(self.value)
    }

    fn average(&self) -> Option<f64> {
        (self.count > 0).then_some(self.value / self.count as f64)
    }
}

#[derive(Default)]
struct Accumulator {
    tx_count: u32,
    success_count: u32,
    pnl: Sum,
    gas: Sum,
    sample_tx_hashes: Vec<String>,
}

impl Accumulator {
    fn add(&mut self, tx: &ClassifiedTransaction, success: bool) {
        self.tx_count += 1;
        if success {
            self.success_count += 1;
        }
        self.pnl.add(tx.estimated_pnl_usd);
        self.gas.add(tx.gas_cost_usd);
        push_unique_sample(&mut self.sample_tx_hashes, &tx.tx_hash, 5);
    }

    fn into_route(self, route: String) -> RouteStat {
        RouteStat {
            route,
            tx_count: self.tx_count,
            success_count: self.success_count,
            success_rate: ratio(self.success_count, self.tx_count),
            estimated_pnl_usd: self.pnl.value(),
            gas_cost_usd: self.gas.value(),
            sample_tx_hashes: self.sample_tx_hashes,
        }
    }

    fn into_strategy(self, strategy_type: String) -> StrategyBreakdownRow {
        StrategyBreakdownRow {
            strategy_type,
            tx_count: self.tx_count,
            success_count: self.success_count,
            estimated_pnl_usd: self.pnl.value(),
        }
    }

    fn into_strategy_protocol(self, protocol: String) -> StrategyProtocolStat {
        StrategyProtocolStat {
            protocol,
            tx_count: self.tx_count,
            success_count: self.success_count,
            success_rate: ratio(self.success_count, self.tx_count),
            estimated_pnl_usd: self.pnl.value(),
            gas_cost_usd: self.gas.value(),
            sample_tx_hashes: self.sample_tx_hashes,
        }
    }
}

struct ProtocolAccumulator {
    protocol: String,
    inner: Accumulator,
    arbitrage_tx_count: u32,
}

impl ProtocolAccumulator {
    fn add(&mut self, tx: &ClassifiedTransaction, success: bool) {
        self.inner.add(tx, success);
    }

    fn into_stat(self) -> ProtocolStat {
        ProtocolStat {
            protocol: self.protocol,
            tx_count: self.inner.tx_count,
            arbitrage_tx_count: self.arbitrage_tx_count,
            success_count: self.inner.success_count,
            success_rate: ratio(self.inner.success_count, self.inner.tx_count),
            estimated_pnl_usd: self.inner.pnl.value(),
            gas_cost_usd: self.inner.gas.value(),
            sample_tx_hashes: self.inner.sample_tx_hashes,
        }
    }
}

struct AddressAccumulator {
    configured_role: Option<String>,
    inferred_role: String,
    arbitrage_tx_count: u32,
    inner: Accumulator,
}

impl AddressAccumulator {
    fn add(&mut self, tx: &ClassifiedTransaction, success: bool) {
        self.inner.add(tx, success);
    }

    fn into_stat(self, address: String) -> AddressRoleStat {
        AddressRoleStat {
            address,
            configured_role: self.configured_role,
            inferred_role: self.inferred_role,
            tx_count: self.inner.tx_count,
            arbitrage_tx_count: self.arbitrage_tx_count,
            success_count: self.inner.success_count,
            estimated_pnl_usd: self.inner.pnl.value(),
            sample_tx_hashes: self.inner.sample_tx_hashes,
        }
    }
}

fn behavior_profile(
    strategy_breakdown: &[StrategyBreakdownRow],
    tx_count: u32,
    active_addresses_count: u32,
) -> ClusterBehaviorProfile {
    let primary = strategy_breakdown
        .iter()
        .max_by(|left, right| left.tx_count.cmp(&right.tx_count));
    ClusterBehaviorProfile {
        primary_behavior: primary
            .map(|item| item.strategy_type.clone())
            .unwrap_or_else(|| "unknown".to_owned()),
        primary_behavior_tx_count: primary.map(|item| item.tx_count).unwrap_or(0),
        primary_behavior_tx_share: primary.and_then(|item| ratio(item.tx_count, tx_count)),
        active_addresses_count,
        behavior_breakdown: strategy_breakdown.to_vec(),
    }
}

fn strategy_detail_report(
    strategy_type: &str,
    total: Accumulator,
    route_stats: BTreeMap<String, Accumulator>,
    protocol_stats: BTreeMap<String, Accumulator>,
    representative_transactions: Vec<RepresentativeTransaction>,
) -> Option<StrategyDetailReport> {
    if total.tx_count == 0 {
        return None;
    }
    Some(StrategyDetailReport {
        strategy_type: strategy_type.to_owned(),
        tx_count: total.tx_count,
        success_count: total.success_count,
        failed_count: total.tx_count.saturating_sub(total.success_count),
        success_rate: ratio(total.success_count, total.tx_count),
        estimated_pnl_usd: total.pnl.value(),
        gas_cost_usd: total.gas.value(),
        avg_estimated_pnl_usd: total.pnl.average(),
        avg_gas_cost_usd: total.gas.average(),
        pnl_to_gas_ratio: ratio_f64(total.pnl.value(), total.gas.value()),
        top_routes: route_stats
            .into_iter()
            .map(|(route, stat)| stat.into_route(route))
            .collect::<Vec<_>>()
            .sorted_by_tx_count(),
        top_protocols: protocol_stats
            .into_iter()
            .map(|(protocol, stat)| stat.into_strategy_protocol(protocol))
            .collect::<Vec<_>>()
            .sorted_by_tx_count(),
        representative_transactions,
    })
}

fn representative_transactions(
    txs: &[ClassifiedTransaction],
    limit: usize,
) -> Vec<RepresentativeTransaction> {
    let mut items = txs
        .iter()
        .map(representative_transaction)
        .collect::<Vec<_>>();
    sort_representative_transactions(&mut items);
    items.truncate(limit);
    items
}

fn representative_transactions_for_behavior(
    txs: &[ClassifiedTransaction],
    behavior_tag: &BehaviorTag,
    limit: usize,
) -> Vec<RepresentativeTransaction> {
    let mut items = txs
        .iter()
        .filter(|tx| &tx.behavior_tag == behavior_tag)
        .map(representative_transaction)
        .collect::<Vec<_>>();
    sort_representative_transactions(&mut items);
    items.truncate(limit);
    items
}

fn representative_transaction(tx: &ClassifiedTransaction) -> RepresentativeTransaction {
    RepresentativeTransaction {
        tx_hash: tx.tx_hash.clone(),
        behavior_tag: strategy_type(tx),
        protocol: tx.protocol.clone(),
        address: tx.address.clone(),
        block_number: tx.block_number,
        timestamp: tx.timestamp,
        estimated_pnl_usd: tx.estimated_pnl_usd,
        gas_cost_usd: tx.gas_cost_usd,
        route: route_key(tx),
    }
}

fn sort_representative_transactions(items: &mut [RepresentativeTransaction]) {
    items.sort_by(|left, right| {
        metric_sort_key(right.estimated_pnl_usd)
            .cmp(&metric_sort_key(left.estimated_pnl_usd))
            .then_with(|| right.block_number.cmp(&left.block_number))
    });
}

fn metric_sort_key(value: Option<f64>) -> i64 {
    value
        .filter(|value| value.is_finite())
        .map(|value| (value * 100.0).round() as i64)
        .unwrap_or(i64::MIN)
}

fn route_key(tx: &ClassifiedTransaction) -> String {
    match (tx.token_out.as_deref(), tx.token_in.as_deref()) {
        (Some(out), Some(input)) if !out.is_empty() && !input.is_empty() => {
            format!("{} -> {}", short_token(out), short_token(input))
        }
        (None, Some(input)) => format!("unknown -> {}", short_token(input)),
        (Some(out), None) => format!("{} -> unknown", short_token(out)),
        _ => "unknown_route".to_owned(),
    }
}

fn short_token(token: &str) -> String {
    let token = token.to_ascii_lowercase();
    match token.as_str() {
        "0x4200000000000000000000000000000000000006" => "WETH".to_owned(),
        "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913" => "USDC".to_owned(),
        "0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca" => "USDbC".to_owned(),
        "0xfde4c96c8593536e31f229ea8f37b2ada2699bb2" => "USDT".to_owned(),
        _ if token.len() > 12 => format!("{}...{}", &token[..8], &token[token.len() - 4..]),
        _ => token,
    }
}

fn strategy_type(tx: &ClassifiedTransaction) -> String {
    match tx.behavior_tag {
        BehaviorTag::Arbitrage => "arbitrage",
        BehaviorTag::Liquidation => "liquidation",
        BehaviorTag::Lp => "lp",
        BehaviorTag::Position => "position",
        BehaviorTag::Unknown => "unknown",
    }
    .to_owned()
}

fn is_successful(tx: &ClassifiedTransaction) -> bool {
    tx.estimated_pnl_usd
        .map(|value| value >= 0.0)
        .unwrap_or(true)
}

fn infer_address_role(tx: &ClassifiedTransaction) -> String {
    if tx.behavior_tag == BehaviorTag::Arbitrage {
        "arbitrage_executor".to_owned()
    } else {
        "participant".to_owned()
    }
}

fn ratio(numerator: u32, denominator: u32) -> Option<f64> {
    (denominator > 0).then_some(numerator as f64 / denominator as f64)
}

fn ratio_f64(numerator: Option<f64>, denominator: Option<f64>) -> Option<f64> {
    match (numerator, denominator) {
        (Some(numerator), Some(denominator)) if denominator.is_finite() && denominator != 0.0 => {
            Some(numerator / denominator)
        }
        _ => None,
    }
}

fn push_unique_sample(items: &mut Vec<String>, value: &str, limit: usize) {
    if items.len() < limit && !items.iter().any(|item| item == value) {
        items.push(value.to_owned());
    }
}

trait TxCountSortable {
    fn tx_count(&self) -> u32;
}

impl TxCountSortable for RouteStat {
    fn tx_count(&self) -> u32 {
        self.tx_count
    }
}

impl TxCountSortable for ProtocolStat {
    fn tx_count(&self) -> u32 {
        self.tx_count
    }
}

impl TxCountSortable for AddressRoleStat {
    fn tx_count(&self) -> u32 {
        self.tx_count
    }
}

impl TxCountSortable for StrategyBreakdownRow {
    fn tx_count(&self) -> u32 {
        self.tx_count
    }
}

impl TxCountSortable for StrategyProtocolStat {
    fn tx_count(&self) -> u32 {
        self.tx_count
    }
}

trait SortByTxCount: Sized {
    fn sorted_by_tx_count(self) -> Self;
}

impl<T> SortByTxCount for Vec<T>
where
    T: TxCountSortable,
{
    fn sorted_by_tx_count(mut self) -> Self {
        self.sort_by(|left, right| right.tx_count().cmp(&left.tx_count()));
        self
    }
}
