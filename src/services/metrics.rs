use std::{collections::{BTreeMap, BTreeSet}, sync::Arc};

use chrono::{DateTime, Duration, Utc};
use serde_json::json;
use uuid::Uuid;

use crate::{
    core::errors::{LedgerError, LedgerResult},
    domain::{
        entities::{ClassifiedTransaction, ClusterMetricsSnapshot},
        enums::{BehaviorTag, MetricsWindow},
        interfaces::AnalysisRepository,
    },
    services::behavior::stable_uuid_from_key,
};

#[derive(Clone)]
pub struct ClusterMetricsService {
    repository: Arc<dyn AnalysisRepository>,
}

impl ClusterMetricsService {
    pub fn new(repository: Arc<dyn AnalysisRepository>) -> Self {
        Self { repository }
    }

    pub async fn refresh_cluster_metrics(
        &self,
        cluster_id: Uuid,
    ) -> LedgerResult<Vec<ClusterMetricsSnapshot>> {
        let cluster = self
            .repository
            .get_cluster(cluster_id)
            .await?
            .ok_or_else(|| LedgerError::NotFound(format!("cluster {cluster_id} not found")))?;
        let now = Utc::now();
        let windows = [
            (MetricsWindow::OneHour, now - Duration::hours(1)),
            (MetricsWindow::TwentyFourHours, now - Duration::hours(24)),
            (MetricsWindow::SevenDays, now - Duration::days(7)),
        ];

        let mut snapshots = Vec::new();
        for (window, start) in windows {
            let txs = self
                .repository
                .list_classified_transactions_for_cluster(cluster_id, Some(start), Some(now))
                .await?;
            snapshots.push(self.build_snapshot(cluster.cluster_id, cluster.chain_id, window, start, now, &txs));
        }

        self.repository
            .replace_cluster_metrics_snapshots(cluster_id, &snapshots)
            .await?;
        Ok(snapshots)
    }

    pub async fn list_cluster_metrics(
        &self,
        cluster_id: Uuid,
    ) -> LedgerResult<Vec<ClusterMetricsSnapshot>> {
        self.repository
            .get_cluster(cluster_id)
            .await?
            .ok_or_else(|| LedgerError::NotFound(format!("cluster {cluster_id} not found")))?;
        self.repository.list_cluster_metrics_snapshots(cluster_id).await
    }

    fn build_snapshot(
        &self,
        cluster_id: Uuid,
        chain_id: u64,
        window: MetricsWindow,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        txs: &[ClassifiedTransaction],
    ) -> ClusterMetricsSnapshot {
        let tx_count = txs.len() as u32;
        let active_addresses_count = txs
            .iter()
            .map(|tx| tx.address.clone())
            .collect::<BTreeSet<_>>()
            .len() as u32;
        let volume_usd = sum_option_f64(txs.iter().map(|tx| tx.value_usd));
        let gas_cost_usd = sum_option_f64(txs.iter().map(|tx| tx.gas_cost_usd));
        let realized_pnl_usd = sum_option_f64(txs.iter().map(|tx| tx.estimated_pnl_usd));
        let external_inflow_usd = sum_positive_metric(txs, "external_inflow_usd");
        let external_outflow_usd = sum_positive_metric(txs, "external_outflow_usd");
        let estimated_capital = match (external_inflow_usd, external_outflow_usd) {
            (Some(inflow), Some(outflow)) => Some((inflow + outflow) / 2.0),
            (Some(inflow), None) => Some(inflow / 2.0),
            (None, Some(outflow)) => Some(outflow / 2.0),
            _ => None,
        };
        let roi = match (realized_pnl_usd, estimated_capital) {
            (Some(pnl), Some(capital)) if capital.abs() > f64::EPSILON => Some(pnl / capital),
            _ => None,
        };

        ClusterMetricsSnapshot {
            snapshot_id: snapshot_id(cluster_id, &window, end),
            cluster_id,
            chain_id,
            window,
            timestamp: end,
            tx_count,
            volume_usd,
            gas_cost_usd,
            realized_pnl_usd,
            roi,
            active_addresses_count,
            behavior_breakdown: behavior_breakdown_json(txs),
            metadata: json!({
                "window_start": start,
                "window_end": end,
                "external_inflow_usd": external_inflow_usd,
                "external_outflow_usd": external_outflow_usd,
                "estimated_capital_usd": estimated_capital,
            }),
        }
    }
}

fn snapshot_id(cluster_id: Uuid, window: &MetricsWindow, timestamp: DateTime<Utc>) -> Uuid {
    let key = format!("{cluster_id}:{}:{}", metrics_window_key(window), timestamp.format("%Y-%m-%dT%H"));
    stable_uuid_from_key(&key)
}

fn metrics_window_key(window: &MetricsWindow) -> &'static str {
    match window {
        MetricsWindow::OneHour => "1h",
        MetricsWindow::TwentyFourHours => "24h",
        MetricsWindow::SevenDays => "7d",
    }
}

fn sum_option_f64<I>(values: I) -> Option<f64>
where
    I: Iterator<Item = Option<f64>>,
{
    let collected = values.flatten().collect::<Vec<_>>();
    if collected.is_empty() {
        None
    } else {
        Some(collected.into_iter().sum())
    }
}

fn sum_positive_metric(txs: &[ClassifiedTransaction], field: &str) -> Option<f64> {
    let mut sum = 0.0f64;
    let mut seen = false;
    for tx in txs {
        if let Some(value) = tx
            .raw_json
            .get(field)
            .and_then(|value| value.as_f64())
        {
            seen = true;
            sum += value;
        }
    }
    if seen { Some(sum) } else { None }
}

fn behavior_breakdown_json(txs: &[ClassifiedTransaction]) -> serde_json::Value {
    let total = txs.len() as f64;
    let mut counts = BTreeMap::<&'static str, u32>::from([
        ("arbitrage", 0),
        ("position", 0),
        ("lp", 0),
        ("liquidation", 0),
        ("unknown", 0),
    ]);

    for tx in txs {
        let key = match tx.behavior_tag {
            BehaviorTag::Arbitrage => "arbitrage",
            BehaviorTag::Position => "position",
            BehaviorTag::Lp => "lp",
            BehaviorTag::Liquidation => "liquidation",
            BehaviorTag::Unknown => "unknown",
        };
        *counts.entry(key).or_default() += 1;
    }

    json!(
        counts
            .into_iter()
            .map(|(key, count)| {
                (
                    key,
                    json!({
                        "count": count,
                        "ratio": if total > 0.0 { (count as f64) / total } else { 0.0 },
                    }),
                )
            })
            .collect::<BTreeMap<_, _>>()
    )
}
