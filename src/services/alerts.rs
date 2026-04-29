use std::{collections::BTreeMap, sync::Arc};

use chrono::Utc;
use serde_json::json;
use uuid::Uuid;

use crate::{
    core::errors::{LedgerError, LedgerResult},
    domain::{
        entities::{ClusterAlert, ClusterMetricsSnapshot},
        enums::{AlertSeverity, AlertType, MetricsWindow},
        interfaces::AnalysisRepository,
    },
    services::behavior::alert_bucket_key,
};

const DEFAULT_EXTERNAL_INFLOW_THRESHOLD_USD: f64 = 100_000.0;

#[derive(Clone)]
pub struct ClusterAlertService {
    repository: Arc<dyn AnalysisRepository>,
    external_inflow_threshold_usd: f64,
}

impl ClusterAlertService {
    pub fn new(repository: Arc<dyn AnalysisRepository>) -> Self {
        Self {
            repository,
            external_inflow_threshold_usd: DEFAULT_EXTERNAL_INFLOW_THRESHOLD_USD,
        }
    }

    pub async fn refresh_cluster_alerts(&self, cluster_id: Uuid) -> LedgerResult<Vec<ClusterAlert>> {
        let cluster = self
            .repository
            .get_cluster(cluster_id)
            .await?
            .ok_or_else(|| LedgerError::NotFound(format!("cluster {cluster_id} not found")))?;
        let snapshots = self.repository.list_cluster_metrics_snapshots(cluster_id).await?;
        let latest = latest_snapshots_by_window(&snapshots);
        let mut alerts = Vec::new();

        if let Some(one_hour) = latest.get(&MetricsWindow::OneHour) {
            if let Some(roi) = one_hour.roi
                && roi > 0.02
            {
                alerts.push(build_alert(
                    cluster_id,
                    cluster.chain_id,
                    AlertType::RoiAnomaly,
                    if roi > 0.05 { AlertSeverity::High } else { AlertSeverity::Medium },
                    "1h ROI anomaly",
                    format!("1h ROI reached {:.2}%.", roi * 100.0),
                    one_hour.timestamp,
                    json!({ "window": "1h", "roi": roi }),
                ));
            }

            let average_hourly_tx_count = latest
                .get(&MetricsWindow::TwentyFourHours)
                .map(|snapshot| snapshot.tx_count as f64 / 24.0)
                .unwrap_or(0.0);
            if average_hourly_tx_count > 0.0
                && (one_hour.tx_count as f64) > average_hourly_tx_count * 2.0
            {
                alerts.push(build_alert(
                    cluster_id,
                    cluster.chain_id,
                    AlertType::TxCountSpike,
                    AlertSeverity::Medium,
                    "1h tx count spike",
                    format!(
                        "1h tx_count {} is above 2x the 24h hourly average {:.2}.",
                        one_hour.tx_count, average_hourly_tx_count
                    ),
                    one_hour.timestamp,
                    json!({
                        "window": "1h",
                        "tx_count": one_hour.tx_count,
                        "hourly_average_24h": average_hourly_tx_count,
                    }),
                ));
            }

            let average_hourly_volume = latest
                .get(&MetricsWindow::TwentyFourHours)
                .and_then(|snapshot| snapshot.volume_usd)
                .map(|value| value / 24.0)
                .unwrap_or(0.0);
            if average_hourly_volume > 0.0
                && one_hour.volume_usd.unwrap_or_default() > average_hourly_volume * 2.0
            {
                alerts.push(build_alert(
                    cluster_id,
                    cluster.chain_id,
                    AlertType::VolumeSpike,
                    AlertSeverity::Medium,
                    "1h volume spike",
                    format!(
                        "1h volume ${:.2} is above 2x the 24h hourly average ${:.2}.",
                        one_hour.volume_usd.unwrap_or_default(),
                        average_hourly_volume
                    ),
                    one_hour.timestamp,
                    json!({
                        "window": "1h",
                        "volume_usd": one_hour.volume_usd,
                        "hourly_average_24h": average_hourly_volume,
                    }),
                ));
            }

            let arbitrage_ratio_1h = ratio_from_snapshot(one_hour, "arbitrage").unwrap_or(0.0);
            let arbitrage_ratio_24h = latest
                .get(&MetricsWindow::TwentyFourHours)
                .and_then(|snapshot| ratio_from_snapshot(snapshot, "arbitrage"))
                .unwrap_or(0.0);
            if arbitrage_ratio_1h > 0.60 && arbitrage_ratio_24h < 0.30 {
                alerts.push(build_alert(
                    cluster_id,
                    cluster.chain_id,
                    AlertType::BehaviorShift,
                    AlertSeverity::High,
                    "Arbitrage behavior spike",
                    format!(
                        "1h arbitrage ratio {:.2}% jumped from 24h ratio {:.2}%.",
                        arbitrage_ratio_1h * 100.0,
                        arbitrage_ratio_24h * 100.0
                    ),
                    one_hour.timestamp,
                    json!({
                        "window": "1h",
                        "arbitrage_ratio_1h": arbitrage_ratio_1h,
                        "arbitrage_ratio_24h": arbitrage_ratio_24h,
                    }),
                ));
            }

            let inflow = one_hour
                .metadata
                .get("external_inflow_usd")
                .and_then(|value| value.as_f64())
                .unwrap_or_default();
            if inflow > self.external_inflow_threshold_usd {
                alerts.push(build_alert(
                    cluster_id,
                    cluster.chain_id,
                    AlertType::ExternalInflow,
                    if inflow > self.external_inflow_threshold_usd * 2.0 {
                        AlertSeverity::High
                    } else {
                        AlertSeverity::Medium
                    },
                    "Large external inflow",
                    format!(
                        "1h external inflow reached ${:.2}, above the ${:.2} threshold.",
                        inflow, self.external_inflow_threshold_usd
                    ),
                    one_hour.timestamp,
                    json!({
                        "window": "1h",
                        "external_inflow_usd": inflow,
                        "threshold_usd": self.external_inflow_threshold_usd,
                    }),
                ));
            }
        }

        if let Some(twenty_four_hours) = latest.get(&MetricsWindow::TwentyFourHours)
            && let Some(roi) = twenty_four_hours.roi
            && roi > 0.05
        {
            alerts.push(build_alert(
                cluster_id,
                cluster.chain_id,
                AlertType::RoiAnomaly,
                if roi > 0.10 { AlertSeverity::High } else { AlertSeverity::Medium },
                "24h ROI anomaly",
                format!("24h ROI reached {:.2}%.", roi * 100.0),
                twenty_four_hours.timestamp,
                json!({ "window": "24h", "roi": roi }),
            ));
        }

        self.repository.insert_cluster_alerts(&alerts).await?;
        Ok(alerts)
    }

    pub async fn list_cluster_alerts(
        &self,
        cluster_id: Uuid,
        include_read: bool,
        limit: u32,
    ) -> LedgerResult<Vec<ClusterAlert>> {
        self.repository
            .get_cluster(cluster_id)
            .await?
            .ok_or_else(|| LedgerError::NotFound(format!("cluster {cluster_id} not found")))?;
        self.repository
            .list_cluster_alerts(cluster_id, include_read, limit.max(1))
            .await
    }

    pub async fn mark_alert_read(
        &self,
        cluster_id: Uuid,
        alert_id: Uuid,
    ) -> LedgerResult<ClusterAlert> {
        self.repository
            .mark_cluster_alert_read(cluster_id, alert_id)
            .await?
            .ok_or_else(|| LedgerError::NotFound(format!("alert {alert_id} not found")))
    }
}

fn latest_snapshots_by_window(
    snapshots: &[ClusterMetricsSnapshot],
) -> BTreeMap<MetricsWindow, ClusterMetricsSnapshot> {
    let mut latest: BTreeMap<MetricsWindow, ClusterMetricsSnapshot> = BTreeMap::new();
    for snapshot in snapshots {
        match latest.get(&snapshot.window) {
            Some(existing) if existing.timestamp >= snapshot.timestamp => {}
            _ => {
                latest.insert(snapshot.window.clone(), snapshot.clone());
            }
        }
    }
    latest
}

fn ratio_from_snapshot(snapshot: &ClusterMetricsSnapshot, key: &str) -> Option<f64> {
    snapshot
        .behavior_breakdown
        .get(key)
        .and_then(|value| value.get("ratio"))
        .and_then(|value| value.as_f64())
}

fn build_alert(
    cluster_id: Uuid,
    chain_id: u64,
    alert_type: AlertType,
    severity: AlertSeverity,
    title: &str,
    message: String,
    triggered_at: chrono::DateTime<Utc>,
    related_metric: serde_json::Value,
) -> ClusterAlert {
    let scope = format!("{alert_type:?}:{title}");
    ClusterAlert {
        alert_id: alert_bucket_key(cluster_id, &scope, triggered_at),
        cluster_id,
        chain_id,
        alert_type,
        severity,
        title: title.to_owned(),
        message,
        related_metric,
        triggered_at,
        is_read: false,
        read_at: None,
        metadata: json!({}),
    }
}
