use std::sync::Arc;

use chrono::Utc;
use serde_json::json;
use uuid::Uuid;

use crate::{
    core::errors::{LedgerError, LedgerResult},
    domain::{
        entities::{ClassifiedTransaction, ClusterTrackingState},
        enums::{BehaviorTag, ClusterBackfillStatus, ClusterTrackingStatus},
        interfaces::AnalysisRepository,
    },
};

#[derive(Clone, Debug)]
pub struct ClassifiedTransactionsPage {
    pub items: Vec<ClassifiedTransaction>,
    pub total: u64,
    pub page: u32,
    pub page_size: u32,
}

#[derive(Clone)]
pub struct ClusterTrackingService {
    repository: Arc<dyn AnalysisRepository>,
}

impl ClusterTrackingService {
    pub fn new(repository: Arc<dyn AnalysisRepository>) -> Self {
        Self { repository }
    }

    pub async fn get_tracking_state(&self, cluster_id: Uuid) -> LedgerResult<ClusterTrackingState> {
        let cluster = self
            .repository
            .get_cluster(cluster_id)
            .await?
            .ok_or_else(|| LedgerError::NotFound(format!("cluster {cluster_id} not found")))?;

        if let Some(state) = self
            .repository
            .get_cluster_tracking_state(cluster_id)
            .await?
        {
            return Ok(state);
        }

        let last_processed_block = self
            .repository
            .list_cluster_backfill_runs(cluster_id)
            .await?
            .into_iter()
            .filter(|run| {
                matches!(
                    run.status,
                    ClusterBackfillStatus::Completed
                        | ClusterBackfillStatus::Partial
                        | ClusterBackfillStatus::Failed
                )
            })
            .filter_map(|run| run.end_block)
            .max();

        Ok(ClusterTrackingState {
            cluster_id,
            chain_id: cluster.chain_id,
            status: ClusterTrackingStatus::Paused,
            last_processed_block,
            last_processed_at: None,
            last_updated_at: None,
            error_message: None,
            tracked_address_count: cluster.addresses.len() as u32,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            metadata: json!({
                "source": "derived_default"
            }),
        })
    }

    pub async fn enable_tracking(&self, cluster_id: Uuid) -> LedgerResult<ClusterTrackingState> {
        let mut state = self.get_tracking_state(cluster_id).await?;
        let now = Utc::now();
        state.status = ClusterTrackingStatus::Active;
        state.error_message = None;
        state.updated_at = now;
        state.last_updated_at = Some(now);
        state.metadata = json!({
            "source": "enabled_via_api"
        });
        self.repository
            .upsert_cluster_tracking_state(&state)
            .await?;
        Ok(state)
    }

    pub async fn pause_tracking(&self, cluster_id: Uuid) -> LedgerResult<ClusterTrackingState> {
        let mut state = self.get_tracking_state(cluster_id).await?;
        let now = Utc::now();
        state.status = ClusterTrackingStatus::Paused;
        state.updated_at = now;
        state.last_updated_at = Some(now);
        state.metadata = json!({
            "source": "paused_via_api"
        });
        self.repository
            .upsert_cluster_tracking_state(&state)
            .await?;
        Ok(state)
    }

    pub async fn list_recent_classified_transactions(
        &self,
        cluster_id: Uuid,
        limit: u32,
    ) -> LedgerResult<Vec<ClassifiedTransaction>> {
        self.repository
            .get_cluster(cluster_id)
            .await?
            .ok_or_else(|| LedgerError::NotFound(format!("cluster {cluster_id} not found")))?;
        self.repository
            .list_recent_classified_transactions(cluster_id, limit.max(1))
            .await
    }

    pub async fn list_classified_transactions_page(
        &self,
        cluster_id: Uuid,
        page: u32,
        page_size: u32,
        behavior_tag: Option<BehaviorTag>,
        protocol_query: Option<String>,
        sort_by: String,
        descending: bool,
    ) -> LedgerResult<ClassifiedTransactionsPage> {
        self.repository
            .get_cluster(cluster_id)
            .await?
            .ok_or_else(|| LedgerError::NotFound(format!("cluster {cluster_id} not found")))?;
        let safe_page = page.max(1);
        let safe_page_size = page_size.clamp(1, 100);
        let offset = (safe_page - 1) * safe_page_size;
        let (items, total) = self
            .repository
            .list_classified_transactions_page(
                cluster_id,
                safe_page_size,
                offset,
                behavior_tag,
                protocol_query,
                sort_by,
                descending,
            )
            .await?;
        Ok(ClassifiedTransactionsPage {
            items,
            total,
            page: safe_page,
            page_size: safe_page_size,
        })
    }
}
