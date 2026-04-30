use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::core::errors::LedgerResult;
use crate::domain::entities::{
    AddressCluster, ClassifiedTransaction, ClusterAddress, ClusterAlert, ClusterBackfillRun,
    ClusterCoverage, ClusterDiscoveredTx, ClusterMetricsSnapshot, ClusterTrackingState,
    EvidencePack, RecognizerResult, TransactionAnalysis,
};
use crate::domain::value_objects::{AddressTxRef, AssetFlow, ValidationMessage};
use crate::ingestion::models::RawTransactionBundle;

#[async_trait]
pub trait AnalysisRepository: Send + Sync {
    async fn save_transaction_bundle(&self, bundle: &RawTransactionBundle) -> LedgerResult<()>;
    async fn save_analysis(&self, analysis: &TransactionAnalysis) -> LedgerResult<()>;
    async fn save_evidence_pack(&self, pack: &EvidencePack) -> LedgerResult<()>;
    async fn get_analysis(
        &self,
        chain_id: u64,
        tx_hash: &str,
    ) -> LedgerResult<Option<TransactionAnalysis>>;
    async fn get_evidence_pack(&self, pack_id: &str) -> LedgerResult<Option<EvidencePack>>;
    async fn get_evidence_pack_by_tx(
        &self,
        chain_id: u64,
        tx_hash: &str,
    ) -> LedgerResult<Option<EvidencePack>>;
    async fn create_cluster(&self, cluster: &AddressCluster) -> LedgerResult<()>;
    async fn replace_cluster_addresses(
        &self,
        cluster_id: uuid::Uuid,
        addresses: &[ClusterAddress],
    ) -> LedgerResult<()>;
    async fn delete_cluster(&self, cluster_id: uuid::Uuid) -> LedgerResult<()>;
    async fn get_cluster(&self, cluster_id: uuid::Uuid) -> LedgerResult<Option<AddressCluster>>;
    async fn list_clusters(&self) -> LedgerResult<Vec<AddressCluster>>;
    async fn list_analyses_for_cluster(
        &self,
        chain_id: u64,
        addresses: &[String],
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
    ) -> LedgerResult<Vec<TransactionAnalysis>>;
    async fn create_cluster_backfill_run(&self, run: &ClusterBackfillRun) -> LedgerResult<()>;
    async fn update_cluster_backfill_run(&self, run: &ClusterBackfillRun) -> LedgerResult<()>;
    async fn list_cluster_backfill_runs(
        &self,
        cluster_id: uuid::Uuid,
    ) -> LedgerResult<Vec<ClusterBackfillRun>>;
    async fn claim_next_pending_cluster_backfill_run(
        &self,
    ) -> LedgerResult<Option<ClusterBackfillRun>>;
    async fn upsert_cluster_discovered_txs(&self, txs: &[ClusterDiscoveredTx]) -> LedgerResult<()>;
    async fn update_cluster_discovered_tx_status(
        &self,
        cluster_id: uuid::Uuid,
        tx_hash: &str,
        status: crate::domain::enums::ClusterTxStatus,
        latest_run_id: uuid::Uuid,
        error: Option<String>,
        analyzed_at: Option<DateTime<Utc>>,
    ) -> LedgerResult<()>;
    async fn get_cluster_coverage(
        &self,
        cluster_id: uuid::Uuid,
    ) -> LedgerResult<Option<ClusterCoverage>>;
    async fn upsert_cluster_tracking_state(
        &self,
        state: &ClusterTrackingState,
    ) -> LedgerResult<()>;
    async fn get_cluster_tracking_state(
        &self,
        cluster_id: uuid::Uuid,
    ) -> LedgerResult<Option<ClusterTrackingState>>;
    async fn list_active_cluster_tracking_states(&self) -> LedgerResult<Vec<ClusterTrackingState>>;
    async fn upsert_classified_transactions(
        &self,
        txs: &[ClassifiedTransaction],
    ) -> LedgerResult<()>;
    async fn list_recent_classified_transactions(
        &self,
        cluster_id: uuid::Uuid,
        limit: u32,
    ) -> LedgerResult<Vec<ClassifiedTransaction>>;
    async fn list_classified_transactions_for_cluster(
        &self,
        cluster_id: uuid::Uuid,
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
    ) -> LedgerResult<Vec<ClassifiedTransaction>>;
    async fn replace_cluster_metrics_snapshots(
        &self,
        cluster_id: uuid::Uuid,
        snapshots: &[ClusterMetricsSnapshot],
    ) -> LedgerResult<()>;
    async fn list_cluster_metrics_snapshots(
        &self,
        cluster_id: uuid::Uuid,
    ) -> LedgerResult<Vec<ClusterMetricsSnapshot>>;
    async fn insert_cluster_alerts(&self, alerts: &[ClusterAlert]) -> LedgerResult<()>;
    async fn list_cluster_alerts(
        &self,
        cluster_id: uuid::Uuid,
        include_read: bool,
        limit: u32,
    ) -> LedgerResult<Vec<ClusterAlert>>;
    async fn mark_cluster_alert_read(
        &self,
        cluster_id: uuid::Uuid,
        alert_id: uuid::Uuid,
    ) -> LedgerResult<Option<ClusterAlert>>;
}

pub type DynRepository = Arc<dyn AnalysisRepository>;

pub trait FlowDeriver: Send + Sync {
    fn derive(
        &self,
        bundle: &RawTransactionBundle,
        analysis: &TransactionAnalysis,
    ) -> Vec<AssetFlow>;
}

pub trait Validator: Send + Sync {
    fn validate(
        &self,
        bundle: &RawTransactionBundle,
        analysis: &TransactionAnalysis,
        pack: Option<&EvidencePack>,
    ) -> Vec<ValidationMessage>;
}

pub trait Recognizer: Send + Sync {
    fn recognizer_id(&self) -> &str;
    fn version(&self) -> &str;
    fn order(&self) -> u32;
    fn description(&self) -> &str;
    fn recognize(&self, analysis: &TransactionAnalysis) -> LedgerResult<Option<RecognizerResult>>;
}

#[async_trait]
pub trait ClusterTxSource: Send + Sync {
    async fn get_last_indexed_block(&self, chain_id: u64) -> LedgerResult<u64>;
    async fn query_address_refs(
        &self,
        chain_id: u64,
        address: &str,
        limit: u32,
        from_block: Option<u64>,
        to_block: Option<u64>,
    ) -> LedgerResult<Vec<AddressTxRef>>;
}
