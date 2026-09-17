use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::core::errors::LedgerResult;
use crate::domain::entities::{
    AddressAnalysisCandidate, AddressAnalysisJob, AddressCluster, AddressLabel,
    AddressLabelAssertion, AddressProfile, ClassifiedTransaction, ClusterAddress, ClusterAlert,
    ClusterBackfillRun, ClusterCoverage, ClusterDiscoveredTx, ClusterMetricsSnapshot,
    ClusterStrategyReportSnapshot, ClusterTrackingState, EvidencePack, ProtocolAddressMetadata,
    RecognizerResult, TransactionAnalysis,
};
use crate::domain::enums::BehaviorTag;
use crate::domain::value_objects::{
    AddressTxDiscovery, AddressTxRef, AssetFlow, ChainDataSourceStatus, ValidationMessage,
};
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
    /// Address intelligence must include a wallet that appears only inside a
    /// deterministic token/native flow, not only the outer transaction sender
    /// or recipient.
    async fn list_analyses_for_observed_addresses(
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
    async fn upsert_cluster_tracking_state(&self, state: &ClusterTrackingState)
    -> LedgerResult<()>;
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
    async fn list_classified_transactions_page(
        &self,
        cluster_id: uuid::Uuid,
        limit: u32,
        offset: u32,
        behavior_tag: Option<BehaviorTag>,
        protocol_query: Option<String>,
        sort_by: String,
        descending: bool,
    ) -> LedgerResult<(Vec<ClassifiedTransaction>, u64)>;
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
    async fn upsert_cluster_strategy_report_snapshot(
        &self,
        snapshot: &ClusterStrategyReportSnapshot,
    ) -> LedgerResult<()>;
    async fn get_cluster_strategy_report_snapshot(
        &self,
        cluster_id: uuid::Uuid,
        window_key: &str,
    ) -> LedgerResult<Option<ClusterStrategyReportSnapshot>>;
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
    async fn enqueue_address_analysis_candidates(
        &self,
        chain_id: u64,
        candidates: &[AddressAnalysisCandidate],
    ) -> LedgerResult<u64>;
    async fn claim_next_address_analysis_job(&self) -> LedgerResult<Option<AddressAnalysisJob>>;
    async fn claim_next_address_analysis_job_up_to(
        &self,
        maximum_priority: i32,
    ) -> LedgerResult<Option<AddressAnalysisJob>>;
    async fn claim_next_address_analysis_job_for_chain_up_to(
        &self,
        chain_id: u64,
        maximum_priority: i32,
    ) -> LedgerResult<Option<AddressAnalysisJob>>;
    async fn upsert_address_profile(&self, profile: &AddressProfile) -> LedgerResult<()>;
    async fn upsert_address_label_assertions(
        &self,
        assertions: &[AddressLabelAssertion],
    ) -> LedgerResult<()>;
    async fn upsert_protocol_address_metadata(
        &self,
        metadata: &[ProtocolAddressMetadata],
    ) -> LedgerResult<()>;
    async fn list_protocol_address_metadata(
        &self,
        chain_id: u64,
        addresses: &[String],
        block_number: Option<u64>,
    ) -> LedgerResult<Vec<ProtocolAddressMetadata>>;
    async fn complete_address_analysis_job(&self, chain_id: u64, address: &str)
    -> LedgerResult<()>;
    async fn fail_address_analysis_job(
        &self,
        chain_id: u64,
        address: &str,
        error: &str,
    ) -> LedgerResult<()>;
    async fn list_address_profiles(
        &self,
        chain_id: u64,
        addresses: &[String],
    ) -> LedgerResult<Vec<AddressProfile>>;
    async fn list_address_labels(
        &self,
        chain_id: u64,
        addresses: &[String],
    ) -> LedgerResult<Vec<AddressLabel>>;
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

    async fn get_data_source_status(&self, chain_id: u64) -> LedgerResult<ChainDataSourceStatus> {
        let indexed_to_block = self.get_last_indexed_block(chain_id).await?;
        Ok(ChainDataSourceStatus {
            chain_id,
            data_source: "canonical_index".to_owned(),
            indexed_from_block: None,
            indexed_to_block: Some(indexed_to_block),
            chain_head: None,
            ingestion_lag: None,
            last_updated_at: None,
            history_mode: "source_not_attested".to_owned(),
            full_history_available: false,
            available: true,
            limitation: Some(
                "The transaction source does not attest its indexed start, chain head, or freshness."
                    .to_owned(),
            ),
        })
    }

    async fn query_address_refs(
        &self,
        chain_id: u64,
        address: &str,
        limit: u32,
        from_block: Option<u64>,
        to_block: Option<u64>,
    ) -> LedgerResult<Vec<AddressTxRef>>;

    async fn query_address_refs_with_coverage(
        &self,
        chain_id: u64,
        address: &str,
        limit: u32,
        from_block: Option<u64>,
        to_block: Option<u64>,
    ) -> LedgerResult<AddressTxDiscovery> {
        Ok(AddressTxDiscovery {
            refs: self
                .query_address_refs(chain_id, address, limit, from_block, to_block)
                .await?,
            coverage_complete: None,
            indexed_block_count: None,
            requested_block_count: None,
        })
    }
}
