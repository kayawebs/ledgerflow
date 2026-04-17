use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::core::errors::LedgerResult;
use crate::domain::entities::{
    AddressCluster, ClusterAddress, EvidencePack, RecognizerResult, TransactionAnalysis,
};
use crate::domain::value_objects::{AssetFlow, ValidationMessage};
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
    async fn get_cluster(&self, cluster_id: uuid::Uuid) -> LedgerResult<Option<AddressCluster>>;
    async fn list_analyses_for_cluster(
        &self,
        chain_id: u64,
        addresses: &[String],
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
    ) -> LedgerResult<Vec<TransactionAnalysis>>;
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
