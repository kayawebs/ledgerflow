use std::collections::HashMap;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::core::errors::LedgerResult;
use crate::domain::entities::{AddressCluster, ClusterAddress, EvidencePack, TransactionAnalysis};
use crate::domain::interfaces::AnalysisRepository;
use crate::ingestion::models::RawTransactionBundle;

#[derive(Default)]
pub struct InMemoryAnalysisRepository {
    bundles: RwLock<HashMap<(u64, String), RawTransactionBundle>>,
    analyses: RwLock<HashMap<(u64, String), TransactionAnalysis>>,
    packs: RwLock<HashMap<String, EvidencePack>>,
    clusters: RwLock<HashMap<Uuid, AddressCluster>>,
}

#[async_trait]
impl AnalysisRepository for InMemoryAnalysisRepository {
    async fn save_transaction_bundle(&self, bundle: &RawTransactionBundle) -> LedgerResult<()> {
        self.bundles.write().await.insert(
            (bundle.tx.chain_id, bundle.tx.tx_hash.clone()),
            bundle.clone(),
        );
        Ok(())
    }

    async fn save_analysis(&self, analysis: &TransactionAnalysis) -> LedgerResult<()> {
        self.analyses.write().await.insert(
            (analysis.chain_id, analysis.tx_hash.clone()),
            analysis.clone(),
        );
        Ok(())
    }

    async fn save_evidence_pack(&self, pack: &EvidencePack) -> LedgerResult<()> {
        self.packs
            .write()
            .await
            .insert(pack.pack_id.to_string(), pack.clone());
        Ok(())
    }

    async fn get_analysis(
        &self,
        chain_id: u64,
        tx_hash: &str,
    ) -> LedgerResult<Option<TransactionAnalysis>> {
        Ok(self
            .analyses
            .read()
            .await
            .get(&(chain_id, tx_hash.to_owned()))
            .cloned())
    }

    async fn get_evidence_pack(&self, pack_id: &str) -> LedgerResult<Option<EvidencePack>> {
        Ok(self.packs.read().await.get(pack_id).cloned())
    }

    async fn get_evidence_pack_by_tx(
        &self,
        _chain_id: u64,
        tx_hash: &str,
    ) -> LedgerResult<Option<EvidencePack>> {
        Ok(self
            .packs
            .read()
            .await
            .values()
            .find(|pack| pack.tx_hash.as_deref() == Some(tx_hash))
            .cloned())
    }

    async fn create_cluster(&self, cluster: &AddressCluster) -> LedgerResult<()> {
        self.clusters
            .write()
            .await
            .insert(cluster.cluster_id, cluster.clone());
        Ok(())
    }

    async fn replace_cluster_addresses(
        &self,
        cluster_id: Uuid,
        addresses: &[ClusterAddress],
    ) -> LedgerResult<()> {
        if let Some(cluster) = self.clusters.write().await.get_mut(&cluster_id) {
            cluster.addresses = addresses.to_vec();
        }
        Ok(())
    }

    async fn get_cluster(&self, cluster_id: Uuid) -> LedgerResult<Option<AddressCluster>> {
        Ok(self.clusters.read().await.get(&cluster_id).cloned())
    }

    async fn list_analyses_for_cluster(
        &self,
        chain_id: u64,
        addresses: &[String],
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
    ) -> LedgerResult<Vec<TransactionAnalysis>> {
        let address_set = addresses
            .iter()
            .map(|item| item.to_lowercase())
            .collect::<Vec<_>>();
        let analyses = self
            .analyses
            .read()
            .await
            .values()
            .filter(|analysis| analysis.chain_id == chain_id)
            .filter(|analysis| {
                address_set.contains(&analysis.from_address.to_lowercase())
                    || analysis
                        .to_address
                        .as_ref()
                        .map(|item| address_set.contains(&item.to_lowercase()))
                        .unwrap_or(false)
            })
            .filter(|analysis| within_range(analysis.block_timestamp, start, end))
            .cloned()
            .collect::<Vec<_>>();
        Ok(analyses)
    }
}

fn within_range(
    timestamp: Option<DateTime<Utc>>,
    start: Option<DateTime<Utc>>,
    end: Option<DateTime<Utc>>,
) -> bool {
    let Some(timestamp) = timestamp else {
        return true;
    };
    if let Some(start) = start {
        if timestamp < start {
            return false;
        }
    }
    if let Some(end) = end {
        if timestamp > end {
            return false;
        }
    }
    true
}
