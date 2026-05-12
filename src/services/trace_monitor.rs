use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use chrono::{Duration, Utc};
use serde_json::{Value, json};
use sqlx::{Row, types::Json};
use tracing::{debug, warn};
use uuid::Uuid;

use crate::{
    core::errors::{LedgerError, LedgerResult},
    domain::{enums::AssetType, interfaces::AnalysisRepository, value_objects::AssetFlow},
    flows::derivation::derive_basic_flows,
    indexer::rpc::IndexerRpcClient,
    ingestion::fetchers::TransactionBundleFetcher,
    repositories::sqlx::SqlxAnalysisRepository,
};

#[derive(Clone, Debug)]
pub struct TraceMonitorConfig {
    pub chain_id: u64,
    pub worker_id: String,
    pub start_block: Option<u64>,
    pub confirmations: u64,
    pub block_batch_size: u64,
    pub retention_days: i64,
}

#[derive(Clone, Debug, Default)]
pub struct TraceMonitorIteration {
    pub latest_block: u64,
    pub target_block: u64,
    pub start_block: Option<u64>,
    pub end_block: Option<u64>,
    pub processed_blocks: u64,
    pub processed_txs: u64,
    pub failed_txs: u64,
    pub address_delta_rows: u64,
    pub cluster_delta_rows: u64,
}

#[derive(Clone, Debug, Default)]
struct DeltaAccumulator {
    amount_in: i128,
    amount_out: i128,
    flow_count: u32,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct AddressDeltaKey {
    address: String,
    asset_type: String,
    token_address: String,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ClusterDeltaKey {
    cluster_id: Uuid,
    asset_type: String,
    token_address: String,
}

pub struct TraceMonitorService {
    repository: SqlxAnalysisRepository,
    rpc_client: Arc<IndexerRpcClient>,
    fetcher: TransactionBundleFetcher,
    config: TraceMonitorConfig,
}

impl TraceMonitorService {
    pub fn new(
        repository: SqlxAnalysisRepository,
        rpc_client: Arc<IndexerRpcClient>,
        fetcher: TransactionBundleFetcher,
        config: TraceMonitorConfig,
    ) -> Self {
        Self {
            repository,
            rpc_client,
            fetcher,
            config,
        }
    }

    pub async fn process_once(&self) -> LedgerResult<TraceMonitorIteration> {
        let latest_block = self.rpc_client.get_latest_block_number().await?;
        let target_block = latest_block.saturating_sub(self.config.confirmations);
        let last_processed = self.load_last_processed_block().await?;
        let start_block = last_processed
            .map(|block| block.saturating_add(1))
            .or(self.config.start_block)
            .unwrap_or(target_block);

        let mut iteration = TraceMonitorIteration {
            latest_block,
            target_block,
            start_block: Some(start_block),
            ..Default::default()
        };

        if start_block > target_block {
            iteration.start_block = None;
            self.cleanup_expired_rows().await?;
            return Ok(iteration);
        }

        let end_block = target_block.min(
            start_block
                .saturating_add(self.config.block_batch_size.max(1))
                .saturating_sub(1),
        );
        iteration.end_block = Some(end_block);

        let clusters = self.load_cluster_scopes().await?;
        for block_number in start_block..=end_block {
            let tx_hashes = self.block_tx_hashes(block_number).await?;
            for tx_hash in tx_hashes {
                match self.process_tx(&tx_hash, &clusters).await {
                    Ok((address_rows, cluster_rows)) => {
                        iteration.processed_txs += 1;
                        iteration.address_delta_rows += address_rows;
                        iteration.cluster_delta_rows += cluster_rows;
                    }
                    Err(err) => {
                        iteration.failed_txs += 1;
                        warn!(tx_hash, error = %err, "trace monitor tx failed");
                    }
                }
            }
            self.upsert_progress(Some(block_number)).await?;
            iteration.processed_blocks += 1;
        }

        self.cleanup_expired_rows().await?;
        Ok(iteration)
    }

    async fn process_tx(
        &self,
        tx_hash: &str,
        clusters: &[ClusterScope],
    ) -> LedgerResult<(u64, u64)> {
        let bundle = self
            .fetcher
            .fetch_without_code(self.config.chain_id, tx_hash)
            .await?;
        let flows = derive_basic_flows(&bundle);
        let address_deltas = aggregate_address_deltas(&flows);
        let address_rows = self
            .upsert_address_deltas(
                &bundle.tx.tx_hash,
                bundle.tx.block_number.unwrap_or_default(),
                bundle.tx.block_timestamp,
                &bundle.tx.from_address,
                bundle.tx.to_address.as_deref(),
                bundle.trace.is_some(),
                &address_deltas,
            )
            .await?;

        let cluster_deltas = aggregate_cluster_deltas(&flows, clusters);
        let cluster_rows = self
            .upsert_cluster_deltas(
                &bundle.tx.tx_hash,
                bundle.tx.block_number.unwrap_or_default(),
                bundle.tx.block_timestamp,
                bundle.trace.is_some(),
                &cluster_deltas,
            )
            .await?;
        Ok((address_rows, cluster_rows))
    }

    async fn block_tx_hashes(&self, block_number: u64) -> LedgerResult<Vec<String>> {
        let block = self.rpc_client.get_block_with_transactions(block_number).await?;
        Ok(block
            .get("transactions")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(|item| {
                        item.as_str()
                            .map(ToOwned::to_owned)
                            .or_else(|| item.get("hash").and_then(Value::as_str).map(ToOwned::to_owned))
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default())
    }

    async fn load_last_processed_block(&self) -> LedgerResult<Option<u64>> {
        let row = sqlx::query(
            "select last_processed_block from trace_monitor_progress where worker_id = $1",
        )
        .bind(&self.config.worker_id)
        .fetch_optional(self.repository.pool())
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        row.map(|row| {
            row.try_get::<Option<i64>, _>("last_processed_block")
                .map(|value| value.map(|block| block as u64))
                .map_err(|err| LedgerError::Persistence(err.to_string()))
        })
        .transpose()
        .map(Option::flatten)
    }

    async fn upsert_progress(&self, block_number: Option<u64>) -> LedgerResult<()> {
        sqlx::query(
            r#"
            insert into trace_monitor_progress (
              worker_id, chain_id, last_processed_block, last_processed_at, metadata, created_at, updated_at
            )
            values ($1, $2, $3, now(), $4, now(), now())
            on conflict (worker_id) do update set
              chain_id = excluded.chain_id,
              last_processed_block = excluded.last_processed_block,
              last_processed_at = excluded.last_processed_at,
              metadata = excluded.metadata,
              updated_at = now()
            "#,
        )
        .bind(&self.config.worker_id)
        .bind(self.config.chain_id as i64)
        .bind(block_number.map(|value| value as i64))
        .bind(Json(json!({
            "confirmations": self.config.confirmations,
            "block_batch_size": self.config.block_batch_size,
            "retention_days": self.config.retention_days,
        })))
        .execute(self.repository.pool())
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        Ok(())
    }

    async fn load_cluster_scopes(&self) -> LedgerResult<Vec<ClusterScope>> {
        let clusters = self.repository.list_clusters().await?;
        Ok(clusters
            .into_iter()
            .filter(|cluster| cluster.chain_id == self.config.chain_id)
            .map(|cluster| ClusterScope {
                cluster_id: cluster.cluster_id,
                addresses: cluster
                    .addresses
                    .into_iter()
                    .map(|address| address.address.to_ascii_lowercase())
                    .collect(),
            })
            .collect())
    }

    async fn upsert_address_deltas(
        &self,
        tx_hash: &str,
        block_number: u64,
        block_timestamp: Option<chrono::DateTime<Utc>>,
        tx_from: &str,
        tx_to: Option<&str>,
        trace_available: bool,
        deltas: &BTreeMap<AddressDeltaKey, DeltaAccumulator>,
    ) -> LedgerResult<u64> {
        let mut rows = 0;
        for (key, delta) in deltas {
            let net = delta.amount_in.saturating_sub(delta.amount_out);
            sqlx::query(
                r#"
                insert into trace_address_asset_deltas (
                  chain_id, tx_hash, block_number, block_timestamp, address, asset_type, token_address,
                  amount_in_raw, amount_out_raw, net_raw, flow_count, metadata, created_at, updated_at
                )
                values ($1,$2,$3,$4,$5,$6,$7,$8::numeric,$9::numeric,$10::numeric,$11,$12,now(),now())
                on conflict (chain_id, tx_hash, address, asset_type, token_address) do update set
                  block_number = excluded.block_number,
                  block_timestamp = excluded.block_timestamp,
                  amount_in_raw = excluded.amount_in_raw,
                  amount_out_raw = excluded.amount_out_raw,
                  net_raw = excluded.net_raw,
                  flow_count = excluded.flow_count,
                  metadata = excluded.metadata,
                  updated_at = now()
                "#,
            )
            .bind(self.config.chain_id as i64)
            .bind(tx_hash)
            .bind(block_number as i64)
            .bind(block_timestamp)
            .bind(&key.address)
            .bind(&key.asset_type)
            .bind(&key.token_address)
            .bind(delta.amount_in.to_string())
            .bind(delta.amount_out.to_string())
            .bind(net.to_string())
            .bind(delta.flow_count as i32)
            .bind(Json(json!({
                "tx_from": tx_from.to_ascii_lowercase(),
                "tx_to": tx_to.map(str::to_ascii_lowercase),
                "trace_available": trace_available,
            })))
            .execute(self.repository.pool())
            .await
            .map_err(|err| LedgerError::Persistence(err.to_string()))?;
            rows += 1;
        }
        Ok(rows)
    }

    async fn upsert_cluster_deltas(
        &self,
        tx_hash: &str,
        block_number: u64,
        block_timestamp: Option<chrono::DateTime<Utc>>,
        trace_available: bool,
        deltas: &BTreeMap<ClusterDeltaKey, ClusterDeltaAccumulator>,
    ) -> LedgerResult<u64> {
        let mut rows = 0;
        for (key, delta) in deltas {
            let net = delta.amount_in.saturating_sub(delta.amount_out);
            if delta.amount_in == 0 && delta.amount_out == 0 {
                continue;
            }
            sqlx::query(
                r#"
                insert into trace_cluster_asset_deltas (
                  chain_id, cluster_id, tx_hash, block_number, block_timestamp, asset_type, token_address,
                  amount_in_raw, amount_out_raw, net_raw, contributing_address_count, metadata, created_at, updated_at
                )
                values ($1,$2,$3,$4,$5,$6,$7,$8::numeric,$9::numeric,$10::numeric,$11,$12,now(),now())
                on conflict (chain_id, cluster_id, tx_hash, asset_type, token_address) do update set
                  block_number = excluded.block_number,
                  block_timestamp = excluded.block_timestamp,
                  amount_in_raw = excluded.amount_in_raw,
                  amount_out_raw = excluded.amount_out_raw,
                  net_raw = excluded.net_raw,
                  contributing_address_count = excluded.contributing_address_count,
                  metadata = excluded.metadata,
                  updated_at = now()
                "#,
            )
            .bind(self.config.chain_id as i64)
            .bind(key.cluster_id)
            .bind(tx_hash)
            .bind(block_number as i64)
            .bind(block_timestamp)
            .bind(&key.asset_type)
            .bind(&key.token_address)
            .bind(delta.amount_in.to_string())
            .bind(delta.amount_out.to_string())
            .bind(net.to_string())
            .bind(delta.addresses.len() as i32)
            .bind(Json(json!({
                "trace_available": trace_available,
                "addresses": delta.addresses.iter().cloned().collect::<Vec<_>>(),
            })))
            .execute(self.repository.pool())
            .await
            .map_err(|err| LedgerError::Persistence(err.to_string()))?;
            rows += 1;
        }
        Ok(rows)
    }

    async fn cleanup_expired_rows(&self) -> LedgerResult<()> {
        let cutoff = Utc::now() - Duration::days(self.config.retention_days.max(1));
        for table in ["trace_address_asset_deltas", "trace_cluster_asset_deltas"] {
            let deleted = sqlx::query(&format!(
                "delete from {table} where block_timestamp is not null and block_timestamp < $1"
            ))
            .bind(cutoff)
            .execute(self.repository.pool())
            .await
            .map_err(|err| LedgerError::Persistence(err.to_string()))?
            .rows_affected();
            if deleted > 0 {
                debug!(table, deleted, "trace monitor cleanup removed old rows");
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct ClusterScope {
    cluster_id: Uuid,
    addresses: BTreeSet<String>,
}

#[derive(Clone, Debug, Default)]
struct ClusterDeltaAccumulator {
    amount_in: i128,
    amount_out: i128,
    addresses: BTreeSet<String>,
}

fn aggregate_address_deltas(flows: &[AssetFlow]) -> BTreeMap<AddressDeltaKey, DeltaAccumulator> {
    let mut deltas = BTreeMap::new();
    for flow in flows {
        let amount = parse_amount_i128(&flow.amount_raw);
        if amount == 0 {
            continue;
        }
        let asset_type = asset_type_key(&flow.asset_type);
        let token_address = flow
            .token_address
            .clone()
            .unwrap_or_else(|| "native".to_owned())
            .to_ascii_lowercase();
        let from_key = AddressDeltaKey {
            address: flow.from_address.to_ascii_lowercase(),
            asset_type: asset_type.clone(),
            token_address: token_address.clone(),
        };
        let to_key = AddressDeltaKey {
            address: flow.to_address.to_ascii_lowercase(),
            asset_type,
            token_address,
        };
        let from_delta = deltas.entry(from_key).or_insert_with(DeltaAccumulator::default);
        from_delta.amount_out = from_delta.amount_out.saturating_add(amount);
        from_delta.flow_count += 1;
        let to_delta = deltas.entry(to_key).or_insert_with(DeltaAccumulator::default);
        to_delta.amount_in = to_delta.amount_in.saturating_add(amount);
        to_delta.flow_count += 1;
    }
    deltas
}

fn aggregate_cluster_deltas(
    flows: &[AssetFlow],
    clusters: &[ClusterScope],
) -> BTreeMap<ClusterDeltaKey, ClusterDeltaAccumulator> {
    let mut deltas = BTreeMap::new();
    for flow in flows {
        let amount = parse_amount_i128(&flow.amount_raw);
        if amount == 0 {
            continue;
        }
        let from = flow.from_address.to_ascii_lowercase();
        let to = flow.to_address.to_ascii_lowercase();
        for cluster in clusters {
            let from_in_scope = cluster.addresses.contains(&from);
            let to_in_scope = cluster.addresses.contains(&to);
            if from_in_scope == to_in_scope {
                continue;
            }
            let key = ClusterDeltaKey {
                cluster_id: cluster.cluster_id,
                asset_type: asset_type_key(&flow.asset_type),
                token_address: flow
                    .token_address
                    .clone()
                    .unwrap_or_else(|| "native".to_owned())
                    .to_ascii_lowercase(),
            };
            let delta = deltas
                .entry(key)
                .or_insert_with(ClusterDeltaAccumulator::default);
            if to_in_scope {
                delta.amount_in = delta.amount_in.saturating_add(amount);
                delta.addresses.insert(to.clone());
            }
            if from_in_scope {
                delta.amount_out = delta.amount_out.saturating_add(amount);
                delta.addresses.insert(from.clone());
            }
        }
    }
    deltas
}

fn asset_type_key(value: &AssetType) -> String {
    match value {
        AssetType::Native => "native",
        AssetType::Erc20 => "erc20",
    }
    .to_owned()
}

fn parse_amount_i128(value: &str) -> i128 {
    value.parse::<i128>().unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use crate::domain::value_objects::EvidenceRef;

    use super::*;

    fn flow(from: &str, to: &str, token: Option<&str>, amount: &str) -> AssetFlow {
        AssetFlow {
            flow_id: "flow".to_owned(),
            asset_type: if token.is_some() {
                AssetType::Erc20
            } else {
                AssetType::Native
            },
            token_address: token.map(ToOwned::to_owned),
            from_address: from.to_owned(),
            to_address: to.to_owned(),
            amount_raw: amount.to_owned(),
            direction_hint: None,
            evidence_refs: Vec::<EvidenceRef>::new(),
            derivation_method: "test".to_owned(),
            confidence: 1.0,
            metadata: json!({}),
        }
    }

    #[test]
    fn cluster_delta_ignores_internal_transfers_and_keeps_boundary_flows() {
        let cluster_id = Uuid::new_v4();
        let cluster = ClusterScope {
            cluster_id,
            addresses: ["0xaaa".to_owned(), "0xbbb".to_owned()].into_iter().collect(),
        };
        let flows = vec![
            flow("0xaaa", "0xbbb", Some("0xtoken"), "10"),
            flow("0xccc", "0xaaa", Some("0xtoken"), "25"),
            flow("0xbbb", "0xddd", Some("0xtoken"), "7"),
        ];

        let deltas = aggregate_cluster_deltas(&flows, &[cluster]);
        let delta = deltas.values().next().expect("cluster delta");
        assert_eq!(delta.amount_in, 25);
        assert_eq!(delta.amount_out, 7);
        assert_eq!(delta.addresses.len(), 2);
    }
}
