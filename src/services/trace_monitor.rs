use std::collections::{BTreeMap, BTreeSet};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use chrono::{DateTime, Duration, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::{Row, types::Json};
use tracing::{debug, warn};
use uuid::Uuid;

use crate::{
    core::errors::{LedgerError, LedgerResult},
    domain::{enums::AssetType, interfaces::AnalysisRepository, value_objects::AssetFlow},
    flows::derivation::derive_basic_flows,
    indexer::rpc::IndexerRpcClient,
    ingestion::rpc::HttpRpcClient,
    ingestion::{fetchers::TransactionBundleFetcher, models::RawTransactionBundle},
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
    pub candidate_rows: u64,
    pub skipped_blocks: u64,
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

#[derive(Clone, Debug)]
struct LightMonitorCandidate {
    kind: &'static str,
    score: i32,
    dex_swap_count: u32,
    liquidation_event_count: u32,
    metadata: Value,
}

#[derive(Clone)]
pub struct TraceMonitorService {
    repository: SqlxAnalysisRepository,
    rpc_client: Arc<IndexerRpcClient>,
    block_receipt_rpc: Arc<HttpRpcClient>,
    light_fetcher: TransactionBundleFetcher,
    config: TraceMonitorConfig,
    startup_anchor_initialized: Arc<AtomicBool>,
}

#[derive(Clone, Debug)]
struct BlockTxRef {
    tx_hash: String,
    tx_payload: Option<Value>,
    receipt_payload: Option<Value>,
    block_timestamp: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct TopAddressDeltasQuery {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub top_n: Option<u32>,
}

#[derive(Clone, Debug, Serialize)]
pub struct TopAddressDelta {
    pub address: String,
    pub asset_type: String,
    pub token_address: String,
    pub net_raw: String,
    pub amount_in_raw: String,
    pub amount_out_raw: String,
    pub tx_count: u64,
    pub tx_hashes: Vec<String>,
}

impl TraceMonitorService {
    pub fn new(
        repository: SqlxAnalysisRepository,
        rpc_client: Arc<IndexerRpcClient>,
        block_receipt_rpc: Arc<HttpRpcClient>,
        light_fetcher: TransactionBundleFetcher,
        config: TraceMonitorConfig,
    ) -> Self {
        Self {
            repository,
            rpc_client,
            block_receipt_rpc,
            light_fetcher,
            config,
            startup_anchor_initialized: Arc::new(AtomicBool::new(false)),
        }
    }

    pub async fn process_once(&self) -> LedgerResult<TraceMonitorIteration> {
        let latest_block = self.rpc_client.get_latest_block_number().await?;
        let target_block = latest_block.saturating_sub(self.config.confirmations);
        let skip_history_on_startup = !self.startup_anchor_initialized.swap(true, Ordering::SeqCst);
        let last_processed = if skip_history_on_startup {
            None
        } else {
            self.load_last_processed_block().await?
        };
        let (start_block, skipped_blocks) = next_start_block(
            skip_history_on_startup,
            last_processed,
            self.config.start_block,
            target_block,
            self.config.block_batch_size.max(1),
        );

        let mut iteration = TraceMonitorIteration {
            latest_block,
            target_block,
            start_block: Some(start_block),
            skipped_blocks,
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
            let tx_refs = self.block_tx_refs(block_number).await?;
            for tx_ref in tx_refs {
                let tx_hash = tx_ref.tx_hash.clone();
                match self.process_light_tx(tx_ref, &clusters).await {
                    Ok((address_rows, cluster_rows, candidate_rows)) => {
                        iteration.processed_txs += 1;
                        iteration.address_delta_rows += address_rows;
                        iteration.cluster_delta_rows += cluster_rows;
                        iteration.candidate_rows += candidate_rows;
                    }
                    Err(err) => {
                        iteration.failed_txs += 1;
                        warn!(
                            tx_hash,
                            error = %err,
                            "trace monitor light tx failed"
                        );
                    }
                }
            }
            self.upsert_progress(Some(block_number)).await?;
            iteration.processed_blocks += 1;
        }

        self.cleanup_expired_rows().await?;
        Ok(iteration)
    }

    async fn process_light_tx(
        &self,
        tx_ref: BlockTxRef,
        clusters: &[ClusterScope],
    ) -> LedgerResult<(u64, u64, u64)> {
        let bundle = match (tx_ref.tx_payload, tx_ref.receipt_payload) {
            (Some(tx_payload), Some(receipt_payload)) => {
                self.light_fetcher
                    .fetch_without_code_from_payloads_with_timestamp(
                        self.config.chain_id,
                        tx_payload,
                        receipt_payload,
                        tx_ref.block_timestamp,
                    )
                    .await?
            }
            (Some(tx_payload), None) => {
                self.light_fetcher
                    .fetch_without_code_from_tx_payload_with_timestamp(
                        self.config.chain_id,
                        tx_payload,
                        tx_ref.block_timestamp,
                    )
                    .await?
            }
            (None, _) => {
                self.light_fetcher
                    .fetch_without_code_with_timestamp(
                        self.config.chain_id,
                        &tx_ref.tx_hash,
                        tx_ref.block_timestamp,
                    )
                    .await?
            }
        };
        let flows = derive_basic_flows(&bundle);
        let candidates = detect_light_candidates(&bundle, &flows);
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
        let candidate_rows = self.upsert_candidates(&bundle, &candidates).await?;
        Ok((address_rows, cluster_rows, candidate_rows))
    }

    pub async fn top_address_deltas(
        &self,
        query: &TopAddressDeltasQuery,
    ) -> LedgerResult<Vec<TopAddressDelta>> {
        let top_n = query.top_n.unwrap_or(25).clamp(1, 200);
        let rows = sqlx::query(
            r#"
            with top_addresses as (
              select
                address,
                asset_type,
                token_address,
                sum(net_raw)::text as net_raw,
                sum(amount_in_raw)::text as amount_in_raw,
                sum(amount_out_raw)::text as amount_out_raw,
                count(distinct tx_hash)::bigint as tx_count
              from trace_address_asset_deltas
              where chain_id = $1
                and block_timestamp >= $2
                and block_timestamp <= $3
                and net_raw > 0
              group by address, asset_type, token_address
              order by sum(net_raw) desc
              limit $4
            )
            select
              top_addresses.address,
              top_addresses.asset_type,
              top_addresses.token_address,
              top_addresses.net_raw,
              top_addresses.amount_in_raw,
              top_addresses.amount_out_raw,
              top_addresses.tx_count,
              array(
                select d.tx_hash
                from trace_address_asset_deltas d
                where d.chain_id = $1
                  and d.block_timestamp >= $2
                  and d.block_timestamp <= $3
                  and d.address = top_addresses.address
                  and d.asset_type = top_addresses.asset_type
                  and d.token_address = top_addresses.token_address
                  and d.net_raw > 0
                order by d.net_raw desc, d.block_number desc
                limit 50
              ) as tx_hashes
            from top_addresses
            "#,
        )
        .bind(self.config.chain_id as i64)
        .bind(query.start)
        .bind(query.end)
        .bind(top_n as i64)
        .fetch_all(self.repository.pool())
        .await
        .map_err(|err| LedgerError::Persistence(err.to_string()))?;

        rows.into_iter()
            .map(|row| {
                Ok(TopAddressDelta {
                    address: row
                        .try_get("address")
                        .map_err(|err| LedgerError::Persistence(err.to_string()))?,
                    asset_type: row
                        .try_get("asset_type")
                        .map_err(|err| LedgerError::Persistence(err.to_string()))?,
                    token_address: row
                        .try_get("token_address")
                        .map_err(|err| LedgerError::Persistence(err.to_string()))?,
                    net_raw: row
                        .try_get("net_raw")
                        .map_err(|err| LedgerError::Persistence(err.to_string()))?,
                    amount_in_raw: row
                        .try_get("amount_in_raw")
                        .map_err(|err| LedgerError::Persistence(err.to_string()))?,
                    amount_out_raw: row
                        .try_get("amount_out_raw")
                        .map_err(|err| LedgerError::Persistence(err.to_string()))?,
                    tx_count: row
                        .try_get::<i64, _>("tx_count")
                        .map(|value| value as u64)
                        .map_err(|err| LedgerError::Persistence(err.to_string()))?,
                    tx_hashes: row
                        .try_get::<Vec<String>, _>("tx_hashes")
                        .map_err(|err| LedgerError::Persistence(err.to_string()))?,
                })
            })
            .collect()
    }

    async fn block_tx_refs(&self, block_number: u64) -> LedgerResult<Vec<BlockTxRef>> {
        let block = self
            .rpc_client
            .get_block_with_transactions(block_number)
            .await?;
        let block_timestamp = block
            .get("timestamp")
            .and_then(Value::as_str)
            .and_then(|value| u64::from_str_radix(value.trim_start_matches("0x"), 16).ok())
            .and_then(|value| Utc.timestamp_opt(value as i64, 0).single());
        let receipts_by_hash = match self
            .block_receipt_rpc
            .get_block_receipts(block_number)
            .await
        {
            Ok(receipts) => receipts
                .into_iter()
                .filter_map(|receipt| {
                    let tx_hash = receipt
                        .get("transactionHash")
                        .and_then(Value::as_str)?
                        .to_ascii_lowercase();
                    Some((tx_hash, receipt))
                })
                .collect::<BTreeMap<_, _>>(),
            Err(err) => {
                debug!(
                    block_number,
                    error = %err,
                    "block receipts unavailable; falling back to per-transaction receipts"
                );
                BTreeMap::new()
            }
        };
        Ok(block
            .get("transactions")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(|item| {
                        let tx_hash = item.as_str().map(ToOwned::to_owned).or_else(|| {
                            item.get("hash")
                                .and_then(Value::as_str)
                                .map(ToOwned::to_owned)
                        })?;
                        let tx_payload = item.as_object().map(|_| item.clone());
                        let receipt_payload =
                            receipts_by_hash.get(&tx_hash.to_ascii_lowercase()).cloned();
                        Some(BlockTxRef {
                            tx_hash,
                            tx_payload,
                            receipt_payload,
                            block_timestamp,
                        })
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

    async fn upsert_candidates(
        &self,
        bundle: &RawTransactionBundle,
        candidates: &[LightMonitorCandidate],
    ) -> LedgerResult<u64> {
        let mut rows = 0;
        for candidate in candidates {
            sqlx::query(
                r#"
                insert into trace_monitor_candidates (
                  chain_id, tx_hash, candidate_kind, block_number, block_timestamp,
                  score, dex_swap_count, liquidation_event_count, metadata, created_at, updated_at
                )
                values ($1,$2,$3,$4,$5,$6,$7,$8,$9,now(),now())
                on conflict (chain_id, tx_hash, candidate_kind) do update set
                  block_number = excluded.block_number,
                  block_timestamp = excluded.block_timestamp,
                  score = greatest(trace_monitor_candidates.score, excluded.score),
                  dex_swap_count = excluded.dex_swap_count,
                  liquidation_event_count = excluded.liquidation_event_count,
                  metadata = excluded.metadata,
                  updated_at = now()
                "#,
            )
            .bind(self.config.chain_id as i64)
            .bind(&bundle.tx.tx_hash)
            .bind(candidate.kind)
            .bind(bundle.tx.block_number.unwrap_or_default() as i64)
            .bind(bundle.tx.block_timestamp)
            .bind(candidate.score)
            .bind(candidate.dex_swap_count as i32)
            .bind(candidate.liquidation_event_count as i32)
            .bind(Json(candidate.metadata.clone()))
            .execute(self.repository.pool())
            .await
            .map_err(|err| LedgerError::Persistence(err.to_string()))?;
            rows += 1;
        }
        Ok(rows)
    }

    async fn cleanup_expired_rows(&self) -> LedgerResult<()> {
        let cutoff = Utc::now() - Duration::days(self.config.retention_days.max(1));
        for table in [
            "trace_address_asset_deltas",
            "trace_cluster_asset_deltas",
            "trace_monitor_candidates",
        ] {
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
        let from_delta = deltas
            .entry(from_key)
            .or_insert_with(DeltaAccumulator::default);
        from_delta.amount_out = from_delta.amount_out.saturating_add(amount);
        from_delta.flow_count += 1;
        let to_delta = deltas
            .entry(to_key)
            .or_insert_with(DeltaAccumulator::default);
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

fn detect_light_candidates(
    bundle: &RawTransactionBundle,
    flows: &[AssetFlow],
) -> Vec<LightMonitorCandidate> {
    let dex_swap_count = bundle
        .receipt
        .logs
        .iter()
        .filter(|log| {
            log.topics
                .first()
                .map(|topic| is_dex_swap_topic(topic))
                .unwrap_or(false)
        })
        .count() as u32;
    let liquidation_event_count = bundle
        .receipt
        .logs
        .iter()
        .filter(|log| {
            log.topics
                .first()
                .map(|topic| is_liquidation_topic(topic))
                .unwrap_or(false)
        })
        .count() as u32;
    let transfer_token_count = flows
        .iter()
        .filter_map(|flow| flow.token_address.as_deref())
        .map(str::to_ascii_lowercase)
        .collect::<BTreeSet<_>>()
        .len();
    let mut candidates = Vec::new();
    if liquidation_event_count > 0 {
        candidates.push(LightMonitorCandidate {
            kind: "liquidation_event",
            score: 100,
            dex_swap_count,
            liquidation_event_count,
            metadata: json!({
                "detection": "receipt_topic",
                "transfer_token_count": transfer_token_count,
            }),
        });
    }
    if dex_swap_count >= 2 {
        candidates.push(LightMonitorCandidate {
            kind: "dex_multi_swap",
            score: 70 + i32::try_from(dex_swap_count.min(10)).unwrap_or_default(),
            dex_swap_count,
            liquidation_event_count,
            metadata: json!({
                "detection": "receipt_topic",
                "transfer_token_count": transfer_token_count,
                "candidate_reason": "multiple dex swap events in one transaction",
            }),
        });
    }
    candidates
}

fn is_dex_swap_topic(topic: &str) -> bool {
    matches!(
        topic.to_ascii_lowercase().as_str(),
        "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"
            | "0xc42079f94a6350d7e6235f291749249f928cc2ac818eb64b52398e603a73a2f"
    )
}

fn is_liquidation_topic(topic: &str) -> bool {
    matches!(
        topic.to_ascii_lowercase().as_str(),
        "0xa4946ede45d0c6f06a0f5ce92c9ad3b4751452d2fe0e25010783bcab57a67e41"
            | "0xe413a321e8681d83188be0d2364605a8f594ac97918daee13129e6b4f89b8f3"
    )
}

fn next_start_block(
    skip_history_on_startup: bool,
    last_processed: Option<u64>,
    configured_start_block: Option<u64>,
    target_block: u64,
    block_batch_size: u64,
) -> (u64, u64) {
    if skip_history_on_startup {
        return (target_block, 0);
    }
    let requested_start = last_processed
        .map(|block| block.saturating_add(1))
        .or(configured_start_block)
        .unwrap_or(target_block);
    let live_window_start = target_block
        .saturating_sub(block_batch_size.max(1))
        .saturating_add(1);
    if requested_start < live_window_start {
        (
            live_window_start,
            live_window_start.saturating_sub(requested_start),
        )
    } else {
        (requested_start, 0)
    }
}

#[cfg(test)]
mod tests {
    use crate::domain::value_objects::EvidenceRef;
    use crate::ingestion::models::{RawReceipt, RawTransaction};

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
            addresses: ["0xaaa".to_owned(), "0xbbb".to_owned()]
                .into_iter()
                .collect(),
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

    #[test]
    fn startup_start_block_ignores_persisted_history_once() {
        assert_eq!(
            next_start_block(true, Some(100), Some(200), 500, 30),
            (500, 0)
        );
        assert_eq!(
            next_start_block(false, Some(100), Some(200), 500, 30),
            (471, 370)
        );
        assert_eq!(next_start_block(false, None, Some(490), 500, 30), (490, 0));
        assert_eq!(next_start_block(false, None, None, 500, 30), (500, 0));
    }

    #[test]
    fn receipt_detector_keeps_liquidations_and_multi_swap_routes_only() {
        let bundle = RawTransactionBundle {
            tx: RawTransaction {
                chain_id: 8453,
                tx_hash: "0xcandidate".to_owned(),
                block_number: Some(10),
                block_timestamp: None,
                from_address: "0xaaa".to_owned(),
                to_address: Some("0xbbb".to_owned()),
                value: "0x0".to_owned(),
                input_data: "0x".to_owned(),
                nonce: None,
                tx_index: None,
            },
            receipt: RawReceipt {
                tx_hash: "0xcandidate".to_owned(),
                status: 1,
                gas_used: None,
                effective_gas_price: None,
                logs: vec![
                    crate::ingestion::models::RawLog {
                        log_index: 1,
                        address: "0xpool1".to_owned(),
                        topics: vec![
                            "0xc42079f94a6350d7e6235f291749249f928cc2ac818eb64b52398e603a73a2f"
                                .to_owned(),
                        ],
                        data: "0x".to_owned(),
                    },
                    crate::ingestion::models::RawLog {
                        log_index: 2,
                        address: "0xpool2".to_owned(),
                        topics: vec![
                            "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"
                                .to_owned(),
                        ],
                        data: "0x".to_owned(),
                    },
                    crate::ingestion::models::RawLog {
                        log_index: 3,
                        address: "0xprotocol".to_owned(),
                        topics: vec![
                            "0xa4946ede45d0c6f06a0f5ce92c9ad3b4751452d2fe0e25010783bcab57a67e41"
                                .to_owned(),
                        ],
                        data: "0x".to_owned(),
                    },
                ],
            },
            trace: None,
            target_code: None,
            snapshots: vec![],
            metadata: json!({}),
        };

        let candidates = detect_light_candidates(&bundle, &[]);
        assert!(
            candidates
                .iter()
                .any(|item| item.kind == "liquidation_event")
        );
        assert!(candidates.iter().any(|item| item.kind == "dex_multi_swap"));
    }
}
