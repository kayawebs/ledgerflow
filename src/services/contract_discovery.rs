use std::{collections::BTreeSet, sync::Arc};

use chrono::{TimeZone, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row, types::Json};
use tokio::{sync::Semaphore, task::JoinSet};
use tracing::info;

use crate::{
    core::errors::{LedgerError, LedgerResult},
    domain::{entities::AddressAnalysisCandidate, interfaces::AnalysisRepository},
    indexer::{clickhouse::ClickHouseClient, models::RawLog},
    ingestion::rpc::RpcClient,
    repositories::sqlx::SqlxAnalysisRepository,
    services::intelligence_import::{
        DatasetAddress, DatasetLabel, DatasetProtocolDeployment, DatasetSource,
        IntelligenceDatasetManifest, dataset_checksum, import_intelligence_dataset,
    },
};

const UI_MULTIPLIER_UPDATED_TOPIC: &str =
    "0x2205df4534432b2f60654a3fdb48737ffdaf3e9edb1a498bd985bc026b15b055";
const TRANSFER_WITH_UI_AMOUNT_TOPIC: &str =
    "0x0226a2f5c1ae0e071aeec3d4ebafcefdc5c549be11f40ed27e76e802acccf374";
const V2_PAIR_CREATED_TOPIC: &str =
    "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9";
const V3_POOL_CREATED_TOPIC: &str =
    "0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118";
const SOLIDLY_PAIR_CREATED_TOPIC: &str =
    "0xc4805696c66d7cf352fc1d6bb633ad5ee82f6cb577c453024b6e0eb8306c6fc9";
const SOLIDLY_POOL_CREATED_TOPIC: &str =
    "0x2128d88d14c80cb081c1252a5acff7a264671bf199ce226b53788fb26065005e";
const EVENT_TOPICS: [&str; 6] = [
    UI_MULTIPLIER_UPDATED_TOPIC,
    TRANSFER_WITH_UI_AMOUNT_TOPIC,
    V2_PAIR_CREATED_TOPIC,
    V3_POOL_CREATED_TOPIC,
    SOLIDLY_PAIR_CREATED_TOPIC,
    SOLIDLY_POOL_CREATED_TOPIC,
];

const TOKEN0_SELECTOR: &str = "0x0dfe1681";
const TOKEN1_SELECTOR: &str = "0xd21220a7";
const FACTORY_SELECTOR: &str = "0xc45a0155";
const GET_RESERVES_SELECTOR: &str = "0x0902f1ac";
const FEE_SELECTOR: &str = "0xddca3f43";
const TICK_SPACING_SELECTOR: &str = "0xd0c93a7c";
const SLOT0_SELECTOR: &str = "0x3850c7bd";
const STABLE_SELECTOR: &str = "0x22be3de1";
const GET_PAIR_SELECTOR: &str = "0xe6a43905";

const CONTRACT_DISCOVERY_SOURCE: &str = "onchain_contract_discovery";
const STOCK_EVENT_SOURCE: &str = "robinhood_stock_token_events";

#[derive(Clone, Debug)]
pub struct ContractDiscoveryConfig {
    pub chain_id: u32,
    pub worker_id: String,
    pub batch_size: u32,
    pub lookback_blocks: u32,
    pub rpc_concurrency: usize,
}

#[derive(Clone)]
pub struct ContractDiscoveryService {
    repository: SqlxAnalysisRepository,
    pool: PgPool,
    clickhouse: ClickHouseClient,
    rpc_client: Arc<dyn RpcClient>,
    config: ContractDiscoveryConfig,
    contract_source: DatasetSource,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct ContractDiscoveryIteration {
    pub min_available_block: Option<u32>,
    pub max_available_block: Option<u32>,
    pub start_block: Option<u32>,
    pub end_block: Option<u32>,
    pub observed_logs: u64,
    pub pool_events: u64,
    pub validated_pools: u64,
    pub rejected_pools: u64,
    pub multiplier_events: u64,
    pub ui_transfer_events: u64,
    pub queued_addresses: u64,
    pub caught_up: bool,
}

#[derive(Clone, Debug)]
struct DiscoveryCursor {
    last_processed_block: u32,
    last_processed_block_hash: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum PoolVariant {
    V2Compatible,
    V3Compatible,
    SolidlyCompatible,
}

impl PoolVariant {
    fn as_str(self) -> &'static str {
        match self {
            Self::V2Compatible => "v2_compatible",
            Self::V3Compatible => "v3_compatible",
            Self::SolidlyCompatible => "solidly_compatible",
        }
    }

    fn protocol_family(self) -> &'static str {
        match self {
            Self::V2Compatible => "uniswap_v2_compatible",
            Self::V3Compatible => "uniswap_v3_compatible",
            Self::SolidlyCompatible => "solidly_compatible",
        }
    }
}

#[derive(Clone, Debug, Serialize)]
struct PoolCreationEvent {
    variant: PoolVariant,
    event_name: String,
    event_signature: String,
    factory_address: String,
    pool_address: String,
    token0_address: String,
    token1_address: String,
    fee: Option<u32>,
    tick_spacing: Option<i32>,
    stable: Option<bool>,
    block_number: u32,
    block_hash: String,
    tx_hash: String,
    tx_index: u32,
    log_index: u32,
    raw_topics: Vec<String>,
    raw_data: String,
}

#[derive(Clone, Debug, Serialize)]
struct PoolValidation {
    status: String,
    error: Option<String>,
    pool_code_hash: Option<String>,
    factory_code_hash: Option<String>,
    token0_code_hash: Option<String>,
    token1_code_hash: Option<String>,
}

impl PoolValidation {
    fn validated(
        pool_code_hash: String,
        factory_code_hash: String,
        token0_code_hash: String,
        token1_code_hash: String,
    ) -> Self {
        Self {
            status: "validated".to_owned(),
            error: None,
            pool_code_hash: Some(pool_code_hash),
            factory_code_hash: Some(factory_code_hash),
            token0_code_hash: Some(token0_code_hash),
            token1_code_hash: Some(token1_code_hash),
        }
    }

    fn rejected(error: impl Into<String>) -> Self {
        Self {
            status: "rejected".to_owned(),
            error: Some(error.into()),
            pool_code_hash: None,
            factory_code_hash: None,
            token0_code_hash: None,
            token1_code_hash: None,
        }
    }

    fn error(error: impl Into<String>) -> Self {
        Self {
            status: "error".to_owned(),
            error: Some(error.into()),
            pool_code_hash: None,
            factory_code_hash: None,
            token0_code_hash: None,
            token1_code_hash: None,
        }
    }
}

impl ContractDiscoveryService {
    pub async fn new(
        repository: SqlxAnalysisRepository,
        clickhouse: ClickHouseClient,
        rpc_client: Arc<dyn RpcClient>,
        config: ContractDiscoveryConfig,
    ) -> LedgerResult<Self> {
        let pool = repository.pool().clone();
        let contract_source = load_dataset_source(&pool, CONTRACT_DISCOVERY_SOURCE).await?;
        let service = Self {
            repository,
            pool,
            clickhouse,
            rpc_client,
            config,
            contract_source,
        };
        let queued_tokens = service.enqueue_existing_pool_tokens().await?;
        if queued_tokens > 0 {
            info!(
                queued_tokens,
                "queued token metadata checks from validated pool inventory"
            );
        }
        Ok(service)
    }

    pub async fn process_once(&self) -> LedgerResult<ContractDiscoveryIteration> {
        let result = self.process_once_inner().await;
        if let Err(error) = &result {
            let _ = self.mark_cursor_error(&error.to_string()).await;
        }
        result
    }

    async fn process_once_inner(&self) -> LedgerResult<ContractDiscoveryIteration> {
        let Some((min_block, max_block)) = self
            .clickhouse
            .canonical_block_bounds(self.config.chain_id)
            .await?
        else {
            return Ok(ContractDiscoveryIteration::default());
        };
        let cursor = self.load_cursor().await?;
        let start_block = self
            .next_start_block(cursor.as_ref(), min_block, max_block)
            .await?;
        let end_block = max_block.min(
            start_block
                .saturating_add(self.config.batch_size.max(1))
                .saturating_sub(1),
        );
        self.ensure_stock_registry_ready().await?;
        self.mark_cursor_running(start_block, end_block).await?;

        let logs = self
            .clickhouse
            .canonical_logs_by_topics(self.config.chain_id, start_block, end_block, &EVENT_TOPICS)
            .await?;
        let stock_tokens = self.load_validated_stock_tokens().await?;
        let known_pools = self.load_known_pools().await?;
        let pool_events = logs
            .iter()
            .filter_map(|log| match parse_pool_creation_event(log) {
                Ok(event) => event,
                Err(_) => None,
            })
            .collect::<Vec<_>>();
        let validations = self.validate_pool_events(&pool_events).await?;
        let transient_errors = validations
            .iter()
            .filter(|(_, validation)| validation.status == "error")
            .count();
        if transient_errors > 0 {
            for (event, validation) in &validations {
                if validation.status == "error" {
                    self.upsert_discovery_candidate(event, validation, None)
                        .await?;
                }
            }
            let error = format!(
                "{transient_errors} pool validations hit transient RPC errors; cursor was not advanced"
            );
            return Err(LedgerError::TransientIngestion(error));
        }

        self.mark_range_for_reconciliation(start_block, end_block)
            .await?;
        let (multiplier_events, ui_transfer_events) =
            self.upsert_stock_token_events(&logs, &stock_tokens).await?;
        let mut validated_pools = 0_u64;
        let mut rejected_pools = 0_u64;
        let mut queued_addresses = 0_u64;
        for (event, validation) in validations {
            if validation.status != "validated" {
                rejected_pools += 1;
                self.upsert_discovery_candidate(&event, &validation, None)
                    .await?;
                continue;
            }
            let (dataset_key, dataset_version) =
                self.promote_validated_pool(&event, &validation).await?;
            self.upsert_discovery_candidate(
                &event,
                &validation,
                Some((&dataset_key, &dataset_version)),
            )
            .await?;
            self.upsert_factory_and_pool_metadata(
                &event,
                &validation,
                &dataset_key,
                &dataset_version,
            )
            .await?;
            if !known_pools.contains(&event.pool_address) {
                queued_addresses += self.enqueue_discovered_addresses(&event).await?;
            }
            validated_pools += 1;
        }
        self.retract_orphaned_datasets(start_block, end_block)
            .await?;
        let end_hash = self
            .clickhouse
            .get_canonical_block(self.config.chain_id, end_block)
            .await?
            .map(|block| block.block_hash)
            .unwrap_or_default();
        self.mark_cursor_success(end_block, &end_hash, logs.len() as u64)
            .await?;

        Ok(ContractDiscoveryIteration {
            min_available_block: Some(min_block),
            max_available_block: Some(max_block),
            start_block: Some(start_block),
            end_block: Some(end_block),
            observed_logs: logs.len() as u64,
            pool_events: pool_events.len() as u64,
            validated_pools,
            rejected_pools,
            multiplier_events,
            ui_transfer_events,
            queued_addresses,
            caught_up: end_block >= max_block,
        })
    }

    async fn next_start_block(
        &self,
        cursor: Option<&DiscoveryCursor>,
        min_block: u32,
        max_block: u32,
    ) -> LedgerResult<u32> {
        let Some(cursor) = cursor else {
            return Ok(min_block);
        };
        let cursor_is_canonical = self
            .clickhouse
            .get_canonical_block(self.config.chain_id, cursor.last_processed_block)
            .await?
            .is_some_and(|block| {
                block
                    .block_hash
                    .eq_ignore_ascii_case(&cursor.last_processed_block_hash)
            });
        if !cursor_is_canonical || cursor.last_processed_block >= max_block {
            return Ok(cursor
                .last_processed_block
                .saturating_sub(self.config.lookback_blocks.saturating_sub(1))
                .max(min_block));
        }
        Ok(cursor.last_processed_block.saturating_add(1).max(min_block))
    }

    async fn load_cursor(&self) -> LedgerResult<Option<DiscoveryCursor>> {
        let row = sqlx::query(
            r#"
            select last_processed_block, last_processed_block_hash
            from onchain_event_cursors
            where worker_id = $1
            "#,
        )
        .bind(&self.config.worker_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(persistence_error)?;
        Ok(row.and_then(|row| {
            let block = row.get::<Option<i64>, _>("last_processed_block")?;
            Some(DiscoveryCursor {
                last_processed_block: block.max(0) as u32,
                last_processed_block_hash: row
                    .get::<Option<String>, _>("last_processed_block_hash")
                    .unwrap_or_default(),
            })
        }))
    }

    async fn mark_cursor_running(&self, start_block: u32, end_block: u32) -> LedgerResult<()> {
        sqlx::query(
            r#"
            insert into onchain_event_cursors (
              worker_id, chain_id, source_table, scan_from_block, scan_to_block,
              lookback_blocks, status, metadata, created_at, updated_at
            )
            values ($1,$2,'clickhouse.logs_canonical',$3,$4,$5,'running',$6,now(),now())
            on conflict (worker_id) do update set
              chain_id = excluded.chain_id,
              source_table = excluded.source_table,
              scan_from_block = excluded.scan_from_block,
              scan_to_block = excluded.scan_to_block,
              lookback_blocks = excluded.lookback_blocks,
              status = 'running',
              last_error = null,
              metadata = onchain_event_cursors.metadata || excluded.metadata,
              updated_at = now()
            "#,
        )
        .bind(&self.config.worker_id)
        .bind(self.config.chain_id as i64)
        .bind(start_block as i64)
        .bind(end_block as i64)
        .bind(self.config.lookback_blocks as i64)
        .bind(Json(json!({"event_topics": EVENT_TOPICS})))
        .execute(&self.pool)
        .await
        .map_err(persistence_error)?;
        Ok(())
    }

    async fn mark_cursor_success(
        &self,
        end_block: u32,
        end_hash: &str,
        event_count: u64,
    ) -> LedgerResult<()> {
        sqlx::query(
            r#"
            update onchain_event_cursors
            set last_processed_block = $2,
                last_processed_block_hash = $3,
                status = 'idle',
                retry_count = 0,
                last_error = null,
                last_event_count = $4,
                last_success_at = now(),
                updated_at = now()
            where worker_id = $1
            "#,
        )
        .bind(&self.config.worker_id)
        .bind(end_block as i64)
        .bind(end_hash)
        .bind(event_count as i64)
        .execute(&self.pool)
        .await
        .map_err(persistence_error)?;
        Ok(())
    }

    async fn mark_cursor_error(&self, error: &str) -> LedgerResult<()> {
        sqlx::query(
            r#"
            update onchain_event_cursors
            set status = 'error',
                retry_count = retry_count + 1,
                last_error = $2,
                updated_at = now()
            where worker_id = $1
            "#,
        )
        .bind(&self.config.worker_id)
        .bind(truncate_text(error, 8_000))
        .execute(&self.pool)
        .await
        .map_err(persistence_error)?;
        Ok(())
    }

    async fn load_validated_stock_tokens(&self) -> LedgerResult<BTreeSet<String>> {
        let rows = sqlx::query(
            r#"
            select contract_address
            from stock_token_deployments
            where chain_id = $1 and validation_status = 'validated'
            "#,
        )
        .bind(self.config.chain_id as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(persistence_error)?;
        Ok(rows
            .into_iter()
            .map(|row| row.get::<String, _>("contract_address"))
            .collect())
    }

    async fn load_known_pools(&self) -> LedgerResult<BTreeSet<String>> {
        let rows = sqlx::query(
            r#"
            select pool_address
            from dex_pool_metadata
            where chain_id = $1
            "#,
        )
        .bind(self.config.chain_id as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(persistence_error)?;
        Ok(rows
            .into_iter()
            .map(|row| row.get::<String, _>("pool_address"))
            .collect())
    }

    async fn ensure_stock_registry_ready(&self) -> LedgerResult<()> {
        let ready = sqlx::query_scalar::<_, bool>(
            r#"
            select coalesce(bool_and(not endpoints.enabled or cursors.last_success_at is not null), true)
            from intelligence_source_endpoints endpoints
            join intelligence_sync_cursors cursors using (endpoint_key)
            where endpoints.parser_kind = 'robinhood_stock_assets'
              and endpoints.chain_id = $1
            "#,
        )
        .bind(self.config.chain_id as i64)
        .fetch_one(&self.pool)
        .await
        .map_err(persistence_error)?;
        if ready {
            Ok(())
        } else {
            Err(LedgerError::TransientIngestion(
                "stock-token registry has not completed its first successful sync".to_owned(),
            ))
        }
    }

    async fn validate_pool_events(
        &self,
        events: &[PoolCreationEvent],
    ) -> LedgerResult<Vec<(PoolCreationEvent, PoolValidation)>> {
        let semaphore = Arc::new(Semaphore::new(self.config.rpc_concurrency.clamp(1, 32)));
        let mut tasks = JoinSet::new();
        for event in events.iter().cloned() {
            let rpc_client = self.rpc_client.clone();
            let semaphore = semaphore.clone();
            let chain_id = u64::from(self.config.chain_id);
            tasks.spawn(async move {
                let validation = match semaphore.acquire_owned().await {
                    Ok(_permit) => validate_pool_event(rpc_client, chain_id, &event).await,
                    Err(err) => PoolValidation::error(err.to_string()),
                };
                (event, validation)
            });
        }
        let mut results = Vec::new();
        while let Some(result) = tasks.join_next().await {
            results.push(result.map_err(|err| {
                LedgerError::TransientIngestion(format!("validation task failed: {err}"))
            })?);
        }
        results.sort_by_key(|(event, _)| (event.block_number, event.tx_index, event.log_index));
        Ok(results)
    }

    async fn mark_range_for_reconciliation(
        &self,
        start_block: u32,
        end_block: u32,
    ) -> LedgerResult<()> {
        let mut tx = self.pool.begin().await.map_err(persistence_error)?;
        sqlx::query(
            r#"
            update contract_discovery_candidates
            set is_canonical = false, status = 'orphaned', updated_at = now()
            where chain_id = $1 and block_number between $2 and $3
            "#,
        )
        .bind(self.config.chain_id as i64)
        .bind(start_block as i64)
        .bind(end_block as i64)
        .execute(&mut *tx)
        .await
        .map_err(persistence_error)?;
        sqlx::query(
            r#"
            update dex_pool_metadata
            set validation_status = 'orphaned', updated_at = now()
            where chain_id = $1 and creation_block between $2 and $3
            "#,
        )
        .bind(self.config.chain_id as i64)
        .bind(start_block as i64)
        .bind(end_block as i64)
        .execute(&mut *tx)
        .await
        .map_err(persistence_error)?;
        sqlx::query(
            r#"
            update stock_token_multiplier_events
            set is_canonical = false
            where chain_id = $1 and block_number between $2 and $3
            "#,
        )
        .bind(self.config.chain_id as i64)
        .bind(start_block as i64)
        .bind(end_block as i64)
        .execute(&mut *tx)
        .await
        .map_err(persistence_error)?;
        sqlx::query(
            r#"
            update stock_token_ui_transfer_events
            set is_canonical = false, updated_at = now()
            where chain_id = $1 and block_number between $2 and $3
            "#,
        )
        .bind(self.config.chain_id as i64)
        .bind(start_block as i64)
        .bind(end_block as i64)
        .execute(&mut *tx)
        .await
        .map_err(persistence_error)?;
        tx.commit().await.map_err(persistence_error)
    }

    async fn upsert_stock_token_events(
        &self,
        logs: &[RawLog],
        stock_tokens: &BTreeSet<String>,
    ) -> LedgerResult<(u64, u64)> {
        let mut multiplier_events = 0_u64;
        let mut ui_transfer_events = 0_u64;
        let mut tx = self.pool.begin().await.map_err(persistence_error)?;
        for log in logs {
            let address = log.address.to_ascii_lowercase();
            if !stock_tokens.contains(&address) {
                continue;
            }
            let topic0 = log
                .topics
                .first()
                .map(|topic| topic.to_ascii_lowercase())
                .unwrap_or_default();
            if topic0 == UI_MULTIPLIER_UPDATED_TOPIC {
                let words = data_words(&log.data)?;
                if words.len() < 3 {
                    continue;
                }
                let effective_at = word_as_u64(words[2])
                    .and_then(|timestamp| Utc.timestamp_opt(timestamp as i64, 0).single());
                sqlx::query(
                    r#"
                    insert into stock_token_multiplier_events (
                      chain_id, contract_address, block_number, block_hash, tx_hash,
                      tx_index, log_index, old_value, new_value, effective_at,
                      event_signature, source_key, is_canonical, metadata,
                      observed_at, created_at
                    )
                    values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,true,$13,now(),now())
                    on conflict (chain_id, tx_hash, log_index) do update set
                      contract_address = excluded.contract_address,
                      block_number = excluded.block_number,
                      block_hash = excluded.block_hash,
                      tx_index = excluded.tx_index,
                      old_value = excluded.old_value,
                      new_value = excluded.new_value,
                      effective_at = excluded.effective_at,
                      event_signature = excluded.event_signature,
                      source_key = excluded.source_key,
                      is_canonical = true,
                      metadata = excluded.metadata,
                      observed_at = now()
                    "#,
                )
                .bind(self.config.chain_id as i64)
                .bind(&address)
                .bind(log.block_number as i64)
                .bind(log.block_hash.to_ascii_lowercase())
                .bind(log.tx_hash.to_ascii_lowercase())
                .bind(log.tx_index as i64)
                .bind(log.log_index as i64)
                .bind(abi_uint_text(words[0]))
                .bind(abi_uint_text(words[1]))
                .bind(effective_at)
                .bind(UI_MULTIPLIER_UPDATED_TOPIC)
                .bind(STOCK_EVENT_SOURCE)
                .bind(Json(
                    json!({"raw_data": log.data, "raw_topics": log.topics}),
                ))
                .execute(&mut *tx)
                .await
                .map_err(persistence_error)?;
                multiplier_events += 1;
            } else if topic0 == TRANSFER_WITH_UI_AMOUNT_TOPIC && log.topics.len() >= 3 {
                let words = data_words(&log.data)?;
                if words.len() < 2 {
                    continue;
                }
                let Some(from_address) = topic_address(&log.topics[1]) else {
                    continue;
                };
                let Some(to_address) = topic_address(&log.topics[2]) else {
                    continue;
                };
                sqlx::query(
                    r#"
                    insert into stock_token_ui_transfer_events (
                      chain_id, contract_address, block_number, block_hash, tx_hash,
                      tx_index, log_index, from_address, to_address, raw_amount,
                      ui_amount, event_signature, source_key, is_canonical, metadata,
                      observed_at, created_at, updated_at
                    )
                    values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,true,$14,now(),now(),now())
                    on conflict (chain_id, tx_hash, log_index) do update set
                      contract_address = excluded.contract_address,
                      block_number = excluded.block_number,
                      block_hash = excluded.block_hash,
                      tx_index = excluded.tx_index,
                      from_address = excluded.from_address,
                      to_address = excluded.to_address,
                      raw_amount = excluded.raw_amount,
                      ui_amount = excluded.ui_amount,
                      event_signature = excluded.event_signature,
                      source_key = excluded.source_key,
                      is_canonical = true,
                      metadata = excluded.metadata,
                      observed_at = now(),
                      updated_at = now()
                    "#,
                )
                .bind(self.config.chain_id as i64)
                .bind(&address)
                .bind(log.block_number as i64)
                .bind(log.block_hash.to_ascii_lowercase())
                .bind(log.tx_hash.to_ascii_lowercase())
                .bind(log.tx_index as i64)
                .bind(log.log_index as i64)
                .bind(from_address)
                .bind(to_address)
                .bind(abi_uint_text(words[0]))
                .bind(abi_uint_text(words[1]))
                .bind(TRANSFER_WITH_UI_AMOUNT_TOPIC)
                .bind(STOCK_EVENT_SOURCE)
                .bind(Json(
                    json!({"raw_data": log.data, "raw_topics": log.topics}),
                ))
                .execute(&mut *tx)
                .await
                .map_err(persistence_error)?;
                ui_transfer_events += 1;
            }
        }
        tx.commit().await.map_err(persistence_error)?;
        Ok((multiplier_events, ui_transfer_events))
    }

    async fn promote_validated_pool(
        &self,
        event: &PoolCreationEvent,
        validation: &PoolValidation,
    ) -> LedgerResult<(String, String)> {
        let protocol_key = protocol_key(event);
        let display_name = protocol_display_name(event);
        let family = event.variant.protocol_family();
        let factory_dataset_key = format!(
            "discovered_factory_{}_{}_{}",
            self.config.chain_id,
            event.factory_address.trim_start_matches("0x"),
            family
        );
        let factory_dataset_version = validation.factory_code_hash.clone().ok_or_else(|| {
            LedgerError::AnalysisInvariant("factory code hash missing".to_owned())
        })?;
        let factory_manifest = IntelligenceDatasetManifest {
            dataset_key: factory_dataset_key.clone(),
            dataset_version: factory_dataset_version.clone(),
            chain_id: u64::from(self.config.chain_id),
            source: self.contract_source.clone(),
            addresses: vec![DatasetAddress {
                address: event.factory_address.clone(),
                address_kind: "contract".to_owned(),
                labels: vec![
                    discovery_label(
                        "dex_factory",
                        "contract_role",
                        "Factory observed emitting a validated pool-creation event",
                        event,
                    ),
                    discovery_label(
                        &format!("protocol:{protocol_key}"),
                        "protocol",
                        "Protocol identity scoped to a discovered factory address",
                        event,
                    ),
                ],
                token_metadata: None,
                protocol_deployments: vec![DatasetProtocolDeployment {
                    protocol_key: protocol_key.clone(),
                    display_name: display_name.clone(),
                    protocol_family: family.to_owned(),
                    contract_role: "factory".to_owned(),
                    confidence: 0.90,
                    metadata: json!({
                        "classification": "interface_compatible_not_brand_attribution",
                        "factory_address": event.factory_address,
                        "first_observed_event": event,
                        "code_hash": validation.factory_code_hash,
                    }),
                }],
                metadata: json!({
                    "code_hash": validation.factory_code_hash,
                    "discovery_method": "canonical_factory_event_v1",
                }),
            }],
            metadata: json!({"event": event, "validation": validation}),
        };
        import_intelligence_dataset(&self.pool, &factory_manifest, &factory_dataset_version)
            .await?;

        let pool_dataset_key = format!(
            "discovered_pool_{}_{}",
            self.config.chain_id,
            event.pool_address.trim_start_matches("0x")
        );
        let evidence = json!({"event": event, "validation": validation});
        let pool_dataset_version = dataset_checksum(&evidence.to_string());
        let pool_manifest = IntelligenceDatasetManifest {
            dataset_key: pool_dataset_key.clone(),
            dataset_version: pool_dataset_version.clone(),
            chain_id: u64::from(self.config.chain_id),
            source: self.contract_source.clone(),
            addresses: vec![DatasetAddress {
                address: event.pool_address.clone(),
                address_kind: "contract".to_owned(),
                labels: vec![
                    discovery_label(
                        "dex_pool",
                        "contract_role",
                        "Pool validated from a canonical factory creation event and RPC probes",
                        event,
                    ),
                    discovery_label(
                        &format!("pool_variant:{}", event.variant.as_str()),
                        "contract_role",
                        "Interface-compatible pool variant; not a brand attribution",
                        event,
                    ),
                    discovery_label(
                        &format!("protocol:{protocol_key}"),
                        "protocol",
                        "Protocol identity scoped to the emitting factory address",
                        event,
                    ),
                ],
                token_metadata: None,
                protocol_deployments: vec![DatasetProtocolDeployment {
                    protocol_key: protocol_key.clone(),
                    display_name,
                    protocol_family: family.to_owned(),
                    contract_role: "pool".to_owned(),
                    confidence: 0.90,
                    metadata: json!({
                        "classification": "interface_compatible_not_brand_attribution",
                        "factory_address": event.factory_address,
                        "token0": event.token0_address,
                        "token1": event.token1_address,
                        "fee": event.fee,
                        "tick_spacing": event.tick_spacing,
                        "stable": event.stable,
                        "creation_event": event,
                        "code_hash": validation.pool_code_hash,
                    }),
                }],
                metadata: evidence.clone(),
            }],
            metadata: evidence,
        };
        import_intelligence_dataset(&self.pool, &pool_manifest, &pool_dataset_version).await?;
        Ok((pool_dataset_key, pool_dataset_version))
    }

    async fn upsert_discovery_candidate(
        &self,
        event: &PoolCreationEvent,
        validation: &PoolValidation,
        dataset: Option<(&str, &str)>,
    ) -> LedgerResult<()> {
        let status = if validation.status == "validated" {
            "validated"
        } else {
            validation.status.as_str()
        };
        let (dataset_key, dataset_version) = dataset.unzip();
        sqlx::query(
            r#"
            insert into contract_discovery_candidates (
              chain_id, address, candidate_kind, discovery_method, event_name,
              event_signature, source_address, source_key, block_number, block_hash,
              tx_hash, tx_index, log_index, protocol_family, contract_role,
              token0_address, token1_address, fee, tick_spacing, stable, status,
              confidence, validation_error, code_hash, dataset_key, dataset_version,
              is_canonical, evidence, first_seen_at, last_seen_at, created_at, updated_at
            )
            values (
              $1,$2,'dex_pool','canonical_factory_event_v1',$3,$4,$5,$6,$7,$8,
              $9,$10,$11,$12,'pool',$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,
              true,$24,now(),now(),now(),now()
            )
            on conflict (chain_id, tx_hash, log_index, address, candidate_kind) do update set
              block_number = excluded.block_number,
              block_hash = excluded.block_hash,
              source_address = excluded.source_address,
              protocol_family = excluded.protocol_family,
              token0_address = excluded.token0_address,
              token1_address = excluded.token1_address,
              fee = excluded.fee,
              tick_spacing = excluded.tick_spacing,
              stable = excluded.stable,
              status = excluded.status,
              confidence = excluded.confidence,
              validation_error = excluded.validation_error,
              code_hash = excluded.code_hash,
              dataset_key = excluded.dataset_key,
              dataset_version = excluded.dataset_version,
              is_canonical = true,
              evidence = excluded.evidence,
              last_seen_at = now(),
              updated_at = now()
            "#,
        )
        .bind(self.config.chain_id as i64)
        .bind(&event.pool_address)
        .bind(&event.event_name)
        .bind(&event.event_signature)
        .bind(&event.factory_address)
        .bind(CONTRACT_DISCOVERY_SOURCE)
        .bind(event.block_number as i64)
        .bind(&event.block_hash)
        .bind(&event.tx_hash)
        .bind(event.tx_index as i64)
        .bind(event.log_index as i64)
        .bind(event.variant.protocol_family())
        .bind(&event.token0_address)
        .bind(&event.token1_address)
        .bind(event.fee.map(|value| value as i32))
        .bind(event.tick_spacing)
        .bind(event.stable)
        .bind(status)
        .bind(if status == "validated" { 0.90_f64 } else { 0.0 })
        .bind(&validation.error)
        .bind(&validation.pool_code_hash)
        .bind(dataset_key)
        .bind(dataset_version)
        .bind(Json(json!({"event": event, "validation": validation})))
        .execute(&self.pool)
        .await
        .map_err(persistence_error)?;
        Ok(())
    }

    async fn upsert_factory_and_pool_metadata(
        &self,
        event: &PoolCreationEvent,
        validation: &PoolValidation,
        pool_dataset_key: &str,
        pool_dataset_version: &str,
    ) -> LedgerResult<()> {
        let protocol_key = protocol_key(event);
        let family = event.variant.protocol_family();
        let factory_dataset_key = format!(
            "discovered_factory_{}_{}_{}",
            self.config.chain_id,
            event.factory_address.trim_start_matches("0x"),
            family
        );
        let factory_dataset_version = validation.factory_code_hash.as_deref().ok_or_else(|| {
            LedgerError::AnalysisInvariant("factory code hash missing".to_owned())
        })?;
        let pool_code_hash = validation
            .pool_code_hash
            .as_deref()
            .ok_or_else(|| LedgerError::AnalysisInvariant("pool code hash missing".to_owned()))?;
        sqlx::query(
            r#"
            insert into dex_factory_metadata (
              chain_id, factory_address, protocol_key, protocol_family, display_name,
              code_hash, validation_status, source_key, first_event_block,
              last_event_block, observed_event_count, dataset_key, dataset_version,
              metadata, first_seen_at, last_seen_at, created_at, updated_at
            )
            values ($1,$2,$3,$4,$5,$6,'active',$7,$8,$8,1,$9,$10,$11,now(),now(),now(),now())
            on conflict (chain_id, factory_address, protocol_family) do update set
              protocol_key = excluded.protocol_key,
              display_name = excluded.display_name,
              code_hash = excluded.code_hash,
              validation_status = 'active',
              first_event_block = least(dex_factory_metadata.first_event_block, excluded.first_event_block),
              last_event_block = greatest(dex_factory_metadata.last_event_block, excluded.last_event_block),
              observed_event_count = greatest(dex_factory_metadata.observed_event_count, 1),
              dataset_key = excluded.dataset_key,
              dataset_version = excluded.dataset_version,
              metadata = dex_factory_metadata.metadata || excluded.metadata,
              last_seen_at = now(),
              updated_at = now()
            "#,
        )
        .bind(self.config.chain_id as i64)
        .bind(&event.factory_address)
        .bind(&protocol_key)
        .bind(family)
        .bind(protocol_display_name(event))
        .bind(factory_dataset_version)
        .bind(CONTRACT_DISCOVERY_SOURCE)
        .bind(event.block_number as i64)
        .bind(factory_dataset_key)
        .bind(factory_dataset_version)
        .bind(Json(json!({"last_event": event})))
        .execute(&self.pool)
        .await
        .map_err(persistence_error)?;

        sqlx::query(
            r#"
            update dex_factory_metadata factories
            set observed_event_count = (
                  select count(*)::bigint
                  from contract_discovery_candidates candidates
                  where candidates.chain_id = factories.chain_id
                    and candidates.source_address = factories.factory_address
                    and candidates.protocol_family = factories.protocol_family
                    and candidates.status = 'validated'
                    and candidates.is_canonical
                ),
                updated_at = now()
            where factories.chain_id = $1
              and factories.factory_address = $2
              and factories.protocol_family = $3
            "#,
        )
        .bind(self.config.chain_id as i64)
        .bind(&event.factory_address)
        .bind(family)
        .execute(&self.pool)
        .await
        .map_err(persistence_error)?;

        sqlx::query(
            r#"
            insert into dex_pool_metadata (
              chain_id, pool_address, factory_address, protocol_key, pool_variant,
              token0_address, token1_address, fee, tick_spacing, stable,
              creation_block, creation_block_hash, creation_tx_hash, creation_log_index,
              code_hash, validation_status, source_key, dataset_key, dataset_version,
              metadata, first_seen_at, last_seen_at, created_at, updated_at
            )
            values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,'active',$16,$17,$18,$19,now(),now(),now(),now())
            on conflict (chain_id, pool_address) do update set
              factory_address = excluded.factory_address,
              protocol_key = excluded.protocol_key,
              pool_variant = excluded.pool_variant,
              token0_address = excluded.token0_address,
              token1_address = excluded.token1_address,
              fee = excluded.fee,
              tick_spacing = excluded.tick_spacing,
              stable = excluded.stable,
              creation_block = excluded.creation_block,
              creation_block_hash = excluded.creation_block_hash,
              creation_tx_hash = excluded.creation_tx_hash,
              creation_log_index = excluded.creation_log_index,
              code_hash = excluded.code_hash,
              validation_status = 'active',
              source_key = excluded.source_key,
              dataset_key = excluded.dataset_key,
              dataset_version = excluded.dataset_version,
              metadata = excluded.metadata,
              last_seen_at = now(),
              updated_at = now()
            "#,
        )
        .bind(self.config.chain_id as i64)
        .bind(&event.pool_address)
        .bind(&event.factory_address)
        .bind(protocol_key)
        .bind(event.variant.as_str())
        .bind(&event.token0_address)
        .bind(&event.token1_address)
        .bind(event.fee.map(|value| value as i32))
        .bind(event.tick_spacing)
        .bind(event.stable)
        .bind(event.block_number as i64)
        .bind(&event.block_hash)
        .bind(&event.tx_hash)
        .bind(event.log_index as i64)
        .bind(pool_code_hash)
        .bind(CONTRACT_DISCOVERY_SOURCE)
        .bind(pool_dataset_key)
        .bind(pool_dataset_version)
        .bind(Json(json!({"event": event, "validation": validation})))
        .execute(&self.pool)
        .await
        .map_err(persistence_error)?;
        Ok(())
    }

    async fn enqueue_discovered_addresses(&self, event: &PoolCreationEvent) -> LedgerResult<u64> {
        let candidates = vec![
            AddressAnalysisCandidate {
                address: event.pool_address.clone(),
                priority: 20,
                metadata: json!({
                    "source": "onchain_factory_event",
                    "roles": ["suspected_pool"],
                    "factory": event.factory_address,
                    "creation_tx_hash": event.tx_hash,
                }),
            },
            AddressAnalysisCandidate {
                address: event.factory_address.clone(),
                priority: 25,
                metadata: json!({
                    "source": "onchain_factory_event",
                    "roles": ["suspected_protocol"],
                    "creation_tx_hash": event.tx_hash,
                }),
            },
            AddressAnalysisCandidate {
                address: event.token0_address.clone(),
                priority: 30,
                metadata: json!({
                    "source": "onchain_factory_event",
                    "roles": ["suspected_token"],
                    "related_pool": event.pool_address,
                }),
            },
            AddressAnalysisCandidate {
                address: event.token1_address.clone(),
                priority: 30,
                metadata: json!({
                    "source": "onchain_factory_event",
                    "roles": ["suspected_token"],
                    "related_pool": event.pool_address,
                }),
            },
        ];
        self.repository
            .enqueue_address_analysis_candidates(u64::from(self.config.chain_id), &candidates)
            .await
    }

    async fn enqueue_existing_pool_tokens(&self) -> LedgerResult<u64> {
        let rows = sqlx::query(
            r#"
            with pool_tokens as (
              select token0_address as token_address
              from dex_pool_metadata
              where chain_id = $1 and validation_status = 'active'
              union all
              select token1_address as token_address
              from dex_pool_metadata
              where chain_id = $1 and validation_status = 'active'
            ), tokens as (
              select token_address, count(*)::bigint as pool_count
              from pool_tokens
              group by token_address
            )
            select tokens.token_address, tokens.pool_count
            from tokens
            left join address_analysis_jobs jobs
              on jobs.chain_id = $1 and jobs.address = tokens.token_address
            where jobs.address is null
               or not (coalesce(jobs.metadata->'roles', '[]'::jsonb) ? 'suspected_token')
            order by tokens.token_address
            "#,
        )
        .bind(self.config.chain_id as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(persistence_error)?;
        let candidates = rows
            .into_iter()
            .map(|row| AddressAnalysisCandidate {
                address: row.get::<String, _>("token_address"),
                priority: 30,
                metadata: json!({
                    "source": "validated_dex_pool_inventory",
                    "roles": ["suspected_token"],
                    "related_pool_count": row.get::<i64, _>("pool_count"),
                }),
            })
            .collect::<Vec<_>>();
        self.repository
            .enqueue_address_analysis_candidates(u64::from(self.config.chain_id), &candidates)
            .await
    }

    async fn retract_orphaned_datasets(
        &self,
        start_block: u32,
        end_block: u32,
    ) -> LedgerResult<()> {
        let mut tx = self.pool.begin().await.map_err(persistence_error)?;
        sqlx::query(
            r#"
            update address_label_assertions assertions
            set status = 'retracted', updated_at = now()
            from contract_discovery_candidates candidates
            where candidates.chain_id = $1
              and candidates.block_number between $2 and $3
              and candidates.is_canonical = false
              and candidates.dataset_key = assertions.dataset_key
              and candidates.dataset_version = assertions.dataset_version
              and assertions.status = 'active'
            "#,
        )
        .bind(self.config.chain_id as i64)
        .bind(start_block as i64)
        .bind(end_block as i64)
        .execute(&mut *tx)
        .await
        .map_err(persistence_error)?;
        sqlx::query(
            r#"
            update protocol_deployments deployments
            set status = 'retracted', updated_at = now()
            from contract_discovery_candidates candidates
            where candidates.chain_id = $1
              and candidates.block_number between $2 and $3
              and candidates.is_canonical = false
              and candidates.dataset_key = deployments.dataset_key
              and candidates.dataset_version = deployments.dataset_version
              and deployments.status = 'active'
            "#,
        )
        .bind(self.config.chain_id as i64)
        .bind(start_block as i64)
        .bind(end_block as i64)
        .execute(&mut *tx)
        .await
        .map_err(persistence_error)?;
        sqlx::query(
            r#"
            update intelligence_datasets datasets
            set status = 'retracted'
            from contract_discovery_candidates candidates
            where candidates.chain_id = $1
              and candidates.block_number between $2 and $3
              and candidates.is_canonical = false
              and candidates.dataset_key = datasets.dataset_key
              and candidates.dataset_version = datasets.dataset_version
              and datasets.status = 'active'
            "#,
        )
        .bind(self.config.chain_id as i64)
        .bind(start_block as i64)
        .bind(end_block as i64)
        .execute(&mut *tx)
        .await
        .map_err(persistence_error)?;
        tx.commit().await.map_err(persistence_error)
    }
}

async fn validate_pool_event(
    rpc_client: Arc<dyn RpcClient>,
    chain_id: u64,
    event: &PoolCreationEvent,
) -> PoolValidation {
    if event.token0_address == event.token1_address {
        return PoolValidation::rejected("pool event contains identical token addresses");
    }
    let pool_code = match rpc_client
        .get_code(chain_id, &event.pool_address, None)
        .await
    {
        Ok(code) if !is_empty_code(&code) => code,
        Ok(_) => return PoolValidation::rejected("pool has no bytecode"),
        Err(err) => return PoolValidation::error(err.to_string()),
    };
    let factory_code = match rpc_client
        .get_code(chain_id, &event.factory_address, None)
        .await
    {
        Ok(code) if !is_empty_code(&code) => code,
        Ok(_) => return PoolValidation::rejected("factory emitter has no bytecode"),
        Err(err) => return PoolValidation::error(err.to_string()),
    };
    let token0_code = match rpc_client
        .get_code(chain_id, &event.token0_address, None)
        .await
    {
        Ok(code) if !is_empty_code(&code) => code,
        Ok(_) => return PoolValidation::rejected("token0 has no bytecode"),
        Err(err) => return PoolValidation::error(err.to_string()),
    };
    let token1_code = match rpc_client
        .get_code(chain_id, &event.token1_address, None)
        .await
    {
        Ok(code) if !is_empty_code(&code) => code,
        Ok(_) => return PoolValidation::rejected("token1 has no bytecode"),
        Err(err) => return PoolValidation::error(err.to_string()),
    };

    let token0 = match required_address_call(
        rpc_client.as_ref(),
        chain_id,
        &event.pool_address,
        TOKEN0_SELECTOR,
        "token0",
    )
    .await
    {
        Ok(value) => value,
        Err(validation) => return validation,
    };
    let token1 = match required_address_call(
        rpc_client.as_ref(),
        chain_id,
        &event.pool_address,
        TOKEN1_SELECTOR,
        "token1",
    )
    .await
    {
        Ok(value) => value,
        Err(validation) => return validation,
    };
    if token0 != event.token0_address || token1 != event.token1_address {
        return PoolValidation::rejected(format!(
            "pool getters do not match creation event: token0={token0}, token1={token1}"
        ));
    }
    if let Err(validation) =
        validate_factory_relationship(rpc_client.as_ref(), chain_id, event).await
    {
        return validation;
    }

    let variant_result = match event.variant {
        PoolVariant::V2Compatible => required_call(
            rpc_client.as_ref(),
            chain_id,
            &event.pool_address,
            GET_RESERVES_SELECTOR,
            "getReserves",
        )
        .await
        .and_then(|value| {
            (value.trim_start_matches("0x").len() >= 192)
                .then_some(())
                .ok_or_else(|| PoolValidation::rejected("getReserves response is too short"))
        }),
        PoolVariant::V3Compatible => {
            validate_v3_interfaces(rpc_client.as_ref(), chain_id, event).await
        }
        PoolVariant::SolidlyCompatible => {
            validate_solidly_interfaces(rpc_client.as_ref(), chain_id, event).await
        }
    };
    if let Err(validation) = variant_result {
        return validation;
    }
    PoolValidation::validated(
        code_hash(&pool_code),
        code_hash(&factory_code),
        code_hash(&token0_code),
        code_hash(&token1_code),
    )
}

async fn validate_factory_relationship(
    rpc_client: &dyn RpcClient,
    chain_id: u64,
    event: &PoolCreationEvent,
) -> Result<(), PoolValidation> {
    match rpc_client
        .eth_call(chain_id, &event.pool_address, FACTORY_SELECTOR, None)
        .await
    {
        Ok(value) => {
            let factory = abi_word_address(&value)
                .ok_or_else(|| PoolValidation::rejected("factory response is not an address"))?;
            if factory == event.factory_address {
                return Ok(());
            }
            return Err(PoolValidation::rejected(format!(
                "pool factory getter returned {factory}, expected {}",
                event.factory_address
            )));
        }
        Err(err)
            if event.variant == PoolVariant::V2Compatible
                && is_deterministic_call_failure(&err.to_string()) => {}
        Err(err) if is_deterministic_call_failure(&err.to_string()) => {
            return Err(PoolValidation::rejected(format!(
                "required factory call reverted: {err}"
            )));
        }
        Err(err) => {
            return Err(PoolValidation::error(format!(
                "required factory call failed: {err}"
            )));
        }
    }

    let call_data = encode_two_address_call(
        GET_PAIR_SELECTOR,
        &event.token0_address,
        &event.token1_address,
    )
    .ok_or_else(|| PoolValidation::rejected("could not encode factory getPair call"))?;
    let value = required_call(
        rpc_client,
        chain_id,
        &event.factory_address,
        &call_data,
        "factory.getPair",
    )
    .await?;
    let pool = abi_word_address(&value)
        .ok_or_else(|| PoolValidation::rejected("factory getPair response is not an address"))?;
    if pool != event.pool_address {
        return Err(PoolValidation::rejected(format!(
            "factory getPair returned {pool}, expected {}",
            event.pool_address
        )));
    }
    Ok(())
}

async fn validate_v3_interfaces(
    rpc_client: &dyn RpcClient,
    chain_id: u64,
    event: &PoolCreationEvent,
) -> Result<(), PoolValidation> {
    let fee = required_call(
        rpc_client,
        chain_id,
        &event.pool_address,
        FEE_SELECTOR,
        "fee",
    )
    .await?
    .pipe(|value| abi_word_u32(&value))
    .ok_or_else(|| PoolValidation::rejected("fee response is not uint24"))?;
    let tick_spacing = required_call(
        rpc_client,
        chain_id,
        &event.pool_address,
        TICK_SPACING_SELECTOR,
        "tickSpacing",
    )
    .await?
    .pipe(|value| abi_word_i24(&value))
    .ok_or_else(|| PoolValidation::rejected("tickSpacing response is not int24"))?;
    if Some(fee) != event.fee || Some(tick_spacing) != event.tick_spacing {
        return Err(PoolValidation::rejected(format!(
            "V3 fee/tickSpacing mismatch: fee={fee}, tick_spacing={tick_spacing}"
        )));
    }
    let slot0 = required_call(
        rpc_client,
        chain_id,
        &event.pool_address,
        SLOT0_SELECTOR,
        "slot0",
    )
    .await?;
    if slot0.trim_start_matches("0x").len() < 64 {
        return Err(PoolValidation::rejected("slot0 response is too short"));
    }
    Ok(())
}

async fn validate_solidly_interfaces(
    rpc_client: &dyn RpcClient,
    chain_id: u64,
    event: &PoolCreationEvent,
) -> Result<(), PoolValidation> {
    let stable = required_call(
        rpc_client,
        chain_id,
        &event.pool_address,
        STABLE_SELECTOR,
        "stable",
    )
    .await?
    .pipe(|value| abi_word_bool(&value))
    .ok_or_else(|| PoolValidation::rejected("stable response is not bool"))?;
    if Some(stable) != event.stable {
        return Err(PoolValidation::rejected(format!(
            "Solidly stable flag mismatch: {stable}"
        )));
    }
    let reserves = required_call(
        rpc_client,
        chain_id,
        &event.pool_address,
        GET_RESERVES_SELECTOR,
        "getReserves",
    )
    .await?;
    if reserves.trim_start_matches("0x").len() < 192 {
        return Err(PoolValidation::rejected(
            "getReserves response is too short",
        ));
    }
    Ok(())
}

async fn required_address_call(
    rpc_client: &dyn RpcClient,
    chain_id: u64,
    address: &str,
    selector: &str,
    name: &str,
) -> Result<String, PoolValidation> {
    let value = required_call(rpc_client, chain_id, address, selector, name).await?;
    abi_word_address(&value)
        .ok_or_else(|| PoolValidation::rejected(format!("{name} response is not an address")))
}

async fn required_call(
    rpc_client: &dyn RpcClient,
    chain_id: u64,
    address: &str,
    selector: &str,
    name: &str,
) -> Result<String, PoolValidation> {
    match rpc_client.eth_call(chain_id, address, selector, None).await {
        Ok(value) => Ok(value),
        Err(err) if is_deterministic_call_failure(&err.to_string()) => Err(
            PoolValidation::rejected(format!("required {name} call reverted: {err}")),
        ),
        Err(err) => Err(PoolValidation::error(format!(
            "required {name} call failed: {err}"
        ))),
    }
}

fn parse_pool_creation_event(log: &RawLog) -> LedgerResult<Option<PoolCreationEvent>> {
    let topic0 = log
        .topics
        .first()
        .map(|topic| topic.to_ascii_lowercase())
        .unwrap_or_default();
    let factory_address = normalize_evm_address(&log.address)?;
    let (variant, event_name, token0, token1, pool, fee, tick_spacing, stable) =
        match topic0.as_str() {
            V2_PAIR_CREATED_TOPIC if log.topics.len() >= 3 => {
                let words = data_words(&log.data)?;
                if words.is_empty() {
                    return Ok(None);
                }
                (
                    PoolVariant::V2Compatible,
                    "PairCreated",
                    topic_address(&log.topics[1]),
                    topic_address(&log.topics[2]),
                    word_address(words[0]),
                    None,
                    None,
                    None,
                )
            }
            V3_POOL_CREATED_TOPIC if log.topics.len() >= 4 => {
                let words = data_words(&log.data)?;
                if words.len() < 2 {
                    return Ok(None);
                }
                (
                    PoolVariant::V3Compatible,
                    "PoolCreated",
                    topic_address(&log.topics[1]),
                    topic_address(&log.topics[2]),
                    word_address(words[1]),
                    topic_u32(&log.topics[3]),
                    word_i24(words[0]),
                    None,
                )
            }
            SOLIDLY_PAIR_CREATED_TOPIC | SOLIDLY_POOL_CREATED_TOPIC if log.topics.len() >= 3 => {
                let words = data_words(&log.data)?;
                if words.len() < 2 {
                    return Ok(None);
                }
                (
                    PoolVariant::SolidlyCompatible,
                    if topic0 == SOLIDLY_PAIR_CREATED_TOPIC {
                        "PairCreated"
                    } else {
                        "PoolCreated"
                    },
                    topic_address(&log.topics[1]),
                    topic_address(&log.topics[2]),
                    word_address(words[1]),
                    None,
                    None,
                    word_bool(words[0]),
                )
            }
            _ => return Ok(None),
        };
    let (Some(token0_address), Some(token1_address), Some(pool_address)) = (token0, token1, pool)
    else {
        return Ok(None);
    };
    Ok(Some(PoolCreationEvent {
        variant,
        event_name: event_name.to_owned(),
        event_signature: topic0,
        factory_address,
        pool_address,
        token0_address,
        token1_address,
        fee,
        tick_spacing,
        stable,
        block_number: log.block_number,
        block_hash: log.block_hash.to_ascii_lowercase(),
        tx_hash: log.tx_hash.to_ascii_lowercase(),
        tx_index: log.tx_index,
        log_index: log.log_index,
        raw_topics: log.topics.clone(),
        raw_data: log.data.clone(),
    }))
}

async fn load_dataset_source(pool: &PgPool, source_key: &str) -> LedgerResult<DatasetSource> {
    let row = sqlx::query(
        r#"
        select source_key, display_name, source_kind, trust_score, owner, homepage, metadata
        from intelligence_sources
        where source_key = $1 and status = 'active'
        "#,
    )
    .bind(source_key)
    .fetch_optional(pool)
    .await
    .map_err(persistence_error)?
    .ok_or_else(|| LedgerError::NotFound(format!("intelligence source {source_key}")))?;
    Ok(DatasetSource {
        source_key: row.get("source_key"),
        display_name: row.get("display_name"),
        source_kind: row.get("source_kind"),
        trust_score: row.get::<f64, _>("trust_score") as f32,
        owner: row.get("owner"),
        homepage: row.get("homepage"),
        metadata: row.get::<Json<Value>, _>("metadata").0,
    })
}

fn protocol_key(event: &PoolCreationEvent) -> String {
    format!(
        "{}:{}",
        event.variant.protocol_family(),
        event.factory_address
    )
}

fn protocol_display_name(event: &PoolCreationEvent) -> String {
    format!(
        "{} factory {}",
        event.variant.as_str().replace('_', " "),
        short_address(&event.factory_address)
    )
}

fn discovery_label(
    label_key: &str,
    dimension: &str,
    description: &str,
    event: &PoolCreationEvent,
) -> DatasetLabel {
    DatasetLabel {
        label_key: label_key.to_owned(),
        dimension: dimension.to_owned(),
        exclusive_group: None,
        confidence: 0.90,
        description: Some(description.to_owned()),
        evidence: json!({"creation_event": event}),
    }
}

fn data_words(data: &str) -> LedgerResult<Vec<&str>> {
    let body = data.trim().strip_prefix("0x").unwrap_or(data.trim());
    if body.len() % 64 != 0 || !body.chars().all(|character| character.is_ascii_hexdigit()) {
        return Err(LedgerError::MalformedEvidence(
            "event data is not ABI-word-aligned hex".to_owned(),
        ));
    }
    Ok(body
        .as_bytes()
        .chunks(64)
        .filter_map(|chunk| std::str::from_utf8(chunk).ok())
        .collect())
}

fn topic_address(topic: &str) -> Option<String> {
    let body = topic.trim().strip_prefix("0x").unwrap_or(topic.trim());
    if body.len() != 64 || !body.chars().all(|character| character.is_ascii_hexdigit()) {
        return None;
    }
    normalize_evm_address(&format!("0x{}", body.get(24..)?)).ok()
}

fn word_address(word: &str) -> Option<String> {
    if word.len() != 64 {
        return None;
    }
    normalize_evm_address(&format!("0x{}", word.get(24..)?)).ok()
}

fn topic_u32(topic: &str) -> Option<u32> {
    let body = topic.trim().strip_prefix("0x").unwrap_or(topic.trim());
    u32::from_str_radix(body.get(body.len().saturating_sub(8)..)?, 16).ok()
}

fn word_i24(word: &str) -> Option<i32> {
    let value = u32::from_str_radix(word.get(58..64)?, 16).ok()?;
    Some(if value & 0x80_0000 != 0 {
        value as i32 - (1 << 24)
    } else {
        value as i32
    })
}

fn word_bool(word: &str) -> Option<bool> {
    word_as_u64(word).map(|value| value != 0)
}

fn word_as_u64(word: &str) -> Option<u64> {
    u64::from_str_radix(word.get(word.len().saturating_sub(16)..)?, 16).ok()
}

fn abi_word_address(raw: &str) -> Option<String> {
    let body = raw.trim().strip_prefix("0x").unwrap_or(raw.trim());
    word_address(body.get(..64)?)
}

fn abi_word_u32(raw: &str) -> Option<u32> {
    let body = raw.trim().strip_prefix("0x").unwrap_or(raw.trim());
    u32::from_str_radix(body.get(56..64)?, 16).ok()
}

fn abi_word_i24(raw: &str) -> Option<i32> {
    let body = raw.trim().strip_prefix("0x").unwrap_or(raw.trim());
    word_i24(body.get(..64)?)
}

fn abi_word_bool(raw: &str) -> Option<bool> {
    let body = raw.trim().strip_prefix("0x").unwrap_or(raw.trim());
    word_bool(body.get(..64)?)
}

fn abi_uint_text(word: &str) -> String {
    u128::from_str_radix(word, 16)
        .map(|value| value.to_string())
        .unwrap_or_else(|_| format!("0x{word}"))
}

fn encode_two_address_call(selector: &str, left: &str, right: &str) -> Option<String> {
    let selector = selector.strip_prefix("0x")?;
    let left = normalize_evm_address(left).ok()?;
    let right = normalize_evm_address(right).ok()?;
    Some(format!(
        "0x{selector}{:0>64}{:0>64}",
        left.trim_start_matches("0x"),
        right.trim_start_matches("0x")
    ))
}

fn normalize_evm_address(value: &str) -> LedgerResult<String> {
    let normalized = value.trim().to_ascii_lowercase();
    let body = normalized.strip_prefix("0x").ok_or_else(|| {
        LedgerError::MalformedEvidence(format!("address must start with 0x: {value}"))
    })?;
    if body.len() != 40 || !body.chars().all(|character| character.is_ascii_hexdigit()) {
        return Err(LedgerError::MalformedEvidence(format!(
            "invalid EVM address: {value}"
        )));
    }
    Ok(normalized)
}

fn is_empty_code(code: &str) -> bool {
    let body = code.trim().trim_start_matches("0x");
    body.is_empty() || body.chars().all(|character| character == '0')
}

fn code_hash(code: &str) -> String {
    let body = code.trim().trim_start_matches("0x");
    let bytes = hex::decode(body).unwrap_or_else(|_| body.as_bytes().to_vec());
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

fn is_deterministic_call_failure(error: &str) -> bool {
    let error = error.to_ascii_lowercase();
    error.contains("execution reverted")
        || error.contains("invalid opcode")
        || error.contains("revert")
}

fn short_address(address: &str) -> String {
    if address.len() <= 14 {
        return address.to_owned();
    }
    format!("{}…{}", &address[..8], &address[address.len() - 4..])
}

fn truncate_text(value: &str, max_characters: usize) -> String {
    value.chars().take(max_characters).collect()
}

fn persistence_error(error: impl std::fmt::Display) -> LedgerError {
    LedgerError::Persistence(error.to_string())
}

trait Pipe: Sized {
    fn pipe<T>(self, function: impl FnOnce(Self) -> T) -> T {
        function(self)
    }
}

impl<T> Pipe for T {}

#[cfg(test)]
mod tests {
    use super::*;

    fn word_address_for_test(address: &str) -> String {
        format!("{:0>64}", address.trim_start_matches("0x"))
    }

    fn word_u32(value: u32) -> String {
        format!("{value:064x}")
    }

    #[test]
    fn parses_v2_pair_created_event() {
        let token0 = "0x1111111111111111111111111111111111111111";
        let token1 = "0x2222222222222222222222222222222222222222";
        let pair = "0x3333333333333333333333333333333333333333";
        let log = RawLog {
            chain_id: 4663,
            block_number: 100,
            block_hash: format!("0x{}", "aa".repeat(32)),
            tx_hash: format!("0x{}", "bb".repeat(32)),
            tx_index: 1,
            log_index: 2,
            address: "0x4444444444444444444444444444444444444444".to_owned(),
            topics: vec![
                V2_PAIR_CREATED_TOPIC.to_owned(),
                format!("0x{}", word_address_for_test(token0)),
                format!("0x{}", word_address_for_test(token1)),
            ],
            data: format!("0x{}{}", word_address_for_test(pair), word_u32(7)),
            is_removed: 0,
        };
        let event = parse_pool_creation_event(&log).unwrap().unwrap();
        assert_eq!(event.variant, PoolVariant::V2Compatible);
        assert_eq!(event.pool_address, pair);
        assert_eq!(event.token0_address, token0);
        assert_eq!(event.token1_address, token1);
    }

    #[test]
    fn parses_v3_pool_created_event() {
        let token0 = "0x1111111111111111111111111111111111111111";
        let token1 = "0x2222222222222222222222222222222222222222";
        let pool = "0x3333333333333333333333333333333333333333";
        let log = RawLog {
            chain_id: 4663,
            block_number: 101,
            block_hash: format!("0x{}", "aa".repeat(32)),
            tx_hash: format!("0x{}", "bb".repeat(32)),
            tx_index: 3,
            log_index: 4,
            address: "0x4444444444444444444444444444444444444444".to_owned(),
            topics: vec![
                V3_POOL_CREATED_TOPIC.to_owned(),
                format!("0x{}", word_address_for_test(token0)),
                format!("0x{}", word_address_for_test(token1)),
                format!("0x{}", word_u32(10_000)),
            ],
            data: format!("0x{}{}", word_u32(200), word_address_for_test(pool)),
            is_removed: 0,
        };
        let event = parse_pool_creation_event(&log).unwrap().unwrap();
        assert_eq!(event.variant, PoolVariant::V3Compatible);
        assert_eq!(event.pool_address, pool);
        assert_eq!(event.fee, Some(10_000));
        assert_eq!(event.tick_spacing, Some(200));
    }

    #[test]
    fn decodes_negative_int24() {
        let word = format!("{}ffffc4", "f".repeat(58));
        assert_eq!(word_i24(&word), Some(-60));
    }

    #[test]
    fn encodes_factory_get_pair_call_without_address_constants() {
        let encoded = encode_two_address_call(
            GET_PAIR_SELECTOR,
            "0x1111111111111111111111111111111111111111",
            "0x2222222222222222222222222222222222222222",
        )
        .unwrap();
        assert_eq!(encoded.len(), 2 + 8 + 64 + 64);
        assert!(encoded.starts_with(GET_PAIR_SELECTOR));
    }

    #[test]
    fn protocol_identity_is_factory_scoped_without_brand_claim() {
        let event = PoolCreationEvent {
            variant: PoolVariant::V2Compatible,
            event_name: "PairCreated".to_owned(),
            event_signature: V2_PAIR_CREATED_TOPIC.to_owned(),
            factory_address: "0x5555555555555555555555555555555555555555".to_owned(),
            pool_address: "0x6666666666666666666666666666666666666666".to_owned(),
            token0_address: "0x7777777777777777777777777777777777777777".to_owned(),
            token1_address: "0x8888888888888888888888888888888888888888".to_owned(),
            fee: None,
            tick_spacing: None,
            stable: None,
            block_number: 1,
            block_hash: format!("0x{}", "aa".repeat(32)),
            tx_hash: format!("0x{}", "bb".repeat(32)),
            tx_index: 0,
            log_index: 0,
            raw_topics: Vec::new(),
            raw_data: "0x".to_owned(),
        };
        assert_eq!(
            protocol_key(&event),
            "uniswap_v2_compatible:0x5555555555555555555555555555555555555555"
        );
        assert!(
            !protocol_display_name(&event)
                .to_ascii_lowercase()
                .contains("uniswap")
        );
    }
}
