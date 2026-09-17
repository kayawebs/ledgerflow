use chrono::Utc;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;

use crate::core::errors::{LedgerError, LedgerResult};
use crate::indexer::models::{
    AddressTxDirection, AddressTxRef, CanonicalAddressObservation, CanonicalBlock,
    ChainReorganization, IndexedBlock, IndexerApiKeyRecord, IndexerApiKeyStatus,
    IndexerStorageStats, IndexerWorkerProgress, LazyIndexingRequest, RawLog, RawTransaction,
    normalize_address,
};

#[derive(Clone)]
pub struct ClickHouseClient {
    base_url: String,
    database: String,
    user: String,
    password: Option<String>,
    client: reqwest::Client,
}

impl ClickHouseClient {
    pub fn new(base_url: String, database: String, user: String, password: Option<String>) -> Self {
        Self {
            base_url,
            database,
            user,
            password,
            client: reqwest::Client::new(),
        }
    }

    pub async fn bootstrap(&self) -> LedgerResult<()> {
        self.execute(&format!("CREATE DATABASE IF NOT EXISTS {}", self.database))
            .await?;
        self.execute(&format!(
            r#"
            CREATE TABLE IF NOT EXISTS {}.address_tx_refs (
              chain_id UInt32,
              address String,
              tx_hash String,
              block_number UInt32,
              tx_index UInt32,
              direction Enum8('from' = 1, 'to' = 2),
              from_address Nullable(String),
              to_address Nullable(String),
              inserted_at DateTime DEFAULT now()
            )
            ENGINE = ReplacingMergeTree(inserted_at)
            PARTITION BY intDiv(block_number, 1000000)
            ORDER BY (chain_id, address, block_number, tx_index, direction, tx_hash)
            "#,
            self.database
        ))
        .await?;
        self.execute(&format!(
            "ALTER TABLE {}.address_tx_refs ADD COLUMN IF NOT EXISTS from_address Nullable(String)",
            self.database
        ))
        .await?;
        self.execute(&format!(
            "ALTER TABLE {}.address_tx_refs ADD COLUMN IF NOT EXISTS to_address Nullable(String)",
            self.database
        ))
        .await?;
        self.execute(&format!(
            "ALTER TABLE {}.address_tx_refs ADD COLUMN IF NOT EXISTS block_hash String DEFAULT '' AFTER block_number",
            self.database
        ))
        .await?;
        self.execute(&format!(
            r#"
            CREATE TABLE IF NOT EXISTS {}.indexer_progress (
              chain_id UInt32,
              worker_id String,
              last_indexed_block UInt32,
              updated_at DateTime DEFAULT now()
            )
            ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY (chain_id, worker_id)
            "#,
            self.database
        ))
        .await?;
        self.execute(&format!(
            r#"
            CREATE TABLE IF NOT EXISTS {}.indexer_worker_progress (
              chain_id UInt32,
              worker_id String,
              worker_kind LowCardinality(String),
              last_indexed_block UInt32,
              last_indexed_hash String,
              range_start UInt32,
              range_end Nullable(UInt32),
              rpc_url String,
              retry_count UInt32,
              last_error String,
              last_success_at Nullable(DateTime64(3)),
              head_block Nullable(UInt32),
              lag_blocks Nullable(UInt32),
              updated_at DateTime64(3) DEFAULT now64(3)
            )
            ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY (chain_id, worker_id)
            "#,
            self.database
        ))
        .await?;
        self.execute(&format!(
            r#"
            CREATE TABLE IF NOT EXISTS {}.canonical_blocks (
              chain_id UInt32,
              block_number UInt32,
              block_hash String,
              parent_hash String,
              timestamp UInt64,
              l1_block_number Nullable(UInt64),
              finality_state LowCardinality(String),
              is_canonical UInt8,
              worker_id LowCardinality(String),
              observed_at DateTime64(3) DEFAULT now64(3)
            )
            ENGINE = ReplacingMergeTree(observed_at)
            PARTITION BY intDiv(block_number, 1000000)
            ORDER BY (chain_id, block_number, block_hash)
            "#,
            self.database
        ))
        .await?;
        self.execute(&format!(
            r#"
            CREATE TABLE IF NOT EXISTS {}.transactions_raw (
              chain_id UInt32,
              block_number UInt32,
              block_hash String,
              tx_index UInt32,
              tx_hash String,
              from_address String,
              to_address Nullable(String),
              nonce UInt64,
              value String,
              input String CODEC(ZSTD(3)),
              status Nullable(UInt8),
              gas_used Nullable(UInt64),
              effective_gas_price Nullable(String),
              contract_address Nullable(String),
              observed_at DateTime64(3) DEFAULT now64(3)
            )
            ENGINE = ReplacingMergeTree(observed_at)
            PARTITION BY intDiv(block_number, 1000000)
            ORDER BY (chain_id, block_number, block_hash, tx_index, tx_hash)
            "#,
            self.database
        ))
        .await?;
        self.execute(&format!(
            r#"
            CREATE TABLE IF NOT EXISTS {}.logs_raw (
              chain_id UInt32,
              block_number UInt32,
              block_hash String,
              tx_hash String,
              tx_index UInt32,
              log_index UInt32,
              address String,
              topics Array(String),
              data String CODEC(ZSTD(3)),
              is_removed UInt8,
              observed_at DateTime64(3) DEFAULT now64(3)
            )
            ENGINE = ReplacingMergeTree(observed_at)
            PARTITION BY intDiv(block_number, 1000000)
            ORDER BY (chain_id, block_number, block_hash, tx_index, log_index, tx_hash)
            "#,
            self.database
        ))
        .await?;
        self.execute(&format!(
            r#"
            CREATE TABLE IF NOT EXISTS {}.traces_raw (
              chain_id UInt32,
              block_number UInt32,
              block_hash String,
              tx_hash String,
              trace_id String,
              trace_type LowCardinality(String),
              selection_reason LowCardinality(String),
              payload String CODEC(ZSTD(3)),
              observed_at DateTime64(3) DEFAULT now64(3)
            )
            ENGINE = ReplacingMergeTree(observed_at)
            PARTITION BY intDiv(block_number, 1000000)
            ORDER BY (chain_id, block_number, block_hash, tx_hash, trace_id)
            "#,
            self.database
        ))
        .await?;
        self.execute(&format!(
            r#"
            CREATE TABLE IF NOT EXISTS {}.chain_reorganizations (
              event_id String,
              chain_id UInt32,
              worker_id LowCardinality(String),
              previous_tip UInt32,
              common_ancestor UInt32,
              new_head UInt32,
              depth UInt32,
              detected_at DateTime64(3) DEFAULT now64(3)
            )
            ENGINE = ReplacingMergeTree(detected_at)
            ORDER BY (chain_id, event_id)
            "#,
            self.database
        ))
        .await?;
        self.execute(&format!(
            r#"
            CREATE VIEW IF NOT EXISTS {}.transactions_canonical AS
            SELECT facts.*
            FROM {}.transactions_raw AS facts FINAL
            INNER JOIN (
              SELECT chain_id, block_number, block_hash
              FROM {}.canonical_blocks FINAL
              WHERE is_canonical = 1
            ) AS canonical
            USING (chain_id, block_number, block_hash)
            "#,
            self.database, self.database, self.database
        ))
        .await?;
        self.execute(&format!(
            r#"
            CREATE VIEW IF NOT EXISTS {}.logs_canonical AS
            SELECT facts.*
            FROM {}.logs_raw AS facts FINAL
            INNER JOIN (
              SELECT chain_id, block_number, block_hash
              FROM {}.canonical_blocks FINAL
              WHERE is_canonical = 1
            ) AS canonical
            USING (chain_id, block_number, block_hash)
            "#,
            self.database, self.database, self.database
        ))
        .await?;
        self.execute(&format!(
            r#"
            CREATE VIEW IF NOT EXISTS {}.traces_canonical AS
            SELECT facts.*
            FROM {}.traces_raw AS facts FINAL
            INNER JOIN (
              SELECT chain_id, block_number, block_hash
              FROM {}.canonical_blocks FINAL
              WHERE is_canonical = 1
            ) AS canonical
            USING (chain_id, block_number, block_hash)
            "#,
            self.database, self.database, self.database
        ))
        .await?;
        self.execute(&format!(
            r#"
            CREATE TABLE IF NOT EXISTS {}.indexed_blocks (
              chain_id UInt32,
              block_number UInt32,
              block_hash String,
              indexed_at DateTime64(3) DEFAULT now64(3)
            )
            ENGINE = ReplacingMergeTree(indexed_at)
            PARTITION BY intDiv(block_number, 1000000)
            ORDER BY (chain_id, block_number)
            "#,
            self.database
        ))
        .await?;
        self.execute(&format!(
            r#"
            CREATE TABLE IF NOT EXISTS {}.indexing_requests (
              request_id String,
              chain_id UInt32,
              requested_address String,
              from_block UInt32,
              to_block UInt32,
              next_block UInt32,
              status LowCardinality(String),
              last_error String,
              updated_at DateTime64(3) DEFAULT now64(3)
            )
            ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY (chain_id, request_id)
            "#,
            self.database
        ))
        .await?;
        self.execute(&format!(
            r#"
            CREATE TABLE IF NOT EXISTS {}.api_keys (
              key_id String,
              token_hash String,
              owner_user_id String,
              owner_email String,
              label String,
              token_prefix String,
              status Enum8('active' = 1, 'revoked' = 2),
              created_at DateTime,
              updated_at DateTime,
              last_used_at Nullable(DateTime)
            )
            ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY (owner_user_id, key_id)
            "#,
            self.database
        ))
        .await?;
        Ok(())
    }

    pub async fn insert_refs(&self, refs: &[AddressTxRef]) -> LedgerResult<()> {
        if refs.is_empty() {
            return Ok(());
        }
        let mut body = format!(
            "INSERT INTO {}.address_tx_refs FORMAT JSONEachRow\n",
            self.database
        );
        for item in refs {
            body.push_str(
                &serde_json::to_string(item)
                    .map_err(|err| LedgerError::Persistence(err.to_string()))?,
            );
            body.push('\n');
        }
        self.raw_post(&body)
            .await
            .map_err(|err| LedgerError::Persistence(err.to_string()))
    }

    pub async fn insert_canonical_blocks(&self, blocks: &[CanonicalBlock]) -> LedgerResult<()> {
        self.insert_json_rows("canonical_blocks", blocks).await
    }

    pub async fn insert_raw_transactions(
        &self,
        transactions: &[RawTransaction],
    ) -> LedgerResult<()> {
        self.insert_json_rows("transactions_raw", transactions)
            .await
    }

    pub async fn insert_raw_logs(&self, logs: &[RawLog]) -> LedgerResult<()> {
        self.insert_json_rows("logs_raw", logs).await
    }

    pub async fn get_canonical_block(
        &self,
        chain_id: u32,
        block_number: u32,
    ) -> LedgerResult<Option<CanonicalBlock>> {
        let payload = self
            .query_json(&format!(
                r#"
                SELECT chain_id, block_number, block_hash, parent_hash, timestamp,
                       l1_block_number, finality_state, is_canonical, worker_id
                FROM {}.canonical_blocks FINAL
                WHERE chain_id = {}
                  AND block_number = {}
                  AND is_canonical = 1
                ORDER BY observed_at DESC
                LIMIT 1
                FORMAT JSON
                "#,
                self.database, chain_id, block_number
            ))
            .await?;
        deserialize_first_row(&payload)
    }

    pub async fn canonical_blocks_in_range(
        &self,
        chain_id: u32,
        from_block: u32,
        to_block: u32,
    ) -> LedgerResult<Vec<CanonicalBlock>> {
        let payload = self
            .query_json(&format!(
                r#"
                SELECT chain_id, block_number, block_hash, parent_hash, timestamp,
                       l1_block_number, finality_state, is_canonical, worker_id
                FROM {}.canonical_blocks FINAL
                WHERE chain_id = {}
                  AND block_number BETWEEN {} AND {}
                  AND is_canonical = 1
                ORDER BY block_number
                FORMAT JSON
                "#,
                self.database, chain_id, from_block, to_block
            ))
            .await?;
        deserialize_rows(&payload)
    }

    pub async fn canonical_block_bounds(&self, chain_id: u32) -> LedgerResult<Option<(u32, u32)>> {
        let payload = self
            .query_json(&format!(
                r#"
                SELECT count() AS block_count,
                       min(block_number) AS min_block,
                       max(block_number) AS max_block
                FROM {}.canonical_blocks FINAL
                WHERE chain_id = {}
                  AND is_canonical = 1
                FORMAT JSON
                "#,
                self.database, chain_id
            ))
            .await?;
        let Some(row) = payload["data"].as_array().and_then(|rows| rows.first()) else {
            return Ok(None);
        };
        if row.get("block_count").and_then(value_as_u64) == Some(0) {
            return Ok(None);
        }
        let Some(min_block) = row.get("min_block").and_then(value_as_u64) else {
            return Ok(None);
        };
        let Some(max_block) = row.get("max_block").and_then(value_as_u64) else {
            return Ok(None);
        };
        Ok(Some((min_block as u32, max_block as u32)))
    }

    pub async fn canonical_logs_by_topics(
        &self,
        chain_id: u32,
        from_block: u32,
        to_block: u32,
        topic0s: &[&str],
    ) -> LedgerResult<Vec<RawLog>> {
        if from_block > to_block || topic0s.is_empty() {
            return Ok(Vec::new());
        }
        for topic in topic0s {
            let body = topic.strip_prefix("0x").ok_or_else(|| {
                LedgerError::MalformedEvidence(format!("event topic must start with 0x: {topic}"))
            })?;
            if body.len() != 64 || !body.chars().all(|character| character.is_ascii_hexdigit()) {
                return Err(LedgerError::MalformedEvidence(format!(
                    "invalid event topic: {topic}"
                )));
            }
        }
        let topics = topic0s
            .iter()
            .map(|topic| format!("'{}'", topic.to_ascii_lowercase()))
            .collect::<Vec<_>>()
            .join(",");
        let payload = self
            .query_json(&format!(
                r#"
                SELECT chain_id, block_number, block_hash, tx_hash, tx_index,
                       log_index, address, topics, data, is_removed
                FROM {}.logs_canonical
                WHERE chain_id = {}
                  AND block_number BETWEEN {} AND {}
                  AND length(topics) > 0
                  AND lower(topics[1]) IN ({})
                ORDER BY block_number, tx_index, log_index
                FORMAT JSON
                "#,
                self.database, chain_id, from_block, to_block, topics
            ))
            .await?;
        deserialize_rows(&payload)
    }

    pub async fn canonical_address_observations(
        &self,
        chain_id: u32,
        from_block: u32,
        to_block: u32,
    ) -> LedgerResult<Vec<CanonicalAddressObservation>> {
        if from_block > to_block {
            return Ok(Vec::new());
        }
        let payload = self
            .query_json(&format!(
                r#"
                SELECT
                  chain_id,
                  address,
                  observation_role,
                  min(block_number) AS first_seen_block,
                  max(block_number) AS last_seen_block,
                  count() AS observation_count,
                  argMin(block_hash, tuple(block_number, tx_index, coalesce(log_index, 0))) AS sample_block_hash,
                  argMin(tx_hash, tuple(block_number, tx_index, coalesce(log_index, 0))) AS sample_tx_hash,
                  argMin(log_index, tuple(block_number, tx_index, coalesce(log_index, 0))) AS sample_log_index
                FROM (
                  SELECT
                    chain_id, block_number, block_hash, tx_hash, tx_index,
                    CAST(NULL AS Nullable(UInt32)) AS log_index,
                    lower(from_address) AS address,
                    'transaction_sender' AS observation_role
                  FROM {}.transactions_canonical
                  WHERE chain_id = {} AND block_number BETWEEN {} AND {}

                  UNION ALL

                  SELECT
                    chain_id, block_number, block_hash, tx_hash, tx_index,
                    CAST(NULL AS Nullable(UInt32)) AS log_index,
                    lower(assumeNotNull(to_address)) AS address,
                    'transaction_recipient' AS observation_role
                  FROM {}.transactions_canonical
                  WHERE chain_id = {} AND block_number BETWEEN {} AND {}
                    AND isNotNull(to_address) AND assumeNotNull(to_address) != ''

                  UNION ALL

                  SELECT
                    chain_id, block_number, block_hash, tx_hash, tx_index,
                    CAST(NULL AS Nullable(UInt32)) AS log_index,
                    lower(assumeNotNull(contract_address)) AS address,
                    'contract_created' AS observation_role
                  FROM {}.transactions_canonical
                  WHERE chain_id = {} AND block_number BETWEEN {} AND {}
                    AND isNotNull(contract_address) AND assumeNotNull(contract_address) != ''

                  UNION ALL

                  SELECT
                    chain_id, block_number, block_hash, tx_hash, tx_index,
                    toNullable(log_index) AS log_index,
                    lower(address) AS address,
                    'log_emitter' AS observation_role
                  FROM {}.logs_canonical
                  WHERE chain_id = {} AND block_number BETWEEN {} AND {}

                  UNION ALL

                  SELECT
                    chain_id, block_number, block_hash, tx_hash, tx_index,
                    toNullable(log_index) AS log_index,
                    concat('0x', substring(lower(topic), 27, 40)) AS address,
                    'indexed_event_address' AS observation_role
                  FROM {}.logs_canonical
                  ARRAY JOIN arrayEnumerate(topics) AS topic_index, topics AS topic
                  WHERE chain_id = {} AND block_number BETWEEN {} AND {}
                    AND topic_index IN (2, 3)
                    AND lower(topics[1]) IN (
                      '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef',
                      '0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925',
                      '0x0226a2f5c1ae0e071aeec3d4ebafcefdc5c549be11f40ed27e76e802acccf374',
                      '0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9',
                      '0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118',
                      '0xc4805696c66d7cf352fc1d6bb633ad5ee82f6cb577c453024b6e0eb8306c6fc9',
                      '0x2128d88d14c80cb081c1252a5acff7a264671bf199ce226b53788fb26065005e'
                    )
                    AND length(topic) = 66
                    AND substring(lower(topic), 1, 2) = '0x'
                    AND substring(lower(topic), 3, 24) = repeat('0', 24)
                    AND substring(lower(topic), 27, 40) != repeat('0', 40)
                ) observations
                WHERE length(address) = 42
                  AND address != '0x0000000000000000000000000000000000000000'
                GROUP BY chain_id, address, observation_role
                ORDER BY address, observation_role
                FORMAT JSON
                "#,
                self.database,
                chain_id,
                from_block,
                to_block,
                self.database,
                chain_id,
                from_block,
                to_block,
                self.database,
                chain_id,
                from_block,
                to_block,
                self.database,
                chain_id,
                from_block,
                to_block,
                self.database,
                chain_id,
                from_block,
                to_block,
            ))
            .await?;
        deserialize_rows(&payload)
    }

    pub async fn mark_canonical_range_orphaned(
        &self,
        chain_id: u32,
        from_block: u32,
        to_block: u32,
    ) -> LedgerResult<u64> {
        if from_block > to_block {
            return Ok(0);
        }
        let mut blocks = self
            .canonical_blocks_in_range(chain_id, from_block, to_block)
            .await?;
        for block in &mut blocks {
            block.is_canonical = 0;
            block.finality_state = "orphaned".to_owned();
        }
        self.insert_canonical_blocks(&blocks).await?;
        Ok(blocks.len() as u64)
    }

    pub async fn insert_reorganization(
        &self,
        reorganization: &ChainReorganization,
    ) -> LedgerResult<()> {
        self.insert_json_rows(
            "chain_reorganizations",
            std::slice::from_ref(reorganization),
        )
        .await
    }

    pub async fn get_progress(&self, chain_id: u32, worker_id: &str) -> LedgerResult<Option<u64>> {
        if let Some(progress) = self.get_worker_progress(chain_id, worker_id).await? {
            return Ok(Some(u64::from(progress.last_indexed_block)));
        }
        self.get_legacy_progress(chain_id, worker_id).await
    }

    pub async fn get_legacy_progress(
        &self,
        chain_id: u32,
        worker_id: &str,
    ) -> LedgerResult<Option<u64>> {
        let worker_id = escape_sql(worker_id);
        let payload = self
            .query_json(&format!(
                r#"
                SELECT last_indexed_block
                FROM {}.indexer_progress FINAL
                WHERE chain_id = {}
                  AND worker_id = '{}'
                ORDER BY updated_at DESC
                LIMIT 1
                FORMAT JSON
                "#,
                self.database, chain_id, worker_id
            ))
            .await?;
        Ok(payload["data"]
            .as_array()
            .and_then(|rows| rows.first())
            .and_then(|row| row.get("last_indexed_block"))
            .and_then(Value::as_u64))
    }

    pub async fn get_worker_progress(
        &self,
        chain_id: u32,
        worker_id: &str,
    ) -> LedgerResult<Option<IndexerWorkerProgress>> {
        let payload = self
            .query_json(&format!(
                r#"
                SELECT chain_id, worker_id, worker_kind, last_indexed_block,
                       last_indexed_hash, range_start, range_end, rpc_url,
                       retry_count, last_error, toString(last_success_at) AS last_success_at,
                       head_block, lag_blocks
                FROM {}.indexer_worker_progress FINAL
                WHERE chain_id = {}
                  AND worker_id = '{}'
                LIMIT 1
                FORMAT JSON
                "#,
                self.database,
                chain_id,
                escape_sql(worker_id)
            ))
            .await?;
        deserialize_first_row(&payload)
    }

    pub async fn list_worker_progress(
        &self,
        chain_id: u32,
    ) -> LedgerResult<Vec<IndexerWorkerProgress>> {
        let payload = self
            .query_json(&format!(
                r#"
                SELECT chain_id, worker_id, worker_kind, last_indexed_block,
                       last_indexed_hash, range_start, range_end, rpc_url,
                       retry_count, last_error, toString(last_success_at) AS last_success_at,
                       head_block, lag_blocks
                FROM {}.indexer_worker_progress FINAL
                WHERE chain_id = {}
                ORDER BY worker_kind, worker_id
                FORMAT JSON
                "#,
                self.database, chain_id
            ))
            .await?;
        deserialize_rows(&payload)
    }

    pub async fn upsert_worker_progress(
        &self,
        progress: &IndexerWorkerProgress,
    ) -> LedgerResult<()> {
        let mut row = serde_json::to_value(progress)
            .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        if let Some(last_success_at) = row.get_mut("last_success_at")
            && last_success_at.is_null()
        {
            *last_success_at = Value::Null;
        }
        let body = format!(
            "INSERT INTO {}.indexer_worker_progress FORMAT JSONEachRow\n{}",
            self.database,
            serde_json::to_string(&row).map_err(|err| LedgerError::Persistence(err.to_string()))?
        );
        self.raw_post(&body)
            .await
            .map_err(|err| LedgerError::Persistence(err.to_string()))
    }

    pub async fn storage_stats(&self, chain_id: u32) -> LedgerResult<IndexerStorageStats> {
        let payload = self
            .query_json(&format!(
                r#"
                SELECT
                  (SELECT count() FROM {}.canonical_blocks FINAL WHERE chain_id = {} AND is_canonical = 1) AS canonical_block_count,
                  (SELECT count() FROM {}.transactions_raw FINAL WHERE chain_id = {}) AS raw_transaction_count,
                  (SELECT count() FROM {}.logs_raw FINAL WHERE chain_id = {}) AS raw_log_count,
                  (SELECT count() FROM {}.traces_raw FINAL WHERE chain_id = {}) AS raw_trace_count,
                  (SELECT count() FROM {}.canonical_blocks FINAL WHERE chain_id = {} AND is_canonical = 0) AS orphaned_block_count,
                  (SELECT count() FROM {}.chain_reorganizations FINAL WHERE chain_id = {}) AS reorg_count
                FORMAT JSON
                "#,
                self.database,
                chain_id,
                self.database,
                chain_id,
                self.database,
                chain_id,
                self.database,
                chain_id,
                self.database,
                chain_id,
                self.database,
                chain_id
            ))
            .await?;
        Ok(deserialize_first_row(&payload)?.unwrap_or_default())
    }

    pub async fn set_progress(
        &self,
        chain_id: u32,
        worker_id: &str,
        last_indexed_block: u64,
    ) -> LedgerResult<()> {
        let body = format!(
            "INSERT INTO {}.indexer_progress FORMAT JSONEachRow\n{}",
            self.database,
            serde_json::to_string(&serde_json::json!({
                "chain_id": chain_id,
                "worker_id": worker_id,
                "last_indexed_block": last_indexed_block as u32,
            }))
            .map_err(|err| LedgerError::Persistence(err.to_string()))?
        );
        self.raw_post(&body)
            .await
            .map_err(|err| LedgerError::Persistence(err.to_string()))
    }

    pub async fn insert_indexed_blocks(&self, blocks: &[IndexedBlock]) -> LedgerResult<()> {
        if blocks.is_empty() {
            return Ok(());
        }
        let mut body = format!(
            "INSERT INTO {}.indexed_blocks FORMAT JSONEachRow\n",
            self.database
        );
        for block in blocks {
            body.push_str(
                &serde_json::to_string(block)
                    .map_err(|err| LedgerError::Persistence(err.to_string()))?,
            );
            body.push('\n');
        }
        self.raw_post(&body)
            .await
            .map_err(|err| LedgerError::Persistence(err.to_string()))
    }

    pub async fn indexed_block_numbers(
        &self,
        chain_id: u32,
        from_block: u32,
        to_block: u32,
    ) -> LedgerResult<Vec<u32>> {
        let payload = self
            .query_json(&format!(
                r#"
                SELECT block_number
                FROM {}.indexed_blocks FINAL
                WHERE chain_id = {}
                  AND block_number BETWEEN {} AND {}
                ORDER BY block_number
                FORMAT JSON
                "#,
                self.database, chain_id, from_block, to_block
            ))
            .await?;
        Ok(payload["data"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|row| {
                row.get("block_number")
                    .and_then(value_as_u64)
                    .and_then(|value| u32::try_from(value).ok())
            })
            .collect())
    }

    /// Returns blocks whose canonical transaction rows still lack receipt
    /// fields. A block may already be present in `indexed_blocks` because the
    /// lightweight history path indexed its transaction envelope only; that
    /// must not make a receipt/log backfill request look complete.
    pub async fn block_numbers_missing_receipts(
        &self,
        chain_id: u32,
        from_block: u32,
        to_block: u32,
    ) -> LedgerResult<Vec<u32>> {
        let payload = self
            .query_json(&format!(
                r#"
                SELECT block_number
                FROM {}.transactions_canonical
                WHERE chain_id = {}
                  AND block_number BETWEEN {} AND {}
                GROUP BY block_number
                HAVING countIf(status IS NULL) > 0
                ORDER BY block_number
                FORMAT JSON
                "#,
                self.database, chain_id, from_block, to_block
            ))
            .await?;
        Ok(payload["data"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|row| {
                row.get("block_number")
                    .and_then(value_as_u64)
                    .and_then(|value| u32::try_from(value).ok())
            })
            .collect())
    }

    pub async fn count_indexed_blocks(
        &self,
        chain_id: u32,
        from_block: u32,
        to_block: u32,
        excluded_range: Option<(u32, u32)>,
    ) -> LedgerResult<u64> {
        let exclusion = excluded_range
            .map(|(start, end)| format!("AND NOT (block_number BETWEEN {start} AND {end})"))
            .unwrap_or_default();
        let payload = self
            .query_json(&format!(
                r#"
                SELECT count() AS indexed_block_count
                FROM {}.indexed_blocks FINAL
                WHERE chain_id = {}
                  AND block_number BETWEEN {} AND {}
                  {}
                FORMAT JSON
                "#,
                self.database, chain_id, from_block, to_block, exclusion
            ))
            .await?;
        Ok(payload["data"]
            .as_array()
            .and_then(|rows| rows.first())
            .and_then(|row| row.get("indexed_block_count"))
            .and_then(value_as_u64)
            .unwrap_or(0))
    }

    pub async fn max_indexed_block(&self, chain_id: u32) -> LedgerResult<Option<u64>> {
        let payload = self
            .query_json(&format!(
                r#"
                SELECT max(block_number) AS max_indexed_block
                FROM {}.indexed_blocks FINAL
                WHERE chain_id = {}
                FORMAT JSON
                "#,
                self.database, chain_id
            ))
            .await?;
        Ok(payload["data"]
            .as_array()
            .and_then(|rows| rows.first())
            .and_then(|row| row.get("max_indexed_block"))
            .and_then(value_as_u64))
    }

    pub async fn get_indexing_request(
        &self,
        chain_id: u32,
        request_id: &str,
    ) -> LedgerResult<Option<LazyIndexingRequest>> {
        let payload = self
            .query_json(&format!(
                r#"
                SELECT request_id, chain_id, requested_address, from_block, to_block,
                       next_block, status, last_error
                FROM {}.indexing_requests FINAL
                WHERE chain_id = {}
                  AND request_id = '{}'
                LIMIT 1
                FORMAT JSON
                "#,
                self.database,
                chain_id,
                escape_sql(request_id)
            ))
            .await?;
        payload["data"]
            .as_array()
            .and_then(|rows| rows.first())
            .cloned()
            .map(|item| {
                serde_json::from_value(item)
                    .map_err(|err| LedgerError::Persistence(err.to_string()))
            })
            .transpose()
    }

    pub async fn next_indexing_request(
        &self,
        chain_id: u32,
    ) -> LedgerResult<Option<LazyIndexingRequest>> {
        let payload = self
            .query_json(&format!(
                r#"
                SELECT request_id, chain_id, requested_address, from_block, to_block,
                       next_block, status, last_error
                FROM {}.indexing_requests FINAL
                WHERE chain_id = {}
                  AND status IN ('queued', 'running')
                ORDER BY updated_at ASC, request_id ASC
                LIMIT 1
                FORMAT JSON
                "#,
                self.database, chain_id
            ))
            .await?;
        payload["data"]
            .as_array()
            .and_then(|rows| rows.first())
            .cloned()
            .map(|item| {
                serde_json::from_value(item)
                    .map_err(|err| LedgerError::Persistence(err.to_string()))
            })
            .transpose()
    }

    pub async fn upsert_indexing_request(&self, request: &LazyIndexingRequest) -> LedgerResult<()> {
        let body = format!(
            "INSERT INTO {}.indexing_requests FORMAT JSONEachRow\n{}",
            self.database,
            serde_json::to_string(request)
                .map_err(|err| LedgerError::Persistence(err.to_string()))?
        );
        self.raw_post(&body)
            .await
            .map_err(|err| LedgerError::Persistence(err.to_string()))
    }

    pub async fn count_pending_indexing_requests(&self, chain_id: u32) -> LedgerResult<u64> {
        let payload = self
            .query_json(&format!(
                r#"
                SELECT count() AS pending_request_count
                FROM {}.indexing_requests FINAL
                WHERE chain_id = {}
                  AND status IN ('queued', 'running')
                FORMAT JSON
                "#,
                self.database, chain_id
            ))
            .await?;
        Ok(payload["data"]
            .as_array()
            .and_then(|rows| rows.first())
            .and_then(|row| row.get("pending_request_count"))
            .and_then(value_as_u64)
            .unwrap_or(0))
    }

    pub async fn query_address_refs(
        &self,
        chain_id: u32,
        address: &str,
        limit: u32,
        from_block: Option<u32>,
        to_block: Option<u32>,
        direction: Option<AddressTxDirection>,
    ) -> LedgerResult<Vec<AddressTxRef>> {
        let address = normalize_address(address)?;
        let conditions = build_conditions(chain_id, &address, from_block, to_block, direction);
        let payload = self
            .query_json(&format!(
                r#"
                SELECT refs.chain_id AS chain_id, refs.address AS address,
                       refs.tx_hash AS tx_hash, refs.block_number AS block_number,
                       refs.block_hash AS block_hash, refs.tx_index AS tx_index,
                       refs.direction AS direction, refs.from_address AS from_address,
                       refs.to_address AS to_address
                FROM {}.address_tx_refs AS refs FINAL
                LEFT JOIN (
                  SELECT chain_id, block_number, block_hash, is_canonical
                  FROM {}.canonical_blocks FINAL
                  WHERE chain_id = {} AND is_canonical = 1
                ) AS canonical
                  ON canonical.chain_id = refs.chain_id
                 AND canonical.block_number = refs.block_number
                 AND canonical.block_hash = refs.block_hash
                WHERE {}
                  AND (refs.block_hash = '' OR canonical.is_canonical = 1)
                ORDER BY refs.block_number DESC, refs.tx_index DESC, refs.direction ASC
                LIMIT {}
                FORMAT JSON
                "#,
                self.database,
                self.database,
                chain_id,
                conditions.join(" AND "),
                limit
            ))
            .await?;
        payload["data"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|item| {
                serde_json::from_value(item)
                    .map_err(|err| LedgerError::Persistence(err.to_string()))
            })
            .collect()
    }

    pub async fn query_address_hashes(
        &self,
        chain_id: u32,
        address: &str,
        limit: u32,
        from_block: Option<u32>,
        to_block: Option<u32>,
        direction: Option<AddressTxDirection>,
    ) -> LedgerResult<Vec<String>> {
        let address = normalize_address(address)?;
        let conditions = build_conditions(chain_id, &address, from_block, to_block, direction);
        let payload = self
            .query_json(&format!(
                r#"
                SELECT refs.tx_hash AS tx_hash,
                       max(refs.block_number) AS block_number,
                       max(refs.tx_index) AS tx_index
                FROM {}.address_tx_refs AS refs FINAL
                LEFT JOIN (
                  SELECT chain_id, block_number, block_hash, is_canonical
                  FROM {}.canonical_blocks FINAL
                  WHERE chain_id = {} AND is_canonical = 1
                ) AS canonical
                  ON canonical.chain_id = refs.chain_id
                 AND canonical.block_number = refs.block_number
                 AND canonical.block_hash = refs.block_hash
                WHERE {}
                  AND (refs.block_hash = '' OR canonical.is_canonical = 1)
                GROUP BY refs.tx_hash
                ORDER BY block_number DESC, tx_index DESC
                LIMIT {}
                FORMAT JSON
                "#,
                self.database,
                self.database,
                chain_id,
                conditions.join(" AND "),
                limit
            ))
            .await?;
        Ok(payload["data"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|row| {
                row.get("tx_hash")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            })
            .collect())
    }

    pub async fn query_refs_missing_endpoints(
        &self,
        chain_id: u32,
        limit: u32,
    ) -> LedgerResult<Vec<AddressTxRef>> {
        let payload = self
            .query_json(&format!(
                r#"
                SELECT refs.chain_id AS chain_id, refs.address AS address,
                       refs.tx_hash AS tx_hash, refs.block_number AS block_number,
                       refs.block_hash AS block_hash, refs.tx_index AS tx_index,
                       refs.direction AS direction, refs.from_address AS from_address,
                       refs.to_address AS to_address
                FROM {}.address_tx_refs AS refs FINAL
                LEFT JOIN (
                  SELECT chain_id, block_number, block_hash, is_canonical
                  FROM {}.canonical_blocks FINAL
                  WHERE chain_id = {} AND is_canonical = 1
                ) AS canonical
                  ON canonical.chain_id = refs.chain_id
                 AND canonical.block_number = refs.block_number
                 AND canonical.block_hash = refs.block_hash
                WHERE refs.chain_id = {}
                  AND isNull(refs.from_address)
                  AND (refs.block_hash = '' OR canonical.is_canonical = 1)
                ORDER BY refs.block_number ASC, refs.tx_index ASC, refs.direction ASC, refs.tx_hash ASC
                LIMIT {}
                FORMAT JSON
                "#,
                self.database, self.database, chain_id, chain_id, limit
            ))
            .await?;
        payload["data"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|item| {
                serde_json::from_value(item)
                    .map_err(|err| LedgerError::Persistence(err.to_string()))
            })
            .collect()
    }

    pub async fn count_refs_missing_endpoints(&self, chain_id: u32) -> LedgerResult<u64> {
        let payload = self
            .query_json(&format!(
                r#"
                SELECT count() AS pending_rows
                FROM {}.address_tx_refs AS refs FINAL
                LEFT JOIN (
                  SELECT chain_id, block_number, block_hash, is_canonical
                  FROM {}.canonical_blocks FINAL
                  WHERE chain_id = {} AND is_canonical = 1
                ) AS canonical
                  ON canonical.chain_id = refs.chain_id
                 AND canonical.block_number = refs.block_number
                 AND canonical.block_hash = refs.block_hash
                WHERE refs.chain_id = {}
                  AND isNull(refs.from_address)
                  AND (refs.block_hash = '' OR canonical.is_canonical = 1)
                FORMAT JSON
                "#,
                self.database, self.database, chain_id, chain_id
            ))
            .await?;
        Ok(payload["data"]
            .as_array()
            .and_then(|rows| rows.first())
            .and_then(|row| row.get("pending_rows"))
            .and_then(|value| {
                value
                    .as_u64()
                    .or_else(|| value.as_str().and_then(|text| text.parse::<u64>().ok()))
            })
            .unwrap_or(0))
    }

    pub async fn insert_api_key(
        &self,
        record: &IndexerApiKeyRecord,
        token_hash: &str,
    ) -> LedgerResult<()> {
        let token_hash = if token_hash.is_empty() {
            record.token_hash.as_deref().unwrap_or_default()
        } else {
            token_hash
        };
        let body = format!(
            "INSERT INTO {}.api_keys FORMAT JSONEachRow\n{}",
            self.database,
            serde_json::to_string(&serde_json::json!({
                "key_id": record.key_id,
                "token_hash": token_hash,
                "owner_user_id": record.owner_user_id,
                "owner_email": record.owner_email,
                "label": record.label,
                "token_prefix": record.token_prefix,
                "status": match record.status {
                    IndexerApiKeyStatus::Active => "active",
                    IndexerApiKeyStatus::Revoked => "revoked",
                },
                "created_at": record.created_at.format("%Y-%m-%d %H:%M:%S").to_string(),
                "updated_at": record.updated_at.format("%Y-%m-%d %H:%M:%S").to_string(),
                "last_used_at": record
                    .last_used_at
                    .map(|value| value.format("%Y-%m-%d %H:%M:%S").to_string()),
            }))
            .map_err(|err| LedgerError::Persistence(err.to_string()))?
        );
        self.raw_post(&body)
            .await
            .map_err(|err| LedgerError::Persistence(err.to_string()))
    }

    pub async fn list_api_keys(
        &self,
        owner_user_id: &str,
    ) -> LedgerResult<Vec<IndexerApiKeyRecord>> {
        let payload = self
            .query_json(&format!(
                r#"
                SELECT key_id, owner_user_id, owner_email, label, token_prefix, status, created_at, updated_at, last_used_at
                     , token_hash
                FROM {}.api_keys FINAL
                WHERE owner_user_id = '{}'
                ORDER BY created_at DESC
                FORMAT JSON
                "#,
                self.database,
                escape_sql(owner_user_id)
            ))
            .await?;
        payload["data"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|item| {
                serde_json::from_value(item)
                    .map_err(|err| LedgerError::Persistence(err.to_string()))
            })
            .collect()
    }

    pub async fn get_api_key_by_id(
        &self,
        key_id: &str,
    ) -> LedgerResult<Option<IndexerApiKeyRecord>> {
        let payload = self
            .query_json(&format!(
                r#"
                SELECT key_id, owner_user_id, owner_email, label, token_prefix, status, created_at, updated_at, last_used_at
                     , token_hash
                FROM {}.api_keys FINAL
                WHERE key_id = '{}'
                LIMIT 1
                FORMAT JSON
                "#,
                self.database,
                escape_sql(key_id)
            ))
            .await?;
        payload["data"]
            .as_array()
            .and_then(|rows| rows.first())
            .cloned()
            .map(|item| {
                serde_json::from_value(item)
                    .map_err(|err| LedgerError::Persistence(err.to_string()))
            })
            .transpose()
    }

    pub async fn get_active_api_key_by_hash(
        &self,
        token_hash: &str,
    ) -> LedgerResult<Option<IndexerApiKeyRecord>> {
        let payload = self
            .query_json(&format!(
                r#"
                SELECT key_id, owner_user_id, owner_email, label, token_prefix, status, created_at, updated_at, last_used_at
                     , token_hash
                FROM {}.api_keys FINAL
                WHERE token_hash = '{}'
                  AND status = 'active'
                LIMIT 1
                FORMAT JSON
                "#,
                self.database,
                escape_sql(token_hash)
            ))
            .await?;
        payload["data"]
            .as_array()
            .and_then(|rows| rows.first())
            .cloned()
            .map(|item| {
                serde_json::from_value(item)
                    .map_err(|err| LedgerError::Persistence(err.to_string()))
            })
            .transpose()
    }

    pub async fn touch_api_key_last_used(&self, record: &IndexerApiKeyRecord) -> LedgerResult<()> {
        let mut updated = record.clone();
        updated.last_used_at = Some(Utc::now());
        updated.updated_at = Utc::now();
        self.insert_api_key(&updated, "").await
    }

    async fn insert_json_rows<T: Serialize>(&self, table: &str, rows: &[T]) -> LedgerResult<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut body = format!(
            "INSERT INTO {}.{} FORMAT JSONEachRow\n",
            self.database, table
        );
        for row in rows {
            body.push_str(
                &serde_json::to_string(row)
                    .map_err(|err| LedgerError::Persistence(err.to_string()))?,
            );
            body.push('\n');
        }
        self.raw_post(&body)
            .await
            .map_err(|err| LedgerError::Persistence(err.to_string()))
    }

    async fn execute(&self, sql: &str) -> LedgerResult<()> {
        self.raw_post(sql)
            .await
            .map_err(|err| LedgerError::Persistence(err.to_string()))
    }

    async fn query_json(&self, sql: &str) -> LedgerResult<Value> {
        let response = self
            .request_builder(format!(
                "{}/?database={}&output_format_json_quote_64bit_integers=0",
                self.base_url, self.database
            ))
            .body(sql.to_owned())
            .send()
            .await
            .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        let status = response.status();
        let text = response
            .text()
            .await
            .map_err(|err| LedgerError::Persistence(err.to_string()))?;
        if !status.is_success() {
            return Err(LedgerError::Persistence(format!(
                "clickhouse query failed: {status} {text}"
            )));
        }
        serde_json::from_str(&text).map_err(|err| LedgerError::Persistence(err.to_string()))
    }

    async fn raw_post(&self, body: &str) -> Result<(), String> {
        let response = self
            .request_builder(self.base_url.clone())
            .body(body.to_owned())
            .send()
            .await
            .map_err(|err| err.to_string())?;
        let status = response.status();
        let text = response.text().await.map_err(|err| err.to_string())?;
        if !status.is_success() {
            return Err(format!("clickhouse request failed: {status} {text}"));
        }
        Ok(())
    }

    fn request_builder(&self, url: String) -> reqwest::RequestBuilder {
        self.client
            .post(url)
            .basic_auth(&self.user, self.password.clone())
    }
}

fn escape_sql(value: &str) -> String {
    value.replace('\'', "''")
}

fn value_as_u64(value: &Value) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| value.as_str().and_then(|text| text.parse::<u64>().ok()))
}

fn deserialize_first_row<T: DeserializeOwned>(payload: &Value) -> LedgerResult<Option<T>> {
    payload["data"]
        .as_array()
        .and_then(|rows| rows.first())
        .cloned()
        .map(|row| {
            serde_json::from_value(row).map_err(|err| LedgerError::Persistence(err.to_string()))
        })
        .transpose()
}

fn deserialize_rows<T: DeserializeOwned>(payload: &Value) -> LedgerResult<Vec<T>> {
    payload["data"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|row| {
            serde_json::from_value(row).map_err(|err| LedgerError::Persistence(err.to_string()))
        })
        .collect()
}

fn build_conditions(
    chain_id: u32,
    address: &str,
    from_block: Option<u32>,
    to_block: Option<u32>,
    direction: Option<AddressTxDirection>,
) -> Vec<String> {
    let mut conditions = vec![
        format!("refs.chain_id = {chain_id}"),
        format!("refs.address = '{}'", escape_sql(address)),
    ];
    if let Some(from_block) = from_block {
        conditions.push(format!("refs.block_number >= {from_block}"));
    }
    if let Some(to_block) = to_block {
        conditions.push(format!("refs.block_number <= {to_block}"));
    }
    if let Some(direction) = direction {
        let direction = match direction {
            AddressTxDirection::From => "from",
            AddressTxDirection::To => "to",
        };
        conditions.push(format!("refs.direction = '{direction}'"));
    }
    conditions
}
