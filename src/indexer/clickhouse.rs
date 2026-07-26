use chrono::Utc;
use serde_json::Value;

use crate::core::errors::{LedgerError, LedgerResult};
use crate::indexer::models::{
    AddressTxDirection, AddressTxRef, IndexedBlock, IndexerApiKeyRecord, IndexerApiKeyStatus,
    LazyIndexingRequest, normalize_address,
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

    pub async fn get_progress(&self, chain_id: u32, worker_id: &str) -> LedgerResult<Option<u64>> {
        let worker_id = escape_sql(worker_id);
        let payload = self
            .query_json(&format!(
                r#"
                SELECT last_indexed_block
                FROM {}.indexer_progress
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
                SELECT chain_id, address, tx_hash, block_number, tx_index, direction, from_address, to_address
                FROM {}.address_tx_refs
                WHERE {}
                ORDER BY block_number DESC, tx_index DESC, direction ASC
                LIMIT {}
                FORMAT JSON
                "#,
                self.database,
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
                SELECT tx_hash, max(block_number) AS block_number, max(tx_index) AS tx_index
                FROM {}.address_tx_refs
                WHERE {}
                GROUP BY tx_hash
                ORDER BY block_number DESC, tx_index DESC
                LIMIT {}
                FORMAT JSON
                "#,
                self.database,
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
                SELECT chain_id, address, tx_hash, block_number, tx_index, direction, from_address, to_address
                FROM {}.address_tx_refs
                WHERE chain_id = {}
                  AND isNull(from_address)
                ORDER BY block_number ASC, tx_index ASC, direction ASC, tx_hash ASC
                LIMIT {}
                FORMAT JSON
                "#,
                self.database, chain_id, limit
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
                FROM {}.address_tx_refs
                WHERE chain_id = {}
                  AND isNull(from_address)
                FORMAT JSON
                "#,
                self.database, chain_id
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

    async fn execute(&self, sql: &str) -> LedgerResult<()> {
        self.raw_post(sql)
            .await
            .map_err(|err| LedgerError::Persistence(err.to_string()))
    }

    async fn query_json(&self, sql: &str) -> LedgerResult<Value> {
        let response = self
            .request_builder(format!("{}/?database={}", self.base_url, self.database))
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

fn build_conditions(
    chain_id: u32,
    address: &str,
    from_block: Option<u32>,
    to_block: Option<u32>,
    direction: Option<AddressTxDirection>,
) -> Vec<String> {
    let mut conditions = vec![
        format!("chain_id = {chain_id}"),
        format!("address = '{}'", escape_sql(address)),
    ];
    if let Some(from_block) = from_block {
        conditions.push(format!("block_number >= {from_block}"));
    }
    if let Some(to_block) = to_block {
        conditions.push(format!("block_number <= {to_block}"));
    }
    if let Some(direction) = direction {
        let direction = match direction {
            AddressTxDirection::From => "from",
            AddressTxDirection::To => "to",
        };
        conditions.push(format!("direction = '{direction}'"));
    }
    conditions
}
