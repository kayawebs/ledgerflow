use serde_json::Value;

use crate::core::errors::{LedgerError, LedgerResult};
use crate::indexer::models::{AddressTxDirection, AddressTxRef, normalize_address};

#[derive(Clone)]
pub struct ClickHouseClient {
    base_url: String,
    database: String,
    user: String,
    password: Option<String>,
    client: reqwest::Client,
}

impl ClickHouseClient {
    pub fn new(
        base_url: String,
        database: String,
        user: String,
        password: Option<String>,
    ) -> Self {
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
                SELECT chain_id, address, tx_hash, block_number, tx_index, direction
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
