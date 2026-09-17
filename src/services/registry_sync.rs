use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use reqwest::{
    Client, StatusCode,
    header::{HeaderMap, HeaderName, HeaderValue, IF_MODIFIED_SINCE, IF_NONE_MATCH},
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row, types::Json};
use tokio::{sync::Semaphore, task::JoinSet};
use tracing::{info, warn};
use uuid::Uuid;

use crate::{
    core::errors::{LedgerError, LedgerResult},
    ingestion::rpc::RpcClient,
    services::{
        intelligence_import::{
            DatasetAddress, DatasetLabel, DatasetProtocolDeployment, DatasetSource,
            DatasetTokenMetadata, IntelligenceDatasetManifest, import_intelligence_dataset,
        },
        token_metadata::{
            ERC20_DECIMALS_SELECTOR, ERC20_NAME_SELECTOR, ERC20_SYMBOL_SELECTOR,
            decode_erc20_string, parse_uint_hex,
        },
    },
};

const DEFAULT_MAX_RESPONSE_BYTES: usize = 10 * 1024 * 1024;
const MAINNET_EXPLORER_ADDRESS_PREFIX: &str = "https://robinhoodchain.blockscout.com/address/";

#[derive(Clone)]
pub struct RegistrySyncService {
    pool: PgPool,
    http_client: Client,
    rpc_client: Arc<dyn RpcClient>,
    rpc_concurrency: usize,
    max_response_bytes: usize,
}

#[derive(Clone, Debug)]
struct RegistryEndpoint {
    endpoint_key: String,
    chain_id: Option<u64>,
    endpoint_url: String,
    parser_kind: String,
    sync_interval_seconds: u64,
    http_headers: Value,
    metadata: Value,
    etag: Option<String>,
    last_modified: Option<String>,
    last_checksum_sha256: Option<String>,
    consecutive_failures: u32,
    source: DatasetSource,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct RegistrySyncBatchSummary {
    pub claimed: u64,
    pub succeeded: u64,
    pub unchanged: u64,
    pub not_modified: u64,
    pub failed: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct RegistrySyncOutcome {
    pub endpoint_key: String,
    pub status: String,
    pub fetched_item_count: u64,
    pub validated_item_count: u64,
    pub imported_item_count: u64,
    pub rejected_item_count: u64,
    pub dataset_key: Option<String>,
    pub dataset_version: Option<String>,
    pub error_message: Option<String>,
}

#[derive(Debug)]
struct FetchedResponse {
    status: StatusCode,
    headers: Value,
    etag: Option<String>,
    last_modified: Option<String>,
    content_type: Option<String>,
    body: Vec<u8>,
    body_text: String,
    checksum_sha256: String,
}

#[derive(Debug)]
struct ProcessResult {
    fetched: u64,
    validated: u64,
    imported: u64,
    rejected: u64,
    dataset_key: Option<String>,
    dataset_version: Option<String>,
    metadata: Value,
    fatal_error: Option<String>,
}

impl Default for ProcessResult {
    fn default() -> Self {
        Self {
            fetched: 0,
            validated: 0,
            imported: 0,
            rejected: 0,
            dataset_key: None,
            dataset_version: None,
            metadata: json!({}),
            fatal_error: None,
        }
    }
}

impl RegistrySyncService {
    pub fn new(
        pool: PgPool,
        rpc_client: Arc<dyn RpcClient>,
        rpc_concurrency: usize,
    ) -> LedgerResult<Self> {
        let http_client = Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(45))
            .user_agent("ledgerflow-registry-sync/0.1")
            .build()
            .map_err(|err| LedgerError::TransientIngestion(err.to_string()))?;
        Ok(Self {
            pool,
            http_client,
            rpc_client,
            rpc_concurrency: rpc_concurrency.clamp(1, 64),
            max_response_bytes: DEFAULT_MAX_RESPONSE_BYTES,
        })
    }

    pub async fn sync_due_endpoints(
        &self,
        max_endpoints: usize,
    ) -> LedgerResult<RegistrySyncBatchSummary> {
        let mut summary = RegistrySyncBatchSummary::default();
        for _ in 0..max_endpoints.max(1) {
            let Some(endpoint) = self.claim_next_due_endpoint().await? else {
                break;
            };
            summary.claimed += 1;
            let endpoint_key = endpoint.endpoint_key.clone();
            match self.sync_endpoint(endpoint).await {
                Ok(outcome) => {
                    match outcome.status.as_str() {
                        "succeeded" => summary.succeeded += 1,
                        "unchanged" => summary.unchanged += 1,
                        "not_modified" => summary.not_modified += 1,
                        _ => summary.failed += 1,
                    }
                    info!(
                        endpoint_key,
                        status = outcome.status,
                        fetched = outcome.fetched_item_count,
                        validated = outcome.validated_item_count,
                        imported = outcome.imported_item_count,
                        rejected = outcome.rejected_item_count,
                        "registry source sync finished"
                    );
                }
                Err(err) => {
                    summary.failed += 1;
                    warn!(endpoint_key, error = %err, "registry source sync bookkeeping failed");
                }
            }
        }
        Ok(summary)
    }

    async fn claim_next_due_endpoint(&self) -> LedgerResult<Option<RegistryEndpoint>> {
        let mut tx = self.pool.begin().await.map_err(persistence_error)?;
        let row = sqlx::query(
            r#"
            select
              endpoints.endpoint_key,
              endpoints.chain_id,
              endpoints.endpoint_url,
              endpoints.parser_kind,
              endpoints.sync_interval_seconds,
              endpoints.http_headers,
              endpoints.metadata as endpoint_metadata,
              cursors.etag,
              cursors.last_modified,
              cursors.last_checksum_sha256,
              cursors.consecutive_failures,
              sources.source_key,
              sources.display_name,
              sources.source_kind,
              sources.trust_score,
              sources.owner,
              sources.homepage,
              sources.metadata as source_metadata
            from intelligence_sync_cursors cursors
            join intelligence_source_endpoints endpoints
              on endpoints.endpoint_key = cursors.endpoint_key
            join intelligence_sources sources
              on sources.source_key = endpoints.source_key
            where cursors.next_sync_at <= now()
              and endpoints.enabled
              and sources.status = 'active'
            order by cursors.next_sync_at, endpoints.endpoint_key
            for update of cursors skip locked
            limit 1
            "#,
        )
        .fetch_optional(&mut *tx)
        .await
        .map_err(persistence_error)?;

        let Some(row) = row else {
            tx.commit().await.map_err(persistence_error)?;
            return Ok(None);
        };
        let endpoint_key: String = row.try_get("endpoint_key").map_err(persistence_error)?;
        let sync_interval_seconds = positive_u64(
            row.try_get::<i64, _>("sync_interval_seconds")
                .map_err(persistence_error)?,
            "sync_interval_seconds",
        )?;
        sqlx::query(
            r#"
            update intelligence_sync_cursors
            set next_sync_at = now() + ($2::bigint * interval '1 second'),
                updated_at = now()
            where endpoint_key = $1
            "#,
        )
        .bind(&endpoint_key)
        .bind(sync_interval_seconds as i64)
        .execute(&mut *tx)
        .await
        .map_err(persistence_error)?;
        tx.commit().await.map_err(persistence_error)?;

        let chain_id = row
            .try_get::<Option<i64>, _>("chain_id")
            .map_err(persistence_error)?
            .map(|value| positive_u64(value, "chain_id"))
            .transpose()?;
        Ok(Some(RegistryEndpoint {
            endpoint_key,
            chain_id,
            endpoint_url: row.try_get("endpoint_url").map_err(persistence_error)?,
            parser_kind: row.try_get("parser_kind").map_err(persistence_error)?,
            sync_interval_seconds,
            http_headers: row
                .try_get::<Json<Value>, _>("http_headers")
                .map_err(persistence_error)?
                .0,
            metadata: row
                .try_get::<Json<Value>, _>("endpoint_metadata")
                .map_err(persistence_error)?
                .0,
            etag: row.try_get("etag").map_err(persistence_error)?,
            last_modified: row.try_get("last_modified").map_err(persistence_error)?,
            last_checksum_sha256: row
                .try_get("last_checksum_sha256")
                .map_err(persistence_error)?,
            consecutive_failures: row
                .try_get::<i32, _>("consecutive_failures")
                .map_err(persistence_error)?
                .max(0) as u32,
            source: DatasetSource {
                source_key: row.try_get("source_key").map_err(persistence_error)?,
                display_name: row.try_get("display_name").map_err(persistence_error)?,
                source_kind: row.try_get("source_kind").map_err(persistence_error)?,
                trust_score: row
                    .try_get::<f64, _>("trust_score")
                    .map_err(persistence_error)? as f32,
                owner: row.try_get("owner").map_err(persistence_error)?,
                homepage: row.try_get("homepage").map_err(persistence_error)?,
                metadata: row
                    .try_get::<Json<Value>, _>("source_metadata")
                    .map_err(persistence_error)?
                    .0,
            },
        }))
    }

    async fn sync_endpoint(&self, endpoint: RegistryEndpoint) -> LedgerResult<RegistrySyncOutcome> {
        let run_id = self.create_run(&endpoint).await?;
        let result = self.execute_endpoint(run_id, &endpoint).await;
        match result {
            Ok((status, process, response)) => {
                let error_message = process.fatal_error.clone();
                if let Some(error) = error_message.as_deref() {
                    self.finish_failed_run(run_id, &endpoint, error, Some(&process))
                        .await?;
                    return Ok(outcome_from_process(
                        &endpoint.endpoint_key,
                        "failed",
                        process,
                        error_message,
                    ));
                }
                self.finish_successful_run(run_id, &endpoint, status, &process, response.as_ref())
                    .await?;
                Ok(outcome_from_process(
                    &endpoint.endpoint_key,
                    status,
                    process,
                    None,
                ))
            }
            Err(err) => {
                let error = truncate_text(&err.to_string(), 8_000);
                self.finish_failed_run(run_id, &endpoint, &error, None)
                    .await?;
                Ok(RegistrySyncOutcome {
                    endpoint_key: endpoint.endpoint_key,
                    status: "failed".to_owned(),
                    fetched_item_count: 0,
                    validated_item_count: 0,
                    imported_item_count: 0,
                    rejected_item_count: 0,
                    dataset_key: None,
                    dataset_version: None,
                    error_message: Some(error),
                })
            }
        }
    }

    async fn create_run(&self, endpoint: &RegistryEndpoint) -> LedgerResult<Uuid> {
        let row = sqlx::query(
            r#"
            insert into intelligence_sync_runs (endpoint_key, status, metadata)
            values ($1, 'running', $2)
            returning run_id
            "#,
        )
        .bind(&endpoint.endpoint_key)
        .bind(Json(json!({
            "parser_kind": endpoint.parser_kind,
            "chain_id": endpoint.chain_id,
            "endpoint_metadata": endpoint.metadata,
        })))
        .fetch_one(&self.pool)
        .await
        .map_err(persistence_error)?;
        row.try_get("run_id").map_err(persistence_error)
    }

    async fn execute_endpoint(
        &self,
        run_id: Uuid,
        endpoint: &RegistryEndpoint,
    ) -> LedgerResult<(&'static str, ProcessResult, Option<FetchedResponse>)> {
        let response = self.fetch(endpoint).await?;
        if response.status == StatusCode::NOT_MODIFIED {
            return Ok(("not_modified", ProcessResult::default(), Some(response)));
        }

        let snapshot_id = self.save_snapshot(run_id, endpoint, &response).await?;
        if !response.status.is_success() {
            return Err(LedgerError::TransientIngestion(format!(
                "{} returned HTTP {} (snapshot {})",
                endpoint.endpoint_url, response.status, snapshot_id
            )));
        }
        if endpoint.last_checksum_sha256.as_deref() == Some(&response.checksum_sha256) {
            return Ok(("unchanged", ProcessResult::default(), Some(response)));
        }
        let process = self
            .process_snapshot(endpoint, snapshot_id, &response)
            .await?;
        Ok(("succeeded", process, Some(response)))
    }

    async fn fetch(&self, endpoint: &RegistryEndpoint) -> LedgerResult<FetchedResponse> {
        let mut request = self.http_client.get(&endpoint.endpoint_url);
        let custom_headers = parse_request_headers(&endpoint.http_headers)?;
        request = request.headers(custom_headers);
        if let Some(etag) = endpoint.etag.as_deref() {
            request = request.header(IF_NONE_MATCH, etag);
        }
        if let Some(last_modified) = endpoint.last_modified.as_deref() {
            request = request.header(IF_MODIFIED_SINCE, last_modified);
        }
        let response = request
            .send()
            .await
            .map_err(|err| LedgerError::TransientIngestion(err.to_string()))?;
        let status = response.status();
        let raw_headers = response.headers().clone();
        let etag = header_string(&raw_headers, "etag");
        let last_modified = header_string(&raw_headers, "last-modified");
        let content_type = header_string(&raw_headers, "content-type");
        if response.content_length().unwrap_or(0) > self.max_response_bytes as u64 {
            return Err(LedgerError::MalformedEvidence(format!(
                "{} response exceeds {} bytes",
                endpoint.endpoint_url, self.max_response_bytes
            )));
        }
        let body = response
            .bytes()
            .await
            .map_err(|err| LedgerError::TransientIngestion(err.to_string()))?
            .to_vec();
        if body.len() > self.max_response_bytes {
            return Err(LedgerError::MalformedEvidence(format!(
                "{} response exceeds {} bytes",
                endpoint.endpoint_url, self.max_response_bytes
            )));
        }
        let body_text = String::from_utf8(body.clone()).map_err(|err| {
            LedgerError::MalformedEvidence(format!(
                "{} response is not UTF-8: {err}",
                endpoint.endpoint_url
            ))
        })?;
        Ok(FetchedResponse {
            status,
            headers: response_headers_json(&raw_headers),
            etag,
            last_modified,
            content_type,
            checksum_sha256: sha256_bytes(&body),
            body,
            body_text,
        })
    }

    async fn save_snapshot(
        &self,
        run_id: Uuid,
        endpoint: &RegistryEndpoint,
        response: &FetchedResponse,
    ) -> LedgerResult<Uuid> {
        let row = sqlx::query(
            r#"
            insert into intelligence_source_snapshots (
              endpoint_key, run_id, http_status, content_type, etag, last_modified,
              checksum_sha256, body_size_bytes, body_text, response_headers
            )
            values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
            returning snapshot_id
            "#,
        )
        .bind(&endpoint.endpoint_key)
        .bind(run_id)
        .bind(i32::from(response.status.as_u16()))
        .bind(&response.content_type)
        .bind(&response.etag)
        .bind(&response.last_modified)
        .bind(&response.checksum_sha256)
        .bind(response.body.len() as i64)
        .bind(&response.body_text)
        .bind(Json(response.headers.clone()))
        .fetch_one(&self.pool)
        .await
        .map_err(persistence_error)?;
        let snapshot_id: Uuid = row.try_get("snapshot_id").map_err(persistence_error)?;
        sqlx::query(
            r#"
            update intelligence_sync_runs
            set http_status = $2, checksum_sha256 = $3, snapshot_id = $4
            where run_id = $1
            "#,
        )
        .bind(run_id)
        .bind(i32::from(response.status.as_u16()))
        .bind(&response.checksum_sha256)
        .bind(snapshot_id)
        .execute(&self.pool)
        .await
        .map_err(persistence_error)?;
        Ok(snapshot_id)
    }

    async fn process_snapshot(
        &self,
        endpoint: &RegistryEndpoint,
        snapshot_id: Uuid,
        response: &FetchedResponse,
    ) -> LedgerResult<ProcessResult> {
        match endpoint.parser_kind.as_str() {
            "robinhood_stock_assets" => {
                self.process_stock_assets(endpoint, snapshot_id, response)
                    .await
            }
            "robinhood_corporate_actions" => {
                self.process_corporate_actions(endpoint, snapshot_id, response)
                    .await
            }
            "robinhood_token_contracts_html" => {
                self.process_token_contracts(endpoint, snapshot_id, response)
                    .await
            }
            "robinhood_protocol_contracts_html" => {
                self.process_protocol_contracts(endpoint, snapshot_id, response)
                    .await
            }
            other => Err(LedgerError::MalformedEvidence(format!(
                "unsupported registry parser_kind {other}"
            ))),
        }
    }

    async fn finish_successful_run(
        &self,
        run_id: Uuid,
        endpoint: &RegistryEndpoint,
        status: &str,
        process: &ProcessResult,
        response: Option<&FetchedResponse>,
    ) -> LedgerResult<()> {
        sqlx::query(
            r#"
            update intelligence_sync_runs
            set status = $2,
                http_status = coalesce(http_status, $3),
                checksum_sha256 = coalesce(checksum_sha256, $4),
                fetched_item_count = $5,
                validated_item_count = $6,
                imported_item_count = $7,
                rejected_item_count = $8,
                dataset_key = $9,
                dataset_version = $10,
                error_message = null,
                metadata = metadata || $11,
                finished_at = now()
            where run_id = $1
            "#,
        )
        .bind(run_id)
        .bind(status)
        .bind(response.map(|value| i32::from(value.status.as_u16())))
        .bind(response.map(|value| value.checksum_sha256.as_str()))
        .bind(process.fetched as i64)
        .bind(process.validated as i64)
        .bind(process.imported as i64)
        .bind(process.rejected as i64)
        .bind(&process.dataset_key)
        .bind(&process.dataset_version)
        .bind(Json(process.metadata.clone()))
        .execute(&self.pool)
        .await
        .map_err(persistence_error)?;

        sqlx::query(
            r#"
            update intelligence_sync_cursors
            set etag = coalesce($2, etag),
                last_modified = coalesce($3, last_modified),
                last_checksum_sha256 = coalesce($4, last_checksum_sha256),
                next_sync_at = now() + ($5::bigint * interval '1 second'),
                last_success_at = now(),
                last_run_id = $6,
                consecutive_failures = 0,
                last_error = null,
                updated_at = now()
            where endpoint_key = $1
            "#,
        )
        .bind(&endpoint.endpoint_key)
        .bind(response.and_then(|value| value.etag.as_deref()))
        .bind(response.and_then(|value| value.last_modified.as_deref()))
        .bind(response.and_then(|value| {
            (value.status != StatusCode::NOT_MODIFIED).then_some(value.checksum_sha256.as_str())
        }))
        .bind(endpoint.sync_interval_seconds as i64)
        .bind(run_id)
        .execute(&self.pool)
        .await
        .map_err(persistence_error)?;
        Ok(())
    }

    async fn finish_failed_run(
        &self,
        run_id: Uuid,
        endpoint: &RegistryEndpoint,
        error: &str,
        process: Option<&ProcessResult>,
    ) -> LedgerResult<()> {
        let empty = ProcessResult::default();
        let process = process.unwrap_or(&empty);
        sqlx::query(
            r#"
            update intelligence_sync_runs
            set status = 'failed',
                fetched_item_count = $2,
                validated_item_count = $3,
                imported_item_count = $4,
                rejected_item_count = $5,
                dataset_key = $6,
                dataset_version = $7,
                error_message = $8,
                metadata = metadata || $9,
                finished_at = now()
            where run_id = $1
            "#,
        )
        .bind(run_id)
        .bind(process.fetched as i64)
        .bind(process.validated as i64)
        .bind(process.imported as i64)
        .bind(process.rejected as i64)
        .bind(&process.dataset_key)
        .bind(&process.dataset_version)
        .bind(error)
        .bind(Json(process.metadata.clone()))
        .execute(&self.pool)
        .await
        .map_err(persistence_error)?;

        let failure_number = endpoint.consecutive_failures.saturating_add(1);
        let exponential = 60_u64.saturating_mul(1_u64 << failure_number.saturating_sub(1).min(6));
        let backoff_seconds = exponential
            .min(endpoint.sync_interval_seconds)
            .clamp(60, 3_600);
        sqlx::query(
            r#"
            update intelligence_sync_cursors
            set next_sync_at = now() + ($2::bigint * interval '1 second'),
                last_run_id = $3,
                consecutive_failures = consecutive_failures + 1,
                last_error = $4,
                updated_at = now()
            where endpoint_key = $1
            "#,
        )
        .bind(&endpoint.endpoint_key)
        .bind(backoff_seconds as i64)
        .bind(run_id)
        .bind(error)
        .execute(&self.pool)
        .await
        .map_err(persistence_error)?;
        Ok(())
    }

    async fn process_stock_assets(
        &self,
        endpoint: &RegistryEndpoint,
        snapshot_id: Uuid,
        response: &FetchedResponse,
    ) -> LedgerResult<ProcessResult> {
        let chain_id = required_chain_id(endpoint)?;
        let envelope: StockAssetsEnvelope =
            serde_json::from_slice(&response.body).map_err(|err| {
                LedgerError::MalformedEvidence(format!("invalid Robinhood assets JSON: {err}"))
            })?;
        if envelope.assets.is_empty() {
            return Err(LedgerError::MalformedEvidence(
                "Robinhood assets response contains no assets".to_owned(),
            ));
        }

        let mut invalid_addresses = BTreeMap::new();
        let mut targets = Vec::new();
        for asset in &envelope.assets {
            for deployment in asset
                .deployments
                .iter()
                .filter(|deployment| deployment.chain_id == chain_id)
            {
                match normalize_evm_address(&deployment.contract_address) {
                    Ok(address) => {
                        targets.push((asset.id.clone(), asset.token_symbol.clone(), address))
                    }
                    Err(err) => {
                        invalid_addresses.insert(
                            deployment.contract_address.to_ascii_lowercase(),
                            TokenValidation::rejected(err.to_string()),
                        );
                    }
                }
            }
        }
        if targets.is_empty() && invalid_addresses.is_empty() {
            return Err(LedgerError::MalformedEvidence(format!(
                "Robinhood assets response has no deployments for chain {chain_id}"
            )));
        }

        let validations = self.validate_stock_targets(chain_id, targets).await;
        let mut validation_by_address = invalid_addresses;
        for (_, address, validation) in validations {
            validation_by_address.insert(address, validation);
        }

        self.upsert_stock_asset_snapshot(
            endpoint,
            snapshot_id,
            chain_id,
            &envelope.assets,
            &validation_by_address,
        )
        .await?;

        let mut addresses = Vec::new();
        let mut validation_report = Vec::new();
        let mut transient_errors = 0_u64;
        let mut rejected = 0_u64;
        for asset in &envelope.assets {
            for deployment in asset
                .deployments
                .iter()
                .filter(|deployment| deployment.chain_id == chain_id)
            {
                let address = deployment.contract_address.to_ascii_lowercase();
                let Some(validation) = validation_by_address.get(&address) else {
                    rejected += 1;
                    continue;
                };
                validation_report.push(validation_report_row(
                    &asset.token_symbol,
                    &address,
                    validation,
                ));
                if validation.status == "error" {
                    transient_errors += 1;
                    rejected += 1;
                    continue;
                }
                if validation.status != "validated" {
                    rejected += 1;
                    continue;
                }
                let symbol = validation
                    .symbol
                    .clone()
                    .unwrap_or_else(|| asset.token_symbol.clone());
                let name = validation
                    .name
                    .clone()
                    .unwrap_or_else(|| asset.token_name.clone());
                let decimals = validation.decimals.unwrap_or(18);
                let symbol_slug = slugify(&asset.token_symbol);
                addresses.push(DatasetAddress {
                    address: address.clone(),
                    address_kind: "contract".to_owned(),
                    labels: vec![
                        dataset_label(
                            "stock_token",
                            "asset_identity",
                            None,
                            "Official Robinhood Stock Token deployment",
                            json!({"asset_uid": asset.id, "snapshot_id": snapshot_id}),
                        ),
                        dataset_label(
                            &format!("token:{symbol_slug}"),
                            "asset_identity",
                            Some("token_identity"),
                            &format!("Official {} token deployment", asset.token_symbol),
                            json!({"asset_uid": asset.id, "snapshot_id": snapshot_id}),
                        ),
                    ],
                    token_metadata: Some(DatasetTokenMetadata {
                        symbol,
                        name,
                        decimals: i32::from(decimals),
                    }),
                    protocol_deployments: Vec::new(),
                    metadata: json!({
                        "asset_uid": asset.id,
                        "official_symbol": asset.token_symbol,
                        "official_name": asset.token_name,
                        "status": asset.status,
                        "current_multiplier": asset.current_multiplier,
                        "pending_multiplier": normalized_optional_text(asset.pending_multiplier.as_deref()),
                        "logo_url": asset.logo_url,
                        "snapshot_id": snapshot_id,
                        "validation": validation,
                    }),
                });
            }
        }

        let dataset_key = format!("robinhood_stock_assets_{chain_id}");
        let dataset_version = response.checksum_sha256.clone();
        let mut result = ProcessResult {
            fetched: envelope.assets.len() as u64,
            validated: addresses.len() as u64,
            imported: 0,
            rejected,
            dataset_key: Some(dataset_key.clone()),
            dataset_version: Some(dataset_version.clone()),
            metadata: json!({"validations": validation_report}),
            fatal_error: None,
        };
        if transient_errors > 0 {
            result.fatal_error = Some(format!(
                "{transient_errors} stock-token deployments could not be checked through RPC; active dataset was preserved"
            ));
            return Ok(result);
        }
        if addresses.is_empty() {
            result.fatal_error = Some(
                "no Stock Token deployment passed on-chain validation; active dataset was preserved"
                    .to_owned(),
            );
            return Ok(result);
        }

        let manifest = IntelligenceDatasetManifest {
            dataset_key,
            dataset_version,
            chain_id,
            source: endpoint.source.clone(),
            addresses,
            metadata: json!({
                "endpoint_key": endpoint.endpoint_key,
                "snapshot_id": snapshot_id,
                "source_checksum_sha256": response.checksum_sha256,
            }),
        };
        import_intelligence_dataset(&self.pool, &manifest, &response.checksum_sha256).await?;
        result.imported = manifest.addresses.len() as u64;
        Ok(result)
    }

    async fn validate_stock_targets(
        &self,
        chain_id: u64,
        targets: Vec<(String, String, String)>,
    ) -> Vec<(String, String, TokenValidation)> {
        let semaphore = Arc::new(Semaphore::new(self.rpc_concurrency));
        let mut tasks = JoinSet::new();
        for (asset_uid, expected_symbol, address) in targets {
            let semaphore = semaphore.clone();
            let rpc_client = self.rpc_client.clone();
            tasks.spawn(async move {
                let permit = semaphore.acquire_owned().await;
                let validation = match permit {
                    Ok(_permit) => {
                        validate_erc20_contract(
                            rpc_client,
                            chain_id,
                            &address,
                            Some(&expected_symbol),
                            Some(18),
                        )
                        .await
                    }
                    Err(err) => TokenValidation::error(err.to_string()),
                };
                (asset_uid, address, validation)
            });
        }
        let mut validations = Vec::new();
        while let Some(joined) = tasks.join_next().await {
            match joined {
                Ok(validation) => validations.push(validation),
                Err(err) => validations.push((
                    "unknown".to_owned(),
                    "unknown".to_owned(),
                    TokenValidation::error(err.to_string()),
                )),
            }
        }
        validations
    }

    async fn upsert_stock_asset_snapshot(
        &self,
        endpoint: &RegistryEndpoint,
        snapshot_id: Uuid,
        chain_id: u64,
        assets: &[OfficialStockAsset],
        validations: &BTreeMap<String, TokenValidation>,
    ) -> LedgerResult<()> {
        let mut tx = self.pool.begin().await.map_err(persistence_error)?;
        for asset in assets {
            let pending_multiplier = normalized_optional_text(asset.pending_multiplier.as_deref());
            let pending_effective_at =
                parse_timestamp_value(asset.pending_multiplier_effective_time.as_ref());
            let raw_asset = serde_json::to_value(asset)
                .map_err(|err| LedgerError::MalformedEvidence(err.to_string()))?;
            sqlx::query(
                r#"
                insert into stock_token_assets (
                  asset_uid, token_symbol, token_name, status, current_multiplier,
                  pending_multiplier, pending_multiplier_effective_at, logo_url,
                  trading_capabilities, source_key, snapshot_id, metadata,
                  first_seen_at, last_seen_at, created_at, updated_at
                )
                values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,now(),now(),now(),now())
                on conflict (asset_uid) do update set
                  token_symbol = excluded.token_symbol,
                  token_name = excluded.token_name,
                  status = excluded.status,
                  current_multiplier = excluded.current_multiplier,
                  pending_multiplier = excluded.pending_multiplier,
                  pending_multiplier_effective_at = excluded.pending_multiplier_effective_at,
                  logo_url = excluded.logo_url,
                  trading_capabilities = excluded.trading_capabilities,
                  source_key = excluded.source_key,
                  snapshot_id = excluded.snapshot_id,
                  metadata = excluded.metadata,
                  last_seen_at = now(),
                  updated_at = now()
                "#,
            )
            .bind(&asset.id)
            .bind(&asset.token_symbol)
            .bind(&asset.token_name)
            .bind(&asset.status)
            .bind(&asset.current_multiplier)
            .bind(pending_multiplier)
            .bind(pending_effective_at)
            .bind(&asset.logo_url)
            .bind(Json(asset.trading_capabilities.clone()))
            .bind(&endpoint.source.source_key)
            .bind(snapshot_id)
            .bind(Json(raw_asset))
            .execute(&mut *tx)
            .await
            .map_err(persistence_error)?;

            for deployment in asset
                .deployments
                .iter()
                .filter(|deployment| deployment.chain_id == chain_id)
            {
                let address = deployment.contract_address.to_ascii_lowercase();
                let validation = validations.get(&address).cloned().unwrap_or_else(|| {
                    TokenValidation::rejected("deployment address was not validated".to_owned())
                });
                sqlx::query(
                    r#"
                    insert into stock_token_deployments (
                      asset_uid, chain_id, contract_address, current_multiplier,
                      pending_multiplier, pending_multiplier_effective_at,
                      validation_status, validation_error, code_hash, onchain_symbol,
                      onchain_name, onchain_decimals, source_key, snapshot_id,
                      validated_at, first_seen_at, last_seen_at, metadata, created_at, updated_at
                    )
                    values (
                      $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,
                      case when $7 = 'validated' then now() else null end,
                      now(),now(),$15,now(),now()
                    )
                    on conflict (asset_uid, chain_id, contract_address) do update set
                      current_multiplier = excluded.current_multiplier,
                      pending_multiplier = excluded.pending_multiplier,
                      pending_multiplier_effective_at = excluded.pending_multiplier_effective_at,
                      validation_status = excluded.validation_status,
                      validation_error = excluded.validation_error,
                      code_hash = excluded.code_hash,
                      onchain_symbol = excluded.onchain_symbol,
                      onchain_name = excluded.onchain_name,
                      onchain_decimals = excluded.onchain_decimals,
                      source_key = excluded.source_key,
                      snapshot_id = excluded.snapshot_id,
                      validated_at = excluded.validated_at,
                      last_seen_at = now(),
                      metadata = excluded.metadata,
                      updated_at = now()
                    "#,
                )
                .bind(&asset.id)
                .bind(chain_id as i64)
                .bind(&address)
                .bind(&asset.current_multiplier)
                .bind(normalized_optional_text(
                    asset.pending_multiplier.as_deref(),
                ))
                .bind(pending_effective_at)
                .bind(&validation.status)
                .bind(&validation.error)
                .bind(&validation.code_hash)
                .bind(&validation.symbol)
                .bind(&validation.name)
                .bind(validation.decimals.map(i32::from))
                .bind(&endpoint.source.source_key)
                .bind(snapshot_id)
                .bind(Json(json!({"official_deployment": deployment})))
                .execute(&mut *tx)
                .await
                .map_err(persistence_error)?;
            }
        }
        tx.commit().await.map_err(persistence_error)
    }

    async fn process_corporate_actions(
        &self,
        endpoint: &RegistryEndpoint,
        snapshot_id: Uuid,
        response: &FetchedResponse,
    ) -> LedgerResult<ProcessResult> {
        let envelope: CorporateActionsEnvelope =
            serde_json::from_slice(&response.body).map_err(|err| {
                LedgerError::MalformedEvidence(format!(
                    "invalid Robinhood corporate-actions JSON: {err}"
                ))
            })?;
        let mut tx = self.pool.begin().await.map_err(persistence_error)?;
        for action in &envelope.corp_actions {
            let process_date = action.process_date.as_ref().and_then(OfficialDate::to_date);
            let deployments = serde_json::to_value(&action.deployments)
                .map_err(|err| LedgerError::MalformedEvidence(err.to_string()))?;
            sqlx::query(
                r#"
                insert into stock_token_corporate_actions (
                  action_id, token_symbol, action_type, status, process_date,
                  deployments, details, source_key, snapshot_id,
                  first_seen_at, last_seen_at, created_at, updated_at
                )
                values ($1,$2,$3,$4,$5,$6,$7,$8,$9,now(),now(),now(),now())
                on conflict (action_id) do update set
                  token_symbol = excluded.token_symbol,
                  action_type = excluded.action_type,
                  status = excluded.status,
                  process_date = excluded.process_date,
                  deployments = excluded.deployments,
                  details = excluded.details,
                  source_key = excluded.source_key,
                  snapshot_id = excluded.snapshot_id,
                  last_seen_at = now(),
                  updated_at = now()
                "#,
            )
            .bind(&action.id)
            .bind(&action.token_symbol)
            .bind(&action.action_type)
            .bind(&action.status)
            .bind(process_date)
            .bind(Json(deployments))
            .bind(Json(action.details.clone()))
            .bind(&endpoint.source.source_key)
            .bind(snapshot_id)
            .execute(&mut *tx)
            .await
            .map_err(persistence_error)?;
        }
        tx.commit().await.map_err(persistence_error)?;
        Ok(ProcessResult {
            fetched: envelope.corp_actions.len() as u64,
            validated: envelope.corp_actions.len() as u64,
            imported: envelope.corp_actions.len() as u64,
            metadata: json!({"snapshot_id": snapshot_id}),
            ..ProcessResult::default()
        })
    }

    async fn process_token_contracts(
        &self,
        endpoint: &RegistryEndpoint,
        snapshot_id: Uuid,
        response: &FetchedResponse,
    ) -> LedgerResult<ProcessResult> {
        let chain_id = required_chain_id(endpoint)?;
        let entries = parse_official_mainnet_table(&response.body_text)?;
        if entries.is_empty() {
            return Err(LedgerError::MalformedEvidence(
                "official Token Contracts page contains no Robinhood mainnet addresses".to_owned(),
            ));
        }
        let semaphore = Arc::new(Semaphore::new(self.rpc_concurrency));
        let mut tasks = JoinSet::new();
        for entry in entries.clone() {
            let semaphore = semaphore.clone();
            let rpc_client = self.rpc_client.clone();
            tasks.spawn(async move {
                let validation = match semaphore.acquire_owned().await {
                    Ok(_permit) => {
                        validate_erc20_contract(
                            rpc_client,
                            chain_id,
                            &entry.address,
                            Some(&entry.name),
                            None,
                        )
                        .await
                    }
                    Err(err) => TokenValidation::error(err.to_string()),
                };
                (entry, validation)
            });
        }

        let mut addresses = Vec::new();
        let mut report = Vec::new();
        let mut rejected = 0_u64;
        let mut transient_errors = 0_u64;
        while let Some(joined) = tasks.join_next().await {
            let (entry, validation) = joined.map_err(|err| {
                LedgerError::TransientIngestion(format!("token validation task failed: {err}"))
            })?;
            report.push(validation_report_row(
                &entry.name,
                &entry.address,
                &validation,
            ));
            if validation.status == "error" {
                transient_errors += 1;
                rejected += 1;
                continue;
            }
            if validation.status != "validated" {
                rejected += 1;
                continue;
            }
            let symbol = validation
                .symbol
                .clone()
                .unwrap_or_else(|| entry.name.clone());
            let name = validation
                .name
                .clone()
                .unwrap_or_else(|| entry.name.clone());
            let decimals = validation.decimals.unwrap_or(18);
            let symbol_slug = slugify(&entry.name);
            let mut labels = vec![
                dataset_label(
                    "canonical_token",
                    "asset_identity",
                    None,
                    "Robinhood Chain official token contract",
                    json!({"snapshot_id": snapshot_id}),
                ),
                dataset_label(
                    &format!("token:{symbol_slug}"),
                    "asset_identity",
                    Some("token_identity"),
                    &format!("Robinhood Chain official {} token", entry.name),
                    json!({"snapshot_id": snapshot_id}),
                ),
            ];
            if is_stable_asset(&symbol, &name) {
                labels.push(dataset_label(
                    "stablecoin",
                    "asset_role",
                    None,
                    "Stable quote asset inferred from an official token entry and validated ERC20 metadata",
                    json!({"snapshot_id": snapshot_id, "symbol": symbol, "name": name, "classifier": "stable_asset_symbol_v1"}),
                ));
            }
            if is_wrapped_native_asset(&symbol, &name) {
                labels.push(dataset_label(
                    "wrapped_native",
                    "asset_role",
                    None,
                    "Wrapped native asset inferred from an official token entry and validated ERC20 metadata",
                    json!({"snapshot_id": snapshot_id, "symbol": symbol, "name": name, "classifier": "wrapped_native_symbol_v1"}),
                ));
            }
            addresses.push(DatasetAddress {
                address: entry.address,
                address_kind: "contract".to_owned(),
                labels,
                token_metadata: Some(DatasetTokenMetadata {
                    symbol,
                    name,
                    decimals: i32::from(decimals),
                }),
                protocol_deployments: Vec::new(),
                metadata: json!({"official_table_name": entry.name, "snapshot_id": snapshot_id, "validation": validation}),
            });
        }

        let dataset_key = format!("robinhood_token_contracts_{chain_id}");
        let dataset_version = response.checksum_sha256.clone();
        let mut result = ProcessResult {
            fetched: entries.len() as u64,
            validated: addresses.len() as u64,
            rejected,
            dataset_key: Some(dataset_key.clone()),
            dataset_version: Some(dataset_version.clone()),
            metadata: json!({"validations": report}),
            ..ProcessResult::default()
        };
        if transient_errors > 0 {
            result.fatal_error = Some(format!(
                "{transient_errors} official token contracts could not be checked through RPC; active dataset was preserved"
            ));
            return Ok(result);
        }
        if addresses.is_empty() {
            result.fatal_error = Some(
                "no official token contract passed on-chain validation; active dataset was preserved"
                    .to_owned(),
            );
            return Ok(result);
        }
        let manifest = IntelligenceDatasetManifest {
            dataset_key,
            dataset_version,
            chain_id,
            source: endpoint.source.clone(),
            addresses,
            metadata: json!({
                "endpoint_key": endpoint.endpoint_key,
                "snapshot_id": snapshot_id,
                "source_checksum_sha256": response.checksum_sha256,
            }),
        };
        import_intelligence_dataset(&self.pool, &manifest, &response.checksum_sha256).await?;
        result.imported = manifest.addresses.len() as u64;
        Ok(result)
    }

    async fn process_protocol_contracts(
        &self,
        endpoint: &RegistryEndpoint,
        snapshot_id: Uuid,
        response: &FetchedResponse,
    ) -> LedgerResult<ProcessResult> {
        let chain_id = required_chain_id(endpoint)?;
        let entries = parse_official_mainnet_table(&response.body_text)?;
        if entries.is_empty() {
            return Err(LedgerError::MalformedEvidence(
                "official Protocol Contracts page contains no Robinhood mainnet addresses"
                    .to_owned(),
            ));
        }

        let unique_addresses = entries
            .iter()
            .map(|entry| entry.address.clone())
            .collect::<BTreeSet<_>>();
        let semaphore = Arc::new(Semaphore::new(self.rpc_concurrency));
        let mut tasks = JoinSet::new();
        let mut validation_by_address = BTreeMap::new();
        for address in unique_addresses {
            if is_small_precompile_address(&address) {
                validation_by_address.insert(address, ContractValidation::precompile());
                continue;
            }
            let semaphore = semaphore.clone();
            let rpc_client = self.rpc_client.clone();
            tasks.spawn(async move {
                let validation = match semaphore.acquire_owned().await {
                    Ok(_permit) => validate_contract_code(rpc_client, chain_id, &address).await,
                    Err(err) => ContractValidation::error(err.to_string()),
                };
                (address, validation)
            });
        }
        while let Some(joined) = tasks.join_next().await {
            let (address, validation) = joined.map_err(|err| {
                LedgerError::TransientIngestion(format!("protocol validation task failed: {err}"))
            })?;
            validation_by_address.insert(address, validation);
        }

        let mut addresses_by_address: BTreeMap<String, DatasetAddress> = BTreeMap::new();
        let mut report = Vec::new();
        let mut rejected = 0_u64;
        let mut transient_errors = 0_u64;
        for entry in &entries {
            let Some(validation) = validation_by_address.get(&entry.address) else {
                rejected += 1;
                continue;
            };
            report.push(json!({
                "name": entry.name,
                "address": entry.address,
                "status": validation.status,
                "error": validation.error,
                "code_hash": validation.code_hash,
                "precompile": validation.precompile,
            }));
            if validation.status == "error" {
                transient_errors += 1;
                rejected += 1;
                continue;
            }
            if validation.status != "validated" {
                rejected += 1;
                continue;
            }
            let (protocol_key, display_name, protocol_family) = protocol_semantics(&entry.name);
            let role = slugify(&entry.name);
            let address_kind = if validation.precompile {
                "precompile"
            } else {
                "contract"
            };
            let address = addresses_by_address
                .entry(entry.address.clone())
                .or_insert_with(|| DatasetAddress {
                    address: entry.address.clone(),
                    address_kind: address_kind.to_owned(),
                    labels: vec![dataset_label(
                        "official_robinhood_contract",
                        "provenance",
                        None,
                        "Contract listed in Robinhood Chain official documentation",
                        json!({"snapshot_id": snapshot_id}),
                    )],
                    token_metadata: None,
                    protocol_deployments: Vec::new(),
                    metadata: json!({
                        "snapshot_id": snapshot_id,
                        "code_hash": validation.code_hash,
                        "precompile": validation.precompile,
                    }),
                });
            let protocol_label = format!("protocol:{protocol_key}");
            if !address
                .labels
                .iter()
                .any(|label| label.label_key == protocol_label)
            {
                address.labels.push(dataset_label(
                    &protocol_label,
                    "protocol",
                    None,
                    &format!("Official {display_name} deployment"),
                    json!({"snapshot_id": snapshot_id}),
                ));
            }
            address
                .protocol_deployments
                .push(DatasetProtocolDeployment {
                    protocol_key,
                    display_name,
                    protocol_family,
                    contract_role: role,
                    confidence: 1.0,
                    metadata: json!({
                        "official_table_name": entry.name,
                        "snapshot_id": snapshot_id,
                        "precompile": validation.precompile,
                        "code_hash": validation.code_hash,
                    }),
                });
        }

        let addresses = addresses_by_address.into_values().collect::<Vec<_>>();
        let imported_deployments = addresses
            .iter()
            .map(|address| address.protocol_deployments.len() as u64)
            .sum::<u64>();
        let dataset_key = format!("robinhood_protocol_contracts_{chain_id}");
        let dataset_version = response.checksum_sha256.clone();
        let mut result = ProcessResult {
            fetched: entries.len() as u64,
            validated: entries.len() as u64 - rejected,
            rejected,
            dataset_key: Some(dataset_key.clone()),
            dataset_version: Some(dataset_version.clone()),
            metadata: json!({"validations": report}),
            ..ProcessResult::default()
        };
        if transient_errors > 0 {
            result.fatal_error = Some(format!(
                "{transient_errors} protocol contracts could not be checked through RPC; active dataset was preserved"
            ));
            return Ok(result);
        }
        if addresses.is_empty() {
            result.fatal_error = Some(
                "no official protocol contract passed validation; active dataset was preserved"
                    .to_owned(),
            );
            return Ok(result);
        }
        let manifest = IntelligenceDatasetManifest {
            dataset_key,
            dataset_version,
            chain_id,
            source: endpoint.source.clone(),
            addresses,
            metadata: json!({
                "endpoint_key": endpoint.endpoint_key,
                "snapshot_id": snapshot_id,
                "source_checksum_sha256": response.checksum_sha256,
            }),
        };
        import_intelligence_dataset(&self.pool, &manifest, &response.checksum_sha256).await?;
        result.imported = imported_deployments;
        Ok(result)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct OfficialDeployment {
    contract_address: String,
    chain_id: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct OfficialStockAsset {
    id: String,
    token_symbol: String,
    token_name: String,
    #[serde(default)]
    deployments: Vec<OfficialDeployment>,
    current_multiplier: String,
    #[serde(default)]
    pending_multiplier: Option<String>,
    #[serde(default)]
    pending_multiplier_effective_time: Option<Value>,
    status: String,
    #[serde(default)]
    logo_url: Option<String>,
    #[serde(default = "json_object")]
    trading_capabilities: Value,
}

#[derive(Debug, Deserialize)]
struct StockAssetsEnvelope {
    assets: Vec<OfficialStockAsset>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct OfficialDate {
    year: i32,
    month: u32,
    day: u32,
}

impl OfficialDate {
    fn to_date(&self) -> Option<NaiveDate> {
        NaiveDate::from_ymd_opt(self.year, self.month, self.day)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct OfficialCorporateAction {
    id: String,
    #[serde(rename = "type")]
    action_type: String,
    status: String,
    #[serde(default)]
    process_date: Option<OfficialDate>,
    token_symbol: String,
    #[serde(default)]
    deployments: Vec<OfficialDeployment>,
    #[serde(default = "json_object")]
    details: Value,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CorporateActionsEnvelope {
    corp_actions: Vec<OfficialCorporateAction>,
}

#[derive(Clone, Debug)]
struct HtmlAddressEntry {
    name: String,
    address: String,
}

#[derive(Clone, Debug, Serialize)]
struct TokenValidation {
    status: String,
    error: Option<String>,
    code_hash: Option<String>,
    symbol: Option<String>,
    name: Option<String>,
    decimals: Option<u8>,
}

impl TokenValidation {
    fn rejected(error: String) -> Self {
        Self {
            status: "rejected".to_owned(),
            error: Some(error),
            code_hash: None,
            symbol: None,
            name: None,
            decimals: None,
        }
    }

    fn error(error: String) -> Self {
        Self {
            status: "error".to_owned(),
            error: Some(error),
            code_hash: None,
            symbol: None,
            name: None,
            decimals: None,
        }
    }
}

#[derive(Clone, Debug)]
struct ContractValidation {
    status: String,
    error: Option<String>,
    code_hash: Option<String>,
    precompile: bool,
}

impl ContractValidation {
    fn precompile() -> Self {
        Self {
            status: "validated".to_owned(),
            error: None,
            code_hash: None,
            precompile: true,
        }
    }

    fn error(error: String) -> Self {
        Self {
            status: "error".to_owned(),
            error: Some(error),
            code_hash: None,
            precompile: false,
        }
    }
}

async fn validate_erc20_contract(
    rpc_client: Arc<dyn RpcClient>,
    chain_id: u64,
    address: &str,
    expected_symbol: Option<&str>,
    expected_decimals: Option<u8>,
) -> TokenValidation {
    let code = match rpc_client.get_code(chain_id, address, None).await {
        Ok(code) => code,
        Err(err) => return TokenValidation::error(err.to_string()),
    };
    if is_empty_code(&code) {
        return TokenValidation::rejected("eth_getCode returned empty bytecode".to_owned());
    }
    let code_hash = Some(bytecode_hash(&code));
    let (symbol_result, name_result, decimals_result) = tokio::join!(
        rpc_client.eth_call(chain_id, address, ERC20_SYMBOL_SELECTOR, None),
        rpc_client.eth_call(chain_id, address, ERC20_NAME_SELECTOR, None),
        rpc_client.eth_call(chain_id, address, ERC20_DECIMALS_SELECTOR, None),
    );
    let (symbol_raw, name_raw, decimals_raw) = match (symbol_result, name_result, decimals_result) {
        (Ok(symbol), Ok(name), Ok(decimals)) => (symbol, name, decimals),
        (symbol, name, decimals) => {
            let errors = [symbol.err(), name.err(), decimals.err()]
                .into_iter()
                .flatten()
                .map(|err| err.to_string())
                .collect::<Vec<_>>()
                .join("; ");
            return TokenValidation {
                status: "error".to_owned(),
                error: Some(format!("ERC20 metadata RPC failed: {errors}")),
                code_hash,
                symbol: None,
                name: None,
                decimals: None,
            };
        }
    };
    let symbol = decode_erc20_string(&symbol_raw);
    let name = decode_erc20_string(&name_raw);
    let decimals = parse_uint_hex(&decimals_raw).and_then(|value| u8::try_from(value).ok());
    if symbol.is_none() || name.is_none() || decimals.is_none() {
        return TokenValidation {
            status: "rejected".to_owned(),
            error: Some("contract did not return complete ERC20 symbol/name/decimals".to_owned()),
            code_hash,
            symbol,
            name,
            decimals,
        };
    }
    if let Some(expected_symbol) = expected_symbol
        && !symbol
            .as_deref()
            .is_some_and(|value| value.eq_ignore_ascii_case(expected_symbol.trim()))
    {
        return TokenValidation {
            status: "rejected".to_owned(),
            error: Some(format!(
                "on-chain symbol {:?} does not match official symbol {expected_symbol}",
                symbol
            )),
            code_hash,
            symbol,
            name,
            decimals,
        };
    }
    if let Some(expected_decimals) = expected_decimals
        && decimals != Some(expected_decimals)
    {
        return TokenValidation {
            status: "rejected".to_owned(),
            error: Some(format!(
                "on-chain decimals {:?} does not match required {expected_decimals}",
                decimals
            )),
            code_hash,
            symbol,
            name,
            decimals,
        };
    }
    TokenValidation {
        status: "validated".to_owned(),
        error: None,
        code_hash,
        symbol,
        name,
        decimals,
    }
}

async fn validate_contract_code(
    rpc_client: Arc<dyn RpcClient>,
    chain_id: u64,
    address: &str,
) -> ContractValidation {
    match rpc_client.get_code(chain_id, address, None).await {
        Ok(code) if is_empty_code(&code) => ContractValidation {
            status: "rejected".to_owned(),
            error: Some("eth_getCode returned empty bytecode".to_owned()),
            code_hash: None,
            precompile: false,
        },
        Ok(code) => ContractValidation {
            status: "validated".to_owned(),
            error: None,
            code_hash: Some(bytecode_hash(&code)),
            precompile: false,
        },
        Err(err) => ContractValidation::error(err.to_string()),
    }
}

fn parse_official_mainnet_table(html: &str) -> LedgerResult<Vec<HtmlAddressEntry>> {
    let mut seen = BTreeSet::new();
    let mut entries = Vec::new();
    for row in html.split("</tr>") {
        let Some(marker_position) = row.find(MAINNET_EXPLORER_ADDRESS_PREFIX) else {
            continue;
        };
        let address_start = marker_position + MAINNET_EXPLORER_ADDRESS_PREFIX.len();
        let Some(candidate) = row.get(address_start..address_start + 42) else {
            continue;
        };
        let Ok(address) = normalize_evm_address(candidate) else {
            continue;
        };
        let Some(name) = first_table_cell_text(row) else {
            continue;
        };
        if name.is_empty() || !seen.insert((name.clone(), address.clone())) {
            continue;
        }
        entries.push(HtmlAddressEntry { name, address });
    }
    if html.contains(MAINNET_EXPLORER_ADDRESS_PREFIX) && entries.is_empty() {
        return Err(LedgerError::MalformedEvidence(
            "official HTML contains mainnet links but table rows could not be parsed".to_owned(),
        ));
    }
    Ok(entries)
}

fn first_table_cell_text(row: &str) -> Option<String> {
    let cell_start = row.find("<td")?;
    let content_start = cell_start + row.get(cell_start..)?.find('>')? + 1;
    let content_end = content_start + row.get(content_start..)?.find("</td>")?;
    let raw = row.get(content_start..content_end)?;
    let mut text = String::new();
    let mut inside_tag = false;
    for character in raw.chars() {
        match character {
            '<' => inside_tag = true,
            '>' => inside_tag = false,
            _ if !inside_tag => text.push(character),
            _ => {}
        }
    }
    let decoded = decode_html_entities(text.trim());
    (!decoded.is_empty()).then_some(decoded)
}

fn decode_html_entities(value: &str) -> String {
    value
        .replace("&amp;", "&")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&nbsp;", " ")
        .trim()
        .to_owned()
}

fn protocol_semantics(name: &str) -> (String, String, String) {
    let lower = name.to_ascii_lowercase();
    if lower.contains("permit2") {
        (
            "permit2".to_owned(),
            "Permit2".to_owned(),
            "dex_infrastructure".to_owned(),
        )
    } else if lower.contains("multicall") {
        (
            "multicall".to_owned(),
            "Multicall".to_owned(),
            "utility".to_owned(),
        )
    } else {
        (
            "robinhood_chain".to_owned(),
            "Robinhood Chain".to_owned(),
            "chain_infrastructure".to_owned(),
        )
    }
}

fn is_small_precompile_address(address: &str) -> bool {
    let Some(body) = address.strip_prefix("0x") else {
        return false;
    };
    body.len() == 40
        && body
            .get(..36)
            .is_some_and(|prefix| prefix.chars().all(|ch| ch == '0'))
        && body
            .get(36..)
            .and_then(|suffix| u16::from_str_radix(suffix, 16).ok())
            .is_some_and(|value| value > 0)
}

fn required_chain_id(endpoint: &RegistryEndpoint) -> LedgerResult<u64> {
    endpoint.chain_id.ok_or_else(|| {
        LedgerError::MalformedEvidence(format!(
            "endpoint {} has no chain_id",
            endpoint.endpoint_key
        ))
    })
}

fn dataset_label(
    label_key: &str,
    dimension: &str,
    exclusive_group: Option<&str>,
    description: &str,
    evidence: Value,
) -> DatasetLabel {
    DatasetLabel {
        label_key: label_key.to_owned(),
        dimension: dimension.to_owned(),
        exclusive_group: exclusive_group.map(ToOwned::to_owned),
        confidence: 1.0,
        description: Some(description.to_owned()),
        evidence,
    }
}

fn is_stable_asset(symbol: &str, name: &str) -> bool {
    let symbol = symbol.trim().to_ascii_uppercase();
    let name = name.trim().to_ascii_lowercase();
    matches!(
        symbol.as_str(),
        "USDC"
            | "USDBC"
            | "USDT"
            | "DAI"
            | "USDS"
            | "PYUSD"
            | "FRAX"
            | "LUSD"
            | "GHO"
            | "USD0"
            | "USDG"
            | "USDE"
    ) || name.contains("usd coin")
        || name.contains("stablecoin")
}

fn is_wrapped_native_asset(symbol: &str, name: &str) -> bool {
    let symbol = symbol.trim().to_ascii_uppercase();
    let name = name.trim().to_ascii_lowercase();
    matches!(symbol.as_str(), "WETH" | "WETH.E")
        || matches!(name.as_str(), "wrapped ether" | "wrapped eth")
}

fn outcome_from_process(
    endpoint_key: &str,
    status: &str,
    process: ProcessResult,
    error_message: Option<String>,
) -> RegistrySyncOutcome {
    RegistrySyncOutcome {
        endpoint_key: endpoint_key.to_owned(),
        status: status.to_owned(),
        fetched_item_count: process.fetched,
        validated_item_count: process.validated,
        imported_item_count: process.imported,
        rejected_item_count: process.rejected,
        dataset_key: process.dataset_key,
        dataset_version: process.dataset_version,
        error_message,
    }
}

fn validation_report_row(name: &str, address: &str, validation: &TokenValidation) -> Value {
    json!({
        "name": name,
        "address": address,
        "status": validation.status,
        "error": validation.error,
        "code_hash": validation.code_hash,
        "symbol": validation.symbol,
        "onchain_name": validation.name,
        "decimals": validation.decimals,
    })
}

fn parse_request_headers(value: &Value) -> LedgerResult<HeaderMap> {
    let object = value.as_object().ok_or_else(|| {
        LedgerError::MalformedEvidence("endpoint http_headers must be a JSON object".to_owned())
    })?;
    let mut headers = HeaderMap::new();
    for (name, value) in object {
        if matches!(
            name.to_ascii_lowercase().as_str(),
            "authorization" | "cookie" | "proxy-authorization" | "x-api-key"
        ) {
            return Err(LedgerError::MalformedEvidence(format!(
                "secret-bearing header {name} is not allowed in database endpoint configuration"
            )));
        }
        let value = value.as_str().ok_or_else(|| {
            LedgerError::MalformedEvidence(format!("HTTP header {name} must be a string"))
        })?;
        let name = HeaderName::from_bytes(name.as_bytes())
            .map_err(|err| LedgerError::MalformedEvidence(err.to_string()))?;
        let value = HeaderValue::from_str(value)
            .map_err(|err| LedgerError::MalformedEvidence(err.to_string()))?;
        headers.insert(name, value);
    }
    Ok(headers)
}

fn response_headers_json(headers: &HeaderMap) -> Value {
    let mut object = Map::new();
    for (name, value) in headers {
        if let Ok(value) = value.to_str() {
            object.insert(name.as_str().to_owned(), Value::String(value.to_owned()));
        }
    }
    Value::Object(object)
}

fn header_string(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned)
}

fn normalize_evm_address(value: &str) -> LedgerResult<String> {
    let normalized = value.trim().to_ascii_lowercase();
    let Some(body) = normalized.strip_prefix("0x") else {
        return Err(LedgerError::MalformedEvidence(format!(
            "address must start with 0x: {value}"
        )));
    };
    if body.len() != 40 || !body.chars().all(|ch| ch.is_ascii_hexdigit()) {
        return Err(LedgerError::MalformedEvidence(format!(
            "invalid EVM address: {value}"
        )));
    }
    Ok(normalized)
}

fn positive_u64(value: i64, field: &str) -> LedgerResult<u64> {
    u64::try_from(value)
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| LedgerError::MalformedEvidence(format!("{field} must be positive")))
}

fn is_empty_code(code: &str) -> bool {
    let body = code.trim().trim_start_matches("0x");
    body.is_empty() || body.chars().all(|character| character == '0')
}

fn bytecode_hash(code: &str) -> String {
    let body = code.trim().trim_start_matches("0x");
    match hex::decode(body) {
        Ok(bytes) => sha256_bytes(&bytes),
        Err(_) => sha256_bytes(body.as_bytes()),
    }
}

fn sha256_bytes(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

fn normalized_optional_text(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn parse_timestamp_value(value: Option<&Value>) -> Option<DateTime<Utc>> {
    let value = value?;
    if let Some(raw) = value.as_str() {
        if raw.trim().is_empty() {
            return None;
        }
        if let Ok(parsed) = DateTime::parse_from_rfc3339(raw) {
            return Some(parsed.with_timezone(&Utc));
        }
        if let Ok(seconds) = raw.parse::<i64>() {
            return Utc.timestamp_opt(seconds, 0).single();
        }
    }
    if let Some(seconds) = value.as_i64() {
        return Utc.timestamp_opt(seconds, 0).single();
    }
    None
}

fn slugify(value: &str) -> String {
    let mut output = String::new();
    let mut separator = false;
    for character in value.chars() {
        if character.is_ascii_alphanumeric() {
            if separator && !output.is_empty() {
                output.push('_');
            }
            output.push(character.to_ascii_lowercase());
            separator = false;
        } else {
            separator = true;
        }
    }
    output.trim_matches('_').to_owned()
}

fn json_object() -> Value {
    json!({})
}

fn truncate_text(value: &str, max_characters: usize) -> String {
    value.chars().take(max_characters).collect()
}

fn persistence_error(error: impl std::fmt::Display) -> LedgerError {
    LedgerError::Persistence(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_only_mainnet_blockscout_rows() {
        let html = concat!(
            "<tr><td>Mainnet Contract</td><td><a href=\"https://robinhoodchain.blockscout.com/address/",
            "0x1111111111111111111111111111111111111111\">address</a></td></tr>",
            "<tr><td>L1 Contract</td><td><a href=\"https://etherscan.io/address/",
            "0x2222222222222222222222222222222222222222\">address</a></td></tr>",
            "<tr><td>Testnet Contract</td><td><a href=\"https://explorer.testnet.chain.robinhood.com/address/",
            "0x3333333333333333333333333333333333333333\">address</a></td></tr>"
        );
        let entries = parse_official_mainnet_table(html).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "Mainnet Contract");
        assert_eq!(
            entries[0].address,
            "0x1111111111111111111111111111111111111111"
        );
    }

    #[test]
    fn parses_current_stock_asset_shape() {
        let raw = r#"{
          "assets": [{
            "id": "asset-1",
            "tokenSymbol": "EXAMPLE",
            "tokenName": "Example Token",
            "deployments": [{
              "contractAddress": "0x4444444444444444444444444444444444444444",
              "chainId": 4663
            }],
            "currentMultiplier": "1.000000000000000000",
            "pendingMultiplier": "",
            "status": "ASSET_STATUS_ACTIVE",
            "tradingCapabilities": {"market": {"whole": "TRADABLE"}}
          }]
        }"#;
        let parsed: StockAssetsEnvelope = serde_json::from_str(raw).unwrap();
        assert_eq!(parsed.assets.len(), 1);
        assert_eq!(parsed.assets[0].deployments[0].chain_id, 4663);
    }

    #[test]
    fn parses_current_corporate_action_shape() {
        let raw = r#"{
          "corpActions": [{
            "id": "action-1",
            "type": "CORPORATE_ACTION_TYPE_CASH_DIVIDEND",
            "status": "CORPORATE_ACTION_STATUS_IN_PROGRESS",
            "processDate": {"year": 2026, "month": 7, "day": 31},
            "tokenSymbol": "EXAMPLE",
            "deployments": [],
            "details": {"cashDividend": {"rate": "0.10"}}
          }]
        }"#;
        let parsed: CorporateActionsEnvelope = serde_json::from_str(raw).unwrap();
        assert_eq!(parsed.corp_actions.len(), 1);
        assert_eq!(
            parsed.corp_actions[0]
                .process_date
                .as_ref()
                .and_then(OfficialDate::to_date),
            NaiveDate::from_ymd_opt(2026, 7, 31)
        );
    }

    #[test]
    fn classifies_small_system_addresses_as_precompiles() {
        assert!(is_small_precompile_address(
            "0x0000000000000000000000000000000000000064"
        ));
        assert!(!is_small_precompile_address(
            "0x1111111111111111111111111111111111111111"
        ));
    }

    #[test]
    fn slugifies_registry_names_without_encoding_addresses() {
        assert_eq!(slugify("L2 Arb-Custom Gateway"), "l2_arb_custom_gateway");
        assert_eq!(slugify("BRK.B"), "brk_b");
    }

    #[test]
    fn derives_runtime_asset_roles_from_metadata_not_addresses() {
        assert!(is_stable_asset("USDC", "USD Coin"));
        assert!(is_stable_asset("custom", "Example Stablecoin"));
        assert!(!is_stable_asset("WETH", "Wrapped Ether"));
        assert!(is_wrapped_native_asset("WETH", "Wrapped Ether"));
        assert!(!is_wrapped_native_asset("ETH", "Ether"));
    }
}
