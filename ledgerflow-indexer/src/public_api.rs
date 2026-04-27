use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::Mutex;

use crate::admin::{AuthenticatedApiKey, PostgresAdminStore};
use crate::core::errors::{LedgerError, LedgerResult};
use crate::indexer::clickhouse::AddressTxCursor;
use crate::indexer::legacy_api::{ApiError, parse_direction};
use crate::indexer::models::{AddressTxDirection, AddressTxRef};
use crate::indexer::service::IndexerService;

#[derive(Clone)]
pub struct PublicApiState {
    service: Arc<IndexerService>,
    admin_store: Arc<PostgresAdminStore>,
    test_rate_limiter: Arc<TestTokenRateLimiter>,
}

#[derive(Debug, Deserialize)]
pub struct AddressTxQuery {
    pub limit: Option<u32>,
    pub from_block: Option<u32>,
    pub to_block: Option<u32>,
    pub direction: Option<String>,
    pub cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct BatchAddressTxRequest {
    pub addresses: Vec<String>,
    pub limit: Option<u32>,
    pub from_block: Option<u32>,
    pub to_block: Option<u32>,
    pub direction: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct PublicRefsResponse {
    pub chain: &'static str,
    pub chain_id: u32,
    pub address: String,
    pub limit: u32,
    pub next_cursor: Option<String>,
    pub items: Vec<AddressTxRef>,
}

#[derive(Debug, Serialize)]
pub struct PublicHashesResponse {
    pub chain: &'static str,
    pub chain_id: u32,
    pub address: String,
    pub limit: u32,
    pub tx_hashes: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct BatchRefsResponse {
    pub chain: &'static str,
    pub chain_id: u32,
    pub items: Vec<AddressTxRef>,
    pub per_address_counts: BTreeMap<String, usize>,
}

enum AuthContext {
    Customer(AuthenticatedApiKey),
    Internal,
    PublicTest,
}

struct TestTokenRateLimiter {
    requests: Mutex<HashMap<String, VecDeque<Instant>>>,
}

impl TestTokenRateLimiter {
    fn new() -> Self {
        Self {
            requests: Mutex::new(HashMap::new()),
        }
    }

    async fn allow(&self, key: &str, per_second: u32, burst: u32) -> bool {
        let max_in_window = per_second.max(burst).max(1) as usize;
        let cutoff = Instant::now() - Duration::from_secs(1);
        let mut requests = self.requests.lock().await;
        let bucket = requests.entry(key.to_owned()).or_default();
        while bucket.front().is_some_and(|instant| *instant < cutoff) {
            bucket.pop_front();
        }
        if bucket.len() >= max_in_window {
            return false;
        }
        bucket.push_back(Instant::now());
        true
    }
}

pub fn router(service: Arc<IndexerService>, admin_store: Arc<PostgresAdminStore>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/v1/chains/base/progress", get(progress))
        .route("/v1/base/address/{address}/txs", get(query_address_txs))
        .route(
            "/v1/base/address/{address}/tx-hashes",
            get(query_address_tx_hashes),
        )
        .route("/v1/base/addresses/txs", post(batch_query_address_txs))
        .route("/v1/base/tx/{tx_hash}", get(get_transaction))
        .route(
            "/v1/base/tx/{tx_hash}/receipt",
            get(get_transaction_receipt),
        )
        .route("/v1/base/block/latest", get(get_latest_block))
        .route("/v1/base/block/{block_number}", get(get_block))
        .with_state(PublicApiState {
            service,
            admin_store,
            test_rate_limiter: Arc::new(TestTokenRateLimiter::new()),
        })
}

async fn health() -> Json<serde_json::Value> {
    Json(json!({ "status": "ok", "service": "ledgerflow-indexer" }))
}

async fn progress(
    State(state): State<PublicApiState>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, ApiError> {
    let endpoint = "GET /v1/chains/base/progress";
    let auth = authorize(&state, &headers, endpoint).await?;
    let progress = match state.service.get_progress().await {
        Ok(progress) => progress,
        Err(err) => {
            record_failure(&state, &auth, endpoint, &err).await;
            return Err(ApiError::from(err));
        }
    };
    record_success(&state, &auth, endpoint).await?;
    Ok(Json(json!({
        "chain": "base",
        "chain_id": state.service.config.chain_id,
        "worker_id": state.service.config.worker_id,
        "last_indexed_block": progress,
    })))
}

async fn query_address_txs(
    State(state): State<PublicApiState>,
    Path(address): Path<String>,
    Query(query): Query<AddressTxQuery>,
    headers: HeaderMap,
) -> Result<Json<PublicRefsResponse>, ApiError> {
    let endpoint = "GET /v1/base/address/{address}/txs";
    let auth = authorize(&state, &headers, endpoint).await?;
    let direction = parse_direction(query.direction.as_deref())?;
    let cursor = parse_cursor(query.cursor.as_deref()).map_err(ApiError::from)?;
    let limit = effective_limit(
        &state,
        &auth,
        query.limit,
        state.service.config.query_limit_default,
        state.service.config.query_limit_max,
    );
    let response = match state
        .service
        .query_address(
            &address,
            Some(limit.saturating_add(1)),
            query.from_block,
            query.to_block,
            direction,
            cursor,
        )
        .await
    {
        Ok(response) => response,
        Err(err) => {
            record_failure(&state, &auth, endpoint, &err).await;
            return Err(ApiError::from(err));
        }
    };
    record_success(&state, &auth, endpoint).await?;
    let mut items = response.refs;
    let next_cursor = if items.len() > limit as usize {
        items.truncate(limit as usize);
        items.last().map(encode_cursor)
    } else {
        None
    };
    Ok(Json(PublicRefsResponse {
        chain: "base",
        chain_id: state.service.config.chain_id,
        address: response.address,
        limit,
        next_cursor,
        items,
    }))
}

async fn query_address_tx_hashes(
    State(state): State<PublicApiState>,
    Path(address): Path<String>,
    Query(query): Query<AddressTxQuery>,
    headers: HeaderMap,
) -> Result<Json<PublicHashesResponse>, ApiError> {
    let endpoint = "GET /v1/base/address/{address}/tx-hashes";
    let auth = authorize(&state, &headers, endpoint).await?;
    let direction = parse_direction(query.direction.as_deref())?;
    let limit = effective_limit(
        &state,
        &auth,
        query.limit,
        state.service.config.query_limit_default,
        state.service.config.query_limit_max,
    );
    let response = match state
        .service
        .query_address_hashes(
            &address,
            Some(limit),
            query.from_block,
            query.to_block,
            direction,
        )
        .await
    {
        Ok(response) => response,
        Err(err) => {
            record_failure(&state, &auth, endpoint, &err).await;
            return Err(ApiError::from(err));
        }
    };
    record_success(&state, &auth, endpoint).await?;
    Ok(Json(PublicHashesResponse {
        chain: "base",
        chain_id: state.service.config.chain_id,
        address: response.address,
        limit,
        tx_hashes: response.tx_hashes,
    }))
}

async fn batch_query_address_txs(
    State(state): State<PublicApiState>,
    headers: HeaderMap,
    Json(request): Json<BatchAddressTxRequest>,
) -> Result<Json<BatchRefsResponse>, ApiError> {
    let endpoint = "POST /v1/base/addresses/txs";
    let auth = authorize(&state, &headers, endpoint).await?;
    if request.addresses.len() > 50 {
        return Err(ApiError::conflict(
            "batch address lookup accepts at most 50 addresses".to_owned(),
        ));
    }
    let direction = parse_direction(request.direction.as_deref())?;
    let limit = effective_limit(
        &state,
        &auth,
        request.limit,
        state.service.config.query_limit_default,
        state.service.config.query_limit_max,
    );
    let mut items = Vec::new();
    let mut per_address_counts = BTreeMap::new();

    for address in request.addresses {
        let response = match state
            .service
            .query_address(
                &address,
                Some(limit),
                request.from_block,
                request.to_block,
                direction.clone(),
                None,
            )
            .await
        {
            Ok(response) => response,
            Err(err) => {
                record_failure(&state, &auth, endpoint, &err).await;
                return Err(ApiError::from(err));
            }
        };
        per_address_counts.insert(response.address, response.refs.len());
        items.extend(response.refs);
    }

    items.sort_by(|left, right| {
        right
            .block_number
            .cmp(&left.block_number)
            .then(right.tx_index.cmp(&left.tx_index))
            .then(left.address.cmp(&right.address))
    });

    record_success(&state, &auth, endpoint).await?;
    Ok(Json(BatchRefsResponse {
        chain: "base",
        chain_id: state.service.config.chain_id,
        items,
        per_address_counts,
    }))
}

async fn get_transaction(
    State(state): State<PublicApiState>,
    Path(tx_hash): Path<String>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    let endpoint = "GET /v1/base/tx/{tx_hash}";
    let auth = authorize(&state, &headers, endpoint).await?;
    let value = match state.service.get_transaction_by_hash(&tx_hash).await {
        Ok(value) => value,
        Err(err) => {
            record_failure(&state, &auth, endpoint, &err).await;
            return Err(ApiError::from(err));
        }
    };
    record_success(&state, &auth, endpoint).await?;
    Ok(Json(
        json!({ "chain": "base", "chain_id": state.service.config.chain_id, "transaction": value }),
    ))
}

async fn get_transaction_receipt(
    State(state): State<PublicApiState>,
    Path(tx_hash): Path<String>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    let endpoint = "GET /v1/base/tx/{tx_hash}/receipt";
    let auth = authorize(&state, &headers, endpoint).await?;
    let value = match state.service.get_transaction_receipt(&tx_hash).await {
        Ok(value) => value,
        Err(err) => {
            record_failure(&state, &auth, endpoint, &err).await;
            return Err(ApiError::from(err));
        }
    };
    record_success(&state, &auth, endpoint).await?;
    Ok(Json(
        json!({ "chain": "base", "chain_id": state.service.config.chain_id, "receipt": value }),
    ))
}

async fn get_latest_block(
    State(state): State<PublicApiState>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    let endpoint = "GET /v1/base/block/latest";
    let auth = authorize(&state, &headers, endpoint).await?;
    let block_number = match state.service.get_latest_block_number().await {
        Ok(block_number) => block_number,
        Err(err) => {
            record_failure(&state, &auth, endpoint, &err).await;
            return Err(ApiError::from(err));
        }
    };
    record_success(&state, &auth, endpoint).await?;
    Ok(Json(
        json!({ "chain": "base", "chain_id": state.service.config.chain_id, "block_number": block_number }),
    ))
}

async fn get_block(
    State(state): State<PublicApiState>,
    Path(block_number): Path<u64>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    let endpoint = "GET /v1/base/block/{block_number}";
    let auth = authorize(&state, &headers, endpoint).await?;
    let value = match state.service.get_block_by_number(block_number).await {
        Ok(value) => value,
        Err(err) => {
            record_failure(&state, &auth, endpoint, &err).await;
            return Err(ApiError::from(err));
        }
    };
    record_success(&state, &auth, endpoint).await?;
    Ok(Json(
        json!({ "chain": "base", "chain_id": state.service.config.chain_id, "block": value }),
    ))
}

async fn authorize(
    state: &PublicApiState,
    headers: &HeaderMap,
    endpoint: &str,
) -> Result<AuthContext, ApiError> {
    let api_key = bearer_token(headers).map_err(ApiError::from)?;
    if state
        .service
        .config
        .internal_api_token
        .as_deref()
        .is_some_and(|token| token == api_key)
    {
        return Ok(AuthContext::Internal);
    }
    if state
        .service
        .config
        .test_api_token
        .as_deref()
        .is_some_and(|token| token == api_key)
    {
        ensure_test_endpoint_allowed(endpoint)?;
        let client_key = format!("test:{}", client_ip_key(headers));
        let allowed = state
            .test_rate_limiter
            .allow(
                &client_key,
                state.service.config.test_api_rps,
                state.service.config.test_api_burst,
            )
            .await;
        if !allowed {
            return Err(ApiError::too_many_requests(
                "public test token rate limit exceeded for this IP".to_owned(),
            ));
        }
        return Ok(AuthContext::PublicTest);
    }
    state
        .admin_store
        .authenticate_api_key(api_key)
        .await
        .map(AuthContext::Customer)
        .map_err(ApiError::from)
}

async fn record_success(
    state: &PublicApiState,
    auth: &AuthContext,
    endpoint: &str,
) -> Result<(), ApiError> {
    let AuthContext::Customer(auth) = auth else {
        return Ok(());
    };
    state
        .admin_store
        .record_usage(
            auth.user_id,
            auth.key_id,
            endpoint,
            auth.quota_source.as_str(),
            "success",
        )
        .await
        .map_err(ApiError::from)?;
    Ok(())
}

async fn record_failure(
    state: &PublicApiState,
    auth: &AuthContext,
    endpoint: &str,
    err: &LedgerError,
) {
    let AuthContext::Customer(auth) = auth else {
        return;
    };
    let status = status_for_error(err).as_u16() as i32;
    let _ = state
        .admin_store
        .record_usage(
            auth.user_id,
            auth.key_id,
            endpoint,
            auth.quota_source.as_str(),
            "error",
        )
        .await;
    let _ = state
        .admin_store
        .record_request_failure(
            Some(auth.user_id),
            Some(auth.key_id),
            endpoint,
            Some(auth.quota_source.as_str()),
            status,
            &err.to_string(),
        )
        .await;
}

fn bearer_token(headers: &HeaderMap) -> LedgerResult<&str> {
    let value = headers
        .get(axum::http::header::AUTHORIZATION)
        .ok_or_else(|| LedgerError::Unauthorized("missing Authorization header".to_owned()))?
        .to_str()
        .map_err(|_| LedgerError::Unauthorized("invalid Authorization header".to_owned()))?;
    value
        .strip_prefix("Bearer ")
        .filter(|token| !token.trim().is_empty())
        .map(str::trim)
        .ok_or_else(|| LedgerError::Unauthorized("expected Bearer api key".to_owned()))
}

fn effective_limit(
    state: &PublicApiState,
    auth: &AuthContext,
    limit: Option<u32>,
    default: u32,
    max: u32,
) -> u32 {
    let limit = limit.unwrap_or(default).max(1).min(max);
    match auth {
        AuthContext::PublicTest => limit.min(state.service.config.test_api_max_limit.max(1)),
        AuthContext::Customer(_) | AuthContext::Internal => limit,
    }
}

fn ensure_test_endpoint_allowed(endpoint: &str) -> Result<(), ApiError> {
    match endpoint {
        "GET /v1/chains/base/progress"
        | "GET /v1/base/address/{address}/txs"
        | "GET /v1/base/address/{address}/tx-hashes" => Ok(()),
        _ => Err(ApiError::conflict(
            "public test token only supports progress and single-address lookup endpoints"
                .to_owned(),
        )),
    }
}

fn client_ip_key(headers: &HeaderMap) -> String {
    headers
        .get("x-forwarded-for")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(',').next())
        .or_else(|| {
            headers
                .get("x-real-ip")
                .and_then(|value| value.to_str().ok())
        })
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("unknown")
        .to_owned()
}

fn parse_cursor(cursor: Option<&str>) -> LedgerResult<Option<AddressTxCursor>> {
    let Some(cursor) = cursor.filter(|value| !value.trim().is_empty()) else {
        return Ok(None);
    };
    let parts = cursor.split(':').collect::<Vec<_>>();
    if parts.len() != 3 {
        return Err(LedgerError::MalformedEvidence(
            "cursor must be block_number:tx_index:direction".to_owned(),
        ));
    }
    let block_number = parts[0]
        .parse::<u32>()
        .map_err(|err| LedgerError::MalformedEvidence(format!("invalid cursor block: {err}")))?;
    let tx_index = parts[1]
        .parse::<u32>()
        .map_err(|err| LedgerError::MalformedEvidence(format!("invalid cursor tx index: {err}")))?;
    let direction_rank = parts[2].parse::<u8>().map_err(|err| {
        LedgerError::MalformedEvidence(format!("invalid cursor direction: {err}"))
    })?;
    if !matches!(direction_rank, 1 | 2) {
        return Err(LedgerError::MalformedEvidence(
            "cursor direction must be 1 or 2".to_owned(),
        ));
    }
    Ok(Some(AddressTxCursor {
        block_number,
        tx_index,
        direction_rank,
    }))
}

fn encode_cursor(item: &AddressTxRef) -> String {
    let direction_rank = match item.direction {
        AddressTxDirection::From => 1,
        AddressTxDirection::To => 2,
    };
    format!("{}:{}:{}", item.block_number, item.tx_index, direction_rank)
}

fn status_for_error(err: &LedgerError) -> StatusCode {
    match err {
        LedgerError::MalformedEvidence(_) => StatusCode::BAD_REQUEST,
        LedgerError::Conflict(_) => StatusCode::CONFLICT,
        LedgerError::Unauthorized(_) => StatusCode::UNAUTHORIZED,
        LedgerError::PaymentRequired(_) => StatusCode::PAYMENT_REQUIRED,
        LedgerError::TransientIngestion(_) | LedgerError::Persistence(_) => {
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    #[test]
    fn test_client_ip_key_prefers_forwarded_ip() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-forwarded-for",
            HeaderValue::from_static("203.0.113.8, 10.0.0.2"),
        );
        headers.insert("x-real-ip", HeaderValue::from_static("127.0.0.1"));
        assert_eq!(client_ip_key(&headers), "203.0.113.8");
    }

    #[test]
    fn test_demo_endpoints_are_restricted() {
        assert!(ensure_test_endpoint_allowed("GET /v1/chains/base/progress").is_ok());
        assert!(ensure_test_endpoint_allowed("GET /v1/base/address/{address}/txs").is_ok());
        assert!(ensure_test_endpoint_allowed("GET /v1/base/tx/{tx_hash}").is_err());
        assert!(ensure_test_endpoint_allowed("POST /v1/base/addresses/txs").is_err());
    }
}
