use std::{
    env,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use axum::{
    Router,
    body::Bytes,
    extract::State,
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use reqwest::Client;
use serde::Serialize;
use serde_json::{Value, json};
use tokio::sync::{Mutex, Semaphore};
use tracing::{error, info, warn};

#[derive(Clone, Debug)]
pub struct RpcGatewayConfig {
    pub bind_addr: SocketAddr,
    pub upstream_url: String,
    pub max_in_flight: usize,
    pub rps: f64,
    pub burst: f64,
    pub upstream_timeout_ms: u64,
}

impl RpcGatewayConfig {
    pub fn from_env() -> Self {
        Self {
            bind_addr: env::var("LEDGERFLOW_RPC_GATEWAY_BIND_ADDR")
                .unwrap_or_else(|_| "0.0.0.0:8545".to_owned())
                .parse()
                .expect("LEDGERFLOW_RPC_GATEWAY_BIND_ADDR should be a socket address"),
            upstream_url: env::var("LEDGERFLOW_RPC_GATEWAY_UPSTREAM_URL")
                .or_else(|_| env::var("LEDGERFLOW_RPC_UPSTREAM_URL"))
                .unwrap_or_else(|_| "http://host.docker.internal:18545".to_owned()),
            max_in_flight: parse_usize("LEDGERFLOW_RPC_GATEWAY_MAX_IN_FLIGHT").unwrap_or(8),
            rps: parse_f64("LEDGERFLOW_RPC_GATEWAY_RPS").unwrap_or(20.0).max(0.1),
            burst: parse_f64("LEDGERFLOW_RPC_GATEWAY_BURST").unwrap_or(20.0).max(1.0),
            upstream_timeout_ms: parse_u64("LEDGERFLOW_RPC_GATEWAY_UPSTREAM_TIMEOUT_MS")
                .unwrap_or(30_000)
                .max(1_000),
        }
    }
}

#[derive(Clone)]
pub struct RpcGatewayState {
    config: Arc<RpcGatewayConfig>,
    client: Client,
    limiter: Arc<TokenBucket>,
    in_flight: Arc<Semaphore>,
}

impl RpcGatewayState {
    pub fn new(config: RpcGatewayConfig) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_millis(config.upstream_timeout_ms))
            .build()
            .expect("rpc gateway http client should build");
        let limiter = Arc::new(TokenBucket::new(config.rps, config.burst));
        let max_in_flight = config.max_in_flight.max(1);
        Self {
            config: Arc::new(config),
            client,
            limiter,
            in_flight: Arc::new(Semaphore::new(max_in_flight)),
        }
    }
}

pub fn router(state: RpcGatewayState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/", post(proxy_rpc))
        .with_state(state)
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    upstream_url: String,
    max_in_flight: usize,
    rps: f64,
    burst: f64,
}

async fn health(State(state): State<RpcGatewayState>) -> impl IntoResponse {
    axum::Json(HealthResponse {
        status: "ok",
        upstream_url: state.config.upstream_url.clone(),
        max_in_flight: state.config.max_in_flight,
        rps: state.config.rps,
        burst: state.config.burst,
    })
}

async fn proxy_rpc(State(state): State<RpcGatewayState>, body: Bytes) -> Response {
    let payload = match serde_json::from_slice::<Value>(&body) {
        Ok(payload) => payload,
        Err(err) => {
            return json_error_response(
                StatusCode::BAD_REQUEST,
                None,
                -32700,
                &format!("invalid json: {err}"),
            );
        }
    };
    let request_profile = profile_payload(&payload);
    let permit = match state.in_flight.clone().acquire_owned().await {
        Ok(permit) => permit,
        Err(err) => {
            return json_error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                request_profile.id,
                -32000,
                &format!("rpc gateway unavailable: {err}"),
            );
        }
    };
    let started_wait = Instant::now();
    state.limiter.acquire(request_profile.cost).await;
    let wait_ms = started_wait.elapsed().as_millis();

    let response = state
        .client
        .post(&state.config.upstream_url)
        .header(header::CONTENT_TYPE, "application/json")
        .body(body)
        .send()
        .await;
    drop(permit);

    match response {
        Ok(response) => {
            let status = response.status();
            let bytes = match response.bytes().await {
                Ok(bytes) => bytes,
                Err(err) => {
                    error!(error = %err, "rpc gateway failed reading upstream response");
                    return json_error_response(
                        StatusCode::BAD_GATEWAY,
                        request_profile.id,
                        -32000,
                        &format!("failed reading upstream response: {err}"),
                    );
                }
            };
            if wait_ms > 0 || request_profile.cost > 1.0 {
                info!(
                    methods = %request_profile.methods,
                    cost = request_profile.cost,
                    wait_ms,
                    status = status.as_u16(),
                    "rpc gateway forwarded request"
                );
            }
            response_from_upstream(status, bytes)
        }
        Err(err) => {
            warn!(
                methods = %request_profile.methods,
                cost = request_profile.cost,
                wait_ms,
                error = %err,
                "rpc gateway upstream request failed"
            );
            json_error_response(
                StatusCode::BAD_GATEWAY,
                request_profile.id,
                -32000,
                &format!("upstream rpc request failed: {err}"),
            )
        }
    }
}

fn response_from_upstream(status: StatusCode, bytes: Bytes) -> Response {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    (status, headers, bytes).into_response()
}

fn json_error_response(
    status: StatusCode,
    id: Option<Value>,
    code: i64,
    message: &str,
) -> Response {
    let body = json!({
        "jsonrpc": "2.0",
        "id": id.unwrap_or(Value::Null),
        "error": {
            "code": code,
            "message": message,
        }
    });
    (status, axum::Json(body)).into_response()
}

#[derive(Clone, Debug)]
struct RequestProfile {
    cost: f64,
    methods: String,
    id: Option<Value>,
}

fn profile_payload(payload: &Value) -> RequestProfile {
    match payload {
        Value::Array(items) => {
            let mut cost = 0.0;
            let mut methods = Vec::new();
            for item in items {
                let profile = profile_single(item);
                cost += profile.cost;
                methods.push(profile.methods);
            }
            RequestProfile {
                cost: cost.max(1.0),
                methods: methods.join(","),
                id: None,
            }
        }
        _ => profile_single(payload),
    }
}

fn profile_single(payload: &Value) -> RequestProfile {
    let method = payload
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    RequestProfile {
        cost: method_cost(method, payload),
        methods: method.to_owned(),
        id: payload.get("id").cloned(),
    }
}

fn method_cost(method: &str, payload: &Value) -> f64 {
    match method {
        "debug_traceTransaction" | "trace_replayTransaction" | "trace_transaction" => 10.0,
        "eth_getBlockReceipts" => 5.0,
        "eth_getBlockByNumber" if block_request_includes_transactions(payload) => 3.0,
        "eth_getLogs" => 4.0,
        "eth_call" => 2.0,
        "eth_getCode" | "eth_getBalance" | "eth_getTransactionReceipt" => 1.5,
        "eth_blockNumber" | "net_version" | "web3_clientVersion" => 0.25,
        _ => 1.0,
    }
}

fn block_request_includes_transactions(payload: &Value) -> bool {
    payload
        .get("params")
        .and_then(Value::as_array)
        .and_then(|params| params.get(1))
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

struct TokenBucket {
    state: Mutex<TokenBucketState>,
    refill_per_sec: f64,
    capacity: f64,
}

struct TokenBucketState {
    tokens: f64,
    last_refill: Instant,
}

impl TokenBucket {
    fn new(refill_per_sec: f64, capacity: f64) -> Self {
        Self {
            state: Mutex::new(TokenBucketState {
                tokens: capacity,
                last_refill: Instant::now(),
            }),
            refill_per_sec,
            capacity,
        }
    }

    async fn acquire(&self, requested_cost: f64) {
        let cost = requested_cost.clamp(0.1, self.capacity.max(0.1));
        loop {
            let wait = {
                let mut state = self.state.lock().await;
                let now = Instant::now();
                let elapsed = now.duration_since(state.last_refill).as_secs_f64();
                if elapsed > 0.0 {
                    state.tokens = (state.tokens + elapsed * self.refill_per_sec).min(self.capacity);
                    state.last_refill = now;
                }
                if state.tokens >= cost {
                    state.tokens -= cost;
                    return;
                }
                let missing = cost - state.tokens;
                Duration::from_secs_f64(missing / self.refill_per_sec)
            };
            tokio::time::sleep(wait).await;
        }
    }
}

fn parse_usize(key: &str) -> Option<usize> {
    env::var(key).ok().and_then(|value| value.parse().ok())
}

fn parse_u64(key: &str) -> Option<u64> {
    env::var(key).ok().and_then(|value| value.parse().ok())
}

fn parse_f64(key: &str) -> Option<f64> {
    env::var(key).ok().and_then(|value| value.parse().ok())
}
