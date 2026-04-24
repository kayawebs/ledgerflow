use std::env;

#[derive(Clone, Debug)]
pub struct IndexerConfig {
    pub bind_addr: String,
    pub enable_legacy_api: bool,
    pub rpc_url: String,
    pub clickhouse_url: String,
    pub clickhouse_database: String,
    pub clickhouse_user: String,
    pub clickhouse_password: Option<String>,
    pub admin_database_url: String,
    pub postmark_server_token: Option<String>,
    pub email_from: String,
    pub billing_treasury_address: String,
    pub billing_min_confirmations: u64,
    pub chain_id: u32,
    pub start_block: u64,
    pub end_block: Option<u64>,
    pub batch_size: u64,
    pub rpc_concurrency: usize,
    pub rpc_request_delay_ms: u64,
    pub rpc_retry_base_ms: u64,
    pub rpc_retry_max_ms: u64,
    pub rpc_max_retries: u32,
    pub live_follow: bool,
    pub poll_interval_ms: u64,
    pub worker_id: String,
    pub query_limit_default: u32,
    pub query_limit_max: u32,
    pub internal_api_token: Option<String>,
}

impl IndexerConfig {
    pub fn from_env() -> Self {
        Self {
            bind_addr: env::var("LEDGERFLOW_INDEXER_BIND_ADDR")
                .unwrap_or_else(|_| "0.0.0.0:8090".to_owned()),
            enable_legacy_api: parse_bool("LEDGERFLOW_INDEXER_ENABLE_LEGACY_API").unwrap_or(false),
            rpc_url: env::var("LEDGERFLOW_INDEXER_RPC_URL")
                .unwrap_or_else(|_| "http://127.0.0.1:18545".to_owned()),
            clickhouse_url: env::var("LEDGERFLOW_INDEXER_CLICKHOUSE_URL")
                .unwrap_or_else(|_| "http://127.0.0.1:8123".to_owned()),
            clickhouse_database: env::var("LEDGERFLOW_INDEXER_CLICKHOUSE_DATABASE")
                .unwrap_or_else(|_| "ledgerflow_indexer".to_owned()),
            clickhouse_user: env::var("LEDGERFLOW_INDEXER_CLICKHOUSE_USER")
                .unwrap_or_else(|_| "ledgerflow".to_owned()),
            clickhouse_password: env::var("LEDGERFLOW_INDEXER_CLICKHOUSE_PASSWORD").ok(),
            admin_database_url: env::var("LEDGERFLOW_INDEXER_ADMIN_DATABASE_URL").unwrap_or_else(
                |_| "postgres://postgres:postgres@127.0.0.1:5532/ledgerflow".to_owned(),
            ),
            postmark_server_token: env::var("LEDGERFLOW_POSTMARK_SERVER_TOKEN")
                .ok()
                .filter(|value| !value.is_empty()),
            email_from: env::var("LEDGERFLOW_EMAIL_FROM")
                .unwrap_or_else(|_| "verify@ledgerflow.local".to_owned()),
            billing_treasury_address: env::var("LEDGERFLOW_BILLING_TREASURY_ADDRESS")
                .unwrap_or_else(|_| "0x0000000000000000000000000000000000000000".to_owned()),
            billing_min_confirmations: parse_u64("LEDGERFLOW_BILLING_MIN_CONFIRMATIONS")
                .unwrap_or(6),
            chain_id: parse_u32("LEDGERFLOW_INDEXER_CHAIN_ID").unwrap_or(8453),
            start_block: parse_u64("LEDGERFLOW_INDEXER_START_BLOCK").unwrap_or(0),
            end_block: parse_u64("LEDGERFLOW_INDEXER_END_BLOCK"),
            batch_size: parse_u64("LEDGERFLOW_INDEXER_BATCH_SIZE").unwrap_or(400),
            rpc_concurrency: parse_usize("LEDGERFLOW_INDEXER_RPC_CONCURRENCY").unwrap_or(8),
            rpc_request_delay_ms: parse_u64("LEDGERFLOW_INDEXER_RPC_REQUEST_DELAY_MS")
                .unwrap_or(10),
            rpc_retry_base_ms: parse_u64("LEDGERFLOW_INDEXER_RPC_RETRY_BASE_MS").unwrap_or(1_000),
            rpc_retry_max_ms: parse_u64("LEDGERFLOW_INDEXER_RPC_RETRY_MAX_MS").unwrap_or(15_000),
            rpc_max_retries: parse_u32("LEDGERFLOW_INDEXER_RPC_MAX_RETRIES").unwrap_or(10),
            live_follow: parse_bool("LEDGERFLOW_INDEXER_LIVE_FOLLOW").unwrap_or(true),
            poll_interval_ms: parse_u64("LEDGERFLOW_INDEXER_POLL_INTERVAL_MS").unwrap_or(2_000),
            worker_id: env::var("LEDGERFLOW_INDEXER_WORKER_ID")
                .unwrap_or_else(|_| "base_mainnet_top_level".to_owned()),
            query_limit_default: parse_u32("LEDGERFLOW_INDEXER_QUERY_LIMIT_DEFAULT")
                .unwrap_or(1_000),
            query_limit_max: parse_u32("LEDGERFLOW_INDEXER_QUERY_LIMIT_MAX").unwrap_or(10_000),
            internal_api_token: env::var("LEDGERFLOW_INDEXER_INTERNAL_API_TOKEN")
                .ok()
                .filter(|value| !value.trim().is_empty()),
        }
    }
}

fn parse_u64(key: &str) -> Option<u64> {
    env::var(key)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
}

fn parse_u32(key: &str) -> Option<u32> {
    env::var(key)
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
}

fn parse_usize(key: &str) -> Option<usize> {
    env::var(key)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
}

fn parse_bool(key: &str) -> Option<bool> {
    env::var(key)
        .ok()
        .map(|value| matches!(value.as_str(), "true" | "1" | "yes"))
}
