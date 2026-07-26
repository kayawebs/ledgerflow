use std::env;

#[derive(Clone, Debug)]
pub struct IndexerConfig {
    pub bind_addr: String,
    pub internal_api_token: Option<String>,
    pub rpc_url: String,
    pub clickhouse_url: String,
    pub clickhouse_database: String,
    pub clickhouse_user: String,
    pub clickhouse_password: Option<String>,
    pub chain_id: u32,
    pub start_block: u64,
    pub end_block: Option<u64>,
    pub batch_size: u64,
    pub rpc_concurrency: usize,
    pub rpc_batch_request_size: usize,
    pub rpc_request_delay_ms: u64,
    pub rpc_retry_base_ms: u64,
    pub rpc_retry_max_ms: u64,
    pub rpc_max_retries: u32,
    pub lazy_enabled: bool,
    pub live_follow: bool,
    pub poll_interval_ms: u64,
    pub worker_id: String,
    pub query_limit_default: u32,
    pub query_limit_max: u32,
    pub endpoint_backfill_enabled: bool,
    pub endpoint_backfill_batch_size: u32,
}

impl IndexerConfig {
    pub fn from_env() -> Self {
        Self {
            bind_addr: env::var("LEDGERFLOW_INDEXER_BIND_ADDR")
                .unwrap_or_else(|_| "0.0.0.0:8090".to_owned()),
            internal_api_token: env::var("LEDGERFLOW_INDEXER_INTERNAL_API_TOKEN").ok(),
            rpc_url: env::var("LEDGERFLOW_INDEXER_RPC_URL")
                .or_else(|_| env::var("LEDGERFLOW_RPC_URL"))
                .unwrap_or_else(|_| "http://127.0.0.1:8545".to_owned()),
            clickhouse_url: env::var("LEDGERFLOW_INDEXER_CLICKHOUSE_URL")
                .unwrap_or_else(|_| "http://127.0.0.1:8123".to_owned()),
            clickhouse_database: env::var("LEDGERFLOW_INDEXER_CLICKHOUSE_DATABASE")
                .unwrap_or_else(|_| "ledgerflow_indexer".to_owned()),
            clickhouse_user: env::var("LEDGERFLOW_INDEXER_CLICKHOUSE_USER")
                .unwrap_or_else(|_| "default".to_owned()),
            clickhouse_password: env::var("LEDGERFLOW_INDEXER_CLICKHOUSE_PASSWORD").ok(),
            chain_id: parse_u32("LEDGERFLOW_INDEXER_CHAIN_ID").unwrap_or(8453),
            start_block: parse_u64("LEDGERFLOW_INDEXER_START_BLOCK").unwrap_or(0),
            end_block: parse_u64("LEDGERFLOW_INDEXER_END_BLOCK"),
            batch_size: parse_u64("LEDGERFLOW_INDEXER_BATCH_SIZE").unwrap_or(500),
            rpc_concurrency: parse_usize("LEDGERFLOW_INDEXER_RPC_CONCURRENCY").unwrap_or(8),
            rpc_batch_request_size: parse_usize("LEDGERFLOW_INDEXER_RPC_BATCH_REQUEST_SIZE")
                .unwrap_or(1),
            rpc_request_delay_ms: parse_u64("LEDGERFLOW_INDEXER_RPC_REQUEST_DELAY_MS").unwrap_or(0),
            rpc_retry_base_ms: parse_u64("LEDGERFLOW_INDEXER_RPC_RETRY_BASE_MS").unwrap_or(500),
            rpc_retry_max_ms: parse_u64("LEDGERFLOW_INDEXER_RPC_RETRY_MAX_MS").unwrap_or(10_000),
            rpc_max_retries: parse_u32("LEDGERFLOW_INDEXER_RPC_MAX_RETRIES").unwrap_or(8),
            lazy_enabled: parse_bool("LEDGERFLOW_INDEXER_LAZY_ENABLED").unwrap_or(true),
            live_follow: parse_bool("LEDGERFLOW_INDEXER_LIVE_FOLLOW").unwrap_or(true),
            poll_interval_ms: parse_u64("LEDGERFLOW_INDEXER_POLL_INTERVAL_MS").unwrap_or(2_000),
            worker_id: env::var("LEDGERFLOW_INDEXER_WORKER_ID")
                .unwrap_or_else(|_| "base_mainnet_top_level".to_owned()),
            query_limit_default: parse_u32("LEDGERFLOW_INDEXER_QUERY_LIMIT_DEFAULT")
                .unwrap_or(1_000),
            query_limit_max: parse_u32("LEDGERFLOW_INDEXER_QUERY_LIMIT_MAX").unwrap_or(10_000),
            endpoint_backfill_enabled: parse_bool("LEDGERFLOW_INDEXER_ENDPOINT_BACKFILL_ENABLED")
                .unwrap_or(true),
            endpoint_backfill_batch_size: parse_u32(
                "LEDGERFLOW_INDEXER_ENDPOINT_BACKFILL_BATCH_SIZE",
            )
            .unwrap_or(2_000),
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
