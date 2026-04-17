use std::env;

#[derive(Clone, Debug)]
pub struct IndexerConfig {
    pub bind_addr: String,
    pub rpc_url: String,
    pub clickhouse_url: String,
    pub clickhouse_database: String,
    pub clickhouse_user: String,
    pub clickhouse_password: Option<String>,
    pub chain_id: u32,
    pub start_block: u64,
    pub end_block: Option<u64>,
    pub batch_size: u64,
    pub live_follow: bool,
    pub poll_interval_ms: u64,
    pub worker_id: String,
    pub query_limit_default: u32,
    pub query_limit_max: u32,
}

impl IndexerConfig {
    pub fn from_env() -> Self {
        Self {
            bind_addr: env::var("LEDGERFLOW_INDEXER_BIND_ADDR")
                .unwrap_or_else(|_| "0.0.0.0:8090".to_owned()),
            rpc_url: env::var("LEDGERFLOW_INDEXER_RPC_URL")
                .or_else(|_| env::var("LEDGERFLOW_RPC_URL"))
                .unwrap_or_else(|_| "https://mainnet.base.org".to_owned()),
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
            batch_size: parse_u64("LEDGERFLOW_INDEXER_BATCH_SIZE").unwrap_or(100),
            live_follow: parse_bool("LEDGERFLOW_INDEXER_LIVE_FOLLOW").unwrap_or(true),
            poll_interval_ms: parse_u64("LEDGERFLOW_INDEXER_POLL_INTERVAL_MS").unwrap_or(2_000),
            worker_id: env::var("LEDGERFLOW_INDEXER_WORKER_ID")
                .unwrap_or_else(|_| "base_mainnet_top_level".to_owned()),
            query_limit_default: parse_u32("LEDGERFLOW_INDEXER_QUERY_LIMIT_DEFAULT")
                .unwrap_or(1_000),
            query_limit_max: parse_u32("LEDGERFLOW_INDEXER_QUERY_LIMIT_MAX").unwrap_or(10_000),
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

fn parse_bool(key: &str) -> Option<bool> {
    env::var(key)
        .ok()
        .map(|value| matches!(value.as_str(), "true" | "1" | "yes"))
}
