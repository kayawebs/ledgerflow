use std::env;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexerWorkerMode {
    Lazy,
    Live,
    History,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexerHistoryReceiptMode {
    Block,
    Transaction,
}

impl IndexerHistoryReceiptMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Block => "block",
            Self::Transaction => "transaction",
        }
    }

    fn from_env() -> Self {
        match env::var("LEDGERFLOW_INDEXER_HISTORY_RECEIPT_MODE")
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str()
        {
            "block" => Self::Block,
            _ => Self::Transaction,
        }
    }
}

impl IndexerWorkerMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Lazy => "lazy",
            Self::Live => "live",
            Self::History => "history",
        }
    }

    fn from_env(lazy_enabled: bool) -> Self {
        match env::var("LEDGERFLOW_INDEXER_WORKER_MODE")
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str()
        {
            "live" => Self::Live,
            "history" => Self::History,
            "lazy" => Self::Lazy,
            _ if lazy_enabled => Self::Lazy,
            _ => Self::Live,
        }
    }
}

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
    pub rpc_request_timeout_ms: u64,
    pub rpc_retry_base_ms: u64,
    pub rpc_retry_max_ms: u64,
    pub rpc_max_retries: u32,
    pub lazy_enabled: bool,
    pub lazy_worker_enabled: bool,
    pub lazy_max_range_blocks: u64,
    pub worker_mode: IndexerWorkerMode,
    pub api_enabled: bool,
    pub live_follow: bool,
    pub live_head_lag_blocks: u64,
    pub live_bootstrap_blocks: u64,
    pub history_head_lag_blocks: u64,
    pub history_boundary_worker_id: String,
    pub history_include_receipts: bool,
    pub history_receipt_mode: IndexerHistoryReceiptMode,
    pub reorg_lookback_blocks: u64,
    pub inter_batch_delay_ms: u64,
    pub poll_interval_ms: u64,
    pub worker_id: String,
    pub query_limit_default: u32,
    pub query_limit_max: u32,
    pub endpoint_backfill_enabled: bool,
    pub endpoint_backfill_batch_size: u32,
}

impl IndexerConfig {
    pub fn from_env() -> Self {
        let lazy_enabled = parse_bool("LEDGERFLOW_INDEXER_LAZY_ENABLED").unwrap_or(true);
        let worker_mode = IndexerWorkerMode::from_env(lazy_enabled);
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
            rpc_request_timeout_ms: parse_u64("LEDGERFLOW_INDEXER_RPC_REQUEST_TIMEOUT_MS")
                .unwrap_or(30_000)
                .max(1_000),
            rpc_retry_base_ms: parse_u64("LEDGERFLOW_INDEXER_RPC_RETRY_BASE_MS").unwrap_or(500),
            rpc_retry_max_ms: parse_u64("LEDGERFLOW_INDEXER_RPC_RETRY_MAX_MS").unwrap_or(10_000),
            rpc_max_retries: parse_u32("LEDGERFLOW_INDEXER_RPC_MAX_RETRIES").unwrap_or(8),
            lazy_enabled,
            lazy_worker_enabled: parse_bool("LEDGERFLOW_INDEXER_LAZY_WORKER_ENABLED")
                .unwrap_or(true),
            lazy_max_range_blocks: parse_u64("LEDGERFLOW_INDEXER_LAZY_MAX_RANGE_BLOCKS")
                .unwrap_or(250_000)
                .max(1),
            worker_mode,
            api_enabled: parse_bool("LEDGERFLOW_INDEXER_API_ENABLED").unwrap_or(true),
            live_follow: parse_bool("LEDGERFLOW_INDEXER_LIVE_FOLLOW").unwrap_or(true),
            live_head_lag_blocks: parse_u64("LEDGERFLOW_INDEXER_LIVE_HEAD_LAG_BLOCKS").unwrap_or(2),
            live_bootstrap_blocks: parse_u64("LEDGERFLOW_INDEXER_LIVE_BOOTSTRAP_BLOCKS")
                .unwrap_or(128),
            history_head_lag_blocks: parse_u64("LEDGERFLOW_INDEXER_HISTORY_HEAD_LAG_BLOCKS")
                .unwrap_or(128),
            history_boundary_worker_id: env::var("LEDGERFLOW_INDEXER_HISTORY_BOUNDARY_WORKER_ID")
                .unwrap_or_else(|_| "robinhood_live".to_owned()),
            history_include_receipts: parse_bool("LEDGERFLOW_INDEXER_HISTORY_INCLUDE_RECEIPTS")
                .unwrap_or(true),
            history_receipt_mode: IndexerHistoryReceiptMode::from_env(),
            reorg_lookback_blocks: parse_u64("LEDGERFLOW_INDEXER_REORG_LOOKBACK_BLOCKS")
                .unwrap_or(128),
            inter_batch_delay_ms: parse_u64("LEDGERFLOW_INDEXER_INTER_BATCH_DELAY_MS").unwrap_or(0),
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
