use std::env;

const DEFAULT_ENABLED_RECOGNIZERS: &[&str] = &[
    "selector_presence",
    "failed_execution_classifier",
    "erc20_transfer_flow",
    "erc20_approval",
    "native_transfer",
    "batch_router_heuristic",
    "swap_flow",
    "approval_swap_scenario",
    "bridge_like_transfer",
    "deposit_withdraw_pattern",
    "arbitrage_like_scenario",
    "liquidation_like_scenario",
];

#[derive(Clone, Debug)]
pub struct AppConfig {
    pub env: String,
    pub bind_addr: String,
    pub database_url: String,
    pub rpc_url: String,
    pub indexer_api_base_url: Option<String>,
    pub indexer_api_bearer_token: Option<String>,
    pub default_chain_id: u64,
    pub trace_enabled: bool,
    pub cluster_backfill_query_limit: u32,
    pub cluster_auto_backfill_on_create: bool,
    pub cluster_backfill_worker_poll_interval_ms: u64,
    pub cluster_continuous_watch_enabled: bool,
    pub cluster_continuous_watch_lookback_blocks: u64,
    pub trace_monitor_enabled: bool,
    pub trace_monitor_worker_id: String,
    pub trace_monitor_start_block: Option<u64>,
    pub trace_monitor_confirmations: u64,
    pub trace_monitor_block_batch_size: u64,
    pub trace_monitor_poll_interval_ms: u64,
    pub trace_monitor_retention_days: i64,
    pub trace_monitor_bind_addr: String,
    pub trace_monitor_report_enabled: bool,
    pub trace_monitor_report_interval_ms: u64,
    pub trace_monitor_report_window_minutes: i64,
    pub trace_monitor_report_top_n: u32,
    pub trace_monitor_report_min_raw_score: String,
    pub trace_monitor_report_run_on_start: bool,
    pub trace_monitor_report_email_to: String,
    pub trace_monitor_notify_min_usd: f64,
    pub telegram_bot_token: Option<String>,
    pub telegram_chat_id: Option<String>,
    pub price_worker_poll_interval_ms: u64,
    pub address_whale_threshold_usd: f64,
    pub email_from: Option<String>,
    pub email_delivery_mode: String,
    pub postmark_server_token: Option<String>,
    pub postmark_message_stream: String,
    pub resend_api_key: Option<String>,
    pub enabled_recognizers: Vec<String>,
    pub plugins: Vec<String>,
}

impl AppConfig {
    pub fn from_env() -> Self {
        Self {
            env: env::var("LEDGERFLOW_ENV").unwrap_or_else(|_| "local".to_owned()),
            bind_addr: env::var("LEDGERFLOW_BIND_ADDR")
                .unwrap_or_else(|_| "127.0.0.1:3000".to_owned()),
            database_url: env::var("LEDGERFLOW_DATABASE_URL").unwrap_or_else(|_| {
                "postgres://postgres:postgres@127.0.0.1:5532/ledgerflow".to_owned()
            }),
            rpc_url: env::var("LEDGERFLOW_RPC_URL")
                .unwrap_or_else(|_| "http://127.0.0.1:8545".to_owned()),
            indexer_api_base_url: env::var("LEDGERFLOW_INDEXER_API_BASE_URL").ok(),
            indexer_api_bearer_token: env::var("LEDGERFLOW_INDEXER_API_BEARER_TOKEN").ok(),
            default_chain_id: env::var("LEDGERFLOW_DEFAULT_CHAIN_ID")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(8453),
            trace_enabled: env::var("LEDGERFLOW_TRACE_ENABLED")
                .map(|v| matches!(v.as_str(), "true" | "1" | "yes"))
                .unwrap_or(false),
            cluster_backfill_query_limit: env::var("LEDGERFLOW_CLUSTER_BACKFILL_QUERY_LIMIT")
                .ok()
                .and_then(|v| v.parse::<u32>().ok())
                .unwrap_or(10_000),
            cluster_auto_backfill_on_create: env::var("LEDGERFLOW_CLUSTER_AUTO_BACKFILL_ON_CREATE")
                .map(|v| matches!(v.as_str(), "true" | "1" | "yes"))
                .unwrap_or(true),
            cluster_backfill_worker_poll_interval_ms: env::var(
                "LEDGERFLOW_CLUSTER_BACKFILL_WORKER_POLL_INTERVAL_MS",
            )
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(120_000),
            cluster_continuous_watch_enabled: env::var(
                "LEDGERFLOW_CLUSTER_CONTINUOUS_WATCH_ENABLED",
            )
            .map(|v| matches!(v.as_str(), "true" | "1" | "yes"))
            .unwrap_or(true),
            cluster_continuous_watch_lookback_blocks: env::var(
                "LEDGERFLOW_CLUSTER_CONTINUOUS_WATCH_LOOKBACK_BLOCKS",
            )
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(8),
            trace_monitor_enabled: env::var("LEDGERFLOW_TRACE_MONITOR_ENABLED")
                .map(|v| matches!(v.as_str(), "true" | "1" | "yes"))
                .unwrap_or(true),
            trace_monitor_worker_id: env::var("LEDGERFLOW_TRACE_MONITOR_WORKER_ID")
                .unwrap_or_else(|_| "base_mainnet_trace_monitor".to_owned()),
            trace_monitor_start_block: env::var("LEDGERFLOW_TRACE_MONITOR_START_BLOCK")
                .ok()
                .and_then(|v| v.parse::<u64>().ok()),
            trace_monitor_confirmations: env::var("LEDGERFLOW_TRACE_MONITOR_CONFIRMATIONS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(3),
            trace_monitor_block_batch_size: env::var("LEDGERFLOW_TRACE_MONITOR_BLOCK_BATCH_SIZE")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(30),
            trace_monitor_poll_interval_ms: env::var("LEDGERFLOW_TRACE_MONITOR_POLL_INTERVAL_MS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(10_000),
            trace_monitor_retention_days: env::var("LEDGERFLOW_TRACE_MONITOR_RETENTION_DAYS")
                .ok()
                .and_then(|v| v.parse::<i64>().ok())
                .unwrap_or(7),
            trace_monitor_bind_addr: env::var("LEDGERFLOW_TRACE_MONITOR_BIND_ADDR")
                .unwrap_or_else(|_| "127.0.0.1:3010".to_owned()),
            trace_monitor_report_enabled: env::var("LEDGERFLOW_TRACE_MONITOR_REPORT_ENABLED")
                .map(|v| matches!(v.as_str(), "true" | "1" | "yes"))
                .unwrap_or(true),
            trace_monitor_report_interval_ms: env::var(
                "LEDGERFLOW_TRACE_MONITOR_REPORT_INTERVAL_MS",
            )
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(3_600_000),
            trace_monitor_report_window_minutes: env::var(
                "LEDGERFLOW_TRACE_MONITOR_REPORT_WINDOW_MINUTES",
            )
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(60),
            trace_monitor_report_top_n: env::var("LEDGERFLOW_TRACE_MONITOR_REPORT_TOP_N")
                .ok()
                .and_then(|v| v.parse::<u32>().ok())
                .unwrap_or(10),
            trace_monitor_report_min_raw_score: env::var(
                "LEDGERFLOW_TRACE_MONITOR_REPORT_MIN_RAW_SCORE",
            )
            .unwrap_or_else(|_| "0".to_owned()),
            trace_monitor_report_run_on_start: env::var(
                "LEDGERFLOW_TRACE_MONITOR_REPORT_RUN_ON_START",
            )
            .map(|v| matches!(v.as_str(), "true" | "1" | "yes"))
            .unwrap_or(true),
            trace_monitor_report_email_to: env::var("LEDGERFLOW_TRACE_MONITOR_REPORT_EMAIL_TO")
                .unwrap_or_else(|_| "wcy19960411@gmail.com".to_owned()),
            trace_monitor_notify_min_usd: env::var("LEDGERFLOW_TRACE_MONITOR_NOTIFY_MIN_USD")
                .ok()
                .and_then(|v| v.parse::<f64>().ok())
                .unwrap_or(10_000.0),
            telegram_bot_token: env::var("LEDGERFLOW_TELEGRAM_BOT_TOKEN").ok(),
            telegram_chat_id: env::var("LEDGERFLOW_TELEGRAM_CHAT_ID").ok(),
            price_worker_poll_interval_ms: env::var("LEDGERFLOW_PRICE_WORKER_POLL_INTERVAL_MS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(10_000),
            address_whale_threshold_usd: env::var("LEDGERFLOW_ADDRESS_WHALE_THRESHOLD_USD")
                .ok()
                .and_then(|v| v.parse::<f64>().ok())
                .unwrap_or(100_000.0),
            email_from: env::var("LEDGERFLOW_EMAIL_FROM").ok(),
            email_delivery_mode: env::var("LEDGERFLOW_EMAIL_DELIVERY_MODE")
                .unwrap_or_else(|_| "auto".to_owned()),
            postmark_server_token: env::var("LEDGERFLOW_POSTMARK_SERVER_TOKEN")
                .or_else(|_| env::var("POSTMARK_SERVER_TOKEN"))
                .ok(),
            postmark_message_stream: env::var("LEDGERFLOW_POSTMARK_MESSAGE_STREAM")
                .unwrap_or_else(|_| "outbound".to_owned()),
            resend_api_key: env::var("LEDGERFLOW_RESEND_API_KEY")
                .or_else(|_| env::var("RESEND_API_KEY"))
                .ok(),
            enabled_recognizers: parse_enabled_recognizers(
                env::var("LEDGERFLOW_ENABLED_RECOGNIZERS").ok().as_deref(),
            ),
            plugins: parse_list(
                &env::var("LEDGERFLOW_PLUGINS")
                    .unwrap_or_else(|_| "sample_builtin_pack".to_owned()),
            ),
        }
    }
}

fn parse_list(input: &str) -> Vec<String> {
    input
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn parse_enabled_recognizers(input: Option<&str>) -> Vec<String> {
    let mut recognizers = DEFAULT_ENABLED_RECOGNIZERS
        .iter()
        .map(|value| (*value).to_owned())
        .collect::<Vec<_>>();
    if let Some(input) = input {
        for recognizer_id in parse_list(input) {
            if !recognizers
                .iter()
                .any(|existing| existing == &recognizer_id)
            {
                recognizers.push(recognizer_id);
            }
        }
    }
    recognizers
}
