use std::env;

#[derive(Clone, Debug)]
pub struct AppConfig {
    pub env: String,
    pub bind_addr: String,
    pub database_url: String,
    pub rpc_url: String,
    pub default_chain_id: u64,
    pub trace_enabled: bool,
    pub enabled_recognizers: Vec<String>,
    pub plugins: Vec<String>,
}

impl AppConfig {
    pub fn from_env() -> Self {
        Self {
            env: env::var("LEDGERFLOW_ENV").unwrap_or_else(|_| "local".to_owned()),
            bind_addr: env::var("LEDGERFLOW_BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".to_owned()),
            database_url: env::var("LEDGERFLOW_DATABASE_URL")
                .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/ledgerflow".to_owned()),
            rpc_url: env::var("LEDGERFLOW_RPC_URL").unwrap_or_else(|_| "https://mainnet.base.org".to_owned()),
            default_chain_id: env::var("LEDGERFLOW_DEFAULT_CHAIN_ID")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(8453),
            trace_enabled: env::var("LEDGERFLOW_TRACE_ENABLED")
                .map(|v| matches!(v.as_str(), "true" | "1" | "yes"))
                .unwrap_or(false),
            enabled_recognizers: parse_list(
                &env::var("LEDGERFLOW_ENABLED_RECOGNIZERS").unwrap_or_else(|_| {
                    "selector_presence,erc20_transfer_flow,native_transfer,batch_router_heuristic,approval_swap_scenario"
                        .to_owned()
                }),
            ),
            plugins: parse_list(&env::var("LEDGERFLOW_PLUGINS").unwrap_or_else(|_| "sample_builtin_pack".to_owned())),
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
