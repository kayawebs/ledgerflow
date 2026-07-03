use ledgerflow::{
    core::{env::load_dotenv, logging::init_logging},
    rpc_gateway::{RpcGatewayConfig, RpcGatewayState, router},
};
use tracing::info;

#[tokio::main]
async fn main() {
    load_dotenv();
    init_logging();
    let config = RpcGatewayConfig::from_env();
    let bind_addr = config.bind_addr;
    let upstream_url = config.upstream_url.clone();
    let max_in_flight = config.max_in_flight;
    let rps = config.rps;
    let burst = config.burst;
    let state = RpcGatewayState::new(config);
    info!(
        %bind_addr,
        %upstream_url,
        max_in_flight,
        rps,
        burst,
        "rpc gateway started"
    );
    let listener = tokio::net::TcpListener::bind(bind_addr)
        .await
        .expect("rpc gateway bind should succeed");
    axum::serve(listener, router(state))
        .await
        .expect("rpc gateway should run");
}
