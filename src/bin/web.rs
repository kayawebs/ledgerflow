use ledgerflow::{
    core::logging::init_logging,
    web::{self, WebConfig},
};

#[tokio::main]
async fn main() {
    init_logging();
    let config = WebConfig::from_env();
    let bind_addr = config.bind_addr.clone();
    let app = web::build_router(config)
        .await
        .expect("web router should build");
    let listener = tokio::net::TcpListener::bind(&bind_addr)
        .await
        .expect("web bind should succeed");
    axum::serve(listener, app)
        .await
        .expect("web server should run");
}
