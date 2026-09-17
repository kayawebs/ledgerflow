use ledgerflow::{
    core::{env::load_dotenv, logging::init_logging},
    market_web,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    load_dotenv();
    init_logging();
    let (router, bind_addr) = market_web::router_from_env().await?;
    let listener = tokio::net::TcpListener::bind(&bind_addr).await?;
    tracing::info!(%bind_addr, "LedgerFlow market web listening");
    axum::serve(listener, router).await?;
    Ok(())
}
