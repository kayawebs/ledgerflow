use std::sync::Arc;

use ledgerflow::{
    api,
    core::{config::AppConfig, env::load_dotenv, logging::init_logging},
    domain::interfaces::ClusterTxSource,
    ingestion::{fetchers::TransactionBundleFetcher, rpc::HttpRpcClient},
    plugins::loader::load_plugins,
    recognition::registry::RecognizerRegistry,
    repositories::sqlx::SqlxAnalysisRepository,
    services::{
        analysis::AnalysisService,
        behavior_assertions::BehaviorReviewService,
        cluster_backfill::ClusterBackfillService,
        discovery::ClusterDiscoveryService,
        indexer_client::IndexerHttpClient,
        observation_intelligence::{ObservationIntelligenceConfig, ObservationIntelligenceService},
        pricing::PriceService,
        searcher_cluster_intelligence::{
            SearcherClusterConfig, SearcherClusterIntelligenceService,
        },
        token_metadata::TokenMetadataService,
    },
};
use std::sync::Arc as StdArc;

#[tokio::main]
async fn main() {
    load_dotenv();
    init_logging();
    let config = AppConfig::from_env();

    let mut registry = RecognizerRegistry::new();
    let plugin_context = load_plugins(&config.plugins, &mut registry);

    let repository = SqlxAnalysisRepository::connect(&config.database_url)
        .await
        .expect("database connection should succeed");
    sqlx::migrate!("./migrations")
        .run(repository.pool())
        .await
        .expect("migrations should succeed");
    let behavior_review_service = Arc::new(BehaviorReviewService::new(repository.pool().clone()));
    let pilot_pool = repository.pool().clone();
    let rpc_client = StdArc::new(HttpRpcClient::new(config.rpc_url.clone()));
    let fetcher = TransactionBundleFetcher::new(rpc_client.clone(), config.trace_enabled);
    let token_metadata_service = Arc::new(TokenMetadataService::new(
        repository.pool().clone(),
        rpc_client.clone(),
    ));
    let price_service = Arc::new(PriceService::new(repository.pool().clone()));
    let service = Arc::new(
        AnalysisService::new(
            fetcher,
            registry,
            plugin_context,
            Arc::new(repository),
            config.enabled_recognizers.clone(),
        )
        .with_token_metadata_service(token_metadata_service)
        .with_price_service(price_service),
    );
    let tx_source = config.indexer_api_base_url.clone().map(|url| {
        Arc::new(IndexerHttpClient::new(
            url,
            config.indexer_api_bearer_token.clone(),
        )) as Arc<dyn ClusterTxSource>
    });
    let observation_components = config.indexer_api_base_url.clone().map(|url| {
        let observation_source = Arc::new(
            IndexerHttpClient::new(url, config.indexer_api_bearer_token.clone()).known_only(),
        ) as Arc<dyn ClusterTxSource>;
        let observation_service = Arc::new(ObservationIntelligenceService::new(
            pilot_pool.clone(),
            rpc_client.clone(),
            service.clone(),
            observation_source.clone(),
            ObservationIntelligenceConfig {
                chain_id: config.default_chain_id,
                ..ObservationIntelligenceConfig::default()
            },
        ));
        let searcher_cluster_service = Arc::new(SearcherClusterIntelligenceService::new(
            pilot_pool.clone(),
            rpc_client.clone(),
            service.clone(),
            observation_source,
            SearcherClusterConfig {
                chain_id: config.default_chain_id,
                ..SearcherClusterConfig::default()
            },
        ));
        (observation_service, searcher_cluster_service)
    });
    let observation_service = observation_components
        .as_ref()
        .map(|(observation, _)| observation.clone());
    let searcher_cluster_service = observation_components
        .as_ref()
        .map(|(_, cluster)| cluster.clone());
    let discovery_service = tx_source
        .clone()
        .map(|source| Arc::new(ClusterDiscoveryService::new(source, rpc_client.clone())));
    let mut backfill_service = ClusterBackfillService::new(
        Arc::clone(&service),
        tx_source,
        config.cluster_backfill_query_limit,
        config.cluster_continuous_watch_lookback_blocks,
    );
    if let Some(discovery_service) = discovery_service.clone() {
        backfill_service = backfill_service.with_discovery_service(discovery_service);
    }
    let backfill_service = Arc::new(backfill_service);

    let listener = tokio::net::TcpListener::bind(&config.bind_addr)
        .await
        .expect("bind should succeed");
    axum::serve(
        listener,
        api::router(
            service,
            backfill_service,
            rpc_client,
            behavior_review_service,
            config.cluster_auto_backfill_on_create,
            config.default_chain_id,
            pilot_pool,
            observation_service,
            searcher_cluster_service,
        ),
    )
    .await
    .expect("server should run");
}
