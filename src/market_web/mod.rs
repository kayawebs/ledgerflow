use std::{collections::BTreeMap, env, net::SocketAddr, str::FromStr, sync::Arc, time::Duration};

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{Html, IntoResponse, Response},
    routing::{get, post},
};
use chrono::{DateTime, Utc};
use k256::ecdsa::{RecoveryId, Signature, VerifyingKey};
use num_bigint::BigInt;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest as Sha2Digest, Sha256};
use sha3::{Digest as Sha3Digest, Keccak256};
use sqlx::{Row, postgres::PgPoolOptions};
use tokio::sync::RwLock;
use uuid::Uuid;

mod analytics;
mod execution;
mod intel;
mod og;
mod pages;
mod ratelimit;
mod share;
mod ui;

use crate::services::market_data::run_market_migrations;
use crate::services::market_pnl::EarningsWindow;
use crate::services::market_read::{MarketReadService, TokenBoard};
use crate::services::protocol_market::{
    MarketAddressLookup, MarketLaunch, MarketOverviewStats, ProtocolClusterCard,
    ProtocolClusterDetail, ProtocolMarketService, VerifiedEarningsRankingRow, VerifiedExecutionRow,
};

const SESSION_COOKIE: &str = "ledgerflow_market_session";
const ROBINHOOD_CHAIN_ID: u64 = 4663;

#[derive(Clone)]
pub struct MarketWebState {
    service: Arc<ProtocolMarketService>,
    market: Arc<MarketReadService>,
    overview_cache: Arc<RwLock<Option<OverviewCache>>>,
    client: Client,
    base_evidence_url: String,
    public_base_url: String,
    remote_cache_ttl: Duration,
    wallet_challenge_ttl: chrono::Duration,
    wallet_session_ttl: chrono::Duration,
    remote_evidence: Arc<RwLock<Option<RemoteEvidenceCache>>>,
    og_cache: Arc<RwLock<BTreeMap<String, CachedCard>>>,
}

/// A rendered social card. Cards are pure functions of numbers that move every
/// few seconds, so they are cached briefly and served with a longer edge TTL.
#[derive(Clone)]
struct CachedCard {
    rendered_at: std::time::Instant,
    bytes: Arc<Vec<u8>>,
}

const OG_CACHE_TTL: Duration = Duration::from_secs(120);
const OG_CACHE_CAPACITY: usize = 256;

/// Social crawlers need absolute URLs, and they fetch them without a request
/// of ours to copy the host from, so the public origin is remembered here.
static PUBLIC_BASE_URL: std::sync::OnceLock<String> = std::sync::OnceLock::new();

fn public_base_url() -> &'static str {
    PUBLIC_BASE_URL
        .get()
        .map(String::as_str)
        .unwrap_or("https://ledgerflow.top")
}

/// Overview aggregates change at most every few seconds; caching them keeps
/// the landing page and its refresh fragments off the database.
#[derive(Clone)]
struct OverviewCache {
    loaded_at: std::time::Instant,
    ticker: crate::services::market_read::MarketTicker,
    hot: Vec<crate::services::market_read::TokenRow>,
    gainers: Vec<crate::services::market_read::TokenRow>,
    newest: Vec<crate::services::market_read::TokenRow>,
    graduating: Vec<crate::services::market_read::TokenRow>,
}

const OVERVIEW_CACHE_TTL: Duration = Duration::from_secs(10);

#[derive(Clone)]
struct RemoteEvidenceCache {
    loaded_at: std::time::Instant,
    records: Arc<Vec<Value>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Locale {
    En,
    Zh,
}

impl Locale {
    fn from_query(value: Option<&str>) -> Self {
        if value.is_some_and(|value| value.eq_ignore_ascii_case("zh")) {
            Self::Zh
        } else {
            Self::En
        }
    }

    fn code(self) -> &'static str {
        match self {
            Self::En => "en",
            Self::Zh => "zh",
        }
    }

    fn text<'a>(self, en: &'a str, zh: &'a str) -> &'a str {
        match self {
            Self::En => en,
            Self::Zh => zh,
        }
    }

    /// Same choice for strings that have to be built before they are picked.
    fn text_owned(self, en: String, zh: String) -> String {
        match self {
            Self::En => en,
            Self::Zh => zh,
        }
    }
}

#[derive(Debug, Deserialize, Default)]
struct PageQuery {
    lang: Option<String>,
    chain_id: Option<u64>,
    window: Option<String>,
    address: Option<String>,
    category: Option<String>,
    tab: Option<String>,
    sort: Option<String>,
    dir: Option<String>,
    q: Option<String>,
    page: Option<i64>,
    interval: Option<i32>,
    period: Option<String>,
}

#[derive(Clone, Copy)]
struct PageContext<'a> {
    locale: Locale,
    chain_id: u64,
    window: &'a str,
}

impl<'a> PageContext<'a> {
    fn new(query: &'a PageQuery) -> Self {
        Self {
            locale: Locale::from_query(query.lang.as_deref()),
            chain_id: if query.chain_id == Some(8453) {
                8453
            } else {
                ROBINHOOD_CHAIN_ID
            },
            window: normalize_window(query.window.as_deref()),
        }
    }

    fn url(self, path: &str) -> String {
        format!(
            "{path}?lang={}&chain_id={}&window={}",
            self.locale.code(),
            self.chain_id,
            self.window
        )
    }
}

#[derive(Debug, Deserialize)]
struct WalletChallengeRequest {
    address: String,
    chain_id: Option<u64>,
}

#[derive(Debug, Serialize)]
struct WalletChallengeResponse {
    challenge_id: Uuid,
    message: String,
    expires_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
struct WalletVerifyRequest {
    challenge_id: Uuid,
    address: String,
    signature: String,
}

#[derive(Debug, Serialize)]
struct WalletSessionResponse {
    authenticated: bool,
    address: Option<String>,
}

#[derive(Debug)]
struct MarketWebError {
    status: StatusCode,
    message: String,
}

impl IntoResponse for MarketWebError {
    fn into_response(self) -> Response {
        (self.status, Json(json!({ "error": self.message }))).into_response()
    }
}

impl From<crate::core::errors::LedgerError> for MarketWebError {
    fn from(error: crate::core::errors::LedgerError) -> Self {
        internal_error(error)
    }
}

pub async fn router_from_env() -> Result<(Router, SocketAddr), Box<dyn std::error::Error>> {
    let database_url = env::var("LEDGERFLOW_MARKET_DATABASE_URL")
        .or_else(|_| env::var("LEDGERFLOW_DATABASE_URL"))?;
    let bind_addr = env::var("LEDGERFLOW_MARKET_WEB_BIND_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:3001".to_owned())
        .parse::<SocketAddr>()?;
    let public_base_url = env::var("LEDGERFLOW_MARKET_WEB_BASE_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:3001".to_owned());
    let public_base_url_setting = public_base_url.clone();
    let base_evidence_url = env::var("LEDGERFLOW_MARKET_BASE_EVIDENCE_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:3201/public/evidence/events.json".to_owned());
    let remote_cache_ttl = env::var("LEDGERFLOW_MARKET_REMOTE_CACHE_TTL_SECONDS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or_else(|| Duration::from_secs(30));
    let wallet_challenge_ttl = env_duration("LEDGERFLOW_MARKET_WALLET_CHALLENGE_TTL_SECONDS", 300);
    let wallet_session_ttl = env_duration("LEDGERFLOW_MARKET_WALLET_SESSION_TTL_SECONDS", 604_800);
    let pool = PgPoolOptions::new()
        .max_connections(12)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&database_url)
        .await?;
    sqlx::migrate!("./migrations").run(&pool).await?;
    run_market_migrations(&database_url).await?;
    let _ = PUBLIC_BASE_URL.set(public_base_url_setting.clone());
    let state = MarketWebState {
        market: Arc::new(MarketReadService::new(pool.clone())),
        overview_cache: Arc::new(RwLock::new(None)),
        service: Arc::new(ProtocolMarketService::new(pool)),
        client: Client::builder().timeout(Duration::from_secs(8)).build()?,
        base_evidence_url,
        public_base_url,
        remote_cache_ttl,
        wallet_challenge_ttl,
        wallet_session_ttl,
        remote_evidence: Arc::new(RwLock::new(None)),
        og_cache: Arc::new(RwLock::new(BTreeMap::new())),
    };
    let warm_state = state.clone();
    tokio::spawn(async move {
        if let Err(error) = remote_evidence_records(&warm_state).await {
            tracing::warn!(error = %error.message, "Base public evidence cache warm-up failed");
        }
    });
    Ok((router(state), bind_addr))
}

pub fn router(state: MarketWebState) -> Router {
    let analytics = Arc::new(analytics::AnalyticsState {
        recorder: analytics::Recorder::spawn(state.service.pool().clone()),
        // A per-process salt: visitor digests cannot be reproduced by anyone
        // holding the table, and they reset whenever the site restarts.
        salt: Uuid::new_v4().to_string(),
    });
    let limiter = Arc::new(ratelimit::RateLimiter::new(
        env_u32("LEDGERFLOW_MARKET_RATE_LIMIT_PER_MINUTE", 300),
        env_u32("LEDGERFLOW_MARKET_CARD_RATE_LIMIT_PER_MINUTE", 60),
    ));
    Router::new()
        .route("/", get(overview_page))
        .route("/launches", get(launches_page))
        .route("/launches/{launch_id}", get(launch_detail_page))
        .route("/clusters", get(clusters_page))
        .route("/clusters/{cluster_id}", get(cluster_detail_page))
        .route("/address/{address}", get(address_page))
        .route("/arbitrage", get(arbitrage_page))
        .route("/liquidations", get(liquidations_page))
        .route("/seed-library", get(seed_library_page))
        .route("/assets/ledgerflow-mark.svg", get(brand_mark_svg))
        .route("/lookup", get(wallet_lookup))
        .route("/og/home.png", get(og_home_card))
        .route("/og/launch/{launch_id}/card.png", get(og_launch_card))
        .route("/og/address/{address}/card.png", get(og_address_card))
        .route("/og/board/{board}/card.png", get(og_board_card))
        .route("/og/wallets/{window}/card.png", get(og_wallets_card))
        .route("/robots.txt", get(robots_txt))
        .route("/sitemap.xml", get(sitemap_xml))
        .route("/auth/wallet/challenge", post(wallet_challenge))
        .route("/auth/wallet/verify", post(wallet_verify))
        .route("/auth/wallet/logout", post(wallet_logout))
        .route("/api/session", get(wallet_session))
        .route("/fragments/ticker", get(ticker_fragment))
        .route("/fragments/overview/boards", get(overview_boards_fragment))
        .route("/fragments/overview/feed", get(overview_feed_fragment))
        .route("/fragments/launches", get(launch_table_fragment))
        .route("/fragments/arbitrage/feed", get(arbitrage_feed_fragment))
        .route(
            "/fragments/launches/{launch_id}/trades",
            get(token_trades_fragment),
        )
        .route("/api/market/overview", get(market_overview_api))
        .route("/api/market/launches", get(market_launches_api))
        .route("/api/market/clusters", get(market_clusters_api))
        .route(
            "/api/market/arbitrage-rankings",
            get(market_arbitrage_rankings_api),
        )
        .route("/api/market/address-lookup", get(market_address_lookup_api))
        .route(
            "/api/market/clusters/{cluster_id}",
            get(market_cluster_detail_api),
        )
        .route("/health", get(health))
        .route("/ready", get(readiness))
        .route("/metrics", get(metrics))
        .with_state(state)
        .layer(axum::middleware::from_fn_with_state(
            analytics,
            analytics::record_view,
        ))
        // Outermost, so a throttled caller costs nothing beyond the check.
        .layer(axum::middleware::from_fn_with_state(
            limiter,
            ratelimit::enforce,
        ))
}

async fn overview_page(
    State(state): State<MarketWebState>,
    Query(query): Query<PageQuery>,
    headers: HeaderMap,
) -> Result<Response, MarketWebError> {
    let context = PageContext::new(&query);
    if context.chain_id != ROBINHOOD_CHAIN_ID {
        return legacy_overview_page(state, query, headers).await;
    }
    let (ticker, boards, feed, wallet) = tokio::try_join!(
        async { overview_data(&state).await.map(|data| data.ticker) },
        render_overview_boards(&state, &context),
        render_overview_feed(&state, &context),
        session_address(&state, &headers),
    )?;
    let social = share::home_social(context.locale, &ticker);
    Ok(Html(layout_social(
        &context,
        context.locale.text("Pons market", "Pons 行情"),
        "overview",
        wallet.as_deref(),
        Some(&social),
        &pages::overview(&context, &ticker, &boards, &feed),
    ))
    .into_response())
}

async fn legacy_overview_page(
    state: MarketWebState,
    query: PageQuery,
    headers: HeaderMap,
) -> Result<Response, MarketWebError> {
    let context = PageContext::new(&query);
    let stats = load_overview(&state, context.chain_id, context.window).await?;
    let wallet = session_address(&state, &headers).await?;
    Ok(Html(layout(
        &context,
        context.locale.text("Market Overview", "市场总览"),
        "overview",
        wallet.as_deref(),
        &render_overview(&context, &stats, &[], &[]),
    ))
    .into_response())
}

async fn overview_data(state: &MarketWebState) -> Result<OverviewCache, MarketWebError> {
    if let Some(cache) = state.overview_cache.read().await.as_ref()
        && cache.loaded_at.elapsed() < OVERVIEW_CACHE_TTL
    {
        return Ok(cache.clone());
    }
    let chain_id = ROBINHOOD_CHAIN_ID;
    let (ticker, hot, gainers, newest, graduating) = tokio::try_join!(
        state.market.ticker(chain_id),
        state.market.board(chain_id, TokenBoard::Hot, 8),
        state.market.board(chain_id, TokenBoard::Gainers, 8),
        state.market.newest_launches(chain_id, 8),
        state.market.board(chain_id, TokenBoard::Graduating, 8),
    )?;
    let cache = OverviewCache {
        loaded_at: std::time::Instant::now(),
        ticker,
        hot,
        gainers,
        newest,
        graduating,
    };
    *state.overview_cache.write().await = Some(cache.clone());
    Ok(cache)
}

async fn render_overview_boards(
    state: &MarketWebState,
    context: &PageContext<'_>,
) -> Result<String, MarketWebError> {
    let data = overview_data(state).await?;
    Ok(pages::fragment_overview_boards(
        context,
        &pages::OverviewBoards {
            hot: &data.hot,
            gainers: &data.gainers,
            newest: &data.newest,
            graduating: &data.graduating,
        },
    ))
}

async fn render_overview_feed(
    state: &MarketWebState,
    context: &PageContext<'_>,
) -> Result<String, MarketWebError> {
    let trades = state
        .market
        .big_trades(context.chain_id, pages::WHALE_TRADE_MIN_USD, 3_000, 12)
        .await?;
    Ok(pages::fragment_trade_feed(context, &trades))
}

async fn ticker_fragment(
    State(state): State<MarketWebState>,
    Query(query): Query<PageQuery>,
) -> Result<Html<String>, MarketWebError> {
    let context = PageContext::new(&query);
    let ticker = overview_data(&state).await?.ticker;
    Ok(Html(pages::fragment_ticker(&context, &ticker)))
}

async fn overview_boards_fragment(
    State(state): State<MarketWebState>,
    Query(query): Query<PageQuery>,
) -> Result<Html<String>, MarketWebError> {
    let context = PageContext::new(&query);
    Ok(Html(render_overview_boards(&state, &context).await?))
}

async fn overview_feed_fragment(
    State(state): State<MarketWebState>,
    Query(query): Query<PageQuery>,
) -> Result<Html<String>, MarketWebError> {
    let context = PageContext::new(&query);
    Ok(Html(render_overview_feed(&state, &context).await?))
}

fn launch_table_query(query: &PageQuery) -> pages::LaunchTableQuery<'_> {
    pages::LaunchTableQuery {
        board: TokenBoard::parse(query.tab.as_deref()),
        sort: query
            .sort
            .as_deref()
            .filter(|sort| crate::services::market_read::TOKEN_SORTS.contains(sort)),
        descending: query.dir.as_deref() != Some("asc"),
        search: query
            .q
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty()),
    }
}

async fn render_token_table(
    state: &MarketWebState,
    context: &PageContext<'_>,
    query: &PageQuery,
) -> Result<String, MarketWebError> {
    let table_query = launch_table_query(query);
    // Hot and volume boards list exactly the tokens active in 24h, which the
    // cached overview already counts.
    let cached_total = matches!(table_query.board, TokenBoard::Hot | TokenBoard::Volume)
        && table_query.search.is_none()
        && context.chain_id == ROBINHOOD_CHAIN_ID;
    let mut page = state
        .market
        .token_page(
            context.chain_id,
            table_query.board,
            table_query.sort,
            table_query.search,
            table_query.descending,
            query.page.unwrap_or(1),
            50,
            !cached_total,
        )
        .await?;
    if cached_total {
        page.total = overview_data(state).await?.ticker.active_tokens_24h;
    }
    Ok(pages::fragment_launch_table(context, &table_query, &page))
}

async fn launches_page(
    State(state): State<MarketWebState>,
    Query(query): Query<PageQuery>,
    headers: HeaderMap,
) -> Result<Html<String>, MarketWebError> {
    let context = PageContext::new(&query);
    let (table, wallet) = tokio::try_join!(
        render_token_table(&state, &context, &query),
        session_address(&state, &headers),
    )?;
    let table_query = launch_table_query(&query);
    let social = share::board_social(table_query.board, context.locale);
    Ok(Html(layout_social(
        &context,
        context.locale.text("Launches", "发币"),
        "launches",
        wallet.as_deref(),
        Some(&social),
        &pages::launches(&context, &table_query, &table),
    )))
}

async fn launch_table_fragment(
    State(state): State<MarketWebState>,
    Query(query): Query<PageQuery>,
) -> Result<Html<String>, MarketWebError> {
    let context = PageContext::new(&query);
    Ok(Html(render_token_table(&state, &context, &query).await?))
}

async fn token_trades_fragment(
    State(state): State<MarketWebState>,
    Path(launch_id): Path<Uuid>,
    Query(query): Query<PageQuery>,
) -> Result<Html<String>, MarketWebError> {
    let context = PageContext::new(&query);
    let trades = state.market.recent_trades(launch_id, 30).await?;
    Ok(Html(pages::fragment_token_trades(&context, &trades)))
}

async fn launch_detail_page(
    State(state): State<MarketWebState>,
    Path(launch_id): Path<Uuid>,
    Query(query): Query<PageQuery>,
    headers: HeaderMap,
) -> Result<Response, MarketWebError> {
    let context = PageContext::new(&query);
    let wallet = session_address(&state, &headers).await?;
    let interval = match query.interval {
        Some(minutes @ (1 | 5 | 15 | 60)) => minutes,
        _ => 5,
    };
    if let Some(token) = state.market.token(context.chain_id, launch_id).await? {
        let (candles, trades) = tokio::try_join!(
            state.market.candles(launch_id, interval, 120),
            state.market.recent_trades(launch_id, 30),
        )?;
        let title = token.symbol.clone().unwrap_or_else(|| "Token".to_owned());
        let social = share::token_social(&token, context.locale);
        return Ok(Html(layout_social(
            &context,
            &title,
            "launches",
            wallet.as_deref(),
            Some(&social),
            &pages::token_detail(
                &context,
                &pages::TokenDetail {
                    token: &token,
                    candles: &candles,
                    interval,
                    trades: &trades,
                },
            ),
        ))
        .into_response());
    }
    let launch = state
        .service
        .launches(context.chain_id, context.window, 500)
        .await?
        .into_iter()
        .find(|launch| launch.launch_id == launch_id);
    let Some(launch) = launch else {
        return Ok((
            StatusCode::NOT_FOUND,
            Html(layout(
                &context,
                context.locale.text("Launch not found", "未找到发币记录"),
                "launches",
                wallet.as_deref(),
                &empty_state(
                    &context,
                    "This launch has no indexed trades yet.",
                    "该代币暂无已索引的成交。",
                ),
            )),
        )
            .into_response());
    };
    let title = launch
        .symbol
        .clone()
        .unwrap_or_else(|| "Launch intelligence".to_owned());
    Ok(Html(layout(
        &context,
        &title,
        "launches",
        wallet.as_deref(),
        &render_launch_detail(&context, &launch),
    ))
    .into_response())
}

async fn clusters_page(
    State(state): State<MarketWebState>,
    Query(query): Query<PageQuery>,
    headers: HeaderMap,
) -> Result<Response, MarketWebError> {
    let context = PageContext::new(&query);
    let mut lookup_error = None;
    if let Some(input) = query
        .address
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        match normalize_address(input) {
            Ok(address) => {
                return Ok(
                    axum::response::Redirect::to(&intel::address_url(&context, &address))
                        .into_response(),
                );
            }
            Err(error) => lookup_error = Some(error.message),
        }
    }
    let window = EarningsWindow::parse(query.period.as_deref());
    let (wallets, clusters, newest, wallet) = tokio::try_join!(
        async {
            state
                .market
                .wallet_leaderboard(context.chain_id, window.key(), 50, 0)
                .await
                .map_err(MarketWebError::from)
        },
        async {
            state
                .market
                .cluster_leaderboard(context.chain_id, window.key(), 50)
                .await
                .map_err(MarketWebError::from)
        },
        async {
            state
                .market
                .newest_clusters(context.chain_id, window.key(), 12)
                .await
                .map_err(MarketWebError::from)
        },
        session_address(&state, &headers),
    )?;
    let social = share::wallets_social(window.key(), context.locale);
    Ok(Html(layout_social(
        &context,
        context.locale.text("Address Intelligence", "地址情报"),
        "clusters",
        wallet.as_deref(),
        Some(&social),
        &intel::intel(
            &context,
            &intel::IntelPage {
                window,
                wallets: &wallets,
                clusters: &clusters,
                newest_clusters: &newest,
                lookup_error: lookup_error.as_deref(),
            },
        ),
    ))
    .into_response())
}

async fn address_page(
    State(state): State<MarketWebState>,
    Path(address): Path<String>,
    Query(query): Query<PageQuery>,
    headers: HeaderMap,
) -> Result<Html<String>, MarketWebError> {
    let context = PageContext::new(&query);
    let address = normalize_address(&address)?;
    let chain_id = context.chain_id;
    let (earnings, positions, lookup, day, week, month, arbitrage, wallet) = tokio::try_join!(
        async {
            state
                .market
                .address_earnings(chain_id, &address)
                .await
                .map_err(MarketWebError::from)
        },
        async {
            state
                .market
                .address_positions(chain_id, &address, 100)
                .await
                .map_err(MarketWebError::from)
        },
        async {
            state
                .service
                .address_lookup(chain_id, &address)
                .await
                .map_err(MarketWebError::from)
        },
        async {
            state
                .market
                .wallet_rank(chain_id, "24h", &address)
                .await
                .map_err(MarketWebError::from)
        },
        async {
            state
                .market
                .wallet_rank(chain_id, "7d", &address)
                .await
                .map_err(MarketWebError::from)
        },
        async {
            state
                .market
                .wallet_rank(chain_id, "30d", &address)
                .await
                .map_err(MarketWebError::from)
        },
        async {
            state
                .market
                .bot_executions(chain_id, &address, 20)
                .await
                .map_err(MarketWebError::from)
        },
        session_address(&state, &headers),
    )?;
    let ranks = [
        (EarningsWindow::Day, day),
        (EarningsWindow::Week, week),
        (EarningsWindow::Month, month),
    ];
    let social = share::address_social(&address, &earnings, context.locale);
    Ok(Html(layout_social(
        &context,
        &ui::short_address(&address),
        "clusters",
        wallet.as_deref(),
        Some(&social),
        &intel::address(
            &context,
            &intel::AddressPage {
                address: &address,
                earnings: &earnings,
                ranks: &ranks,
                positions: &positions,
                lookup: &lookup,
                arbitrage: &arbitrage,
            },
        ),
    )))
}

async fn cluster_detail_page(
    State(state): State<MarketWebState>,
    Path(cluster_id): Path<Uuid>,
    Query(query): Query<PageQuery>,
    headers: HeaderMap,
) -> Result<Response, MarketWebError> {
    let context = PageContext::new(&query);
    let wallet = session_address(&state, &headers).await?;
    let Some(cluster) = state
        .service
        .cluster_detail(context.chain_id, cluster_id)
        .await?
    else {
        return Ok((
            StatusCode::NOT_FOUND,
            Html(layout(
                &context,
                context.locale.text("Cluster not found", "未找到聚类"),
                "clusters",
                wallet.as_deref(),
                &empty_state(
                    &context,
                    "This protocol-scoped cluster is not available on the selected network.",
                    "当前网络中没有这个协议域聚类。",
                ),
            )),
        )
            .into_response());
    };
    let earnings =
        if context.chain_id == ROBINHOOD_CHAIN_ID && cluster.cluster.cluster_kind == "searcher" {
            state
                .service
                .verified_arbitrage_rankings(context.chain_id, context.window, 500)
                .await?
                .into_iter()
                .filter(|row| {
                    cluster.cluster.subject_address.as_deref() == Some(row.subject_address.as_str())
                })
                .collect()
        } else {
            Vec::new()
        };
    let earnings_section = if cluster.cluster.cluster_kind == "issuer_cohort" {
        let (rank, members) = tokio::try_join!(
            state
                .market
                .cluster_rank(context.chain_id, "30d", cluster_id),
            state.market.cluster_member_earnings(cluster_id, 50),
        )?;
        intel::cluster_earnings(&context, rank.as_ref(), &members)
    } else {
        String::new()
    };
    Ok(Html(layout(
        &context,
        context.locale.text("Cluster Evidence", "聚类证据"),
        "clusters",
        wallet.as_deref(),
        &format!(
            "{earnings_section}{}",
            render_cluster_detail(
                &context,
                &cluster,
                &earnings,
                normalize_cluster_category(query.category.as_deref()),
            )
        ),
    ))
    .into_response())
}

async fn arbitrage_page(
    State(state): State<MarketWebState>,
    Query(query): Query<PageQuery>,
    headers: HeaderMap,
) -> Result<Html<String>, MarketWebError> {
    let context = PageContext::new(&query);
    let wallet = session_address(&state, &headers).await?;
    if context.chain_id != ROBINHOOD_CHAIN_ID {
        let remote = remote_events(&state, "dex_arbitrage", 100).await?;
        return Ok(Html(layout(
            &context,
            context.locale.text("Arbitrage Intelligence", "套利情报"),
            "arbitrage",
            wallet.as_deref(),
            &render_arbitrage(&context, &[], &[], &remote),
        )));
    }
    let window = EarningsWindow::parse(query.period.as_deref());
    let hours = match window {
        EarningsWindow::Day => 24,
        EarningsWindow::Week => 168,
        EarningsWindow::Month => 720,
    };
    let chain_id = context.chain_id;
    let (overview, bots, biggest, feed, verified, rankings) = tokio::try_join!(
        async {
            state
                .market
                .arbitrage_overview(chain_id, hours)
                .await
                .map_err(MarketWebError::from)
        },
        async {
            state
                .market
                .bot_leaderboard(chain_id, window.key(), 50)
                .await
                .map_err(MarketWebError::from)
        },
        async {
            state
                .market
                .biggest_arbitrages(chain_id, hours, 10)
                .await
                .map_err(MarketWebError::from)
        },
        render_arbitrage_feed(&state, &context),
        async {
            state
                .service
                .verified_arbitrage(chain_id, 50)
                .await
                .map_err(MarketWebError::from)
        },
        async {
            state
                .service
                .verified_arbitrage_rankings(chain_id, window.key(), 100)
                .await
                .map_err(MarketWebError::from)
        },
    )?;
    let verified_section = render_arbitrage(&context, &rankings, &verified, &[]);
    Ok(Html(layout(
        &context,
        context.locale.text("Arbitrage bots", "套利机器人"),
        "arbitrage",
        wallet.as_deref(),
        &execution::arbitrage(
            &context,
            &execution::ArbitragePage {
                window,
                overview: &overview,
                bots: &bots,
                biggest: &biggest,
                feed: &feed,
                verified_section: &verified_section,
            },
        ),
    )))
}

async fn render_arbitrage_feed(
    state: &MarketWebState,
    context: &PageContext<'_>,
) -> Result<String, MarketWebError> {
    let rows = state
        .market
        .recent_profitable_arbitrages(context.chain_id, 1.0, 12)
        .await?;
    Ok(execution::arbitrage_list(context, &rows))
}

async fn arbitrage_feed_fragment(
    State(state): State<MarketWebState>,
    Query(query): Query<PageQuery>,
) -> Result<Html<String>, MarketWebError> {
    let context = PageContext::new(&query);
    Ok(Html(render_arbitrage_feed(&state, &context).await?))
}

async fn liquidations_page(
    State(state): State<MarketWebState>,
    Query(query): Query<PageQuery>,
    headers: HeaderMap,
) -> Result<Html<String>, MarketWebError> {
    let context = PageContext::new(&query);
    let wallet = session_address(&state, &headers).await?;
    if context.chain_id != ROBINHOOD_CHAIN_ID {
        let records = remote_events(&state, "liquidation", 100).await?;
        return Ok(Html(layout(
            &context,
            context.locale.text("Liquidation Intelligence", "清算情报"),
            "liquidations",
            wallet.as_deref(),
            &render_liquidations(&context, &records),
        )));
    }
    let chain_id = context.chain_id;
    let (overview, liquidators, biggest, recent, head) = tokio::try_join!(
        async {
            state
                .market
                .liquidation_overview(chain_id, 30)
                .await
                .map_err(MarketWebError::from)
        },
        async {
            state
                .market
                .liquidator_leaderboard(chain_id, 30, 30)
                .await
                .map_err(MarketWebError::from)
        },
        async {
            state
                .market
                .liquidations(chain_id, 30, true, 8)
                .await
                .map_err(MarketWebError::from)
        },
        async {
            state
                .market
                .liquidations(chain_id, 30, false, 50)
                .await
                .map_err(MarketWebError::from)
        },
        async {
            sqlx::query_scalar::<_, i64>(
                "select last_position from market.projector_checkpoints where checkpoint_key = $1",
            )
            .bind(format!("candles:{chain_id}"))
            .fetch_optional(state.service.pool())
            .await
            .map_err(internal_error)
        },
    )?;
    Ok(Html(layout(
        &context,
        context.locale.text("Liquidations", "清算"),
        "liquidations",
        wallet.as_deref(),
        &execution::liquidations(
            &context,
            &execution::LiquidationPage {
                overview: &overview,
                head_block: head.map(|block| block as u64),
                liquidators: &liquidators,
                biggest: &biggest,
                recent: &recent,
            },
        ),
    )))
}

async fn seed_library_page(
    State(state): State<MarketWebState>,
    Query(query): Query<PageQuery>,
    headers: HeaderMap,
) -> Result<Html<String>, MarketWebError> {
    let context = PageContext::new(&query);
    let seeds = state.service.seed_library(context.chain_id).await?;
    let wallet = session_address(&state, &headers).await?;
    let mut body = String::new();
    for (domain, domain_seeds) in seeds {
        let rows = domain_seeds
            .iter()
            .map(|seed| {
                let label = match context.locale {
                    Locale::En => &seed.label_en,
                    Locale::Zh => &seed.label_zh,
                };
                format!(
                    "<tr><td><strong>{}</strong><small>{}</small></td><td><code>{}</code></td><td>{:.0}%</td><td><a href=\"{}\" rel=\"noreferrer\">Source ↗</a></td></tr>",
                    escape_html(label),
                    escape_html(&seed.seed_role),
                    truncate(&seed.address, 8),
                    seed.confidence * 100.0,
                    escape_html(&seed.provenance_url),
                )
            })
            .collect::<String>();
        body.push_str(&format!(
            "<section class=\"panel\"><div class=\"section-head\"><h2>{}</h2><span>{} {}</span></div><div class=\"table-wrap\"><table><tbody>{}</tbody></table></div></section>",
            escape_html(&domain),
            domain_seeds.len(),
            context.locale.text("seeds", "个种子"),
            rows,
        ));
    }
    if body.is_empty() {
        body = empty_state(
            &context,
            "No protocol seeds are imported for this network.",
            "该网络尚未导入协议种子。",
        );
    }
    let content = format!(
        "{}{}",
        page_intro(
            &context,
            "Protocol Addresses",
            "协议地址",
            "Factories, routers, lockers and quote assets are stored in PostgreSQL with protocol scope and provenance.",
            "Factory、Router、Locker 和报价资产均带协议范围与来源存于 PostgreSQL。",
        ),
        body
    );
    Ok(Html(layout(
        &context,
        context.locale.text("Protocol Seed Library", "协议种子库"),
        "",
        wallet.as_deref(),
        &content,
    )))
}

async fn market_overview_api(
    State(state): State<MarketWebState>,
    Query(query): Query<PageQuery>,
) -> Result<Json<MarketOverviewStats>, MarketWebError> {
    let context = PageContext::new(&query);
    Ok(Json(
        load_overview(&state, context.chain_id, context.window).await?,
    ))
}

async fn market_launches_api(
    State(state): State<MarketWebState>,
    Query(query): Query<PageQuery>,
) -> Result<Json<Vec<MarketLaunch>>, MarketWebError> {
    let context = PageContext::new(&query);
    if context.chain_id != ROBINHOOD_CHAIN_ID {
        return Ok(Json(Vec::new()));
    }
    Ok(Json(
        state
            .service
            .launches(context.chain_id, context.window, 100)
            .await?,
    ))
}

async fn market_clusters_api(
    State(state): State<MarketWebState>,
    Query(query): Query<PageQuery>,
) -> Result<Json<Vec<ProtocolClusterCard>>, MarketWebError> {
    let context = PageContext::new(&query);
    let category = normalize_cluster_category(query.category.as_deref());
    let clusters = state
        .service
        .clusters(context.chain_id, 500)
        .await?
        .into_iter()
        .filter(|cluster| {
            category == "all"
                || cluster_category(&cluster.domain_kind, &cluster.cluster_kind) == category
        })
        .collect();
    Ok(Json(clusters))
}

async fn market_arbitrage_rankings_api(
    State(state): State<MarketWebState>,
    Query(query): Query<PageQuery>,
) -> Result<Json<Vec<VerifiedEarningsRankingRow>>, MarketWebError> {
    let context = PageContext::new(&query);
    if context.chain_id != ROBINHOOD_CHAIN_ID {
        return Ok(Json(Vec::new()));
    }
    Ok(Json(
        state
            .service
            .verified_arbitrage_rankings(context.chain_id, context.window, 100)
            .await?,
    ))
}

async fn market_address_lookup_api(
    State(state): State<MarketWebState>,
    Query(query): Query<PageQuery>,
) -> Result<Json<MarketAddressLookup>, MarketWebError> {
    let context = PageContext::new(&query);
    let address = normalize_address(
        query
            .address
            .as_deref()
            .ok_or_else(|| bad_request("address is required"))?,
    )?;
    Ok(Json(
        state
            .service
            .address_lookup(context.chain_id, &address)
            .await?,
    ))
}

async fn market_cluster_detail_api(
    State(state): State<MarketWebState>,
    Path(cluster_id): Path<Uuid>,
    Query(query): Query<PageQuery>,
) -> Result<Response, MarketWebError> {
    let context = PageContext::new(&query);
    match state
        .service
        .cluster_detail(context.chain_id, cluster_id)
        .await?
    {
        Some(cluster) => Ok(Json(cluster).into_response()),
        None => Ok((
            StatusCode::NOT_FOUND,
            Json(json!({"error": "cluster not found"})),
        )
            .into_response()),
    }
}

async fn wallet_challenge(
    State(state): State<MarketWebState>,
    Json(request): Json<WalletChallengeRequest>,
) -> Result<Json<WalletChallengeResponse>, MarketWebError> {
    let address = normalize_address(&request.address)?;
    let chain_id = request.chain_id.unwrap_or(ROBINHOOD_CHAIN_ID);
    if chain_id == 0 || chain_id > i64::MAX as u64 {
        return Err(bad_request("invalid wallet network"));
    }
    let challenge_id = Uuid::new_v4();
    let nonce = Uuid::new_v4().simple().to_string();
    let issued_at = Utc::now();
    let expires_at = issued_at + state.wallet_challenge_ttl;
    let message = format!(
        "LedgerFlow Market wants you to sign in with your Ethereum account:\n{address}\n\nSign in to personalize LedgerFlow market intelligence. This request does not initiate a transaction.\n\nURI: {}\nVersion: 1\nChain ID: {chain_id}\nNonce: {nonce}\nIssued At: {}\nExpiration Time: {}",
        state.public_base_url,
        issued_at.to_rfc3339(),
        expires_at.to_rfc3339(),
    );
    sqlx::query(
        "update market_wallet_challenges set consumed_at=now() where wallet_address=$1 and consumed_at is null",
    )
    .bind(&address)
    .execute(state.service.pool())
    .await
    .map_err(internal_error)?;
    sqlx::query(
        "insert into market_wallet_challenges (challenge_id,wallet_address,chain_id,nonce,message,expires_at) values ($1,$2,$3,$4,$5,$6)",
    )
    .bind(challenge_id)
    .bind(&address)
    .bind(chain_id as i64)
    .bind(&nonce)
    .bind(&message)
    .bind(expires_at)
    .execute(state.service.pool())
    .await
    .map_err(internal_error)?;
    Ok(Json(WalletChallengeResponse {
        challenge_id,
        message,
        expires_at,
    }))
}

async fn wallet_verify(
    State(state): State<MarketWebState>,
    Json(request): Json<WalletVerifyRequest>,
) -> Result<Response, MarketWebError> {
    let address = normalize_address(&request.address)?;
    let row = sqlx::query(
        "select wallet_address,message,expires_at,consumed_at from market_wallet_challenges where challenge_id=$1",
    )
    .bind(request.challenge_id)
    .fetch_optional(state.service.pool())
    .await
    .map_err(internal_error)?
    .ok_or_else(|| bad_request("wallet challenge not found"))?;
    let expected_address: String = row.try_get("wallet_address").map_err(internal_error)?;
    let message: String = row.try_get("message").map_err(internal_error)?;
    let expires_at: DateTime<Utc> = row.try_get("expires_at").map_err(internal_error)?;
    let consumed_at: Option<DateTime<Utc>> = row.try_get("consumed_at").map_err(internal_error)?;
    if expected_address != address || consumed_at.is_some() || expires_at <= Utc::now() {
        return Err(bad_request("wallet challenge is invalid or expired"));
    }
    verify_personal_signature(&address, &message, &request.signature)?;

    let raw_token = format!("{}.{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
    let token_hash = sha256_hex(raw_token.as_bytes());
    let session_expires = Utc::now() + state.wallet_session_ttl;
    let mut tx = state.service.pool().begin().await.map_err(internal_error)?;
    let consumed = sqlx::query(
        "update market_wallet_challenges set consumed_at=now() where challenge_id=$1 and consumed_at is null and expires_at>now()",
    )
    .bind(request.challenge_id)
    .execute(&mut *tx)
    .await
    .map_err(internal_error)?;
    if consumed.rows_affected() != 1 {
        return Err(bad_request("wallet challenge was already used"));
    }
    sqlx::query(
        "insert into market_wallet_accounts (wallet_address) values ($1) on conflict (wallet_address) do update set last_connected_at=now()",
    )
    .bind(&address)
    .execute(&mut *tx)
    .await
    .map_err(internal_error)?;
    sqlx::query(
        "insert into market_wallet_sessions (wallet_address,token_hash,expires_at) values ($1,$2,$3)",
    )
    .bind(&address)
    .bind(&token_hash)
    .bind(session_expires)
    .execute(&mut *tx)
    .await
    .map_err(internal_error)?;
    tx.commit().await.map_err(internal_error)?;
    let secure = if state.public_base_url.starts_with("https://") {
        "; Secure"
    } else {
        ""
    };
    let cookie = format!(
        "{SESSION_COOKIE}={raw_token}; Path=/; HttpOnly; SameSite=Lax; Max-Age={}{secure}",
        state.wallet_session_ttl.num_seconds()
    );
    Ok((
        [(header::SET_COOKIE, cookie)],
        Json(WalletSessionResponse {
            authenticated: true,
            address: Some(address),
        }),
    )
        .into_response())
}

async fn wallet_logout(State(state): State<MarketWebState>, headers: HeaderMap) -> Response {
    if let Some(token) = cookie_value(&headers, SESSION_COOKIE) {
        let _ =
            sqlx::query("update market_wallet_sessions set revoked_at=now() where token_hash=$1")
                .bind(sha256_hex(token.as_bytes()))
                .execute(state.service.pool())
                .await;
    }
    (
        [(
            header::SET_COOKIE,
            format!("{SESSION_COOKIE}=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0"),
        )],
        Json(WalletSessionResponse {
            authenticated: false,
            address: None,
        }),
    )
        .into_response()
}

async fn wallet_session(
    State(state): State<MarketWebState>,
    headers: HeaderMap,
) -> Result<Json<WalletSessionResponse>, MarketWebError> {
    let address = session_address(&state, &headers).await?;
    Ok(Json(WalletSessionResponse {
        authenticated: address.is_some(),
        address,
    }))
}

async fn session_address(
    state: &MarketWebState,
    headers: &HeaderMap,
) -> Result<Option<String>, MarketWebError> {
    let Some(token) = cookie_value(headers, SESSION_COOKIE) else {
        return Ok(None);
    };
    let row = sqlx::query(
        "update market_wallet_sessions set last_seen_at=now() where token_hash=$1 and revoked_at is null and expires_at>now() returning wallet_address",
    )
    .bind(sha256_hex(token.as_bytes()))
    .fetch_optional(state.service.pool())
    .await
    .map_err(internal_error)?;
    Ok(row.map(|row| row.get("wallet_address")))
}

async fn load_overview(
    state: &MarketWebState,
    chain_id: u64,
    window: &str,
) -> Result<MarketOverviewStats, MarketWebError> {
    if chain_id == ROBINHOOD_CHAIN_ID {
        return state
            .service
            .overview(chain_id, window)
            .await
            .map_err(Into::into);
    }
    if chain_id != 8453 {
        return Err(bad_request("unsupported network"));
    }
    let records = remote_evidence_records(state).await?;
    let mut stats = MarketOverviewStats::default();
    let cutoff = Utc::now() - chrono::Duration::hours(24);
    for record in records
        .iter()
        .filter(|record| value_u64(record, "chain_id") == Some(8453))
    {
        if value_str(record, "event_type") != Some("dex_arbitrage") {
            continue;
        }
        let created_at = value_str(record, "created_at").and_then(parse_time);
        stats.latest_detected_arbitrage_at = newest(stats.latest_detected_arbitrage_at, created_at);
        if created_at.is_some_and(|value| value >= cutoff) {
            stats.detected_arbitrage_24h += 1;
        }
        if verified_profit(record) {
            stats.verified_arbitrage_archive += 1;
            if created_at.is_some_and(|value| value >= cutoff) {
                stats.verified_arbitrage_24h += 1;
            }
            stats.latest_verified_arbitrage_at =
                newest(stats.latest_verified_arbitrage_at, created_at);
        }
    }
    Ok(stats)
}

async fn remote_events(
    state: &MarketWebState,
    event_type: &str,
    limit: usize,
) -> Result<Vec<Value>, MarketWebError> {
    Ok(remote_evidence_records(state)
        .await?
        .iter()
        .filter(|record| {
            value_u64(record, "chain_id") == Some(8453)
                && value_str(record, "event_type") == Some(event_type)
        })
        .take(limit)
        .cloned()
        .collect())
}

async fn remote_evidence_records(
    state: &MarketWebState,
) -> Result<Arc<Vec<Value>>, MarketWebError> {
    if let Some(cache) = state.remote_evidence.read().await.as_ref()
        && cache.loaded_at.elapsed() < state.remote_cache_ttl
    {
        return Ok(cache.records.clone());
    }
    let records = state
        .client
        .get(&state.base_evidence_url)
        .send()
        .await
        .map_err(|error| upstream_error(format!("Base evidence unavailable: {error}")))?
        .error_for_status()
        .map_err(|error| upstream_error(format!("Base evidence request failed: {error}")))?
        .json::<Vec<Value>>()
        .await
        .map_err(|error| upstream_error(format!("Base evidence response is invalid: {error}")))?;
    let records = Arc::new(records);
    *state.remote_evidence.write().await = Some(RemoteEvidenceCache {
        loaded_at: std::time::Instant::now(),
        records: records.clone(),
    });
    Ok(records)
}

async fn health() -> Json<Value> {
    Json(json!({ "status": "ok", "service": "ledgerflow-market-web" }))
}

async fn readiness(State(state): State<MarketWebState>) -> Response {
    match sqlx::query_scalar::<_, i32>("select 1")
        .fetch_one(state.service.pool())
        .await
    {
        Ok(_) => (StatusCode::OK, Json(json!({ "status": "ready" }))).into_response(),
        Err(_) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "status": "not_ready" })),
        )
            .into_response(),
    }
}

async fn metrics(State(state): State<MarketWebState>) -> Response {
    let stats = state.service.overview(ROBINHOOD_CHAIN_ID, "24h").await.ok();
    let body = format!(
        "# TYPE ledgerflow_market_up gauge\nledgerflow_market_up 1\nledgerflow_market_protocol_domains {}\nledgerflow_market_protocol_seeds {}\nledgerflow_market_launch_data_ready {}\nledgerflow_market_verified_arbitrage_archive {}\n",
        stats
            .as_ref()
            .map(|value| value.protocol_domain_count)
            .unwrap_or_default(),
        stats
            .as_ref()
            .map(|value| value.protocol_seed_count)
            .unwrap_or_default(),
        u8::from(stats.as_ref().is_some_and(|value| value.launch_data_ready)),
        stats
            .as_ref()
            .map(|value| value.verified_arbitrage_archive)
            .unwrap_or_default(),
    );
    ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], body).into_response()
}

async fn brand_mark_svg() -> Response {
    (
        [(header::CONTENT_TYPE, "image/svg+xml; charset=utf-8")],
        brand_mark_svg_source(),
    )
        .into_response()
}

fn render_overview(
    context: &PageContext<'_>,
    stats: &MarketOverviewStats,
    launches: &[MarketLaunch],
    executions: &[VerifiedExecutionRow],
) -> String {
    let l = context.locale;
    let launch_count = stats
        .launch_count
        .map(format_number)
        .unwrap_or_else(|| unavailable(l));
    let movement = match (stats.rising_launch_count, stats.falling_launch_count) {
        (Some(rising), Some(falling)) => format!(
            "<span class=\"positive\">{} ↑</span><span class=\"slash\">/</span><span class=\"negative\">{} ↓</span>",
            format_number(rising),
            format_number(falling)
        ),
        _ => unavailable(l),
    };
    let participants = match (stats.verified_winner_count, stats.verified_loser_count) {
        (Some(winners), Some(losers)) => format!(
            "<span class=\"positive\">{}</span><small>{}</small><span>{}</span><small>{}</small>",
            format_number(winners),
            l.text("winners", "盈利"),
            format_number(losers),
            l.text("losing", "亏损")
        ),
        _ => match (stats.estimated_winner_count, stats.estimated_loser_count) {
            (Some(winners), Some(losers)) => format!(
                "<span class=\"positive\">{}</span><small>{}</small><span>{}</span><small>{}</small>",
                format_number(winners),
                l.text("estimated winners", "估算盈利"),
                format_number(losers),
                l.text("estimated losses", "估算亏损")
            ),
            _ => unavailable(l),
        },
    };
    let execution_metric = format!(
        "<span class=\"accent\">{}</span><small>{}</small><span>{}</span><small>{}</small>",
        format_number(stats.verified_arbitrage_archive),
        l.text("verified", "已验证"),
        format_number(stats.detected_arbitrage_24h),
        l.text("detected · 24h", "检测到 · 24h")
    );
    let freshness = stats
        .latest_detected_arbitrage_at
        .map(format_time)
        .unwrap_or_else(|| "—".to_owned());
    format!(
        r#"<section class="hero">
  <div class="hero-copy"><span class="eyebrow">LEDGERFLOW</span><h1>{headline}</h1><p>{subtitle}</p><div class="hero-actions"><a class="button primary" href="{launch_url}">{launch_cta}</a><a class="button" href="{arb_url}">{arb_cta}</a></div></div>
  <aside class="fresh-card"><span>{fresh_label}</span><strong>{freshness}</strong><div><b>{domains}</b> {domains_label} · <b>{seeds}</b> {seeds_label}</div></aside>
</section>
<section class="metrics">
  {launch_metric}{movement_metric}{participant_metric}{execution_card}
</section>
<section class="split">
  <div class="panel"><div class="section-head"><div><span class="eyebrow">{launch_label}</span><h2>{launch_title}</h2></div><a href="{launch_url}">{all_label}</a></div>{launch_table}</div>
  <div class="panel"><div class="section-head"><div><span class="eyebrow">{execution_label}</span><h2>{execution_title}</h2></div><a href="{arb_url}">{evidence_label}</a></div>{execution_rows}</div>
</section>"#,
        headline = l.text("Market Intelligence", "市场情报"),
        subtitle = l.text(
            "Launches · clusters · verified execution",
            "发币 · 聚类 · 已验证执行"
        ),
        launch_url = context.url("/launches"),
        launch_cta = l.text("Launches", "发币"),
        arb_url = context.url("/arbitrage"),
        arb_cta = l.text("Earnings", "收益"),
        fresh_label = l.text("LATEST EVIDENCE", "最新证据"),
        freshness = freshness,
        domains = stats.protocol_domain_count,
        seeds = stats.protocol_seed_count,
        domains_label = l.text("domains", "协议域"),
        seeds_label = l.text("seeds", "种子"),
        launch_metric = metric_card(
            "01",
            l.text("Launches observed", "已观察发币"),
            &launch_count,
            context.window,
            l.text("Protocol event projection", "协议事件投影")
        ),
        movement_metric = metric_card(
            "02",
            l.text("Rising / falling", "上涨 / 下跌"),
            &movement,
            context.window,
            l.text("Comparable price points", "可比价格点")
        ),
        participant_metric = metric_card(
            "03",
            l.text("Participant outcomes", "参与者盈亏"),
            &participants,
            context.window,
            l.text(
                "Verified, or estimated before gas",
                "已验证，或不含 gas 的估算"
            )
        ),
        execution_card = metric_card(
            "04",
            l.text("Arbitrage evidence", "套利证据"),
            &execution_metric,
            "archive + 24h",
            &format!(
                "{} {}",
                stats.verified_arbitrage_actors,
                l.text("verified actors", "个已验证地址")
            )
        ),
        launch_label = l.text("LAUNCH RADAR", "发币雷达"),
        launch_title = l.text("Latest launches", "最新发币"),
        all_label = l.text("View all →", "查看全部 →"),
        launch_table = render_launch_table(context, launches, 8),
        execution_label = l.text("VERIFIED EXECUTIONS", "已验证执行"),
        execution_title = l.text("Recent earnings", "最近收益"),
        evidence_label = l.text("Open arbitrage →", "打开套利 →"),
        execution_rows = render_execution_cards(context, executions, 6),
    )
}

fn render_launch_table(
    context: &PageContext<'_>,
    launches: &[MarketLaunch],
    limit: usize,
) -> String {
    let l = context.locale;
    if launches.is_empty() {
        let detail = if context.chain_id == ROBINHOOD_CHAIN_ID {
            l.text(
                "Pons factories are in the database seed library. Launch, price, flow and participant projections have not produced a verified snapshot yet.",
                "Pons 工厂已进入数据库种子库；发币、价格、资金流和参与者投影尚未产出首个已验证快照。",
            )
        } else {
            l.text(
                "The first launch-intelligence domain is Pons on Robinhood Chain. Base launch data is not shown as zero.",
                "首个发币情报域是 Robinhood Chain 上的 Pons；Base 发币数据不会被误显示为 0。",
            )
        };
        return format!(
            "<div class=\"signal-contract\"><div><span>01</span><strong>{}</strong><small>{}</small></div><div><span>02</span><strong>{}</strong><small>{}</small></div><div><span>03</span><strong>{}</strong><small>{}</small></div><div><span>04</span><strong>{}</strong><small>{}</small></div></div><div class=\"honest-empty compact\"><i></i><div><strong>{}</strong><p>{}</p><small>{}</small></div></div>",
            l.text("Price & movement", "价格与涨跌"),
            l.text("Comparable windowed prices", "同窗口可比价格"),
            l.text("Capital velocity", "短时资金流速"),
            l.text("5m · 15m · 1h net flow", "5m · 15m · 1h 净流入"),
            l.text("Capital origin", "资金来源"),
            l.text(
                "Issuer cohort · external · unresolved",
                "发币关联 · 外部 · 待确认"
            ),
            l.text("Verified outcomes", "已验证盈亏"),
            l.text("Realized winners and losses only", "仅统计已实现盈利与亏损"),
            l.text("Launch intelligence is being connected", "发币情报正在接入"),
            detail,
            l.text(
                "No placeholder market data is displayed.",
                "当前没有展示任何占位市场数据。"
            )
        );
    }
    let mut rows = String::new();
    for launch in launches.iter().take(limit) {
        let token = launch
            .symbol
            .as_deref()
            .or(launch.name.as_deref())
            .unwrap_or("Unknown");
        let movement = launch
            .price_change_percent
            .as_deref()
            .map(percent_html)
            .unwrap_or_else(|| unavailable(l));
        let velocity = launch
            .inflow_velocity_quote_per_minute
            .as_deref()
            .map(|value| format!("{} / min", escape_html(value)))
            .unwrap_or_else(|| unavailable(l));
        let origin = match (
            launch.issuer_cohort_buyers,
            launch.external_buyers,
            launch.unresolved_buyers,
        ) {
            (Some(issuer), Some(external), Some(unresolved)) => format!(
                "<b>{issuer}</b> {} · <b>{external}</b> {} · {unresolved} {}",
                l.text("issuer", "关联"),
                l.text("external", "外部"),
                l.text("unresolved", "待确认")
            ),
            _ => unavailable(l),
        };
        let outcomes = match (
            launch.verified_winning_participants,
            launch.verified_losing_participants,
        ) {
            (Some(winners), Some(losers)) if launch.outcome_coverage_ratio.unwrap_or(0.0) > 0.0 => {
                format!(
                    "<span class=\"positive\">{winners} W</span> / <span class=\"negative\">{losers} L</span>"
                )
            }
            _ => match (
                launch.estimated_winning_participants,
                launch.estimated_losing_participants,
            ) {
                (Some(winners), Some(losers)) => format!(
                    "<span class=\"positive\">~{winners} W</span> / <span class=\"negative\">~{losers} L</span>"
                ),
                _ => unavailable(l),
            },
        };
        let time = launch
            .launched_at
            .map(format_time)
            .unwrap_or_else(|| format!("block {}", launch.launch_block));
        let state = if launch.curve_trade_count > 0 {
            format!(
                "{} · {} {}",
                launch.launch_status.to_uppercase(),
                launch.curve_trade_count,
                l.text("curve trades", "笔曲线交易")
            )
        } else {
            launch.launch_status.to_uppercase()
        };
        rows.push_str(&format!(
            "<tr><td><a class=\"token-cell\" href=\"{}\"><span>{}</span><div><strong>{}</strong><small>{} · {}</small></div></a></td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td><em class=\"state\">{}</em></td></tr>",
            context.url(&format!("/launches/{}", launch.launch_id)),
            escape_html(&token.chars().take(2).collect::<String>().to_uppercase()),
            escape_html(token),
            truncate(&launch.token_address, 6),
            time,
            movement,
            velocity,
            origin,
            outcomes,
            escape_html(&state),
        ));
    }
    format!(
        "<div class=\"table-wrap\"><table><thead><tr><th>{}</th><th>{}</th><th>{}</th><th>{}</th><th>{}</th><th>{}</th></tr></thead><tbody>{}</tbody></table></div>",
        l.text("Launch", "代币"),
        l.text("Price", "涨跌"),
        l.text("Flow velocity", "资金流速"),
        l.text("Capital origin", "资金来源"),
        l.text("Verified outcomes", "已验证盈亏"),
        l.text("State", "状态"),
        rows
    )
}

fn render_launch_detail(context: &PageContext<'_>, launch: &MarketLaunch) -> String {
    let l = context.locale;
    let token = launch
        .symbol
        .as_deref()
        .or(launch.name.as_deref())
        .unwrap_or("Unknown token");
    let movement = launch
        .price_change_percent
        .as_deref()
        .map(|value| format!("{value}%"))
        .unwrap_or_else(|| l.text("Not indexed", "尚未索引").to_owned());
    let outcomes = match (
        launch.verified_winning_participants,
        launch.verified_losing_participants,
    ) {
        (Some(winners), Some(losers)) if launch.outcome_coverage_ratio.unwrap_or(0.0) > 0.0 => {
            format!(
                "{winners} {} · {losers} {}",
                l.text("verified winners", "个已验证盈利"),
                l.text("verified losses", "个已验证亏损")
            )
        }
        _ => match (
            launch.estimated_winning_participants,
            launch.estimated_losing_participants,
        ) {
            (Some(winners), Some(losers)) => format!(
                "~{winners} {} · ~{losers} {}",
                l.text("estimated winners before gas", "个不含 gas 的估算盈利"),
                l.text("estimated losses before gas", "个不含 gas 的估算亏损")
            ),
            _ => l
                .text("Economic accounting is not complete", "经济核算尚未完成")
                .to_owned(),
        },
    };
    let domain = match l {
        Locale::En => &launch.domain_name_en,
        Locale::Zh => &launch.domain_name_zh,
    };
    format!(
        r#"<a class="back" href="{back}">← {back_label}</a>
<section class="detail-hero"><div><span class="eyebrow">{domain}</span><h1>{token}</h1><code>{address}</code><div class="tags"><em class="state">{status}</em><span>{time}</span></div></div><aside><span>{movement_label}</span><strong>{movement}</strong><small>{window}</small></aside></section>
<section class="detail-grid">
  <article class="panel"><h2>{profit_title}</h2><div class="answer"><strong>{outcomes}</strong></div></article>
  <article class="panel"><h2>{flow_title}</h2><dl><div><dt>{velocity}</dt><dd>{velocity_value}</dd></div><div><dt>{net_flow}</dt><dd>{net_flow_value}</dd></div><div><dt>{buyers}</dt><dd>{buyer_value}</dd></div></dl></article>
  <article class="panel"><h2>{origin_title}</h2><dl><div><dt>{issuer}</dt><dd>{issuer_value}</dd></div><div><dt>{external}</dt><dd>{external_value}</dd></div><div><dt>{unresolved}</dt><dd>{unresolved_value}</dd></div></dl></article>
  <article class="panel"><h2>{cluster_title}</h2><code>{creator}</code></article>
</section>
<section class="panel limitations"><div class="section-head"><h2>{activity_title}</h2><span>{limitations_count} {limitations_label}</span></div><dl><div><dt>{trades_label}</dt><dd>{trade_count}</dd></div><div><dt>{buys_label}</dt><dd>{buy_count}</dd></div><div><dt>{sells_label}</dt><dd>{sell_count}</dd></div><div><dt>{latest_label}</dt><dd>{latest_trade}</dd></div></dl></section>"#,
        back = context.url("/launches"),
        back_label = l.text("Launch Radar", "发币雷达"),
        domain = escape_html(domain),
        token = escape_html(token),
        address = escape_html(&launch.token_address),
        status = escape_html(&launch.launch_status.to_uppercase()),
        time = launch
            .launched_at
            .map(format_time)
            .unwrap_or_else(|| format!("Block {}", launch.launch_block)),
        movement_label = l.text("Price movement", "价格变化"),
        movement = movement,
        window = context.window,
        profit_title = l.text("Verified outcomes", "已验证盈亏"),
        outcomes = outcomes,
        flow_title = l.text("Flow", "资金流"),
        velocity = l.text("Inflow velocity", "流入速度"),
        velocity_value = optional_text(launch.inflow_velocity_quote_per_minute.as_deref(), l),
        net_flow = l.text("Observed net flow", "观察净流量"),
        net_flow_value = optional_text(launch.net_flow_quote.as_deref(), l),
        buyers = l.text("Buyers / sellers", "买家 / 卖家"),
        buyer_value = match (launch.unique_buyers, launch.unique_sellers) {
            (Some(b), Some(s)) => format!("{b} / {s}"),
            _ => "—".to_owned(),
        },
        origin_title = l.text("Participants", "参与者"),
        issuer = l.text("Issuer-associated cohort", "发币方关联群体"),
        issuer_value = optional_i64(launch.issuer_cohort_buyers),
        external = l.text("External participants", "外部参与者"),
        external_value = optional_i64(launch.external_buyers),
        unresolved = l.text("Unresolved", "待确认"),
        unresolved_value = optional_i64(launch.unresolved_buyers),
        cluster_title = l.text("Creator", "发币地址"),
        creator = launch
            .creator_address
            .as_deref()
            .map(escape_html)
            .unwrap_or_else(|| l.text("Creator unavailable", "发币地址不可用").to_owned()),
        activity_title = l.text("Curve activity", "曲线交易"),
        limitations_count = launch.limitations.len(),
        limitations_label = l.text("limitations", "项限制"),
        trades_label = l.text("Total curve trades", "曲线交易总数"),
        trade_count = launch.curve_trade_count,
        buys_label = l.text("Buys", "买入"),
        buy_count = launch.curve_buy_count,
        sells_label = l.text("Sells", "卖出"),
        sell_count = launch.curve_sell_count,
        latest_label = l.text("Latest trade", "最近交易"),
        latest_trade = launch
            .latest_curve_trade_at
            .map(format_time)
            .unwrap_or_else(|| l
                .text("No indexed curve trade", "暂无已索引曲线交易")
                .to_owned()),
    )
}

fn normalize_cluster_category(value: Option<&str>) -> &'static str {
    match value {
        Some("tokens") => "tokens",
        Some("arbitrage") => "arbitrage",
        Some("liquidations") => "liquidations",
        Some("other") => "other",
        _ => "all",
    }
}

fn cluster_category(domain_kind: &str, cluster_kind: &str) -> &'static str {
    if domain_kind == "token_launch" {
        "tokens"
    } else if domain_kind == "liquidation" || cluster_kind == "liquidator" {
        "liquidations"
    } else if domain_kind == "dex_execution" && cluster_kind == "searcher" {
        "arbitrage"
    } else {
        "other"
    }
}

fn cluster_filter_url(context: &PageContext<'_>, category: &str, address: Option<&str>) -> String {
    let mut url = format!(
        "{}&category={}",
        context.url("/clusters"),
        urlencoding::encode(category)
    );
    if let Some(address) = address.filter(|value| !value.trim().is_empty()) {
        url.push_str("&address=");
        url.push_str(&urlencoding::encode(address));
    }
    url
}

fn cluster_detail_url(context: &PageContext<'_>, cluster_id: Uuid, category: &str) -> String {
    format!(
        "{}&category={}",
        context.url(&format!("/clusters/{cluster_id}")),
        urlencoding::encode(category)
    )
}

fn render_cluster_detail(
    context: &PageContext<'_>,
    detail: &ProtocolClusterDetail,
    earnings: &[VerifiedEarningsRankingRow],
    category: &str,
) -> String {
    let l = context.locale;
    let cluster = &detail.cluster;
    let domain = match l {
        Locale::En => &cluster.domain_name_en,
        Locale::Zh => &cluster.domain_name_zh,
    };
    let members = detail
        .members
        .iter()
        .map(|member| {
            let evidence_link = member
                .supporting_tx_hashes
                .first()
                .map(|hash| {
                    format!(
                        "<a href=\"{}\" target=\"_blank\" rel=\"noreferrer\">{} ↗</a>",
                        explorer_tx_url(context.chain_id, hash),
                        l.text("Transaction", "交易")
                    )
                })
                .unwrap_or_else(|| "—".to_owned());
            format!(
                "<tr><td><a href=\"{}&address={}\"><code>{}</code></a></td><td><strong>{}</strong><small>{}</small></td><td>{}</td><td>{:.0}%</td><td>{}</td><td>{}</td></tr>",
                context.url("/clusters"),
                member.address,
                truncate(&member.address, 8),
                humanize(&member.member_role),
                escape_html(&member.evidence_state.to_uppercase()),
                format_number(member.interaction_count),
                member.confidence * 100.0,
                if member.include_in_economic_scope { l.text("Included", "已纳入") } else { l.text("Excluded", "未纳入") },
                evidence_link,
            )
        })
        .collect::<String>();
    let edges = if detail.edges.is_empty() {
        format!(
            "<tr><td colspan=\"5\">{}</td></tr>",
            l.text("No relationship edges.", "暂无关系边。")
        )
    } else {
        detail
            .edges
            .iter()
            .map(|edge| {
                let evidence_link = edge
                    .supporting_tx_hashes
                    .first()
                    .map(|hash| {
                        format!(
                            "<a href=\"{}\" target=\"_blank\" rel=\"noreferrer\">{} ↗</a>",
                            explorer_tx_url(context.chain_id, hash),
                            l.text("Evidence", "证据")
                        )
                    })
                    .unwrap_or_else(|| "—".to_owned());
                format!(
                    "<tr><td><code>{}</code><small>{}</small></td><td><strong>{}</strong><small>{}</small></td><td><code>{}</code></td><td>{}</td><td>{}</td></tr>",
                    truncate(&edge.source_address, 6),
                    escape_html(&edge.evidence_state.to_uppercase()),
                    humanize(&edge.relationship_type),
                    escape_html(&edge.evidence_reason),
                    truncate(&edge.target_address, 6),
                    format_number(edge.interaction_count),
                    evidence_link,
                )
            })
            .collect::<String>()
    };
    let earnings = if cluster.cluster_kind == "searcher" {
        render_cluster_earnings(context, earnings)
    } else {
        String::new()
    };
    format!(
        r#"<a class="back" href="{back}">← {back_label}</a><section class="detail-hero cluster-detail-hero"><div><span class="eyebrow">{domain}</span><h1>{kind}</h1><code>{subject}</code><div class="tags"><em class="state">{state}</em><span>{member_count} {members_label}</span><span>{candidate_label}</span></div></div><aside><span>{confidence_label}</span><strong>{confidence:.0}%</strong><small>{scope}</small></aside></section>{earnings}<section class="panel"><div class="section-head"><h2>{members_title}</h2><span>{member_count}</span></div><div class="table-wrap"><table><thead><tr><th>{address_label}</th><th>{role_label}</th><th>{interactions_label}</th><th>{confidence_label}</th><th>{economic_label}</th><th>{evidence_label}</th></tr></thead><tbody>{members}</tbody></table></div></section><section class="panel"><div class="section-head"><h2>{relationships_title}</h2><span>{edge_count}</span></div><div class="table-wrap"><table><thead><tr><th>{source_label}</th><th>{relationship_label}</th><th>{target_label}</th><th>{interactions_label}</th><th>{evidence_label}</th></tr></thead><tbody>{edges}</tbody></table></div></section>"#,
        back = cluster_filter_url(context, category, None),
        back_label = l.text("Address Intelligence", "地址情报"),
        domain = escape_html(domain),
        kind = humanize(&cluster.cluster_kind),
        subject = cluster
            .subject_address
            .as_deref()
            .map(escape_html)
            .unwrap_or_else(|| "—".to_owned()),
        state = escape_html(&cluster.evidence_state.to_uppercase()),
        member_count = detail.members.len(),
        members_label = l.text("addresses", "个地址"),
        candidate_label = l.text("Candidate association", "候选关联"),
        confidence_label = l.text("Confidence", "置信度"),
        confidence = cluster.confidence * 100.0,
        scope = l.text("Protocol-scoped", "仅限协议域"),
        earnings = earnings,
        members_title = l.text("Addresses", "地址"),
        address_label = l.text("Address", "地址"),
        role_label = l.text("Scoped role", "域内角色"),
        interactions_label = l.text("Interactions", "交互"),
        economic_label = l.text("Economic scope", "经济范围"),
        evidence_label = l.text("Evidence", "证据"),
        members = members,
        relationships_title = l.text("Relationships", "关系"),
        edge_count = detail.edges.len(),
        source_label = l.text("Source", "来源地址"),
        relationship_label = l.text("Relationship", "关系"),
        target_label = l.text("Target", "目标地址"),
        edges = edges,
    )
}

fn render_cluster_earnings(
    context: &PageContext<'_>,
    earnings: &[VerifiedEarningsRankingRow],
) -> String {
    let l = context.locale;
    let body = if earnings.is_empty() {
        format!(
            "<div class=\"rank-empty\">{}</div>",
            l.text(
                "No verified earnings in this window.",
                "该区间暂无已验证收益。"
            )
        )
    } else {
        earnings
            .iter()
            .map(|row| {
                format!(
                    "<article class=\"earning-card\"><span>{}</span><strong>{}</strong><dl><div><dt>{}</dt><dd>{}</dd></div><div><dt>Gas</dt><dd>{}</dd></div><div><dt>{}</dt><dd>{:.1}%</dd></div></dl></article>",
                    escape_html(
                        row.profit_token_symbol
                            .as_deref()
                            .unwrap_or(&truncate(&row.profit_token_address, 6))
                    ),
                    format_ranked_profit(row),
                    l.text("Executions", "执行"),
                    format_number(row.verified_execution_count),
                    format_gas_cost(&row.exact_gas_cost_raw),
                    l.text("Coverage", "覆盖率"),
                    row.coverage_ratio * 100.0,
                )
            })
            .collect()
    };
    format!(
        "<section class=\"panel\"><div class=\"section-head\"><h2>{} · {}</h2><span>{}</span></div><div class=\"earnings-grid\">{}</div></section>",
        l.text("Verified earnings", "已验证收益"),
        context.window,
        l.text("Per asset · gas separate", "按资产 · gas 单列"),
        body,
    )
}

fn render_earnings_rankings(
    context: &PageContext<'_>,
    rankings: &[VerifiedEarningsRankingRow],
) -> String {
    let l = context.locale;
    if rankings.is_empty() {
        return format!(
            "<section class=\"panel ranking-panel\"><div class=\"section-head\"><h2>{} · {}</h2><span>{}</span></div><div class=\"rank-empty\">{}</div></section>",
            l.text("Earnings ranking", "收益排行"),
            context.window,
            l.text("Verified only", "仅已验证"),
            l.text(
                "No verified earnings in this window.",
                "该区间暂无已验证收益。"
            ),
        );
    }
    let mut groups = BTreeMap::<&str, Vec<&VerifiedEarningsRankingRow>>::new();
    for row in rankings {
        groups
            .entry(&row.profit_token_address)
            .or_default()
            .push(row);
    }
    let groups = groups
        .into_values()
        .map(|rows| {
            let asset = rows[0]
                .profit_token_symbol
                .as_deref()
                .unwrap_or(&rows[0].profit_token_address);
            let body = rows
                .iter()
                .map(|row| {
                    let subject_url = row.cluster_id.map_or_else(
                        || cluster_filter_url(context, "arbitrage", Some(&row.subject_address)),
                        |cluster_id| cluster_detail_url(context, cluster_id, "arbitrage"),
                    );
                    format!(
                        "<tr><td><b>#{}</b></td><td><a href=\"{}\"><code>{}</code></a></td><td class=\"positive\"><strong>{}</strong></td><td>{}</td><td>{}</td><td>{:.1}%<small>{} / {}</small></td><td>{}</td></tr>",
                        row.rank,
                        subject_url,
                        truncate(&row.subject_address, 8),
                        format_ranked_profit(row),
                        format_gas_cost(&row.exact_gas_cost_raw),
                        format_number(row.verified_execution_count),
                        row.coverage_ratio * 100.0,
                        row.economically_verified_count,
                        row.economically_evaluated_count,
                        row.last_execution_at.map(format_time).unwrap_or_else(|| "—".to_owned()),
                    )
                })
                .collect::<String>();
            format!(
                "<div class=\"asset-ranking\"><div class=\"asset-heading\"><strong>{}</strong><code>{}</code></div><div class=\"table-wrap\"><table><thead><tr><th>#</th><th>{}</th><th>{}</th><th>Gas</th><th>{}</th><th>{}</th><th>{}</th></tr></thead><tbody>{}</tbody></table></div></div>",
                escape_html(asset),
                truncate(&rows[0].profit_token_address, 8),
                l.text("Cluster / actor", "聚类 / 地址"),
                l.text("Verified gross", "已验证毛收益"),
                l.text("Executions", "执行"),
                l.text("Coverage", "覆盖率"),
                l.text("Last active", "最近活动"),
                body,
            )
        })
        .collect::<String>();
    format!(
        "<section class=\"panel ranking-panel\"><div class=\"section-head\"><h2>{} · {}</h2><span>{}</span></div>{}</section>",
        l.text("Earnings ranking", "收益排行"),
        context.window,
        l.text("Per asset · gas separate", "按资产 · gas 单列"),
        groups,
    )
}

fn render_arbitrage(
    context: &PageContext<'_>,
    rankings: &[VerifiedEarningsRankingRow],
    rows: &[VerifiedExecutionRow],
    remote: &[Value],
) -> String {
    let l = context.locale;
    let table = if context.chain_id == ROBINHOOD_CHAIN_ID {
        render_execution_table(context, rows)
    } else if remote.is_empty() {
        empty_state(
            context,
            "No Base arbitrage evidence is available from the public feed.",
            "Base 公共证据流中暂无套利记录。",
        )
    } else {
        render_remote_event_table(context, remote, "arbitrage")
    };
    let rankings = if context.chain_id == ROBINHOOD_CHAIN_ID {
        render_earnings_rankings(context, rankings)
    } else {
        String::new()
    };
    format!(
        "{}{}<section class=\"panel\"><div class=\"section-head\"><h2>{}</h2><span>{}</span></div>{}</section>",
        page_intro(context, "Arbitrage Earnings", "套利收益", "", "",),
        rankings,
        l.text("Verified executions", "已验证执行"),
        if context.chain_id == ROBINHOOD_CHAIN_ID {
            format!("{} {}", rows.len(), l.text("records", "条"))
        } else {
            format!("{} {}", remote.len(), l.text("records", "条"))
        },
        table,
    )
}

fn render_liquidations(context: &PageContext<'_>, records: &[Value]) -> String {
    let l = context.locale;
    let table = if context.chain_id != 8453 {
        format!(
            "{}<a class=\"button small\" href=\"/liquidations?lang={}&chain_id=8453&window={}\">{}</a>",
            empty_state(
                context,
                "No verified Robinhood liquidation is available in the current source.",
                "当前数据源中暂无已验证 Robinhood 清算记录。"
            ),
            l.code(),
            context.window,
            l.text("View Base liquidations", "查看 Base 清算"),
        )
    } else if records.is_empty() {
        empty_state(
            context,
            "No liquidation evidence is available.",
            "暂无清算证据。",
        )
    } else {
        render_remote_event_table(context, records, "liquidation")
    };
    format!(
        "{}<section class=\"panel\"><div class=\"section-head\"><div><span class=\"eyebrow\">{}</span><h2>{}</h2></div><span>{} {}</span></div>{}</section>",
        page_intro(
            context,
            "Liquidations",
            "清算",
            "Records come from transaction, receipt, logs and deterministic asset flow. Missing profit remains unavailable instead of becoming zero.",
            "记录来自交易、回执、日志与确定性资产流；无法确认的利润保持不可用，不会变成 0。",
        ),
        l.text("LIQUIDATION EVIDENCE", "清算证据"),
        l.text("Public evidence records", "公开证据记录"),
        records.len(),
        l.text("records", "条"),
        table,
    )
}

fn render_execution_cards(
    context: &PageContext<'_>,
    rows: &[VerifiedExecutionRow],
    limit: usize,
) -> String {
    let l = context.locale;
    if rows.is_empty() {
        return format!(
            "<div class=\"honest-empty compact\"><div><strong>{}</strong><p>{}</p></div></div>",
            l.text(
                "No verified records in this source",
                "该数据源暂无已验证记录"
            ),
            l.text(
                "Candidate activity is not promoted into this list.",
                "候选活动不会被提升到该列表。"
            ),
        );
    }
    rows.iter()
        .take(limit)
        .map(|row| {
            format!(
                "<a class=\"execution-row\" href=\"{}\" target=\"_blank\"><em>VERIFIED</em><div><strong>{}</strong><small>{}</small></div><b>{}</b></a>",
                explorer_tx_url(context.chain_id, &row.tx_hash),
                truncate(&row.subject_address, 6),
                row.verified_at.map(format_time).unwrap_or_else(|| l.text("Time unavailable", "时间不可用").to_owned()),
                format_profit(row),
            )
        })
        .collect()
}

fn render_execution_table(context: &PageContext<'_>, rows: &[VerifiedExecutionRow]) -> String {
    let l = context.locale;
    if rows.is_empty() {
        return empty_state(
            context,
            "No deterministic arbitrage PnL is available.",
            "暂无确定性套利收益。",
        );
    }
    let body = rows
        .iter()
        .map(|row| {
            format!(
                "<tr><td><a href=\"{}\" target=\"_blank\"><strong>{}</strong><small>{}</small></a></td><td><code>{}</code></td><td class=\"positive\">{}</td><td>{}</td><td>{:.0}%</td></tr>",
                explorer_tx_url(context.chain_id, &row.tx_hash),
                truncate(&row.tx_hash, 8),
                row.verified_at.map(format_time).unwrap_or_else(|| "—".to_owned()),
                truncate(&row.subject_address, 6),
                format_profit(row),
                row.gas_cost_raw.as_deref().map(format_gas_cost).unwrap_or_else(|| "—".to_owned()),
                row.confidence * 100.0,
            )
        })
        .collect::<String>();
    format!(
        "<div class=\"table-wrap\"><table><thead><tr><th>{}</th><th>{}</th><th>{}</th><th>Gas</th><th>{}</th></tr></thead><tbody>{}</tbody></table></div>",
        l.text("Transaction", "交易"),
        l.text("Actor", "地址"),
        l.text("Verified gross", "已验证毛收益"),
        l.text("Confidence", "置信度"),
        body,
    )
}

fn render_remote_event_table(context: &PageContext<'_>, records: &[Value], kind: &str) -> String {
    let l = context.locale;
    let body = records
        .iter()
        .map(|record| {
            let hash = value_str(record, "tx_hash").unwrap_or("unknown");
            let conclusion = value_str(record, "conclusion")
                .unwrap_or(l.text("Conclusion unavailable", "结论不可用"));
            let severity = value_str(record, "severity").unwrap_or("unknown");
            let confidence = record
                .get("confidence")
                .and_then(Value::as_f64)
                .map(|value| format!("{:.0}%", value * 100.0))
                .unwrap_or_else(|| "—".to_owned());
            let profit = record
                .pointer("/profit_and_cost/profit_raw")
                .and_then(Value::as_str)
                .map(|value| format!("{} raw", escape_html(value)))
                .unwrap_or_else(|| l.text("Not verified", "未验证").to_owned());
            format!(
                "<tr><td><a href=\"{}\" target=\"_blank\"><strong>{}</strong><small>{}</small></a></td><td><em class=\"severity {}\">{}</em></td><td>{}</td><td>{}</td><td>{}</td></tr>",
                explorer_tx_url(8453, hash),
                truncate(hash, 8),
                value_str(record, "created_at").and_then(parse_time).map(format_time).unwrap_or_else(|| "—".to_owned()),
                escape_html(severity),
                escape_html(&severity.to_uppercase()),
                escape_html(conclusion),
                profit,
                confidence,
            )
        })
        .collect::<String>();
    format!(
        "<div class=\"table-wrap\"><table><thead><tr><th>{}</th><th>{}</th><th>{}</th><th>{}</th><th>{}</th></tr></thead><tbody>{}</tbody></table></div>",
        l.text("Transaction", "交易"),
        l.text("Severity", "级别"),
        if kind == "liquidation" {
            l.text("Liquidation conclusion", "清算结论")
        } else {
            l.text("Arbitrage conclusion", "套利结论")
        },
        l.text("Verified profit", "已验证收益"),
        l.text("Confidence", "置信度"),
        body,
    )
}

fn page_intro(
    context: &PageContext<'_>,
    title_en: &str,
    title_zh: &str,
    _copy_en: &str,
    _copy_zh: &str,
) -> String {
    format!(
        "<section class=\"page-intro\"><h1>{}</h1><span>{} · {}</span></section>",
        context.locale.text(title_en, title_zh),
        if context.chain_id == ROBINHOOD_CHAIN_ID {
            "Robinhood Chain"
        } else {
            "Base"
        },
        context.window,
    )
}

fn metric_card(index: &str, label: &str, value: &str, window: &str, _note: &str) -> String {
    format!(
        "<article class=\"metric\"><div><span>{}</span><small>{}</small></div><strong>{}</strong><p>{}</p></article>",
        index,
        escape_html(label),
        value,
        escape_html(window),
    )
}

/// Resolves the wallet lookup box. A redirect rather than a rendered page so
/// the visitor lands on the canonical, shareable address URL.
async fn wallet_lookup(Query(query): Query<PageQuery>) -> Response {
    let context = PageContext::new(&query);
    let target = query
        .address
        .as_deref()
        .map(str::trim)
        .and_then(|address| normalize_address(address).ok())
        .map(|address| context.url(&format!("/address/{address}")))
        .unwrap_or_else(|| context.url("/clusters"));
    (StatusCode::SEE_OTHER, [(header::LOCATION, target)]).into_response()
}

/// Serves a social card. Cards are rendered off the async workers because
/// rasterising is CPU work, and repeated crawls of the same link reuse the
/// cached bytes instead of re-querying the market tables.
async fn card_response(
    state: &MarketWebState,
    key: String,
    card: og::Card,
) -> Result<Response, MarketWebError> {
    let bytes = tokio::task::spawn_blocking(move || og::render(&card))
        .await
        .map_err(internal_error)?
        .map_err(internal_error)?;
    let bytes = Arc::new(bytes);
    let mut cache = state.og_cache.write().await;
    if cache.len() >= OG_CACHE_CAPACITY {
        cache.clear();
    }
    cache.insert(
        key,
        CachedCard {
            rendered_at: std::time::Instant::now(),
            bytes: bytes.clone(),
        },
    );
    drop(cache);
    Ok(png_response(bytes))
}

async fn cached_card(state: &MarketWebState, key: &str) -> Option<Arc<Vec<u8>>> {
    let cache = state.og_cache.read().await;
    cache
        .get(key)
        .filter(|cached| cached.rendered_at.elapsed() < OG_CACHE_TTL)
        .map(|cached| cached.bytes.clone())
}

fn png_response(bytes: Arc<Vec<u8>>) -> Response {
    (
        [
            (header::CONTENT_TYPE, "image/png"),
            // Short browser life, longer edge life: the numbers move, but a
            // crawl storm on one link should not reach this process.
            (
                header::CACHE_CONTROL,
                "public, max-age=120, s-maxage=600, stale-while-revalidate=600",
            ),
        ],
        bytes.as_slice().to_vec(),
    )
        .into_response()
}

async fn og_home_card(State(state): State<MarketWebState>) -> Result<Response, MarketWebError> {
    let key = "home".to_owned();
    if let Some(bytes) = cached_card(&state, &key).await {
        return Ok(png_response(bytes));
    }
    let data = overview_data(&state).await?;
    let card = share::home_card(&data.ticker, &data.gainers);
    card_response(&state, key, card).await
}

async fn og_launch_card(
    State(state): State<MarketWebState>,
    Path(launch_id): Path<Uuid>,
) -> Result<Response, MarketWebError> {
    let key = format!("launch:{launch_id}");
    if let Some(bytes) = cached_card(&state, &key).await {
        return Ok(png_response(bytes));
    }
    let Some(token) = state.market.token(ROBINHOOD_CHAIN_ID, launch_id).await? else {
        // A crawler must still get an image, so unknown links fall back to the
        // site card rather than a broken preview.
        return og_home_card(State(state)).await;
    };
    let card = share::token_card(&token);
    card_response(&state, key, card).await
}

async fn og_address_card(
    State(state): State<MarketWebState>,
    Path(address): Path<String>,
) -> Result<Response, MarketWebError> {
    let address = normalize_address(&address)?;
    let key = format!("address:{address}");
    if let Some(bytes) = cached_card(&state, &key).await {
        return Ok(png_response(bytes));
    }
    let (earnings, rank) = tokio::try_join!(
        async {
            state
                .market
                .address_earnings(ROBINHOOD_CHAIN_ID, &address)
                .await
                .map_err(MarketWebError::from)
        },
        async {
            state
                .market
                .wallet_rank(ROBINHOOD_CHAIN_ID, "24h", &address)
                .await
                .map_err(MarketWebError::from)
        },
    )?;
    let card = share::address_card(&address, &earnings, rank.as_ref());
    card_response(&state, key, card).await
}

async fn og_board_card(
    State(state): State<MarketWebState>,
    Path(board): Path<String>,
) -> Result<Response, MarketWebError> {
    let board = TokenBoard::parse(Some(board.as_str()));
    let key = format!("board:{}", board.key());
    if let Some(bytes) = cached_card(&state, &key).await {
        return Ok(png_response(bytes));
    }
    let tokens = state.market.board(ROBINHOOD_CHAIN_ID, board, 5).await?;
    let (title, subline) = share::board_title(board);
    let card = share::board_card(title, subline, &tokens);
    card_response(&state, key, card).await
}

async fn og_wallets_card(
    State(state): State<MarketWebState>,
    Path(window): Path<String>,
) -> Result<Response, MarketWebError> {
    let window = EarningsWindow::parse(Some(window.as_str()));
    let key = format!("wallets:{}", window.key());
    if let Some(bytes) = cached_card(&state, &key).await {
        return Ok(png_response(bytes));
    }
    let rows = state
        .market
        .wallet_leaderboard(ROBINHOOD_CHAIN_ID, window.key(), 5, 0)
        .await?;
    let card = share::wallets_card(window.key(), &rows);
    card_response(&state, key, card).await
}

/// Crawlers get an explicit invitation to the pages worth indexing, and an
/// explicit refusal for the fragment and API routes that only serve the UI.
async fn robots_txt() -> Response {
    let body = format!(
        "User-agent: *\nAllow: /\nDisallow: /fragments/\nDisallow: /api/\nDisallow: /auth/\nSitemap: {}/sitemap.xml\n",
        public_base_url().trim_end_matches('/')
    );
    ([(header::CONTENT_TYPE, "text/plain; charset=utf-8")], body).into_response()
}

/// Only the stable hubs are listed. Individual launches turn over by the
/// thousand per day, so listing them would be stale before it was fetched.
async fn sitemap_xml() -> Response {
    let base = public_base_url().trim_end_matches('/');
    let urls: String = ["/", "/launches", "/clusters", "/arbitrage", "/liquidations"]
        .into_iter()
        .map(|path| format!("<url><loc>{base}{path}</loc><changefreq>hourly</changefreq></url>"))
        .collect();
    let body = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?><urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">{urls}</urlset>"#
    );
    (
        [(header::CONTENT_TYPE, "application/xml; charset=utf-8")],
        body,
    )
        .into_response()
}

fn layout(
    context: &PageContext<'_>,
    title: &str,
    active: &str,
    wallet: Option<&str>,
    content: &str,
) -> String {
    layout_social(context, title, active, wallet, None, content)
}

/// Same page, with the social preview a shareable page declares for itself.
fn layout_social(
    context: &PageContext<'_>,
    title: &str,
    active: &str,
    wallet: Option<&str>,
    social: Option<&share::Social>,
    content: &str,
) -> String {
    let l = context.locale;
    let fallback;
    let social = match social {
        Some(social) => social,
        None => {
            fallback = share::Social::site(l, "");
            &fallback
        }
    };
    let social_meta = share::meta_tags(social, public_base_url());
    let nav = pages::NAV
        .into_iter()
        .map(|(key, path, en, zh)| {
            format!(
                "<a class=\"{}\" href=\"{}\">{}</a>",
                if active == key { "active" } else { "" },
                context.url(path),
                l.text(en, zh),
            )
        })
        .collect::<String>();
    let wallet_button = wallet
        .map(|address| {
            format!(
                "<button class=\"wallet connected\" type=\"button\" data-wallet-logout>● {}</button>",
                truncate(address, 6)
            )
        })
        .unwrap_or_else(|| {
            format!(
                "<button class=\"wallet\" type=\"button\" data-wallet-connect>{}</button>",
                l.text("Connect wallet", "连接钱包")
            )
        });
    let other_lang = if l == Locale::En { "zh" } else { "en" };
    let language_label = if l == Locale::En { "中文" } else { "EN" };
    format!(
        r#"<!doctype html><html lang="{lang}"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"><title>{title} · LedgerFlow</title>{social_meta}<link rel="icon" type="image/svg+xml" href="/assets/ledgerflow-mark.svg"><style>{legacy}{styles}</style></head><body>
<header class="top"><div class="bar"><a class="brand" href="{home}"><span>{mark}</span><div><strong>LedgerFlow</strong><small>MARKET INTELLIGENCE</small></div></a><nav class="main">{nav}</nav><div class="tools"><form><input type="hidden" name="lang" value="{lang}"><select name="chain_id" onchange="this.form.submit()" aria-label="Network"><option value="4663" {rh}>Robinhood</option><option value="8453" {base}>{base_label}</option></select></form><a class="lang" href="?lang={other_lang}&chain_id={chain_id}">{language_label}</a>{wallet_button}</div></div>
<div class="ticker"><div data-refresh="{ticker_url}" data-every="15" data-eager></div></div></header>
<main>{content}</main><footer class="site"><span>{footer_copy}</span><a href="{seed_url}">{seed_label} →</a></footer>{wallet_script}{script}</body></html>"#,
        lang = l.code(),
        title = escape_html(title),
        social_meta = social_meta,
        // Base carries no market data yet, so the option says so rather than
        // dropping a visitor onto an empty site.
        base_label = l.text("Base (soon)", "Base（即将）"),
        legacy = LEGACY_STYLES,
        styles = ui::STYLES,
        home = context.url("/"),
        mark = brand_mark_svg_source(),
        nav = nav,
        rh = if context.chain_id == ROBINHOOD_CHAIN_ID {
            "selected"
        } else {
            ""
        },
        base = if context.chain_id == 8453 { "selected" } else { "" },
        other_lang = other_lang,
        chain_id = context.chain_id,
        language_label = language_label,
        wallet_button = wallet_button,
        ticker_url = pages::fragment_url(context, "/fragments/ticker", ""),
        content = content,
        footer_copy = l.text(
            "Computed from on-chain data via LedgerFlow's own node. Estimates are labelled. Not financial advice.",
            "数据由 LedgerFlow 自有节点读取链上数据计算，估算值均有标注，不构成投资建议。"
        ),
        seed_url = context.url("/seed-library"),
        seed_label = l.text("Protocol seeds", "协议种子"),
        wallet_script = WALLET_SCRIPT,
        script = ui::SCRIPT,
    )
}

fn empty_state(context: &PageContext<'_>, title_en: &str, title_zh: &str) -> String {
    format!(
        "<div class=\"honest-empty\"><i></i><strong>{}</strong></div>",
        context.locale.text(title_en, title_zh),
    )
}

fn verify_personal_signature(
    address: &str,
    message: &str,
    signature_hex: &str,
) -> Result<(), MarketWebError> {
    let signature_hex = signature_hex
        .trim()
        .strip_prefix("0x")
        .unwrap_or(signature_hex.trim());
    let raw =
        hex::decode(signature_hex).map_err(|_| bad_request("invalid wallet signature encoding"))?;
    if raw.len() != 65 {
        return Err(bad_request("wallet signature must be 65 bytes"));
    }
    let signature =
        Signature::try_from(&raw[..64]).map_err(|_| bad_request("invalid wallet signature"))?;
    let recovery_value = match raw[64] {
        27 | 28 => raw[64] - 27,
        0 | 1 => raw[64],
        _ => return Err(bad_request("invalid wallet recovery id")),
    };
    let recovery_id = RecoveryId::try_from(recovery_value)
        .map_err(|_| bad_request("invalid wallet recovery id"))?;
    let prefix = format!("\x19Ethereum Signed Message:\n{}", message.len());
    let mut hasher = Keccak256::new();
    Sha3Digest::update(&mut hasher, prefix.as_bytes());
    Sha3Digest::update(&mut hasher, message.as_bytes());
    let digest = hasher.finalize();
    let key = VerifyingKey::recover_from_prehash(&digest, &signature, recovery_id)
        .map_err(|_| bad_request("wallet signature recovery failed"))?;
    let encoded = key.to_encoded_point(false);
    let recovered_hash = Keccak256::digest(&encoded.as_bytes()[1..]);
    let recovered = format!("0x{}", hex::encode(&recovered_hash[12..]));
    if recovered != address {
        return Err(bad_request(
            "wallet signature does not match the requested address",
        ));
    }
    Ok(())
}

fn format_profit(row: &VerifiedExecutionRow) -> String {
    let Some(decimals) = row.profit_token_decimals.filter(|value| *value >= 0) else {
        return format!(
            "{} raw · {}",
            escape_html(&row.profit_amount_raw),
            escape_html(
                row.profit_token_symbol
                    .as_deref()
                    .unwrap_or(&truncate(&row.profit_token_address, 6))
            )
        );
    };
    let Some(value) = BigInt::from_str(&row.profit_amount_raw).ok() else {
        return format!("{} raw", escape_html(&row.profit_amount_raw));
    };
    format_decimal(
        &value,
        decimals as usize,
        row.profit_token_symbol.as_deref().unwrap_or("token"),
    )
}

fn format_ranked_profit(row: &VerifiedEarningsRankingRow) -> String {
    let Some(decimals) = row.profit_token_decimals.filter(|value| *value >= 0) else {
        return format!(
            "{} raw · {}",
            escape_html(&row.verified_gross_profit_raw),
            escape_html(
                row.profit_token_symbol
                    .as_deref()
                    .unwrap_or(&row.profit_token_address)
            )
        );
    };
    let Some(value) = BigInt::from_str(&row.verified_gross_profit_raw).ok() else {
        return format!("{} raw", escape_html(&row.verified_gross_profit_raw));
    };
    format_decimal(
        &value,
        decimals as usize,
        row.profit_token_symbol.as_deref().unwrap_or("token"),
    )
}

fn format_gas_cost(raw: &str) -> String {
    BigInt::from_str(raw)
        .ok()
        .map(|value| format_decimal(&-value, 18, "native"))
        .unwrap_or_else(|| format!("{} raw", escape_html(raw)))
}

fn format_decimal(value: &BigInt, decimals: usize, symbol: &str) -> String {
    let negative = value.sign() == num_bigint::Sign::Minus;
    let digits = value.magnitude().to_str_radix(10);
    let padded = if digits.len() <= decimals {
        format!("{}{}", "0".repeat(decimals + 1 - digits.len()), digits)
    } else {
        digits
    };
    let split = padded.len() - decimals;
    let (integer, fractional) = padded.split_at(split);
    let fractional = fractional.trim_end_matches('0');
    let number = if fractional.is_empty() {
        integer.to_owned()
    } else {
        format!("{integer}.{}", &fractional[..fractional.len().min(6)])
    };
    format!(
        "{}{} {}",
        if negative { "-" } else { "+" },
        number,
        escape_html(symbol)
    )
}

fn normalize_address(value: &str) -> Result<String, MarketWebError> {
    let value = value.trim().to_ascii_lowercase();
    let body = value.strip_prefix("0x").unwrap_or(&value);
    if body.len() != 40 || !body.chars().all(|character| character.is_ascii_hexdigit()) {
        return Err(bad_request("invalid EVM address"));
    }
    Ok(format!("0x{body}"))
}

fn cookie_value(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(header::COOKIE)?
        .to_str()
        .ok()?
        .split(';')
        .find_map(|item| {
            let (key, value) = item.trim().split_once('=')?;
            (key == name).then(|| value.to_owned())
        })
}

fn bad_request(message: impl Into<String>) -> MarketWebError {
    MarketWebError {
        status: StatusCode::BAD_REQUEST,
        message: message.into(),
    }
}

fn upstream_error(message: impl Into<String>) -> MarketWebError {
    MarketWebError {
        status: StatusCode::BAD_GATEWAY,
        message: message.into(),
    }
}

/// Reads a positive integer setting, falling back to the default when unset or
/// unparseable. Zero is a valid value and disables the limit it configures.
fn env_u32(key: &str, default: u32) -> u32 {
    env::var(key)
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(default)
}

fn internal_error(error: impl std::fmt::Display) -> MarketWebError {
    MarketWebError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        message: error.to_string(),
    }
}

fn sha256_hex(value: &[u8]) -> String {
    let mut digest = Sha256::new();
    Sha2Digest::update(&mut digest, value);
    hex::encode(digest.finalize())
}

fn normalize_window(value: Option<&str>) -> &'static str {
    match value {
        Some("5m") => "5m",
        Some("15m") => "15m",
        Some("1h") => "1h",
        Some("6h") => "6h",
        Some("7d") => "7d",
        Some("30d") => "30d",
        Some("lifetime") => "lifetime",
        _ => "24h",
    }
}

fn env_duration(key: &str, default_seconds: i64) -> chrono::Duration {
    let seconds = env::var(key)
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default_seconds);
    chrono::Duration::seconds(seconds)
}

fn value_str<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
    value.get(key).and_then(Value::as_str)
}

fn value_u64(value: &Value, key: &str) -> Option<u64> {
    value.get(key).and_then(Value::as_u64)
}

fn parse_time(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|value| value.with_timezone(&Utc))
}

fn newest(
    current: Option<DateTime<Utc>>,
    candidate: Option<DateTime<Utc>>,
) -> Option<DateTime<Utc>> {
    match (current, candidate) {
        (Some(current), Some(candidate)) => Some(current.max(candidate)),
        (None, value) | (value, None) => value,
    }
}

fn verified_profit(record: &Value) -> bool {
    record
        .pointer("/profit_and_cost/profit_raw")
        .is_some_and(|value| !value.is_null())
        && record
            .pointer("/technical_evidence/deterministic_profit/status")
            .and_then(Value::as_str)
            == Some("verified")
}

fn format_time(value: DateTime<Utc>) -> String {
    value.format("%Y-%m-%d %H:%M UTC").to_string()
}

fn format_number(value: i64) -> String {
    let source = value.abs().to_string();
    let mut grouped = String::new();
    for (index, character) in source.chars().rev().enumerate() {
        if index > 0 && index % 3 == 0 {
            grouped.push(',');
        }
        grouped.push(character);
    }
    let mut result = grouped.chars().rev().collect::<String>();
    if value < 0 {
        result.insert(0, '-');
    }
    result
}

fn unavailable(locale: Locale) -> String {
    format!(
        "<span class=\"unavailable\">{}</span>",
        locale.text("Not indexed", "尚未索引")
    )
}

fn optional_text(value: Option<&str>, locale: Locale) -> String {
    value
        .map(escape_html)
        .unwrap_or_else(|| locale.text("Not indexed", "尚未索引").to_owned())
}

fn optional_i64(value: Option<i64>) -> String {
    value.map(format_number).unwrap_or_else(|| "—".to_owned())
}

fn percent_html(value: &str) -> String {
    let positive = !value.starts_with('-');
    format!(
        "<span class=\"{}\">{}{}</span>",
        if positive { "positive" } else { "negative" },
        if positive { "+" } else { "" },
        escape_html(value)
    )
}

fn truncate(value: &str, keep: usize) -> String {
    if value.len() <= keep * 2 {
        value.to_owned()
    } else {
        format!("{}…{}", &value[..keep], &value[value.len() - keep..])
    }
}

fn humanize(value: &str) -> String {
    value
        .split('_')
        .map(|part| {
            let mut characters = part.chars();
            characters
                .next()
                .map(|first| format!("{}{}", first.to_uppercase(), characters.as_str()))
                .unwrap_or_default()
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn escape_html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

fn explorer_tx_url(chain_id: u64, hash: &str) -> String {
    if chain_id == ROBINHOOD_CHAIN_ID {
        format!(
            "https://robinhoodchain.blockscout.com/tx/{}",
            escape_html(hash)
        )
    } else {
        format!("https://basescan.org/tx/{}", escape_html(hash))
    }
}

fn brand_mark_svg_source() -> &'static str {
    r##"<svg viewBox="0 0 64 64" xmlns="http://www.w3.org/2000/svg" aria-hidden="true" role="img"><rect x="6" y="6" width="52" height="52" rx="12" fill="#111"/><path d="M20 19H44V26H28V31H42V38H28V45H20V19Z" fill="#fff"/><path d="M32 19H44V26H40V45H32V19Z" fill="#f60"/></svg>"##
}

const WALLET_SCRIPT: &str = r#"<script>
const connect=document.querySelector('[data-wallet-connect]');
if(connect) connect.addEventListener('click',async()=>{try{if(!window.ethereum)throw new Error(document.documentElement.lang==='zh'?'未检测到浏览器钱包':'No browser wallet detected');connect.disabled=true;connect.textContent=document.documentElement.lang==='zh'?'连接中…':'Connecting…';const [address]=await window.ethereum.request({method:'eth_requestAccounts'});const chainHex=await window.ethereum.request({method:'eth_chainId'});const challenge=await fetch('/auth/wallet/challenge',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({address,chain_id:parseInt(chainHex,16)})}).then(async r=>{const v=await r.json();if(!r.ok)throw new Error(v.error);return v});const signature=await window.ethereum.request({method:'personal_sign',params:[challenge.message,address]});await fetch('/auth/wallet/verify',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({challenge_id:challenge.challenge_id,address,signature})}).then(async r=>{const v=await r.json();if(!r.ok)throw new Error(v.error)});location.href='/address/'+address.toLowerCase()+'?lang='+document.documentElement.lang}catch(error){alert(error.message);connect.disabled=false;connect.textContent=document.documentElement.lang==='zh'?'连接钱包':'Connect wallet'}});
const logout=document.querySelector('[data-wallet-logout]');if(logout) logout.addEventListener('click',async()=>{await fetch('/auth/wallet/logout',{method:'POST'});location.reload()});
</script>"#;

const LEGACY_STYLES: &str = r#"
.hero{display:grid;grid-template-columns:minmax(0,1fr) 360px;gap:46px;align-items:end;padding:20px 0 30px}.eyebrow{display:block;color:#c1bbb3;font-size:12px;letter-spacing:.12em;font-weight:700}.hero h1{font-size:clamp(34px,3.2vw,44px);line-height:1.04;max-width:850px;margin:10px 0 12px;letter-spacing:-.045em}.hero-copy>p{color:var(--muted);font-size:14px;max-width:790px}.hero-actions{display:flex;gap:10px;margin-top:18px}.button{display:inline-flex;align-items:center;justify-content:center;min-height:42px;border:1px solid #393939;border-radius:8px;padding:0 16px;font-size:13px;font-weight:700;background:#151515}.button.primary{background:var(--orange);border-color:var(--orange);color:white}.button.small{min-height:35px;margin-top:12px}.fresh-card{border-left:1px solid var(--line);padding:5px 0 4px 24px}.fresh-card>span{font-size:12px;color:#c0b9b0;letter-spacing:.1em}.fresh-card>strong{display:block;font-size:18px;margin:8px 0}.fresh-card div{font-size:12px;color:var(--muted);margin-top:10px}.fresh-card b{color:var(--text)}
.metrics{display:grid;grid-template-columns:repeat(4,1fr);border:1px solid var(--line);border-radius:12px;overflow:hidden;margin-bottom:22px}.metric{padding:18px 22px;min-height:120px;background:var(--panel);border-right:1px solid var(--line)}.metric:last-child{border:0}.metric>div{display:flex;gap:10px;color:var(--muted);font-size:12px}.metric>strong{display:flex;align-items:baseline;gap:7px;min-height:40px;margin:13px 0 0;font-size:27px;line-height:1.15}.metric>strong small{font-size:12px;color:var(--muted);font-weight:500}.metric p{font-size:12px;color:var(--muted);font-style:normal;margin:3px 0}.accent{color:var(--orange)}.positive{color:var(--green)}.negative{color:var(--red)}.slash{color:#555}.unavailable{font-size:17px;color:#777}
.split{display:grid;grid-template-columns:minmax(0,1.5fr) minmax(350px,.65fr);gap:22px;margin-bottom:22px}.panel{border:1px solid var(--line);border-radius:12px;background:var(--panel);padding:22px;margin-bottom:22px}.section-head{display:flex;align-items:center;justify-content:space-between;gap:16px;margin-bottom:18px}.section-head h2,.panel h2{font-size:19px;margin:3px 0}.section-head>a,.section-head>span{color:var(--muted);font-size:12px}.table-wrap{overflow:auto}table{border-collapse:collapse;width:100%;font-size:12px}th{text-align:left;color:#888;font-weight:600;white-space:nowrap;padding:9px 12px;border-bottom:1px solid var(--line)}td{padding:13px 12px;border-bottom:1px solid #222;vertical-align:middle}tbody tr:last-child td{border-bottom:0}td small,td strong{display:block}td small{color:#777;margin-top:2px}td code{font-size:12px;color:#bbb}.token-cell{display:flex;align-items:center;gap:10px;min-width:170px}.token-cell>span{display:grid;place-items:center;width:34px;height:34px;border-radius:9px;background:#251a12;color:#ff9e5d;font-weight:800}.state,.severity,.execution-row em{display:inline-flex;padding:3px 7px;border-radius:5px;background:#1f1c18;color:#e6aa7e;font-size:12px;font-style:normal;white-space:nowrap}.severity.high,.severity.critical{background:#2c1717;color:#ffaaa3}
.honest-empty{min-height:180px;display:flex;align-items:center;justify-content:center;gap:16px;border:1px dashed #343434;border-radius:9px;padding:24px;text-align:left}.honest-empty i{width:10px;height:10px;background:var(--orange);border-radius:50%;box-shadow:0 0 0 7px rgba(255,102,0,.1)}.honest-empty strong{font-size:15px}.honest-empty p{max-width:650px;color:var(--muted);font-size:13px;margin:7px 0}.honest-empty small{font-size:12px;color:#777}.honest-empty.compact{min-height:120px}.execution-row{display:grid;grid-template-columns:auto 1fr auto;align-items:center;gap:12px;padding:14px 0;border-bottom:1px solid #222}.execution-row:last-child{border:0}.execution-row div strong,.execution-row div small{display:block}.execution-row div small{font-size:12px;color:#777}.execution-row>b{font-size:13px;color:var(--green)}.method{display:grid;grid-template-columns:300px 1fr;gap:50px;align-items:center}.method ol{display:grid;grid-template-columns:repeat(3,1fr);list-style:none;padding:0;margin:0;gap:20px}.method li{font-size:13px;color:#ccc}.method li b{display:block;color:var(--orange);font-size:12px;margin-bottom:8px}
.signal-contract{display:grid;grid-template-columns:repeat(4,1fr);border:1px solid var(--line);border-radius:9px;margin-bottom:14px;overflow:hidden}.signal-contract>div{display:flex;flex-direction:column;gap:4px;padding:15px;border-right:1px solid var(--line)}.signal-contract>div:last-child{border:0}.signal-contract span{font-size:12px;color:var(--orange)}.signal-contract strong{font-size:13px}.signal-contract small{font-size:12px;color:var(--muted)}
.page-intro{padding:28px 0 38px}.page-intro h1{font-size:48px;margin-bottom:15px}.cluster-section-head{margin-top:30px}.cluster-filters{display:flex;flex-wrap:wrap;gap:8px;margin:-3px 0 18px}.cluster-filters a{display:inline-flex;align-items:center;gap:8px;min-height:36px;padding:0 13px;border:1px solid var(--line);border-radius:18px;color:var(--muted);font-size:12px;background:#101010}.cluster-filters a:hover,.cluster-filters a.active{border-color:#80502f;color:#fff;background:#241910}.cluster-filters b{display:inline-grid;place-items:center;min-width:20px;height:20px;padding:0 5px;border-radius:10px;background:#282828;color:#d6d0c8;font-size:12px}.cluster-filters a.active b{background:#663317;color:#ffd2b3}.cluster-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:16px}.cluster{display:block;border:1px solid var(--line);border-radius:12px;background:var(--panel);padding:20px;transition:border-color .15s ease}.cluster:hover{border-color:#555}.cluster>div{display:flex;justify-content:space-between;align-items:center}.cluster>div small{font-size:12px;color:var(--muted)}.cluster h3{margin:18px 0 5px}.cluster code{font-size:12px;color:#aaa}.cluster dl,.detail-grid dl,.lookup-match dl{margin:18px 0}.cluster dl div,.detail-grid dl div,.lookup-match dl div{display:flex;justify-content:space-between;padding:8px 0;border-bottom:1px solid #222}.cluster dt,.detail-grid dt,.lookup-match dt{color:var(--muted);font-size:12px}.cluster dd,.detail-grid dd,.lookup-match dd{margin:0;font-size:13px}.cluster p,.scope-note p,.limitations p,.cluster-explanation p{color:var(--muted);font-size:13px}.open-link{display:block;color:#ddd;font-size:12px;margin-top:14px}.address-lookup{display:grid;grid-template-columns:minmax(260px,.7fr) minmax(480px,1.3fr);gap:28px;align-items:start}.address-lookup>div>p{color:var(--muted);font-size:13px;max-width:530px}.address-lookup form>div{display:grid;grid-template-columns:1fr auto;gap:8px}.address-lookup input{min-width:0;height:48px;border:1px solid #3b3b3b;border-radius:8px;background:#090909;color:var(--text);padding:0 14px;font:13px ui-monospace,SFMono-Regular,Menlo,monospace}.address-lookup button{height:48px;border:0;border-radius:8px;padding:0 20px;background:var(--orange);color:white;font-weight:750;cursor:pointer}.lookup-prompt,.lookup-error{grid-column:1/-1;border-top:1px solid var(--line);padding-top:18px}.lookup-prompt p,.lookup-error span{display:block;color:var(--muted);font-size:13px;margin:5px 0}.lookup-error strong{color:var(--red)}.lookup-results{grid-column:1/-1;border-top:1px solid var(--line);padding-top:20px}.lookup-address{display:flex;align-items:center;gap:12px;min-width:0}.lookup-address span,.lookup-address small{color:var(--muted);font-size:12px}.lookup-address code{font-size:13px;overflow-wrap:anywhere}.lookup-address small{margin-left:auto}.lookup-metrics{display:grid;grid-template-columns:repeat(4,1fr);border:1px solid var(--line);border-radius:9px;overflow:hidden;margin:18px 0}.lookup-metrics>div{padding:13px;border-right:1px solid var(--line)}.lookup-metrics>div:last-child{border:0}.lookup-metrics span,.lookup-metrics strong{display:block}.lookup-metrics span{color:var(--muted);font-size:12px}.lookup-metrics strong{font-size:19px;margin-top:4px}.lookup-columns{display:grid;grid-template-columns:1fr 1fr;gap:16px}.lookup-columns>section{border:1px solid var(--line);border-radius:9px;padding:14px}.mini-head{display:flex;justify-content:space-between;align-items:center;color:var(--muted);font-size:12px;margin-bottom:8px}.lookup-match{display:grid;grid-template-columns:minmax(0,1fr) minmax(180px,.7fr) auto;gap:14px;align-items:center;border-top:1px solid #242424;padding:13px 0}.lookup-match>div strong,.lookup-match>div small{display:block}.lookup-match>div strong{font-size:13px;margin-top:5px}.lookup-match>div small{font-size:12px;color:var(--muted)}.lookup-match dl{margin:0}.lookup-match>span{color:var(--orange)}.lookup-none{display:flex;flex-direction:column;gap:4px;border-top:1px solid #242424;padding:14px 0}.lookup-none strong{font-size:13px}.lookup-none span,.lookup-limitation{color:var(--muted);font-size:12px}.protocol-hit{display:flex;justify-content:space-between;gap:14px;border-top:1px solid #242424;padding:13px 0}.protocol-hit strong,.protocol-hit small{display:block}.protocol-hit small{color:var(--muted);font-size:12px}.protocol-hit>div:last-child{text-align:right}.protocol-hit span,.protocol-hit b{display:block;font-size:12px}.protocol-hit span{color:var(--orange)}.cluster-detail-hero h1{font-size:42px}.cluster-explanation{border-left:3px solid var(--orange)}.back{display:inline-block;color:var(--muted);font-size:13px;margin-bottom:22px}.detail-hero{display:flex;justify-content:space-between;align-items:flex-end;border-bottom:1px solid var(--line);padding:5px 0 32px;margin-bottom:22px}.detail-hero h1{font-size:50px;margin:10px 0 4px}.detail-hero code{color:var(--muted)}.detail-hero aside{display:flex;flex-direction:column;align-items:flex-end}.detail-hero aside>strong{font-size:38px}.detail-hero aside span,.detail-hero aside small{color:var(--muted);font-size:12px}.tags{display:flex;align-items:center;gap:10px;margin-top:15px;color:var(--muted);font-size:12px}.detail-grid{display:grid;grid-template-columns:repeat(2,1fr);gap:22px}.answer{padding:18px;background:#151515;border-radius:8px;margin-top:18px}.answer strong{font-size:18px}.answer p{font-size:13px;color:var(--muted)}
.page-intro{display:flex;align-items:baseline;justify-content:space-between;gap:20px;padding:12px 0 22px}.page-intro h1{font-size:29px;line-height:1.15;margin:0;letter-spacing:-.03em}.page-intro>span{font-size:12px;color:var(--muted)}
.address-lookup{grid-template-columns:minmax(220px,.5fr) minmax(480px,1.5fr);gap:24px}.address-lookup input,.address-lookup button{height:44px}.lookup-prompt,.lookup-error{padding-top:15px}.cluster-detail-hero h1{font-size:38px}.back{margin-bottom:18px}.detail-hero{padding-bottom:25px}.detail-hero h1{font-size:42px}.detail-hero aside>strong{font-size:34px}.detail-grid{gap:16px}.answer{padding:14px;margin-top:12px}
.ranking-panel>.section-head{margin-bottom:10px}.asset-ranking{margin-top:18px;border-top:1px solid var(--line);padding-top:14px}.asset-ranking:first-of-type{margin-top:0;border-top:0;padding-top:0}.asset-heading{display:flex;align-items:center;gap:10px;margin-bottom:7px}.asset-heading strong{font-size:16px}.asset-heading code{font-size:12px;color:var(--muted)}.rank-empty{display:flex;align-items:center;min-height:74px;color:var(--muted);font-size:13px}.earnings-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:12px}.earning-card{border:1px solid var(--line);border-radius:9px;padding:15px;background:#0d0d0d}.earning-card>span{display:block;color:var(--muted);font-size:12px}.earning-card>strong{display:block;color:var(--green);font-size:21px;margin:6px 0 10px}.earning-card dl{margin:0}.earning-card dl div{display:flex;justify-content:space-between;border-top:1px solid #222;padding:6px 0}.earning-card dt{color:var(--muted);font-size:12px}.earning-card dd{margin:0;font-size:12px}
@media(max-width:1200px){.split{grid-template-columns:1fr}.metrics{grid-template-columns:repeat(2,1fr)}.metric:nth-child(2){border-right:0}.metric:nth-child(-n+2){border-bottom:1px solid var(--line)}.cluster-grid{grid-template-columns:repeat(2,1fr)}.address-lookup{grid-template-columns:1fr}.lookup-columns{grid-template-columns:1fr}.earnings-grid{grid-template-columns:repeat(2,minmax(0,1fr))}}
"#;

#[cfg(test)]
mod tests {
    use super::*;
    use k256::ecdsa::SigningKey;

    #[test]
    fn wallet_signature_accepts_matching_eip_191_signer() {
        let signing_key = SigningKey::from_bytes(&[7_u8; 32].into()).expect("valid test key");
        let verifying_key = signing_key.verifying_key().to_encoded_point(false);
        let address_hash = Keccak256::digest(&verifying_key.as_bytes()[1..]);
        let address = format!("0x{}", hex::encode(&address_hash[12..]));
        let message = "LedgerFlow wallet authentication test";
        let prefix = format!("\x19Ethereum Signed Message:\n{}", message.len());
        let mut hasher = Keccak256::new();
        Sha3Digest::update(&mut hasher, prefix.as_bytes());
        Sha3Digest::update(&mut hasher, message.as_bytes());
        let digest = hasher.finalize();
        let (signature, recovery_id) = signing_key
            .sign_prehash_recoverable(&digest)
            .expect("recoverable signature");
        let mut raw = signature.to_bytes().to_vec();
        raw.push(recovery_id.to_byte() + 27);

        verify_personal_signature(&address, message, &format!("0x{}", hex::encode(raw)))
            .expect("matching EIP-191 signature");
    }

    #[test]
    fn wallet_signature_rejects_malformed_input() {
        assert!(
            verify_personal_signature(
                "0x0000000000000000000000000000000000000000",
                "hello",
                "0x12"
            )
            .is_err()
        );
    }

    #[test]
    fn new_web_has_no_email_auth_copy_or_route() {
        assert!(!WALLET_SCRIPT.contains("email"));
        assert_ne!(SESSION_COOKIE, "ledgerflow_session");
    }

    #[test]
    fn css_never_uses_sub_twelve_pixel_type() {
        for value in [
            "font-size:11px",
            "font-size:10px",
            "font-size:9px",
            "font-size:8px",
        ] {
            assert!(
                !LEGACY_STYLES.contains(value) && !ui::STYLES.contains(value),
                "found forbidden size {value}"
            );
        }
    }

    #[test]
    fn market_navigation_uses_query_first_address_intelligence_name() {
        let query = PageQuery::default();
        let context = PageContext::new(&query);
        let page = layout(&context, "test", "clusters", None, "");
        assert!(page.contains("Address Intelligence"));
        assert!(!page.contains("Smart Money"));
        assert!(!page.contains("赚钱地址"));
    }

    #[test]
    fn network_and_window_are_normalized() {
        let base = PageQuery {
            lang: Some("zh".to_owned()),
            chain_id: Some(8453),
            window: Some("7d".to_owned()),
            ..PageQuery::default()
        };
        let context = PageContext::new(&base);
        assert_eq!(context.chain_id, 8453);
        assert_eq!(context.window, "7d");
        assert_eq!(context.locale, Locale::Zh);
    }

    #[test]
    fn protocol_clusters_have_stable_user_facing_categories() {
        assert_eq!(cluster_category("token_launch", "issuer_cohort"), "tokens");
        assert_eq!(cluster_category("dex_execution", "searcher"), "arbitrage");
        assert_eq!(
            cluster_category("liquidation", "liquidator"),
            "liquidations"
        );
        assert_eq!(cluster_category("dex_execution", "buyer_cohort"), "other");
        assert_eq!(normalize_cluster_category(Some("unknown")), "all");
    }

    #[test]
    fn page_intro_is_compact_and_self_describing() {
        let query = PageQuery::default();
        let context = PageContext::new(&query);
        let html = page_intro(
            &context,
            "Arbitrage Earnings",
            "套利收益",
            "unused",
            "unused",
        );
        assert!(html.contains("Arbitrage Earnings"));
        assert!(html.contains("Robinhood Chain · 24h"));
        assert!(!html.contains("<p>"));
        assert!(!html.contains("unused"));
    }

    #[test]
    fn earnings_format_keeps_profit_and_native_gas_separate() {
        let row = VerifiedEarningsRankingRow {
            rank: 1,
            cluster_id: None,
            subject_address: "0x1111111111111111111111111111111111111111".to_owned(),
            profit_token_address: "0x2222222222222222222222222222222222222222".to_owned(),
            profit_token_symbol: Some("WETH".to_owned()),
            profit_token_decimals: Some(18),
            verified_gross_profit_raw: "1250000000000000000".to_owned(),
            exact_gas_cost_raw: "100000000000000000".to_owned(),
            verified_execution_count: 2,
            economically_verified_count: 2,
            economically_evaluated_count: 4,
            coverage_ratio: 0.5,
            last_execution_at: None,
        };
        assert_eq!(format_ranked_profit(&row), "+1.25 WETH");
        assert_eq!(format_gas_cost(&row.exact_gas_cost_raw), "-0.1 native");
    }
}
