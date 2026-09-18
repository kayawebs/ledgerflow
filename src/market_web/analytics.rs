//! Traffic counting for the public site.
//!
//! A launch that cannot be measured cannot be steered, so page views are
//! recorded here. What is stored is deliberately thin: the path, where the
//! visit came from, and a salted digest that rotates every day, so returning
//! visitors can be counted within a day and not tracked beyond it. No IP
//! address or user agent is written.

use std::sync::Arc;

use axum::{extract::Request, http::header, middleware::Next, response::Response};
use chrono::Utc;
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use tokio::sync::mpsc::{Sender, error::TrySendError};

/// One recorded visit, before it reaches the database.
#[derive(Clone, Debug)]
pub struct View {
    pub path: String,
    pub referrer_host: Option<String>,
    pub utm_source: Option<String>,
    pub country: Option<String>,
    pub device: Option<String>,
    pub visitor_hash: Option<String>,
}

/// Views are batched: a page view must never make a visitor wait on a write,
/// and a burst of traffic should cost one insert rather than hundreds.
const CHANNEL_CAPACITY: usize = 4_096;
const FLUSH_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);
const FLUSH_BATCH: usize = 256;

#[derive(Clone)]
pub struct Recorder {
    sender: Sender<View>,
}

impl Recorder {
    /// Starts the background writer and returns the handle the middleware uses.
    pub fn spawn(pool: PgPool) -> Self {
        let (sender, mut receiver) = tokio::sync::mpsc::channel::<View>(CHANNEL_CAPACITY);
        tokio::spawn(async move {
            let mut pending: Vec<View> = Vec::with_capacity(FLUSH_BATCH);
            let mut ticker = tokio::time::interval(FLUSH_INTERVAL);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    received = receiver.recv() => match received {
                        Some(view) => {
                            pending.push(view);
                            if pending.len() >= FLUSH_BATCH {
                                flush(&pool, &mut pending).await;
                            }
                        }
                        None => {
                            flush(&pool, &mut pending).await;
                            break;
                        }
                    },
                    _ = ticker.tick() => flush(&pool, &mut pending).await,
                }
            }
        });
        Self { sender }
    }

    /// Never blocks: if the writer is behind, the view is dropped rather than
    /// slowing the page down.
    pub fn record(&self, view: View) {
        if let Err(TrySendError::Full(_)) = self.sender.try_send(view) {
            tracing::debug!("page view dropped: analytics queue full");
        }
    }
}

async fn flush(pool: &PgPool, pending: &mut Vec<View>) {
    if pending.is_empty() {
        return;
    }
    let batch = std::mem::take(pending);
    let paths: Vec<String> = batch.iter().map(|view| view.path.clone()).collect();
    let referrers: Vec<Option<String>> = batch.iter().map(|v| v.referrer_host.clone()).collect();
    let utm: Vec<Option<String>> = batch.iter().map(|v| v.utm_source.clone()).collect();
    let countries: Vec<Option<String>> = batch.iter().map(|v| v.country.clone()).collect();
    let devices: Vec<Option<String>> = batch.iter().map(|v| v.device.clone()).collect();
    let visitors: Vec<Option<String>> = batch.iter().map(|v| v.visitor_hash.clone()).collect();
    let result = sqlx::query(
        r#"insert into market.page_views (path, referrer_host, utm_source, country, device, visitor_hash)
           select * from unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[])"#,
    )
    .bind(&paths)
    .bind(&referrers)
    .bind(&utm)
    .bind(&countries)
    .bind(&devices)
    .bind(&visitors)
    .execute(pool)
    .await;
    if let Err(error) = result {
        tracing::warn!(%error, rows = batch.len(), "page view batch insert failed");
    }
}

/// Only pages a person can land on are counted. Fragments, APIs, cards and
/// health probes would drown the real signal.
fn countable(path: &str) -> bool {
    !(path.starts_with("/fragments")
        || path.starts_with("/api")
        || path.starts_with("/auth")
        || path.starts_with("/og/")
        || path.starts_with("/assets")
        || path == "/health"
        || path == "/ready"
        || path == "/metrics"
        || path == "/robots.txt"
        || path == "/sitemap.xml")
}

/// Collapses a referrer to its host, which is what a campaign readout needs.
fn referrer_host(value: &str) -> Option<String> {
    let value = value.trim();
    let without_scheme = value
        .split_once("://")
        .map(|(_, rest)| rest)
        .unwrap_or(value);
    let host = without_scheme
        .split(['/', '?', '#'])
        .next()
        .unwrap_or("")
        .trim_start_matches("www.")
        .to_ascii_lowercase();
    (!host.is_empty() && host.len() <= 128).then_some(host)
}

fn utm_source(query: Option<&str>) -> Option<String> {
    query?.split('&').find_map(|pair| {
        let (key, value) = pair.split_once('=')?;
        (key == "utm_source" && !value.is_empty())
            .then(|| value.chars().take(64).collect::<String>())
    })
}

/// Crawlers and previews are labelled rather than dropped: a spike of link
/// unfurls is itself a signal that the link is being shared.
fn device_of(user_agent: &str) -> &'static str {
    let agent = user_agent.to_ascii_lowercase();
    if agent.contains("bot")
        || agent.contains("crawler")
        || agent.contains("spider")
        || agent.contains("preview")
        || agent.contains("twitterbot")
        || agent.contains("facebookexternalhit")
    {
        "bot"
    } else if agent.contains("mobile") || agent.contains("android") || agent.contains("iphone") {
        "mobile"
    } else {
        "desktop"
    }
}

/// Salted, day-rotating digest of the caller. The salt lives only in this
/// process, so the value cannot be linked back to a person or across days.
fn visitor_hash(salt: &str, ip: &str, user_agent: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(salt.as_bytes());
    digest.update(Utc::now().format("%Y-%m-%d").to_string().as_bytes());
    digest.update(ip.as_bytes());
    digest.update(user_agent.as_bytes());
    hex::encode(&digest.finalize()[..12])
}

/// Middleware that records one view per page request.
pub async fn record_view(
    axum::extract::State(state): axum::extract::State<Arc<AnalyticsState>>,
    request: Request,
    next: Next,
) -> Response {
    let path = request.uri().path().to_string();
    if countable(&path) {
        let headers = request.headers();
        let header_value = |name: &str| {
            headers
                .get(name)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned)
        };
        let user_agent = header_value(header::USER_AGENT.as_str()).unwrap_or_default();
        // Behind Cloudflare the socket address is the proxy, so the forwarded
        // client address is what identifies a visitor.
        let ip = header_value("cf-connecting-ip")
            .or_else(|| {
                header_value("x-forwarded-for")
                    .and_then(|value| value.split(',').next().map(|first| first.trim().to_owned()))
            })
            .unwrap_or_default();
        state.recorder.record(View {
            path,
            referrer_host: header_value(header::REFERER.as_str())
                .as_deref()
                .and_then(referrer_host),
            utm_source: utm_source(request.uri().query()),
            country: header_value("cf-ipcountry").filter(|code| code.len() <= 8),
            device: Some(device_of(&user_agent).to_owned()),
            visitor_hash: Some(visitor_hash(&state.salt, &ip, &user_agent)),
        });
    }
    next.run(request).await
}

pub struct AnalyticsState {
    pub recorder: Recorder,
    pub salt: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_pages_but_not_machinery() {
        assert!(countable("/"));
        assert!(countable("/launches"));
        assert!(countable("/address/0xabc"));
        assert!(!countable("/fragments/ticker"));
        assert!(!countable("/og/home.png"));
        assert!(!countable("/og/launch/abc/card.png"));
        assert!(!countable("/metrics"));
    }

    #[test]
    fn referrers_collapse_to_a_host() {
        assert_eq!(referrer_host("https://t.co/abc"), Some("t.co".to_owned()));
        assert_eq!(
            referrer_host("https://www.Google.com/search?q=x"),
            Some("google.com".to_owned())
        );
        assert_eq!(referrer_host(""), None);
    }

    #[test]
    fn reads_the_campaign_tag() {
        assert_eq!(
            utm_source(Some("lang=en&utm_source=twitter")),
            Some("twitter".to_owned())
        );
        assert_eq!(utm_source(Some("lang=en")), None);
        assert_eq!(utm_source(None), None);
    }

    #[test]
    fn labels_link_unfurls_as_bots() {
        assert_eq!(device_of("Twitterbot/1.0"), "bot");
        assert_eq!(device_of("Mozilla/5.0 (iPhone; CPU iPhone OS)"), "mobile");
        assert_eq!(device_of("Mozilla/5.0 (X11; Linux x86_64)"), "desktop");
    }

    #[test]
    fn visitor_hash_hides_the_caller_and_differs_per_visitor() {
        let one = visitor_hash("salt", "1.2.3.4", "agent");
        let two = visitor_hash("salt", "5.6.7.8", "agent");
        assert_ne!(one, two);
        assert_eq!(one, visitor_hash("salt", "1.2.3.4", "agent"));
        assert!(!one.contains("1.2.3.4"));
        assert_eq!(one.len(), 24);
    }
}
