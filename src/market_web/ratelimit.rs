//! Per-caller request limits.
//!
//! Social cards are generated per address and per launch, so their URLs can be
//! enumerated: each new one costs a database query and a rasterise. Cloudflare
//! absorbs repeats of the same URL, but not a walk through many different ones,
//! so the process keeps its own fixed-window counter per caller.

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use axum::{
    extract::{Request, State},
    http::{HeaderMap, StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use tokio::sync::Mutex;

const WINDOW: Duration = Duration::from_secs(60);
/// Counters are dropped once their window has passed; this bounds the map in
/// the meantime so a flood of distinct addresses cannot grow it without limit.
const MAX_TRACKED_CALLERS: usize = 50_000;

#[derive(Clone, Copy)]
struct Counter {
    window_start: Instant,
    hits: u32,
}

pub struct RateLimiter {
    /// Requests per minute for ordinary pages. Zero disables the limit.
    pub page_limit: u32,
    /// Requests per minute for the social card routes, which cost more.
    pub card_limit: u32,
    counters: Mutex<HashMap<(bool, String), Counter>>,
}

impl RateLimiter {
    pub fn new(page_limit: u32, card_limit: u32) -> Self {
        Self {
            page_limit,
            card_limit,
            counters: Mutex::new(HashMap::new()),
        }
    }

    /// Returns false when the caller has used up this minute's allowance.
    async fn allow(&self, cards: bool, caller: &str) -> bool {
        let limit = if cards {
            self.card_limit
        } else {
            self.page_limit
        };
        if limit == 0 {
            return true;
        }
        let now = Instant::now();
        let mut counters = self.counters.lock().await;
        if counters.len() > MAX_TRACKED_CALLERS {
            counters.retain(|_, counter| now.duration_since(counter.window_start) < WINDOW);
            if counters.len() > MAX_TRACKED_CALLERS {
                counters.clear();
            }
        }
        let counter = counters
            .entry((cards, caller.to_owned()))
            .or_insert(Counter {
                window_start: now,
                hits: 0,
            });
        if now.duration_since(counter.window_start) >= WINDOW {
            counter.window_start = now;
            counter.hits = 0;
        }
        counter.hits += 1;
        counter.hits <= limit
    }
}

/// Health and metrics endpoints are never limited: monitoring must keep working
/// while a caller is being throttled.
fn exempt(path: &str) -> bool {
    matches!(path, "/health" | "/ready" | "/metrics")
}

fn is_card(path: &str) -> bool {
    path.starts_with("/og/")
}

/// Behind Cloudflare the socket address is the proxy, so the forwarded client
/// address identifies the caller.
fn caller(headers: &HeaderMap) -> String {
    let value = |name: &str| {
        headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned)
    };
    value("cf-connecting-ip")
        .or_else(|| {
            value("x-forwarded-for")
                .and_then(|value| value.split(',').next().map(|first| first.trim().to_owned()))
        })
        .unwrap_or_else(|| "unknown".to_owned())
}

pub async fn enforce(
    State(limiter): State<Arc<RateLimiter>>,
    request: Request,
    next: Next,
) -> Response {
    let path = request.uri().path().to_owned();
    if !exempt(&path) {
        let cards = is_card(&path);
        let caller = caller(request.headers());
        if !limiter.allow(cards, &caller).await {
            tracing::debug!(%path, "rate limited");
            return (
                StatusCode::TOO_MANY_REQUESTS,
                [(header::RETRY_AFTER, "60")],
                "rate limited\n",
            )
                .into_response();
        }
    }
    next.run(request).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn allows_up_to_the_limit_then_refuses() {
        let limiter = RateLimiter::new(2, 1);
        assert!(limiter.allow(false, "1.2.3.4").await);
        assert!(limiter.allow(false, "1.2.3.4").await);
        assert!(!limiter.allow(false, "1.2.3.4").await);
        // A different caller has its own allowance.
        assert!(limiter.allow(false, "5.6.7.8").await);
    }

    #[tokio::test]
    async fn cards_are_counted_separately_from_pages() {
        let limiter = RateLimiter::new(5, 1);
        assert!(limiter.allow(true, "1.2.3.4").await);
        assert!(!limiter.allow(true, "1.2.3.4").await);
        // The page allowance is untouched by the card requests.
        assert!(limiter.allow(false, "1.2.3.4").await);
    }

    #[tokio::test]
    async fn a_zero_limit_disables_the_check() {
        let limiter = RateLimiter::new(0, 0);
        for _ in 0..1_000 {
            assert!(limiter.allow(false, "1.2.3.4").await);
        }
    }

    #[test]
    fn monitoring_and_card_paths_are_classified() {
        assert!(exempt("/health"));
        assert!(!exempt("/launches"));
        assert!(is_card("/og/home.png"));
        assert!(is_card("/og/address/0xabc/card.png"));
        assert!(!is_card("/address/0xabc"));
    }
}
