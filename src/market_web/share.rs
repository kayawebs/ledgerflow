//! Social previews and the share affordances that produce them.
//!
//! A link to this site is far more often pasted than typed, so each page worth
//! sharing declares a [`Social`] preview and offers a one-click share that
//! pre-fills the numbers somebody would otherwise retype.

use super::{Locale, PageContext, escape_html, og, ui};
use crate::services::market_read::{AddressEarnings, MarketTicker, TokenRow, WalletRankRow};

/// What a social client should show for a page.
pub struct Social {
    pub title: String,
    pub description: String,
    /// Card path such as `/og/launch/{id}.png`, resolved against the public
    /// base URL because social crawlers do not follow relative image URLs.
    pub image_path: String,
    pub canonical_path: String,
}

impl Social {
    /// The preview for pages that have nothing more specific to say.
    pub fn site(locale: Locale, canonical_path: &str) -> Self {
        Self {
            title: locale
                .text(
                    "LedgerFlow · Pons market intelligence",
                    "LedgerFlow · Pons 行情情报",
                )
                .to_string(),
            description: locale.text(
                "Live Pons launches, wallet and cluster profit, arbitrage and liquidations on Robinhood Chain, computed from our own node.",
                "Robinhood Chain 上的 Pons 发币、钱包与集群收益、套利与清算，全部由自有节点链上数据计算。",
            ).to_string(),
            image_path: "/og/home.png".to_string(),
            canonical_path: canonical_path.to_string(),
        }
    }
}

/// Renders the `<meta>` block. `base` is the public origin, e.g.
/// `https://ledgerflow.top`.
pub fn meta_tags(social: &Social, base: &str) -> String {
    let base = base.trim_end_matches('/');
    let image = format!("{base}{}", social.image_path);
    let url = format!("{base}{}", social.canonical_path);
    // Pages that do not own a canonical path (fragments, error states) declare
    // none rather than pointing every one of them at the same URL.
    let canonical = if social.canonical_path.is_empty() {
        String::new()
    } else {
        format!("\n<link rel=\"canonical\" href=\"{}\">", escape_html(&url))
    };
    format!(
        r#"<meta name="description" content="{description}">{canonical}
<meta property="og:site_name" content="LedgerFlow">
<meta property="og:type" content="website">
<meta property="og:title" content="{title}">
<meta property="og:description" content="{description}">
<meta property="og:url" content="{url}">
<meta property="og:image" content="{image}">
<meta property="og:image:width" content="{width}">
<meta property="og:image:height" content="{height}">
<meta name="twitter:card" content="summary_large_image">
<meta name="twitter:title" content="{title}">
<meta name="twitter:description" content="{description}">
<meta name="twitter:image" content="{image}">"#,
        title = escape_html(&social.title),
        description = escape_html(&social.description),
        canonical = canonical,
        url = escape_html(&url),
        image = escape_html(&image),
        width = og::WIDTH,
        height = og::HEIGHT,
    )
}

/// Share controls. The tweet text is composed server side so it carries the
/// numbers; the URL is taken from the address bar by the client script, which
/// keeps this correct behind any proxy or custom domain.
pub fn share_bar(context: &PageContext<'_>, tweet: &str) -> String {
    let l = context.locale;
    format!(
        r#"<div class="share"><button type="button" class="share-x" data-tweet="{tweet}">{share}</button><button type="button" class="share-copy" data-copy>{copy}</button></div>"#,
        tweet = escape_html(tweet),
        share = l.text("Share on X", "分享到 X"),
        copy = l.text("Copy link", "复制链接"),
    )
}

/// Preview for the landing page: the live totals are what makes the link worth
/// opening, so they go in the description.
pub fn home_social(locale: Locale, ticker: &MarketTicker) -> Social {
    let volume = ui::usd(Some(ticker.volume_24h_usd));
    let trades = ui::count(ticker.trades_24h);
    let launches = ui::count(ticker.launches_24h);
    Social {
        title: locale
            .text("Pons market, live · LedgerFlow", "Pons 实时行情 · LedgerFlow")
            .to_string(),
        description: locale.text_owned(
            format!(
                "{volume} traded across {trades} trades and {launches} new launches in the last 24h on Robinhood Chain."
            ),
            format!(
                "Robinhood Chain 过去 24 小时成交额 {volume}，成交 {trades} 笔，新发币 {launches} 个。"
            ),
        ),
        image_path: "/og/home.png".to_string(),
        canonical_path: "/".to_string(),
    }
}

pub fn token_social(token: &TokenRow, locale: Locale) -> Social {
    let symbol = symbol_of(token);
    let price = format!("{}{}", approx(token), ui::price_usd(token.price_usd));
    let change = percent(token.change_24h_pct);
    let volume = format!("{}{}", approx(token), ui::usd(token.volume_24h_usd));
    let mcap = format!("{}{}", approx(token), ui::usd(token.market_cap_usd));
    Social {
        title: format!("{symbol} {price} ({change} 24h) · Pons"),
        description: locale.text_owned(
            format!("{symbol} on Pons: {volume} traded in 24h, {mcap} market cap. Live price, candles and trades on LedgerFlow."),
            format!("{symbol} 在 Pons：24 小时成交 {volume}，市值 {mcap}。LedgerFlow 提供实时价格、K 线与成交明细。"),
        ),
        image_path: format!("/og/launch/{}/card.png", token.launch_id),
        canonical_path: format!("/launches/{}", token.launch_id),
    }
}

pub fn board_social(board: crate::services::market_read::TokenBoard, locale: Locale) -> Social {
    let (title, subline) = board_title(board);
    Social {
        title: locale.text_owned(
            format!("{title} · Pons on LedgerFlow"),
            format!("{title} · LedgerFlow Pons 行情"),
        ),
        description: subline.to_string(),
        image_path: format!("/og/board/{}/card.png", board.key()),
        canonical_path: "/launches".to_string(),
    }
}

pub fn address_social(address: &str, earnings: &AddressEarnings, locale: Locale) -> Social {
    let realized = ui::signed_usd(Some(earnings.realized_usd));
    let short = ui::short_address(address);
    let tokens = ui::count(earnings.tokens);
    Social {
        title: format!("{short} · {realized} realized on Pons"),
        description: locale.text_owned(
            format!("Realized profit {realized} across {tokens} Pons launches, computed from curve trades on Robinhood Chain."),
            format!("在 {tokens} 个 Pons 代币上的已实现收益 {realized}，由 Robinhood Chain 曲线成交计算。"),
        ),
        image_path: format!("/og/address/{address}/card.png"),
        canonical_path: format!("/address/{address}"),
    }
}

/// Percent without markup, for tweet text and card faces.
fn percent(value: Option<f64>) -> String {
    match value.filter(|value| value.is_finite()) {
        None => "—".to_string(),
        Some(value) => {
            let arrow = if value > 0.0005 {
                "+"
            } else if value < -0.0005 {
                "-"
            } else {
                ""
            };
            let magnitude = value.abs();
            let text = if magnitude >= 10_000.0 {
                format!("{}%", ui::compact(magnitude))
            } else if magnitude >= 100.0 {
                format!("{magnitude:.0}%")
            } else {
                format!("{magnitude:.1}%")
            };
            format!("{arrow}{text}")
        }
    }
}

fn symbol_of(token: &TokenRow) -> String {
    token
        .symbol
        .clone()
        .filter(|symbol| !symbol.trim().is_empty())
        .unwrap_or_else(|| "TOKEN".to_string())
}

/// Derived prices are flagged on the page, so the card and tweet flag them too
/// rather than presenting an estimate as a quote.
fn approx(token: &TokenRow) -> &'static str {
    match token.price_grade.as_deref() {
        Some("reliable") => "",
        _ => "≈",
    }
}

pub fn token_card(token: &TokenRow) -> og::Card {
    let symbol = symbol_of(token);
    let graduated = token.graduated_at.is_some() || token.lifecycle_phase == "pool_created";
    let mut stats = vec![
        og::Stat::new(
            "24H VOLUME",
            format!("{}{}", approx(token), ui::usd(token.volume_24h_usd)),
        ),
        og::Stat::new(
            "MARKET CAP",
            format!("{}{}", approx(token), ui::usd(token.market_cap_usd)),
        ),
        og::Stat::new(
            "24H TRADES",
            token
                .trades_24h
                .map(|trades| ui::count(trades as i64))
                .unwrap_or_else(|| "—".to_string()),
        ),
    ];
    stats.push(if graduated {
        og::Stat::toned("STATUS", "GRADUATED", og::Tone::Accent)
    } else {
        og::Stat::new(
            "BONDING",
            token
                .bonding_progress_pct
                .map(|pct| format!("{pct:.0}%"))
                .unwrap_or_else(|| "—".to_string()),
        )
    });
    og::Card {
        eyebrow: "PONS LAUNCH · ROBINHOOD CHAIN".to_string(),
        headline: symbol,
        subline: token
            .name
            .clone()
            .filter(|name| !name.trim().is_empty())
            .map(|name| match &token.quote_symbol {
                Some(quote) => format!("{name} · quoted in {quote}"),
                None => name,
            }),
        hero_label: Some("PRICE".to_string()),
        hero_value: Some(format!(
            "{}{}",
            approx(token),
            ui::price_usd(token.price_usd)
        )),
        hero_tone: og::Tone::Neutral,
        hero_delta: Some((
            percent(token.change_24h_pct),
            og::Tone::of(token.change_24h_pct),
        )),
        stats,
        rows: Vec::new(),
        note: Some(
            "Prices and volume computed from Pons curve trades and Uniswap V4 pool swaps"
                .to_string(),
        ),
        spark: token.sparkline_24h.clone().unwrap_or_default(),
        ..og::Card::default()
    }
}

pub fn token_tweet(token: &TokenRow, locale: Locale) -> String {
    let symbol = symbol_of(token);
    let change = percent(token.change_24h_pct);
    let volume = ui::usd(token.volume_24h_usd);
    locale.text_owned(
        format!("${symbol} is {change} over 24h on Pons · {volume} traded. Live on LedgerFlow:"),
        format!("${symbol} 24 小时 {change}，成交额 {volume}。LedgerFlow 实时行情："),
    )
}

pub fn home_card(ticker: &MarketTicker, gainers: &[TokenRow]) -> og::Card {
    let rows = gainers
        .iter()
        .take(5)
        .enumerate()
        .map(|(index, token)| og::Row {
            rank: index + 1,
            name: symbol_of(token),
            detail: format!(
                "{}{} 24h volume",
                approx(token),
                ui::usd(token.volume_24h_usd)
            ),
            value: percent(token.change_24h_pct),
            tone: og::Tone::of(token.change_24h_pct),
        })
        .collect();
    og::Card {
        eyebrow: "PONS · ROBINHOOD CHAIN".to_string(),
        headline: "Top gainers, 24h".to_string(),
        subline: Some(format!(
            "{} traded across {} trades in the last 24h",
            ui::usd(Some(ticker.volume_24h_usd)),
            ui::count(ticker.trades_24h)
        )),
        rows,
        ..og::Card::default()
    }
}

/// Card headline for each launches tab.
pub fn board_title(
    board: crate::services::market_read::TokenBoard,
) -> (&'static str, &'static str) {
    use crate::services::market_read::TokenBoard::*;
    match board {
        Hot => ("Hot right now", "Most traded Pons launches, last hour"),
        Gainers => ("Top gainers, 24h", "Biggest 24h moves with real volume"),
        Losers => ("Top losers, 24h", "Biggest 24h drawdowns with real volume"),
        Volume => ("Most traded, 24h", "Where the money is going"),
        New => ("Fresh launches", "Newest Pons tokens"),
        Graduating => (
            "About to graduate",
            "Closest to completing the bonding curve",
        ),
        Graduated => ("Graduated", "Now trading in Uniswap V4 pools"),
        MarketCap => ("Largest by market cap", "Pons launches by market cap"),
    }
}

pub fn board_card(title: &str, subline: &str, tokens: &[TokenRow]) -> og::Card {
    let rows = tokens
        .iter()
        .take(5)
        .enumerate()
        .map(|(index, token)| og::Row {
            rank: index + 1,
            name: symbol_of(token),
            detail: format!(
                "{}{} · {}",
                approx(token),
                ui::usd(token.market_cap_usd),
                percent(token.change_24h_pct)
            ),
            value: format!("{}{}", approx(token), ui::usd(token.volume_24h_usd)),
            tone: og::Tone::Neutral,
        })
        .collect();
    og::Card {
        eyebrow: "PONS · ROBINHOOD CHAIN".to_string(),
        headline: title.to_string(),
        subline: Some(subline.to_string()),
        rows,
        ..og::Card::default()
    }
}

/// Leaderboard card: the ranked wallets are the page's whole point, so the
/// card is the list itself.
pub fn wallets_card(window_label: &str, rows: &[WalletRankRow]) -> og::Card {
    let entries = rows
        .iter()
        .take(5)
        .map(|row| og::Row {
            rank: row.rank as usize,
            name: ui::short_address(&row.address),
            detail: format!(
                "{} tokens · {}% win rate",
                ui::count(row.tokens_traded as i64),
                if row.tokens_traded > 0 {
                    (row.winning_tokens as f64 / row.tokens_traded as f64 * 100.0).round() as i64
                } else {
                    0
                }
            ),
            value: ui::signed_usd(Some(row.realized_usd)),
            tone: og::Tone::of(Some(row.realized_usd)),
        })
        .collect();
    og::Card {
        eyebrow: "PONS TRADERS · ROBINHOOD CHAIN".to_string(),
        headline: format!("Who is making money, {window_label}"),
        subline: Some("Ranked by realized profit on Pons launches".to_string()),
        rows: entries,
        ..og::Card::default()
    }
}

pub fn wallets_social(window: &str, locale: Locale) -> Social {
    Social {
        title: locale.text_owned(
            format!("Top Pons traders, {window} · LedgerFlow"),
            format!("Pons 收益排行 {window} · LedgerFlow"),
        ),
        description: locale.text(
            "Wallets and address clusters ranked by realized profit on Pons launches, computed from every curve trade.",
            "按 Pons 已实现收益排序的钱包与地址集群，基于全部联合曲线成交计算。",
        ).to_string(),
        image_path: format!("/og/wallets/{window}/card.png"),
        canonical_path: "/clusters".to_string(),
    }
}

pub fn wallets_tweet(rows: &[WalletRankRow], locale: Locale) -> String {
    match rows.first() {
        Some(top) => locale.text_owned(
            format!(
                "Top Pons trader is up {} across {} tokens right now. Full ranking on LedgerFlow:",
                ui::signed_usd(Some(top.realized_usd)),
                ui::count(top.tokens_traded as i64)
            ),
            format!(
                "Pons 收益榜首已实现 {}，覆盖 {} 个代币。完整排行在 LedgerFlow：",
                ui::signed_usd(Some(top.realized_usd)),
                ui::count(top.tokens_traded as i64)
            ),
        ),
        None => locale
            .text(
                "Who is making money on Pons, ranked by realized profit:",
                "谁在 Pons 上赚钱，按已实现收益排行：",
            )
            .to_string(),
    }
}

pub fn board_tweet(
    board: crate::services::market_read::TokenBoard,
    tokens: &[TokenRow],
    locale: Locale,
) -> String {
    let (title, _) = board_title(board);
    match tokens.first() {
        Some(top) => locale.text_owned(
            format!(
                "{title} on Pons: {} leads at {}. Live board on LedgerFlow:",
                symbol_of(top),
                percent(top.change_24h_pct)
            ),
            format!(
                "Pons {title}：{} 领涨 {}。实时榜单在 LedgerFlow：",
                symbol_of(top),
                percent(top.change_24h_pct)
            ),
        ),
        None => format!("{title} on Pons, live on LedgerFlow:"),
    }
}

/// The card for a single settled win. This is the artifact people paste.
pub fn win_card(win: &crate::services::market_read::WinRow) -> og::Card {
    let symbol = win
        .symbol
        .clone()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "?".to_owned());
    let multiple = if win.multiple >= 10.0 {
        format!("{:.1}x", win.multiple)
    } else {
        format!("{:.2}x", win.multiple)
    };
    og::Card {
        eyebrow: "PONS · ROBINHOOD CHAIN".to_string(),
        headline: format!(
            "{} → {}",
            ui::usd(Some(win.cost_usd)),
            ui::usd(Some(win.proceeds_usd))
        ),
        subline: Some(format!("{} on ${symbol}", ui::short_address(&win.address))),
        hero_label: Some("MULTIPLE".to_string()),
        hero_value: Some(multiple),
        hero_tone: og::Tone::Up,
        hero_delta: None,
        stats: vec![
            og::Stat::toned("PROFIT", ui::signed_usd(Some(win.profit_usd)), og::Tone::Up),
            og::Stat::new("PUT IN", ui::usd(Some(win.cost_usd))),
            og::Stat::new("TOOK OUT", ui::usd(Some(win.proceeds_usd))),
            og::Stat::new(
                "TRADES",
                format!("{}B / {}S", win.buy_count, win.sell_count),
            ),
        ],
        note: Some(
            "Settled on chain · average cost, excluding gas · past results are not a forecast"
                .to_string(),
        ),
        ..og::Card::default()
    }
}

pub fn win_social(win: &crate::services::market_read::WinRow, locale: Locale) -> Social {
    let symbol = win
        .symbol
        .clone()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "?".to_owned());
    Social {
        title: format!(
            "{} → {} on ${symbol}",
            ui::usd(Some(win.cost_usd)),
            ui::usd(Some(win.proceeds_usd))
        ),
        description: locale.text_owned(
            format!(
                "{} turned {} into {} on a Pons launch. Settled on chain, computed from every curve trade.",
                ui::short_address(&win.address),
                ui::usd(Some(win.cost_usd)),
                ui::usd(Some(win.proceeds_usd))
            ),
            format!(
                "{} 在一个 Pons 发币上把 {} 变成了 {}。链上已结算，由全部曲线成交计算。",
                ui::short_address(&win.address),
                ui::usd(Some(win.cost_usd)),
                ui::usd(Some(win.proceeds_usd))
            ),
        ),
        image_path: format!("/og/win/{}/{}/card.png", win.launch_id, win.address),
        canonical_path: format!("/wins/{}/{}", win.launch_id, win.address),
    }
}

pub fn win_tweet(win: &crate::services::market_read::WinRow, locale: Locale) -> String {
    let symbol = win
        .symbol
        .clone()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "?".to_owned());
    let multiple = format!("{:.1}x", win.multiple);
    locale.text_owned(
        format!(
            "{} → {} ({multiple}) on ${symbol}. Every Pons win, as it settles:",
            ui::usd(Some(win.cost_usd)),
            ui::usd(Some(win.proceeds_usd))
        ),
        format!(
            "${symbol} 上 {} 变 {}，{multiple}。Pons 上每一笔落袋战绩：",
            ui::usd(Some(win.cost_usd)),
            ui::usd(Some(win.proceeds_usd))
        ),
    )
}

pub fn wins_social(locale: Locale, summary: &crate::services::market_read::WinSummary) -> Social {
    Social {
        title: locale
            .text("Who just got paid on Pons", "谁刚刚在 Pons 上赚到了")
            .to_string(),
        description: locale.text_owned(
            format!(
                "{} closed wins in 24h, {} of them above 10x, biggest single win {}.",
                ui::count(summary.wins),
                ui::count(summary.wins_10x),
                ui::usd(summary.biggest_profit_usd)
            ),
            format!(
                "24 小时内 {} 笔落袋战绩，其中 {} 笔超过 10 倍，最大一笔 {}。",
                ui::count(summary.wins),
                ui::count(summary.wins_10x),
                ui::usd(summary.biggest_profit_usd)
            ),
        ),
        image_path: "/og/wins/card.png".to_string(),
        canonical_path: "/wins".to_string(),
    }
}

/// The scoreboard card: the day's biggest multiples.
pub fn wins_card(wins: &[crate::services::market_read::WinRow]) -> og::Card {
    let rows = wins
        .iter()
        .take(5)
        .enumerate()
        .map(|(index, win)| og::Row {
            rank: index + 1,
            name: format!(
                "{} → {}",
                ui::usd(Some(win.cost_usd)),
                ui::usd(Some(win.proceeds_usd))
            ),
            detail: format!(
                "${} · {}",
                win.symbol.clone().unwrap_or_else(|| "?".to_owned()),
                ui::short_address(&win.address)
            ),
            value: if win.multiple >= 10.0 {
                format!("{:.1}x", win.multiple)
            } else {
                format!("{:.2}x", win.multiple)
            },
            tone: og::Tone::Up,
        })
        .collect();
    og::Card {
        eyebrow: "PONS · ROBINHOOD CHAIN".to_string(),
        headline: "Who just got paid".to_string(),
        subline: Some("Settled positions on Pons launches, last 24h".to_string()),
        rows,
        ..og::Card::default()
    }
}

/// Win rate over the tokens a wallet actually closed out.
fn win_rate(earnings: &AddressEarnings) -> Option<f64> {
    (earnings.tokens > 0).then(|| earnings.winning_tokens as f64 / earnings.tokens as f64 * 100.0)
}

pub fn address_card(
    address: &str,
    earnings: &AddressEarnings,
    best: Option<&WalletRankRow>,
) -> og::Card {
    let mut stats = vec![
        og::Stat::new("TOKENS TRADED", ui::count(earnings.tokens)),
        og::Stat::new(
            "WIN RATE",
            win_rate(earnings)
                .map(|rate| format!("{rate:.0}%"))
                .unwrap_or_else(|| "—".to_string()),
        ),
    ];
    if let Some(row) = best {
        if let Some(multiple) = row.best_multiple {
            stats.push(og::Stat::toned(
                "BEST TRADE",
                format!("{multiple:.1}x"),
                og::Tone::Up,
            ));
        }
        stats.push(og::Stat::toned(
            "RANK 24H",
            format!("#{}", row.rank),
            og::Tone::Accent,
        ));
    } else {
        stats.push(og::Stat::new(
            "COST BASIS",
            ui::usd(Some(earnings.cost_usd)),
        ));
    }
    og::Card {
        eyebrow: "WALLET · PONS LAUNCHES".to_string(),
        headline: ui::short_address(address),
        subline: Some("Realized profit from Pons curve trades".to_string()),
        hero_label: Some("REALIZED PNL".to_string()),
        hero_value: Some(ui::signed_usd(Some(earnings.realized_usd))),
        hero_tone: og::Tone::of(Some(earnings.realized_usd)),
        hero_delta: None,
        note: Some(
            "Average-cost realized profit · excludes gas · read from our own Robinhood Chain node"
                .to_string(),
        ),
        stats,
        ..og::Card::default()
    }
}

pub fn address_tweet(earnings: &AddressEarnings, locale: Locale) -> String {
    let realized = ui::signed_usd(Some(earnings.realized_usd));
    let tokens = ui::count(earnings.tokens);
    let rate = win_rate(earnings)
        .map(|rate| format!("{rate:.0}%"))
        .unwrap_or_else(|| "—".to_string());
    locale.text_owned(
        format!("My Pons realized PnL: {realized} across {tokens} tokens, {rate} win rate. Check any wallet on LedgerFlow:"),
        format!("我在 Pons 的已实现收益：{realized}，共 {tokens} 个代币，胜率 {rate}。在 LedgerFlow 查任意地址："),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percent_keeps_sign_and_compacts_extremes() {
        assert_eq!(percent(Some(7130.4)), "+7130%");
        assert_eq!(percent(Some(-12.34)), "-12.3%");
        assert_eq!(percent(Some(18_200_000_000.0)), "+18.2B%");
        assert_eq!(percent(None), "—");
    }

    #[test]
    fn meta_tags_resolve_absolute_urls() {
        let social = Social {
            title: "MUSEPAD".to_string(),
            description: "up".to_string(),
            image_path: "/og/launch/abc.png".to_string(),
            canonical_path: "/launches/abc".to_string(),
        };
        let tags = meta_tags(&social, "https://ledgerflow.top/");
        assert!(tags.contains(r#"content="https://ledgerflow.top/og/launch/abc.png""#));
        assert!(tags.contains(r#"href="https://ledgerflow.top/launches/abc""#));
        assert!(tags.contains(r#"name="twitter:card" content="summary_large_image""#));
    }

    #[test]
    fn tweet_text_is_escaped_into_the_button_attribute() {
        let query = super::super::PageQuery::default();
        let context = PageContext::new(&query);
        let bar = share_bar(&context, r#"a "quoted" <tag>"#);
        assert!(bar.contains("&quot;quoted&quot;"));
        assert!(bar.contains("&lt;tag&gt;"));
    }
}
