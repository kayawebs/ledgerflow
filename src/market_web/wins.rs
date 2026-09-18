//! The scoreboard.
//!
//! Nothing on this site pulls harder than a stranger who just turned two
//! hundred dollars into nine thousand, so the wins are reported as they close:
//! what went in, what came out, on which token, and whose address it was. These
//! are settled positions, not predictions — the page promises nothing about
//! whether the next one works.

use crate::services::market_read::{WinBoard, WinRow, WinSummary};

use super::{PageContext, escape_html, intel::address_url, share, ui};

pub struct WinsPage<'a> {
    pub board: WinBoard,
    pub hours: i32,
    pub summary: &'a WinSummary,
    pub wins: &'a [WinRow],
    pub feed: &'a str,
}

fn board_label(context: &PageContext<'_>, board: WinBoard) -> &'static str {
    let l = context.locale;
    match board {
        WinBoard::Multiple => l.text("Biggest multiples", "倍数榜"),
        WinBoard::Profit => l.text("Biggest profits", "金额榜"),
        WinBoard::Latest => l.text("Just closed", "最新落袋"),
    }
}

fn multiple(value: f64) -> String {
    if value >= 100.0 {
        format!("{value:.0}x")
    } else if value >= 10.0 {
        format!("{value:.1}x")
    } else {
        format!("{value:.2}x")
    }
}

fn symbol_of(win: &WinRow) -> String {
    win.symbol
        .clone()
        .filter(|symbol| !symbol.trim().is_empty())
        .unwrap_or_else(|| "?".to_owned())
}

/// One line of the live feed.
fn feed_row(context: &PageContext<'_>, win: &WinRow) -> String {
    let l = context.locale;
    format!(
        "<a class=\"row win\" href=\"{href}\"><span class=\"avatar\" style=\"--h:140\">🚀</span>\
         <div class=\"name\"><strong>{cost} → <span class=\"up\">{proceeds}</span> <span class=\"mult\">{multiple}</span></strong>\
         <small>${symbol} · <span class=\"mono\">{address}</span></small></div>\
         <div class=\"val\"><strong class=\"up\">{profit}</strong><small>{age}</small></div></a>",
        href = context.url(&format!("/wins/{}/{}", win.launch_id, win.address)),
        cost = ui::usd(Some(win.cost_usd)),
        proceeds = ui::usd(Some(win.proceeds_usd)),
        multiple = multiple(win.multiple),
        symbol = escape_html(&symbol_of(win)),
        address = escape_html(&ui::short_address(&win.address)),
        profit = ui::signed_usd(Some(win.profit_usd)),
        age = ui::age(win.closed_at, l),
    )
}

/// The feed body, refreshed in place on both the landing page and this one.
pub fn feed(context: &PageContext<'_>, wins: &[WinRow]) -> String {
    let l = context.locale;
    if wins.is_empty() {
        return format!(
            "<div class=\"empty\">{}</div>",
            l.text("No wins closed yet", "暂无已落袋的战绩")
        );
    }
    wins.iter().map(|win| feed_row(context, win)).collect()
}

fn table(context: &PageContext<'_>, wins: &[WinRow]) -> String {
    let l = context.locale;
    if wins.is_empty() {
        return format!(
            "<div class=\"empty\">{}</div>",
            l.text("No wins in this window", "该时间窗内暂无战绩")
        );
    }
    let rows = wins
        .iter()
        .enumerate()
        .map(|(index, win)| {
            format!(
                "<tr data-href=\"{href}\"><td class=\"l\">{rank}</td>\
                 <td class=\"l\"><div class=\"tok\">{avatar}<div><strong>${symbol}</strong><small class=\"mono\">{address}</small></div></div></td>\
                 <td>{cost}</td><td class=\"up\"><strong>{proceeds}</strong></td>\
                 <td><span class=\"mult\">{multiple}</span></td><td class=\"up\"><strong>{profit}</strong></td>\
                 <td>{trades}</td><td class=\"muted\">{held}</td><td class=\"muted\">{age}</td></tr>",
                href = context.url(&format!("/wins/{}/{}", win.launch_id, win.address)),
                rank = ui::rank(index + 1),
                avatar = ui::token_avatar(win.symbol.as_deref(), &win.token_address),
                symbol = escape_html(&symbol_of(win)),
                address = escape_html(&ui::short_address(&win.address)),
                cost = ui::usd(Some(win.cost_usd)),
                proceeds = ui::usd(Some(win.proceeds_usd)),
                multiple = multiple(win.multiple),
                profit = ui::signed_usd(Some(win.profit_usd)),
                trades = format!("{}B / {}S", win.buy_count, win.sell_count),
                held = held_for(context, win),
                age = ui::age(win.closed_at, l),
            )
        })
        .collect::<String>();
    format!(
        "<div class=\"table-wrap\"><table class=\"market\"><thead><tr><th class=\"l\">#</th><th class=\"l\">{}</th>\
         <th>{}</th><th>{}</th><th>{}</th><th>{}</th><th>{}</th><th>{}</th><th>{}</th></tr></thead><tbody>{rows}</tbody></table></div>",
        l.text("Token / wallet", "代币 / 钱包"),
        l.text("Put in", "投入"),
        l.text("Took out", "拿回"),
        l.text("Multiple", "倍数"),
        l.text("Profit", "利润"),
        l.text("Trades", "成交"),
        l.text("Held", "持有"),
        l.text("Closed", "落袋"),
    )
}

/// How long the position was open, in words.
fn held_for(context: &PageContext<'_>, win: &WinRow) -> String {
    let l = context.locale;
    match (win.opened_at, win.closed_at) {
        (Some(open), Some(close)) => {
            let seconds = (close - open).num_seconds().max(0);
            if seconds < 90 {
                l.text_owned(format!("{seconds}s"), format!("{seconds} 秒"))
            } else if seconds < 5_400 {
                l.text_owned(
                    format!("{}m", seconds / 60),
                    format!("{} 分钟", seconds / 60),
                )
            } else if seconds < 172_800 {
                l.text_owned(
                    format!("{}h", seconds / 3_600),
                    format!("{} 小时", seconds / 3_600),
                )
            } else {
                l.text_owned(
                    format!("{}d", seconds / 86_400),
                    format!("{} 天", seconds / 86_400),
                )
            }
        }
        _ => "—".to_owned(),
    }
}

pub fn page(context: &PageContext<'_>, page: &WinsPage<'_>) -> String {
    let l = context.locale;
    let summary = page.summary;
    let window_label = if page.hours >= 168 {
        l.text("7d", "7 天")
    } else {
        l.text("24h", "24 小时")
    };
    let stat = |label: String, value: String, note: String, glow: bool| {
        format!(
            "<div class=\"stat{}\"><span>{label}</span><strong class=\"num\">{value}</strong><small>{note}</small></div>",
            if glow { " glow" } else { "" }
        )
    };
    let stats = [
        stat(
            l.text_owned(
                format!("Biggest win · {window_label}"),
                format!("最大一笔 · {window_label}"),
            ),
            format!(
                "<span class=\"up\">{}</span>",
                ui::usd(summary.biggest_profit_usd)
            ),
            l.text("from a single position", "单个仓位").to_owned(),
            true,
        ),
        stat(
            l.text_owned(
                format!("Wins · {window_label}"),
                format!("落袋战绩 · {window_label}"),
            ),
            ui::count(summary.wins),
            l.text_owned(
                format!("{} wallets", ui::count(summary.winners)),
                format!("{} 个钱包", ui::count(summary.winners)),
            ),
            false,
        ),
        stat(
            l.text("10x and above", "10 倍以上").to_owned(),
            ui::count(summary.wins_10x),
            l.text_owned(
                format!("{} above 20x", ui::count(summary.wins_20x)),
                format!("其中 {} 笔超过 20 倍", ui::count(summary.wins_20x)),
            ),
            false,
        ),
        stat(
            l.text("Taken off the table", "合计落袋").to_owned(),
            format!(
                "<span class=\"up\">{}</span>",
                ui::usd(Some(summary.total_profit_usd))
            ),
            l.text("realized, not on paper", "已实现，非浮盈")
                .to_owned(),
            false,
        ),
    ]
    .concat();

    let tabs = WinBoard::ALL
        .into_iter()
        .map(|option| {
            format!(
                "<a class=\"{}\" href=\"{}&board={}&hours={}\">{}</a>",
                if option == page.board { "active" } else { "" },
                context.url("/wins"),
                option.key(),
                page.hours,
                board_label(context, option),
            )
        })
        .collect::<String>();
    let windows = [(24, l.text("24h", "24 小时")), (168, l.text("7d", "7 天"))]
        .into_iter()
        .map(|(hours, label)| {
            format!(
                "<a class=\"{}\" href=\"{}&board={}&hours={hours}\">{label}</a>",
                if hours == page.hours { "active" } else { "" },
                context.url("/wins"),
                page.board.key(),
            )
        })
        .collect::<String>();

    format!(
        "<section class=\"page-head\"><div><h1>{title}</h1><p>{subtitle}</p></div><nav class=\"windows\">{windows}</nav></section>\
         <section class=\"stats\">{stats}</section>\
         <div class=\"toolbar\"><nav class=\"tabs\">{tabs}</nav></div>\
         <section class=\"grid2\">\
           <article class=\"card\"><div class=\"card-head\"><h2>{board_title}</h2><span class=\"muted\">{window_label}</span></div>{table}</article>\
           <article class=\"card\"><div class=\"card-head\"><h2>⚡ {feed_title}</h2><span class=\"muted\">{feed_note}</span></div>\
             <div class=\"card-body feed\" data-refresh=\"{feed_url}\" data-every=\"10\">{feed}</div></article>\
         </section>\
         <p class=\"note\">{method}</p>",
        title = l.text("Who just got paid", "谁刚刚赚到了"),
        subtitle = l.text(
            "Closed positions on Pons launches: what went in, what came out. Settled on chain, not on paper.",
            "Pons 发币上已经落袋的仓位：投了多少，拿回多少。链上已结算，不是浮盈。"
        ),
        board_title = board_label(context, page.board),
        table = table(context, page.wins),
        feed_title = l.text("Just closed", "刚刚落袋"),
        feed_note = l.text("live", "实时"),
        feed_url = super::pages::fragment_url(context, "/fragments/wins", ""),
        feed = page.feed,
        method = l.text(
            "A win is a position that was fully sold for at least twice what it cost, with at least $20 put in, by average cost and excluding gas. Curve trades only. Past results say nothing about what the next trade does.",
            "战绩的口径：完全卖出、拿回至少两倍成本、投入不低于 $20 的仓位，平均成本法，不含 gas，只统计曲线成交。历史战绩不代表下一笔会怎样。"
        ),
    )
}

/// A single win, which is the thing people share.
pub fn detail(context: &PageContext<'_>, win: &WinRow) -> String {
    let l = context.locale;
    let kv = |label: &str, value: String| {
        format!("<div><span>{label}</span><strong class=\"num\">{value}</strong></div>")
    };
    format!(
        "<a class=\"note\" href=\"{back}\">← {back_label}</a>\
         <section class=\"token-hero\"><div class=\"id\">{avatar}<div><h1>${symbol}</h1>\
           <div class=\"muted\"><a class=\"mono\" href=\"{wallet_href}\">{address}</a></div></div></div>\
           <div class=\"price\"><strong class=\"up\">{cost} → {proceeds}</strong>\
             <div><span class=\"mult big\">{multiple}</span></div>{share}</div></section>\
         <article class=\"card\" style=\"margin-bottom:12px\"><div class=\"kv\">{stats}</div></article>\
         <p class=\"note\">{method}</p>",
        back = context.url("/wins"),
        back_label = l.text("All wins", "全部战绩"),
        avatar = ui::token_avatar(win.symbol.as_deref(), &win.token_address),
        symbol = escape_html(&symbol_of(win)),
        wallet_href = address_url(context, &win.address),
        address = escape_html(&win.address),
        cost = ui::usd(Some(win.cost_usd)),
        proceeds = ui::usd(Some(win.proceeds_usd)),
        multiple = multiple(win.multiple),
        share = share::share_bar(context, &share::win_tweet(win, l)),
        stats = [
            kv(l.text("Profit", "利润"), format!("<span class=\"up\">{}</span>", ui::signed_usd(Some(win.profit_usd)))),
            kv(l.text("Put in", "投入"), ui::usd(Some(win.cost_usd))),
            kv(l.text("Took out", "拿回"), ui::usd(Some(win.proceeds_usd))),
            kv(l.text("Held for", "持有时长"), held_for(context, win)),
            kv(l.text("Trades", "成交笔数"), format!("{}B / {}S", win.buy_count, win.sell_count)),
            kv(l.text("Closed", "落袋时间"), ui::age(win.closed_at, l)),
        ].concat(),
        method = l.text(
            "Average cost basis, excluding gas, curve trades only. This is one settled position, not a recommendation.",
            "平均成本法，不含 gas，只统计曲线成交。这是一个已结算的仓位，不构成任何建议。"
        ),
    )
}
