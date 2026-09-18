//! Arbitrage and liquidation earnings pages.

use super::{PageContext, escape_html, explorer_tx_url, intel::address_url, ui};
use crate::services::{
    market_pnl::EarningsWindow,
    market_read::{
        ArbitrageOverview, ArbitrageRow, BotRankRow, LiquidationOverview, LiquidationRow,
        LiquidatorRankRow,
    },
};

pub struct ArbitragePage<'a> {
    pub window: EarningsWindow,
    pub overview: &'a ArbitrageOverview,
    pub bots: &'a [BotRankRow],
    pub biggest: &'a [ArbitrageRow],
    pub feed: &'a str,
    pub verified_section: &'a str,
}

pub fn arbitrage(context: &PageContext<'_>, page: &ArbitragePage<'_>) -> String {
    let l = context.locale;
    let overview = page.overview;
    let windows = window_tabs(context, "/arbitrage", page.window);
    let stat = |label: &str, value: String, note: String, glow: bool| {
        format!(
            "<div class=\"stat{}\"><span>{label}</span><strong class=\"num\">{value}</strong><small>{note}</small></div>",
            if glow { " glow" } else { "" }
        )
    };
    let stats = [
        // Executions lead: the aggregate net is often negative because most
        // attempts lose their gas, which is a real finding but a poor headline.
        stat(
            l.text("Arbitrage executions", "套利执行"),
            ui::count(overview.executions),
            format!(
                "{} {}",
                percent(overview.profitable, overview.estimated),
                l.text("profitable", "盈利")
            ),
            true,
        ),
        stat(
            &format!(
                "{} · {}",
                l.text("Estimated net profit", "估算净利润"),
                page.window.key()
            ),
            format!(
                "<span class=\"{}\">{}</span>",
                if overview.net_usd >= 0.0 {
                    "up"
                } else {
                    "down"
                },
                ui::signed_usd(Some(overview.net_usd))
            ),
            format!(
                "{} {} {}",
                l.text("gross", "毛利"),
                ui::usd(Some(overview.gross_usd)),
                ui::trust(l, ui::Trust::Estimated)
            ),
            false,
        ),
        stat(
            l.text("Active bots", "活跃机器人"),
            ui::count(overview.bots),
            // Bots whose every execution is unpriced cannot be ranked, so the
            // gap between the two counts is stated rather than shown as $0.
            match overview.bots - overview.priced_bots {
                0 => format!(
                    "{} {}",
                    l.text("latest", "最新"),
                    ui::age(overview.latest_at, l)
                ),
                unranked => format!(
                    "{} {} {}",
                    ui::count(overview.priced_bots),
                    l.text("ranked ·", "个可排名 ·"),
                    l.text_owned(
                        format!("{} profit unknown", ui::count(unranked)),
                        format!("{} 个收益未知", ui::count(unranked)),
                    )
                ),
            },
            false,
        ),
        stat(
            l.text("Gas spent", "Gas 消耗"),
            ui::usd(Some(overview.gas_usd)),
            l.text("exact receipt gas", "receipt 精确 gas").to_owned(),
            false,
        ),
        stat(
            l.text("Deterministically verified", "确定性验证"),
            ui::count(overview.verified),
            ui::trust(l, ui::Trust::Verified),
            false,
        ),
    ]
    .concat();
    format!(
        "<section class=\"page-head\"><div><h1>{title}</h1><p>{subtitle}</p></div><nav class=\"windows\">{windows}</nav></section>\
         <section class=\"stats\">{stats}</section>\
         <section class=\"podium\">{podium}</section>\
         <section class=\"grid2\"><article class=\"card\"><div class=\"card-head\"><h2>🤖 {bots_title}</h2><span class=\"muted\">{trust}</span></div>{bots}</article>\
           <article class=\"card\"><div class=\"card-head\"><h2>💥 {biggest_title}</h2><span class=\"muted\">{window}</span></div><div class=\"card-body\">{biggest}</div></article></section>\
         <article class=\"card\" style=\"margin-bottom:16px\"><div class=\"card-head\"><h2>⚡ {feed_title}</h2><span class=\"muted\">{feed_note}</span></div><div class=\"card-body feed\" data-refresh=\"{feed_url}\" data-every=\"10\">{feed}</div></article>\
         <div class=\"legacy\">{verified}</div>\
         <p class=\"note\">{method}</p>",
        title = l.text("Arbitrage bots", "套利机器人"),
        subtitle = l.text(
            "Every detected multi-pool arbitrage on Robinhood Chain, valued from its receipt.",
            "Robinhood Chain 上检测到的每一笔多池套利，按交易回执计算收益。"
        ),
        podium = bot_podium(context, page.bots),
        bots_title = l.text("Bot earnings ranking", "机器人收益榜"),
        trust = ui::trust(l, ui::Trust::Estimated),
        bots = bot_table(context, page.bots),
        biggest_title = l.text("Biggest single arbitrages", "单笔最大套利"),
        window = page.window.key(),
        biggest = arbitrage_list(context, page.biggest),
        feed_title = l.text("Live profitable arbitrage", "实时盈利套利"),
        feed_note = l.text("≥ $1 net · last 6h", "净利 ≥ $1 · 最近 6 小时"),
        feed_url = super::pages::fragment_url(context, "/fragments/arbitrage/feed", ""),
        feed = page.feed,
        verified = page.verified_section,
        method = l.text(
            "Estimated profit nets ERC-20 transfers into and out of the transaction sender and its entry contract, values each token at the hourly USD price and subtracts exact gas. Totals and rankings count only executions whose every token is priced from pool state or a stablecoin anchor; ≈ marks a result that depends on swap-median prices. Profits settled to other addresses and native transfers are not counted, and deterministically verified executions supersede the estimate.",
            "估算利润 = 交易发送者及其入口合约的 ERC-20 净流入按小时美元价格估值，再减去精确 gas。总额与排行只统计所有相关代币都有可靠价格（池子状态或稳定币锚定）的执行；≈ 表示该结果依赖成交中位数推导的价格。结算到其他地址的利润及原生币转账不计入；有确定性验证结果时以验证值为准。"
        ),
    )
}

fn bot_podium(context: &PageContext<'_>, bots: &[BotRankRow]) -> String {
    let l = context.locale;
    if bots.is_empty() {
        return format!(
            "<article class=\"card\"><div class=\"empty\">{}</div></article>",
            l.text("Rankings are being computed", "排行榜计算中")
        );
    }
    bots.iter()
        .take(3)
        .map(|bot| {
            format!(
                "<a class=\"card{first}\" href=\"{href}\"><div class=\"card-head\" style=\"padding:0;border:0\">{rank}<span class=\"muted mono\">{address}</span>{verified}</div>\
                 <div class=\"pnl {class} num\">{net}</div><div class=\"muted\">{executions} {executions_label} · {win_label} {win}</div>\
                 <div class=\"note\">{best_label} <span class=\"up\">{best}</span> · {senders} {senders_label}</div></a>",
                first = if bot.rank == 1 { " first" } else { "" },
                href = address_url(context, &bot.bot_address),
                rank = ui::rank(bot.rank as usize),
                address = ui::short_address(&bot.bot_address),
                verified = if bot.verified_executions > 0 {
                    format!(" {}", ui::trust(l, ui::Trust::Verified))
                } else {
                    String::new()
                },
                class = if bot.net_usd >= 0.0 { "up" } else { "down" },
                net = ui::signed_usd(Some(bot.net_usd)),
                executions = ui::count(bot.executions.into()),
                executions_label = l.text("executions", "次执行"),
                win_label = l.text("win", "胜率"),
                win = percent(bot.profitable_executions.into(), (bot.executions - bot.unknown_executions).into()),
                best_label = l.text("Best", "最佳"),
                best = ui::signed_usd(bot.best_net_usd),
                senders = bot.senders,
                senders_label = l.text("senders", "个发送地址"),
            )
        })
        .collect()
}

fn bot_table(context: &PageContext<'_>, bots: &[BotRankRow]) -> String {
    let l = context.locale;
    if bots.is_empty() {
        return format!(
            "<div class=\"empty\">{}</div>",
            l.text("No bots ranked yet", "暂无排名")
        );
    }
    let rows = bots
        .iter()
        .take(50)
        .map(|bot| {
            format!(
                "<tr data-href=\"{href}\"><td class=\"l\">{rank}</td><td class=\"l mono\">{address}{kind}</td><td class=\"{class}\"><strong>{net}</strong></td><td>{gross}</td><td>{gas}</td><td>{executions}</td><td>{win}</td><td>{senders}</td><td class=\"muted\">{age}</td></tr>",
                href = address_url(context, &bot.bot_address),
                rank = ui::rank(bot.rank as usize),
                address = ui::short_address(&bot.bot_address),
                kind = if bot.address_kind.as_deref() == Some("contract") { " <span class=\"badge\">🤖</span>" } else { "" },
                class = if bot.net_usd >= 0.0 { "up" } else { "down" },
                net = ui::signed_usd(Some(bot.net_usd)),
                gross = ui::usd(Some(bot.gross_usd)),
                gas = ui::usd(Some(bot.gas_usd)),
                executions = ui::count(bot.executions.into()),
                win = percent(bot.profitable_executions.into(), (bot.executions - bot.unknown_executions).into()),
                senders = bot.senders,
                age = ui::age(bot.last_execution_at, l),
            )
        })
        .collect::<String>();
    format!(
        "<div class=\"table-wrap\"><table class=\"market\"><thead><tr><th class=\"l\">#</th><th class=\"l\">{}</th><th>{}</th><th>{}</th><th>Gas</th><th>{}</th><th>{}</th><th>{}</th><th>{}</th></tr></thead><tbody>{rows}</tbody></table></div>",
        l.text("Bot contract", "机器人合约"),
        l.text("Net profit", "净利润"),
        l.text("Gross", "毛利"),
        l.text("Executions", "执行次数"),
        l.text("Win rate", "胜率"),
        l.text("Senders", "发送地址"),
        l.text("Active", "最近活跃"),
    )
}

pub fn arbitrage_list(context: &PageContext<'_>, rows: &[ArbitrageRow]) -> String {
    let l = context.locale;
    if rows.is_empty() {
        return format!(
            "<div class=\"empty\">{}</div>",
            l.text("No profitable arbitrage yet", "暂无盈利套利")
        );
    }
    rows.iter()
        .map(|row| {
            format!(
                "<a class=\"row\" href=\"{tx}\" target=\"_blank\" rel=\"noopener\"><span class=\"avatar\" style=\"--h:{hue}\">⚡</span><div class=\"name\"><strong>{tokens}{verified}</strong><small>{bot} · {pools} {pools_label} · {age}</small></div><div class=\"val\"><strong class=\"{class}\">{net}</strong><small>gas {gas}</small></div></a>",
                tx = explorer_tx_url(context.chain_id, &row.tx_hash),
                hue = row.bot_address.bytes().last().unwrap_or(0) as u32 * 360 / 256,
                tokens = escape_html(row.tokens.as_deref().unwrap_or("—")),
                verified = if row.is_verified { format!(" {}", ui::trust(l, ui::Trust::Verified)) } else { String::new() },
                bot = ui::short_address(&row.bot_address),
                pools = row.pool_count,
                pools_label = l.text("pools", "池"),
                age = ui::age(row.block_time, l),
                class = match row.net_usd { Some(value) if value < 0.0 => "down", _ => "up" },
                net = if row.status == "unknown" {
                    ui::trust(l, ui::Trust::Unknown)
                } else if row.reliable_pricing {
                    ui::signed_usd(row.net_usd)
                } else {
                    // Cross-token result priced from swap medians.
                    format!("≈{}", ui::signed_usd(row.net_usd))
                },
                gas = ui::usd(row.gas_usd),
            )
        })
        .collect()
}

pub struct LiquidationPage<'a> {
    pub overview: &'a LiquidationOverview,
    pub head_block: Option<u64>,
    pub liquidators: &'a [LiquidatorRankRow],
    pub biggest: &'a [LiquidationRow],
    pub recent: &'a [LiquidationRow],
}

pub fn liquidations(context: &PageContext<'_>, page: &LiquidationPage<'_>) -> String {
    let l = context.locale;
    let overview = page.overview;
    let scanning = match (overview.scanned_to_block, page.head_block) {
        (Some(scanned), Some(head)) if (scanned as u64) + 10_000 < head => format!(
            "<p class=\"note\">⏳ {} {}%</p>",
            l.text(
                "Historical liquidation scan in progress:",
                "历史清算扫描进行中："
            ),
            (scanned as f64 / head as f64 * 100.0).round()
        ),
        _ => String::new(),
    };
    let stat = |label: &str, value: String, note: String, glow: bool| {
        format!(
            "<div class=\"stat{}\"><span>{label}</span><strong class=\"num\">{value}</strong><small>{note}</small></div>",
            if glow { " glow" } else { "" }
        )
    };
    let stats = [
        // Debt repaid leads because every liquidation carries it; the reward
        // is only known for the subset that could be priced, so it follows
        // with its coverage stated.
        stat(
            l.text("Debt repaid · 30d", "偿还债务 · 30d"),
            ui::usd(Some(overview.repaid_usd)),
            format!(
                "{} {}",
                l.text("collateral seized", "没收抵押"),
                ui::usd(Some(overview.seized_usd))
            ),
            true,
        ),
        stat(
            l.text("Liquidations · 30d", "清算次数 · 30d"),
            ui::count(overview.liquidations),
            format!(
                "{} {}",
                ui::count(overview.borrowers),
                l.text("borrowers liquidated", "个借款人被清算")
            ),
            false,
        ),
        stat(
            l.text("Liquidator bonus", "清算奖励"),
            format!(
                "<span class=\"up\">{}</span>",
                ui::signed_usd(Some(overview.bonus_usd))
            ),
            l.text_owned(
                format!(
                    "priced for {} of {} {}",
                    ui::count(overview.priced_bonuses),
                    ui::count(overview.liquidations),
                    ui::trust(l, ui::Trust::Estimated)
                ),
                format!(
                    "{} / {} 笔可定价 {}",
                    ui::count(overview.priced_bonuses),
                    ui::count(overview.liquidations),
                    ui::trust(l, ui::Trust::Estimated)
                ),
            ),
            false,
        ),
        stat(
            l.text("Active liquidators", "活跃清算人"),
            ui::count(overview.liquidators),
            match overview.liquidators - overview.ranked_liquidators {
                0 => format!(
                    "{} {}",
                    l.text("latest", "最新"),
                    ui::age(overview.latest_at, l)
                ),
                unranked => l.text_owned(
                    format!(
                        "{} ranked · {} reward unknown",
                        ui::count(overview.ranked_liquidators),
                        ui::count(unranked)
                    ),
                    format!(
                        "{} 个可排名 · {} 个奖励未知",
                        ui::count(overview.ranked_liquidators),
                        ui::count(unranked)
                    ),
                ),
            },
            false,
        ),
        stat(
            l.text("Bad-debt events", "坏账事件"),
            ui::count(overview.bad_debt_events),
            l.text("debt not covered by collateral", "抵押不足以覆盖债务")
                .to_owned(),
            false,
        ),
    ]
    .concat();
    format!(
        "<section class=\"page-head\"><div><h1>{title}</h1><p>{subtitle}</p></div></section>{scanning}\
         <section class=\"stats\">{stats}</section>\
         <section class=\"grid2\"><article class=\"card\"><div class=\"card-head\"><h2>🏆 {leader_title}</h2><span class=\"muted\">30d</span></div>{leaders}</article>\
           <article class=\"card\"><div class=\"card-head\"><h2>💰 {biggest_title}</h2></div><div class=\"card-body\">{biggest}</div></article></section>\
         <article class=\"card\"><div class=\"card-head\"><h2>🛟 {recent_title}</h2></div>{recent}</article>\
         <p class=\"note\">{method}</p>",
        title = l.text("Liquidations", "清算"),
        subtitle = l.text(
            "Morpho-style lending liquidations on Robinhood Chain, read from LedgerFlow's node.",
            "Robinhood Chain 上 Morpho 类借贷协议的清算，由 LedgerFlow 节点直接读取。"
        ),
        leader_title = l.text("Liquidator ranking", "清算人收益榜"),
        leaders = liquidator_table(context, page.liquidators),
        biggest_title = l.text("Biggest bonuses", "最大清算奖励"),
        biggest = liquidation_list(context, page.biggest),
        recent_title = l.text("Recent liquidations", "最新清算"),
        recent = liquidation_table(context, page.recent),
        method = l.text(
            "Bonus = seized collateral value − repaid debt value at the hourly USD price, before gas. Venues are accepted only when they answer idToMarketParams for the liquidated market.",
            "清算奖励 = 没收抵押品价值 − 偿还债务价值（按小时美元价格，未扣 gas）。仅接受能对被清算市场响应 idToMarketParams 的合约。"
        ),
    )
}

fn liquidator_table(context: &PageContext<'_>, rows: &[LiquidatorRankRow]) -> String {
    let l = context.locale;
    if rows.is_empty() {
        return format!(
            "<div class=\"empty\">{}</div>",
            l.text("No liquidations in the last 30 days", "最近 30 天暂无清算")
        );
    }
    let body = rows
        .iter()
        .enumerate()
        .map(|(index, row)| {
            format!(
                "<tr data-href=\"{href}\"><td class=\"l\">{rank}</td><td class=\"l mono\">{address}</td><td class=\"up\"><strong>{bonus}</strong></td><td>{count}</td><td>{repaid}</td><td>{best}</td><td>{markets}</td><td class=\"muted\">{age}</td></tr>",
                href = address_url(context, &row.liquidator),
                rank = ui::rank(index + 1),
                address = ui::short_address(&row.liquidator),
                bonus = ui::signed_usd(row.bonus_usd),
                count = row.liquidations,
                repaid = ui::usd(row.repaid_usd),
                best = ui::signed_usd(row.best_bonus_usd),
                markets = row.markets,
                age = ui::age(row.last_at, l),
            )
        })
        .collect::<String>();
    format!(
        "<div class=\"table-wrap\"><table class=\"market\"><thead><tr><th class=\"l\">#</th><th class=\"l\">{}</th><th>{}</th><th>{}</th><th>{}</th><th>{}</th><th>{}</th><th>{}</th></tr></thead><tbody>{body}</tbody></table></div>",
        l.text("Liquidator", "清算人"),
        l.text("Bonus", "奖励"),
        l.text("Liquidations", "次数"),
        l.text("Debt repaid", "偿还债务"),
        l.text("Best", "最大单笔"),
        l.text("Markets", "市场"),
        l.text("Active", "最近活跃"),
    )
}

fn liquidation_list(context: &PageContext<'_>, rows: &[LiquidationRow]) -> String {
    let l = context.locale;
    if rows.is_empty() {
        return format!(
            "<div class=\"empty\">{}</div>",
            l.text("No liquidations yet", "暂无清算")
        );
    }
    rows.iter()
        .map(|row| {
            format!(
                "<a class=\"row\" href=\"{tx}\" target=\"_blank\" rel=\"noopener\"><span class=\"avatar\" style=\"--h:140\">🛟</span><div class=\"name\"><strong>{collateral} → {loan}</strong><small>{liquidator} · {age}</small></div><div class=\"val\"><strong class=\"up\">{bonus}</strong><small>{repaid_label} {repaid}</small></div></a>",
                tx = explorer_tx_url(context.chain_id, &row.tx_hash),
                collateral = escape_html(row.collateral_symbol.as_deref().unwrap_or("?")),
                loan = escape_html(row.loan_symbol.as_deref().unwrap_or("?")),
                liquidator = ui::short_address(&row.liquidator),
                age = ui::age(row.block_time, l),
                bonus = ui::signed_usd(row.bonus_usd),
                repaid_label = l.text("repaid", "偿还"),
                repaid = ui::usd(row.repaid_usd),
            )
        })
        .collect()
}

fn liquidation_table(context: &PageContext<'_>, rows: &[LiquidationRow]) -> String {
    let l = context.locale;
    if rows.is_empty() {
        return format!(
            "<div class=\"empty\">{}</div>",
            l.text("No liquidations yet", "暂无清算")
        );
    }
    let body = rows
        .iter()
        .map(|row| {
            format!(
                "<tr><td class=\"l muted\">{age}</td><td class=\"l\"><strong>{collateral}</strong> → {loan}{bad_debt}</td><td>{seized}</td><td>{repaid}</td><td class=\"up\">{bonus}</td><td class=\"l\"><a class=\"mono\" href=\"{liquidator_href}\">{liquidator}</a></td><td class=\"l mono\">{borrower}</td><td class=\"l\"><a class=\"muted\" href=\"{tx}\" target=\"_blank\" rel=\"noopener\">{hash} ↗</a></td></tr>",
                age = ui::age(row.block_time, l),
                collateral = escape_html(row.collateral_symbol.as_deref().unwrap_or("?")),
                loan = escape_html(row.loan_symbol.as_deref().unwrap_or("?")),
                bad_debt = if row.bad_debt { format!(" <span class=\"badge hot\">{}</span>", l.text("bad debt", "坏账")) } else { String::new() },
                seized = row.seized_amount.map(ui::compact).unwrap_or_else(|| "—".to_owned()),
                repaid = ui::usd(row.repaid_usd),
                bonus = ui::signed_usd(row.bonus_usd),
                liquidator_href = address_url(context, &row.liquidator),
                liquidator = ui::short_address(&row.liquidator),
                borrower = ui::short_address(&row.borrower),
                tx = explorer_tx_url(context.chain_id, &row.tx_hash),
                hash = ui::short_address(&row.tx_hash),
            )
        })
        .collect::<String>();
    format!(
        "<div class=\"table-wrap\"><table class=\"market\"><thead><tr><th class=\"l\">{}</th><th class=\"l\">{}</th><th>{}</th><th>{}</th><th>{}</th><th class=\"l\">{}</th><th class=\"l\">{}</th><th class=\"l\">Tx</th></tr></thead><tbody>{body}</tbody></table></div>",
        l.text("Age", "时间"),
        l.text("Collateral → debt", "抵押 → 债务"),
        l.text("Seized", "没收数量"),
        l.text("Repaid", "偿还"),
        l.text("Bonus", "奖励"),
        l.text("Liquidator", "清算人"),
        l.text("Borrower", "借款人"),
    )
}

fn window_tabs(context: &PageContext<'_>, path: &str, active: EarningsWindow) -> String {
    EarningsWindow::ALL
        .into_iter()
        .map(|window| {
            format!(
                "<a class=\"{}\" href=\"{path}?lang={}&chain_id={}&period={}\">{}</a>",
                if window == active { "active" } else { "" },
                context.locale.code(),
                context.chain_id,
                window.key(),
                window.key()
            )
        })
        .collect()
}

fn percent(part: i64, total: i64) -> String {
    if total <= 0 {
        return "—".to_owned();
    }
    format!("{:.0}%", part as f64 / total as f64 * 100.0)
}
