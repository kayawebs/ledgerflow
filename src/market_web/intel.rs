//! Address intelligence pages: earnings podium, wallet and cluster rankings,
//! newly discovered clusters, and per-address earnings.

use super::{PageContext, escape_html, ui};
use crate::services::{
    market_pnl::EarningsWindow,
    market_read::{AddressEarnings, ArbitrageRow, ClusterRankRow, PositionRow, WalletRankRow},
    protocol_market::MarketAddressLookup,
};

pub struct IntelPage<'a> {
    pub window: EarningsWindow,
    pub wallets: &'a [WalletRankRow],
    pub clusters: &'a [ClusterRankRow],
    pub newest_clusters: &'a [ClusterRankRow],
    pub lookup_error: Option<&'a str>,
}

pub fn intel(context: &PageContext<'_>, page: &IntelPage<'_>) -> String {
    let l = context.locale;
    let window_tabs = EarningsWindow::ALL
        .into_iter()
        .map(|window| {
            format!(
                "<a class=\"{}\" href=\"/clusters?lang={}&chain_id={}&period={}\">{}</a>",
                if window == page.window { "active" } else { "" },
                l.code(),
                context.chain_id,
                window.key(),
                window.key()
            )
        })
        .collect::<String>();
    let domain_tabs = format!(
        "<nav class=\"tabs\"><a class=\"active\" href=\"{}\">🚀 {}</a><a href=\"{}\">⚡ {}</a><a href=\"{}\">🛟 {}</a></nav>",
        context.url("/clusters"),
        l.text("Launch traders", "发币交易者"),
        context.url("/arbitrage"),
        l.text("Arbitrage", "套利"),
        context.url("/liquidations"),
        l.text("Liquidations", "清算"),
    );
    let computed = page
        .wallets
        .first()
        .map(|row| ui::age(Some(row.computed_at), l))
        .unwrap_or_else(|| "—".to_owned());
    format!(
        "<section class=\"page-head\"><div><h1>{title}</h1><p>{subtitle}</p></div><nav class=\"windows\">{window_tabs}</nav></section>\
         <div class=\"toolbar\">{domain_tabs}<form class=\"search\" action=\"/clusters\"><input type=\"hidden\" name=\"lang\" value=\"{lang}\"><input type=\"hidden\" name=\"chain_id\" value=\"{chain}\"><input name=\"address\" placeholder=\"{placeholder}\" autocomplete=\"off\"><button>{search}</button></form></div>\
         {error}\
         <section class=\"podium\">{podium}</section>\
         <section class=\"grid2\"><article class=\"card\"><div class=\"card-head\"><h2>🏆 {wallet_title}</h2><span class=\"muted\">{trust} · {updated} {computed}</span></div>{wallets}</article>\
           <article class=\"card\"><div class=\"card-head\"><h2>🆕 {new_title}</h2><span class=\"muted\">{new_note}</span></div><div class=\"card-body\">{newest}</div></article></section>\
         <article class=\"card\"><div class=\"card-head\"><h2>👥 {cluster_title}</h2><span class=\"muted\">{cluster_note}</span></div>{clusters}</article>\
         <p class=\"note\">{method}</p>",
        title = l.text("Who is making money", "谁在赚钱"),
        subtitle = l.text(
            "Ranked earnings of Pons traders and protocol-scoped clusters, computed from every curve trade.",
            "基于全部联合曲线成交计算的 Pons 交易者与协议域地址集群收益排行。"
        ),
        lang = l.code(),
        chain = context.chain_id,
        placeholder = l.text("Look up any address 0x…", "查询任意地址 0x…"),
        search = l.text("Look up", "查询"),
        error = page
            .lookup_error
            .map(|error| format!("<p class=\"note down\">{}</p>", escape_html(error)))
            .unwrap_or_default(),
        podium = podium(context, page.wallets),
        wallet_title = l.text("Top traders", "交易者收益榜"),
        trust = ui::trust(l, ui::Trust::Estimated),
        updated = l.text("updated", "更新于"),
        computed = format!("{computed} {}", l.text("ago", "前")),
        wallets = wallet_table(context, page.wallets),
        new_title = l.text("Newly identified clusters", "最新识别的集群"),
        new_note = page.window.key(),
        newest = newest_clusters(context, page.newest_clusters),
        cluster_title = l.text("Top earning clusters", "集群收益榜"),
        cluster_note = l.text(
            "Creator + repeat buyers of the creator's launches",
            "创建者 + 反复买入其发币的地址"
        ),
        clusters = cluster_table(context, page.clusters),
        method = l.text(
            "Earnings = sell proceeds + remaining tokens at the latest curve price − purchase cost (average cost, USD at trade time). Gas, transfers and other venues are excluded; wallets that sold more than they bought on the curve are not ranked. Clusters are valid only inside the Pons domain and never imply common ownership.",
            "收益 = 卖出所得 + 剩余代币按最新曲线价格估值 − 买入成本（平均成本法，美元按成交时价格换算）。不含 gas、转账及其他交易场所；曲线卖出多于买入的地址不参与排名。集群仅在 Pons 协议域内成立，不代表同一实体。"
        ),
    )
}

fn podium(context: &PageContext<'_>, wallets: &[WalletRankRow]) -> String {
    let l = context.locale;
    if wallets.is_empty() {
        return format!(
            "<article class=\"card\"><div class=\"empty\">{}</div></article>",
            l.text("Rankings are being computed", "排行榜计算中")
        );
    }
    wallets
        .iter()
        .take(3)
        .map(|wallet| {
            format!(
                "<a class=\"card{first}\" href=\"{href}\"><div class=\"card-head\" style=\"padding:0;border:0\">{rank}<span class=\"muted mono\">{address}</span>{kind}</div>\
                 <div class=\"pnl up num\">{pnl}</div><div class=\"muted\">{win_label} {win_rate} · {tokens} {tokens_label} · {trades} {trades_label}</div>\
                 <div class=\"note\">{best}</div></a>",
                first = if wallet.rank == 1 { " first" } else { "" },
                href = address_url(context, &wallet.address),
                rank = ui::rank(wallet.rank as usize),
                address = ui::short_address(&wallet.address),
                kind = kind_badge(context, wallet.address_kind.as_deref()),
                pnl = ui::signed_usd(Some(wallet.pnl_usd)),
                win_label = l.text("Win rate", "胜率"),
                win_rate = win_rate(wallet.winning_tokens, wallet.tokens_traded),
                tokens = wallet.tokens_traded,
                tokens_label = l.text("tokens", "个币"),
                trades = ui::count(wallet.trades.into()),
                trades_label = l.text("trades", "笔"),
                best = best_trade(context, wallet),
            )
        })
        .collect()
}

fn wallet_table(context: &PageContext<'_>, wallets: &[WalletRankRow]) -> String {
    let l = context.locale;
    if wallets.is_empty() {
        return format!(
            "<div class=\"empty\">{}</div>",
            l.text("No ranked wallets yet", "暂无排名")
        );
    }
    let rows = wallets
        .iter()
        .map(|wallet| {
            format!(
                "<tr data-href=\"{href}\"><td class=\"l\">{rank}</td><td class=\"l\"><span class=\"mono\">{address}</span> {kind}{clusters}</td><td class=\"{pnl_class}\"><strong>{pnl}</strong></td><td>{realized}</td><td>{unrealized}</td><td>{win}</td><td>{tokens}</td><td class=\"l\">{best}</td><td class=\"muted\">{age}</td></tr>",
                href = address_url(context, &wallet.address),
                rank = ui::rank(wallet.rank as usize),
                address = ui::short_address(&wallet.address),
                kind = kind_badge(context, wallet.address_kind.as_deref()),
                clusters = if wallet.cluster_count > 0 {
                    format!(" <span class=\"badge\">👥 {}</span>", wallet.cluster_count)
                } else {
                    String::new()
                },
                pnl_class = if wallet.pnl_usd >= 0.0 { "up" } else { "down" },
                pnl = ui::signed_usd(Some(wallet.pnl_usd)),
                realized = ui::signed_usd(Some(wallet.realized_usd)),
                unrealized = ui::signed_usd(Some(wallet.unrealized_usd)),
                win = win_rate(wallet.winning_tokens, wallet.tokens_traded),
                tokens = wallet.tokens_traded,
                best = best_trade(context, wallet),
                age = ui::age(wallet.last_trade_at, l),
            )
        })
        .collect::<String>();
    format!(
        "<div class=\"table-wrap\"><table class=\"market\"><thead><tr><th class=\"l\">#</th><th class=\"l\">{}</th><th>{}</th><th>{}</th><th>{}</th><th>{}</th><th>{}</th><th class=\"l\">{}</th><th>{}</th></tr></thead><tbody>{rows}</tbody></table></div>",
        l.text("Wallet", "地址"),
        l.text("Earnings", "收益"),
        l.text("Realized", "已实现"),
        l.text("Unrealized", "未实现"),
        l.text("Win rate", "胜率"),
        l.text("Tokens", "币种数"),
        l.text("Best trade", "最佳一笔"),
        l.text("Active", "最近活跃"),
    )
}

fn cluster_table(context: &PageContext<'_>, clusters: &[ClusterRankRow]) -> String {
    let l = context.locale;
    let ranked = clusters
        .iter()
        .filter(|cluster| cluster.pnl_usd > 0.0)
        .take(50)
        .collect::<Vec<_>>();
    if ranked.is_empty() {
        return format!(
            "<div class=\"empty\">{}</div>",
            l.text(
                "No profitable clusters in this window",
                "该时间段暂无盈利集群"
            )
        );
    }
    let rows = ranked
        .iter()
        .map(|cluster| {
            format!(
                "<tr data-href=\"{href}\"><td class=\"l\">{rank}</td><td class=\"l\"><strong>{label}</strong><div class=\"muted mono\">{creator}</div></td><td>{members}</td><td class=\"up\"><strong>{pnl}</strong></td><td>{winners}</td><td>{tokens}</td><td class=\"l\">{best}</td><td class=\"muted\">{found}</td></tr>",
                href = cluster_url(context, cluster),
                rank = ui::rank(cluster.rank as usize),
                label = l.text("Issuer cohort", "发币者集群"),
                creator = cluster
                    .subject_address
                    .as_deref()
                    .map(ui::short_address)
                    .unwrap_or_default(),
                members = format!("{} / {}", cluster.active_members, cluster.member_count),
                pnl = ui::signed_usd(Some(cluster.pnl_usd)),
                winners = cluster.winning_members,
                tokens = cluster.tokens_traded,
                best = cluster_best(cluster),
                found = ui::age(cluster.first_seen_at, l),
            )
        })
        .collect::<String>();
    format!(
        "<div class=\"table-wrap\"><table class=\"market\"><thead><tr><th class=\"l\">#</th><th class=\"l\">{}</th><th>{}</th><th>{}</th><th>{}</th><th>{}</th><th class=\"l\">{}</th><th>{}</th></tr></thead><tbody>{rows}</tbody></table></div>",
        l.text("Cluster", "集群"),
        l.text("Active / members", "活跃 / 成员"),
        l.text("Earnings", "收益"),
        l.text("Profitable members", "盈利成员"),
        l.text("Tokens", "币种数"),
        l.text("Best token", "最赚的币"),
        l.text("Identified", "识别于"),
    )
}

fn newest_clusters(context: &PageContext<'_>, clusters: &[ClusterRankRow]) -> String {
    let l = context.locale;
    if clusters.is_empty() {
        return format!(
            "<div class=\"empty\">{}</div>",
            l.text("No new clusters yet", "暂无新集群")
        );
    }
    clusters
        .iter()
        .map(|cluster| {
            format!(
                "<a class=\"row\" href=\"{href}\"><span class=\"avatar\" style=\"--h:{hue}\">👥</span><div class=\"name\"><strong>{creator}</strong><small>{members} {members_label} · {found} {ago}</small></div><div class=\"val\"><strong class=\"{class}\">{pnl}</strong><small>{best}</small></div></a>",
                href = cluster_url(context, cluster),
                hue = cluster.cluster_id.as_bytes()[0] as u32 * 360 / 256,
                creator = cluster
                    .subject_address
                    .as_deref()
                    .map(ui::short_address)
                    .unwrap_or_default(),
                members = cluster.member_count,
                members_label = l.text("members", "成员"),
                found = ui::age(cluster.first_seen_at, l),
                ago = l.text("ago", "前"),
                class = if cluster.pnl_usd >= 0.0 { "up" } else { "down" },
                pnl = ui::signed_usd(Some(cluster.pnl_usd)),
                best = cluster_best(cluster),
            )
        })
        .collect()
}

pub struct AddressPage<'a> {
    pub address: &'a str,
    pub earnings: &'a AddressEarnings,
    pub ranks: &'a [(EarningsWindow, Option<WalletRankRow>)],
    pub positions: &'a [PositionRow],
    pub lookup: &'a MarketAddressLookup,
    pub arbitrage: &'a [ArbitrageRow],
}

pub fn address(context: &PageContext<'_>, page: &AddressPage<'_>) -> String {
    let l = context.locale;
    let earnings = page.earnings;
    let rank_badges = page
        .ranks
        .iter()
        .filter_map(|(window, rank)| {
            rank.as_ref().map(|rank| {
                format!(
                    "<span class=\"badge hot\">🏆 #{} · {}</span>",
                    rank.rank,
                    window.key()
                )
            })
        })
        .collect::<Vec<_>>()
        .join(" ");
    let kv = |label: &str, value: String| {
        format!("<div><span>{label}</span><strong class=\"num\">{value}</strong></div>")
    };
    let stats = [
        kv(
            l.text("Total earnings", "总收益"),
            format!(
                "<span class=\"{}\">{}</span>",
                if earnings.pnl_usd >= 0.0 {
                    "up"
                } else {
                    "down"
                },
                ui::signed_usd(Some(earnings.pnl_usd))
            ),
        ),
        kv(
            l.text("Realized", "已实现"),
            ui::signed_usd(Some(earnings.realized_usd)),
        ),
        kv(
            l.text("Unrealized", "未实现"),
            ui::signed_usd(Some(earnings.unrealized_usd)),
        ),
        kv(l.text("Invested", "投入"), ui::usd(Some(earnings.cost_usd))),
        kv(
            l.text("Win rate", "胜率"),
            win_rate(earnings.winning_tokens as i32, earnings.tokens as i32),
        ),
        kv(
            l.text("Tokens traded", "交易币种"),
            ui::count(earnings.tokens),
        ),
        kv(
            l.text("Launches created", "发币数量"),
            ui::count(earnings.launches_created),
        ),
        kv(
            l.text("Clusters", "所属集群"),
            ui::count(page.lookup.cluster_matches.len() as i64),
        ),
    ]
    .concat();
    let positions = if page.positions.is_empty() {
        format!(
            "<div class=\"empty\">{}</div>",
            l.text("No Pons curve trades", "暂无 Pons 曲线成交")
        )
    } else {
        let rows = page
            .positions
            .iter()
            .map(|position| {
                format!(
                    "<tr data-href=\"{href}\"><td class=\"l\"><div class=\"tok\">{avatar}<div><strong>{symbol}</strong><small>{buys}B / {sells}S</small></div></div></td><td>{cost}</td><td>{proceeds}</td><td>{holding}</td><td class=\"{class}\"><strong>{pnl}</strong></td><td>{multiple}</td><td class=\"muted\">{age}</td></tr>",
                    href = context.url(&format!("/launches/{}", position.launch_id)),
                    avatar = ui::token_avatar(position.symbol.as_deref(), &position.token_address),
                    symbol = escape_html(position.symbol.as_deref().unwrap_or("?")),
                    buys = position.buy_count,
                    sells = position.sell_count,
                    cost = ui::usd(position.cost_usd),
                    proceeds = ui::usd(position.proceeds_usd),
                    holding = ui::usd(position.holding_usd),
                    class = match position.pnl_usd {
                        Some(value) if value >= 0.0 => "up",
                        Some(_) => "down",
                        None => "muted",
                    },
                    pnl = if position.complete_inventory {
                        ui::signed_usd(position.pnl_usd)
                    } else {
                        l.text("Incomplete", "库存不完整").to_owned()
                    },
                    multiple = position
                        .multiple
                        .map(|value| format!("{value:.2}x"))
                        .unwrap_or_else(|| "—".to_owned()),
                    age = ui::age(position.last_trade_at, l),
                )
            })
            .collect::<String>();
        format!(
            "<div class=\"table-wrap\"><table class=\"market\"><thead><tr><th class=\"l\">{}</th><th>{}</th><th>{}</th><th>{}</th><th>{}</th><th>{}</th><th>{}</th></tr></thead><tbody>{rows}</tbody></table></div>",
            l.text("Token", "代币"),
            l.text("Cost", "买入成本"),
            l.text("Proceeds", "卖出所得"),
            l.text("Holding value", "持仓价值"),
            l.text("Earnings", "收益"),
            l.text("Multiple", "倍数"),
            l.text("Last trade", "最近成交"),
        )
    };
    let clusters = if page.lookup.cluster_matches.is_empty() {
        format!(
            "<div class=\"empty\">{}</div>",
            l.text(
                "Not a member of any protocol-scoped cluster",
                "不属于任何协议域集群"
            )
        )
    } else {
        page.lookup
            .cluster_matches
            .iter()
            .map(|matched| {
                format!(
                    "<a class=\"row\" href=\"{href}\"><span class=\"avatar\" style=\"--h:200\">👥</span><div class=\"name\"><strong>{kind}</strong><small>{domain} · {role}</small></div><div class=\"val\"><small>{members} {members_label}</small></div></a>",
                    href = context.url(&format!("/clusters/{}", matched.cluster.cluster_id)),
                    kind = escape_html(&super::humanize(&matched.cluster.cluster_kind)),
                    domain = escape_html(l.text(&matched.cluster.domain_name_en, &matched.cluster.domain_name_zh)),
                    role = escape_html(&super::humanize(&matched.member_role)),
                    members = matched.cluster.member_count,
                    members_label = l.text("members", "成员"),
                )
            })
            .collect()
    };
    format!(
        "<a class=\"note\" href=\"{back}\">← {back_label}</a>\
         <section class=\"token-hero\"><div class=\"id\"><span class=\"avatar\" style=\"--h:{hue}\">🧠</span><div><h1 class=\"mono\" style=\"font-size:20px\">{address}</h1><div>{rank_badges}</div></div></div>\
           <div class=\"price\"><strong class=\"{class}\">{pnl}</strong><div class=\"muted\">{pnl_label} {trust}</div></div></section>\
         <article class=\"card\" style=\"margin-bottom:12px\"><div class=\"kv\">{stats}</div></article>\
         <section class=\"grid2\"><article class=\"card\"><div class=\"card-head\"><h2>{positions_title}</h2><span class=\"muted\">{positions_note}</span></div>{positions}</article>\
           <article class=\"card\"><div class=\"card-head\"><h2>{clusters_title}</h2></div><div class=\"card-body\">{clusters}</div></article></section>{arbitrage}",
        back = context.url("/clusters"),
        back_label = l.text("Rankings", "收益排行"),
        hue = page.address.bytes().last().unwrap_or(0) as u32 * 360 / 256,
        address = escape_html(page.address),
        rank_badges = rank_badges,
        class = if earnings.pnl_usd >= 0.0 {
            "up"
        } else {
            "down"
        },
        pnl = ui::signed_usd(Some(earnings.pnl_usd)),
        pnl_label = l.text("Pons curve earnings", "Pons 曲线收益"),
        trust = ui::trust(l, ui::Trust::Estimated),
        stats = stats,
        positions_title = l.text("Positions", "持仓与交易"),
        positions_note = if earnings.excluded_positions > 0 {
            format!(
                "{} {}",
                earnings.excluded_positions,
                l.text(
                    "positions excluded (incomplete inventory or no price)",
                    "个持仓未计入（库存不完整或无价格）"
                )
            )
        } else {
            String::new()
        },
        positions = positions,
        clusters_title = l.text("Clusters", "所属集群"),
        clusters = clusters,
        arbitrage = if page.arbitrage.is_empty() {
            String::new()
        } else {
            format!(
                "<article class=\"card\"><div class=\"card-head\"><h2>⚡ {}</h2>{}</div><div class=\"card-body\">{}</div></article>",
                l.text("Arbitrage executions", "套利执行"),
                ui::trust(l, ui::Trust::Estimated),
                super::execution::arbitrage_list(context, page.arbitrage)
            )
        },
    )
}

/// Earnings section shown above a cluster's evidence.
pub fn cluster_earnings(
    context: &PageContext<'_>,
    rank: Option<&ClusterRankRow>,
    members: &[(String, String, f64, f64, i64)],
) -> String {
    let l = context.locale;
    let headline = rank
        .map(|rank| {
            format!(
                "<div class=\"stats\"><div class=\"stat glow\"><span>{} · 30d</span><strong class=\"{}\">{}</strong><small>🏆 #{}</small></div><div class=\"stat\"><span>{}</span><strong>{} / {}</strong></div><div class=\"stat\"><span>{}</span><strong>{}</strong></div><div class=\"stat\"><span>{}</span><strong>{}</strong><small>{}</small></div></div>",
                l.text("Cluster earnings", "集群收益"),
                if rank.pnl_usd >= 0.0 { "up" } else { "down" },
                ui::signed_usd(Some(rank.pnl_usd)),
                rank.rank,
                l.text("Active / members", "活跃 / 成员"),
                rank.active_members,
                rank.member_count,
                l.text("Profitable members", "盈利成员"),
                rank.winning_members,
                l.text("Best token", "最赚的币"),
                escape_html(rank.best_symbol.as_deref().unwrap_or("—")),
                ui::signed_usd(rank.best_pnl_usd),
            )
        })
        .unwrap_or_default();
    if members.is_empty() {
        return headline;
    }
    let rows = members
        .iter()
        .enumerate()
        .map(|(index, (address, role, pnl, cost, tokens))| {
            format!(
                "<tr data-href=\"{href}\"><td class=\"l\">{rank}</td><td class=\"l mono\">{short}</td><td class=\"l\">{role}</td><td class=\"{class}\"><strong>{pnl}</strong></td><td>{cost}</td><td>{tokens}</td></tr>",
                href = address_url(context, address),
                rank = ui::rank(index + 1),
                short = ui::short_address(address),
                role = escape_html(&super::humanize(role)),
                class = if *pnl >= 0.0 { "up" } else { "down" },
                pnl = ui::signed_usd(Some(*pnl)),
                cost = ui::usd(Some(*cost)),
            )
        })
        .collect::<String>();
    format!(
        "{headline}<article class=\"card\" style=\"margin-bottom:16px\"><div class=\"card-head\"><h2>{title}</h2>{trust}</div><div class=\"table-wrap\"><table class=\"market\"><thead><tr><th class=\"l\">#</th><th class=\"l\">{member}</th><th class=\"l\">{role}</th><th>{earnings}</th><th>{cost}</th><th>{tokens}</th></tr></thead><tbody>{rows}</tbody></table></div></article>",
        title = l.text(
            "Member earnings on the creator's launches",
            "成员在创建者发币上的收益"
        ),
        trust = ui::trust(l, ui::Trust::Estimated),
        member = l.text("Member", "成员"),
        role = l.text("Role", "角色"),
        earnings = l.text("Earnings", "收益"),
        cost = l.text("Invested", "投入"),
        tokens = l.text("Tokens", "币种数"),
    )
}

pub fn address_url(context: &PageContext<'_>, address: &str) -> String {
    context.url(&format!("/address/{address}"))
}

fn cluster_url(context: &PageContext<'_>, cluster: &ClusterRankRow) -> String {
    context.url(&format!("/clusters/{}", cluster.cluster_id))
}

fn kind_badge(context: &PageContext<'_>, kind: Option<&str>) -> String {
    match kind {
        Some("contract") => format!(
            "<span class=\"badge\">🤖 {}</span>",
            context.locale.text("Contract", "合约")
        ),
        Some("eoa") => "<span class=\"badge\">👤 EOA</span>".to_owned(),
        _ => String::new(),
    }
}

fn win_rate(wins: i32, total: i32) -> String {
    if total <= 0 {
        return "—".to_owned();
    }
    format!("{:.0}%", f64::from(wins) / f64::from(total) * 100.0)
}

fn best_trade(context: &PageContext<'_>, wallet: &WalletRankRow) -> String {
    let (Some(symbol), Some(pnl)) = (wallet.best_symbol.as_deref(), wallet.best_pnl_usd) else {
        return "—".to_owned();
    };
    let multiple = wallet
        .best_multiple
        .filter(|value| value.is_finite() && *value > 0.0)
        .map(|value| format!(" <span class=\"pill up\">{value:.1}x</span>"))
        .unwrap_or_default();
    format!(
        "<a href=\"{}\"><strong>{}</strong></a> {} → <span class=\"up\">{}</span>{multiple}",
        wallet
            .best_launch_id
            .map(|launch_id| context.url(&format!("/launches/{launch_id}")))
            .unwrap_or_default(),
        escape_html(symbol),
        ui::usd(wallet.best_cost_usd),
        ui::signed_usd(Some(pnl)),
    )
}

fn cluster_best(cluster: &ClusterRankRow) -> String {
    match (cluster.best_symbol.as_deref(), cluster.best_pnl_usd) {
        (Some(symbol), Some(pnl)) => {
            format!("{} {}", escape_html(symbol), ui::signed_usd(Some(pnl)))
        }
        _ => "—".to_owned(),
    }
}
