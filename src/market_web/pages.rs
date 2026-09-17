//! Exchange-style launch pages: overview leaderboards, the token market table
//! and token detail. Sections that refresh in place are rendered by the
//! `fragment_*` functions and served under `/fragments/*`.

use chrono::Utc;

use super::{PageContext, escape_html, explorer_tx_url, ui};
use crate::services::market_read::{
    CandleRow, MarketTicker, TokenBoard, TokenPage, TokenRow, TradeRow,
};

/// Header navigation entries: key, path, English, Chinese.
pub const NAV: [(&str, &str, &str, &str); 5] = [
    ("overview", "/", "Overview", "总览"),
    ("launches", "/launches", "Launches", "发币"),
    ("clusters", "/clusters", "Address Intelligence", "地址情报"),
    ("arbitrage", "/arbitrage", "Arbitrage", "套利"),
    ("liquidations", "/liquidations", "Liquidations", "清算"),
];

pub fn fragment_ticker(context: &PageContext<'_>, ticker: &MarketTicker) -> String {
    let l = context.locale;
    let gainer = match (&ticker.top_gainer_symbol, ticker.top_gainer_launch_id) {
        (Some(symbol), Some(launch_id)) => format!(
            "<a href=\"{}\">{} <b>{}</b> {}</a>",
            context.url(&format!("/launches/{launch_id}")),
            l.text("Top gainer", "今日涨幅王"),
            escape_html(symbol),
            ui::change(ticker.top_gainer_change)
        ),
        _ => String::new(),
    };
    format!(
        "<span class=\"live\">{live}</span>\
         <span>ETH<b data-k=\"t-eth\">{eth}</b></span>\
         <span>{vol_label}<b data-k=\"t-vol\">{vol}</b></span>\
         <span>{trades_label}<b data-k=\"t-trades\">{trades}</b></span>\
         <span>{new_label}<b data-k=\"t-new\">{new}</b></span>\
         <span>{grad_label}<b data-k=\"t-grad\">{grad}</b></span>{gainer}",
        live = l.text("Live · Robinhood Chain", "实时 · Robinhood Chain"),
        eth = ui::price_usd(ticker.native_usd),
        vol_label = l.text("24h volume (reliable-priced)", "24h 成交额（可靠定价）"),
        vol = ui::usd(Some(ticker.volume_24h_usd)),
        trades_label = l.text("24h trades", "24h 成交笔数"),
        trades = ui::count(ticker.trades_24h),
        new_label = l.text("New launches 1h", "1h 新币"),
        new = ui::count(ticker.launches_1h),
        grad_label = l.text("Graduated 24h", "24h 毕业"),
        grad = ui::count(ticker.graduations_24h),
    )
}

pub fn overview(
    context: &PageContext<'_>,
    ticker: &MarketTicker,
    boards: &str,
    feed: &str,
) -> String {
    let l = context.locale;
    format!(
        "<section class=\"page-head\"><div><h1>{title}</h1><p>{subtitle}</p></div></section>\
         <section class=\"stats\">{stats}</section>\
         <section data-refresh=\"{boards_url}\" data-every=\"20\">{boards}</section>\
         <section class=\"grid2\">\
           <article class=\"card\"><div class=\"card-head\"><h2>🐋 {feed_title}</h2><span class=\"muted\">{feed_note}</span></div>\
             <div class=\"card-body feed\" data-refresh=\"{feed_url}\" data-every=\"10\">{feed}</div></article>\
           <article class=\"card\"><div class=\"card-head\"><h2>🧭 {explore_title}</h2></div><div class=\"card-body\">{explore}</div></article>\
         </section>",
        title = l.text("Pons market, live", "Pons 实时行情"),
        subtitle = l.text(
            "Every figure is computed from Robinhood Chain data read through LedgerFlow's own node.",
            "所有数据均由 LedgerFlow 自有节点读取 Robinhood Chain 链上数据计算。"
        ),
        stats = overview_stats(context, ticker),
        boards_url = fragment_url(context, "/fragments/overview/boards", ""),
        boards = boards,
        feed_title = l.text("Whale trades", "大额成交"),
        feed_note = l.text("≥ $100 · last few minutes", "≥ $100 · 最近几分钟"),
        feed_url = fragment_url(context, "/fragments/overview/feed", ""),
        feed = feed,
        explore_title = l.text("Explore", "探索"),
        explore = explore_links(context),
    )
}

fn overview_stats(context: &PageContext<'_>, ticker: &MarketTicker) -> String {
    let l = context.locale;
    let stat = |label: &str, value: String, note: String, glow: bool| {
        format!(
            "<div class=\"stat{}\"><span>{label}</span><strong class=\"num\">{value}</strong><small>{note}</small></div>",
            if glow { " glow" } else { "" }
        )
    };
    [
        stat(
            l.text("24h volume (reliable-priced)", "24h 成交额（可靠定价）"),
            ui::usd(Some(ticker.volume_24h_usd)),
            format!(
                "{} {}",
                ui::count(ticker.trades_24h),
                l.text("trades", "笔成交")
            ),
            true,
        ),
        stat(
            l.text("Active tokens 24h", "24h 活跃代币"),
            ui::count(ticker.active_tokens_24h),
            l.text("traded at least once", "至少有一笔成交").to_owned(),
            false,
        ),
        stat(
            l.text("New launches 24h", "24h 新发币"),
            ui::count(ticker.launches_24h),
            format!(
                "{} {}",
                ui::count(ticker.launches_1h),
                l.text("in the last hour", "个在最近 1 小时")
            ),
            false,
        ),
        stat(
            l.text("Graduated 24h", "24h 毕业"),
            ui::count(ticker.graduations_24h),
            l.text("bonding curve completed", "完成联合曲线").to_owned(),
            false,
        ),
        stat(
            l.text("Top gainer 24h", "24h 涨幅王"),
            ticker
                .top_gainer_symbol
                .as_deref()
                .map(escape_html)
                .unwrap_or_else(|| "—".to_owned()),
            strip_tags(&ui::change(ticker.top_gainer_change)),
            false,
        ),
    ]
    .concat()
}

pub struct OverviewBoards<'a> {
    pub hot: &'a [TokenRow],
    pub gainers: &'a [TokenRow],
    pub newest: &'a [TokenRow],
    pub graduating: &'a [TokenRow],
}

pub fn fragment_overview_boards(context: &PageContext<'_>, boards: &OverviewBoards<'_>) -> String {
    let l = context.locale;
    format!(
        "<div class=\"boards\">{}{}{}{}</div>",
        board_card(
            context,
            "🔥",
            l.text("Trending", "热门"),
            "hot",
            boards.hot,
            BoardValue::Volume1h
        ),
        board_card(
            context,
            "🚀",
            l.text("Top gainers 24h", "涨幅榜 24h"),
            "gainers",
            boards.gainers,
            BoardValue::Change24h
        ),
        board_card(
            context,
            "🆕",
            l.text("New launches", "新币"),
            "new",
            boards.newest,
            BoardValue::Age
        ),
        board_card(
            context,
            "🎓",
            l.text("About to graduate", "即将毕业"),
            "graduating",
            boards.graduating,
            BoardValue::Progress
        ),
    )
}

#[derive(Clone, Copy)]
enum BoardValue {
    Volume1h,
    Change24h,
    Age,
    Progress,
}

fn board_card(
    context: &PageContext<'_>,
    icon: &str,
    title: &str,
    tab: &str,
    rows: &[TokenRow],
    value: BoardValue,
) -> String {
    let l = context.locale;
    let body = if rows.is_empty() {
        format!(
            "<div class=\"empty\">{}</div>",
            l.text("Waiting for trades", "等待成交数据")
        )
    } else {
        rows.iter()
            .enumerate()
            .map(|(index, row)| {
                let (primary, secondary) = match value {
                    BoardValue::Volume1h => (
                        ui::change(row.change_1h_pct),
                        format!("{} · 1h", ui::usd(row.volume_1h_usd)),
                    ),
                    BoardValue::Change24h => (
                        ui::change(row.change_24h_pct),
                        format!("{} {}", l.text("Vol", "成交"), ui::usd(row.volume_24h_usd)),
                    ),
                    BoardValue::Age => (
                        ui::change(row.change_1h_pct),
                        format!("{} · {}", ui::age(row.launched_at, l), ui::usd(row.volume_24h_usd)),
                    ),
                    BoardValue::Progress => (
                        ui::progress(row.bonding_progress_pct),
                        format!("MC {}", ui::usd(row.market_cap_usd)),
                    ),
                };
                format!(
                    "<a class=\"row\" href=\"{href}\">{rank}{avatar}<div class=\"name\"><strong>{symbol}</strong><small>MC {mcap} · {price}</small></div><div class=\"val\"><strong data-k=\"{tab}-{id}\">{primary}</strong><small>{secondary}</small></div></a>",
                    href = context.url(&format!("/launches/{}", row.launch_id)),
                    rank = ui::rank(index + 1),
                    avatar = ui::token_avatar(row.symbol.as_deref(), &row.token_address),
                    symbol = escape_html(row.symbol.as_deref().unwrap_or("?")),
                    mcap = ui::usd(row.market_cap_usd),
                    price = graded_price(row),
                    id = row.launch_id,
                )
            })
            .collect::<String>()
    };
    format!(
        "<article class=\"card\"><div class=\"card-head\"><h3>{icon} {title}</h3><a href=\"{more}\">{all} →</a></div><div class=\"card-body\">{body}</div></article>",
        more = tab_url(context, tab),
        all = l.text("All", "全部"),
    )
}

pub fn fragment_trade_feed(context: &PageContext<'_>, trades: &[TradeRow]) -> String {
    let l = context.locale;
    if trades.is_empty() {
        return format!(
            "<div class=\"empty\">{}</div>",
            l.text(
                "No large trades in the last few minutes",
                "最近几分钟没有大额成交"
            )
        );
    }
    trades
        .iter()
        .map(|trade| {
            let buy = trade.trade_side == "buy";
            format!(
                "<a class=\"row\" href=\"{href}\">{avatar}<div class=\"name\"><strong><span class=\"{side_class}\">{side}</span> {symbol}</strong><small>{trader} · {age}</small></div><div class=\"val\"><strong class=\"amt {side_class}\">{usd}</strong><small>{quote}</small></div></a>",
                href = context.url(&format!("/launches/{}", trade.launch_id)),
                avatar = ui::token_avatar(trade.symbol.as_deref(), &trade.token_address),
                side_class = if buy { "buy" } else { "sell" },
                side = if buy { l.text("BUY", "买入") } else { l.text("SELL", "卖出") },
                symbol = escape_html(trade.symbol.as_deref().unwrap_or("?")),
                trader = ui::short_address(&trade.trader_address),
                age = ui::age(trade.occurred_at, l),
                usd = ui::usd(trade.usd_value),
                quote = format!(
                    "{} {}",
                    ui::compact(trade.quote_amount.unwrap_or_default()),
                    escape_html(trade.quote_symbol.as_deref().unwrap_or("ETH"))
                ),
            )
        })
        .collect()
}

fn explore_links(context: &PageContext<'_>) -> String {
    let l = context.locale;
    [
        ("📈", "/launches?tab=gainers", l.text("Biggest movers", "涨跌幅排行"), l.text("24h gainers and losers with real volume", "有真实成交量的 24h 涨跌榜")),
        ("💰", "/launches?tab=volume", l.text("Most traded", "成交额排行"), l.text("Where the money is flowing now", "资金正在流向哪里")),
        ("🎓", "/launches?tab=graduated", l.text("Just graduated", "刚毕业"), l.text("Launches that completed their curve", "已完成联合曲线的代币")),
        ("🧠", "/clusters", l.text("Address intelligence", "地址情报"), l.text("Protocol-scoped wallet clusters and earnings", "协议范围内的地址集群与收益")),
        ("⚡", "/arbitrage", l.text("Arbitrage bots", "套利机器人"), l.text("Verified arbitrage executions", "已验证套利执行")),
    ]
    .into_iter()
    .map(|(icon, path, title, note)| {
        format!(
            "<a class=\"row\" href=\"{}\"><span class=\"avatar\" style=\"--h:24\">{icon}</span><div class=\"name\"><strong>{title}</strong><small>{note}</small></div><div class=\"val muted\">→</div></a>",
            with_params(context, path)
        )
    })
    .collect()
}

pub struct LaunchTableQuery<'a> {
    pub board: TokenBoard,
    pub sort: Option<&'a str>,
    pub descending: bool,
    pub search: Option<&'a str>,
}

pub fn launches(context: &PageContext<'_>, query: &LaunchTableQuery<'_>, table: &str) -> String {
    let l = context.locale;
    let tabs = TokenBoard::ALL
        .into_iter()
        .map(|board| {
            format!(
                "<a class=\"{}\" href=\"{}\">{}</a>",
                if board == query.board { "active" } else { "" },
                tab_url(context, board.key()),
                board_label(context, board)
            )
        })
        .collect::<String>();
    format!(
        "<section class=\"page-head\"><div><h1>{title}</h1><p>{subtitle}</p></div></section>\
         <div class=\"toolbar\"><nav class=\"tabs\">{tabs}</nav>\
           <form class=\"search\" action=\"/launches\"><input type=\"hidden\" name=\"lang\" value=\"{lang}\"><input type=\"hidden\" name=\"chain_id\" value=\"{chain}\"><input type=\"hidden\" name=\"tab\" value=\"{tab}\">\
           <input name=\"q\" value=\"{q}\" placeholder=\"{placeholder}\" autocomplete=\"off\"><button>{search}</button></form></div>\
         <article class=\"card\" data-refresh=\"{refresh}\" data-every=\"15\">{table}</article>\
         <p class=\"note\">{note}</p>",
        title = l.text("Launches", "发币"),
        subtitle = l.text(
            "Pons launches ranked like an exchange. Prices are curve execution prices converted to USD.",
            "像交易所一样浏览 Pons 新币。价格为联合曲线成交价并换算为美元。"
        ),
        lang = l.code(),
        chain = context.chain_id,
        tab = query.board.key(),
        q = escape_html(query.search.unwrap_or("")),
        placeholder = l.text("Symbol or token address", "代币符号或合约地址"),
        search = l.text("Search", "搜索"),
        refresh = escape_html(&launch_table_url(context, query, None, "/fragments/launches")),
        note = l.text(
            "USD totals and the trending, volume and market-cap boards only count tokens quoted in ETH/WETH or a registry stablecoin. ≈ marks USD derived from swap medians (stock- or meme-token quotes): shown for context, never summed or ranked. Price change is measured in the quote asset, so it is unaffected. Market cap = price × total supply; graduated tokens include Uniswap V4 pool trades.",
            "美元总额与热门/成交额/市值榜只统计以 ETH/WETH 或注册表稳定币计价的代币。≈ 表示美元价由成交中位数推导（股票或 meme 代币报价），仅供参考，不计入总额、不参与排名。涨跌幅按报价资产计算，不受影响。市值 = 价格 × 总供应量；已毕业代币包含 Uniswap V4 池内成交。"
        ),
    )
}

pub fn fragment_launch_table(
    context: &PageContext<'_>,
    query: &LaunchTableQuery<'_>,
    page: &TokenPage,
) -> String {
    let l = context.locale;
    let header = |key: &str, label: &str| {
        let active = query.sort == Some(key);
        let descending = if active { !query.descending } else { true };
        let sort_query = LaunchTableQuery {
            board: query.board,
            sort: Some(key),
            descending,
            search: query.search,
        };
        format!(
            "<th><a class=\"{}\" href=\"{}\">{label}{}</a></th>",
            if active { "sorted" } else { "" },
            escape_html(&launch_table_url(context, &sort_query, None, "/launches")),
            if active {
                if query.descending { " ↓" } else { " ↑" }
            } else {
                ""
            }
        )
    };
    if page.rows.is_empty() {
        return format!(
            "<div class=\"empty\">{}</div>",
            l.text("No tokens match this view yet.", "当前视图暂无代币。")
        );
    }
    let offset = (page.page - 1) * page.page_size;
    let rows = page
        .rows
        .iter()
        .enumerate()
        .map(|(index, row)| token_table_row(context, row, offset as usize + index + 1))
        .collect::<String>();
    let pages = (page.total + page.page_size - 1) / page.page_size;
    let pager = if pages > 1 {
        let link = |target: i64, label: &str| {
            format!(
                "<a href=\"{}\">{label}</a>",
                escape_html(&launch_table_url(context, query, Some(target), "/launches"))
            )
        };
        format!(
            "<div class=\"pager\">{}<span>{} / {}</span>{}</div>",
            if page.page > 1 {
                link(page.page - 1, "←")
            } else {
                String::new()
            },
            page.page,
            pages,
            if page.page < pages {
                link(page.page + 1, "→")
            } else {
                String::new()
            },
        )
    } else {
        String::new()
    };
    format!(
        "<div class=\"table-wrap\"><table class=\"market\"><thead><tr><th class=\"l\">#</th><th class=\"l\">{token}</th>{price}{c5}{c1}{c24}{vol}{mcap}{buyers}<th>{progress}</th><th>24h</th>{age}</tr></thead><tbody>{rows}</tbody></table></div>{pager}",
        token = l.text("Token", "代币"),
        price = header("price", l.text("Price", "价格")),
        c5 = header("change_5m", "5m"),
        c1 = header("change_1h", "1h"),
        c24 = header("change_24h", "24h"),
        vol = header("volume", l.text("Volume 24h", "24h 成交额")),
        mcap = header("mcap", l.text("Market cap", "市值")),
        buyers = header("buyers", l.text("Buyers 24h", "24h 买家")),
        progress = l.text("Curve", "毕业进度"),
        age = header("age", l.text("Age", "上线")),
    )
}

fn token_table_row(context: &PageContext<'_>, row: &TokenRow, position: usize) -> String {
    let l = context.locale;
    let spark = row
        .sparkline_24h
        .as_deref()
        .map(|points| ui::sparkline(points, 84, 26))
        .unwrap_or_default();
    let badges = token_badges(context, row);
    format!(
        "<tr data-href=\"{href}\"><td class=\"l\">{rank}</td><td class=\"l\"><div class=\"tok\">{avatar}<div><strong>{symbol} {badges}</strong><small>{name} · {quote}</small></div></div></td>\
         <td data-k=\"p-{id}\">{price}</td><td>{c5}</td><td>{c1}</td><td>{c24}</td><td data-k=\"v-{id}\">{volume}</td><td>{mcap}</td><td>{buyers}</td><td>{progress}</td><td>{spark}</td><td class=\"muted\">{age}</td></tr>",
        href = context.url(&format!("/launches/{}", row.launch_id)),
        rank = ui::rank(position),
        avatar = ui::token_avatar(row.symbol.as_deref(), &row.token_address),
        symbol = escape_html(row.symbol.as_deref().unwrap_or("?")),
        name = escape_html(&truncate_chars(row.name.as_deref().unwrap_or(""), 22)),
        quote = escape_html(row.quote_symbol.as_deref().unwrap_or("ETH")),
        id = row.launch_id,
        price = graded_price(row),
        c5 = ui::change(row.change_5m_pct),
        c1 = ui::change(row.change_1h_pct),
        c24 = ui::change(row.change_24h_pct),
        volume = graded_usd(
            row,
            ui::usd(row.volume_24h_usd),
            row.volume_24h_usd.is_some()
        ),
        mcap = graded_usd(
            row,
            ui::usd(row.market_cap_usd),
            row.market_cap_usd.is_some()
        ),
        buyers = row
            .buyers_24h
            .map(|value| ui::count(value.into()))
            .unwrap_or_else(|| "—".to_owned()),
        progress = ui::progress(row.bonding_progress_pct),
        age = ui::age(row.launched_at, l),
    )
}

fn token_badges(context: &PageContext<'_>, row: &TokenRow) -> String {
    let l = context.locale;
    let mut badges = String::new();
    if row.graduated_at.is_some() {
        badges.push_str(&format!(
            "<span class=\"badge grad\">🎓 {}</span>",
            l.text("Graduated", "已毕业")
        ));
    } else if row.bonding_progress_pct.is_some_and(|value| value >= 80.0) {
        badges.push_str(&format!(
            "<span class=\"badge hot\">🔥 {}</span>",
            l.text("Near grad", "快毕业")
        ));
    }
    if row
        .launched_at
        .is_some_and(|value| Utc::now() - value < chrono::Duration::hours(1))
    {
        badges.push_str(&format!(
            "<span class=\"badge new\">{}</span>",
            l.text("NEW", "新")
        ));
    }
    badges
}

pub struct TokenDetail<'a> {
    pub token: &'a TokenRow,
    pub candles: &'a [CandleRow],
    pub interval: i32,
    pub trades: &'a [TradeRow],
}

pub fn token_detail(context: &PageContext<'_>, detail: &TokenDetail<'_>) -> String {
    let l = context.locale;
    let token = detail.token;
    let intervals = [(1, "1m"), (5, "5m"), (15, "15m"), (60, "1h")]
        .into_iter()
        .map(|(minutes, label)| {
            format!(
                "<a class=\"{}\" href=\"{}&interval={minutes}\">{label}</a>",
                if minutes == detail.interval {
                    "active"
                } else {
                    ""
                },
                context.url(&format!("/launches/{}", token.launch_id)),
            )
        })
        .collect::<String>();
    let kv = |label: &str, value: String| {
        format!("<div><span>{label}</span><strong class=\"num\">{value}</strong></div>")
    };
    let stats = [
        kv(l.text("Market cap", "市值"), ui::usd(token.market_cap_usd)),
        kv(
            l.text("ATH market cap", "历史最高市值"),
            ui::usd(token.ath_market_cap_usd),
        ),
        kv(
            l.text("Volume 24h", "24h 成交额"),
            ui::usd(token.volume_24h_usd),
        ),
        kv(
            l.text("Volume 1h", "1h 成交额"),
            ui::usd(token.volume_1h_usd),
        ),
        kv(
            l.text("Net flow 1h", "1h 净流入"),
            ui::signed_usd(token.net_flow_1h_usd),
        ),
        kv(
            l.text("Net flow 24h", "24h 净流入"),
            ui::signed_usd(token.net_flow_24h_usd),
        ),
        kv(
            l.text("Trades 24h", "24h 成交笔数"),
            format!(
                "{} / {}",
                opt_count(token.buys_24h),
                opt_count(token.sells_24h)
            ),
        ),
        kv(
            l.text("Buyers / sellers 24h", "24h 买家 / 卖家"),
            format!(
                "{} / {}",
                opt_count(token.buyers_24h),
                opt_count(token.sellers_24h)
            ),
        ),
        kv(
            l.text("Curve progress", "毕业进度"),
            ui::progress(token.bonding_progress_pct),
        ),
        kv(
            l.text("Launched", "上线时间"),
            format!("{} {}", ui::age(token.launched_at, l), l.text("ago", "前")),
        ),
    ]
    .concat();
    let changes = [
        ("5m", token.change_5m_pct),
        ("1h", token.change_1h_pct),
        ("6h", token.change_6h_pct),
        ("24h", token.change_24h_pct),
    ]
    .into_iter()
    .map(|(label, value)| format!("<span class=\"muted\">{label}</span> {}", ui::change(value)))
    .collect::<Vec<_>>()
    .join(" &nbsp; ");
    format!(
        "<a class=\"note\" href=\"{back}\">← {back_label}</a>\
         <section class=\"token-hero\"><div class=\"id\">{avatar}<div><h1>{symbol} {badges}</h1><div class=\"muted\">{name} · <code>{address}</code></div><div class=\"note\">{creator_label} <code>{creator}</code> · {quote_label} {quote}</div></div></div>\
           <div class=\"price\"><strong data-k=\"hero-price\">{price}</strong><div>{changes}</div></div></section>\
         <section class=\"grid2\"><article class=\"card\"><div class=\"card-head\"><h2>{chart_title}</h2><nav class=\"windows\">{intervals}</nav></div><div class=\"chart\">{chart}</div></article>\
           <article class=\"card\"><div class=\"card-head\"><h2>{stats_title}</h2>{trust}</div><div class=\"kv\">{stats}</div></article></section>\
         <article class=\"card\"><div class=\"card-head\"><h2>{trades_title}</h2><span class=\"muted\">{trades_note}</span></div><div data-refresh=\"{trades_url}\" data-every=\"10\">{trades}</div></article>",
        back = context.url("/launches"),
        back_label = l.text("All launches", "全部发币"),
        avatar = ui::token_avatar(token.symbol.as_deref(), &token.token_address),
        symbol = escape_html(token.symbol.as_deref().unwrap_or("?")),
        badges = token_badges(context, token),
        name = escape_html(token.name.as_deref().unwrap_or("")),
        address = escape_html(&token.token_address),
        creator_label = l.text("Creator", "创建者"),
        creator = token
            .creator_address
            .as_deref()
            .map(ui::short_address)
            .unwrap_or_else(|| "—".to_owned()),
        quote_label = l.text("Quote", "报价资产"),
        quote = escape_html(token.quote_symbol.as_deref().unwrap_or("ETH")),
        price = graded_price(token),
        changes = changes,
        chart_title = l.text("Price (USD)", "价格 (USD)"),
        intervals = intervals,
        chart = candle_chart(context, detail.candles),
        stats_title = l.text("Market stats", "市场数据"),
        trust = ui::trust(l, ui::Trust::Estimated),
        stats = stats,
        trades_title = l.text("Recent trades", "最新成交"),
        trades_note = l.text(
            "Curve trades and post-graduation pool swaps",
            "联合曲线成交与毕业后池内成交"
        ),
        trades_url = fragment_url(
            context,
            &format!("/fragments/launches/{}/trades", token.launch_id),
            ""
        ),
        trades = fragment_token_trades(context, detail.trades),
    )
}

pub fn fragment_token_trades(context: &PageContext<'_>, trades: &[TradeRow]) -> String {
    let l = context.locale;
    if trades.is_empty() {
        return format!(
            "<div class=\"empty\">{}</div>",
            l.text("No trades yet", "暂无成交")
        );
    }
    let rows = trades
        .iter()
        .map(|trade| {
            let buy = trade.trade_side == "buy";
            format!(
                "<tr><td class=\"l muted\">{age}</td><td class=\"l\"><span class=\"pill {class}\">{side}</span></td><td class=\"{class_text}\">{usd}</td><td>{quote}</td><td>{tokens}</td><td class=\"l\"><code>{trader}</code></td><td class=\"l\"><a class=\"muted\" href=\"{tx}\" target=\"_blank\" rel=\"noopener\">{hash} ↗</a></td></tr>",
                age = ui::age(trade.occurred_at, l),
                class = if buy { "up" } else { "down" },
                class_text = if buy { "up" } else { "down" },
                side = if buy { l.text("Buy", "买入") } else { l.text("Sell", "卖出") },
                usd = ui::usd(trade.usd_value),
                quote = format!("{} {}", ui::compact(trade.quote_amount.unwrap_or_default()), escape_html(trade.quote_symbol.as_deref().unwrap_or("ETH"))),
                tokens = ui::compact(trade.token_amount.unwrap_or_default()),
                trader = ui::short_address(&trade.trader_address),
                tx = explorer_tx_url(context.chain_id, &trade.tx_hash),
                hash = ui::short_address(&trade.tx_hash),
            )
        })
        .collect::<String>();
    format!(
        "<div class=\"table-wrap\"><table class=\"market\"><thead><tr><th class=\"l\">{}</th><th class=\"l\">{}</th><th>USD</th><th>{}</th><th>{}</th><th class=\"l\">{}</th><th class=\"l\">Tx</th></tr></thead><tbody>{rows}</tbody></table></div>",
        l.text("Age", "时间"),
        l.text("Side", "方向"),
        l.text("Quote", "报价数量"),
        l.text("Tokens", "代币数量"),
        l.text("Trader", "交易者"),
    )
}

/// Server-rendered candlestick chart with a volume strip.
pub fn candle_chart(context: &PageContext<'_>, candles: &[CandleRow]) -> String {
    let priced = candles
        .iter()
        .filter(|candle| candle.high.is_some() && candle.low.is_some())
        .collect::<Vec<_>>();
    if priced.len() < 2 {
        return format!(
            "<div class=\"empty\">{}</div>",
            context.locale.text(
                "Not enough priced trades for a chart yet",
                "暂无足够的已定价成交生成走势图"
            )
        );
    }
    const WIDTH: f64 = 1000.0;
    const HEIGHT: f64 = 280.0;
    const PRICE_HEIGHT: f64 = 210.0;
    const LABEL_WIDTH: f64 = 70.0;
    let high = priced
        .iter()
        .filter_map(|candle| candle.high)
        .fold(f64::MIN, f64::max);
    let low = priced
        .iter()
        .filter_map(|candle| candle.low)
        .fold(f64::MAX, f64::min);
    let span = (high - low).max(high * 1e-6).max(f64::MIN_POSITIVE);
    let volume_max = priced
        .iter()
        .filter_map(|candle| candle.volume_usd)
        .fold(0.0_f64, f64::max)
        .max(f64::MIN_POSITIVE);
    let slot = (WIDTH - LABEL_WIDTH) / priced.len() as f64;
    let body_width = (slot * 0.7).max(1.0);
    let y = |value: f64| 8.0 + (high - value) / span * (PRICE_HEIGHT - 16.0);
    let mut svg = format!(
        "<svg viewBox=\"0 0 {WIDTH} {HEIGHT}\" preserveAspectRatio=\"none\" role=\"img\"><g class=\"grid\">"
    );
    for step in 0..=3 {
        let value = low + span * f64::from(step) / 3.0;
        let line_y = y(value);
        svg.push_str(&format!(
            "<line x1=\"0\" x2=\"{:.1}\" y1=\"{line_y:.1}\" y2=\"{line_y:.1}\"/><text x=\"{:.1}\" y=\"{:.1}\">{}</text>",
            WIDTH - LABEL_WIDTH,
            WIDTH - LABEL_WIDTH + 6.0,
            line_y + 3.0,
            escape_html(&ui::price_usd(Some(value)))
        ));
    }
    svg.push_str("</g>");
    for (index, candle) in priced.iter().enumerate() {
        let x = index as f64 * slot + slot / 2.0;
        let (Some(high_value), Some(low_value)) = (candle.high, candle.low) else {
            continue;
        };
        let open = candle.open.unwrap_or(low_value);
        let close = candle.close.unwrap_or(open);
        let class = if close >= open { "up" } else { "down" };
        let top = y(open.max(close));
        let bottom = y(open.min(close));
        svg.push_str(&format!(
            "<g class=\"candle {class}\"><line x1=\"{x:.1}\" x2=\"{x:.1}\" y1=\"{:.1}\" y2=\"{:.1}\"/><rect x=\"{:.1}\" y=\"{top:.1}\" width=\"{body_width:.1}\" height=\"{:.1}\"/></g>",
            y(high_value),
            y(low_value),
            x - body_width / 2.0,
            (bottom - top).max(1.0),
        ));
        if let Some(volume) = candle.volume_usd {
            let bar = volume / volume_max * (HEIGHT - PRICE_HEIGHT - 16.0);
            svg.push_str(&format!(
                "<rect class=\"vol\" x=\"{:.1}\" y=\"{:.1}\" width=\"{body_width:.1}\" height=\"{bar:.1}\"/>",
                x - body_width / 2.0,
                HEIGHT - bar - 12.0,
            ));
        }
    }
    let label = |candle: &CandleRow| candle.bucket_start.format("%m-%d %H:%M").to_string();
    svg.push_str(&format!(
        "<text x=\"2\" y=\"{:.1}\">{}</text><text x=\"{:.1}\" y=\"{:.1}\" text-anchor=\"end\">{}</text></svg>",
        HEIGHT - 1.0,
        label(priced[0]),
        WIDTH - LABEL_WIDTH,
        HEIGHT - 1.0,
        label(priced[priced.len() - 1]),
    ));
    svg
}

fn board_label(context: &PageContext<'_>, board: TokenBoard) -> &'static str {
    let l = context.locale;
    match board {
        TokenBoard::Hot => l.text("🔥 Trending", "🔥 热门"),
        TokenBoard::Gainers => l.text("🚀 Gainers", "🚀 涨幅"),
        TokenBoard::Losers => l.text("📉 Losers", "📉 跌幅"),
        TokenBoard::Volume => l.text("💰 Volume", "💰 成交额"),
        TokenBoard::New => l.text("🆕 New", "🆕 新币"),
        TokenBoard::Graduating => l.text("🎓 Graduating", "🎓 即将毕业"),
        TokenBoard::Graduated => l.text("🏁 Graduated", "🏁 已毕业"),
        TokenBoard::MarketCap => l.text("🏆 Market cap", "🏆 市值"),
    }
}

fn tab_url(context: &PageContext<'_>, tab: &str) -> String {
    format!(
        "/launches?lang={}&chain_id={}&tab={tab}",
        context.locale.code(),
        context.chain_id
    )
}

fn launch_table_url(
    context: &PageContext<'_>,
    query: &LaunchTableQuery<'_>,
    page: Option<i64>,
    path: &str,
) -> String {
    let mut url = format!(
        "{path}?lang={}&chain_id={}&tab={}",
        context.locale.code(),
        context.chain_id,
        query.board.key()
    );
    if let Some(sort) = query.sort {
        url.push_str(&format!(
            "&sort={sort}&dir={}",
            if query.descending { "desc" } else { "asc" }
        ));
    }
    if let Some(search) = query.search.filter(|value| !value.is_empty()) {
        url.push_str(&format!("&q={}", urlencoding::encode(search)));
    }
    if let Some(page) = page {
        url.push_str(&format!("&page={page}"));
    }
    url
}

pub fn fragment_url(context: &PageContext<'_>, path: &str, extra: &str) -> String {
    format!(
        "{path}?lang={}&chain_id={}{extra}",
        context.locale.code(),
        context.chain_id
    )
}

fn with_params(context: &PageContext<'_>, path: &str) -> String {
    let separator = if path.contains('?') { '&' } else { '?' };
    format!(
        "{path}{separator}lang={}&chain_id={}",
        context.locale.code(),
        context.chain_id
    )
}

/// USD price, prefixed with `≈` when the quote asset's USD price is derived
/// from swap medians rather than pool state or a stablecoin anchor.
fn graded_price(row: &TokenRow) -> String {
    graded_usd(row, ui::price_usd(row.price_usd), row.price_usd.is_some())
}

/// Same marker for any other USD figure of the token.
fn graded_usd(row: &TokenRow, rendered: String, has_value: bool) -> String {
    if row.price_grade.as_deref() == Some("derived") && has_value {
        format!(
            "<span title=\"USD derived from swap medians; not summed into totals\">≈{rendered}</span>"
        )
    } else {
        rendered
    }
}

fn opt_count(value: Option<i32>) -> String {
    value
        .map(|value| ui::count(value.into()))
        .unwrap_or_else(|| "—".to_owned())
}

fn truncate_chars(value: &str, keep: usize) -> String {
    if value.chars().count() <= keep {
        return value.to_owned();
    }
    format!("{}…", value.chars().take(keep).collect::<String>())
}

fn strip_tags(html: &str) -> String {
    let mut out = String::new();
    let mut inside = false;
    for character in html.chars() {
        match character {
            '<' => inside = true,
            '>' => inside = false,
            other if !inside => out.push(other),
            _ => {}
        }
    }
    out
}

/// Minimum USD value for the whale trade feed.
pub const WHALE_TRADE_MIN_USD: f64 = 100.0;
