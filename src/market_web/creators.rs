//! Creator track records.
//!
//! The most useful thing a buyer can know about a new launch is what the same
//! person's previous launches did. The aggregate shape of the chain is public,
//! because it is what makes the case for looking; the records themselves are
//! core content behind the wallet gate.

use crate::services::market_read::{CreatorBoard, CreatorCoverage, CreatorProfileRow, TokenRow};

use super::{
    PageContext, access, access::Access, access::Tier, escape_html, intel::address_url, pages, ui,
};

pub struct BoardPage<'a> {
    pub board: CreatorBoard,
    pub coverage: &'a CreatorCoverage,
    pub creators: &'a [CreatorProfileRow],
    pub access: Access,
}

pub struct ProfilePage<'a> {
    pub address: &'a str,
    pub profile: Option<&'a CreatorProfileRow>,
    pub launches: &'a [TokenRow],
    pub access: Access,
}

fn board_label(context: &PageContext<'_>, board: CreatorBoard) -> &'static str {
    let l = context.locale;
    match board {
        CreatorBoard::Damage => l.text("Most buyer losses", "买家亏损最多"),
        CreatorBoard::Prolific => l.text("Most launches", "发币最多"),
        CreatorBoard::NeverGraduated => l.text("Never graduated", "从未毕业"),
        CreatorBoard::Builders => l.text("Actually shipped", "真正做出毕业币"),
    }
}

/// Win rate of a creator's buyers, phrased as the share who lost.
fn loss_share(profile: &CreatorProfileRow) -> String {
    if profile.buyers <= 0 {
        return "—".to_owned();
    }
    format!(
        "{:.0}%",
        profile.losing_buyers as f64 / profile.buyers as f64 * 100.0
    )
}

fn graduation(profile: &CreatorProfileRow) -> String {
    match profile.graduation_rate {
        Some(rate) => format!("{rate:.1}%"),
        None => "—".to_owned(),
    }
}

pub fn board(context: &PageContext<'_>, page: &BoardPage<'_>) -> String {
    let l = context.locale;
    let coverage = page.coverage;
    let tabs = CreatorBoard::ALL
        .into_iter()
        .map(|option| {
            format!(
                "<a class=\"{}\" href=\"{}&board={}\">{}</a>",
                if option == page.board { "active" } else { "" },
                context.url("/creators"),
                option.key(),
                board_label(context, option),
            )
        })
        .collect::<String>();

    let stat = |label: &str, value: String, note: String| {
        format!(
            "<div class=\"stat\"><span>{label}</span><strong class=\"num\">{value}</strong><small>{note}</small></div>"
        )
    };
    // The shape of the chain is public: it is the reason to look at all.
    let stats = [
        stat(
            l.text("Creators tracked", "已追踪创建者"),
            ui::count(coverage.creators),
            l.text_owned(
                format!("{} launches", ui::count(coverage.launches)),
                format!("{} 个发币", ui::count(coverage.launches)),
            ),
        ),
        stat(
            l.text("Serial creators", "连环发币者"),
            ui::count(coverage.serial_creators),
            l.text_owned(
                format!("{} launches, 10+ each", ui::count(coverage.serial_launches)),
                format!(
                    "合计 {} 个币，每人 ≥10",
                    ui::count(coverage.serial_launches)
                ),
            ),
        ),
        stat(
            l.text("Never graduated one", "一个都没毕业过"),
            ui::count(coverage.never_graduated_creators),
            l.text_owned(
                format!(
                    "{} launches between them",
                    ui::count(coverage.never_graduated_launches)
                ),
                format!(
                    "他们发了 {} 个币",
                    ui::count(coverage.never_graduated_launches)
                ),
            ),
        ),
        stat(
            l.text("Resolved to a sender", "已还原到发起地址"),
            format!(
                "{:.0}%",
                if coverage.creators > 0 {
                    coverage.by_origin as f64 / coverage.creators as f64 * 100.0
                } else {
                    0.0
                }
            ),
            l.text("rest still on msg.sender", "其余仍按 msg.sender 归属")
                .to_owned(),
        ),
    ]
    .concat();

    // One row is shown to everyone: proof the record is real, and the reason to
    // want the rest.
    let sample = page
        .creators
        .first()
        .map(|profile| table(context, std::slice::from_ref(profile)))
        .unwrap_or_default();
    let full = if page.creators.is_empty() {
        format!(
            "<div class=\"empty\">{}</div>",
            l.text("Profiles are being computed", "创建者档案计算中")
        )
    } else {
        table(context, page.creators)
    };
    let body = access::gate(context, page.access, Tier::Connected, &sample, &full);

    format!(
        "<section class=\"page-head\"><div><h1>{title}</h1><p>{subtitle}</p></div></section>\
         <section class=\"stats\">{stats}</section>\
         <div class=\"toolbar\"><nav class=\"tabs\">{tabs}</nav></div>\
         <article class=\"card\">{body}</article>\
         <p class=\"note\">{method}</p>",
        title = l.text("Who launches these tokens", "是谁在发这些币"),
        subtitle = l.text(
            "Every launch traced back to the account that sent the transaction, and what that account's launches did next.",
            "每个币都还原到实际发起交易的地址，并统计该地址此前发的币后来怎么样了。"
        ),
        method = l.text(
            "The launch event reports msg.sender, which is a contract whenever the creator went through a router or multicall, so the transaction sender is resolved separately and shown as the creator. Buyer profit and loss covers curve trades only, by average cost, excluding gas.",
            "发币事件记录的是 msg.sender，创建者若经由路由或 multicall 发币则是合约地址，因此这里单独还原了交易发起地址作为创建者。买家盈亏只覆盖曲线成交，平均成本法，不含 gas。"
        ),
    )
}

fn table(context: &PageContext<'_>, creators: &[CreatorProfileRow]) -> String {
    let l = context.locale;
    let rows = creators
        .iter()
        .enumerate()
        .map(|(index, profile)| {
            format!(
                "<tr data-href=\"{href}\"><td class=\"l\">{rank}</td><td class=\"l mono\">{address}{badge}</td>\
                 <td>{launches}</td><td>{graduated}</td><td>{rate}</td><td>{dead}</td>\
                 <td>{buyers}</td><td>{loss_share}</td><td class=\"{pnl_class}\"><strong>{pnl}</strong></td>\
                 <td class=\"muted\">{last}</td></tr>",
                href = context.url(&format!("/creators/{}", profile.creator_address)),
                rank = ui::rank(index + 1),
                address = ui::short_address(&profile.creator_address),
                badge = if profile.attribution == "origin" {
                    String::new()
                } else {
                    format!(
                        " <span class=\"badge\" title=\"{}\">msg.sender</span>",
                        escape_html(l.text(
                            "Transaction sender not resolved yet",
                            "尚未还原交易发起地址"
                        ))
                    )
                },
                launches = ui::count(profile.launches.into()),
                graduated = ui::count(profile.graduated.into()),
                rate = graduation(profile),
                dead = ui::count(profile.dead_launches.into()),
                buyers = ui::count(profile.buyers.into()),
                loss_share = loss_share(profile),
                pnl_class = match profile.buyer_realized_usd {
                    Some(value) if value > 0.0 => "up",
                    Some(value) if value < 0.0 => "down",
                    _ => "muted",
                },
                pnl = ui::signed_usd(profile.buyer_realized_usd),
                last = ui::age(profile.last_launch_at, l),
            )
        })
        .collect::<String>();
    format!(
        "<div class=\"table-wrap\"><table class=\"market\"><thead><tr><th class=\"l\">#</th><th class=\"l\">{}</th>\
         <th>{}</th><th>{}</th><th>{}</th><th>{}</th><th>{}</th><th>{}</th><th>{}</th><th>{}</th></tr></thead>\
         <tbody>{rows}</tbody></table></div>",
        l.text("Creator", "创建者"),
        l.text("Launches", "发币数"),
        l.text("Graduated", "毕业数"),
        l.text("Rate", "毕业率"),
        l.text("Went quiet", "已归零"),
        l.text("Buyers", "买家"),
        l.text("Lost money", "亏损占比"),
        l.text("Buyer P/L", "买家盈亏"),
        l.text("Last launch", "最近发币"),
    )
}

pub fn profile(context: &PageContext<'_>, page: &ProfilePage<'_>) -> String {
    let l = context.locale;
    let Some(profile) = page.profile else {
        return format!(
            "<a class=\"note\" href=\"{back}\">← {back_label}</a>\
             <div class=\"empty\">{empty}</div>",
            back = context.url("/creators"),
            back_label = l.text("All creators", "全部创建者"),
            empty = l.text(
                "No launches recorded for this address.",
                "该地址没有发币记录。"
            ),
        );
    };

    let kv = |label: &str, value: String| {
        format!("<div><span>{label}</span><strong class=\"num\">{value}</strong></div>")
    };
    let record = [
        kv(
            l.text("Graduated", "毕业"),
            format!(
                "{} / {}",
                ui::count(profile.graduated.into()),
                ui::count(profile.launches.into())
            ),
        ),
        kv(l.text("Graduation rate", "毕业率"), graduation(profile)),
        kv(
            l.text("Went quiet", "发完就没成交了"),
            ui::count(profile.dead_launches.into()),
        ),
        kv(
            l.text("Buyers", "买家人数"),
            ui::count(profile.buyers.into()),
        ),
        kv(l.text("Lost money", "亏损买家占比"), loss_share(profile)),
        kv(
            l.text("Buyer profit and loss", "买家已实现盈亏"),
            format!(
                "<span class=\"{}\">{}</span>",
                match profile.buyer_realized_usd {
                    Some(value) if value >= 0.0 => "up",
                    Some(_) => "down",
                    None => "muted",
                },
                ui::signed_usd(profile.buyer_realized_usd)
            ),
        ),
        kv(
            l.text("Best market cap reached", "历史最高市值"),
            ui::usd(profile.best_ath_mcap_usd),
        ),
        kv(
            l.text("Median market cap", "市值中位数"),
            ui::usd(profile.median_ath_mcap_usd),
        ),
    ]
    .concat();

    let launches = if page.launches.is_empty() {
        format!(
            "<div class=\"empty\">{}</div>",
            l.text("No indexed trades", "暂无已索引的成交")
        )
    } else {
        pages::token_rows(context, page.launches)
    };

    let teaser = format!(
        "<article class=\"card\" style=\"margin-bottom:12px\"><div class=\"kv\">{}</div></article>",
        [
            kv(
                l.text("Launches", "发币数"),
                ui::count(profile.launches.into())
            ),
            kv(
                l.text("Last launch", "最近发币"),
                ui::age(profile.last_launch_at, l)
            ),
            kv(
                l.text("First seen", "首次发币"),
                ui::age(profile.first_launch_at, l)
            ),
        ]
        .concat()
    );
    let full = format!(
        "<article class=\"card\" style=\"margin-bottom:12px\"><div class=\"kv\">{record}</div></article>\
         <article class=\"card\"><div class=\"card-head\"><h2>{launches_title}</h2><span class=\"muted\">{launches_note}</span></div>{launches}</article>",
        launches_title = l.text("Their launches", "该地址发过的币"),
        launches_note = l.text("newest first", "按时间倒序"),
    );

    format!(
        "<a class=\"note\" href=\"{back}\">← {back_label}</a>\
         <section class=\"token-hero\"><div class=\"id\"><span class=\"avatar\" style=\"--h:{hue}\">🏭</span>\
           <div><h1 class=\"mono\" style=\"font-size:20px\">{address}</h1><div class=\"muted\">{attribution}</div></div></div>\
           <div class=\"price\"><strong>{launch_count}</strong><div class=\"muted\">{launch_label}</div></div></section>\
         {body}",
        back = context.url("/creators"),
        back_label = l.text("All creators", "全部创建者"),
        hue = page.address.bytes().last().unwrap_or(0) as u32 * 360 / 256,
        address = escape_html(page.address),
        attribution = if profile.attribution == "origin" {
            l.text(
                "Resolved from the launch transaction sender",
                "已还原为发币交易的发起地址",
            )
        } else {
            l.text(
                "Reported by the protocol as msg.sender",
                "协议事件记录的 msg.sender",
            )
        },
        launch_count = ui::count(profile.launches.into()),
        launch_label = l.text("launches", "个发币"),
        body = access::gate(context, page.access, Tier::Connected, &teaser, &full),
    )
}

/// The creator record shown on a launch page. The launch count is public — it
/// is the hook — and the record behind it is not.
pub fn launch_strip(
    context: &PageContext<'_>,
    creator: Option<&CreatorProfileRow>,
    access: Access,
) -> String {
    let l = context.locale;
    let Some(profile) = creator else {
        return String::new();
    };
    if profile.launches <= 1 {
        return String::new();
    }
    let href = context.url(&format!("/creators/{}", profile.creator_address));
    if access.allows(Tier::Connected) {
        let tone = if profile.graduated == 0 { "down" } else { "" };
        return format!(
            r#"<a class="creator-strip {tone}" href="{href}"><span class="creator-label">{label}</span>
<span><strong>{launches}</strong> {launches_label}</span>
<span><strong>{graduated}</strong> {graduated_label}</span>
<span><strong>{dead}</strong> {dead_label}</span>
<span class="{pnl_class}"><strong>{pnl}</strong> {pnl_label}</span>
<span class="more">{more} →</span></a>"#,
            label = escape_html(l.text("This creator", "该创建者")),
            launches = ui::count(profile.launches.into()),
            launches_label = escape_html(l.text("launches", "个币")),
            graduated = ui::count(profile.graduated.into()),
            graduated_label = escape_html(l.text("graduated", "个毕业")),
            dead = ui::count(profile.dead_launches.into()),
            dead_label = escape_html(l.text("went quiet", "个归零")),
            pnl_class = match profile.buyer_realized_usd {
                Some(value) if value > 0.0 => "up",
                Some(value) if value < 0.0 => "down",
                _ => "muted",
            },
            pnl = ui::signed_usd(profile.buyer_realized_usd),
            pnl_label = escape_html(l.text("for their buyers", "买家盈亏")),
            more = escape_html(l.text("Full record", "完整档案")),
        );
    }
    format!(
        r#"<a class="creator-strip locked" href="{href}"><span class="creator-label">{label}</span>
<span><strong>{launches}</strong> {launches_label}</span>
<span class="muted">{prompt}</span>
<span class="more">🔒 {action} →</span></a>"#,
        label = escape_html(l.text("This creator", "该创建者")),
        launches = ui::count(profile.launches.into()),
        launches_label = escape_html(l.text("launches", "个币")),
        prompt = escape_html(l.text(
            "Graduation rate, dead launches and what their buyers made",
            "毕业率、归零数量与买家盈亏"
        )),
        action = escape_html(l.text("Connect to see", "连接钱包查看")),
    )
}
