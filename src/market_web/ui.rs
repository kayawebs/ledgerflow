//! Presentation helpers for the market web: number formatting, small visual
//! components and the shared exchange-style stylesheet and client script.

use chrono::{DateTime, Utc};

use super::{Locale, escape_html};

/// Formats a USD amount with compact suffixes: `$1.23K`, `$4.5M`, `$0.42`.
pub fn usd(value: Option<f64>) -> String {
    let Some(value) = value.filter(|value| value.is_finite()) else {
        return "—".to_owned();
    };
    let sign = if value < 0.0 { "-" } else { "" };
    format!("{sign}${}", compact(value.abs()))
}

/// Formats a signed USD amount with an explicit `+` for gains.
pub fn signed_usd(value: Option<f64>) -> String {
    match value.filter(|value| value.is_finite()) {
        Some(value) if value > 0.0 => format!("+{}", usd(Some(value))),
        other => usd(other),
    }
}

/// Compact magnitude without currency: `950`, `1.2K`, `3.40M`, `1.1B`.
pub fn compact(value: f64) -> String {
    let abs = value.abs();
    let (scaled, suffix) = if abs >= 1e9 {
        (value / 1e9, "B")
    } else if abs >= 1e6 {
        (value / 1e6, "M")
    } else if abs >= 1e3 {
        (value / 1e3, "K")
    } else {
        (value, "")
    };
    let scaled_abs = scaled.abs();
    let digits = if suffix.is_empty() {
        if scaled_abs >= 100.0 {
            0
        } else if scaled_abs >= 1.0 {
            2
        } else if scaled_abs == 0.0 {
            0
        } else {
            return small_decimal(scaled);
        }
    } else if scaled_abs >= 100.0 {
        0
    } else if scaled_abs >= 10.0 {
        1
    } else {
        2
    };
    format!("{scaled:.digits$}{suffix}")
}

/// Renders small numbers with a zero-count subscript, the convention used by
/// launch terminals: `0.00000412` becomes `0.0₅412`.
pub fn small_decimal(value: f64) -> String {
    if value == 0.0 {
        return "0".to_owned();
    }
    let sign = if value < 0.0 { "-" } else { "" };
    let abs = value.abs();
    if abs >= 0.01 {
        return format!("{sign}{abs:.4}");
    }
    let zeros = (-abs.log10()).floor() as i32;
    let significant = abs * 10f64.powi(zeros + 3);
    let digits = format!("{:.0}", significant.round())
        .trim_end_matches('0')
        .to_owned();
    let digits = if digits.is_empty() {
        "0".to_owned()
    } else {
        digits
    };
    if zeros <= 3 {
        return format!("{sign}0.{}{}", "0".repeat(zeros as usize), digits);
    }
    format!("{sign}0.0{}{}", subscript(zeros as u32), digits)
}

/// USD price with adaptive precision.
pub fn price_usd(value: Option<f64>) -> String {
    match value.filter(|value| value.is_finite()) {
        None => "—".to_owned(),
        Some(value) if value.abs() >= 1.0 => format!("${}", compact(value)),
        Some(value) => format!("${}", small_decimal(value)),
    }
}

fn subscript(value: u32) -> String {
    const DIGITS: [char; 10] = ['₀', '₁', '₂', '₃', '₄', '₅', '₆', '₇', '₈', '₉'];
    value
        .to_string()
        .chars()
        .filter_map(|digit| digit.to_digit(10).map(|digit| DIGITS[digit as usize]))
        .collect()
}

/// Percentage change as a colored badge-ready span.
pub fn change(value: Option<f64>) -> String {
    match value.filter(|value| value.is_finite()) {
        None => "<span class=\"chg flat\">—</span>".to_owned(),
        Some(value) => {
            let class = if value > 0.0005 {
                "up"
            } else if value < -0.0005 {
                "down"
            } else {
                "flat"
            };
            let arrow = match class {
                "up" => "▲",
                "down" => "▼",
                _ => "",
            };
            format!(
                "<span class=\"chg {class}\">{arrow}{}</span>",
                percent_text(value.abs())
            )
        }
    }
}

fn percent_text(value: f64) -> String {
    if value >= 10_000.0 {
        format!("{}%", compact(value))
    } else if value >= 100.0 {
        format!("{value:.0}%")
    } else {
        format!("{value:.1}%")
    }
}

/// Integer with thousands separators.
pub fn count(value: i64) -> String {
    let digits = value.unsigned_abs().to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            out.push(',');
        }
        out.push(digit);
    }
    if value < 0 { format!("-{out}") } else { out }
}

/// Relative age such as `12s`, `5m`, `3h`, `2d`.
pub fn age(value: Option<DateTime<Utc>>, locale: Locale) -> String {
    let Some(value) = value else {
        return "—".to_owned();
    };
    let seconds = (Utc::now() - value).num_seconds().max(0);
    let (amount, en, zh) = if seconds < 60 {
        (seconds, "s", "秒")
    } else if seconds < 3_600 {
        (seconds / 60, "m", "分")
    } else if seconds < 86_400 {
        (seconds / 3_600, "h", "小时")
    } else {
        (seconds / 86_400, "d", "天")
    };
    format!("{amount}{}", locale.text(en, zh))
}

pub fn short_address(address: &str) -> String {
    if address.len() <= 12 {
        return address.to_owned();
    }
    format!("{}…{}", &address[..6], &address[address.len() - 4..])
}

/// Deterministic avatar colour + initials for a token.
pub fn token_avatar(symbol: Option<&str>, address: &str) -> String {
    let initials = symbol
        .unwrap_or("?")
        .chars()
        .filter(|character| character.is_alphanumeric())
        .take(2)
        .collect::<String>()
        .to_uppercase();
    let hue = address.bytes().fold(0_u32, |acc, byte| {
        acc.wrapping_mul(31).wrapping_add(byte as u32)
    }) % 360;
    format!(
        "<span class=\"avatar\" style=\"--h:{hue}\">{}</span>",
        escape_html(if initials.is_empty() { "?" } else { &initials })
    )
}

/// Inline SVG sparkline. Missing points are skipped; the stroke colour
/// follows the first-to-last direction.
pub fn sparkline(points: &[Option<f64>], width: u32, height: u32) -> String {
    let values = points
        .iter()
        .enumerate()
        .filter_map(|(index, value)| {
            value
                .filter(|value| value.is_finite())
                .map(|value| (index, value))
        })
        .collect::<Vec<_>>();
    if values.len() < 2 {
        return format!("<svg class=\"spark\" width=\"{width}\" height=\"{height}\"></svg>");
    }
    let min = values
        .iter()
        .map(|(_, value)| *value)
        .fold(f64::INFINITY, f64::min);
    let max = values
        .iter()
        .map(|(_, value)| *value)
        .fold(f64::NEG_INFINITY, f64::max);
    let span = (max - min).max(max.abs() * 1e-9).max(f64::MIN_POSITIVE);
    let last_index = (points.len().max(2) - 1) as f64;
    let coordinates = values
        .iter()
        .map(|(index, value)| {
            let x = *index as f64 / last_index * f64::from(width - 2) + 1.0;
            let y = f64::from(height - 2) - (value - min) / span * f64::from(height - 4) + 1.0;
            format!("{x:.1},{y:.1}")
        })
        .collect::<Vec<_>>()
        .join(" ");
    let rising = values.last().map(|(_, value)| *value) >= values.first().map(|(_, value)| *value);
    format!(
        "<svg class=\"spark {}\" width=\"{width}\" height=\"{height}\" viewBox=\"0 0 {width} {height}\" preserveAspectRatio=\"none\"><polyline points=\"{coordinates}\"/></svg>",
        if rising { "up" } else { "down" }
    )
}

/// Bonding-curve progress bar.
pub fn progress(value: Option<f64>) -> String {
    match value.filter(|value| value.is_finite()) {
        None => "<span class=\"muted\">—</span>".to_owned(),
        Some(value) => {
            let clamped = value.clamp(0.0, 100.0);
            let class = if clamped >= 100.0 {
                "done"
            } else if clamped >= 80.0 {
                "hot"
            } else {
                ""
            };
            format!(
                "<span class=\"progress {class}\"><i style=\"width:{clamped:.1}%\"></i><b>{clamped:.0}%</b></span>"
            )
        }
    }
}

/// Rank marker with medals for the podium.
pub fn rank(position: usize) -> String {
    match position {
        1 => "<span class=\"rank gold\">1</span>".to_owned(),
        2 => "<span class=\"rank silver\">2</span>".to_owned(),
        3 => "<span class=\"rank bronze\">3</span>".to_owned(),
        other => format!("<span class=\"rank\">{other}</span>"),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Trust {
    Verified,
    Estimated,
    Unknown,
}

/// Trust label attached to every monetary figure.
pub fn trust(locale: Locale, level: Trust) -> String {
    let (class, en, zh, title_en, title_zh) = match level {
        Trust::Verified => (
            "verified",
            "Verified",
            "已验证",
            "Deterministically reconciled from receipts, traces and balances",
            "基于 receipt、trace 与余额确定性对账",
        ),
        Trust::Estimated => (
            "estimated",
            "Est.",
            "估算",
            "Computed from on-chain trades with a documented, limited scope",
            "基于链上成交计算，范围有限，详见方法说明",
        ),
        Trust::Unknown => (
            "unknown",
            "Unknown",
            "未知",
            "Required price or input is unavailable",
            "缺少价格或必要输入",
        ),
    };
    format!(
        "<span class=\"trust {class}\" title=\"{}\">{}</span>",
        locale.text(title_en, title_zh),
        locale.text(en, zh)
    )
}

pub const STYLES: &str = r#"
:root{color-scheme:dark;--bg:#07080a;--panel:#0f1115;--panel2:#141821;--line:#20242d;--line2:#2b303b;--text:#eef0f4;--muted:#8b93a3;--dim:#5d6473;--orange:#ff6a00;--orange2:#ff9447;--up:#1fd286;--up-bg:rgba(31,210,134,.12);--down:#ff5c6c;--down-bg:rgba(255,92,108,.12);--gold:#f5c542;--violet:#9d8cff}
*{box-sizing:border-box}html{font:14px/1.45 Inter,ui-sans-serif,system-ui,-apple-system,"Segoe UI","PingFang SC","Microsoft YaHei",sans-serif;background:var(--bg);color:var(--text);-webkit-text-size-adjust:100%}body{margin:0;background:radial-gradient(1200px 500px at 90% -10%,rgba(255,106,0,.08),transparent 60%),var(--bg)}a{color:inherit;text-decoration:none}button,select,input{font:inherit}code,.mono{font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:12px}
.num{font-variant-numeric:tabular-nums}.muted{color:var(--muted)}.dim{color:var(--dim)}.up{color:var(--up)}.down{color:var(--down)}
header.top{position:sticky;top:env(safe-area-inset-top,0px);z-index:20;background:rgba(7,8,10,.92);backdrop-filter:blur(14px);border-bottom:1px solid var(--line)}
.bar{display:flex;align-items:center;gap:18px;height:60px;padding:0 20px;max-width:1600px;margin:0 auto}.brand{display:flex;align-items:center;gap:9px;flex:none}.brand>span{width:30px;height:30px}.brand svg{width:100%;height:100%}.brand strong{font-size:16px;letter-spacing:-.02em}.brand small{display:block;font-size:12px;color:var(--muted);letter-spacing:.12em}
nav.main{display:flex;gap:2px;overflow-x:auto;scrollbar-width:none;flex:1}nav.main::-webkit-scrollbar{display:none}nav.main a{padding:8px 12px;border-radius:8px;color:var(--muted);font-weight:600;white-space:nowrap}nav.main a:hover{color:var(--text)}nav.main a.active{color:var(--text);background:var(--panel2)}nav.main a .new{font-size:12px;color:var(--orange);margin-left:4px;vertical-align:super}
.tools{display:flex;align-items:center;gap:8px;flex:none}.tools select,.tools .lang,.wallet{height:34px;border:1px solid var(--line2);border-radius:8px;background:var(--panel);color:var(--text);padding:0 10px;font-size:13px;display:inline-flex;align-items:center}.wallet{background:var(--orange);border-color:var(--orange);color:#fff;font-weight:700;cursor:pointer}.wallet.connected{background:rgba(31,210,134,.1);color:var(--up);border-color:rgba(31,210,134,.4)}
.ticker{border-top:1px solid var(--line);overflow:hidden;white-space:nowrap;font-size:12px;color:var(--muted)}.ticker>div{display:flex;gap:28px;padding:7px 20px;max-width:1600px;margin:0 auto;overflow-x:auto;scrollbar-width:none}.ticker>div::-webkit-scrollbar{display:none}.ticker span b{color:var(--text);margin-left:6px;font-weight:600}.ticker .live{color:var(--up)}.ticker .live::before{content:"";display:inline-block;width:6px;height:6px;border-radius:50%;background:var(--up);margin-right:6px;animation:pulse 1.6s infinite}
@keyframes pulse{0%{box-shadow:0 0 0 0 rgba(31,210,134,.6)}70%{box-shadow:0 0 0 6px rgba(31,210,134,0)}100%{box-shadow:0 0 0 0 rgba(31,210,134,0)}}
main{max-width:1600px;margin:0 auto;padding:18px 20px 60px}
.page-head{display:flex;align-items:flex-end;justify-content:space-between;gap:16px;flex-wrap:wrap;margin:6px 0 16px}.page-head h1{font-size:26px;margin:0;letter-spacing:-.03em}.page-head p{margin:4px 0 0;color:var(--muted);font-size:13px}
.windows{display:flex;gap:4px;background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:3px}.windows a{padding:5px 10px;border-radius:7px;color:var(--muted);font-size:12px;font-weight:600}.windows a.active{background:var(--panel2);color:var(--text)}
.stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:10px;margin-bottom:16px}.stat{background:var(--panel);border:1px solid var(--line);border-radius:12px;padding:14px 16px}.stat>span{display:block;color:var(--muted);font-size:12px}.stat>strong{display:block;font-size:24px;letter-spacing:-.02em;margin-top:4px}.stat>small{display:block;margin-top:2px;font-size:12px;color:var(--muted)}.stat.glow{border-color:rgba(255,106,0,.45);background:linear-gradient(180deg,rgba(255,106,0,.08),var(--panel) 70%)}
.boards{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px;margin-bottom:16px}
.card{background:var(--panel);border:1px solid var(--line);border-radius:14px;overflow:hidden}.card-head{display:flex;align-items:center;justify-content:space-between;gap:10px;padding:12px 14px;border-bottom:1px solid var(--line)}.card-head h2,.card-head h3{margin:0;font-size:14px;display:flex;align-items:center;gap:6px}.card-head a{color:var(--muted);font-size:12px}.card-head a:hover{color:var(--orange2)}.card-body{padding:4px 0}
.row{display:flex;align-items:center;gap:10px;padding:8px 14px;min-height:50px}.row:hover{background:var(--panel2)}.row .name{min-width:0;flex:1}.row .name strong{display:block;font-size:13px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.row .name small{display:block;color:var(--dim);font-size:12px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.row .val{text-align:right;flex:none}.row .val strong{display:block;font-size:13px}.row .val small{display:block;font-size:12px;color:var(--muted)}
.avatar{--h:20;display:inline-grid;place-items:center;flex:none;width:32px;height:32px;border-radius:50%;background:hsl(var(--h) 55% 22%);color:hsl(var(--h) 85% 72%);font-weight:800;font-size:12px;letter-spacing:-.02em}
.rank{display:inline-grid;place-items:center;flex:none;min-width:22px;height:22px;border-radius:6px;color:var(--muted);font-size:12px;font-weight:700}.rank.gold{background:rgba(245,197,66,.16);color:var(--gold)}.rank.silver{background:rgba(200,206,217,.14);color:#cfd5df}.rank.bronze{background:rgba(214,141,82,.16);color:#e0a174}
.chg{font-weight:700;font-variant-numeric:tabular-nums;white-space:nowrap}.chg.up{color:var(--up)}.chg.down{color:var(--down)}.chg.flat{color:var(--muted)}.pill{display:inline-block;padding:2px 7px;border-radius:6px;font-size:12px;font-weight:700}.pill.up{background:var(--up-bg);color:var(--up)}.pill.down{background:var(--down-bg);color:var(--down)}
.badge{display:inline-flex;align-items:center;gap:4px;padding:1px 6px;border-radius:5px;font-size:12px;font-weight:700;background:var(--panel2);color:var(--muted);white-space:nowrap}.badge.new{background:rgba(157,140,255,.14);color:var(--violet)}.badge.hot{background:rgba(255,106,0,.15);color:var(--orange2)}.badge.grad{background:rgba(245,197,66,.14);color:var(--gold)}
.trust{display:inline-block;padding:0 5px;border-radius:4px;font-size:12px;font-weight:700;vertical-align:middle;letter-spacing:.02em}.trust.verified{background:rgba(31,210,134,.14);color:var(--up)}.trust.estimated{background:rgba(255,148,71,.14);color:var(--orange2)}.trust.unknown{background:var(--panel2);color:var(--dim)}
.progress{position:relative;display:inline-block;width:92px;height:18px;border-radius:9px;background:var(--panel2);overflow:hidden;vertical-align:middle}.progress i{position:absolute;inset:0 auto 0 0;background:linear-gradient(90deg,#6b3a14,var(--orange))}.progress.hot i{background:linear-gradient(90deg,var(--orange),var(--gold))}.progress.done i{background:linear-gradient(90deg,#8a6d1a,var(--gold))}.progress b{position:relative;display:block;text-align:center;font-size:12px;line-height:18px}
.spark{display:block}.spark polyline{fill:none;stroke-width:1.6;stroke-linejoin:round;stroke-linecap:round}.spark.up polyline{stroke:var(--up)}.spark.down polyline{stroke:var(--down)}
.tabs{display:flex;gap:6px;overflow-x:auto;scrollbar-width:none;padding-bottom:2px}.tabs::-webkit-scrollbar{display:none}.tabs a{padding:7px 12px;border-radius:9px;border:1px solid var(--line);background:var(--panel);color:var(--muted);font-weight:600;white-space:nowrap}.tabs a.active{color:#fff;border-color:rgba(255,106,0,.6);background:rgba(255,106,0,.14)}
.toolbar{display:flex;align-items:center;justify-content:space-between;gap:10px;flex-wrap:wrap;margin-bottom:12px}.search{display:flex;gap:6px}.search input{height:36px;width:260px;max-width:60vw;border:1px solid var(--line2);border-radius:9px;background:var(--panel);color:var(--text);padding:0 12px}.search button{height:36px;border:0;border-radius:9px;background:var(--orange);color:#fff;font-weight:700;padding:0 14px;cursor:pointer}
.table-wrap{overflow-x:auto;-webkit-overflow-scrolling:touch}table.market{width:100%;border-collapse:collapse;font-variant-numeric:tabular-nums}table.market th{position:sticky;top:0;background:var(--panel);text-align:right;color:var(--dim);font-size:12px;font-weight:600;text-transform:uppercase;letter-spacing:.04em;padding:10px 12px;border-bottom:1px solid var(--line);white-space:nowrap}table.market th:first-child,table.market th.l{text-align:left}table.market th a:hover{color:var(--text)}table.market th a.sorted{color:var(--orange2)}table.market td{padding:9px 12px;border-bottom:1px solid var(--line);text-align:right;white-space:nowrap}table.market td.l{text-align:left}table.market tbody tr{cursor:pointer}table.market tbody tr:hover{background:var(--panel2)}table.market tbody tr:last-child td{border-bottom:0}
.tok{display:flex;align-items:center;gap:10px;min-width:180px}.tok strong{display:block;font-size:13px}.tok small{display:block;color:var(--dim);font-size:12px}
.pager{display:flex;justify-content:center;gap:8px;padding:14px}.pager a,.pager span{padding:6px 12px;border:1px solid var(--line);border-radius:8px;color:var(--muted)}.pager a:hover{color:var(--text);border-color:var(--line2)}
.feed .row{min-height:44px}.feed .amt{font-weight:700}.feed .buy{color:var(--up)}.feed .sell{color:var(--down)}
.grid2{display:grid;grid-template-columns:minmax(0,1.6fr) minmax(0,1fr);gap:12px;margin-bottom:16px}.grid3{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:12px;margin-bottom:16px}
.podium{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:12px;margin-bottom:16px}.podium .card{padding:16px;position:relative}.podium .card.first{border-color:rgba(245,197,66,.5);background:linear-gradient(180deg,rgba(245,197,66,.09),var(--panel) 65%)}.podium .pnl{font-size:28px;font-weight:800;letter-spacing:-.03em;margin:8px 0 2px}
.empty{padding:28px 16px;text-align:center;color:var(--muted)}.note{color:var(--dim);font-size:12px;margin:10px 2px}
.token-hero{display:flex;align-items:center;justify-content:space-between;gap:16px;flex-wrap:wrap;margin:6px 0 14px}.token-hero .id{display:flex;align-items:center;gap:12px}.token-hero .avatar{width:48px;height:48px;font-size:16px}.token-hero h1{margin:0;font-size:24px}.token-hero .price{text-align:right}.token-hero .price strong{display:block;font-size:30px;letter-spacing:-.03em}
.chart{padding:10px 14px 14px}.chart svg{width:100%;height:280px;display:block}.chart .grid line{stroke:var(--line)}.chart .candle.up{fill:var(--up);stroke:var(--up)}.chart .candle.down{fill:var(--down);stroke:var(--down)}.chart .vol{fill:rgba(139,147,163,.25)}.chart text{fill:var(--dim);font-size:12px}
.kv{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:1px;background:var(--line)}.kv>div{background:var(--panel);padding:10px 14px}.kv span{display:block;color:var(--muted);font-size:12px}.kv strong{display:block;font-size:15px;margin-top:2px}
.flash-up{animation:flashUp 1.2s ease-out}.flash-down{animation:flashDown 1.2s ease-out}@keyframes flashUp{0%{background:rgba(31,210,134,.28)}100%{background:transparent}}@keyframes flashDown{0%{background:rgba(255,92,108,.28)}100%{background:transparent}}
footer.site{max-width:1600px;margin:0 auto;border-top:1px solid var(--line);padding:18px 20px 32px;display:flex;justify-content:space-between;gap:12px;flex-wrap:wrap;color:var(--dim);font-size:12px}footer.site a:hover{color:var(--text)}
.legacy .panel{background:var(--panel);border:1px solid var(--line);border-radius:14px;padding:16px;margin-bottom:14px}
@media(max-width:1280px){.boards{grid-template-columns:repeat(2,minmax(0,1fr))}}
@media(max-width:900px){.grid2,.grid3,.podium{grid-template-columns:1fr}.bar{gap:10px;padding:0 12px}.brand small{display:none}.tools select{max-width:110px}main{padding:12px 12px 50px}.ticker>div{padding:7px 12px}}
@media(max-width:640px){.boards{grid-template-columns:1fr}.bar{flex-wrap:wrap;height:auto;padding:8px 12px;row-gap:6px}nav.main{order:3;flex-basis:100%}.stat>strong{font-size:20px}.search input{width:100%}.search{flex:1}.token-hero .price{text-align:left}}
"#;

/// Client script: polls fragment endpoints, swaps content and flashes changed
/// numeric cells. Rows link to their `data-href`.
pub const SCRIPT: &str = r#"<script>
(()=>{
const num=t=>{const m=(t||'').replace(/[,$%▲▼+]/g,'').match(/-?\d+(\.\d+)?/);return m?parseFloat(m[0]):null};
const refresh=async el=>{
  const url=el.dataset.refresh;if(!url||document.hidden)return;
  try{
    const r=await fetch(url,{headers:{'x-requested-with':'fetch'}});if(!r.ok)return;
    const html=await r.text();
    const old=new Map([...el.querySelectorAll('[data-k]')].map(n=>[n.dataset.k,n.textContent]));
    el.innerHTML=html;
    el.querySelectorAll('[data-k]').forEach(n=>{
      const prev=old.get(n.dataset.k);if(prev===undefined||prev===n.textContent)return;
      const a=num(prev),b=num(n.textContent);
      n.classList.add(a!==null&&b!==null&&b<a?'flash-down':'flash-up');
    });
  }catch(e){}
};
document.querySelectorAll('[data-refresh]').forEach(el=>{
  const every=Math.max(5,parseInt(el.dataset.every||'15',10))*1000;
  if(el.dataset.eager!==undefined)refresh(el);
  setInterval(()=>refresh(el),every);
});
document.addEventListener('click',e=>{
  if(e.target.closest('a,button,input,select'))return;
  const row=e.target.closest('[data-href]');if(row)location.href=row.dataset.href;
});
})();
</script>"#;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_compact_usd() {
        assert_eq!(usd(Some(1234.0)), "$1.23K");
        assert_eq!(usd(Some(45_600_000.0)), "$45.6M");
        assert_eq!(usd(Some(999.4)), "$999");
        assert_eq!(usd(Some(-12.345)), "-$12.35");
        assert_eq!(usd(None), "—");
        assert_eq!(signed_usd(Some(2500.0)), "+$2.50K");
    }

    #[test]
    fn formats_tiny_prices_with_zero_subscript() {
        assert_eq!(small_decimal(0.00000412), "0.0₅412");
        assert_eq!(small_decimal(0.0012), "0.0012");
        assert_eq!(small_decimal(0.042), "0.0420");
        assert_eq!(price_usd(Some(0.000000123)), "$0.0₆123");
        assert_eq!(price_usd(Some(1.5)), "$1.50");
    }

    #[test]
    fn formats_change_badges() {
        assert!(change(Some(35.44)).contains("▲35.4%"));
        assert!(change(Some(-12.0)).contains("down"));
        assert!(change(Some(1041.2)).contains("1041%"));
        assert!(change(Some(125_000.0)).contains("125K%"));
        assert!(change(None).contains("—"));
    }

    #[test]
    fn formats_counts_and_addresses() {
        assert_eq!(count(1_234_567), "1,234,567");
        assert_eq!(count(-1_000), "-1,000");
        assert_eq!(
            short_address("0x0a02ffffffffffffffffffffffffffffff3ba015"),
            "0x0a02…a015"
        );
    }

    #[test]
    fn sparkline_needs_two_points() {
        assert!(!sparkline(&[Some(1.0)], 80, 24).contains("polyline"));
        let rising = sparkline(&[Some(1.0), None, Some(2.0)], 80, 24);
        assert!(rising.contains("spark up"));
        assert!(sparkline(&[Some(2.0), Some(1.0)], 80, 24).contains("spark down"));
    }
}
