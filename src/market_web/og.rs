//! Open Graph card rendering.
//!
//! Any link somebody pastes into Twitter or a chat should arrive with the
//! numbers already on it, so every shareable page declares a card that is
//! drawn here from the same values the page shows. The cards are rasterised
//! server side because social clients do not render SVG.

use std::sync::{Arc, LazyLock};

use resvg::{tiny_skia, usvg};

use super::escape_html;

pub const WIDTH: u32 = 1200;
pub const HEIGHT: u32 = 630;

const BG: &str = "#07080a";
const PANEL: &str = "#0f1115";
const LINE: &str = "#20242d";
const TEXT: &str = "#eef0f4";
const MUTED: &str = "#8b93a3";
const DIM: &str = "#5d6473";
const ORANGE: &str = "#ff6a00";
const UP: &str = "#1fd286";
const DOWN: &str = "#ff5c6c";

/// Colour role for a value, so callers do not spell out hex codes.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Tone {
    Neutral,
    Up,
    Down,
    Accent,
}

impl Tone {
    fn color(self) -> &'static str {
        match self {
            Tone::Neutral => TEXT,
            Tone::Up => UP,
            Tone::Down => DOWN,
            Tone::Accent => ORANGE,
        }
    }

    /// Picks the tone a signed number should be drawn in.
    pub fn of(value: Option<f64>) -> Self {
        match value {
            Some(value) if value > 0.0 => Tone::Up,
            Some(value) if value < 0.0 => Tone::Down,
            _ => Tone::Neutral,
        }
    }
}

/// One labelled figure in the stat strip along the bottom of a card.
pub struct Stat {
    pub label: String,
    pub value: String,
    pub tone: Tone,
}

impl Stat {
    pub fn new(label: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            value: value.into(),
            tone: Tone::Neutral,
        }
    }

    pub fn toned(label: impl Into<String>, value: impl Into<String>, tone: Tone) -> Self {
        Self {
            label: label.into(),
            value: value.into(),
            tone,
        }
    }
}

/// One line of a ranked list card.
pub struct Row {
    pub rank: usize,
    pub name: String,
    pub detail: String,
    pub value: String,
    pub tone: Tone,
}

/// Everything a card draws. Handlers build this from domain rows so the
/// renderer stays independent of the market types.
pub struct Card {
    pub eyebrow: String,
    pub headline: String,
    pub subline: Option<String>,
    pub hero_label: Option<String>,
    pub hero_value: Option<String>,
    pub hero_tone: Tone,
    pub hero_delta: Option<(String, Tone)>,
    pub stats: Vec<Stat>,
    pub rows: Vec<Row>,
    pub spark: Vec<Option<f64>>,
    /// Small print along the bottom, such as the method behind the figure.
    pub note: Option<String>,
    pub footer: String,
}

impl Default for Card {
    fn default() -> Self {
        Self {
            eyebrow: String::new(),
            headline: String::new(),
            subline: None,
            hero_label: None,
            hero_value: None,
            hero_tone: Tone::Neutral,
            hero_delta: None,
            stats: Vec::new(),
            rows: Vec::new(),
            spark: Vec::new(),
            note: None,
            footer: "ledgerflow.top".to_string(),
        }
    }
}

/// The bundled font is embedded so the running binary needs no font packages,
/// and so a card never silently renders as empty boxes on a slim base image.
static FONTS: LazyLock<Arc<usvg::fontdb::Database>> = LazyLock::new(|| {
    let mut db = usvg::fontdb::Database::new();
    db.load_font_data(include_bytes!("../../assets/fonts/DejaVuSans.ttf").to_vec());
    db.load_font_data(include_bytes!("../../assets/fonts/DejaVuSans-Bold.ttf").to_vec());
    db.set_sans_serif_family("DejaVu Sans");
    Arc::new(db)
});

/// Renders a card to PNG bytes.
pub fn render(card: &Card) -> Result<Vec<u8>, String> {
    let svg = svg(card);
    let mut options = usvg::Options {
        fontdb: FONTS.clone(),
        ..usvg::Options::default()
    };
    options.font_family = "DejaVu Sans".to_string();
    let tree = usvg::Tree::from_str(&svg, &options).map_err(|error| error.to_string())?;
    let mut pixmap =
        tiny_skia::Pixmap::new(WIDTH, HEIGHT).ok_or_else(|| "pixmap allocation".to_string())?;
    resvg::render(&tree, tiny_skia::Transform::default(), &mut pixmap.as_mut());
    pixmap.encode_png().map_err(|error| error.to_string())
}

fn svg(card: &Card) -> String {
    let mut body = String::new();

    // Backdrop: flat panel plus the same orange bloom the site header uses.
    body.push_str(&format!(
        r##"<rect width="{WIDTH}" height="{HEIGHT}" fill="{BG}"/>
<defs><radialGradient id="bloom" cx="0.85" cy="0" r="0.75">
<stop offset="0" stop-color="{ORANGE}" stop-opacity="0.20"/>
<stop offset="1" stop-color="{ORANGE}" stop-opacity="0"/>
</radialGradient></defs>
<rect width="{WIDTH}" height="{HEIGHT}" fill="url(#bloom)"/>
<rect x="0" y="0" width="{WIDTH}" height="6" fill="{ORANGE}"/>"##
    ));

    // Brand line.
    body.push_str(&format!(
        r##"<rect x="64" y="56" width="36" height="36" rx="10" fill="{ORANGE}"/>
<text x="74" y="83" font-size="22" font-weight="700" fill="#07080a">L</text>
{brand}{tag}{host}"##,
        brand = text(116.0, 74.0, 24.0, 700, TEXT, "LedgerFlow", Anchor::Start),
        tag = text(
            116.0,
            92.0,
            13.0,
            600,
            MUTED,
            "MARKET INTELLIGENCE",
            Anchor::Start
        ),
        host = text(
            WIDTH as f64 - 64.0,
            84.0,
            16.0,
            600,
            DIM,
            &card.footer,
            Anchor::End
        ),
    ));

    let mut y = 168.0;
    if !card.eyebrow.is_empty() {
        body.push_str(&text(
            64.0,
            y,
            16.0,
            700,
            ORANGE,
            &card.eyebrow,
            Anchor::Start,
        ));
        y += 46.0;
    }

    body.push_str(&text(
        64.0,
        y,
        clamp_headline(&card.headline),
        700,
        TEXT,
        &truncate(&card.headline, 30),
        Anchor::Start,
    ));
    y += 34.0;

    if let Some(subline) = &card.subline {
        body.push_str(&text(
            64.0,
            y,
            20.0,
            500,
            MUTED,
            &truncate(subline, 64),
            Anchor::Start,
        ));
        y += 20.0;
    }

    // Hero figure, right aligned so long symbols never collide with it.
    if let Some(value) = &card.hero_value {
        if let Some(label) = &card.hero_label {
            body.push_str(&text(
                WIDTH as f64 - 64.0,
                190.0,
                16.0,
                600,
                MUTED,
                label,
                Anchor::End,
            ));
        }
        body.push_str(&text(
            WIDTH as f64 - 64.0,
            250.0,
            48.0,
            700,
            card.hero_tone.color(),
            &truncate(value, 18),
            Anchor::End,
        ));
        if let Some((delta, tone)) = &card.hero_delta {
            body.push_str(&text(
                WIDTH as f64 - 64.0,
                294.0,
                28.0,
                700,
                tone.color(),
                delta,
                Anchor::End,
            ));
        }
    }

    if !card.spark.is_empty() {
        body.push_str(&sparkline(&card.spark, 64.0, 330.0, 1072.0, 104.0));
    }

    if !card.rows.is_empty() {
        body.push_str(&rows(&card.rows, y + 24.0));
    }

    // A card shows either a ranked list or the stat strip: both would collide
    // in the lower half of the image. Without a sparkline the strip moves up so
    // the card does not read as half empty.
    if !card.stats.is_empty() && card.rows.is_empty() {
        let top = if card.spark.is_empty() { 386.0 } else { 470.0 };
        body.push_str(&stats(&card.stats, top));
    }

    if let Some(note) = &card.note {
        body.push_str(&text(
            64.0,
            HEIGHT as f64 - 42.0,
            16.0,
            500,
            DIM,
            &truncate(note, 86),
            Anchor::Start,
        ));
    }

    format!(
        r#"<svg xmlns="http://www.w3.org/2000/svg" width="{WIDTH}" height="{HEIGHT}" viewBox="0 0 {WIDTH} {HEIGHT}" font-family="DejaVu Sans">{body}</svg>"#
    )
}

/// Long symbols would otherwise run off the card, so the headline shrinks
/// rather than overflowing.
fn clamp_headline(headline: &str) -> f64 {
    match headline.chars().count() {
        0..=14 => 62.0,
        15..=22 => 48.0,
        _ => 38.0,
    }
}

fn rows(rows: &[Row], top: f64) -> String {
    let mut out = String::new();
    let height = 62.0;
    for (index, row) in rows.iter().take(5).enumerate() {
        let y = top + index as f64 * height;
        out.push_str(&format!(
            r##"<rect x="64" y="{y}" width="1072" height="54" rx="12" fill="{PANEL}" stroke="{LINE}"/>"##,
            y = y
        ));
        out.push_str(&text(
            96.0,
            y + 34.0,
            20.0,
            700,
            DIM,
            &row.rank.to_string(),
            Anchor::Start,
        ));
        out.push_str(&text(
            136.0,
            y + 34.0,
            22.0,
            700,
            TEXT,
            &truncate(&row.name, 18),
            Anchor::Start,
        ));
        out.push_str(&text(
            400.0,
            y + 34.0,
            17.0,
            500,
            MUTED,
            &truncate(&row.detail, 40),
            Anchor::Start,
        ));
        out.push_str(&text(
            1104.0,
            y + 34.0,
            24.0,
            700,
            row.tone.color(),
            &row.value,
            Anchor::End,
        ));
    }
    out
}

fn stats(stats: &[Stat], top: f64) -> String {
    let mut out = String::new();
    let count = stats.len().min(4).max(1);
    let gap = 16.0;
    let total = 1072.0;
    let width = (total - gap * (count as f64 - 1.0)) / count as f64;
    for (index, stat) in stats.iter().take(4).enumerate() {
        let x = 64.0 + index as f64 * (width + gap);
        out.push_str(&format!(
            r##"<rect x="{x}" y="{top}" width="{width}" height="96" rx="14" fill="{PANEL}" stroke="{LINE}"/>"##
        ));
        out.push_str(&text(
            x + 20.0,
            top + 34.0,
            15.0,
            600,
            MUTED,
            &truncate(&stat.label, 22),
            Anchor::Start,
        ));
        out.push_str(&text(
            x + 20.0,
            top + 72.0,
            30.0,
            700,
            stat.tone.color(),
            &truncate(&stat.value, 14),
            Anchor::Start,
        ));
    }
    out
}

/// Draws the 24h shape. Flat or missing series are skipped by the caller.
fn sparkline(points: &[Option<f64>], x: f64, y: f64, width: f64, height: f64) -> String {
    let values: Vec<f64> = points.iter().filter_map(|point| *point).collect();
    if values.len() < 2 {
        return String::new();
    }
    let min = values.iter().cloned().fold(f64::INFINITY, f64::min);
    let max = values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    let span = if (max - min).abs() < f64::EPSILON {
        1.0
    } else {
        max - min
    };
    let step = width / (values.len() - 1) as f64;
    let coords: Vec<String> = values
        .iter()
        .enumerate()
        .map(|(index, value)| {
            let px = x + index as f64 * step;
            let py = y + height - ((value - min) / span) * height;
            format!("{px:.1},{py:.1}")
        })
        .collect();
    let rising = values.last() >= values.first();
    let color = if rising { UP } else { DOWN };
    let area = format!(
        "{x:.1},{bottom:.1} {line} {end:.1},{bottom:.1}",
        bottom = y + height,
        line = coords.join(" "),
        end = x + width,
    );
    format!(
        r##"<polygon points="{area}" fill="{color}" fill-opacity="0.10"/>
<polyline points="{line}" fill="none" stroke="{color}" stroke-width="3" stroke-linejoin="round" stroke-linecap="round"/>"##,
        line = coords.join(" ")
    )
}

enum Anchor {
    Start,
    End,
}

fn text(x: f64, y: f64, size: f64, weight: u16, fill: &str, value: &str, anchor: Anchor) -> String {
    let anchor = match anchor {
        Anchor::Start => "start",
        Anchor::End => "end",
    };
    format!(
        r#"<text x="{x:.1}" y="{y:.1}" font-size="{size}" font-weight="{weight}" fill="{fill}" text-anchor="{anchor}">{}</text>"#,
        escape_html(value)
    )
}

/// Card text is drawn, not laid out, so over-long values are cut here rather
/// than spilling past the edge of the image.
fn truncate(value: &str, max: usize) -> String {
    let chars: Vec<char> = value.chars().collect();
    if chars.len() <= max {
        return value.to_string();
    }
    let mut out: String = chars[..max.saturating_sub(1)].iter().collect();
    out.push('…');
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Card {
        Card {
            eyebrow: "PONS LAUNCH · ROBINHOOD CHAIN".to_string(),
            headline: "MUSEPAD".to_string(),
            subline: Some("Musepad · quoted in ETH".to_string()),
            hero_label: Some("PRICE".to_string()),
            hero_value: Some("$0.000331".to_string()),
            hero_tone: Tone::Neutral,
            hero_delta: Some(("▲7130%".to_string(), Tone::Up)),
            stats: vec![
                Stat::new("24H VOLUME", "$2.61M"),
                Stat::new("MARKET CAP", "$331K"),
                Stat::toned("24H", "+7130%", Tone::Up),
                Stat::new("TRADES", "19,557"),
            ],
            rows: Vec::new(),
            spark: (0..24).map(|i| Some(1.0 + i as f64)).collect(),
            note: None,
            footer: "ledgerflow.top".to_string(),
        }
    }

    #[test]
    fn renders_a_png_of_the_declared_size() {
        let png = render(&sample()).expect("render");
        if let Ok(path) = std::env::var("LEDGERFLOW_OG_DUMP") {
            std::fs::write(path, &png).unwrap();
        }
        assert_eq!(&png[1..4], b"PNG");
        let width = u32::from_be_bytes([png[16], png[17], png[18], png[19]]);
        let height = u32::from_be_bytes([png[20], png[21], png[22], png[23]]);
        assert_eq!((width, height), (WIDTH, HEIGHT));
    }

    #[test]
    fn escapes_text_so_a_symbol_cannot_break_the_document() {
        let mut card = sample();
        card.headline = "<script>&".to_string();
        let markup = svg(&card);
        assert!(markup.contains("&lt;script&gt;&amp;"));
        assert!(render(&card).is_ok());
    }

    #[test]
    fn truncates_instead_of_overflowing() {
        assert_eq!(truncate("abcdefghij", 5), "abcd…");
        assert_eq!(truncate("abc", 5), "abc");
    }

    #[test]
    fn skips_a_sparkline_without_two_points() {
        assert!(sparkline(&[Some(1.0)], 0.0, 0.0, 10.0, 10.0).is_empty());
        assert!(!sparkline(&[Some(1.0), Some(2.0)], 0.0, 0.0, 10.0, 10.0).is_empty());
    }
}
