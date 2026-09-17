use std::{collections::BTreeMap, fmt::Write};

use num_bigint::BigUint;
use sha2::{Digest, Sha256};

use super::{PilotEvidencePack, event_type_name};

pub fn report_inputs_digest(events: &[PilotEvidencePack]) -> String {
    let mut h = Sha256::new();
    for event in events {
        h.update(event.inputs_digest.as_bytes());
    }
    hex::encode(h.finalize())
}

pub fn weekly_markdown(
    project_id: &str,
    week_start: &str,
    week_end: &str,
    events: &[PilotEvidencePack],
) -> String {
    let mut out = String::new();
    let counts = event_counts(events);
    let assets = asset_totals(events);
    let counterparties = counterparty_counts(events);
    writeln!(
        out,
        "# LedgerFlow weekly report — {project_id}\n\nPeriod: `{week_start}` to `{week_end}`\n"
    )
    .unwrap();
    writeln!(
        out,
        "## Summary\n\n{} material events across {} transactions.\n",
        events.len(),
        events
            .iter()
            .map(|v| &v.tx_hash)
            .collect::<std::collections::BTreeSet<_>>()
            .len()
    )
    .unwrap();
    writeln!(out, "| Event type | Count |\n|---|---:|").unwrap();
    for (kind, count) in counts {
        writeln!(out, "| `{kind}` | {count} |").unwrap();
    }
    render_events_md(&mut out, "Key events", events);
    writeln!(out,"\n## Inflows / outflows and asset flow\n\n| Asset | Inflow raw | Outflow raw |\n|---|---:|---:|").unwrap();
    for (asset, (incoming, outgoing)) in assets {
        writeln!(out, "| `{asset}` | {incoming} | {outgoing} |").unwrap();
    }
    render_events_md(
        &mut out,
        "Protocol revenue",
        &events
            .iter()
            .filter(|v| event_type_name(&v.event_type) == "protocol_revenue")
            .cloned()
            .collect::<Vec<_>>(),
    );
    render_events_md(
        &mut out,
        "Arbitrage / liquidation",
        &events
            .iter()
            .filter(|v| {
                matches!(
                    event_type_name(&v.event_type),
                    "dex_arbitrage" | "liquidation"
                )
            })
            .cloned()
            .collect::<Vec<_>>(),
    );
    writeln!(
        out,
        "\n## Counterparties\n\n| Address | Flow count |\n|---|---:|"
    )
    .unwrap();
    for (address, count) in counterparties {
        writeln!(out, "| `{address}` | {count} |").unwrap();
    }
    render_events_md(
        &mut out,
        "Failures",
        &events
            .iter()
            .filter(|v| {
                matches!(
                    event_type_name(&v.event_type),
                    "high_cost_failure" | "repeated_failed_attempts"
                )
            })
            .cloned()
            .collect::<Vec<_>>(),
    );
    writeln!(out, "\n## Limitations\n").unwrap();
    let mut limitations = events
        .iter()
        .flat_map(|v| v.limitations.iter())
        .collect::<Vec<_>>();
    limitations.sort();
    limitations.dedup();
    if limitations.is_empty() {
        writeln!(out, "\n- None recorded.\n").unwrap();
    } else {
        for limitation in limitations {
            writeln!(out, "\n- {limitation}").unwrap();
        }
    }
    out
}

fn render_events_md(out: &mut String, title: &str, events: &[PilotEvidencePack]) {
    writeln!(out,"\n## {title}\n\n| Severity | Type | Tx / link | Conclusion | Confidence |\n|---|---|---|---|---:|").unwrap();
    for event in events {
        let confidence = event
            .confidence
            .map(|v| format!("{:.1}%", v * 100.0))
            .unwrap_or_else(|| "unknown".into());
        writeln!(
            out,
            "| {:?} | `{}` | [{}](/app/tx-analysis?chain_id={}&tx_hash={}) | {} | {} |",
            event.severity,
            event_type_name(&event.event_type),
            short_hash(&event.tx_hash),
            event.chain_id,
            event.tx_hash,
            event.conclusion.replace('|', "\\|"),
            confidence
        )
        .unwrap();
    }
}

pub fn weekly_html(
    project_id: &str,
    week_start: &str,
    week_end: &str,
    events: &[PilotEvidencePack],
) -> String {
    let counts = event_counts(events)
        .into_iter()
        .map(|(k, v)| format!("<li><code>{}</code>: {}</li>", html_escape(k), v))
        .collect::<String>();
    let assets = asset_totals(events)
        .into_iter()
        .map(|(asset, (incoming, outgoing))| {
            format!(
                "<tr><td><code>{}</code></td><td>{}</td><td>{}</td></tr>",
                html_escape(&asset),
                incoming,
                outgoing
            )
        })
        .collect::<String>();
    let counterparties = counterparty_counts(events)
        .into_iter()
        .map(|(address, count)| {
            format!(
                "<tr><td><code>{}</code></td><td>{}</td></tr>",
                html_escape(&address),
                count
            )
        })
        .collect::<String>();
    let revenue = events
        .iter()
        .filter(|event| event.event_type == super::PilotEventType::ProtocolRevenue)
        .cloned()
        .collect::<Vec<_>>();
    let arbitrage_liquidation = events
        .iter()
        .filter(|event| {
            matches!(
                event.event_type,
                super::PilotEventType::DexArbitrage | super::PilotEventType::Liquidation
            )
        })
        .cloned()
        .collect::<Vec<_>>();
    let failures = events
        .iter()
        .filter(|event| {
            matches!(
                event.event_type,
                super::PilotEventType::HighCostFailure
                    | super::PilotEventType::RepeatedFailedAttempts
            )
        })
        .cloned()
        .collect::<Vec<_>>();
    let mut limitations = events
        .iter()
        .flat_map(|event| event.limitations.iter())
        .collect::<Vec<_>>();
    limitations.sort();
    limitations.dedup();
    let limitations = if limitations.is_empty() {
        "<li>None recorded.</li>".to_owned()
    } else {
        limitations
            .into_iter()
            .map(|value| format!("<li>{}</li>", html_escape(value)))
            .collect::<String>()
    };
    format!(
        "<!doctype html><html><head><meta charset=\"utf-8\"><title>LedgerFlow weekly report</title><style>body{{font:15px system-ui;max-width:1200px;margin:40px auto;padding:0 20px;color:#172033}}table{{border-collapse:collapse;width:100%;margin:16px 0}}th,td{{border:1px solid #d8deea;padding:8px;text-align:left;vertical-align:top}}th{{background:#f3f6fb}}code{{overflow-wrap:anywhere}}</style></head><body><h1>LedgerFlow weekly report — {}</h1><p>Period: <code>{}</code> to <code>{}</code></p><h2>Summary</h2><p>{} material events.</p><ul>{}</ul><h2>Key events</h2>{}<h2>Inflows / outflows and asset flow</h2><table><tr><th>Asset</th><th>Inflow raw</th><th>Outflow raw</th></tr>{}</table><h2>Protocol revenue</h2>{}<h2>Arbitrage / liquidation</h2>{}<h2>Counterparties</h2><table><tr><th>Address</th><th>Flow count</th></tr>{}</table><h2>Failures</h2>{}<h2>Limitations</h2><ul>{}</ul><h2>Fact policy</h2><p>Unknown values are not inferred. See each Evidence Pack for limitations.</p></body></html>",
        html_escape(project_id),
        html_escape(week_start),
        html_escape(week_end),
        events.len(),
        counts,
        events_table_html(events),
        assets,
        events_table_html(&revenue),
        events_table_html(&arbitrage_liquidation),
        counterparties,
        events_table_html(&failures),
        limitations
    )
}

fn events_table_html(events: &[PilotEvidencePack]) -> String {
    let rows=events.iter().map(|event|format!("<tr><td>{:?}</td><td>{}</td><td><a href=\"/app/tx-analysis?chain_id={}&amp;tx_hash={}\"><code>{}</code></a></td><td>{}</td><td>{}</td></tr>",event.severity,event_type_name(&event.event_type),event.chain_id,event.tx_hash,short_hash(&event.tx_hash),html_escape(&event.conclusion),event.confidence.map(|v|format!("{:.1}%",v*100.0)).unwrap_or_else(||"unknown".into()))).collect::<String>();
    format!(
        "<table><tr><th>Severity</th><th>Type</th><th>Tx / link</th><th>Conclusion</th><th>Confidence</th></tr>{rows}</table>"
    )
}

fn event_counts(events: &[PilotEvidencePack]) -> BTreeMap<&str, usize> {
    let mut out = BTreeMap::new();
    for event in events {
        *out.entry(event_type_name(&event.event_type)).or_default() += 1;
    }
    out
}
fn asset_totals(events: &[PilotEvidencePack]) -> BTreeMap<String, (BigUint, BigUint)> {
    let mut out = BTreeMap::new();
    for event in events {
        let subject = event
            .subject
            .get("address")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("");
        for flow in &event.asset_flows {
            let Some(amount) = flow
                .get("amount_raw")
                .and_then(serde_json::Value::as_str)
                .and_then(|v| v.parse::<BigUint>().ok())
            else {
                continue;
            };
            let asset = flow
                .get("token_address")
                .and_then(serde_json::Value::as_str)
                .or_else(|| flow.get("asset_type").and_then(serde_json::Value::as_str))
                .unwrap_or("unknown")
                .to_owned();
            let entry = out
                .entry(asset)
                .or_insert_with(|| (BigUint::from(0u8), BigUint::from(0u8)));
            if flow
                .get("to_address")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|v| v.eq_ignore_ascii_case(subject))
            {
                entry.0 += amount
            } else {
                entry.1 += amount
            }
        }
    }
    out
}
fn counterparty_counts(events: &[PilotEvidencePack]) -> BTreeMap<String, usize> {
    let mut out = BTreeMap::new();
    for event in events {
        let subject = event
            .subject
            .get("address")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("");
        for flow in &event.asset_flows {
            for address in [flow.get("from_address"), flow.get("to_address")]
                .into_iter()
                .flatten()
                .filter_map(serde_json::Value::as_str)
            {
                if !address.eq_ignore_ascii_case(subject) {
                    *out.entry(address.to_ascii_lowercase()).or_default() += 1;
                }
            }
        }
    }
    out
}
fn short_hash(value: &str) -> String {
    if value.len() > 18 {
        format!("{}…{}", &value[..10], &value[value.len() - 6..])
    } else {
        value.to_owned()
    }
}
fn html_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pilot::{EVIDENCE_PACK_SCHEMA_VERSION, PilotEventType, PilotSeverity};
    use chrono::Utc;
    use serde_json::json;
    use uuid::Uuid;
    fn event() -> PilotEvidencePack {
        PilotEvidencePack {
            schema_version: EVIDENCE_PACK_SCHEMA_VERSION.into(),
            event_id: Uuid::nil(),
            project_id: "p".into(),
            chain_id: 8453,
            block_number: Some(1),
            tx_hash: "0xabc".into(),
            subject: json!({"address":"0xa"}),
            event_type: PilotEventType::LargeAssetFlow,
            severity: PilotSeverity::High,
            conclusion: "flow".into(),
            entities: vec![],
            asset_flows: vec![
                json!({"amount_raw":"10","from_address":"0xb","to_address":"0xa","asset_type":"native"}),
            ],
            protocol_path: vec![],
            profit_and_cost: json!({}),
            technical_evidence: json!({}),
            confidence: Some(1.0),
            limitations: vec![],
            ruleset_id: "r".into(),
            inputs_digest: "d".into(),
            created_at: Utc::now(),
        }
    }
    #[test]
    fn report_is_deterministic() {
        assert_eq!(
            weekly_markdown("p", "a", "b", &[event()]),
            weekly_markdown("p", "a", "b", &[event()])
        );
    }
    #[test]
    fn report_has_required_sections() {
        let report = weekly_markdown("p", "a", "b", &[event()]);
        let html = weekly_html("p", "a", "b", &[event()]);
        for title in [
            "Summary",
            "Key events",
            "Inflows / outflows",
            "Protocol revenue",
            "Arbitrage / liquidation",
            "Counterparties",
            "Failures",
        ] {
            assert!(report.contains(title));
            assert!(html.contains(title));
        }
        assert!(html.contains("/app/tx-analysis?chain_id=8453"));
    }
}
