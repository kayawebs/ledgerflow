use super::{PilotEvidencePack, event_type_name};
use sha2::{Digest, Sha256};

pub fn dedup_key(project_id: &str, event: &PilotEvidencePack) -> String {
    let subject = event
        .subject
        .get("address")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unknown");
    let raw = format!(
        "{}:{}:{}:{}",
        project_id,
        event_type_name(&event.event_type),
        event.tx_hash.to_ascii_lowercase(),
        subject.to_ascii_lowercase()
    );
    let mut h = Sha256::new();
    h.update(raw.as_bytes());
    hex::encode(h.finalize())
}

pub fn retry_delay_seconds(attempt: u32) -> u64 {
    2_u64.saturating_pow(attempt.min(8))
}

pub fn format_telegram_alert(
    project: &str,
    event: &PilotEvidencePack,
    explanation_link: &str,
) -> String {
    let subject = event
        .subject
        .get("address")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unknown");
    let first_flow = event.asset_flows.first();
    let key_amount = first_flow
        .and_then(|v| v.get("amount_raw"))
        .and_then(serde_json::Value::as_str)
        .map(|amount| {
            let token = first_flow
                .and_then(|v| v.get("token_address"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or("native");
            format!("{amount} raw ({token})")
        })
        .unwrap_or_else(|| "unknown".into());
    let counterparty = first_flow
        .and_then(|flow| {
            let from = flow
                .get("from_address")
                .and_then(serde_json::Value::as_str)?;
            let to = flow.get("to_address").and_then(serde_json::Value::as_str)?;
            Some(if from.eq_ignore_ascii_case(subject) {
                to
            } else {
                from
            })
        })
        .unwrap_or("unknown");
    let protocol = event
        .protocol_path
        .first()
        .and_then(|v| v.get("protocol"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unknown");
    let confidence = event
        .confidence
        .map(|v| format!("{:.1}%", v * 100.0))
        .unwrap_or_else(|| "unknown".into());
    format!(
        "Project: {project}\nSeverity: {:?}\nEvent Type: {}\nConclusion: {}\nKey Amount: {key_amount}\nSubject: {subject}\nCounterparty: {counterparty}\nProtocol: {protocol}\nTx Hash: {}\nExplanation Link: {explanation_link}\nConfidence: {confidence}",
        event.severity,
        event_type_name(&event.event_type),
        event.conclusion,
        event.tx_hash
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use serde_json::json;
    use uuid::Uuid;

    fn pack() -> PilotEvidencePack {
        PilotEvidencePack {
            schema_version: super::super::EVIDENCE_PACK_SCHEMA_VERSION.into(),
            event_id: Uuid::nil(),
            project_id: "pilot".into(),
            chain_id: 8453,
            block_number: Some(1),
            tx_hash: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
            subject: json!({"address":"0x1111111111111111111111111111111111111111"}),
            event_type: super::super::PilotEventType::LargeAssetFlow,
            severity: super::super::PilotSeverity::High,
            conclusion: "deterministic conclusion".into(),
            entities: Vec::new(),
            asset_flows: vec![json!({
                "amount_raw":"1000",
                "token_address":"0x2222222222222222222222222222222222222222",
                "from_address":"0x1111111111111111111111111111111111111111",
                "to_address":"0x3333333333333333333333333333333333333333"
            })],
            protocol_path: vec![json!({"protocol":"Uniswap V3"})],
            profit_and_cost: json!({}),
            technical_evidence: json!({}),
            confidence: Some(0.9),
            limitations: Vec::new(),
            ruleset_id: "rules-v1".into(),
            inputs_digest: "digest".into(),
            created_at: Utc::now(),
        }
    }

    #[test]
    fn retry_is_bounded() {
        assert_eq!(retry_delay_seconds(0), 1);
        assert_eq!(retry_delay_seconds(20), 256);
    }

    #[test]
    fn alert_contains_contract_fields_and_stable_dedup() {
        let event = pack();
        let message = format_telegram_alert("Public Demo", &event, "https://demo/tx");
        for field in [
            "Project: Public Demo",
            "Severity: High",
            "Event Type: large_asset_flow",
            "Conclusion: deterministic conclusion",
            "Key Amount: 1000 raw",
            "Subject:",
            "Counterparty:",
            "Protocol: Uniswap V3",
            "Tx Hash:",
            "Explanation Link: https://demo/tx",
            "Confidence: 90.0%",
        ] {
            assert!(message.contains(field), "missing {field}");
        }
        assert_eq!(dedup_key("pilot", &event), dedup_key("pilot", &event));
        assert_ne!(dedup_key("pilot", &event), dedup_key("other", &event));
    }
}
