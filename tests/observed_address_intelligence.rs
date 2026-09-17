use serde_json::{Value, json};

const SCHEMA: &str = include_str!("../schemas/observed-address-intelligence-v1.schema.json");
const MIGRATION: &str = include_str!("../migrations/0046_observed_address_intelligence_v1.up.sql");
const STABLE_BRIEFS_MIGRATION: &str =
    include_str!("../migrations/0048_stable_observation_briefs.up.sql");
const TRANSFER_ATTRIBUTION_MIGRATION: &str =
    include_str!("../migrations/0049_transfer_participant_action_attribution.up.sql");
const OBSERVATION_SERVICE: &str = include_str!("../src/services/observation_intelligence.rs");
const API_SOURCE: &str = include_str!("../src/api/mod.rs");
const REAL_OBJECT_FIXTURES: &str = include_str!("fixtures/observed_address_intelligence_v1.json");

#[test]
fn intelligence_contract_schema_accepts_minimal_stateful_snapshot() {
    let schema: Value = serde_json::from_str(SCHEMA).expect("schema JSON");
    let validator = jsonschema::validator_for(&schema).expect("valid JSON Schema");
    let snapshot = json!({
        "contract_version": "observed-address-intelligence.v1",
        "chain_id": 8453,
        "address": "0x0000000000000000000000000000000000000001",
        "object": {
            "current_status": "backfilling",
            "progress": {"percent": 62, "transactions_discovered": 1842, "transactions_analyzed": 1420},
            "monitoring_enabled": true,
            "coverage_is_partial": true,
            "coverage_start": "2026-07-20T00:00:00Z",
            "coverage_end": "2026-08-20T00:00:00Z",
            "error": null
        },
        "activity_windows": [],
        "behaviors": [],
        "relationships": [],
        "economics": [],
        "changes": [],
        "timeline": [],
        "briefs": [],
        "limitations": ["Baseline is still being built."]
    });
    validator.validate(&snapshot).expect("snapshot validates");
}

#[test]
fn intelligence_contract_rejects_unsupported_state_and_malformed_address() {
    let schema: Value = serde_json::from_str(SCHEMA).expect("schema JSON");
    let validator = jsonschema::validator_for(&schema).expect("valid JSON Schema");
    let snapshot = json!({
        "contract_version": "observed-address-intelligence.v1",
        "chain_id": 8453,
        "address": "not-an-address",
        "object": {"current_status":"done","progress":{},"monitoring_enabled":true,"coverage_is_partial":false},
        "activity_windows": [], "behaviors": [], "relationships": [], "economics": [],
        "changes": [], "timeline": [], "briefs": [], "limitations": []
    });
    assert!(!validator.is_valid(&snapshot));
}

#[test]
fn migration_has_resumable_state_and_evidence_constraints() {
    for state in [
        "queued",
        "discovering",
        "backfilling",
        "classifying",
        "verifying_economics",
        "building_baseline",
        "ready",
        "stale",
        "failed",
    ] {
        assert!(
            MIGRATION.contains(&format!("'{state}'")),
            "missing state {state}"
        );
    }
    for table in [
        "observed_address_objects",
        "observation_analysis_runs",
        "object_activity_transactions",
        "object_activity_summaries",
        "object_behavior_profiles",
        "object_relationships",
        "object_economic_summaries",
        "object_change_signals",
        "object_briefs",
    ] {
        assert!(MIGRATION.contains(&format!("create table if not exists {table}")));
    }
    assert!(MIGRATION.contains("primary key (chain_id, address, tx_hash)"));
    assert!(MIGRATION.contains("supporting_tx_hashes text[] not null"));
    assert!(MIGRATION.contains("economic_result_status in ('verified', 'estimated', 'unknown')"));
    assert!(STABLE_BRIEFS_MIGRATION.contains("unique (chain_id, address, brief_type)"));
    assert!(STABLE_BRIEFS_MIGRATION.contains("cardinality(supporting_tx_hashes) = 0"));
    assert!(TRANSFER_ATTRIBUTION_MIGRATION.contains("lower(from_address) <> address"));
    assert!(TRANSFER_ATTRIBUTION_MIGRATION.contains("action_label = 'asset_transfer'"));
    assert!(OBSERVATION_SERVICE.contains("transfer_scan_next_end_block"));
    assert!(OBSERVATION_SERVICE.contains("discovery_chunk_budget"));
    assert!(OBSERVATION_SERVICE.contains("change_evidence_hashes"));
    assert!(OBSERVATION_SERVICE.contains("stale_claim_seconds: 120"));
    assert!(API_SOURCE.contains("last_incremental_update is not null) observation_ready"));
}

#[test]
fn regression_fixture_has_at_least_five_real_objects_on_both_supported_chains() {
    let fixtures: Vec<Value> =
        serde_json::from_str(REAL_OBJECT_FIXTURES).expect("valid real-object fixture JSON");
    assert!(fixtures.len() >= 5);
    assert!(
        fixtures
            .iter()
            .any(|item| item.get("chain_id").and_then(Value::as_u64) == Some(8453))
    );
    assert!(
        fixtures
            .iter()
            .any(|item| item.get("chain_id").and_then(Value::as_u64) == Some(4663))
    );
    for fixture in fixtures {
        let address = fixture
            .get("address")
            .and_then(Value::as_str)
            .expect("fixture address");
        assert_eq!(address.len(), 42);
        assert!(address.starts_with("0x"));
        assert!(
            fixture
                .get("selection_evidence")
                .and_then(Value::as_str)
                .is_some()
        );
        // Ownership must not be guessed merely because an address exhibits an
        // execution pattern.
        assert!(
            fixture
                .get("ownership_attribution")
                .is_some_and(Value::is_null)
        );
    }
}
