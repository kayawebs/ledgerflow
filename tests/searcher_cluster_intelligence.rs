use jsonschema::validator_for;
use serde_json::json;

#[test]
fn migration_keeps_infrastructure_out_of_candidate_member_roles() {
    let migration = include_str!("../migrations/0050_searcher_cluster_intelligence_v1.up.sql");
    let member_section = migration
        .split("create table if not exists candidate_execution_cluster_members")
        .nth(1)
        .and_then(|value| value.split("create index").next())
        .expect("candidate member migration section");
    assert!(!member_section.contains("'pool'"));
    assert!(!member_section.contains("'router'"));
    assert!(!member_section.contains("'protocol'"));
    assert!(member_section.contains("include_in_economic_scope"));
    assert!(migration.contains("Funding and settlement remain distinct from verified profit"));
}

#[test]
fn contract_schema_accepts_source_attested_candidate_snapshot() {
    let schema: serde_json::Value = serde_json::from_str(include_str!(
        "../schemas/searcher-cluster-intelligence-v1.schema.json"
    ))
    .expect("schema json");
    let validator = validator_for(&schema).expect("valid schema");
    let snapshot = json!({
        "contract_version":"searcher-cluster-intelligence.v1",
        "chain_id":8453,
        "data_source":{
            "chain_id":8453,
            "data_source":"live_canonical_index_with_address_lazy_history",
            "indexed_from_block":49850596,
            "indexed_to_block":50240310,
            "chain_head":50240312,
            "ingestion_lag":2,
            "last_updated_at":"2026-08-20T23:59:59Z",
            "history_mode":"live_index_plus_bounded_address_lazy_history",
            "full_history_available":false,
            "available":true,
            "limitation":"Historical coverage is object-specific."
        },
        "seed":{
            "seed_address":"0x0000000000000000000000000000000000000001",
            "seed_role":"settlement_recipient_candidate",
            "seed_role_confidence":0.98,
            "seed_role_evidence":{},
            "role_semantics":"candidate"
        },
        "cluster":{
            "cluster_id":"00000000-0000-4000-8000-000000000001",
            "kind":"searcher_candidate",
            "status":"ready",
            "progress":{},
            "coverage_is_partial":false,
            "retry_count":0
        },
        "members":[],
        "relationships":[],
        "windows":[],
        "partial_24h":{},
        "executions":[],
        "changes":[],
        "economic_limitations":[],
        "assets":[],
        "limitations":[]
    });
    if let Err(error) = validator.validate(&snapshot) {
        panic!("snapshot schema validation failed: {error}");
    }
}
