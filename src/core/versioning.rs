use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

pub const PIPELINE_VERSION: &str = "2026.04.v1";
pub const FLOW_DERIVATION_VERSION: &str = "2026.04.v1";
pub const RULESET_ID: &str = "base-core-v0";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct VersionBundle {
    pub ruleset_id: String,
    pub pipeline_version: String,
    pub flow_derivation_version: String,
    pub recognizer_versions: BTreeMap<String, String>,
    pub config_version: String,
}

impl Default for VersionBundle {
    fn default() -> Self {
        Self {
            ruleset_id: RULESET_ID.to_owned(),
            pipeline_version: PIPELINE_VERSION.to_owned(),
            flow_derivation_version: FLOW_DERIVATION_VERSION.to_owned(),
            recognizer_versions: BTreeMap::new(),
            config_version: "default".to_owned(),
        }
    }
}

pub fn stable_digest(value: &Value) -> String {
    let canonical = canonical_json(value);
    let mut hasher = Sha256::new();
    hasher.update(canonical.as_bytes());
    hex::encode(hasher.finalize())
}

pub fn canonical_json(value: &Value) -> String {
    serde_json::to_string(&canonicalize(value))
        .expect("canonical json serialization should succeed")
}

pub fn canonicalize(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut ordered = Map::new();
            let mut keys = map.keys().cloned().collect::<Vec<_>>();
            keys.sort();
            for key in keys {
                if let Some(inner) = map.get(&key) {
                    ordered.insert(key, canonicalize(inner));
                }
            }
            Value::Object(ordered)
        }
        Value::Array(items) => Value::Array(items.iter().map(canonicalize).collect()),
        Value::String(text) => Value::String(text.clone()),
        Value::Number(number) => Value::Number(number.clone()),
        Value::Bool(flag) => Value::Bool(*flag),
        Value::Null => Value::Null,
    }
}

pub fn iso_utc(ts: DateTime<Utc>) -> String {
    ts.to_rfc3339()
}
