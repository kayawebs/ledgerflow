use serde_json::json;

use crate::core::versioning::{VersionBundle, stable_digest};
use crate::ingestion::models::RawTransactionBundle;

pub fn build_input_digest(bundle: &RawTransactionBundle, version_bundle: &VersionBundle) -> String {
    stable_digest(&json!({
        "tx": bundle.tx,
        "receipt": bundle.receipt,
        "trace": bundle.trace,
        "target_code": bundle.target_code,
        "version_bundle": version_bundle,
    }))
}
