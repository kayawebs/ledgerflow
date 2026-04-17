use crate::core::versioning::canonical_json;
use crate::domain::entities::EvidencePack;

pub fn serialize_pack(pack: &EvidencePack) -> String {
    canonical_json(&serde_json::to_value(pack).unwrap_or_default())
}
