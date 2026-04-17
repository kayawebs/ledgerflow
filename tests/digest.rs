use serde_json::json;

use ledgerflow::core::versioning::stable_digest;

#[test]
fn deterministic_digest_ordering() {
    let left = json!({"b":[2, {"y":1, "x":2}], "a":1});
    let right = json!({"a":1, "b":[2, {"x":2, "y":1}]});
    assert_eq!(stable_digest(&left), stable_digest(&right));
}
