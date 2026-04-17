use ledgerflow::recognition::{builtins::builtin_recognizers, registry::RecognizerRegistry};

#[test]
fn registry_ordering_and_enablement() {
    let mut registry = RecognizerRegistry::new();
    for recognizer in builtin_recognizers() {
        registry.register(recognizer);
    }
    let metadata = registry.list_metadata();
    assert_eq!(metadata[0].recognizer_id, "selector_presence");
    let enabled = registry.enabled(&["native_transfer".to_owned(), "selector_presence".to_owned()]);
    let ids = enabled
        .iter()
        .map(|item| item.recognizer_id().to_owned())
        .collect::<Vec<_>>();
    assert_eq!(ids, vec!["selector_presence", "native_transfer"]);
}
