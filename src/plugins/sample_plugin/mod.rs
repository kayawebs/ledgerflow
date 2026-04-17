use std::sync::Arc;

use chrono::Utc;
use serde_json::json;

use crate::domain::entities::{EvidencePack, TransactionAnalysis};
use crate::domain::enums::ValidationStatus;
use crate::domain::interfaces::Validator;
use crate::domain::value_objects::ValidationMessage;
use crate::ingestion::models::RawTransactionBundle;
use crate::plugins::loader::PluginManifest;
use crate::recognition::builtins::builtin_recognizers;
use crate::recognition::registry::RecognizerRegistry;

pub struct TraceAvailabilityValidator;

impl Validator for TraceAvailabilityValidator {
    fn validate(
        &self,
        bundle: &RawTransactionBundle,
        _analysis: &TransactionAnalysis,
        _pack: Option<&EvidencePack>,
    ) -> Vec<ValidationMessage> {
        vec![ValidationMessage {
            validator_id: "trace_availability".to_owned(),
            status: if bundle.trace.is_some() {
                ValidationStatus::Pass
            } else {
                ValidationStatus::Warn
            },
            message: if bundle.trace.is_some() {
                "Trace present".to_owned()
            } else {
                "Trace unavailable; analysis may be partial.".to_owned()
            },
            details: json!({}),
            evidence_refs: Vec::new(),
            created_at: Utc::now(),
        }]
    }
}

pub fn register(registry: &mut RecognizerRegistry) -> (PluginManifest, Vec<Arc<dyn Validator>>) {
    let recognizers = builtin_recognizers();
    let recognizer_ids = recognizers
        .iter()
        .map(|item| item.recognizer_id().to_owned())
        .collect::<Vec<_>>();
    for recognizer in recognizers {
        registry.register(recognizer);
    }
    (
        PluginManifest {
            plugin_id: "sample_builtin_pack".to_owned(),
            version: "1.0.0".to_owned(),
            recognizers: recognizer_ids,
            validators: vec!["trace_availability".to_owned()],
        },
        vec![Arc::new(TraceAvailabilityValidator)],
    )
}
