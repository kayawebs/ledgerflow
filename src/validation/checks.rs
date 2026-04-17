use chrono::Utc;
use serde_json::json;

use crate::core::versioning::RULESET_ID;
use crate::domain::entities::{EvidencePack, TransactionAnalysis};
use crate::domain::enums::ValidationStatus;
use crate::domain::interfaces::Validator;
use crate::domain::value_objects::ValidationMessage;
use crate::ingestion::models::RawTransactionBundle;

pub struct ReplayMetadataValidator;
pub struct EvidenceCompletenessValidator;
pub struct RecognizerSanityValidator;
pub struct AssetConservationPlaceholderValidator;

impl Validator for ReplayMetadataValidator {
    fn validate(
        &self,
        _bundle: &RawTransactionBundle,
        analysis: &TransactionAnalysis,
        _pack: Option<&EvidencePack>,
    ) -> Vec<ValidationMessage> {
        let status = if analysis.analysis_version_metadata.ruleset_id == RULESET_ID
            && !analysis.input_digest.is_empty()
        {
            ValidationStatus::Pass
        } else {
            ValidationStatus::Fail
        };
        vec![ValidationMessage {
            validator_id: "replay_metadata".to_owned(),
            status: status.clone(),
            message: if status == ValidationStatus::Pass {
                "Replay metadata present".to_owned()
            } else {
                "Replay metadata incomplete".to_owned()
            },
            details: json!({}),
            evidence_refs: Vec::new(),
            created_at: Utc::now(),
        }]
    }
}

impl Validator for EvidenceCompletenessValidator {
    fn validate(
        &self,
        bundle: &RawTransactionBundle,
        _analysis: &TransactionAnalysis,
        _pack: Option<&EvidencePack>,
    ) -> Vec<ValidationMessage> {
        let warned = bundle.receipt.logs.is_empty()
            && bundle.trace.is_none()
            && bundle.tx.to_address.is_none();
        vec![ValidationMessage {
            validator_id: "evidence_completeness".to_owned(),
            status: if warned {
                ValidationStatus::Warn
            } else {
                ValidationStatus::Pass
            },
            message: if warned {
                "Receipt has no logs and trace is unavailable for contractless target".to_owned()
            } else {
                "Evidence complete enough for current ruleset".to_owned()
            },
            details: json!({}),
            evidence_refs: Vec::new(),
            created_at: Utc::now(),
        }]
    }
}

impl Validator for RecognizerSanityValidator {
    fn validate(
        &self,
        _bundle: &RawTransactionBundle,
        analysis: &TransactionAnalysis,
        _pack: Option<&EvidencePack>,
    ) -> Vec<ValidationMessage> {
        let invalid = analysis
            .recognizer_results
            .iter()
            .filter(|item| !(0.0..=1.0).contains(&item.confidence))
            .map(|item| item.recognizer_id.clone())
            .collect::<Vec<_>>();
        vec![ValidationMessage {
            validator_id: "recognizer_sanity".to_owned(),
            status: if invalid.is_empty() {
                ValidationStatus::Pass
            } else {
                ValidationStatus::Fail
            },
            message: if invalid.is_empty() {
                "Recognizer confidences sane".to_owned()
            } else {
                format!("Invalid confidence in {}", invalid.join(","))
            },
            details: json!({ "invalid": invalid }),
            evidence_refs: Vec::new(),
            created_at: Utc::now(),
        }]
    }
}

impl Validator for AssetConservationPlaceholderValidator {
    fn validate(
        &self,
        _bundle: &RawTransactionBundle,
        _analysis: &TransactionAnalysis,
        _pack: Option<&EvidencePack>,
    ) -> Vec<ValidationMessage> {
        vec![ValidationMessage {
            validator_id: "asset_conservation_placeholder".to_owned(),
            status: ValidationStatus::Warn,
            message: "Asset conservation checks are not yet implemented for v0.".to_owned(),
            details: json!({}),
            evidence_refs: Vec::new(),
            created_at: Utc::now(),
        }]
    }
}

pub fn default_validators() -> Vec<Box<dyn Validator>> {
    vec![
        Box::new(ReplayMetadataValidator),
        Box::new(EvidenceCompletenessValidator),
        Box::new(RecognizerSanityValidator),
        Box::new(AssetConservationPlaceholderValidator),
    ]
}
