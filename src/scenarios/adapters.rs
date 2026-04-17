use serde_json::json;

use crate::domain::entities::TransactionAnalysis;

#[derive(Default, Clone)]
pub struct ScenarioAdapterService;

impl ScenarioAdapterService {
    pub fn adapt(&self, analysis: &TransactionAnalysis) -> serde_json::Value {
        let labels = analysis
            .recognizer_results
            .iter()
            .map(|item| item.label.clone())
            .collect::<Vec<_>>();
        json!({
            "attempt_hint": analysis.tx_hash,
            "opportunity_hint": labels.first(),
            "labels": labels,
        })
    }
}
