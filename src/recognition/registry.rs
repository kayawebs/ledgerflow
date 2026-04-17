use std::sync::Arc;

use serde::Serialize;
use tracing::error;

use crate::core::errors::{LedgerError, LedgerResult};
use crate::domain::entities::{RecognizerResult, TransactionAnalysis};
use crate::domain::interfaces::Recognizer;
use crate::domain::value_objects::UnresolvedItem;

#[derive(Clone)]
pub struct RecognizerRegistry {
    recognizers: Vec<Arc<dyn Recognizer>>,
}

#[derive(Clone, Debug, Serialize)]
pub struct RecognizerMetadata {
    pub recognizer_id: String,
    pub version: String,
    pub description: String,
    pub order: u32,
}

impl RecognizerRegistry {
    pub fn new() -> Self {
        Self {
            recognizers: Vec::new(),
        }
    }

    pub fn register(&mut self, recognizer: Arc<dyn Recognizer>) {
        self.recognizers.push(recognizer);
        self.recognizers.sort_by_key(|item| item.order());
    }

    pub fn list_metadata(&self) -> Vec<RecognizerMetadata> {
        self.recognizers
            .iter()
            .map(|item| RecognizerMetadata {
                recognizer_id: item.recognizer_id().to_owned(),
                version: item.version().to_owned(),
                description: item.description().to_owned(),
                order: item.order(),
            })
            .collect()
    }

    pub fn enabled(&self, enabled_ids: &[String]) -> Vec<Arc<dyn Recognizer>> {
        self.recognizers
            .iter()
            .filter(|item| enabled_ids.iter().any(|id| id == item.recognizer_id()))
            .cloned()
            .collect()
    }

    pub fn execute(
        &self,
        analysis: &mut TransactionAnalysis,
        enabled_ids: &[String],
    ) -> LedgerResult<Vec<RecognizerResult>> {
        let mut results = Vec::new();
        for recognizer in self.enabled(enabled_ids) {
            match recognizer.recognize(analysis) {
                Ok(Some(result)) => results.push(result),
                Ok(None) => {}
                Err(error) => {
                    error!(recognizer = recognizer.recognizer_id(), tx_hash = %analysis.tx_hash, "recognizer_failed");
                    analysis.unresolved_items.push(UnresolvedItem {
                        code: "recognizer_failure".to_owned(),
                        message: match error {
                            LedgerError::RecognizerExecution(msg) => msg,
                            other => other.to_string(),
                        },
                        evidence_refs: Vec::new(),
                    });
                }
            }
        }
        Ok(results)
    }
}

impl Default for RecognizerRegistry {
    fn default() -> Self {
        Self::new()
    }
}
