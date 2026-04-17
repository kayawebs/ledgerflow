use serde_json::json;

use crate::domain::entities::TransactionAnalysis;
use crate::domain::enums::AssetType;

#[derive(Default, Clone)]
pub struct AccountingService;

impl AccountingService {
    pub fn summarize(&self, analysis: &TransactionAnalysis) -> serde_json::Value {
        let native_flow_count = analysis
            .derived_flows
            .iter()
            .filter(|flow| flow.asset_type == AssetType::Native)
            .count();
        let erc20_flow_count = analysis
            .derived_flows
            .iter()
            .filter(|flow| flow.asset_type == AssetType::Erc20)
            .count();
        json!({
            "native_flow_count": native_flow_count,
            "erc20_flow_count": erc20_flow_count,
            "gas_cost_raw": analysis.normalized_execution_facts.derived_facts.get("gas_cost_raw"),
            "ruleset_ready": false,
        })
    }
}
