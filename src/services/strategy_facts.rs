use serde::Serialize;

use crate::domain::{entities::ClassifiedTransaction, enums::BehaviorTag};

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct TransactionStrategyFact {
    pub tx_hash: String,
    pub behavior_tag: BehaviorTag,
    pub strategy_type: String,
    pub strategy_subtype: String,
    pub protocol_family: Option<String>,
    pub protocol_name: Option<String>,
    pub route: StrategyRoute,
    pub outcome: StrategyOutcome,
    pub value_usd: Option<f64>,
    pub gas_cost_usd: Option<f64>,
    pub estimated_pnl_usd: Option<f64>,
    pub pnl_to_gas_ratio: Option<f64>,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct StrategyRoute {
    pub route_key: String,
    pub token_in: Option<TokenRef>,
    pub token_out: Option<TokenRef>,
    pub token_path: Vec<TokenRef>,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct TokenRef {
    pub address: String,
    pub symbol: Option<String>,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct StrategyOutcome {
    pub execution_success: Option<bool>,
    pub profitable: Option<bool>,
    pub report_success: bool,
    pub failure_reason: Option<String>,
}

pub fn build_strategy_fact(tx: &ClassifiedTransaction) -> TransactionStrategyFact {
    let protocol_name = normalized_protocol_name(tx.protocol.as_deref());
    let protocol_family = protocol_name
        .as_deref()
        .and_then(protocol_family)
        .map(str::to_owned);
    let route = strategy_route(tx);
    let outcome = strategy_outcome(tx);

    TransactionStrategyFact {
        tx_hash: tx.tx_hash.clone(),
        behavior_tag: tx.behavior_tag.clone(),
        strategy_type: strategy_type(&tx.behavior_tag).to_owned(),
        strategy_subtype: strategy_subtype(&tx.behavior_tag, protocol_family.as_deref(), &route),
        protocol_family,
        protocol_name,
        route,
        outcome,
        value_usd: tx.value_usd,
        gas_cost_usd: tx.gas_cost_usd,
        estimated_pnl_usd: tx.estimated_pnl_usd,
        pnl_to_gas_ratio: ratio_f64(tx.estimated_pnl_usd, tx.gas_cost_usd),
    }
}

pub fn strategy_type(behavior_tag: &BehaviorTag) -> &'static str {
    match behavior_tag {
        BehaviorTag::Arbitrage => "arbitrage",
        BehaviorTag::Liquidation => "liquidation",
        BehaviorTag::Lp => "lp",
        BehaviorTag::Position => "position",
        BehaviorTag::Unknown => "unknown",
    }
}

pub fn token_symbol(address: &str) -> Option<&'static str> {
    match address.to_ascii_lowercase().as_str() {
        "0x4200000000000000000000000000000000000006" => Some("WETH"),
        "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913" => Some("USDC"),
        "0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca" => Some("USDbC"),
        "0xfde4c96c8593536e31f229ea8f37b2ada2699bb2" => Some("USDT"),
        _ => None,
    }
}

fn strategy_subtype(
    behavior_tag: &BehaviorTag,
    protocol_family: Option<&str>,
    route: &StrategyRoute,
) -> String {
    match behavior_tag {
        BehaviorTag::Arbitrage => {
            if route.token_in.is_some() && route.token_out.is_some() {
                "token_pair_arbitrage".to_owned()
            } else {
                "arbitrage_unknown_route".to_owned()
            }
        }
        BehaviorTag::Liquidation => protocol_family
            .map(|family| format!("{family}_liquidation"))
            .unwrap_or_else(|| "liquidation_unknown_protocol".to_owned()),
        BehaviorTag::Lp => "liquidity_position".to_owned(),
        BehaviorTag::Position => "position_management".to_owned(),
        BehaviorTag::Unknown => "unknown".to_owned(),
    }
}

fn strategy_route(tx: &ClassifiedTransaction) -> StrategyRoute {
    let token_in = tx.token_in.as_deref().and_then(token_ref);
    let token_out = tx.token_out.as_deref().and_then(token_ref);
    let token_path = [token_out.clone(), token_in.clone()]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    StrategyRoute {
        route_key: route_key(token_out.as_ref(), token_in.as_ref()),
        token_in,
        token_out,
        token_path,
    }
}

fn token_ref(address: &str) -> Option<TokenRef> {
    let address = address.trim();
    if address.is_empty() {
        return None;
    }
    Some(TokenRef {
        address: address.to_ascii_lowercase(),
        symbol: token_symbol(address).map(str::to_owned),
    })
}

fn route_key(token_out: Option<&TokenRef>, token_in: Option<&TokenRef>) -> String {
    match (token_out, token_in) {
        (Some(out), Some(input)) => format!("{} -> {}", token_label(out), token_label(input)),
        (None, Some(input)) => format!("unknown -> {}", token_label(input)),
        (Some(out), None) => format!("{} -> unknown", token_label(out)),
        _ => "unknown_route".to_owned(),
    }
}

fn token_label(token: &TokenRef) -> String {
    token
        .symbol
        .clone()
        .unwrap_or_else(|| short_token(&token.address))
}

fn normalized_protocol_name(protocol: Option<&str>) -> Option<String> {
    protocol
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.to_ascii_lowercase())
}

fn protocol_family(protocol: &str) -> Option<&'static str> {
    if protocol.contains("uniswap") {
        Some("uniswap")
    } else if protocol.contains("aerodrome") {
        Some("aerodrome")
    } else if protocol.contains("aave") {
        Some("aave")
    } else if protocol.contains("morpho") {
        Some("morpho")
    } else if protocol.contains("compound") {
        Some("compound")
    } else if protocol.contains("curve") {
        Some("curve")
    } else if protocol.contains("balancer") {
        Some("balancer")
    } else if protocol.contains("pancake") {
        Some("pancake")
    } else {
        None
    }
}

fn strategy_outcome(tx: &ClassifiedTransaction) -> StrategyOutcome {
    let profitable = tx.estimated_pnl_usd.map(|value| value >= 0.0);
    StrategyOutcome {
        execution_success: None,
        profitable,
        report_success: profitable.unwrap_or(true),
        failure_reason: match tx.estimated_pnl_usd {
            Some(value) if value.is_finite() && value < 0.0 => {
                Some("negative_estimated_pnl".to_owned())
            }
            Some(value) if !value.is_finite() => Some("invalid_estimated_pnl".to_owned()),
            _ => None,
        },
    }
}

fn short_token(token: &str) -> String {
    let token = token.to_ascii_lowercase();
    if token.len() > 12 {
        format!("{}...{}", &token[..8], &token[token.len() - 4..])
    } else {
        token
    }
}

fn ratio_f64(numerator: Option<f64>, denominator: Option<f64>) -> Option<f64> {
    match (numerator, denominator) {
        (Some(numerator), Some(denominator)) if denominator.is_finite() && denominator != 0.0 => {
            Some(numerator / denominator)
        }
        _ => None,
    }
}
