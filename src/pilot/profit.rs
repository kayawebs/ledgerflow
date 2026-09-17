use std::collections::{BTreeMap, BTreeSet};

use num_bigint::{BigInt, BigUint};
use serde_json::{Value, json};

use crate::{
    core::errors::{LedgerError, LedgerResult},
    domain::{
        entities::TransactionAnalysis,
        enums::{AssetType, TransactionStatus},
    },
    ingestion::rpc::RpcClient,
    normalization::service::TRANSFER_TOPIC,
};

use super::{PilotConfig, PilotEventType};

const V2_SWAP_TOPIC: &str = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822";
const V3_SWAP_TOPIC: &str = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67";
const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

pub const PROFIT_METHOD: &str = "archive_balance_snapshot_receipt_trace_reconciliation_v1";

fn scope_roles(role: &str) -> bool {
    matches!(
        role.to_ascii_lowercase().as_str(),
        "arbitrage_executor" | "searcher" | "profit_recipient"
    )
}

fn hex_biguint(value: Option<&str>) -> Option<BigUint> {
    let value = value?.trim().trim_start_matches("0x");
    if value.is_empty() {
        return Some(BigUint::from(0_u8));
    }
    BigUint::parse_bytes(value.as_bytes(), 16)
}

fn decimal_bigint(value: &str) -> Option<BigInt> {
    BigInt::parse_bytes(value.as_bytes(), 10)
}

fn topic_address(value: &str) -> Option<String> {
    let value = value.trim_start_matches("0x");
    (value.len() >= 40).then(|| format!("0x{}", &value[value.len() - 40..]).to_ascii_lowercase())
}

fn balance_of_data(address: &str) -> Option<String> {
    let address = address.trim_start_matches("0x");
    (address.len() == 40).then(|| format!("0x70a08231{:0>64}", address.to_ascii_lowercase()))
}

fn unavailable(reason: &str, scope: &BTreeSet<String>, details: Value) -> Value {
    json!({
        "status": "unavailable",
        "accounting_complete": false,
        "method": PROFIT_METHOD,
        "reason": reason,
        "accounting_scope": scope,
        "details": details,
    })
}

fn receipt_transfer_nets(
    receipt: &Value,
    scope: &BTreeSet<String>,
) -> LedgerResult<(BTreeMap<String, BigInt>, BTreeSet<String>, BTreeSet<String>)> {
    let mut nets = BTreeMap::<String, BigInt>::new();
    let mut pools = BTreeSet::new();
    let mut observed_tokens = BTreeSet::new();
    for log in receipt
        .get("logs")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let topics = log
            .get("topics")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let topic0 = topics
            .first()
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_ascii_lowercase();
        if matches!(topic0.as_str(), V2_SWAP_TOPIC | V3_SWAP_TOPIC) {
            if let Some(address) = log.get("address").and_then(Value::as_str) {
                pools.insert(address.to_ascii_lowercase());
            }
        }
        if topics.len() != 3 || topic0 != TRANSFER_TOPIC {
            continue;
        }
        let data = log.get("data").and_then(Value::as_str).unwrap_or_default();
        if data.strip_prefix("0x").is_none_or(|raw| raw.len() != 64) {
            continue;
        }
        let token = log
            .get("address")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                LedgerError::MalformedEvidence("Transfer log missing address".to_owned())
            })?
            .to_ascii_lowercase();
        let from = topics
            .get(1)
            .and_then(Value::as_str)
            .and_then(topic_address)
            .ok_or_else(|| {
                LedgerError::MalformedEvidence("Transfer log missing from".to_owned())
            })?;
        let to = topics
            .get(2)
            .and_then(Value::as_str)
            .and_then(topic_address)
            .ok_or_else(|| LedgerError::MalformedEvidence("Transfer log missing to".to_owned()))?;
        if !scope.contains(&from) && !scope.contains(&to) {
            continue;
        }
        observed_tokens.insert(token.clone());
        let amount = hex_biguint(Some(data))
            .map(BigInt::from)
            .ok_or_else(|| LedgerError::MalformedEvidence("invalid Transfer amount".to_owned()))?;
        let entry = nets.entry(token).or_insert_with(|| BigInt::from(0_u8));
        if scope.contains(&to) && !scope.contains(&from) && from != ZERO_ADDRESS {
            *entry += &amount;
        }
        if scope.contains(&from) && !scope.contains(&to) && to != ZERO_ADDRESS {
            *entry -= &amount;
        }
    }
    Ok((nets, observed_tokens, pools))
}

async fn token_scope_balances(
    rpc: &dyn RpcClient,
    chain_id: u64,
    token: &str,
    scope: &BTreeSet<String>,
    block: u64,
) -> LedgerResult<(BigInt, BigInt, BTreeMap<String, BigInt>)> {
    let mut before_total = BigInt::from(0_u8);
    let mut after_total = BigInt::from(0_u8);
    let mut per_address_delta = BTreeMap::new();
    for address in scope {
        let data = balance_of_data(address).ok_or_else(|| {
            LedgerError::MalformedEvidence(format!("invalid accounting address {address}"))
        })?;
        let before = rpc
            .eth_call(chain_id, token, &data, Some(block - 1))
            .await?;
        let after = rpc.eth_call(chain_id, token, &data, Some(block)).await?;
        let before = hex_biguint(Some(&before))
            .map(BigInt::from)
            .ok_or_else(|| {
                LedgerError::MalformedEvidence(format!("invalid balanceOf result for {token}"))
            })?;
        let after = hex_biguint(Some(&after)).map(BigInt::from).ok_or_else(|| {
            LedgerError::MalformedEvidence(format!("invalid balanceOf result for {token}"))
        })?;
        before_total += &before;
        after_total += &after;
        per_address_delta.insert(address.clone(), after - before);
    }
    Ok((before_total, after_total, per_address_delta))
}

/// Adds a numeric profit fact only when an explicitly configured searcher scope
/// reconciles across receipt logs, archive balance snapshots, trace-derived native
/// flows and exact Base gas (execution gas plus L1 data fee).
pub async fn verify_deterministic_profit(
    rpc: &dyn RpcClient,
    config: &PilotConfig,
    analysis: &TransactionAnalysis,
) -> LedgerResult<Option<Value>> {
    let has_rule = config
        .rules
        .iter()
        .any(|rule| rule.enabled && rule.event_type == PilotEventType::DexArbitrage);
    if !has_rule {
        return Ok(None);
    }
    let scope = config
        .entities
        .iter()
        .filter(|entity| entity.include && scope_roles(&entity.role))
        .map(|entity| entity.address.to_ascii_lowercase())
        .collect::<BTreeSet<_>>();
    verify_deterministic_profit_for_scope(rpc, analysis, &scope).await
}

/// Chain-neutral entry point for the same fail-closed accounting method. The
/// caller must provide an explicit scope backed by persisted evidence; this
/// function never discovers or guesses related owners/executors.
pub async fn verify_deterministic_profit_for_scope(
    rpc: &dyn RpcClient,
    analysis: &TransactionAnalysis,
    scope: &BTreeSet<String>,
) -> LedgerResult<Option<Value>> {
    if scope.is_empty() {
        return Ok(Some(unavailable(
            "no explicitly configured searcher accounting scope",
            scope,
            json!({}),
        )));
    }
    if analysis.status != TransactionStatus::Success {
        return Ok(Some(unavailable(
            "transaction did not succeed",
            scope,
            json!({}),
        )));
    }
    let Some(block) = analysis.block_number.filter(|block| *block > 0) else {
        return Ok(Some(unavailable(
            "block number unavailable",
            scope,
            json!({}),
        )));
    };
    if !analysis
        .normalized_execution_facts
        .derived_facts
        .get("trace_available")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return Ok(Some(unavailable("trace unavailable", scope, json!({}))));
    }

    let receipt = rpc
        .get_receipt(analysis.chain_id, &analysis.tx_hash)
        .await?;
    if receipt.get("status").and_then(Value::as_str) != Some("0x1") {
        return Ok(Some(unavailable(
            "RPC receipt is not successful",
            scope,
            json!({}),
        )));
    }
    if rpc
        .get_trace(analysis.chain_id, &analysis.tx_hash)
        .await?
        .is_none()
    {
        return Ok(Some(unavailable("RPC trace unavailable", scope, json!({}))));
    }
    let (log_nets, tokens, pools) = receipt_transfer_nets(&receipt, scope)?;
    if pools.len() < 2 {
        return Ok(Some(unavailable(
            "fewer than two distinct deterministic swap-log emitters",
            scope,
            json!({"distinct_swap_pools": pools}),
        )));
    }
    if tokens.is_empty() {
        return Ok(Some(unavailable(
            "no standard ERC-20 scope flows",
            scope,
            json!({}),
        )));
    }

    let block_ref = format!("0x{block:x}");
    let block_logs = rpc
        .get_logs(
            analysis.chain_id,
            json!({"fromBlock": block_ref, "toBlock": block_ref, "topics": [TRANSFER_TOPIC]}),
        )
        .await?;
    let target_hash = analysis.tx_hash.to_ascii_lowercase();
    let same_block_other_scope_transfer = block_logs.iter().any(|log| {
        let topics = log.get("topics").and_then(Value::as_array);
        let Some(topics) = topics.filter(|topics| topics.len() == 3) else {
            return false;
        };
        let from = topics
            .get(1)
            .and_then(Value::as_str)
            .and_then(topic_address);
        let to = topics
            .get(2)
            .and_then(Value::as_str)
            .and_then(topic_address);
        let involves_scope = from.as_ref().is_some_and(|value| scope.contains(value))
            || to.as_ref().is_some_and(|value| scope.contains(value));
        involves_scope
            && log
                .get("transactionHash")
                .and_then(Value::as_str)
                .is_some_and(|hash| !hash.eq_ignore_ascii_case(&target_hash))
    });
    if same_block_other_scope_transfer {
        return Ok(Some(unavailable(
            "another transaction changed a scoped token in the same block",
            scope,
            json!({"block_number": block}),
        )));
    }

    let mut state_nets = BTreeMap::new();
    let mut recipient_deltas = BTreeMap::<String, BTreeMap<String, String>>::new();
    for token in &tokens {
        let (before, after, per_address) =
            token_scope_balances(rpc, analysis.chain_id, token, scope, block).await?;
        let state_net = after - before;
        if log_nets.get(token) != Some(&state_net) {
            return Ok(Some(unavailable(
                "receipt Transfer net does not match archive balance delta",
                scope,
                json!({
                    "token_address": token,
                    "receipt_net_raw": log_nets.get(token).map(ToString::to_string),
                    "state_net_raw": state_net.to_string(),
                }),
            )));
        }
        recipient_deltas.insert(
            token.clone(),
            per_address
                .into_iter()
                .map(|(address, delta)| (address, delta.to_string()))
                .collect(),
        );
        state_nets.insert(token.clone(), state_net);
    }

    let gas_used = hex_biguint(receipt.get("gasUsed").and_then(Value::as_str))
        .ok_or_else(|| LedgerError::MalformedEvidence("receipt gasUsed missing".to_owned()))?;
    let gas_price = hex_biguint(receipt.get("effectiveGasPrice").and_then(Value::as_str))
        .ok_or_else(|| {
            LedgerError::MalformedEvidence("receipt effectiveGasPrice missing".to_owned())
        })?;
    let l1_fee = hex_biguint(receipt.get("l1Fee").and_then(Value::as_str))
        .unwrap_or_else(|| BigUint::from(0_u8));
    let execution_gas = &gas_used * &gas_price;
    let total_gas = &execution_gas + &l1_fee;
    let analysis_gas = analysis
        .normalized_execution_facts
        .derived_facts
        .get("gas_cost_raw")
        .and_then(Value::as_str)
        .and_then(|value| BigUint::parse_bytes(value.as_bytes(), 10));
    if analysis_gas.as_ref() != Some(&total_gas) {
        return Ok(Some(unavailable(
            "analysis gas cost does not reconcile with receipt execution gas plus L1 fee",
            scope,
            json!({
                "analysis_gas_cost_raw": analysis_gas.map(|value| value.to_string()),
                "execution_gas_cost_raw": execution_gas.to_string(),
                "l1_fee_raw": l1_fee.to_string(),
                "rpc_gas_cost_raw": total_gas.to_string(),
            }),
        )));
    }

    let mut native_flow_net = BigInt::from(0_u8);
    for flow in analysis
        .derived_flows
        .iter()
        .filter(|flow| flow.asset_type == AssetType::Native)
    {
        let amount = decimal_bigint(&flow.amount_raw).ok_or_else(|| {
            LedgerError::MalformedEvidence("invalid native flow amount".to_owned())
        })?;
        let from_scope = scope.contains(&flow.from_address.to_ascii_lowercase());
        let to_scope = scope.contains(&flow.to_address.to_ascii_lowercase());
        if to_scope && !from_scope {
            native_flow_net += &amount;
        }
        if from_scope && !to_scope {
            native_flow_net -= &amount;
        }
    }
    let mut native_before = BigInt::from(0_u8);
    let mut native_after = BigInt::from(0_u8);
    for address in scope {
        native_before += BigInt::from(
            hex_biguint(Some(
                &rpc.get_balance(analysis.chain_id, address, Some(block - 1))
                    .await?,
            ))
            .ok_or_else(|| LedgerError::MalformedEvidence("invalid native balance".to_owned()))?,
        );
        native_after += BigInt::from(
            hex_biguint(Some(
                &rpc.get_balance(analysis.chain_id, address, Some(block))
                    .await?,
            ))
            .ok_or_else(|| LedgerError::MalformedEvidence("invalid native balance".to_owned()))?,
        );
    }
    let sender_in_scope = scope.contains(&analysis.from_address.to_ascii_lowercase());
    let expected_native_state_net = native_flow_net.clone()
        - if sender_in_scope {
            BigInt::from(total_gas.clone())
        } else {
            BigInt::from(0_u8)
        };
    let native_state_net = native_after - native_before;
    if native_state_net != expected_native_state_net {
        return Ok(Some(unavailable(
            "trace native flows and exact gas do not match archive native balance delta",
            scope,
            json!({
                "trace_native_net_raw": native_flow_net.to_string(),
                "expected_state_net_raw": expected_native_state_net.to_string(),
                "state_net_raw": native_state_net.to_string(),
            }),
        )));
    }

    let zero = BigInt::from(0_u8);
    let positives = state_nets
        .iter()
        .filter(|(_, value)| **value > zero)
        .collect::<Vec<_>>();
    let negatives = state_nets
        .iter()
        .filter(|(_, value)| **value < zero)
        .map(|(token, value)| (token.clone(), value.to_string()))
        .collect::<BTreeMap<_, _>>();
    if positives.len() != 1 || !negatives.is_empty() || native_flow_net != zero {
        return Ok(Some(unavailable(
            "scope does not have one unambiguous token surplus with no principal outflow",
            scope,
            json!({
                "token_nets": state_nets.iter().map(|(token, value)| (token.clone(), value.to_string())).collect::<BTreeMap<_, _>>(),
                "negative_token_nets": negatives,
                "trace_native_net_raw": native_flow_net.to_string(),
            }),
        )));
    }

    let (profit_token, profit_raw) = positives[0];
    let recipients = recipient_deltas
        .get(profit_token)
        .into_iter()
        .flat_map(|values| values.iter())
        .filter(|(_, delta)| decimal_bigint(delta).is_some_and(|value| value > zero))
        .map(|(address, _)| address.clone())
        .collect::<Vec<_>>();
    Ok(Some(json!({
        "status": "verified",
        "accounting_complete": true,
        "method": PROFIT_METHOD,
        "profit_raw": profit_raw.to_string(),
        "profit_token_address": profit_token,
        "profit_recipient": (recipients.len() == 1).then(|| recipients[0].clone()),
        "accounting_scope": scope,
        "distinct_swap_pools": pools,
        "execution_gas_cost_raw": execution_gas.to_string(),
        "l1_fee_raw": l1_fee.to_string(),
        "gas_cost_raw": total_gas.to_string(),
        "token_nets": state_nets.iter().map(|(token, value)| (token.clone(), value.to_string())).collect::<BTreeMap<_, _>>(),
        "evidence_refs": [
            analysis.tx_hash,
            format!("{}:receipt", analysis.tx_hash),
            format!("{}:trace", analysis.tx_hash),
            format!("{}:archive-balances:{}-{}", analysis.tx_hash, block - 1, block),
        ],
        "limitations": [
            "the positive token surplus is exact only for the explicitly configured accounting scope; net profit after unlike-asset gas conversion is not calculated",
            "non-standard assets that change balances without ERC-20 Transfer logs are excluded"
        ]
    })))
}
