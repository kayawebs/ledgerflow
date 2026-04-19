use serde::Serialize;
use serde_json::Value;

use crate::core::errors::{LedgerError, LedgerResult};
use crate::indexer::models::normalize_address;
use crate::indexer::rpc::IndexerRpcClient;

#[derive(Clone)]
pub struct BaseEthPaymentVerifier {
    rpc: IndexerRpcClient,
    treasury_address: String,
    min_confirmations: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct VerifiedBaseEthPayment {
    pub tx_hash: String,
    pub from_address: String,
    pub to_address: String,
    pub amount_wei: String,
    pub block_number: u64,
    pub confirmations: u64,
}

impl BaseEthPaymentVerifier {
    pub fn new(
        rpc_url: String,
        treasury_address: String,
        min_confirmations: u64,
    ) -> LedgerResult<Self> {
        Ok(Self {
            rpc: IndexerRpcClient::new(rpc_url),
            treasury_address: normalize_address(&treasury_address)?,
            min_confirmations,
        })
    }

    pub async fn verify(
        &self,
        tx_hash: &str,
        expected_from: Option<&str>,
        minimum_amount_wei: u128,
    ) -> LedgerResult<VerifiedBaseEthPayment> {
        let transaction = self.rpc.get_transaction_by_hash(tx_hash).await?;
        if transaction.is_null() {
            return Err(LedgerError::Conflict(
                "payment transaction not found".to_owned(),
            ));
        }
        let receipt = self.rpc.get_transaction_receipt(tx_hash).await?;
        if receipt.is_null() {
            return Err(LedgerError::Conflict(
                "payment receipt not available yet".to_owned(),
            ));
        }

        let status = receipt
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if status != "0x1" {
            return Err(LedgerError::Conflict(
                "payment transaction did not succeed".to_owned(),
            ));
        }

        let from_address =
            normalize_address(transaction.get("from").and_then(Value::as_str).ok_or_else(
                || LedgerError::MalformedEvidence("payment tx missing from".to_owned()),
            )?)?;
        let to_address = normalize_address(
            transaction
                .get("to")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    LedgerError::MalformedEvidence("payment tx missing to".to_owned())
                })?,
        )?;
        if to_address != self.treasury_address {
            return Err(LedgerError::Conflict(format!(
                "payment target mismatch: expected {}, got {}",
                self.treasury_address, to_address
            )));
        }

        if let Some(expected_from) = expected_from {
            let expected_from = normalize_address(expected_from)?;
            if from_address != expected_from {
                return Err(LedgerError::Conflict(format!(
                    "payment sender mismatch: expected {expected_from}, got {from_address}"
                )));
            }
        }

        let amount_wei = parse_hex_u128(
            transaction
                .get("value")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    LedgerError::MalformedEvidence("payment tx missing value".to_owned())
                })?,
        )?;
        if amount_wei < minimum_amount_wei {
            return Err(LedgerError::PaymentRequired(format!(
                "payment amount too low: expected at least {minimum_amount_wei}, got {amount_wei}"
            )));
        }

        let block_number = parse_hex_u64(
            receipt
                .get("blockNumber")
                .or_else(|| transaction.get("blockNumber"))
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    LedgerError::MalformedEvidence("payment tx missing block number".to_owned())
                })?,
        )?;
        let latest_block = self.rpc.get_latest_block_number().await?;
        let confirmations = latest_block.saturating_sub(block_number).saturating_add(1);
        if confirmations < self.min_confirmations {
            return Err(LedgerError::Conflict(format!(
                "payment has {confirmations} confirmations; {} required",
                self.min_confirmations
            )));
        }

        Ok(VerifiedBaseEthPayment {
            tx_hash: tx_hash.to_lowercase(),
            from_address,
            to_address,
            amount_wei: amount_wei.to_string(),
            block_number,
            confirmations,
        })
    }
}

fn parse_hex_u64(value: &str) -> LedgerResult<u64> {
    u64::from_str_radix(value.trim_start_matches("0x"), 16)
        .map_err(|err| LedgerError::MalformedEvidence(err.to_string()))
}

fn parse_hex_u128(value: &str) -> LedgerResult<u128> {
    u128::from_str_radix(value.trim_start_matches("0x"), 16)
        .map_err(|err| LedgerError::MalformedEvidence(err.to_string()))
}
