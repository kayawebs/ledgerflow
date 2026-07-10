use std::{env, time::Duration};

use ledgerflow::{
    core::{
        env::load_dotenv,
        errors::{LedgerError, LedgerResult},
        logging::init_logging,
    },
    indexer::{
        clickhouse::ClickHouseClient,
        config::IndexerConfig,
        models::{AddressTxDirection, AddressTxRef, normalize_address},
    },
};
use serde_json::Value;
use tokio::time::sleep;
use tracing::{error, info, warn};

const DEFAULT_CRYPTOHOUSE_URL: &str = "https://crypto-clickhouse.clickhouse.com/?user=crypto";
const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";
const CRYPTOHOUSE_PAGE_LIMIT: u32 = 1_000;

#[derive(Clone, Debug)]
struct CryptoHouseIndexerBackfillConfig {
    enabled: bool,
    chain_id: u32,
    cryptohouse_url: String,
    worker_id: String,
    start_block: u64,
    end_block: Option<u64>,
    batch_size: u64,
    request_delay_ms: u64,
    query_timeout_secs: u64,
    quota_backoff_secs: u64,
    idle_on_complete: bool,
}

impl CryptoHouseIndexerBackfillConfig {
    fn from_env(indexer_config: &IndexerConfig) -> Self {
        Self {
            enabled: parse_bool("LEDGERFLOW_CRYPTOHOUSE_INDEXER_BACKFILL_ENABLED").unwrap_or(false),
            chain_id: parse_u32("LEDGERFLOW_CRYPTOHOUSE_INDEXER_BACKFILL_CHAIN_ID")
                .unwrap_or(indexer_config.chain_id),
            cryptohouse_url: env::var("LEDGERFLOW_CRYPTOHOUSE_INDEXER_BACKFILL_URL")
                .or_else(|_| env::var("LEDGERFLOW_TRACE_MONITOR_CRYPTOHOUSE_URL"))
                .unwrap_or_else(|_| DEFAULT_CRYPTOHOUSE_URL.to_owned()),
            worker_id: env::var("LEDGERFLOW_CRYPTOHOUSE_INDEXER_BACKFILL_WORKER_ID")
                .unwrap_or_else(|_| "base_mainnet_cryptohouse_backfill".to_owned()),
            start_block: parse_u64("LEDGERFLOW_CRYPTOHOUSE_INDEXER_BACKFILL_START_BLOCK")
                .unwrap_or(indexer_config.start_block),
            end_block: parse_u64("LEDGERFLOW_CRYPTOHOUSE_INDEXER_BACKFILL_END_BLOCK")
                .or(indexer_config.end_block),
            batch_size: parse_u64("LEDGERFLOW_CRYPTOHOUSE_INDEXER_BACKFILL_BATCH_SIZE")
                .unwrap_or(500)
                .clamp(1, 5_000),
            request_delay_ms: parse_u64("LEDGERFLOW_CRYPTOHOUSE_INDEXER_BACKFILL_REQUEST_DELAY_MS")
                .unwrap_or(100),
            query_timeout_secs: parse_u64(
                "LEDGERFLOW_CRYPTOHOUSE_INDEXER_BACKFILL_QUERY_TIMEOUT_SECS",
            )
            .unwrap_or(60)
            .clamp(5, 600),
            quota_backoff_secs: parse_u64(
                "LEDGERFLOW_CRYPTOHOUSE_INDEXER_BACKFILL_QUOTA_BACKOFF_SECS",
            )
            .unwrap_or(600)
            .clamp(60, 3_600),
            idle_on_complete: parse_bool(
                "LEDGERFLOW_CRYPTOHOUSE_INDEXER_BACKFILL_IDLE_ON_COMPLETE",
            )
            .unwrap_or(true),
        }
    }
}

#[tokio::main]
async fn main() {
    load_dotenv();
    init_logging();
    if let Err(err) = run().await {
        error!(error = %err, "cryptohouse indexer backfill failed");
        std::process::exit(1);
    }
}

async fn run() -> LedgerResult<()> {
    let indexer_config = IndexerConfig::from_env();
    let config = CryptoHouseIndexerBackfillConfig::from_env(&indexer_config);
    if !config.enabled {
        info!("cryptohouse indexer backfill disabled");
        idle_forever().await;
    }
    let Some(end_block) = config.end_block else {
        warn!(
            "cryptohouse indexer backfill needs LEDGERFLOW_CRYPTOHOUSE_INDEXER_BACKFILL_END_BLOCK"
        );
        idle_forever().await;
    };

    let clickhouse = ClickHouseClient::new(
        indexer_config.clickhouse_url,
        indexer_config.clickhouse_database,
        indexer_config.clickhouse_user,
        indexer_config.clickhouse_password,
    );
    clickhouse.bootstrap().await?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(config.query_timeout_secs))
        .build()
        .map_err(|err| LedgerError::TransientIngestion(err.to_string()))?;
    let checkpoint = clickhouse
        .get_progress(config.chain_id, &config.worker_id)
        .await?;
    let mut next_block = checkpoint
        .map(|block| block.saturating_add(1))
        .unwrap_or(config.start_block)
        .max(config.start_block);

    info!(
        chain_id = config.chain_id,
        worker_id = %config.worker_id,
        start_block = next_block,
        end_block,
        batch_size = config.batch_size,
        "cryptohouse indexer backfill started"
    );

    while next_block <= end_block {
        let batch_end = end_block.min(
            next_block
                .saturating_add(config.batch_size)
                .saturating_sub(1),
        );
        let (tx_count, refs) = match fetch_address_refs(
            &client,
            &config.cryptohouse_url,
            config.chain_id,
            next_block,
            batch_end,
            config.request_delay_ms,
        )
        .await
        {
            Ok(result) => result,
            Err(err) if is_quota_exceeded(&err) => {
                warn!(
                    start_block = next_block,
                    end_block = batch_end,
                    backoff_secs = config.quota_backoff_secs,
                    "cryptohouse quota exceeded; backing off"
                );
                sleep(Duration::from_secs(config.quota_backoff_secs)).await;
                continue;
            }
            Err(err) => return Err(err),
        };
        clickhouse.insert_refs(&refs).await?;
        clickhouse
            .set_progress(config.chain_id, &config.worker_id, batch_end)
            .await?;
        info!(
            start_block = next_block,
            end_block = batch_end,
            tx_count,
            ref_count = refs.len(),
            "cryptohouse indexer backfill batch imported"
        );
        next_block = batch_end.saturating_add(1);
        sleep_if_configured(config.request_delay_ms).await;
    }

    info!(
        chain_id = config.chain_id,
        worker_id = %config.worker_id,
        end_block,
        "cryptohouse indexer backfill completed"
    );
    if config.idle_on_complete {
        idle_forever().await;
    }
    Ok(())
}

async fn fetch_address_refs(
    client: &reqwest::Client,
    cryptohouse_url: &str,
    chain_id: u32,
    start_block: u64,
    end_block: u64,
    request_delay_ms: u64,
) -> LedgerResult<(usize, Vec<AddressTxRef>)> {
    let mut tx_count = 0_usize;
    let mut refs = Vec::new();
    let mut cursor = None;
    loop {
        let query =
            cryptohouse_transactions_query(start_block, end_block, cursor, CRYPTOHOUSE_PAGE_LIMIT);
        let response = client
            .post(cryptohouse_url)
            .body(query)
            .send()
            .await
            .map_err(|err| LedgerError::TransientIngestion(err.to_string()))?;
        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|err| LedgerError::TransientIngestion(err.to_string()))?;
        if !status.is_success() {
            return Err(LedgerError::TransientIngestion(format!(
                "cryptohouse transactions query failed with status {status}: {}",
                truncate_for_error(&body, 500)
            )));
        }

        let mut page_rows = 0_usize;
        let mut last_cursor = cursor;
        for line in body.lines().map(str::trim).filter(|line| !line.is_empty()) {
            let row: Value = serde_json::from_str(line).map_err(|err| {
                LedgerError::MalformedEvidence(format!(
                    "cryptohouse transaction JSONEachRow parse failed: {err}; row={}",
                    truncate_for_error(line, 300)
                ))
            })?;
            append_refs_from_transaction_row(chain_id, &row, &mut refs)?;
            let block_number = json_u64(row.get("block_number")).ok_or_else(|| {
                LedgerError::MalformedEvidence("row missing block_number".to_owned())
            })?;
            let tx_index = json_u64(row.get("transaction_index")).ok_or_else(|| {
                LedgerError::MalformedEvidence("row missing transaction_index".to_owned())
            })?;
            last_cursor = Some((block_number, tx_index));
            tx_count = tx_count.saturating_add(1);
            page_rows = page_rows.saturating_add(1);
        }
        if page_rows < CRYPTOHOUSE_PAGE_LIMIT as usize {
            break;
        }
        if last_cursor == cursor {
            return Err(LedgerError::MalformedEvidence(
                "cryptohouse transaction cursor did not advance".to_owned(),
            ));
        }
        cursor = last_cursor;
        sleep_if_configured(request_delay_ms).await;
    }
    Ok((tx_count, refs))
}

fn cryptohouse_transactions_query(
    start_block: u64,
    end_block: u64,
    cursor: Option<(u64, u64)>,
    limit: u32,
) -> String {
    let cursor_condition = cursor
        .map(|(block_number, tx_index)| {
            format!(
                "  AND (block_number > {block_number} OR (block_number = {block_number} AND transaction_index > {tx_index}))"
            )
        })
        .unwrap_or_default();
    format!(
        r#"
SELECT
  lower(toString(hash)) AS tx_hash,
  block_number,
  transaction_index,
  lower(toString(from_address)) AS from_address,
  lower(toString(to_address)) AS to_address
FROM base.transactions
WHERE block_number BETWEEN {start_block} AND {end_block}
{cursor_condition}
ORDER BY block_number, transaction_index
LIMIT {limit}
FORMAT JSONEachRow
"#
    )
}

fn append_refs_from_transaction_row(
    chain_id: u32,
    row: &Value,
    refs: &mut Vec<AddressTxRef>,
) -> LedgerResult<()> {
    let tx_hash = row
        .get("tx_hash")
        .and_then(Value::as_str)
        .map(normalize_hash)
        .ok_or_else(|| LedgerError::MalformedEvidence("row missing tx_hash".to_owned()))?;
    let block_number = json_u64(row.get("block_number"))
        .ok_or_else(|| LedgerError::MalformedEvidence("row missing block_number".to_owned()))?
        as u32;
    let tx_index = json_u64(row.get("transaction_index"))
        .ok_or_else(|| LedgerError::MalformedEvidence("row missing transaction_index".to_owned()))?
        as u32;
    let from_address =
        normalize_evm_address(row.get("from_address").and_then(Value::as_str).ok_or_else(
            || LedgerError::MalformedEvidence("row missing from_address".to_owned()),
        )?)?;
    let to_address = row
        .get("to_address")
        .and_then(Value::as_str)
        .and_then(normalize_optional_to_address)
        .transpose()?;

    refs.push(AddressTxRef {
        chain_id,
        address: from_address.clone(),
        tx_hash: tx_hash.clone(),
        block_number,
        tx_index,
        direction: AddressTxDirection::From,
        from_address: Some(from_address.clone()),
        to_address: to_address.clone(),
    });

    if let Some(to_address) = to_address {
        refs.push(AddressTxRef {
            chain_id,
            address: to_address.clone(),
            tx_hash,
            block_number,
            tx_index,
            direction: AddressTxDirection::To,
            from_address: Some(from_address),
            to_address: Some(to_address),
        });
    }
    Ok(())
}

fn normalize_optional_to_address(value: &str) -> Option<LedgerResult<String>> {
    let value = normalize_hash(value);
    if value.is_empty() || value == "0x" || value == ZERO_ADDRESS {
        None
    } else {
        Some(normalize_address(&value))
    }
}

fn normalize_evm_address(value: &str) -> LedgerResult<String> {
    normalize_address(&normalize_hash(value))
}

fn normalize_hash(value: &str) -> String {
    let value = value.trim().to_ascii_lowercase();
    if value.starts_with("0x") {
        value
    } else {
        format!("0x{value}")
    }
}

fn json_u64(value: Option<&Value>) -> Option<u64> {
    match value {
        Some(Value::Number(number)) => number.as_u64(),
        Some(Value::String(value)) => value.parse::<u64>().ok(),
        _ => None,
    }
}

fn parse_u64(key: &str) -> Option<u64> {
    env::var(key)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
}

fn parse_u32(key: &str) -> Option<u32> {
    env::var(key)
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
}

fn parse_bool(key: &str) -> Option<bool> {
    env::var(key).ok().map(|value| {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "true" | "1" | "yes"
        )
    })
}

fn truncate_for_error(value: &str, max_chars: usize) -> String {
    value.chars().take(max_chars).collect()
}

fn is_quota_exceeded(err: &LedgerError) -> bool {
    err.to_string().contains("QUOTA_EXCEEDED")
}

async fn sleep_if_configured(delay_ms: u64) {
    if delay_ms > 0 {
        sleep(Duration::from_millis(delay_ms)).await;
    }
}

async fn idle_forever() -> ! {
    loop {
        sleep(Duration::from_secs(60)).await;
    }
}
