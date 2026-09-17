use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

use chrono::{DateTime, TimeZone, Utc};
use num_bigint::BigUint;
use serde::Serialize;
use serde_json::{Value, json};
use sqlx::{PgPool, Row, types::Json};
use uuid::Uuid;

use crate::{
    core::errors::{LedgerError, LedgerResult},
    indexer::rpc::IndexerRpcClient,
    services::token_metadata::TokenMetadataService,
};

pub const PONS_V1_TOKEN_LAUNCHED_TOPIC: &str =
    "0xdb51ea9ad51ab453a65a4cb7e60c3cb378c9501bb002609f8f97778fb6c4235a";
pub const PONS_V2_TOKEN_LAUNCHED_TOPIC: &str =
    "0x8d4aad4953d0ca700d468f3753aa14432d1b35b43ec6409f051fb6aa43a89607";
pub const PONS_V2_LAUNCH_SWEPT_TOPIC: &str =
    "0xcdb72f157fd3666758a6ce201387ffb52038c7562e4fff352828da1096c4b6b4";
pub const PONS_V2_POOL_GRADUATED_TOPIC: &str =
    "0x0a44ef75df69c534f43cd6c1aa3ef8983065fe5fe79ef9e79f6494e6f258c259";
pub const PONS_V2_CURVE_BUY_TOPIC: &str =
    "0xec36bf571f136799e8dc0b0b8bea4b04d8bd3d43de838aab0d5fc21d4cbfc455";
pub const PONS_V2_CURVE_SELL_TOPIC: &str =
    "0x8113d738abdcb6b38357e9d53a54a7157861a09031b453651f0fe7fe151f59df";

#[derive(Clone, Debug)]
pub struct ProtocolLaunchIndexerConfig {
    pub chain_id: u64,
    pub worker_id_prefix: String,
    pub batch_size: u64,
    pub head_lag_blocks: u64,
    pub lookback_blocks: u64,
    pub header_batch_size: usize,
    pub metadata_fetch_limit: usize,
    pub metadata_fetch_timeout_ms: u64,
    /// Operational bootstrap/validation floor. The database seed's deployment block remains the
    /// default and source of truth when this is unset.
    pub start_block_override: Option<u64>,
}

#[derive(Clone)]
pub struct ProtocolLaunchIndexerService {
    pool: PgPool,
    rpc: IndexerRpcClient,
    token_metadata: TokenMetadataService,
    config: ProtocolLaunchIndexerConfig,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct ProtocolLaunchIndexerIteration {
    pub chain_head: u64,
    pub safe_head: u64,
    pub domains_considered: u64,
    pub domains_processed: u64,
    pub ranges_processed: u64,
    pub logs_observed: u64,
    pub curve_trade_logs_observed: u64,
    pub lifecycle_events_projected: u64,
    pub curve_trades_projected: u64,
    pub launches_projected: u64,
    pub graduations_projected: u64,
    pub token_metadata_ready: u64,
    pub caught_up_domains: u64,
}

#[derive(Clone, Debug)]
struct LaunchDomain {
    domain_id: Uuid,
    chain_id: u64,
    domain_key: String,
    protocol_key: String,
    factory_address: String,
    valid_from_block: u64,
}

#[derive(Clone, Debug)]
struct RpcLog {
    address: String,
    block_number: u64,
    block_hash: String,
    tx_hash: String,
    tx_index: u64,
    log_index: u64,
    topics: Vec<String>,
    data: String,
}

#[derive(Clone, Debug)]
struct CurveLaunchRef {
    launch_id: Uuid,
    token_address: String,
}

#[derive(Clone, Debug)]
struct ParsedCurveTrade {
    trader_address: String,
    recipient_address: String,
    trade_side: &'static str,
    quote_amount_raw: String,
    token_amount_raw: String,
    fee_amount_raw: String,
    tax_amount_raw: String,
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "event_type", rename_all = "snake_case")]
enum ParsedLaunchEvent {
    TokenLaunched {
        token: String,
        creator: String,
        curve: Option<String>,
        quote_token: String,
        pool: Option<String>,
        launch_config_id: String,
        graduation_threshold_raw: Option<String>,
        position_id: Option<String>,
        facts: Value,
    },
    LaunchSwept {
        token: String,
        quote_out_raw: String,
        token_out_raw: String,
    },
    PoolGraduated {
        token: String,
        position_id: String,
        token_amount_raw: String,
        pair_token_amount_raw: String,
    },
}

impl ParsedLaunchEvent {
    fn event_type(&self) -> &'static str {
        match self {
            Self::TokenLaunched { .. } => "token_launched",
            Self::LaunchSwept { .. } => "launch_swept",
            Self::PoolGraduated { .. } => "pool_graduated",
        }
    }

    fn token(&self) -> &str {
        match self {
            Self::TokenLaunched { token, .. }
            | Self::LaunchSwept { token, .. }
            | Self::PoolGraduated { token, .. } => token,
        }
    }

    fn curve(&self) -> Option<&str> {
        match self {
            Self::TokenLaunched { curve, .. } => curve.as_deref(),
            _ => None,
        }
    }
}

impl ProtocolLaunchIndexerService {
    pub fn new(
        pool: PgPool,
        rpc: IndexerRpcClient,
        token_metadata: TokenMetadataService,
        config: ProtocolLaunchIndexerConfig,
    ) -> Self {
        Self {
            pool,
            rpc,
            token_metadata,
            config,
        }
    }

    pub async fn process_once(&self) -> LedgerResult<ProtocolLaunchIndexerIteration> {
        let chain_head = self.rpc.get_latest_block_number().await?;
        let safe_head = chain_head.saturating_sub(self.config.head_lag_blocks);
        let domains = self.load_domains().await?;
        let mut result = ProtocolLaunchIndexerIteration {
            chain_head,
            safe_head,
            domains_considered: domains.len() as u64,
            ..ProtocolLaunchIndexerIteration::default()
        };
        let mut errors = Vec::new();

        for domain in domains {
            match self.process_domain(&domain, safe_head).await {
                Ok(domain_result) => {
                    result.domains_processed += 1;
                    result.ranges_processed += u64::from(domain_result.range_processed);
                    result.logs_observed += domain_result.logs_observed;
                    result.curve_trade_logs_observed += domain_result.curve_trade_logs_observed;
                    result.lifecycle_events_projected += domain_result.projected;
                    result.curve_trades_projected += domain_result.curve_trades_projected;
                    result.launches_projected += domain_result.launches;
                    result.graduations_projected += domain_result.graduations;
                    result.caught_up_domains += u64::from(domain_result.caught_up);
                }
                Err(error) => {
                    self.mark_cursor_error(&domain, &error.to_string())
                        .await
                        .ok();
                    if domain.protocol_key == "pons_v2" {
                        self.mark_curve_cursor_error(&domain, &error.to_string())
                            .await
                            .ok();
                    }
                    errors.push(format!("{}: {error}", domain.domain_key));
                }
            }
        }

        if result.domains_processed == 0 && !errors.is_empty() {
            return Err(LedgerError::TransientIngestion(errors.join("; ")));
        }
        result.token_metadata_ready = self.enrich_token_metadata().await?;
        Ok(result)
    }

    async fn load_domains(&self) -> LedgerResult<Vec<LaunchDomain>> {
        let rows = sqlx::query(
            r#"select domains.domain_id, domains.chain_id, domains.domain_key, domains.protocol_key,
                      seeds.address, seeds.valid_from_block
               from protocol_intelligence_domains domains
               join protocol_seed_addresses seeds
                 on seeds.domain_id=domains.domain_id and seeds.chain_id=domains.chain_id
               where domains.chain_id=$1 and domains.domain_kind='token_launch'
                 and domains.status='active' and seeds.status='active'
                 and seeds.seed_role='launch_factory'
               order by domains.domain_key, seeds.address"#,
        )
        .bind(self.config.chain_id as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(persistence_error)?;

        rows.into_iter()
            .map(|row| {
                let valid_from =
                    row.get::<Option<i64>, _>("valid_from_block")
                        .ok_or_else(|| {
                            LedgerError::MalformedEvidence(format!(
                                "launch factory {} is missing valid_from_block",
                                row.get::<String, _>("address")
                            ))
                        })?;
                Ok(LaunchDomain {
                    domain_id: row.get("domain_id"),
                    chain_id: row.get::<i64, _>("chain_id").max(0) as u64,
                    domain_key: row.get("domain_key"),
                    protocol_key: row.get("protocol_key"),
                    factory_address: row.get("address"),
                    valid_from_block: valid_from.max(0) as u64,
                })
            })
            .collect()
    }

    async fn process_domain(
        &self,
        domain: &LaunchDomain,
        safe_head: u64,
    ) -> LedgerResult<DomainIteration> {
        let domain_start = self
            .config
            .start_block_override
            .unwrap_or(domain.valid_from_block)
            .max(domain.valid_from_block);
        if domain_start > safe_head {
            return Ok(DomainIteration {
                caught_up: true,
                ..DomainIteration::default()
            });
        }
        let cursor = self.load_cursor(domain).await?;
        let start = match cursor {
            None => domain_start,
            Some(last) if last >= safe_head => last
                .saturating_sub(self.config.lookback_blocks.saturating_sub(1))
                .max(domain_start),
            Some(last) => last.saturating_add(1).max(domain_start),
        };
        let end = safe_head.min(
            start
                .saturating_add(self.config.batch_size.max(1))
                .saturating_sub(1),
        );
        self.mark_cursor_running(domain, start, end).await?;
        let curve_range = if domain.protocol_key == "pons_v2" {
            let curve_cursor = self.load_curve_cursor(domain).await?;
            let curve_start = match curve_cursor {
                None => domain_start,
                Some(last) if last >= safe_head => last
                    .saturating_sub(self.config.lookback_blocks.saturating_sub(1))
                    .max(domain_start),
                Some(last) => last.saturating_add(1).max(domain_start),
            };
            let curve_end = safe_head.min(
                curve_start
                    .saturating_add(self.config.batch_size.max(1))
                    .saturating_sub(1),
            );
            self.mark_curve_cursor_running(domain, curve_start, curve_end)
                .await?;
            Some((curve_start, curve_end))
        } else {
            None
        };

        let topics = topics_for_protocol(&domain.protocol_key)?;
        let payload = self
            .rpc
            .get_logs(json!({
                "address": domain.factory_address,
                "fromBlock": format!("0x{start:x}"),
                "toBlock": format!("0x{end:x}"),
                "topics": [topics],
            }))
            .await?;
        let mut logs = payload
            .into_iter()
            .filter(|value| {
                !value
                    .get("removed")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
            })
            .map(parse_rpc_log)
            .collect::<LedgerResult<Vec<_>>>()?;
        logs.sort_by_key(|log| (log.block_number, log.tx_index, log.log_index));

        // Pons V2 curve contracts are created dynamically, so the factory cannot be used as an
        // emitter filter. LedgerFlow queries the two exact topics on its own RPC and accepts only
        // emitters that map to a launch already discovered for this domain.
        let mut curve_logs = if let Some((curve_start, curve_end)) = curve_range {
            self.rpc
                .get_logs(json!({
                    "fromBlock": format!("0x{curve_start:x}"),
                    "toBlock": format!("0x{curve_end:x}"),
                    "topics": [[PONS_V2_CURVE_BUY_TOPIC, PONS_V2_CURVE_SELL_TOPIC]],
                }))
                .await?
                .into_iter()
                .filter(|value| {
                    !value
                        .get("removed")
                        .and_then(Value::as_bool)
                        .unwrap_or(false)
                })
                .map(parse_rpc_log)
                .collect::<LedgerResult<Vec<_>>>()?
        } else {
            Vec::new()
        };
        curve_logs.sort_by_key(|log| (log.block_number, log.tx_index, log.log_index));

        let mut block_numbers = logs
            .iter()
            .chain(curve_logs.iter())
            .map(|log| log.block_number)
            .collect::<BTreeSet<_>>();
        block_numbers.insert(end);
        if let Some((_, curve_end)) = curve_range {
            block_numbers.insert(curve_end);
        }
        let headers = self.fetch_headers(&block_numbers).await?;
        let end_hash = headers
            .get(&end)
            .and_then(|header| header.get("hash"))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_ascii_lowercase();
        let curve_end_hash = curve_range
            .and_then(|(_, curve_end)| headers.get(&curve_end))
            .and_then(|header| header.get("hash"))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_ascii_lowercase();

        let mut projected = 0_u64;
        let mut launches = 0_u64;
        let mut graduations = 0_u64;
        for log in &logs {
            let event = parse_protocol_event(&domain.protocol_key, log)?;
            let occurred_at = headers
                .get(&log.block_number)
                .and_then(header_timestamp)
                .transpose()?;
            self.persist_event(domain, log, &event, occurred_at).await?;
            projected += 1;
            launches += u64::from(matches!(event, ParsedLaunchEvent::TokenLaunched { .. }));
            graduations += u64::from(matches!(event, ParsedLaunchEvent::PoolGraduated { .. }));
        }
        let curve_trades_projected = self
            .persist_curve_trades(domain, &curve_logs, &headers)
            .await?;
        self.mark_cursor_success(domain, end, &end_hash, logs.len() as u64)
            .await?;
        if let Some((_, curve_end)) = curve_range {
            self.mark_curve_cursor_success(
                domain,
                curve_end,
                &curve_end_hash,
                curve_trades_projected,
            )
            .await?;
        }

        Ok(DomainIteration {
            range_processed: true,
            logs_observed: logs.len() as u64,
            curve_trade_logs_observed: curve_logs.len() as u64,
            projected,
            curve_trades_projected,
            launches,
            graduations,
            caught_up: end >= safe_head
                && curve_range.is_none_or(|(_, curve_end)| curve_end >= safe_head),
        })
    }

    async fn persist_curve_trades(
        &self,
        domain: &LaunchDomain,
        logs: &[RpcLog],
        headers: &BTreeMap<u64, Value>,
    ) -> LedgerResult<u64> {
        if logs.is_empty() {
            return Ok(0);
        }
        let curve_addresses = logs
            .iter()
            .map(|log| log.address.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let launch_rows = sqlx::query(
            r#"select launch_id,curve_address,token_address
               from protocol_token_launches
               where domain_id=$1 and curve_address=any($2)"#,
        )
        .bind(domain.domain_id)
        .bind(&curve_addresses)
        .fetch_all(&self.pool)
        .await
        .map_err(persistence_error)?;
        let launches = launch_rows
            .into_iter()
            .map(|row| {
                (
                    row.get::<String, _>("curve_address"),
                    CurveLaunchRef {
                        launch_id: row.get("launch_id"),
                        token_address: row.get("token_address"),
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();

        let mut projected = 0_u64;
        let mut tx = self.pool.begin().await.map_err(persistence_error)?;
        for log in logs {
            let Some(launch) = launches.get(&log.address) else {
                continue;
            };
            let trade = parse_curve_trade(log)?;
            let occurred_at = headers
                .get(&log.block_number)
                .and_then(header_timestamp)
                .transpose()?;
            let evidence = json!({
                "emitter": log.address,
                "topic0": log.topics.first(),
                "raw_topics": log.topics,
                "raw_data": log.data,
                "parser": "pons_curve_trade.v2.v1",
                "curve_membership": "factory_launch_event",
            });
            sqlx::query(
                r#"insert into protocol_launch_trade_facts (
                     launch_id,domain_id,chain_id,protocol_version,lifecycle_phase,
                     curve_address,token_address,trader_address,recipient_address,trade_side,
                     quote_amount_raw,token_amount_raw,fee_amount_raw,tax_amount_raw,
                     block_number,block_hash,tx_hash,tx_index,log_index,occurred_at,
                     data_source,is_canonical,evidence,created_at,updated_at
                   ) values ($1,$2,$3,'v2','curve_trading',$4,$5,$6,$7,$8,$9,$10,$11,$12,
                             $13,$14,$15,$16,$17,$18,'ledgerflow_rpc',true,$19,now(),now())
                   on conflict (chain_id,tx_hash,log_index) do update set
                     launch_id=excluded.launch_id,domain_id=excluded.domain_id,
                     curve_address=excluded.curve_address,token_address=excluded.token_address,
                     trader_address=excluded.trader_address,
                     recipient_address=excluded.recipient_address,trade_side=excluded.trade_side,
                     quote_amount_raw=excluded.quote_amount_raw,
                     token_amount_raw=excluded.token_amount_raw,
                     fee_amount_raw=excluded.fee_amount_raw,tax_amount_raw=excluded.tax_amount_raw,
                     block_number=excluded.block_number,block_hash=excluded.block_hash,
                     tx_index=excluded.tx_index,occurred_at=excluded.occurred_at,
                     data_source='ledgerflow_rpc',is_canonical=true,evidence=excluded.evidence,
                     updated_at=now()"#,
            )
            .bind(launch.launch_id)
            .bind(domain.domain_id)
            .bind(domain.chain_id as i64)
            .bind(&log.address)
            .bind(&launch.token_address)
            .bind(&trade.trader_address)
            .bind(&trade.recipient_address)
            .bind(trade.trade_side)
            .bind(&trade.quote_amount_raw)
            .bind(&trade.token_amount_raw)
            .bind(&trade.fee_amount_raw)
            .bind(&trade.tax_amount_raw)
            .bind(log.block_number as i64)
            .bind(&log.block_hash)
            .bind(&log.tx_hash)
            .bind(log.tx_index as i64)
            .bind(log.log_index as i64)
            .bind(occurred_at)
            .bind(Json(evidence))
            .execute(&mut *tx)
            .await
            .map_err(persistence_error)?;
            projected += 1;
        }
        tx.commit().await.map_err(persistence_error)?;
        Ok(projected)
    }

    async fn enrich_token_metadata(&self) -> LedgerResult<u64> {
        let rows = sqlx::query(
            r#"select launch_id,token_address,launch_block
               from protocol_token_launches
               where chain_id=$1 and (
                 metadata_status='pending' or (
                   metadata_status='error'
                   and coalesce(metadata_attempted_at,to_timestamp(0)) < now()-interval '1 hour'
                 )
               )
               order by launch_block desc
               limit $2"#,
        )
        .bind(self.config.chain_id as i64)
        .bind(self.config.metadata_fetch_limit as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(persistence_error)?;
        let mut tasks = tokio::task::JoinSet::new();
        for row in rows {
            let launch_id: Uuid = row.get("launch_id");
            let token_address: String = row.get("token_address");
            let token_metadata = self.token_metadata.clone();
            let chain_id = self.config.chain_id;
            let timeout_ms = self.config.metadata_fetch_timeout_ms;
            tasks.spawn(async move {
                let fetched = tokio::time::timeout(
                    Duration::from_millis(timeout_ms.max(100)),
                    token_metadata.get_or_fetch(chain_id, &token_address, None),
                )
                .await;
                (launch_id, fetched)
            });
        }

        let mut ready = 0_u64;
        while let Some(joined) = tasks.join_next().await {
            let (launch_id, fetched) = joined.map_err(|error| {
                LedgerError::TransientIngestion(format!("metadata task failed: {error}"))
            })?;
            let (symbol, name, status, error) = match fetched {
                Ok(Ok(metadata)) if metadata.fetch_status == "ok" => {
                    ready += 1;
                    (metadata.symbol, metadata.name, "ready", None)
                }
                Ok(Ok(_)) => (
                    None,
                    None,
                    "error",
                    Some("ERC-20 metadata was unavailable from LedgerFlow RPC".to_owned()),
                ),
                Ok(Err(fetch_error)) => (
                    None,
                    None,
                    "error",
                    Some(fetch_error.to_string().chars().take(500).collect()),
                ),
                Err(_) => (
                    None,
                    None,
                    "error",
                    Some(format!(
                        "LedgerFlow RPC metadata call exceeded {} ms",
                        self.config.metadata_fetch_timeout_ms
                    )),
                ),
            };
            sqlx::query(
                r#"update protocol_token_launches set
                     symbol=coalesce($2,symbol),name=coalesce($3,name),metadata_status=$4,
                     metadata_attempted_at=now(),metadata_error=$5,last_indexed_at=now(),updated_at=now()
                   where launch_id=$1"#,
            )
            .bind(launch_id)
            .bind(symbol)
            .bind(name)
            .bind(status)
            .bind(error)
            .execute(&self.pool)
            .await
            .map_err(persistence_error)?;
        }
        Ok(ready)
    }

    async fn fetch_headers(
        &self,
        block_numbers: &BTreeSet<u64>,
    ) -> LedgerResult<BTreeMap<u64, Value>> {
        let ordered = block_numbers.iter().copied().collect::<Vec<_>>();
        let mut headers = BTreeMap::new();
        for chunk in ordered.chunks(self.config.header_batch_size.max(1)) {
            for (block, header) in self.rpc.get_block_headers(chunk).await? {
                headers.insert(block, header);
            }
        }
        Ok(headers)
    }

    async fn persist_event(
        &self,
        domain: &LaunchDomain,
        log: &RpcLog,
        event: &ParsedLaunchEvent,
        occurred_at: Option<DateTime<Utc>>,
    ) -> LedgerResult<()> {
        let protocol_version = match domain.protocol_key.as_str() {
            "pons_v1" => "v1",
            "pons_v2" => "v2",
            other => other,
        };
        let facts = serde_json::to_value(event)
            .map_err(|error| LedgerError::MalformedEvidence(error.to_string()))?;
        let evidence = json!({
            "emitter": log.address,
            "topic0": log.topics.first(),
            "raw_topics": log.topics,
            "raw_data": log.data,
            "parser": format!("pons_launch_projector.{protocol_version}.v1"),
        });
        let mut tx = self.pool.begin().await.map_err(persistence_error)?;
        sqlx::query(
            r#"insert into protocol_launch_lifecycle_events (
                 domain_id,chain_id,protocol_version,token_address,curve_address,event_type,
                 block_number,block_hash,tx_hash,tx_index,log_index,occurred_at,data_source,
                 is_canonical,facts,evidence,created_at,updated_at
               ) values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,'ledgerflow_rpc',true,$13,$14,now(),now())
               on conflict (chain_id,tx_hash,log_index,event_type) do update set
                 domain_id=excluded.domain_id,protocol_version=excluded.protocol_version,
                 token_address=excluded.token_address,curve_address=excluded.curve_address,
                 block_number=excluded.block_number,block_hash=excluded.block_hash,
                 tx_index=excluded.tx_index,occurred_at=excluded.occurred_at,
                 is_canonical=true,facts=excluded.facts,evidence=excluded.evidence,updated_at=now()"#,
        )
        .bind(domain.domain_id)
        .bind(domain.chain_id as i64)
        .bind(protocol_version)
        .bind(event.token())
        .bind(event.curve())
        .bind(event.event_type())
        .bind(log.block_number as i64)
        .bind(&log.block_hash)
        .bind(&log.tx_hash)
        .bind(log.tx_index as i64)
        .bind(log.log_index as i64)
        .bind(occurred_at)
        .bind(Json(facts.clone()))
        .bind(Json(evidence.clone()))
        .execute(&mut *tx)
        .await
        .map_err(persistence_error)?;

        match event {
            ParsedLaunchEvent::TokenLaunched {
                token,
                creator,
                curve,
                quote_token,
                pool,
                launch_config_id,
                graduation_threshold_raw,
                position_id,
                facts,
            } => {
                let phase = if domain.protocol_key == "pons_v1" {
                    "v1_pool_live"
                } else {
                    "curve_trading"
                };
                sqlx::query(
                    r#"insert into protocol_token_launches (
                         domain_id,chain_id,token_address,creator_address,launch_tx_hash,
                         launch_block,launched_at,pool_address,quote_token_address,launch_status,
                         protocol_version,curve_address,lifecycle_phase,launch_config_id,
                         graduation_threshold_raw,position_id,last_lifecycle_block,data_source,
                         last_indexed_at,evidence,metadata,created_at,updated_at
                       ) values ($1,$2,$3,$4,$5,$6,$7,$8,$9,'trading',$10,$11,$12,$13,$14,$15,
                                 $6,'ledgerflow_rpc',now(),$16,$17,now(),now())
                       on conflict (domain_id,token_address) do update set
                         creator_address=excluded.creator_address,launch_tx_hash=excluded.launch_tx_hash,
                         launch_block=excluded.launch_block,launched_at=excluded.launched_at,
                         pool_address=coalesce(excluded.pool_address,protocol_token_launches.pool_address),
                         quote_token_address=excluded.quote_token_address,launch_status='trading',
                         protocol_version=excluded.protocol_version,curve_address=excluded.curve_address,
                         lifecycle_phase=excluded.lifecycle_phase,
                         launch_config_id=excluded.launch_config_id,
                         graduation_threshold_raw=excluded.graduation_threshold_raw,
                         position_id=coalesce(excluded.position_id,protocol_token_launches.position_id),
                         last_lifecycle_block=greatest(
                           coalesce(protocol_token_launches.last_lifecycle_block,0),excluded.last_lifecycle_block
                         ),data_source='ledgerflow_rpc',last_indexed_at=now(),
                         evidence=excluded.evidence,metadata=protocol_token_launches.metadata||excluded.metadata,
                         updated_at=now()"#,
                )
                .bind(domain.domain_id)
                .bind(domain.chain_id as i64)
                .bind(token)
                .bind(creator)
                .bind(&log.tx_hash)
                .bind(log.block_number as i64)
                .bind(occurred_at)
                .bind(pool)
                .bind(quote_token)
                .bind(protocol_version)
                .bind(curve)
                .bind(phase)
                .bind(launch_config_id)
                .bind(graduation_threshold_raw)
                .bind(position_id)
                .bind(Json(evidence))
                .bind(Json(facts.clone()))
                .execute(&mut *tx)
                .await
                .map_err(persistence_error)?;
            }
            ParsedLaunchEvent::LaunchSwept { token, .. } => {
                sqlx::query(
                    r#"update protocol_token_launches set lifecycle_phase='swept',
                         last_lifecycle_block=greatest(coalesce(last_lifecycle_block,0),$3),
                         last_indexed_at=now(),metadata=metadata||$4,updated_at=now()
                       where domain_id=$1 and token_address=$2"#,
                )
                .bind(domain.domain_id)
                .bind(token)
                .bind(log.block_number as i64)
                .bind(Json(facts))
                .execute(&mut *tx)
                .await
                .map_err(persistence_error)?;
            }
            ParsedLaunchEvent::PoolGraduated {
                token, position_id, ..
            } => {
                sqlx::query(
                    r#"update protocol_token_launches set launch_status='graduated',
                         lifecycle_phase='pool_created',graduation_tx_hash=$3,
                         graduation_block=$4,graduated_at=$5,position_id=$6,
                         last_lifecycle_block=greatest(coalesce(last_lifecycle_block,0),$4),
                         last_indexed_at=now(),metadata=metadata||$7,updated_at=now()
                       where domain_id=$1 and token_address=$2"#,
                )
                .bind(domain.domain_id)
                .bind(token)
                .bind(&log.tx_hash)
                .bind(log.block_number as i64)
                .bind(occurred_at)
                .bind(position_id)
                .bind(Json(facts))
                .execute(&mut *tx)
                .await
                .map_err(persistence_error)?;
            }
        }
        tx.commit().await.map_err(persistence_error)?;
        Ok(())
    }

    fn worker_id(&self, domain: &LaunchDomain) -> String {
        format!("{}:{}", self.config.worker_id_prefix, domain.domain_key)
    }

    fn curve_worker_id(&self, domain: &LaunchDomain) -> String {
        format!(
            "{}_curve:{}",
            self.config.worker_id_prefix, domain.domain_key
        )
    }

    async fn load_cursor(&self, domain: &LaunchDomain) -> LedgerResult<Option<u64>> {
        let value = sqlx::query_scalar::<_, Option<i64>>(
            "select last_processed_block from onchain_event_cursors where worker_id=$1",
        )
        .bind(self.worker_id(domain))
        .fetch_optional(&self.pool)
        .await
        .map_err(persistence_error)?
        .flatten();
        Ok(value.map(|block| block.max(0) as u64))
    }

    async fn load_curve_cursor(&self, domain: &LaunchDomain) -> LedgerResult<Option<u64>> {
        let value = sqlx::query_scalar::<_, Option<i64>>(
            "select last_processed_block from onchain_event_cursors where worker_id=$1",
        )
        .bind(self.curve_worker_id(domain))
        .fetch_optional(&self.pool)
        .await
        .map_err(persistence_error)?
        .flatten();
        Ok(value.map(|block| block.max(0) as u64))
    }

    async fn mark_cursor_running(
        &self,
        domain: &LaunchDomain,
        start: u64,
        end: u64,
    ) -> LedgerResult<()> {
        sqlx::query(
            r#"insert into onchain_event_cursors (
                 worker_id,chain_id,source_table,scan_from_block,scan_to_block,lookback_blocks,
                 status,last_error,metadata,created_at,updated_at
               ) values ($1,$2,'rpc.eth_getLogs',$3,$4,$5,'running',null,$6,now(),now())
               on conflict (worker_id) do update set chain_id=excluded.chain_id,
                 source_table=excluded.source_table,scan_from_block=excluded.scan_from_block,
                 scan_to_block=excluded.scan_to_block,lookback_blocks=excluded.lookback_blocks,
                 status='running',last_error=null,metadata=excluded.metadata,updated_at=now()"#,
        )
        .bind(self.worker_id(domain))
        .bind(self.config.chain_id as i64)
        .bind(start as i64)
        .bind(end as i64)
        .bind(self.config.lookback_blocks as i64)
        .bind(Json(json!({
            "owner": "ledgerflow",
            "domain_key": domain.domain_key,
            "protocol_key": domain.protocol_key,
            "factory": domain.factory_address,
        })))
        .execute(&self.pool)
        .await
        .map_err(persistence_error)?;
        Ok(())
    }

    async fn mark_curve_cursor_running(
        &self,
        domain: &LaunchDomain,
        start: u64,
        end: u64,
    ) -> LedgerResult<()> {
        sqlx::query(
            r#"insert into onchain_event_cursors (
                 worker_id,chain_id,source_table,scan_from_block,scan_to_block,lookback_blocks,
                 status,last_error,metadata,created_at,updated_at
               ) values ($1,$2,'rpc.eth_getLogs.pons_curve',$3,$4,$5,'running',null,$6,now(),now())
               on conflict (worker_id) do update set chain_id=excluded.chain_id,
                 source_table=excluded.source_table,scan_from_block=excluded.scan_from_block,
                 scan_to_block=excluded.scan_to_block,lookback_blocks=excluded.lookback_blocks,
                 status='running',last_error=null,metadata=excluded.metadata,updated_at=now()"#,
        )
        .bind(self.curve_worker_id(domain))
        .bind(self.config.chain_id as i64)
        .bind(start as i64)
        .bind(end as i64)
        .bind(self.config.lookback_blocks as i64)
        .bind(Json(json!({
            "owner": "ledgerflow",
            "domain_key": domain.domain_key,
            "protocol_key": domain.protocol_key,
            "fact_kind": "curve_trade",
        })))
        .execute(&self.pool)
        .await
        .map_err(persistence_error)?;
        Ok(())
    }

    async fn mark_cursor_success(
        &self,
        domain: &LaunchDomain,
        end: u64,
        end_hash: &str,
        event_count: u64,
    ) -> LedgerResult<()> {
        sqlx::query(
            r#"update onchain_event_cursors set last_processed_block=$2,
                 last_processed_block_hash=$3,status='idle',retry_count=0,last_error=null,
                 last_event_count=$4,last_success_at=now(),updated_at=now()
               where worker_id=$1"#,
        )
        .bind(self.worker_id(domain))
        .bind(end as i64)
        .bind(end_hash)
        .bind(event_count as i64)
        .execute(&self.pool)
        .await
        .map_err(persistence_error)?;
        Ok(())
    }

    async fn mark_curve_cursor_success(
        &self,
        domain: &LaunchDomain,
        end: u64,
        end_hash: &str,
        event_count: u64,
    ) -> LedgerResult<()> {
        sqlx::query(
            r#"update onchain_event_cursors set last_processed_block=$2,
                 last_processed_block_hash=$3,status='idle',retry_count=0,last_error=null,
                 last_event_count=$4,last_success_at=now(),updated_at=now()
               where worker_id=$1"#,
        )
        .bind(self.curve_worker_id(domain))
        .bind(end as i64)
        .bind(end_hash)
        .bind(event_count as i64)
        .execute(&self.pool)
        .await
        .map_err(persistence_error)?;
        Ok(())
    }

    async fn mark_cursor_error(&self, domain: &LaunchDomain, error: &str) -> LedgerResult<()> {
        sqlx::query(
            r#"insert into onchain_event_cursors (
                 worker_id,chain_id,source_table,lookback_blocks,status,retry_count,last_error,
                 metadata,created_at,updated_at
               ) values ($1,$2,'rpc.eth_getLogs',$3,'error',1,$4,$5,now(),now())
               on conflict (worker_id) do update set status='error',
                 retry_count=onchain_event_cursors.retry_count+1,last_error=excluded.last_error,
                 updated_at=now()"#,
        )
        .bind(self.worker_id(domain))
        .bind(self.config.chain_id as i64)
        .bind(self.config.lookback_blocks as i64)
        .bind(error)
        .bind(Json(json!({"domain_key": domain.domain_key})))
        .execute(&self.pool)
        .await
        .map_err(persistence_error)?;
        Ok(())
    }

    async fn mark_curve_cursor_error(
        &self,
        domain: &LaunchDomain,
        error: &str,
    ) -> LedgerResult<()> {
        sqlx::query(
            r#"insert into onchain_event_cursors (
                 worker_id,chain_id,source_table,lookback_blocks,status,retry_count,last_error,
                 metadata,created_at,updated_at
               ) values ($1,$2,'rpc.eth_getLogs.pons_curve',$3,'error',1,$4,$5,now(),now())
               on conflict (worker_id) do update set status='error',
                 retry_count=onchain_event_cursors.retry_count+1,last_error=excluded.last_error,
                 updated_at=now()"#,
        )
        .bind(self.curve_worker_id(domain))
        .bind(self.config.chain_id as i64)
        .bind(self.config.lookback_blocks as i64)
        .bind(error)
        .bind(Json(json!({
            "domain_key": domain.domain_key,
            "fact_kind": "curve_trade",
        })))
        .execute(&self.pool)
        .await
        .map_err(persistence_error)?;
        Ok(())
    }
}

#[derive(Clone, Debug, Default)]
struct DomainIteration {
    range_processed: bool,
    logs_observed: u64,
    curve_trade_logs_observed: u64,
    projected: u64,
    curve_trades_projected: u64,
    launches: u64,
    graduations: u64,
    caught_up: bool,
}

fn topics_for_protocol(protocol_key: &str) -> LedgerResult<Vec<&'static str>> {
    match protocol_key {
        "pons_v1" => Ok(vec![PONS_V1_TOKEN_LAUNCHED_TOPIC]),
        "pons_v2" => Ok(vec![
            PONS_V2_TOKEN_LAUNCHED_TOPIC,
            PONS_V2_LAUNCH_SWEPT_TOPIC,
            PONS_V2_POOL_GRADUATED_TOPIC,
        ]),
        other => Err(LedgerError::MalformedEvidence(format!(
            "no launch parser registered for protocol {other}"
        ))),
    }
}

fn parse_rpc_log(value: Value) -> LedgerResult<RpcLog> {
    let string = |key: &str| -> LedgerResult<String> {
        value
            .get(key)
            .and_then(Value::as_str)
            .map(|item| item.to_ascii_lowercase())
            .ok_or_else(|| LedgerError::MalformedEvidence(format!("rpc log missing {key}")))
    };
    let topics = value
        .get("topics")
        .and_then(Value::as_array)
        .ok_or_else(|| LedgerError::MalformedEvidence("rpc log missing topics".to_owned()))?
        .iter()
        .map(|topic| {
            topic
                .as_str()
                .map(|item| item.to_ascii_lowercase())
                .ok_or_else(|| LedgerError::MalformedEvidence("non-string log topic".to_owned()))
        })
        .collect::<LedgerResult<Vec<_>>>()?;
    Ok(RpcLog {
        address: string("address")?,
        block_number: parse_hex_field(&value, "blockNumber")?,
        block_hash: string("blockHash")?,
        tx_hash: string("transactionHash")?,
        tx_index: parse_hex_field(&value, "transactionIndex")?,
        log_index: parse_hex_field(&value, "logIndex")?,
        topics,
        data: string("data")?,
    })
}

fn parse_protocol_event(protocol_key: &str, log: &RpcLog) -> LedgerResult<ParsedLaunchEvent> {
    let topic0 = log.topics.first().map(String::as_str).unwrap_or_default();
    match (protocol_key, topic0) {
        ("pons_v1", PONS_V1_TOKEN_LAUNCHED_TOPIC) => {
            let words = data_words(&log.data)?;
            require_words(&log.topics, 4, "Pons V1 TokenLaunched topics")?;
            require_words(&words, 7, "Pons V1 TokenLaunched data")?;
            Ok(ParsedLaunchEvent::TokenLaunched {
                token: address_from_topic(&log.topics[1])?,
                creator: address_from_topic(&log.topics[2])?,
                curve: None,
                quote_token: address_from_word(&words[0])?,
                pool: Some(address_from_word(&words[1])?),
                launch_config_id: uint_from_word(&words[3])?,
                graduation_threshold_raw: None,
                position_id: Some(uint_from_word(&words[4])?),
                facts: json!({
                    "dex_factory": address_from_topic(&log.topics[3])?,
                    "dex_id": uint_from_word(&words[2])?,
                    "restrictions_end_block": uint_from_word(&words[5])?,
                    "initial_buy_amount_raw": uint_from_word(&words[6])?,
                    "lifecycle_note": "V1 launches directly into its V3 pool; graduation requires a state probe",
                }),
            })
        }
        ("pons_v2", PONS_V2_TOKEN_LAUNCHED_TOPIC) => {
            let words = data_words(&log.data)?;
            require_words(&log.topics, 4, "Pons V2 TokenLaunched topics")?;
            require_words(&words, 3, "Pons V2 TokenLaunched data")?;
            Ok(ParsedLaunchEvent::TokenLaunched {
                token: address_from_topic(&log.topics[1])?,
                creator: address_from_topic(&log.topics[3])?,
                curve: Some(address_from_topic(&log.topics[2])?),
                quote_token: address_from_word(&words[0])?,
                pool: None,
                launch_config_id: uint_from_word(&words[1])?,
                graduation_threshold_raw: Some(uint_from_word(&words[2])?),
                position_id: None,
                facts: json!({"phase": "bonding_curve"}),
            })
        }
        ("pons_v2", PONS_V2_LAUNCH_SWEPT_TOPIC) => {
            let words = data_words(&log.data)?;
            require_words(&log.topics, 2, "Pons V2 LaunchSwept topics")?;
            require_words(&words, 2, "Pons V2 LaunchSwept data")?;
            Ok(ParsedLaunchEvent::LaunchSwept {
                token: address_from_topic(&log.topics[1])?,
                quote_out_raw: uint_from_word(&words[0])?,
                token_out_raw: uint_from_word(&words[1])?,
            })
        }
        ("pons_v2", PONS_V2_POOL_GRADUATED_TOPIC) => {
            let words = data_words(&log.data)?;
            require_words(&log.topics, 2, "Pons V2 PoolGraduated topics")?;
            require_words(&words, 3, "Pons V2 PoolGraduated data")?;
            Ok(ParsedLaunchEvent::PoolGraduated {
                token: address_from_topic(&log.topics[1])?,
                position_id: uint_from_word(&words[0])?,
                token_amount_raw: uint_from_word(&words[1])?,
                pair_token_amount_raw: uint_from_word(&words[2])?,
            })
        }
        _ => Err(LedgerError::MalformedEvidence(format!(
            "unsupported {protocol_key} event topic {topic0}"
        ))),
    }
}

fn parse_curve_trade(log: &RpcLog) -> LedgerResult<ParsedCurveTrade> {
    let topic0 = log.topics.first().map(String::as_str).unwrap_or_default();
    require_words(&log.topics, 3, "Pons V2 curve trade topics")?;
    let words = data_words(&log.data)?;
    require_words(&words, 4, "Pons V2 curve trade data")?;
    let (trade_side, quote_amount_raw, token_amount_raw) = match topic0 {
        PONS_V2_CURVE_BUY_TOPIC => (
            "buy",
            uint_from_word(&words[0])?,
            uint_from_word(&words[1])?,
        ),
        PONS_V2_CURVE_SELL_TOPIC => (
            "sell",
            uint_from_word(&words[1])?,
            uint_from_word(&words[0])?,
        ),
        _ => {
            return Err(LedgerError::MalformedEvidence(format!(
                "unsupported Pons V2 curve trade topic {topic0}"
            )));
        }
    };
    Ok(ParsedCurveTrade {
        trader_address: address_from_topic(&log.topics[1])?,
        recipient_address: address_from_topic(&log.topics[2])?,
        trade_side,
        quote_amount_raw,
        token_amount_raw,
        fee_amount_raw: uint_from_word(&words[2])?,
        tax_amount_raw: uint_from_word(&words[3])?,
    })
}

fn data_words(data: &str) -> LedgerResult<Vec<String>> {
    let body = data.strip_prefix("0x").unwrap_or(data);
    if body.len() % 64 != 0 || !body.chars().all(|character| character.is_ascii_hexdigit()) {
        return Err(LedgerError::MalformedEvidence(
            "event data is not ABI word-aligned hex".to_owned(),
        ));
    }
    Ok(body
        .as_bytes()
        .chunks(64)
        .map(|chunk| String::from_utf8_lossy(chunk).into_owned())
        .collect())
}

fn require_words<T>(items: &[T], count: usize, label: &str) -> LedgerResult<()> {
    if items.len() < count {
        return Err(LedgerError::MalformedEvidence(format!(
            "{label} expected at least {count} words, received {}",
            items.len()
        )));
    }
    Ok(())
}

fn address_from_topic(topic: &str) -> LedgerResult<String> {
    let body = topic.strip_prefix("0x").unwrap_or(topic);
    if body.len() != 64 || !body.chars().all(|character| character.is_ascii_hexdigit()) {
        return Err(LedgerError::MalformedEvidence(format!(
            "invalid indexed address topic {topic}"
        )));
    }
    Ok(format!("0x{}", &body[24..]).to_ascii_lowercase())
}

fn address_from_word(word: &str) -> LedgerResult<String> {
    if word.len() != 64 || !word.chars().all(|character| character.is_ascii_hexdigit()) {
        return Err(LedgerError::MalformedEvidence(format!(
            "invalid address ABI word {word}"
        )));
    }
    Ok(format!("0x{}", &word[24..]).to_ascii_lowercase())
}

fn uint_from_word(word: &str) -> LedgerResult<String> {
    let bytes =
        hex::decode(word).map_err(|error| LedgerError::MalformedEvidence(error.to_string()))?;
    Ok(BigUint::from_bytes_be(&bytes).to_string())
}

fn parse_hex_field(value: &Value, key: &str) -> LedgerResult<u64> {
    let raw = value
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| LedgerError::MalformedEvidence(format!("rpc log missing {key}")))?;
    u64::from_str_radix(raw.trim_start_matches("0x"), 16)
        .map_err(|error| LedgerError::MalformedEvidence(format!("invalid {key}: {error}")))
}

fn header_timestamp(header: &Value) -> Option<LedgerResult<DateTime<Utc>>> {
    header.get("timestamp").and_then(Value::as_str).map(|raw| {
        let timestamp = i64::from_str_radix(raw.trim_start_matches("0x"), 16)
            .map_err(|error| LedgerError::MalformedEvidence(error.to_string()))?;
        Utc.timestamp_opt(timestamp, 0)
            .single()
            .ok_or_else(|| LedgerError::MalformedEvidence("invalid block timestamp".to_owned()))
    })
}

fn persistence_error(error: sqlx::Error) -> LedgerError {
    LedgerError::Persistence(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn topic_address(address: &str) -> String {
        format!("0x{:0>64}", address.trim_start_matches("0x"))
    }

    fn word_address(address: &str) -> String {
        format!("{:0>64}", address.trim_start_matches("0x"))
    }

    fn word_u64(value: u64) -> String {
        format!("{value:064x}")
    }

    fn log(topic0: &str, topics: Vec<String>, words: Vec<String>) -> RpcLog {
        let mut all_topics = vec![topic0.to_owned()];
        all_topics.extend(topics);
        RpcLog {
            address: "0x1111111111111111111111111111111111111111".to_owned(),
            block_number: 100,
            block_hash: format!("0x{:064x}", 2),
            tx_hash: format!("0x{:064x}", 3),
            tx_index: 1,
            log_index: 2,
            topics: all_topics,
            data: format!("0x{}", words.join("")),
        }
    }

    #[test]
    fn parses_pons_v1_launch() {
        let event = parse_protocol_event(
            "pons_v1",
            &log(
                PONS_V1_TOKEN_LAUNCHED_TOPIC,
                vec![
                    topic_address("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
                    topic_address("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
                    topic_address("0xcccccccccccccccccccccccccccccccccccccccc"),
                ],
                vec![
                    word_address("0xdddddddddddddddddddddddddddddddddddddddd"),
                    word_address("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"),
                    word_u64(1),
                    word_u64(2),
                    word_u64(3),
                    word_u64(4),
                    word_u64(5),
                ],
            ),
        )
        .expect("valid event");
        match event {
            ParsedLaunchEvent::TokenLaunched { token, pool, .. } => {
                assert_eq!(token, "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
                assert_eq!(
                    pool.as_deref(),
                    Some("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
                );
            }
            _ => panic!("unexpected event"),
        }
    }

    #[test]
    fn parses_pons_v2_graduation_without_inventing_pool_address() {
        let event = parse_protocol_event(
            "pons_v2",
            &log(
                PONS_V2_POOL_GRADUATED_TOPIC,
                vec![topic_address("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")],
                vec![word_u64(42), word_u64(1_000), word_u64(25)],
            ),
        )
        .expect("valid event");
        match event {
            ParsedLaunchEvent::PoolGraduated {
                position_id,
                token_amount_raw,
                ..
            } => {
                assert_eq!(position_id, "42");
                assert_eq!(token_amount_raw, "1000");
            }
            _ => panic!("unexpected event"),
        }
    }

    #[test]
    fn parses_pons_v2_curve_buy_as_direct_rpc_fact() {
        let trade = parse_curve_trade(&log(
            PONS_V2_CURVE_BUY_TOPIC,
            vec![
                topic_address("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
                topic_address("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
            ],
            vec![word_u64(100), word_u64(250), word_u64(3), word_u64(2)],
        ))
        .expect("valid buy");
        assert_eq!(trade.trade_side, "buy");
        assert_eq!(trade.quote_amount_raw, "100");
        assert_eq!(trade.token_amount_raw, "250");
        assert_eq!(trade.fee_amount_raw, "3");
        assert_eq!(trade.tax_amount_raw, "2");
    }

    #[test]
    fn parses_pons_v2_curve_sell_with_normalized_asset_amounts() {
        let trade = parse_curve_trade(&log(
            PONS_V2_CURVE_SELL_TOPIC,
            vec![
                topic_address("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
                topic_address("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
            ],
            vec![word_u64(250), word_u64(90), word_u64(5), word_u64(1)],
        ))
        .expect("valid sell");
        assert_eq!(trade.trade_side, "sell");
        assert_eq!(trade.quote_amount_raw, "90");
        assert_eq!(trade.token_amount_raw, "250");
    }
}
