use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row, types::Json};
use uuid::Uuid;

use crate::core::errors::{LedgerError, LedgerResult};

pub const PROTOCOL_MARKET_SEED_SCHEMA: &str = "ledgerflow.protocol-market-seeds.v1";

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ProtocolMarketSeedManifest {
    pub schema_version: String,
    pub dataset_key: String,
    pub dataset_version: String,
    pub source: ProtocolMarketSource,
    pub domains: Vec<ProtocolMarketDomainSeed>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ProtocolMarketSource {
    pub source_key: String,
    pub display_name: String,
    pub source_kind: String,
    pub trust_score: f64,
    pub owner: Option<String>,
    pub homepage: Option<String>,
    #[serde(default = "json_object")]
    pub metadata: Value,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ProtocolMarketDomainSeed {
    pub domain_key: String,
    pub chain_id: u64,
    pub protocol_key: String,
    pub protocol_family: String,
    pub display_name_en: String,
    pub display_name_zh: String,
    pub domain_kind: String,
    pub cluster_method_version: String,
    #[serde(default = "json_object")]
    pub metadata: Value,
    pub seeds: Vec<ProtocolMarketAddressSeed>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ProtocolMarketAddressSeed {
    pub address: String,
    pub seed_role: String,
    pub label_en: String,
    pub label_zh: String,
    pub confidence: f64,
    pub provenance_url: String,
    pub valid_from_block: Option<u64>,
    pub valid_to_block: Option<u64>,
    #[serde(default = "json_object")]
    pub evidence: Value,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct ProtocolMarketImportSummary {
    pub dataset_key: String,
    pub dataset_version: String,
    pub domain_rows: u64,
    pub seed_rows: u64,
    pub registry_seed_rows: u64,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct MarketOverviewStats {
    pub protocol_domain_count: i64,
    pub protocol_seed_count: i64,
    pub launch_data_ready: bool,
    pub launch_count: Option<i64>,
    pub rising_launch_count: Option<i64>,
    pub falling_launch_count: Option<i64>,
    pub verified_winner_count: Option<i64>,
    pub verified_loser_count: Option<i64>,
    pub estimated_winner_count: Option<i64>,
    pub estimated_loser_count: Option<i64>,
    pub detected_arbitrage_24h: i64,
    pub verified_arbitrage_24h: i64,
    pub verified_arbitrage_archive: i64,
    pub verified_arbitrage_actors: i64,
    pub latest_detected_arbitrage_at: Option<DateTime<Utc>>,
    pub latest_verified_arbitrage_at: Option<DateTime<Utc>>,
    pub latest_launch_snapshot_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Serialize)]
pub struct MarketLaunch {
    pub launch_id: Uuid,
    pub domain_key: String,
    pub domain_name_en: String,
    pub domain_name_zh: String,
    pub token_address: String,
    pub name: Option<String>,
    pub symbol: Option<String>,
    pub creator_address: Option<String>,
    pub pool_address: Option<String>,
    pub launched_at: Option<DateTime<Utc>>,
    pub launch_block: i64,
    pub launch_status: String,
    pub metadata_status: String,
    pub curve_trade_count: i64,
    pub curve_buy_count: i64,
    pub curve_sell_count: i64,
    pub latest_curve_trade_at: Option<DateTime<Utc>>,
    pub price_quote: Option<String>,
    pub price_change_percent: Option<String>,
    pub net_flow_quote: Option<String>,
    pub inflow_velocity_quote_per_minute: Option<String>,
    pub unique_buyers: Option<i64>,
    pub unique_sellers: Option<i64>,
    pub issuer_cohort_buyers: Option<i64>,
    pub external_buyers: Option<i64>,
    pub unresolved_buyers: Option<i64>,
    pub verified_winning_participants: Option<i64>,
    pub verified_losing_participants: Option<i64>,
    pub estimated_winning_participants: Option<i64>,
    pub estimated_losing_participants: Option<i64>,
    pub outcome_coverage_ratio: Option<f64>,
    pub evidence_coverage_ratio: Option<f64>,
    pub measured_at: Option<DateTime<Utc>>,
    pub limitations: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ProtocolClusterCard {
    pub cluster_id: Uuid,
    pub domain_key: String,
    pub domain_name_en: String,
    pub domain_name_zh: String,
    pub domain_kind: String,
    pub cluster_kind: String,
    pub subject_address: Option<String>,
    pub evidence_state: String,
    pub confidence: f64,
    pub member_count: i64,
    pub economic_member_count: i64,
    pub first_seen_at: Option<DateTime<Utc>>,
    pub last_seen_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Serialize)]
pub struct AddressClusterMatch {
    pub cluster: ProtocolClusterCard,
    pub member_role: String,
    pub member_evidence_state: String,
    pub member_confidence: f64,
    pub interaction_count: i64,
}

#[derive(Clone, Debug, Serialize)]
pub struct ProtocolAddressMatch {
    pub domain_key: Option<String>,
    pub domain_name_en: Option<String>,
    pub domain_name_zh: Option<String>,
    pub protocol_key: String,
    pub protocol_name: String,
    pub address_role: String,
    pub evidence_state: String,
    pub confidence: f64,
    pub source: String,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct AddressActivitySummary {
    pub launches_created: i64,
    pub launch_tokens: i64,
    pub launches_traded: i64,
    pub curve_trades: i64,
    pub verified_arbitrage_executions: i64,
    pub verified_settlements_received: i64,
    pub last_seen_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Serialize)]
pub struct MarketAddressLookup {
    pub chain_id: u64,
    pub address: String,
    pub cluster_matches: Vec<AddressClusterMatch>,
    pub protocol_matches: Vec<ProtocolAddressMatch>,
    pub activity: AddressActivitySummary,
}

#[derive(Clone, Debug, Serialize)]
pub struct ProtocolClusterMemberRow {
    pub address: String,
    pub member_role: String,
    pub evidence_state: String,
    pub confidence: f64,
    pub interaction_count: i64,
    pub include_in_behavior_scope: bool,
    pub include_in_economic_scope: bool,
    pub supporting_tx_hashes: Vec<String>,
    pub evidence: Value,
    pub first_seen_at: Option<DateTime<Utc>>,
    pub last_seen_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ProtocolClusterEdgeRow {
    pub source_address: String,
    pub target_address: String,
    pub relationship_type: String,
    pub evidence_state: String,
    pub confidence: f64,
    pub interaction_count: i64,
    pub supporting_tx_hashes: Vec<String>,
    pub evidence_reason: String,
    pub first_seen_at: Option<DateTime<Utc>>,
    pub last_seen_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ProtocolClusterDetail {
    pub cluster: ProtocolClusterCard,
    pub members: Vec<ProtocolClusterMemberRow>,
    pub edges: Vec<ProtocolClusterEdgeRow>,
}

#[derive(Clone, Debug, Serialize)]
pub struct VerifiedExecutionRow {
    pub tx_hash: String,
    pub subject_address: String,
    pub profit_recipient: Option<String>,
    pub profit_token_address: String,
    pub profit_token_symbol: Option<String>,
    pub profit_token_decimals: Option<i32>,
    pub profit_amount_raw: String,
    pub gas_cost_raw: Option<String>,
    pub confidence: f64,
    pub conclusion: String,
    pub verified_at: Option<DateTime<Utc>>,
    pub limitations: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct VerifiedEarningsRankingRow {
    pub rank: i64,
    pub cluster_id: Option<Uuid>,
    pub subject_address: String,
    pub profit_token_address: String,
    pub profit_token_symbol: Option<String>,
    pub profit_token_decimals: Option<i32>,
    pub verified_gross_profit_raw: String,
    pub exact_gas_cost_raw: String,
    pub verified_execution_count: i64,
    pub economically_verified_count: i64,
    pub economically_evaluated_count: i64,
    pub coverage_ratio: f64,
    pub last_execution_at: Option<DateTime<Utc>>,
}

pub fn parse_protocol_market_manifest(raw: &str) -> LedgerResult<ProtocolMarketSeedManifest> {
    let manifest: ProtocolMarketSeedManifest = serde_json::from_str(raw).map_err(|error| {
        LedgerError::MalformedEvidence(format!("invalid market seed JSON: {error}"))
    })?;
    validate_manifest(&manifest)?;
    Ok(manifest)
}

pub async fn import_protocol_market_manifest(
    pool: &PgPool,
    manifest: &ProtocolMarketSeedManifest,
) -> LedgerResult<ProtocolMarketImportSummary> {
    validate_manifest(manifest)?;
    let manifest_value = serde_json::to_value(manifest)
        .map_err(|error| LedgerError::MalformedEvidence(error.to_string()))?;
    let checksum = sha256_hex(
        &serde_json::to_vec(manifest)
            .map_err(|error| LedgerError::MalformedEvidence(error.to_string()))?,
    );
    let mut tx = pool.begin().await.map_err(db_error)?;

    sqlx::query(
        r#"insert into intelligence_sources (
               source_key,display_name,source_kind,trust_score,owner,homepage,metadata
           ) values ($1,$2,$3,$4,$5,$6,$7)
           on conflict (source_key) do update set
             display_name=excluded.display_name,source_kind=excluded.source_kind,
             trust_score=excluded.trust_score,owner=excluded.owner,homepage=excluded.homepage,
             status='active',metadata=excluded.metadata,updated_at=now()"#,
    )
    .bind(&manifest.source.source_key)
    .bind(&manifest.source.display_name)
    .bind(&manifest.source.source_kind)
    .bind(manifest.source.trust_score)
    .bind(&manifest.source.owner)
    .bind(&manifest.source.homepage)
    .bind(Json(manifest.source.metadata.clone()))
    .execute(&mut *tx)
    .await
    .map_err(db_error)?;

    sqlx::query(
        r#"insert into intelligence_datasets (
               dataset_key,dataset_version,source_key,checksum_sha256,status,manifest,imported_at
           ) values ($1,$2,$3,$4,'active',$5,now())
           on conflict (dataset_key,dataset_version) do update set
             source_key=excluded.source_key,checksum_sha256=excluded.checksum_sha256,
             status='active',manifest=excluded.manifest,imported_at=now()"#,
    )
    .bind(&manifest.dataset_key)
    .bind(&manifest.dataset_version)
    .bind(&manifest.source.source_key)
    .bind(&checksum)
    .bind(Json(manifest_value))
    .execute(&mut *tx)
    .await
    .map_err(db_error)?;

    sqlx::query(
        r#"update protocol_seed_addresses set status='retired',updated_at=now()
           where source_key=$1 and status='active'"#,
    )
    .bind(&manifest.source.source_key)
    .execute(&mut *tx)
    .await
    .map_err(db_error)?;

    let mut summary = ProtocolMarketImportSummary {
        dataset_key: manifest.dataset_key.clone(),
        dataset_version: manifest.dataset_version.clone(),
        ..ProtocolMarketImportSummary::default()
    };

    for domain in &manifest.domains {
        sqlx::query(
            r#"insert into protocols (protocol_key,display_name,protocol_family,website,metadata)
               values ($1,$2,$3,$4,$5)
               on conflict (protocol_key) do update set
                 display_name=excluded.display_name,protocol_family=excluded.protocol_family,
                 website=excluded.website,metadata=protocols.metadata||excluded.metadata,
                 updated_at=now()"#,
        )
        .bind(&domain.protocol_key)
        .bind(&domain.display_name_en)
        .bind(&domain.protocol_family)
        .bind(&manifest.source.homepage)
        .bind(Json(json!({"market_domain": domain.domain_key})))
        .execute(&mut *tx)
        .await
        .map_err(db_error)?;

        let domain_id = sqlx::query_scalar::<_, Uuid>(
            r#"insert into protocol_intelligence_domains (
                   domain_key,chain_id,protocol_key,display_name_en,display_name_zh,
                   domain_kind,cluster_method_version,metadata
               ) values ($1,$2,$3,$4,$5,$6,$7,$8)
               on conflict (domain_key) do update set
                 chain_id=excluded.chain_id,protocol_key=excluded.protocol_key,
                 display_name_en=excluded.display_name_en,display_name_zh=excluded.display_name_zh,
                 domain_kind=excluded.domain_kind,cluster_method_version=excluded.cluster_method_version,
                 metadata=excluded.metadata,status='active',updated_at=now()
               returning domain_id"#,
        )
        .bind(&domain.domain_key)
        .bind(domain.chain_id as i64)
        .bind(&domain.protocol_key)
        .bind(&domain.display_name_en)
        .bind(&domain.display_name_zh)
        .bind(&domain.domain_kind)
        .bind(&domain.cluster_method_version)
        .bind(Json(domain.metadata.clone()))
        .fetch_one(&mut *tx)
        .await
        .map_err(db_error)?;
        summary.domain_rows += 1;

        for seed in &domain.seeds {
            let address = normalize_address(&seed.address)?;
            let seed_digest = sha256_hex(
                &serde_json::to_vec(seed)
                    .map_err(|error| LedgerError::MalformedEvidence(error.to_string()))?,
            );
            sqlx::query(
                r#"insert into chain_addresses (chain_id,address,address_kind,metadata)
                   values ($1,$2,'contract',$3)
                   on conflict (chain_id,address) do update set
                     address_kind='contract',metadata=chain_addresses.metadata||excluded.metadata,
                     last_seen_at=now(),updated_at=now()"#,
            )
            .bind(domain.chain_id as i64)
            .bind(&address)
            .bind(Json(
                json!({"protocol_market_seed": true, "domain_key": domain.domain_key}),
            ))
            .execute(&mut *tx)
            .await
            .map_err(db_error)?;

            sqlx::query(
                r#"insert into protocol_deployments (
                       chain_id,address,protocol_key,contract_role,source_key,method_version,
                       dataset_key,dataset_version,confidence,status,valid_from_block,valid_to_block,
                       metadata
                   ) values ($1,$2,$3,$4,$5,'protocol-market-seed.v1',$6,$7,$8,'active',$9,$10,$11)
                   on conflict (chain_id,address,protocol_key,contract_role,source_key,method_version)
                   do update set confidence=excluded.confidence,status='active',
                     valid_from_block=excluded.valid_from_block,valid_to_block=excluded.valid_to_block,
                     metadata=excluded.metadata,last_seen_at=now(),updated_at=now()"#,
            )
            .bind(domain.chain_id as i64)
            .bind(&address)
            .bind(&domain.protocol_key)
            .bind(&seed.seed_role)
            .bind(&manifest.source.source_key)
            .bind(&manifest.dataset_key)
            .bind(&manifest.dataset_version)
            .bind(seed.confidence)
            .bind(seed.valid_from_block.map(|value| value as i64))
            .bind(seed.valid_to_block.map(|value| value as i64))
            .bind(Json(json!({
                "domain_key": domain.domain_key,
                "label_en": seed.label_en,
                "label_zh": seed.label_zh,
                "provenance_url": seed.provenance_url
            })))
            .execute(&mut *tx)
            .await
            .map_err(db_error)?;

            let market_seed_result = sqlx::query(
                r#"insert into protocol_seed_addresses (
                       domain_id,chain_id,address,seed_role,label_en,label_zh,evidence_state,
                       confidence,provenance_url,source_key,valid_from_block,valid_to_block,
                       evidence,status
                   ) values ($1,$2,$3,$4,$5,$6,'observed',$7,$8,$9,$10,$11,$12,'active')
                   on conflict (domain_id,address,seed_role) do update set
                     label_en=excluded.label_en,label_zh=excluded.label_zh,
                     confidence=excluded.confidence,provenance_url=excluded.provenance_url,
                     source_key=excluded.source_key,valid_from_block=excluded.valid_from_block,
                     valid_to_block=excluded.valid_to_block,evidence=excluded.evidence,
                     evidence_state='observed',status='active',updated_at=now()"#,
            )
            .bind(domain_id)
            .bind(domain.chain_id as i64)
            .bind(&address)
            .bind(&seed.seed_role)
            .bind(&seed.label_en)
            .bind(&seed.label_zh)
            .bind(seed.confidence)
            .bind(&seed.provenance_url)
            .bind(&manifest.source.source_key)
            .bind(seed.valid_from_block.map(|value| value as i64))
            .bind(seed.valid_to_block.map(|value| value as i64))
            .bind(Json(seed.evidence.clone()))
            .execute(&mut *tx)
            .await
            .map_err(db_error)?;
            summary.seed_rows += market_seed_result.rows_affected();

            let registry_seed_result = sqlx::query(
                r#"insert into intelligence_seed_addresses (
                       chain_id,address,seed_kind,roles,source_key,status,priority,confidence,
                       evidence,evidence_digest,first_seen_block,last_seen_block
                   ) values ($1,$2,'protocol_contract',$3,$4,'active',5,$5,$6,$7,$8,$9)
                   on conflict (chain_id,address,seed_kind,source_key) do update set
                     roles=excluded.roles,status='active',priority=excluded.priority,
                     confidence=excluded.confidence,evidence=excluded.evidence,
                     evidence_digest=excluded.evidence_digest,
                     first_seen_block=excluded.first_seen_block,last_seen_block=excluded.last_seen_block,
                     last_seen_at=now(),updated_at=now()"#,
            )
            .bind(domain.chain_id as i64)
            .bind(&address)
            .bind(vec![format!("{}:{}", domain.domain_key, seed.seed_role)])
            .bind(&manifest.source.source_key)
            .bind(seed.confidence)
            .bind(Json(json!({
                "domain_key": domain.domain_key,
                "seed_role": seed.seed_role,
                "provenance_url": seed.provenance_url,
                "evidence": seed.evidence
            })))
            .bind(&seed_digest)
            .bind(seed.valid_from_block.map(|value| value as i64))
            .bind(seed.valid_to_block.map(|value| value as i64))
            .execute(&mut *tx)
            .await
            .map_err(db_error)?;
            summary.registry_seed_rows += registry_seed_result.rows_affected();
        }
    }

    tx.commit().await.map_err(db_error)?;
    Ok(summary)
}

#[derive(Clone)]
pub struct ProtocolMarketService {
    pool: PgPool,
}

impl ProtocolMarketService {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn overview(&self, chain_id: u64, window: &str) -> LedgerResult<MarketOverviewStats> {
        let window = normalize_window(window);
        let row = sqlx::query(
            r#"with latest_metrics as (
                 select distinct on (launch_id) launch_id,price_change_percent,
                   verified_winning_participants,verified_losing_participants,
                   outcome_coverage_ratio,evidence,measured_at
                 from protocol_launch_metric_snapshots
                 where chain_id=$1 and window_key=$2
                 order by launch_id,measured_at desc
               )
               select
                 (select count(*) from protocol_intelligence_domains
                    where chain_id=$1 and status='active')::bigint as domain_count,
                 (select count(*) from protocol_seed_addresses
                    where chain_id=$1 and status='active')::bigint as seed_count,
                 (select count(*) from protocol_token_launches where chain_id=$1)::bigint as launch_count,
                 (select count(*) from latest_metrics)::bigint as snapshot_count,
                 (select count(*) from latest_metrics where price_change_percent>0)::bigint as rising_count,
                 (select count(*) from latest_metrics where price_change_percent<0)::bigint as falling_count,
                 (select coalesce(sum(verified_winning_participants),0) from latest_metrics)::bigint as winners,
                 (select coalesce(sum(verified_losing_participants),0) from latest_metrics)::bigint as losers,
                 (select count(*) from latest_metrics
                    where outcome_coverage_ratio>0)::bigint as verified_outcome_snapshots,
                 (select coalesce(sum((evidence->>'estimated_winning_participants')::bigint),0)
                    from latest_metrics)::bigint as estimated_winners,
                 (select coalesce(sum((evidence->>'estimated_losing_participants')::bigint),0)
                    from latest_metrics)::bigint as estimated_losers,
                 (select coalesce(sum((evidence->>'estimated_evaluated_participants')::bigint),0)
                    from latest_metrics)::bigint as estimated_evaluated,
                 (select max(measured_at) from latest_metrics) as latest_snapshot,
                 (select count(*) from arbitrage_candidates where chain_id=$1
                    and block_timestamp>=now()-interval '24 hours')::bigint as detected_arb_24h,
                 (select max(block_timestamp) from arbitrage_candidates where chain_id=$1) as latest_detected,
                 (select count(*) from economic_verification_results where chain_id=$1
                    and result_status='verified' and verified_at>=now()-interval '24 hours')::bigint as verified_arb_24h,
                 (select count(*) from economic_verification_results where chain_id=$1
                    and result_status='verified')::bigint as verified_arb_archive,
                 (select count(distinct subject_address) from economic_verification_results
                    where chain_id=$1 and result_status='verified')::bigint as verified_arb_actors,
                 (select max(verified_at) from economic_verification_results
                    where chain_id=$1 and result_status='verified') as latest_verified"#,
        )
        .bind(chain_id as i64)
        .bind(window)
        .fetch_one(&self.pool)
        .await
        .map_err(db_error)?;
        let snapshot_count: i64 = row.get("snapshot_count");
        let launch_data_ready = snapshot_count > 0;
        let verified_outcome_snapshots: i64 = row.get("verified_outcome_snapshots");
        let estimated_evaluated: i64 = row.get("estimated_evaluated");
        Ok(MarketOverviewStats {
            protocol_domain_count: row.get("domain_count"),
            protocol_seed_count: row.get("seed_count"),
            launch_data_ready,
            launch_count: launch_data_ready.then(|| row.get("launch_count")),
            rising_launch_count: launch_data_ready.then(|| row.get("rising_count")),
            falling_launch_count: launch_data_ready.then(|| row.get("falling_count")),
            verified_winner_count: (verified_outcome_snapshots > 0).then(|| row.get("winners")),
            verified_loser_count: (verified_outcome_snapshots > 0).then(|| row.get("losers")),
            estimated_winner_count: (estimated_evaluated > 0).then(|| row.get("estimated_winners")),
            estimated_loser_count: (estimated_evaluated > 0).then(|| row.get("estimated_losers")),
            detected_arbitrage_24h: row.get("detected_arb_24h"),
            verified_arbitrage_24h: row.get("verified_arb_24h"),
            verified_arbitrage_archive: row.get("verified_arb_archive"),
            verified_arbitrage_actors: row.get("verified_arb_actors"),
            latest_detected_arbitrage_at: row.get("latest_detected"),
            latest_verified_arbitrage_at: row.get("latest_verified"),
            latest_launch_snapshot_at: row.get("latest_snapshot"),
        })
    }

    pub async fn launches(
        &self,
        chain_id: u64,
        window: &str,
        limit: usize,
    ) -> LedgerResult<Vec<MarketLaunch>> {
        let rows = sqlx::query(
            r#"with latest_metrics as materialized (
                 select distinct on (metric.launch_id) metric.*
                 from protocol_launch_metric_snapshots metric
                 where metric.chain_id=$1 and metric.window_key=$2
                 order by metric.launch_id,metric.measured_at desc
               ), ranked_launches as materialized (
                 select l.launch_id,d.domain_key,d.display_name_en,d.display_name_zh,
                      l.token_address,l.name,l.symbol,l.creator_address,l.pool_address,
                      l.launched_at,l.launch_block,l.launch_status,l.metadata_status,
                      m.price_quote::text as price_quote,
                      m.price_change_percent::text as price_change_percent,
                      m.net_flow_quote::text as net_flow_quote,
                      m.inflow_velocity_quote_per_minute::text as inflow_velocity,
                      m.unique_buyers,m.unique_sellers,m.issuer_cohort_buyers,
                      m.external_buyers,m.unresolved_buyers,
                      m.verified_winning_participants,m.verified_losing_participants,
                      (m.evidence->>'estimated_winning_participants')::bigint
                        as estimated_winning_participants,
                      (m.evidence->>'estimated_losing_participants')::bigint
                        as estimated_losing_participants,
                      m.outcome_coverage_ratio,m.evidence_coverage_ratio,m.measured_at,
                      coalesce(m.limitations,array[]::text[]) as limitations
                 from protocol_token_launches l
                 join protocol_intelligence_domains d on d.domain_id=l.domain_id
                 left join latest_metrics m on m.launch_id=l.launch_id
                 where l.chain_id=$1
                 order by m.inflow_velocity_quote_per_minute desc nulls last,
                          l.launched_at desc nulls last,l.launch_block desc
                 limit $3
               )
               select ranked_launches.*,
                      coalesce(t.trade_count,0)::bigint as curve_trade_count,
                      coalesce(t.buy_count,0)::bigint as curve_buy_count,
                      coalesce(t.sell_count,0)::bigint as curve_sell_count,
                      t.latest_trade_at as latest_curve_trade_at
               from ranked_launches
               left join lateral (
                 select count(*) as trade_count,
                        count(*) filter (where trade_side='buy') as buy_count,
                        count(*) filter (where trade_side='sell') as sell_count,
                        max(occurred_at) as latest_trade_at
                 from protocol_launch_trade_facts trade
                 where trade.launch_id=ranked_launches.launch_id and trade.is_canonical
               ) t on true
               order by ranked_launches.inflow_velocity::numeric desc nulls last,
                        ranked_launches.launched_at desc nulls last,
                        ranked_launches.launch_block desc"#,
        )
        .bind(chain_id as i64)
        .bind(normalize_window(window))
        .bind(limit.clamp(1, 500) as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(db_error)?;
        rows.into_iter()
            .map(|row| {
                Ok(MarketLaunch {
                    launch_id: row.try_get("launch_id").map_err(db_error)?,
                    domain_key: row.try_get("domain_key").map_err(db_error)?,
                    domain_name_en: row.try_get("display_name_en").map_err(db_error)?,
                    domain_name_zh: row.try_get("display_name_zh").map_err(db_error)?,
                    token_address: row.try_get("token_address").map_err(db_error)?,
                    name: row.try_get("name").map_err(db_error)?,
                    symbol: row.try_get("symbol").map_err(db_error)?,
                    creator_address: row.try_get("creator_address").map_err(db_error)?,
                    pool_address: row.try_get("pool_address").map_err(db_error)?,
                    launched_at: row.try_get("launched_at").map_err(db_error)?,
                    launch_block: row.try_get("launch_block").map_err(db_error)?,
                    launch_status: row.try_get("launch_status").map_err(db_error)?,
                    metadata_status: row.try_get("metadata_status").map_err(db_error)?,
                    curve_trade_count: row.try_get("curve_trade_count").map_err(db_error)?,
                    curve_buy_count: row.try_get("curve_buy_count").map_err(db_error)?,
                    curve_sell_count: row.try_get("curve_sell_count").map_err(db_error)?,
                    latest_curve_trade_at: row
                        .try_get("latest_curve_trade_at")
                        .map_err(db_error)?,
                    price_quote: row.try_get("price_quote").map_err(db_error)?,
                    price_change_percent: row.try_get("price_change_percent").map_err(db_error)?,
                    net_flow_quote: row.try_get("net_flow_quote").map_err(db_error)?,
                    inflow_velocity_quote_per_minute: row
                        .try_get("inflow_velocity")
                        .map_err(db_error)?,
                    unique_buyers: row.try_get("unique_buyers").map_err(db_error)?,
                    unique_sellers: row.try_get("unique_sellers").map_err(db_error)?,
                    issuer_cohort_buyers: row.try_get("issuer_cohort_buyers").map_err(db_error)?,
                    external_buyers: row.try_get("external_buyers").map_err(db_error)?,
                    unresolved_buyers: row.try_get("unresolved_buyers").map_err(db_error)?,
                    verified_winning_participants: row
                        .try_get("verified_winning_participants")
                        .map_err(db_error)?,
                    verified_losing_participants: row
                        .try_get("verified_losing_participants")
                        .map_err(db_error)?,
                    estimated_winning_participants: row
                        .try_get("estimated_winning_participants")
                        .map_err(db_error)?,
                    estimated_losing_participants: row
                        .try_get("estimated_losing_participants")
                        .map_err(db_error)?,
                    outcome_coverage_ratio: row
                        .try_get("outcome_coverage_ratio")
                        .map_err(db_error)?,
                    evidence_coverage_ratio: row
                        .try_get("evidence_coverage_ratio")
                        .map_err(db_error)?,
                    measured_at: row.try_get("measured_at").map_err(db_error)?,
                    limitations: row.try_get("limitations").map_err(db_error)?,
                })
            })
            .collect()
    }

    pub async fn clusters(
        &self,
        chain_id: u64,
        limit: usize,
    ) -> LedgerResult<Vec<ProtocolClusterCard>> {
        let rows = sqlx::query(
            r#"select c.cluster_id,d.domain_key,d.display_name_en,d.display_name_zh,d.domain_kind,
                      c.cluster_kind,c.subject_address,c.evidence_state,c.confidence,
                      c.first_seen_at,c.last_seen_at,
                      count(m.address)::bigint as member_count,
                      count(m.address) filter (where m.include_in_economic_scope)::bigint as economic_member_count
               from protocol_scoped_clusters c
               join protocol_intelligence_domains d on d.domain_id=c.domain_id
               left join protocol_scoped_cluster_members m on m.cluster_id=c.cluster_id
               where c.chain_id=$1 and c.status='active'
               group by c.cluster_id,d.domain_key,d.display_name_en,d.display_name_zh,d.domain_kind
               order by c.last_seen_at desc nulls last,c.confidence desc
               limit $2"#,
        )
        .bind(chain_id as i64)
        .bind(limit.clamp(1, 500) as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(db_error)?;
        rows.into_iter()
            .map(|row| {
                Ok(ProtocolClusterCard {
                    cluster_id: row.try_get("cluster_id").map_err(db_error)?,
                    domain_key: row.try_get("domain_key").map_err(db_error)?,
                    domain_name_en: row.try_get("display_name_en").map_err(db_error)?,
                    domain_name_zh: row.try_get("display_name_zh").map_err(db_error)?,
                    domain_kind: row.try_get("domain_kind").map_err(db_error)?,
                    cluster_kind: row.try_get("cluster_kind").map_err(db_error)?,
                    subject_address: row.try_get("subject_address").map_err(db_error)?,
                    evidence_state: row.try_get("evidence_state").map_err(db_error)?,
                    confidence: row.try_get("confidence").map_err(db_error)?,
                    member_count: row.try_get("member_count").map_err(db_error)?,
                    economic_member_count: row
                        .try_get("economic_member_count")
                        .map_err(db_error)?,
                    first_seen_at: row.try_get("first_seen_at").map_err(db_error)?,
                    last_seen_at: row.try_get("last_seen_at").map_err(db_error)?,
                })
            })
            .collect()
    }

    pub async fn cluster_detail(
        &self,
        chain_id: u64,
        cluster_id: Uuid,
    ) -> LedgerResult<Option<ProtocolClusterDetail>> {
        let row = sqlx::query(
            r#"select c.cluster_id,d.domain_key,d.display_name_en,d.display_name_zh,d.domain_kind,
                      c.cluster_kind,c.subject_address,c.evidence_state,c.confidence,
                      c.first_seen_at,c.last_seen_at,
                      count(m.address)::bigint as member_count,
                      count(m.address) filter (where m.include_in_economic_scope)::bigint
                        as economic_member_count
               from protocol_scoped_clusters c
               join protocol_intelligence_domains d on d.domain_id=c.domain_id
               left join protocol_scoped_cluster_members m on m.cluster_id=c.cluster_id
               where c.chain_id=$1 and c.cluster_id=$2 and c.status='active'
               group by c.cluster_id,d.domain_key,d.display_name_en,d.display_name_zh,d.domain_kind"#,
        )
        .bind(chain_id as i64)
        .bind(cluster_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_error)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let cluster = cluster_card_from_row(&row)?;
        let members = sqlx::query(
            r#"select address,member_role,evidence_state,confidence,interaction_count,
                      include_in_behavior_scope,include_in_economic_scope,
                      supporting_tx_hashes,evidence,first_seen_at,last_seen_at
               from protocol_scoped_cluster_members
               where cluster_id=$1
               order by case member_role
                 when 'creator' then 0 when 'worker' then 1 when 'executor' then 2
                 when 'settlement_recipient' then 3 when 'repeat_buyer' then 4 else 5 end,
                 confidence desc,interaction_count desc,address"#,
        )
        .bind(cluster_id)
        .fetch_all(&self.pool)
        .await
        .map_err(db_error)?
        .into_iter()
        .map(|row| {
            Ok(ProtocolClusterMemberRow {
                address: row.try_get("address").map_err(db_error)?,
                member_role: row.try_get("member_role").map_err(db_error)?,
                evidence_state: row.try_get("evidence_state").map_err(db_error)?,
                confidence: row.try_get("confidence").map_err(db_error)?,
                interaction_count: row.try_get("interaction_count").map_err(db_error)?,
                include_in_behavior_scope: row
                    .try_get("include_in_behavior_scope")
                    .map_err(db_error)?,
                include_in_economic_scope: row
                    .try_get("include_in_economic_scope")
                    .map_err(db_error)?,
                supporting_tx_hashes: row.try_get("supporting_tx_hashes").map_err(db_error)?,
                evidence: row
                    .try_get::<Json<Value>, _>("evidence")
                    .map_err(db_error)?
                    .0,
                first_seen_at: row.try_get("first_seen_at").map_err(db_error)?,
                last_seen_at: row.try_get("last_seen_at").map_err(db_error)?,
            })
        })
        .collect::<LedgerResult<Vec<_>>>()?;
        let edges = sqlx::query(
            r#"select source_address,target_address,relationship_type,evidence_state,
                      confidence,interaction_count,supporting_tx_hashes,evidence_reason,
                      first_seen_at,last_seen_at
               from protocol_scoped_cluster_edges where cluster_id=$1
               order by interaction_count desc,confidence desc,relationship_type"#,
        )
        .bind(cluster_id)
        .fetch_all(&self.pool)
        .await
        .map_err(db_error)?
        .into_iter()
        .map(|row| {
            Ok(ProtocolClusterEdgeRow {
                source_address: row.try_get("source_address").map_err(db_error)?,
                target_address: row.try_get("target_address").map_err(db_error)?,
                relationship_type: row.try_get("relationship_type").map_err(db_error)?,
                evidence_state: row.try_get("evidence_state").map_err(db_error)?,
                confidence: row.try_get("confidence").map_err(db_error)?,
                interaction_count: row.try_get("interaction_count").map_err(db_error)?,
                supporting_tx_hashes: row.try_get("supporting_tx_hashes").map_err(db_error)?,
                evidence_reason: row.try_get("evidence_reason").map_err(db_error)?,
                first_seen_at: row.try_get("first_seen_at").map_err(db_error)?,
                last_seen_at: row.try_get("last_seen_at").map_err(db_error)?,
            })
        })
        .collect::<LedgerResult<Vec<_>>>()?;
        Ok(Some(ProtocolClusterDetail {
            cluster,
            members,
            edges,
        }))
    }

    pub async fn address_lookup(
        &self,
        chain_id: u64,
        address: &str,
    ) -> LedgerResult<MarketAddressLookup> {
        let cluster_matches = sqlx::query(
            r#"select c.cluster_id,d.domain_key,d.display_name_en,d.display_name_zh,d.domain_kind,
                      c.cluster_kind,c.subject_address,c.evidence_state,c.confidence,
                      c.first_seen_at,c.last_seen_at,
                      (select count(*) from protocol_scoped_cluster_members all_members
                       where all_members.cluster_id=c.cluster_id)::bigint as member_count,
                      (select count(*) from protocol_scoped_cluster_members economic_members
                       where economic_members.cluster_id=c.cluster_id
                         and economic_members.include_in_economic_scope)::bigint
                        as economic_member_count,
                      member.member_role,member.evidence_state as member_evidence_state,
                      member.confidence as member_confidence,member.interaction_count
               from protocol_scoped_cluster_members member
               join protocol_scoped_clusters c on c.cluster_id=member.cluster_id
               join protocol_intelligence_domains d on d.domain_id=c.domain_id
               where member.chain_id=$1 and member.address=$2 and c.status='active'
               order by member.confidence desc,c.last_seen_at desc nulls last"#,
        )
        .bind(chain_id as i64)
        .bind(address)
        .fetch_all(&self.pool)
        .await
        .map_err(db_error)?
        .into_iter()
        .map(|row| {
            Ok(AddressClusterMatch {
                cluster: cluster_card_from_row(&row)?,
                member_role: row.try_get("member_role").map_err(db_error)?,
                member_evidence_state: row.try_get("member_evidence_state").map_err(db_error)?,
                member_confidence: row.try_get("member_confidence").map_err(db_error)?,
                interaction_count: row.try_get("interaction_count").map_err(db_error)?,
            })
        })
        .collect::<LedgerResult<Vec<_>>>()?;

        let protocol_matches = sqlx::query(
            r#"select d.domain_key,d.display_name_en,d.display_name_zh,d.protocol_key,
                      protocols.display_name as protocol_name,seeds.seed_role as address_role,
                      seeds.evidence_state,seeds.confidence,'protocol_seed_library'::text as source
               from protocol_seed_addresses seeds
               join protocol_intelligence_domains d on d.domain_id=seeds.domain_id
               join protocols on protocols.protocol_key=d.protocol_key
               where seeds.chain_id=$1 and seeds.address=$2 and seeds.status='active'
               union all
               select null::text,null::text,null::text,deployments.protocol_key,
                      protocols.display_name,deployments.contract_role,'observed'::text,
                      deployments.confidence,'protocol_registry'::text
               from protocol_deployments deployments
               join protocols on protocols.protocol_key=deployments.protocol_key
               where deployments.chain_id=$1 and deployments.address=$2
                 and deployments.status='active'
               order by confidence desc,protocol_key,address_role"#,
        )
        .bind(chain_id as i64)
        .bind(address)
        .fetch_all(&self.pool)
        .await
        .map_err(db_error)?
        .into_iter()
        .map(|row| {
            Ok(ProtocolAddressMatch {
                domain_key: row.try_get("domain_key").map_err(db_error)?,
                domain_name_en: row.try_get("display_name_en").map_err(db_error)?,
                domain_name_zh: row.try_get("display_name_zh").map_err(db_error)?,
                protocol_key: row.try_get("protocol_key").map_err(db_error)?,
                protocol_name: row.try_get("protocol_name").map_err(db_error)?,
                address_role: row.try_get("address_role").map_err(db_error)?,
                evidence_state: row.try_get("evidence_state").map_err(db_error)?,
                confidence: row.try_get("confidence").map_err(db_error)?,
                source: row.try_get("source").map_err(db_error)?,
            })
        })
        .collect::<LedgerResult<Vec<_>>>()?;

        let row = sqlx::query(
            r#"select
                 (select count(*) from protocol_token_launches
                  where chain_id=$1 and creator_address=$2)::bigint as launches_created,
                 (select count(*) from protocol_token_launches
                  where chain_id=$1 and token_address=$2)::bigint as launch_tokens,
                 (select count(distinct launch_id) from protocol_launch_trade_facts
                  where chain_id=$1 and is_canonical
                    and (trader_address=$2 or recipient_address=$2))::bigint as launches_traded,
                 (select count(*) from protocol_launch_trade_facts
                  where chain_id=$1 and is_canonical
                    and (trader_address=$2 or recipient_address=$2))::bigint as curve_trades,
                 (select count(*) from economic_verification_results
                  where chain_id=$1 and result_status='verified' and event_type='dex_arbitrage'
                    and subject_address=$2)::bigint as verified_arbitrage_executions,
                 (select count(*) from economic_verification_results
                  where chain_id=$1 and result_status='verified' and event_type='dex_arbitrage'
                    and profit_recipient=$2)::bigint as verified_settlements_received,
                 greatest(
                   (select max(launched_at) from protocol_token_launches
                    where chain_id=$1 and (creator_address=$2 or token_address=$2)),
                   (select max(occurred_at) from protocol_launch_trade_facts
                    where chain_id=$1 and is_canonical
                      and (trader_address=$2 or recipient_address=$2)),
                   (select max(coalesce(verified_at,updated_at)) from economic_verification_results
                    where chain_id=$1 and result_status='verified'
                      and (subject_address=$2 or profit_recipient=$2))
                 ) as last_seen_at"#,
        )
        .bind(chain_id as i64)
        .bind(address)
        .fetch_one(&self.pool)
        .await
        .map_err(db_error)?;
        Ok(MarketAddressLookup {
            chain_id,
            address: address.to_owned(),
            cluster_matches,
            protocol_matches,
            activity: AddressActivitySummary {
                launches_created: row.try_get("launches_created").map_err(db_error)?,
                launch_tokens: row.try_get("launch_tokens").map_err(db_error)?,
                launches_traded: row.try_get("launches_traded").map_err(db_error)?,
                curve_trades: row.try_get("curve_trades").map_err(db_error)?,
                verified_arbitrage_executions: row
                    .try_get("verified_arbitrage_executions")
                    .map_err(db_error)?,
                verified_settlements_received: row
                    .try_get("verified_settlements_received")
                    .map_err(db_error)?,
                last_seen_at: row.try_get("last_seen_at").map_err(db_error)?,
            },
        })
    }

    pub async fn verified_arbitrage(
        &self,
        chain_id: u64,
        limit: usize,
    ) -> LedgerResult<Vec<VerifiedExecutionRow>> {
        let rows = sqlx::query(
            r#"select result.tx_hash,result.subject_address,result.profit_recipient,
                      result.profit_token_address,result.profit_amount_raw,result.gas_cost_raw,
                      result.confidence,result.conclusion,result.verified_at,result.limitations,
                      token.symbol as profit_token_symbol,token.decimals as profit_token_decimals
               from economic_verification_results result
               left join erc20_token_metadata token
                 on token.chain_id=result.chain_id
                and token.token_address=result.profit_token_address
               where result.chain_id=$1 and result.result_status='verified'
               order by result.verified_at desc nulls last,result.updated_at desc
               limit $2"#,
        )
        .bind(chain_id as i64)
        .bind(limit.clamp(1, 500) as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(db_error)?;
        rows.into_iter()
            .map(|row| {
                Ok(VerifiedExecutionRow {
                    tx_hash: row.try_get("tx_hash").map_err(db_error)?,
                    subject_address: row.try_get("subject_address").map_err(db_error)?,
                    profit_recipient: row.try_get("profit_recipient").map_err(db_error)?,
                    profit_token_address: row.try_get("profit_token_address").map_err(db_error)?,
                    profit_token_symbol: row.try_get("profit_token_symbol").map_err(db_error)?,
                    profit_token_decimals: row
                        .try_get("profit_token_decimals")
                        .map_err(db_error)?,
                    profit_amount_raw: row.try_get("profit_amount_raw").map_err(db_error)?,
                    gas_cost_raw: row.try_get("gas_cost_raw").map_err(db_error)?,
                    confidence: row.try_get("confidence").map_err(db_error)?,
                    conclusion: row.try_get("conclusion").map_err(db_error)?,
                    verified_at: row.try_get("verified_at").map_err(db_error)?,
                    limitations: row.try_get("limitations").map_err(db_error)?,
                })
            })
            .collect()
    }

    pub async fn verified_arbitrage_rankings(
        &self,
        chain_id: u64,
        window: &str,
        limit: usize,
    ) -> LedgerResult<Vec<VerifiedEarningsRankingRow>> {
        let window_seconds = market_window_seconds(window);
        let rows = sqlx::query(
            r#"with scoped as (
                 select result.chain_id,result.subject_address,result.result_status,
                        result.profit_token_address,result.profit_amount_raw,result.gas_cost_raw,
                        candidate.block_timestamp
                 from economic_verification_results result
                 join arbitrage_candidates candidate
                   on candidate.chain_id=result.chain_id and candidate.tx_hash=result.tx_hash
                 where result.chain_id=$1
                   and candidate.block_timestamp is not null
                   and ($2::bigint is null or candidate.block_timestamp >=
                        now() - make_interval(secs => $2::double precision))
               ), coverage as (
                 select chain_id,subject_address,count(*)::bigint as evaluated_count,
                        count(*) filter (where result_status='verified')::bigint as verified_count
                 from scoped
                 group by chain_id,subject_address
               ), profit_totals as (
                 select chain_id,subject_address,profit_token_address,
                        sum(profit_amount_raw::numeric) as gross_profit,
                        sum(coalesce(gas_cost_raw,'0')::numeric) as gas_cost,
                        count(*)::bigint as execution_count,
                        max(block_timestamp) as last_execution_at
                 from scoped
                 where result_status='verified'
                   and profit_token_address is not null
                   and profit_amount_raw is not null
                 group by chain_id,subject_address,profit_token_address
               ), ranked as (
                 select totals.*,
                        row_number() over (
                          partition by totals.profit_token_address
                          order by totals.gross_profit desc,totals.subject_address
                        )::bigint as asset_rank
                 from profit_totals totals
               )
               select ranked.asset_rank,matched.cluster_id,ranked.subject_address,
                      ranked.profit_token_address,token.symbol as profit_token_symbol,
                      token.decimals as profit_token_decimals,
                      ranked.gross_profit::text as verified_gross_profit_raw,
                      ranked.gas_cost::text as exact_gas_cost_raw,
                      ranked.execution_count as verified_execution_count,
                      coverage.verified_count as economically_verified_count,
                      coverage.evaluated_count as economically_evaluated_count,
                      coverage.verified_count::double precision /
                        nullif(coverage.evaluated_count,0)::double precision as coverage_ratio,
                      ranked.last_execution_at
               from ranked
               join coverage
                 on coverage.chain_id=ranked.chain_id
                and coverage.subject_address=ranked.subject_address
               left join erc20_token_metadata token
                 on token.chain_id=ranked.chain_id
                and token.token_address=ranked.profit_token_address
               left join lateral (
                 select cluster.cluster_id
                 from protocol_scoped_clusters cluster
                 join protocol_intelligence_domains domain
                   on domain.domain_id=cluster.domain_id and domain.chain_id=cluster.chain_id
                 where cluster.chain_id=ranked.chain_id
                   and cluster.subject_address=ranked.subject_address
                   and cluster.cluster_kind='searcher'
                   and cluster.status='active'
                   and domain.domain_kind='dex_execution'
                 order by cluster.confidence desc,cluster.updated_at desc
                 limit 1
               ) matched on true
               order by ranked.profit_token_address,ranked.asset_rank
               limit $3"#,
        )
        .bind(chain_id as i64)
        .bind(window_seconds)
        .bind(limit.clamp(1, 500) as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(db_error)?;
        rows.into_iter()
            .map(|row| {
                Ok(VerifiedEarningsRankingRow {
                    rank: row.try_get("asset_rank").map_err(db_error)?,
                    cluster_id: row.try_get("cluster_id").map_err(db_error)?,
                    subject_address: row.try_get("subject_address").map_err(db_error)?,
                    profit_token_address: row.try_get("profit_token_address").map_err(db_error)?,
                    profit_token_symbol: row.try_get("profit_token_symbol").map_err(db_error)?,
                    profit_token_decimals: row
                        .try_get("profit_token_decimals")
                        .map_err(db_error)?,
                    verified_gross_profit_raw: row
                        .try_get("verified_gross_profit_raw")
                        .map_err(db_error)?,
                    exact_gas_cost_raw: row.try_get("exact_gas_cost_raw").map_err(db_error)?,
                    verified_execution_count: row
                        .try_get("verified_execution_count")
                        .map_err(db_error)?,
                    economically_verified_count: row
                        .try_get("economically_verified_count")
                        .map_err(db_error)?,
                    economically_evaluated_count: row
                        .try_get("economically_evaluated_count")
                        .map_err(db_error)?,
                    coverage_ratio: row.try_get("coverage_ratio").map_err(db_error)?,
                    last_execution_at: row.try_get("last_execution_at").map_err(db_error)?,
                })
            })
            .collect()
    }

    pub async fn seed_library(
        &self,
        chain_id: u64,
    ) -> LedgerResult<BTreeMap<String, Vec<ProtocolMarketAddressSeed>>> {
        let rows = sqlx::query(
            r#"select d.domain_key,s.address,s.seed_role,s.label_en,s.label_zh,s.confidence,
                      s.provenance_url,s.valid_from_block,s.valid_to_block,s.evidence
               from protocol_seed_addresses s
               join protocol_intelligence_domains d on d.domain_id=s.domain_id
               where s.chain_id=$1 and s.status='active'
               order by d.domain_key,s.seed_role,s.label_en"#,
        )
        .bind(chain_id as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(db_error)?;
        let mut result = BTreeMap::<String, Vec<ProtocolMarketAddressSeed>>::new();
        for row in rows {
            let domain_key: String = row.try_get("domain_key").map_err(db_error)?;
            result
                .entry(domain_key)
                .or_default()
                .push(ProtocolMarketAddressSeed {
                    address: row.try_get("address").map_err(db_error)?,
                    seed_role: row.try_get("seed_role").map_err(db_error)?,
                    label_en: row.try_get("label_en").map_err(db_error)?,
                    label_zh: row.try_get("label_zh").map_err(db_error)?,
                    confidence: row.try_get("confidence").map_err(db_error)?,
                    provenance_url: row.try_get("provenance_url").map_err(db_error)?,
                    valid_from_block: row
                        .try_get::<Option<i64>, _>("valid_from_block")
                        .map_err(db_error)?
                        .map(|value| value as u64),
                    valid_to_block: row
                        .try_get::<Option<i64>, _>("valid_to_block")
                        .map_err(db_error)?
                        .map(|value| value as u64),
                    evidence: row
                        .try_get::<Json<Value>, _>("evidence")
                        .map_err(db_error)?
                        .0,
                });
        }
        Ok(result)
    }
}

fn market_window_seconds(window: &str) -> Option<i64> {
    match window {
        "5m" => Some(5 * 60),
        "15m" => Some(15 * 60),
        "1h" => Some(60 * 60),
        "6h" => Some(6 * 60 * 60),
        "24h" => Some(24 * 60 * 60),
        "7d" => Some(7 * 24 * 60 * 60),
        "30d" => Some(30 * 24 * 60 * 60),
        "lifetime" => None,
        _ => Some(24 * 60 * 60),
    }
}

fn validate_manifest(manifest: &ProtocolMarketSeedManifest) -> LedgerResult<()> {
    if manifest.schema_version != PROTOCOL_MARKET_SEED_SCHEMA {
        return Err(LedgerError::MalformedEvidence(format!(
            "unsupported market seed schema {}",
            manifest.schema_version
        )));
    }
    if manifest.dataset_key.trim().is_empty()
        || manifest.dataset_version.trim().is_empty()
        || manifest.source.source_key.trim().is_empty()
        || manifest.domains.is_empty()
    {
        return Err(LedgerError::MalformedEvidence(
            "dataset, source, and at least one domain are required".to_owned(),
        ));
    }
    if !(0.0..=1.0).contains(&manifest.source.trust_score) {
        return Err(LedgerError::MalformedEvidence(
            "source trust_score must be between 0 and 1".to_owned(),
        ));
    }
    let mut domain_keys = std::collections::BTreeSet::new();
    for domain in &manifest.domains {
        if domain.chain_id == 0
            || domain.domain_key.trim().is_empty()
            || domain.protocol_key.trim().is_empty()
            || !domain_keys.insert(domain.domain_key.as_str())
        {
            return Err(LedgerError::MalformedEvidence(
                "each protocol domain must have a unique key and a non-zero chain".to_owned(),
            ));
        }
        let mut scoped_seeds = std::collections::BTreeSet::new();
        for seed in &domain.seeds {
            let address = normalize_address(&seed.address)?;
            if !scoped_seeds.insert((address, seed.seed_role.as_str())) {
                return Err(LedgerError::MalformedEvidence(format!(
                    "duplicate seed in domain {}",
                    domain.domain_key
                )));
            }
            if !(0.0..=1.0).contains(&seed.confidence)
                || seed.provenance_url.trim().is_empty()
                || seed.label_en.trim().is_empty()
                || seed.label_zh.trim().is_empty()
            {
                return Err(LedgerError::MalformedEvidence(format!(
                    "invalid seed metadata in domain {}",
                    domain.domain_key
                )));
            }
            if seed
                .valid_to_block
                .is_some_and(|last| seed.valid_from_block.is_some_and(|first| first > last))
            {
                return Err(LedgerError::MalformedEvidence(
                    "seed block range is reversed".to_owned(),
                ));
            }
        }
    }
    Ok(())
}

fn normalize_address(value: &str) -> LedgerResult<String> {
    let value = value.trim().to_ascii_lowercase();
    let body = value.strip_prefix("0x").unwrap_or(&value);
    if body.len() != 40 || !body.chars().all(|character| character.is_ascii_hexdigit()) {
        return Err(LedgerError::MalformedEvidence(format!(
            "invalid EVM address {value}"
        )));
    }
    Ok(format!("0x{body}"))
}

fn normalize_window(window: &str) -> &'static str {
    match window {
        "5m" => "5m",
        "15m" => "15m",
        "1h" => "1h",
        "6h" => "6h",
        "7d" => "7d",
        "30d" => "30d",
        "lifetime" => "lifetime",
        _ => "24h",
    }
}

fn sha256_hex(value: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(value);
    hex::encode(digest.finalize())
}

fn json_object() -> Value {
    json!({})
}

fn db_error(error: impl std::fmt::Display) -> LedgerError {
    LedgerError::Persistence(error.to_string())
}

fn cluster_card_from_row(row: &sqlx::postgres::PgRow) -> LedgerResult<ProtocolClusterCard> {
    Ok(ProtocolClusterCard {
        cluster_id: row.try_get("cluster_id").map_err(db_error)?,
        domain_key: row.try_get("domain_key").map_err(db_error)?,
        domain_name_en: row.try_get("display_name_en").map_err(db_error)?,
        domain_name_zh: row.try_get("display_name_zh").map_err(db_error)?,
        domain_kind: row.try_get("domain_kind").map_err(db_error)?,
        cluster_kind: row.try_get("cluster_kind").map_err(db_error)?,
        subject_address: row.try_get("subject_address").map_err(db_error)?,
        evidence_state: row.try_get("evidence_state").map_err(db_error)?,
        confidence: row.try_get("confidence").map_err(db_error)?,
        member_count: row.try_get("member_count").map_err(db_error)?,
        economic_member_count: row.try_get("economic_member_count").map_err(db_error)?,
        first_seen_at: row.try_get("first_seen_at").map_err(db_error)?,
        last_seen_at: row.try_get("last_seen_at").map_err(db_error)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pons_manifest_is_valid_and_all_addresses_are_normalized() {
        let raw = include_str!("../../config/intelligence/robinhood-market-protocols.v1.json");
        let manifest = parse_protocol_market_manifest(raw).expect("valid manifest");
        assert_eq!(manifest.domains.len(), 2);
        assert!(
            manifest
                .domains
                .iter()
                .flat_map(|domain| &domain.seeds)
                .all(|seed| {
                    seed.address == seed.address.to_ascii_lowercase()
                        && seed.provenance_url.starts_with("https://")
                })
        );
    }

    #[test]
    fn invalid_or_global_seed_scope_is_rejected() {
        let mut manifest = parse_protocol_market_manifest(include_str!(
            "../../config/intelligence/robinhood-market-protocols.v1.json"
        ))
        .unwrap();
        manifest.domains[1].domain_key = manifest.domains[0].domain_key.clone();
        assert!(validate_manifest(&manifest).is_err());
    }

    #[test]
    fn earnings_windows_have_explicit_time_bounds() {
        assert_eq!(market_window_seconds("24h"), Some(86_400));
        assert_eq!(market_window_seconds("30d"), Some(2_592_000));
        assert_eq!(market_window_seconds("lifetime"), None);
        assert_eq!(market_window_seconds("invalid"), Some(86_400));
    }
}
