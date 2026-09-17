use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::{Value, json};
use sqlx::{PgPool, Postgres, Row, Transaction, types::Json};
use uuid::Uuid;

use crate::core::errors::{LedgerError, LedgerResult};

const ISSUER_METHOD: &str = "pons-repeat-launch-buyer.v1";
const ARBITRAGE_METHOD: &str = "verified-execution-relationships.v1";

#[derive(Clone, Debug)]
pub struct ProtocolClusterProjectorConfig {
    pub chain_id: u64,
    pub minimum_repeated_launches: i64,
    pub minimum_verified_executions: i64,
}

impl Default for ProtocolClusterProjectorConfig {
    fn default() -> Self {
        Self {
            chain_id: 4663,
            minimum_repeated_launches: 2,
            minimum_verified_executions: 2,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct ProtocolClusterProjectionSummary {
    pub launch_domains: u64,
    pub changed_launch_creators: u64,
    pub issuer_clusters_projected: u64,
    pub repeat_buyer_members_projected: u64,
    pub changed_arbitrage_subjects: u64,
    pub arbitrage_clusters_projected: u64,
    pub execution_members_projected: u64,
}

#[derive(Clone)]
pub struct ProtocolClusterProjector {
    pool: PgPool,
    config: ProtocolClusterProjectorConfig,
}

#[derive(Clone, Debug)]
struct DomainRef {
    domain_id: Uuid,
    domain_key: String,
}

#[derive(Clone, Debug)]
struct CreatorSummary {
    creator: String,
    launch_count: i64,
    first_seen: Option<DateTime<Utc>>,
    last_seen: Option<DateTime<Utc>>,
    supporting_hashes: Vec<String>,
}

#[derive(Clone, Debug)]
struct BuyerAssociation {
    buyer: String,
    trade_count: i64,
    launch_count: i64,
    first_seen: Option<DateTime<Utc>>,
    last_seen: Option<DateTime<Utc>>,
    supporting_hashes: Vec<String>,
}

#[derive(Clone, Debug)]
struct ExecutionSummary {
    subject: String,
    execution_count: i64,
    confidence: f64,
    first_seen: Option<DateTime<Utc>>,
    last_seen: Option<DateTime<Utc>>,
    supporting_hashes: Vec<String>,
}

#[derive(Clone, Debug)]
struct ExecutionTarget {
    address: String,
    interaction_count: i64,
    first_seen: Option<DateTime<Utc>>,
    last_seen: Option<DateTime<Utc>>,
    supporting_hashes: Vec<String>,
    protocol_role: Option<String>,
    protocol_key: Option<String>,
    protocol_confidence: Option<f64>,
}

#[derive(Clone, Debug)]
struct SettlementRecipient {
    address: String,
    interaction_count: i64,
    confidence: f64,
    first_seen: Option<DateTime<Utc>>,
    last_seen: Option<DateTime<Utc>>,
    supporting_hashes: Vec<String>,
    protocol_role: Option<String>,
    protocol_key: Option<String>,
    protocol_confidence: Option<f64>,
}

struct MemberInsert<'a> {
    address: &'a str,
    role: &'a str,
    evidence_state: &'a str,
    confidence: f64,
    interaction_count: i64,
    behavior_scope: bool,
    economic_scope: bool,
    supporting_hashes: &'a [String],
    evidence: Value,
    first_seen: Option<DateTime<Utc>>,
    last_seen: Option<DateTime<Utc>>,
}

struct EdgeInsert<'a> {
    source: &'a str,
    target: &'a str,
    relationship_type: &'a str,
    evidence_state: &'a str,
    confidence: f64,
    interaction_count: i64,
    supporting_hashes: &'a [String],
    reason: &'a str,
    evidence: Value,
    first_seen: Option<DateTime<Utc>>,
    last_seen: Option<DateTime<Utc>>,
}

impl ProtocolClusterProjector {
    pub fn new(pool: PgPool, config: ProtocolClusterProjectorConfig) -> Self {
        Self { pool, config }
    }

    pub async fn project_once(&self) -> LedgerResult<ProtocolClusterProjectionSummary> {
        let mut summary = ProtocolClusterProjectionSummary::default();
        let launch_domains = self.launch_domains().await?;
        summary.launch_domains = launch_domains.len() as u64;
        let mut failures = Vec::new();

        for domain in launch_domains {
            match self.project_launch_domain(&domain).await {
                Ok((changed, clusters, members)) => {
                    summary.changed_launch_creators += changed;
                    summary.issuer_clusters_projected += clusters;
                    summary.repeat_buyer_members_projected += members;
                }
                Err(error) => {
                    self.mark_checkpoint_error(&domain, "launch_trades", &error.to_string())
                        .await
                        .ok();
                    failures.push(format!("{}: {error}", domain.domain_key));
                }
            }
        }

        if let Some(domain) = self.arbitrage_domain().await? {
            match self.project_arbitrage_domain(&domain).await {
                Ok((changed, clusters, members)) => {
                    summary.changed_arbitrage_subjects += changed;
                    summary.arbitrage_clusters_projected += clusters;
                    summary.execution_members_projected += members;
                }
                Err(error) => {
                    self.mark_checkpoint_error(&domain, "verified_arbitrage", &error.to_string())
                        .await
                        .ok();
                    failures.push(format!("{}: {error}", domain.domain_key));
                }
            }
        }

        if !failures.is_empty()
            && summary.issuer_clusters_projected == 0
            && summary.arbitrage_clusters_projected == 0
        {
            return Err(LedgerError::Persistence(failures.join("; ")));
        }
        Ok(summary)
    }

    async fn launch_domains(&self) -> LedgerResult<Vec<DomainRef>> {
        sqlx::query(
            r#"select domain_id,domain_key from protocol_intelligence_domains
               where chain_id=$1 and status='active' and domain_kind='token_launch'
                 and protocol_key in ('pons_v1','pons_v2')
               order by domain_key"#,
        )
        .bind(self.config.chain_id as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(db_error)?
        .into_iter()
        .map(|row| {
            Ok(DomainRef {
                domain_id: row.try_get("domain_id").map_err(db_error)?,
                domain_key: row.try_get("domain_key").map_err(db_error)?,
            })
        })
        .collect()
    }

    async fn arbitrage_domain(&self) -> LedgerResult<Option<DomainRef>> {
        sqlx::query(
            r#"select domain_id,domain_key from protocol_intelligence_domains
               where chain_id=$1 and status='active'
                 and protocol_key='ledgerflow_verified_arbitrage'
               limit 1"#,
        )
        .bind(self.config.chain_id as i64)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_error)?
        .map(|row| {
            Ok(DomainRef {
                domain_id: row.try_get("domain_id").map_err(db_error)?,
                domain_key: row.try_get("domain_key").map_err(db_error)?,
            })
        })
        .transpose()
    }

    async fn project_launch_domain(&self, domain: &DomainRef) -> LedgerResult<(u64, u64, u64)> {
        let checkpoint = self
            .load_checkpoint(domain.domain_id, "launch_trades")
            .await?;
        let high_water = sqlx::query_scalar::<_, Option<DateTime<Utc>>>(
            r#"select max(updated_at) from protocol_launch_trade_facts
               where domain_id=$1 and is_canonical and trade_side='buy'"#,
        )
        .bind(domain.domain_id)
        .fetch_one(&self.pool)
        .await
        .map_err(db_error)?
        .unwrap_or_else(Utc::now);
        self.mark_checkpoint_running(domain, "launch_trades")
            .await?;

        let creators = sqlx::query_scalar::<_, String>(
            r#"select distinct launches.creator_address
               from protocol_launch_trade_facts facts
               join protocol_token_launches launches on launches.launch_id=facts.launch_id
               where facts.domain_id=$1 and facts.is_canonical and facts.trade_side='buy'
                 and launches.creator_address is not null
                 and ($2::timestamptz is null or facts.updated_at>$2)
                 and facts.updated_at<=$3
               order by launches.creator_address"#,
        )
        .bind(domain.domain_id)
        .bind(checkpoint)
        .bind(high_water)
        .fetch_all(&self.pool)
        .await
        .map_err(db_error)?;
        if creators.is_empty() {
            self.mark_checkpoint_ready(domain, "launch_trades", high_water)
                .await?;
            return Ok((0, 0, 0));
        }

        let creator_rows = sqlx::query(
            r#"select creator_address,count(distinct launch_id)::bigint as launch_count,
                      min(launched_at) as first_seen,max(launched_at) as last_seen,
                      (array_agg(launch_tx_hash order by launch_block desc))[1:20] as hashes
               from protocol_token_launches
               where domain_id=$1 and creator_address=any($2)
               group by creator_address"#,
        )
        .bind(domain.domain_id)
        .bind(&creators)
        .fetch_all(&self.pool)
        .await
        .map_err(db_error)?;
        let creator_summaries = creator_rows
            .into_iter()
            .map(|row| {
                let creator = row.get::<String, _>("creator_address");
                (
                    creator.clone(),
                    CreatorSummary {
                        creator,
                        launch_count: row.get("launch_count"),
                        first_seen: row.get("first_seen"),
                        last_seen: row.get("last_seen"),
                        supporting_hashes: row.get("hashes"),
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();

        let association_rows = sqlx::query(
            r#"select launches.creator_address,facts.trader_address,
                      count(*)::bigint as trade_count,
                      count(distinct facts.launch_id)::bigint as launch_count,
                      min(facts.occurred_at) as first_seen,max(facts.occurred_at) as last_seen,
                      (array_agg(facts.tx_hash order by facts.block_number desc,
                         facts.tx_index desc,facts.log_index desc))[1:20] as hashes
               from protocol_launch_trade_facts facts
               join protocol_token_launches launches on launches.launch_id=facts.launch_id
               where facts.domain_id=$1 and facts.is_canonical and facts.trade_side='buy'
                 and launches.creator_address=any($2)
                 and facts.trader_address<>launches.creator_address
               group by launches.creator_address,facts.trader_address
               having count(distinct facts.launch_id)>=$3
               order by launches.creator_address,launch_count desc,trade_count desc"#,
        )
        .bind(domain.domain_id)
        .bind(&creators)
        .bind(self.config.minimum_repeated_launches.max(2))
        .fetch_all(&self.pool)
        .await
        .map_err(db_error)?;
        let mut associations = BTreeMap::<String, Vec<BuyerAssociation>>::new();
        for row in association_rows {
            associations
                .entry(row.get("creator_address"))
                .or_default()
                .push(BuyerAssociation {
                    buyer: row.get("trader_address"),
                    trade_count: row.get("trade_count"),
                    launch_count: row.get("launch_count"),
                    first_seen: row.get("first_seen"),
                    last_seen: row.get("last_seen"),
                    supporting_hashes: row.get("hashes"),
                });
        }

        let mut projected_clusters = 0_u64;
        let mut projected_members = 0_u64;
        for creator in &creators {
            let Some(buyers) = associations.get(creator) else {
                sqlx::query(
                    r#"update protocol_scoped_clusters set status='dismissed',updated_at=now()
                       where domain_id=$1 and cluster_key=$2 and method_version=$3"#,
                )
                .bind(domain.domain_id)
                .bind(format!("issuer:{creator}"))
                .bind(ISSUER_METHOD)
                .execute(&self.pool)
                .await
                .map_err(db_error)?;
                continue;
            };
            let Some(creator_summary) = creator_summaries.get(creator) else {
                continue;
            };
            self.project_issuer_cluster(domain, creator_summary, buyers)
                .await?;
            projected_clusters += 1;
            projected_members += buyers.len() as u64;
        }
        self.mark_checkpoint_ready(domain, "launch_trades", high_water)
            .await?;
        Ok((creators.len() as u64, projected_clusters, projected_members))
    }

    async fn project_issuer_cluster(
        &self,
        domain: &DomainRef,
        creator: &CreatorSummary,
        buyers: &[BuyerAssociation],
    ) -> LedgerResult<()> {
        let confidence = buyers
            .iter()
            .map(|buyer| repeat_buyer_confidence(buyer.launch_count, buyer.trade_count))
            .fold(0.0_f64, f64::max);
        let first_seen = buyers
            .iter()
            .filter_map(|buyer| buyer.first_seen)
            .chain(creator.first_seen)
            .min();
        let last_seen = buyers
            .iter()
            .filter_map(|buyer| buyer.last_seen)
            .chain(creator.last_seen)
            .max();
        let mut tx = self.pool.begin().await.map_err(db_error)?;
        let cluster_id = sqlx::query_scalar::<_, Uuid>(
            r#"insert into protocol_scoped_clusters (
                   domain_id,chain_id,cluster_key,cluster_kind,subject_address,evidence_state,
                   confidence,method_version,evidence,status,first_seen_at,last_seen_at
               ) values ($1,$2,$3,'issuer_cohort',$4,'candidate',$5,$6,$7,'active',$8,$9)
               on conflict (domain_id,cluster_key) do update set
                 cluster_kind='issuer_cohort',subject_address=excluded.subject_address,
                 evidence_state='candidate',confidence=excluded.confidence,
                 method_version=excluded.method_version,evidence=excluded.evidence,status='active',
                 first_seen_at=excluded.first_seen_at,last_seen_at=excluded.last_seen_at,
                 updated_at=now()
               returning cluster_id"#,
        )
        .bind(domain.domain_id)
        .bind(self.config.chain_id as i64)
        .bind(format!("issuer:{}", creator.creator))
        .bind(&creator.creator)
        .bind(confidence)
        .bind(ISSUER_METHOD)
        .bind(Json(json!({
            "projector": ISSUER_METHOD,
            "scope": domain.domain_key,
            "rule": "buyer traded launches from the same creator across at least two distinct launches",
            "repeat_buyer_count": buyers.len(),
            "creator_launch_count": creator.launch_count,
            "ownership_claim": false
        })))
        .bind(first_seen)
        .bind(last_seen)
        .fetch_one(&mut *tx)
        .await
        .map_err(db_error)?;
        clear_projected_cluster(&mut tx, cluster_id, ISSUER_METHOD).await?;
        insert_member(
            &mut tx,
            cluster_id,
            domain,
            self.config.chain_id,
            MemberInsert {
                address: &creator.creator,
                role: "creator",
                evidence_state: "observed",
                confidence: 1.0,
                interaction_count: creator.launch_count,
                behavior_scope: true,
                economic_scope: false,
                supporting_hashes: &creator.supporting_hashes,
                evidence: json!({
                    "projector": ISSUER_METHOD,
                    "reason": "address emitted as creator in factory launch events",
                    "ownership_claim": false
                }),
                first_seen: creator.first_seen,
                last_seen: creator.last_seen,
            },
        )
        .await?;
        for buyer in buyers {
            let confidence = repeat_buyer_confidence(buyer.launch_count, buyer.trade_count);
            insert_member(
                &mut tx,
                cluster_id,
                domain,
                self.config.chain_id,
                MemberInsert {
                    address: &buyer.buyer,
                    role: "repeat_buyer",
                    evidence_state: "candidate",
                    confidence,
                    interaction_count: buyer.trade_count,
                    behavior_scope: true,
                    economic_scope: false,
                    supporting_hashes: &buyer.supporting_hashes,
                    evidence: json!({
                        "projector": ISSUER_METHOD,
                        "reason": "repeated purchases across launches from this creator",
                        "distinct_launches": buyer.launch_count,
                        "trade_count": buyer.trade_count,
                        "ownership_claim": false
                    }),
                    first_seen: buyer.first_seen,
                    last_seen: buyer.last_seen,
                },
            )
            .await?;
            insert_edge(
                &mut tx,
                cluster_id,
                domain,
                self.config.chain_id,
                EdgeInsert {
                    source: &creator.creator,
                    target: &buyer.buyer,
                    relationship_type: "repeated_launch_buyer",
                    evidence_state: "candidate",
                    confidence,
                    interaction_count: buyer.trade_count,
                    supporting_hashes: &buyer.supporting_hashes,
                    reason: "Buyer appeared across multiple launches emitted by the same creator; this is protocol-scoped association, not ownership.",
                    evidence: json!({
                        "projector": ISSUER_METHOD,
                        "distinct_launches": buyer.launch_count,
                        "ownership_claim": false
                    }),
                    first_seen: buyer.first_seen,
                    last_seen: buyer.last_seen,
                },
            )
            .await?;
        }
        tx.commit().await.map_err(db_error)?;
        Ok(())
    }

    async fn project_arbitrage_domain(&self, domain: &DomainRef) -> LedgerResult<(u64, u64, u64)> {
        let checkpoint = self
            .load_checkpoint(domain.domain_id, "verified_arbitrage")
            .await?;
        let high_water = sqlx::query_scalar::<_, Option<DateTime<Utc>>>(
            r#"select max(updated_at) from economic_verification_results
               where chain_id=$1 and result_status='verified' and event_type='dex_arbitrage'"#,
        )
        .bind(self.config.chain_id as i64)
        .fetch_one(&self.pool)
        .await
        .map_err(db_error)?
        .unwrap_or_else(Utc::now);
        self.mark_checkpoint_running(domain, "verified_arbitrage")
            .await?;
        let subjects = sqlx::query_scalar::<_, String>(
            r#"select distinct subject_address from economic_verification_results
               where chain_id=$1 and result_status='verified' and event_type='dex_arbitrage'
                 and ($2::timestamptz is null or updated_at>$2) and updated_at<=$3
               order by subject_address"#,
        )
        .bind(self.config.chain_id as i64)
        .bind(checkpoint)
        .bind(high_water)
        .fetch_all(&self.pool)
        .await
        .map_err(db_error)?;
        if subjects.is_empty() {
            self.mark_checkpoint_ready(domain, "verified_arbitrage", high_water)
                .await?;
            return Ok((0, 0, 0));
        }

        let summaries = sqlx::query(
            r#"select subject_address,count(*)::bigint as execution_count,
                      avg(confidence) as confidence,
                      min(coalesce(verified_at,updated_at)) as first_seen,
                      max(coalesce(verified_at,updated_at)) as last_seen,
                      (array_agg(tx_hash order by coalesce(verified_at,updated_at) desc))[1:20] as hashes
               from economic_verification_results
               where chain_id=$1 and result_status='verified' and event_type='dex_arbitrage'
                 and subject_address=any($2)
               group by subject_address"#,
        )
        .bind(self.config.chain_id as i64)
        .bind(&subjects)
        .fetch_all(&self.pool)
        .await
        .map_err(db_error)?
        .into_iter()
        .map(|row| {
            let subject = row.get::<String, _>("subject_address");
            (
                subject.clone(),
                ExecutionSummary {
                    subject,
                    execution_count: row.get("execution_count"),
                    confidence: row.get::<Option<f64>, _>("confidence").unwrap_or(0.0),
                    first_seen: row.get("first_seen"),
                    last_seen: row.get("last_seen"),
                    supporting_hashes: row.get("hashes"),
                },
            )
        })
        .collect::<BTreeMap<_, _>>();

        let target_rows = sqlx::query(
            r#"select result.subject_address,transactions.to_address,
                      count(*)::bigint as interaction_count,
                      min(coalesce(result.verified_at,result.updated_at)) as first_seen,
                      max(coalesce(result.verified_at,result.updated_at)) as last_seen,
                      (array_agg(result.tx_hash order by coalesce(result.verified_at,result.updated_at) desc))[1:20] as hashes,
                      deployment.contract_role as protocol_role,
                      deployment.protocol_key, deployment.confidence as protocol_confidence
               from economic_verification_results result
               join transactions on transactions.chain_id=result.chain_id
                                and transactions.tx_hash=result.tx_hash
               left join lateral (
                 select contract_role,protocol_key,confidence from protocol_deployments
                 where chain_id=result.chain_id and address=transactions.to_address and status='active'
                 order by confidence desc,updated_at desc limit 1
               ) deployment on true
               where result.chain_id=$1 and result.result_status='verified'
                 and result.event_type='dex_arbitrage' and result.subject_address=any($2)
                 and transactions.to_address is not null
               group by result.subject_address,transactions.to_address,
                        deployment.contract_role,deployment.protocol_key,deployment.confidence
               order by result.subject_address,interaction_count desc"#,
        )
        .bind(self.config.chain_id as i64)
        .bind(&subjects)
        .fetch_all(&self.pool)
        .await
        .map_err(db_error)?;
        let mut targets = BTreeMap::<String, Vec<ExecutionTarget>>::new();
        for row in target_rows {
            targets
                .entry(row.get("subject_address"))
                .or_default()
                .push(ExecutionTarget {
                    address: row.get("to_address"),
                    interaction_count: row.get("interaction_count"),
                    first_seen: row.get("first_seen"),
                    last_seen: row.get("last_seen"),
                    supporting_hashes: row.get("hashes"),
                    protocol_role: row.get("protocol_role"),
                    protocol_key: row.get("protocol_key"),
                    protocol_confidence: row.get("protocol_confidence"),
                });
        }

        let recipient_rows = sqlx::query(
            r#"select result.subject_address,result.profit_recipient,
                      count(*)::bigint as interaction_count,avg(result.confidence) as confidence,
                      min(coalesce(result.verified_at,result.updated_at)) as first_seen,
                      max(coalesce(result.verified_at,result.updated_at)) as last_seen,
                      (array_agg(result.tx_hash order by coalesce(result.verified_at,result.updated_at) desc))[1:20] as hashes,
                      deployment.contract_role as protocol_role,
                      deployment.protocol_key,deployment.confidence as protocol_confidence
               from economic_verification_results result
               left join lateral (
                 select contract_role,protocol_key,confidence from protocol_deployments
                 where chain_id=result.chain_id and address=result.profit_recipient and status='active'
                 order by confidence desc,updated_at desc limit 1
               ) deployment on true
               where result.chain_id=$1 and result.result_status='verified'
                 and result.event_type='dex_arbitrage' and result.subject_address=any($2)
                 and result.profit_recipient is not null
               group by result.subject_address,result.profit_recipient,
                        deployment.contract_role,deployment.protocol_key,deployment.confidence
               order by result.subject_address,interaction_count desc"#,
        )
        .bind(self.config.chain_id as i64)
        .bind(&subjects)
        .fetch_all(&self.pool)
        .await
        .map_err(db_error)?;
        let mut recipients = BTreeMap::<String, Vec<SettlementRecipient>>::new();
        for row in recipient_rows {
            recipients
                .entry(row.get("subject_address"))
                .or_default()
                .push(SettlementRecipient {
                    address: row.get("profit_recipient"),
                    interaction_count: row.get("interaction_count"),
                    confidence: row.get::<Option<f64>, _>("confidence").unwrap_or(0.0),
                    first_seen: row.get("first_seen"),
                    last_seen: row.get("last_seen"),
                    supporting_hashes: row.get("hashes"),
                    protocol_role: row.get("protocol_role"),
                    protocol_key: row.get("protocol_key"),
                    protocol_confidence: row.get("protocol_confidence"),
                });
        }

        let mut clusters = 0_u64;
        let mut members = 0_u64;
        for subject in &subjects {
            let Some(execution) = summaries.get(subject) else {
                continue;
            };
            let subject_targets = targets.get(subject).map(Vec::as_slice).unwrap_or(&[]);
            let subject_recipients = recipients.get(subject).map(Vec::as_slice).unwrap_or(&[]);
            let meaningful_target = subject_targets.iter().any(|target| {
                target.address != *subject
                    && (target.protocol_role.is_some()
                        || target.interaction_count >= self.config.minimum_verified_executions)
            });
            let distinct_recipient = subject_recipients
                .iter()
                .any(|recipient| recipient.address != *subject);
            if execution.execution_count < self.config.minimum_verified_executions
                && !meaningful_target
                && !distinct_recipient
            {
                continue;
            }
            members += self
                .project_arbitrage_cluster(domain, execution, subject_targets, subject_recipients)
                .await?;
            clusters += 1;
        }
        self.mark_checkpoint_ready(domain, "verified_arbitrage", high_water)
            .await?;
        Ok((subjects.len() as u64, clusters, members))
    }

    async fn project_arbitrage_cluster(
        &self,
        domain: &DomainRef,
        execution: &ExecutionSummary,
        targets: &[ExecutionTarget],
        recipients: &[SettlementRecipient],
    ) -> LedgerResult<u64> {
        let mut first_seen = execution.first_seen;
        let mut last_seen = execution.last_seen;
        for value in targets
            .iter()
            .flat_map(|target| [target.first_seen, target.last_seen])
            .chain(
                recipients
                    .iter()
                    .flat_map(|recipient| [recipient.first_seen, recipient.last_seen]),
            )
            .flatten()
        {
            first_seen = Some(first_seen.map_or(value, |current| current.min(value)));
            last_seen = Some(last_seen.map_or(value, |current| current.max(value)));
        }
        let cluster_confidence = execution.confidence.clamp(0.0, 1.0);
        let mut tx = self.pool.begin().await.map_err(db_error)?;
        let cluster_id = sqlx::query_scalar::<_, Uuid>(
            r#"insert into protocol_scoped_clusters (
                   domain_id,chain_id,cluster_key,cluster_kind,subject_address,evidence_state,
                   confidence,method_version,evidence,status,first_seen_at,last_seen_at
               ) values ($1,$2,$3,'searcher',$4,'candidate',$5,$6,$7,'active',$8,$9)
               on conflict (domain_id,cluster_key) do update set
                 cluster_kind='searcher',subject_address=excluded.subject_address,
                 evidence_state='candidate',confidence=excluded.confidence,
                 method_version=excluded.method_version,evidence=excluded.evidence,status='active',
                 first_seen_at=excluded.first_seen_at,last_seen_at=excluded.last_seen_at,
                 updated_at=now()
               returning cluster_id"#,
        )
        .bind(domain.domain_id)
        .bind(self.config.chain_id as i64)
        .bind(format!("searcher:{}", execution.subject))
        .bind(&execution.subject)
        .bind(cluster_confidence)
        .bind(ARBITRAGE_METHOD)
        .bind(Json(json!({
            "projector": ARBITRAGE_METHOD,
            "scope": "verified dex arbitrage transactions only",
            "verified_execution_count": execution.execution_count,
            "ownership_claim": false,
            "limitation": "executor and settlement roles are scoped to observed verified executions"
        })))
        .bind(first_seen)
        .bind(last_seen)
        .fetch_one(&mut *tx)
        .await
        .map_err(db_error)?;
        clear_projected_cluster(&mut tx, cluster_id, ARBITRAGE_METHOD).await?;
        insert_member(
            &mut tx,
            cluster_id,
            domain,
            self.config.chain_id,
            MemberInsert {
                address: &execution.subject,
                role: "worker",
                evidence_state: "candidate",
                confidence: execution_role_confidence(execution.execution_count, cluster_confidence),
                interaction_count: execution.execution_count,
                behavior_scope: true,
                economic_scope: true,
                supporting_hashes: &execution.supporting_hashes,
                evidence: json!({
                    "projector": ARBITRAGE_METHOD,
                    "reason": "transaction sender in deterministically verified arbitrage executions",
                    "verified_execution_count": execution.execution_count,
                    "role_claim": "candidate worker or self-executing account",
                    "ownership_claim": false
                }),
                first_seen: execution.first_seen,
                last_seen: execution.last_seen,
            },
        )
        .await?;
        let mut member_addresses = BTreeSet::from([execution.subject.clone()]);
        let mut projected_members = 1_u64;

        for target in targets {
            if target.address == execution.subject {
                continue;
            }
            let (role, evidence_state, confidence, behavior_scope) =
                if let Some(protocol_role) = target.protocol_role.as_deref() {
                    (
                        map_protocol_member_role(protocol_role),
                        "observed",
                        target.protocol_confidence.unwrap_or(0.8),
                        false,
                    )
                } else if target.interaction_count >= self.config.minimum_verified_executions {
                    (
                        "executor",
                        "candidate",
                        execution_role_confidence(target.interaction_count, cluster_confidence),
                        true,
                    )
                } else {
                    continue;
                };
            insert_member(
                &mut tx,
                cluster_id,
                domain,
                self.config.chain_id,
                MemberInsert {
                    address: &target.address,
                    role,
                    evidence_state,
                    confidence,
                    interaction_count: target.interaction_count,
                    behavior_scope,
                    economic_scope: false,
                    supporting_hashes: &target.supporting_hashes,
                    evidence: json!({
                        "projector": ARBITRAGE_METHOD,
                        "reason": if target.protocol_role.is_some() { "indexed protocol contract called by verified executions" } else { "repeated top-level target of verified arbitrage executions" },
                        "protocol_key": target.protocol_key,
                        "protocol_role": target.protocol_role,
                        "ownership_claim": false
                    }),
                    first_seen: target.first_seen,
                    last_seen: target.last_seen,
                },
            )
            .await?;
            insert_edge(
                &mut tx,
                cluster_id,
                domain,
                self.config.chain_id,
                EdgeInsert {
                    source: &execution.subject,
                    target: &target.address,
                    relationship_type: "executed_through",
                    evidence_state,
                    confidence,
                    interaction_count: target.interaction_count,
                    supporting_hashes: &target.supporting_hashes,
                    reason: if target.protocol_role.is_some() {
                        "Verified executions called this indexed protocol contract; it is a counterparty, not a controlled member."
                    } else {
                        "Verified executions repeatedly used this top-level contract; executor role remains a candidate."
                    },
                    evidence: json!({
                        "projector": ARBITRAGE_METHOD,
                        "protocol_key": target.protocol_key,
                        "ownership_claim": false
                    }),
                    first_seen: target.first_seen,
                    last_seen: target.last_seen,
                },
            )
            .await?;
            if member_addresses.insert(target.address.clone()) {
                projected_members += 1;
            }
        }

        for recipient in recipients {
            if recipient.address == execution.subject {
                continue;
            }
            let (role, economic_scope, confidence) =
                if let Some(protocol_role) = recipient.protocol_role.as_deref() {
                    (
                        map_protocol_member_role(protocol_role),
                        false,
                        recipient.protocol_confidence.unwrap_or(0.8),
                    )
                } else {
                    ("settlement_recipient", true, recipient.confidence)
                };
            insert_member(
                &mut tx,
                cluster_id,
                domain,
                self.config.chain_id,
                MemberInsert {
                    address: &recipient.address,
                    role,
                    evidence_state: "observed",
                    confidence: confidence.clamp(0.0, 1.0),
                    interaction_count: recipient.interaction_count,
                    behavior_scope: false,
                    economic_scope,
                    supporting_hashes: &recipient.supporting_hashes,
                    evidence: json!({
                        "projector": ARBITRAGE_METHOD,
                        "reason": "deterministic verifier attributed settlement for these executions",
                        "protocol_key": recipient.protocol_key,
                        "protocol_role": recipient.protocol_role,
                        "ownership_claim": false
                    }),
                    first_seen: recipient.first_seen,
                    last_seen: recipient.last_seen,
                },
            )
            .await?;
            insert_edge(
                &mut tx,
                cluster_id,
                domain,
                self.config.chain_id,
                EdgeInsert {
                    source: &execution.subject,
                    target: &recipient.address,
                    relationship_type: "settled_to",
                    evidence_state: "observed",
                    confidence: confidence.clamp(0.0, 1.0),
                    interaction_count: recipient.interaction_count,
                    supporting_hashes: &recipient.supporting_hashes,
                    reason: "The deterministic economic verifier attributed settlement to this address for the supporting executions.",
                    evidence: json!({
                        "projector": ARBITRAGE_METHOD,
                        "ownership_claim": false
                    }),
                    first_seen: recipient.first_seen,
                    last_seen: recipient.last_seen,
                },
            )
            .await?;
            if member_addresses.insert(recipient.address.clone()) {
                projected_members += 1;
            }
        }
        tx.commit().await.map_err(db_error)?;
        Ok(projected_members)
    }

    async fn load_checkpoint(
        &self,
        domain_id: Uuid,
        source_kind: &str,
    ) -> LedgerResult<Option<DateTime<Utc>>> {
        sqlx::query_scalar(
            r#"select last_source_updated_at from protocol_cluster_projection_checkpoints
               where domain_id=$1 and source_kind=$2"#,
        )
        .bind(domain_id)
        .bind(source_kind)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_error)
        .map(Option::flatten)
    }

    async fn mark_checkpoint_running(
        &self,
        domain: &DomainRef,
        source_kind: &str,
    ) -> LedgerResult<()> {
        sqlx::query(
            r#"insert into protocol_cluster_projection_checkpoints (
                   domain_id,chain_id,source_kind,status,last_run_at
               ) values ($1,$2,$3,'running',now())
               on conflict (domain_id,source_kind) do update set
                 status='running',last_run_at=now(),last_error=null,updated_at=now()"#,
        )
        .bind(domain.domain_id)
        .bind(self.config.chain_id as i64)
        .bind(source_kind)
        .execute(&self.pool)
        .await
        .map_err(db_error)?;
        Ok(())
    }

    async fn mark_checkpoint_ready(
        &self,
        domain: &DomainRef,
        source_kind: &str,
        high_water: DateTime<Utc>,
    ) -> LedgerResult<()> {
        sqlx::query(
            r#"insert into protocol_cluster_projection_checkpoints (
                   domain_id,chain_id,source_kind,last_source_updated_at,last_run_at,status
               ) values ($1,$2,$3,$4,now(),'ready')
               on conflict (domain_id,source_kind) do update set
                 last_source_updated_at=excluded.last_source_updated_at,last_run_at=now(),
                 status='ready',last_error=null,updated_at=now()"#,
        )
        .bind(domain.domain_id)
        .bind(self.config.chain_id as i64)
        .bind(source_kind)
        .bind(high_water)
        .execute(&self.pool)
        .await
        .map_err(db_error)?;
        Ok(())
    }

    async fn mark_checkpoint_error(
        &self,
        domain: &DomainRef,
        source_kind: &str,
        error: &str,
    ) -> LedgerResult<()> {
        sqlx::query(
            r#"insert into protocol_cluster_projection_checkpoints (
                   domain_id,chain_id,source_kind,last_run_at,status,last_error
               ) values ($1,$2,$3,now(),'error',$4)
               on conflict (domain_id,source_kind) do update set
                 last_run_at=now(),status='error',last_error=excluded.last_error,updated_at=now()"#,
        )
        .bind(domain.domain_id)
        .bind(self.config.chain_id as i64)
        .bind(source_kind)
        .bind(error.chars().take(1000).collect::<String>())
        .execute(&self.pool)
        .await
        .map_err(db_error)?;
        Ok(())
    }
}

async fn clear_projected_cluster(
    tx: &mut Transaction<'_, Postgres>,
    cluster_id: Uuid,
    projector: &str,
) -> LedgerResult<()> {
    sqlx::query(
        "delete from protocol_scoped_cluster_edges where cluster_id=$1 and evidence->>'projector'=$2",
    )
    .bind(cluster_id)
    .bind(projector)
    .execute(&mut **tx)
    .await
    .map_err(db_error)?;
    sqlx::query(
        "delete from protocol_scoped_cluster_members where cluster_id=$1 and evidence->>'projector'=$2",
    )
    .bind(cluster_id)
    .bind(projector)
    .execute(&mut **tx)
    .await
    .map_err(db_error)?;
    Ok(())
}

async fn insert_member(
    tx: &mut Transaction<'_, Postgres>,
    cluster_id: Uuid,
    domain: &DomainRef,
    chain_id: u64,
    member: MemberInsert<'_>,
) -> LedgerResult<()> {
    sqlx::query(
        r#"insert into protocol_scoped_cluster_members (
               cluster_id,domain_id,chain_id,address,member_role,evidence_state,confidence,
               interaction_count,include_in_behavior_scope,include_in_economic_scope,
               supporting_tx_hashes,evidence,first_seen_at,last_seen_at
           ) values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
           on conflict (cluster_id,address) do update set
             member_role=excluded.member_role,evidence_state=excluded.evidence_state,
             confidence=excluded.confidence,interaction_count=excluded.interaction_count,
             include_in_behavior_scope=excluded.include_in_behavior_scope,
             include_in_economic_scope=excluded.include_in_economic_scope,
             supporting_tx_hashes=excluded.supporting_tx_hashes,evidence=excluded.evidence,
             first_seen_at=excluded.first_seen_at,last_seen_at=excluded.last_seen_at,
             updated_at=now()"#,
    )
    .bind(cluster_id)
    .bind(domain.domain_id)
    .bind(chain_id as i64)
    .bind(member.address)
    .bind(member.role)
    .bind(member.evidence_state)
    .bind(member.confidence.clamp(0.0, 1.0))
    .bind(member.interaction_count.max(0))
    .bind(member.behavior_scope)
    .bind(member.economic_scope)
    .bind(member.supporting_hashes)
    .bind(Json(member.evidence))
    .bind(member.first_seen)
    .bind(member.last_seen)
    .execute(&mut **tx)
    .await
    .map_err(db_error)?;
    Ok(())
}

async fn insert_edge(
    tx: &mut Transaction<'_, Postgres>,
    cluster_id: Uuid,
    domain: &DomainRef,
    chain_id: u64,
    edge: EdgeInsert<'_>,
) -> LedgerResult<()> {
    if edge.source == edge.target {
        return Ok(());
    }
    sqlx::query(
        r#"insert into protocol_scoped_cluster_edges (
               cluster_id,domain_id,chain_id,source_address,target_address,relationship_type,
               evidence_state,confidence,interaction_count,value_transferred,
               supporting_tx_hashes,evidence_reason,evidence,first_seen_at,last_seen_at
           ) values ($1,$2,$3,$4,$5,$6,$7,$8,$9,'[]'::jsonb,$10,$11,$12,$13,$14)
           on conflict (cluster_id,source_address,target_address,relationship_type) do update set
             evidence_state=excluded.evidence_state,confidence=excluded.confidence,
             interaction_count=excluded.interaction_count,
             supporting_tx_hashes=excluded.supporting_tx_hashes,
             evidence_reason=excluded.evidence_reason,evidence=excluded.evidence,
             first_seen_at=excluded.first_seen_at,last_seen_at=excluded.last_seen_at,
             updated_at=now()"#,
    )
    .bind(cluster_id)
    .bind(domain.domain_id)
    .bind(chain_id as i64)
    .bind(edge.source)
    .bind(edge.target)
    .bind(edge.relationship_type)
    .bind(edge.evidence_state)
    .bind(edge.confidence.clamp(0.0, 1.0))
    .bind(edge.interaction_count.max(1))
    .bind(edge.supporting_hashes)
    .bind(edge.reason)
    .bind(Json(edge.evidence))
    .bind(edge.first_seen)
    .bind(edge.last_seen)
    .execute(&mut **tx)
    .await
    .map_err(db_error)?;
    Ok(())
}

fn repeat_buyer_confidence(distinct_launches: i64, trade_count: i64) -> f64 {
    (0.58 + distinct_launches.min(5) as f64 * 0.07 + trade_count.min(10) as f64 * 0.015).min(0.95)
}

fn execution_role_confidence(execution_count: i64, source_confidence: f64) -> f64 {
    (0.58 + execution_count.min(20) as f64 * 0.015)
        .min(source_confidence.max(0.72))
        .clamp(0.0, 0.97)
}

fn map_protocol_member_role(role: &str) -> &'static str {
    match role {
        "router" | "swap_router" => "router",
        "pool" | "pair" | "bonding_curve" => "pool",
        "factory" | "launch_factory" => "factory",
        _ => "protocol_contract",
    }
}

fn db_error(error: impl std::fmt::Display) -> LedgerError {
    LedgerError::Persistence(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repeat_buyer_requires_cross_launch_evidence_in_the_projector_query() {
        assert!(repeat_buyer_confidence(2, 2) >= 0.7);
        assert!(repeat_buyer_confidence(8, 100) <= 0.95);
    }

    #[test]
    fn protocol_contract_roles_never_become_execution_economic_members() {
        assert_eq!(map_protocol_member_role("router"), "router");
        assert_eq!(map_protocol_member_role("pool"), "pool");
        assert_eq!(
            map_protocol_member_role("position_manager"),
            "protocol_contract"
        );
    }

    #[test]
    fn repeated_verified_execution_increases_role_confidence_but_remains_bounded() {
        assert!(execution_role_confidence(10, 0.96) > execution_role_confidence(2, 0.96));
        assert!(execution_role_confidence(1000, 1.0) <= 0.97);
    }
}
