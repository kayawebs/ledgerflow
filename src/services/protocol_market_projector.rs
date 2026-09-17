use std::collections::BTreeSet;

use chrono::{DateTime, Timelike, Utc};
use serde::Serialize;
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::core::errors::{LedgerError, LedgerResult};

const METHOD_VERSION: &str = "pons_curve_market_v1";

#[derive(Clone, Debug)]
pub struct ProtocolMarketProjectorConfig {
    pub chain_id: u64,
    pub batch_size: usize,
    pub fact_scan_limit: i64,
    pub history_days: i64,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct ProtocolMarketProjectionSummary {
    pub domains_considered: u64,
    pub launches_selected: u64,
    pub snapshots_projected: u64,
    pub participant_outcomes_projected: u64,
}

#[derive(Clone, Copy)]
struct WindowSpec {
    key: &'static str,
    seconds: Option<i64>,
    project_outcomes: bool,
}

const WINDOWS: [WindowSpec; 8] = [
    WindowSpec {
        key: "5m",
        seconds: Some(300),
        project_outcomes: false,
    },
    WindowSpec {
        key: "15m",
        seconds: Some(900),
        project_outcomes: false,
    },
    WindowSpec {
        key: "1h",
        seconds: Some(3_600),
        project_outcomes: false,
    },
    WindowSpec {
        key: "6h",
        seconds: Some(21_600),
        project_outcomes: false,
    },
    WindowSpec {
        key: "24h",
        seconds: Some(86_400),
        project_outcomes: true,
    },
    WindowSpec {
        key: "7d",
        seconds: Some(604_800),
        project_outcomes: false,
    },
    WindowSpec {
        key: "30d",
        seconds: Some(2_592_000),
        project_outcomes: false,
    },
    WindowSpec {
        key: "lifetime",
        seconds: None,
        project_outcomes: false,
    },
];

#[derive(Clone)]
pub struct ProtocolMarketProjector {
    pool: PgPool,
    config: ProtocolMarketProjectorConfig,
}

impl ProtocolMarketProjector {
    pub fn new(pool: PgPool, config: ProtocolMarketProjectorConfig) -> Self {
        Self { pool, config }
    }

    pub async fn project_once(&self) -> LedgerResult<ProtocolMarketProjectionSummary> {
        let domains = sqlx::query_scalar::<_, Uuid>(
            r#"select domain_id from protocol_intelligence_domains
               where chain_id=$1 and status='active' and domain_kind='token_launch'
                 and protocol_key in ('pons_v1','pons_v2')
               order by domain_key"#,
        )
        .bind(self.config.chain_id as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(db_error)?;

        let mut launch_ids = BTreeSet::new();
        for domain_id in &domains {
            for launch_id in self.changed_launches(*domain_id).await? {
                if launch_ids.len() >= self.config.batch_size.max(1) {
                    break;
                }
                launch_ids.insert(launch_id);
            }
            if launch_ids.len() >= self.config.batch_size.max(1) {
                break;
            }
        }

        let launch_ids = launch_ids.into_iter().collect::<Vec<_>>();
        if launch_ids.is_empty() {
            return Ok(ProtocolMarketProjectionSummary {
                domains_considered: domains.len() as u64,
                ..ProtocolMarketProjectionSummary::default()
            });
        }

        let measured_at = minute_bucket(Utc::now());
        let mut snapshots = 0_u64;
        let mut outcomes = 0_u64;
        for window in WINDOWS {
            if window.project_outcomes {
                outcomes += self
                    .project_participant_outcomes(
                        &launch_ids,
                        window.key,
                        window.seconds,
                        measured_at,
                    )
                    .await?;
            }
            snapshots += self
                .project_snapshots(&launch_ids, window.key, window.seconds, measured_at)
                .await?;
        }

        Ok(ProtocolMarketProjectionSummary {
            domains_considered: domains.len() as u64,
            launches_selected: launch_ids.len() as u64,
            snapshots_projected: snapshots,
            participant_outcomes_projected: outcomes,
        })
    }

    async fn changed_launches(&self, domain_id: Uuid) -> LedgerResult<Vec<Uuid>> {
        let rows = sqlx::query(
            r#"with recent_unprojected as materialized (
                 select facts.launch_id,facts.updated_at
                 from protocol_launch_trade_facts facts
                 where facts.domain_id=$1 and facts.is_canonical and facts.trade_side='buy'
                   and facts.occurred_at >= now() - make_interval(days => $2::int)
                   and not exists (
                     select 1 from protocol_launch_metric_snapshots snapshots
                     where snapshots.launch_id=facts.launch_id
                       and snapshots.window_key='24h'
                       and snapshots.measured_at>=facts.updated_at
                   )
                 order by facts.updated_at desc
                 limit $3
               )
               select launch_id,max(updated_at) as source_updated_at
               from recent_unprojected
               group by launch_id
               order by source_updated_at desc
               limit $4"#,
        )
        .bind(domain_id)
        .bind(self.config.history_days.clamp(1, 365) as i32)
        .bind(self.config.fact_scan_limit.max(100))
        .bind(self.config.batch_size.max(1) as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(db_error)?;
        Ok(rows.into_iter().map(|row| row.get("launch_id")).collect())
    }

    async fn project_participant_outcomes(
        &self,
        launch_ids: &[Uuid],
        window_key: &str,
        window_seconds: Option<i64>,
        measured_at: DateTime<Utc>,
    ) -> LedgerResult<u64> {
        let result = sqlx::query(
            r#"with scoped as (
                 select facts.launch_id,launches.domain_id,launches.chain_id,
                        launches.creator_address,launches.launched_at,
                        facts.trader_address,facts.trade_side,facts.tx_hash,
                        facts.quote_amount_raw::numeric as quote_raw,
                        facts.token_amount_raw::numeric as token_raw,
                        facts.fee_amount_raw::numeric as fee_raw,
                        facts.tax_amount_raw::numeric as tax_raw,
                        coalesce(quote_meta.decimals,
                          case when launches.quote_token_address=
                            '0x0000000000000000000000000000000000000000' then 18 end
                        ) as quote_decimals
                 from protocol_launch_trade_facts facts
                 join protocol_token_launches launches on launches.launch_id=facts.launch_id
                 left join erc20_token_metadata quote_meta
                   on quote_meta.chain_id=launches.chain_id
                  and quote_meta.token_address=launches.quote_token_address
                  and quote_meta.fetch_status='ok'
                 where facts.launch_id=any($1) and facts.is_canonical
                   and facts.occurred_at is not null
                   and ($3::bigint is null or facts.occurred_at >=
                        $4::timestamptz - make_interval(secs => $3::double precision))
               ), balances as (
                 select launch_id,domain_id,chain_id,creator_address,launched_at,
                        trader_address,max(quote_decimals) as quote_decimals,
                        count(distinct tx_hash)::bigint as tx_count,
                        count(*) filter (where trade_side='buy')::bigint as buy_count,
                        count(*) filter (where trade_side='sell')::bigint as sell_count,
                        sum(token_raw) filter (where trade_side='buy') as bought_token_raw,
                        sum(token_raw) filter (where trade_side='sell') as sold_token_raw,
                        sum(quote_raw+fee_raw+tax_raw) filter (where trade_side='buy')
                          as buy_cost_raw,
                        sum(quote_raw-fee_raw-tax_raw) filter (where trade_side='sell')
                          as sell_proceeds_raw
                 from scoped
                 group by launch_id,domain_id,chain_id,creator_address,launched_at,trader_address
                 having count(*) filter (where trade_side='buy')>0
                    and count(*) filter (where trade_side='sell')>0
               ), classified as (
                 select balances.*,
                        case
                          when quote_decimals is not null
                           and launched_at is not null
                           and ($3::bigint is null or launched_at >=
                                $4::timestamptz - make_interval(secs => $3::double precision))
                           and bought_token_raw>0 and sold_token_raw>0
                           and sold_token_raw<=bought_token_raw
                          then 'estimated' else 'unknown'
                        end as result_status,
                        case
                          when quote_decimals is not null
                           and launched_at is not null
                           and ($3::bigint is null or launched_at >=
                                $4::timestamptz - make_interval(secs => $3::double precision))
                           and bought_token_raw>0 and sold_token_raw>0
                           and sold_token_raw<=bought_token_raw
                          then (sell_proceeds_raw -
                                buy_cost_raw*(sold_token_raw/bought_token_raw)) /
                               power(10::numeric,quote_decimals)
                        end as realized_result_quote,
                        case
                          when trader_address=creator_address then 'issuer'
                          when exists (
                            select 1
                            from protocol_scoped_clusters clusters
                            join protocol_scoped_cluster_members members
                              on members.cluster_id=clusters.cluster_id
                            where clusters.domain_id=balances.domain_id
                              and clusters.cluster_kind='issuer_cohort'
                              and clusters.subject_address=balances.creator_address
                              and clusters.status='active'
                              and members.address=balances.trader_address
                          ) then 'related'
                          else 'unresolved'
                        end as cohort,
                        row_number() over (
                          partition by launch_id order by tx_count desc,trader_address
                        ) as participant_rank
                 from balances
               )
               insert into protocol_launch_participant_outcomes (
                 launch_id,domain_id,chain_id,address,cohort,result_status,
                 realized_result_quote,gas_cost_native,buy_count,sell_count,
                 included_transaction_count,excluded_transaction_count,
                 exclusion_reasons,evidence,methodology_version,window_key,measured_at,created_at
               )
               select launch_id,domain_id,chain_id,trader_address,cohort,result_status,
                      realized_result_quote,null,buy_count,sell_count,tx_count,0,
                      array_remove(array[
                        'gas cost is not attributed by the launch-curve projector',
                        'external token transfers and off-curve inventory are outside scope',
                        case when result_status='estimated'
                          then 'realized result uses deterministic average on-curve cost basis'
                          else 'complete within-window inventory scope is unavailable' end
                      ]::text[],null),
                      jsonb_build_object(
                        'method', $5::text,
                        'scope', 'Pons curve trades only',
                        'profit_claim', false,
                        'quote_decimals', quote_decimals,
                        'bought_token_raw', bought_token_raw::text,
                        'sold_token_raw', sold_token_raw::text,
                        'buy_cost_raw', buy_cost_raw::text,
                        'sell_proceeds_raw', sell_proceeds_raw::text
                      ),$5,$2,$4,now()
               from classified
               where participant_rank<=500
               on conflict (launch_id,address,window_key,measured_at) do update set
                 cohort=excluded.cohort,result_status=excluded.result_status,
                 realized_result_quote=excluded.realized_result_quote,
                 gas_cost_native=null,buy_count=excluded.buy_count,sell_count=excluded.sell_count,
                 included_transaction_count=excluded.included_transaction_count,
                 excluded_transaction_count=excluded.excluded_transaction_count,
                 exclusion_reasons=excluded.exclusion_reasons,evidence=excluded.evidence,
                 methodology_version=excluded.methodology_version"#,
        )
        .bind(launch_ids)
        .bind(window_key)
        .bind(window_seconds)
        .bind(measured_at)
        .bind(METHOD_VERSION)
        .execute(&self.pool)
        .await
        .map_err(db_error)?;
        Ok(result.rows_affected())
    }

    async fn project_snapshots(
        &self,
        launch_ids: &[Uuid],
        window_key: &str,
        window_seconds: Option<i64>,
        measured_at: DateTime<Utc>,
    ) -> LedgerResult<u64> {
        let result = sqlx::query(
            r#"with selected as (
                 select launches.launch_id,launches.domain_id,launches.chain_id,
                        launches.creator_address,launches.quote_token_address,
                        token_meta.decimals as token_decimals,
                        coalesce(quote_meta.decimals,
                          case when launches.quote_token_address=
                            '0x0000000000000000000000000000000000000000' then 18 end
                        ) as quote_decimals
                 from protocol_token_launches launches
                 left join erc20_token_metadata token_meta
                   on token_meta.chain_id=launches.chain_id
                  and token_meta.token_address=launches.token_address
                  and token_meta.fetch_status='ok'
                 left join erc20_token_metadata quote_meta
                   on quote_meta.chain_id=launches.chain_id
                  and quote_meta.token_address=launches.quote_token_address
                  and quote_meta.fetch_status='ok'
                 where launches.launch_id=any($1)
               ), facts as (
                 select selected.*,trade.trader_address,trade.trade_side,
                        trade.occurred_at,trade.block_number,trade.tx_index,trade.log_index,
                        trade.quote_amount_raw::numeric /
                          power(10::numeric,selected.quote_decimals) as quote_amount,
                        trade.token_amount_raw::numeric /
                          power(10::numeric,selected.token_decimals) as token_amount
                 from selected
                 join protocol_launch_trade_facts trade on trade.launch_id=selected.launch_id
                 where trade.is_canonical and trade.occurred_at is not null
                   and selected.quote_decimals is not null and selected.token_decimals is not null
                   and ($3::bigint is null or trade.occurred_at >=
                        $4::timestamptz - make_interval(secs => $3::double precision))
               ), priced as (
                 select facts.*,
                        case when token_amount>0 then quote_amount/token_amount end as unit_price
                 from facts
               ), aggregates as (
                 select launch_id,
                        count(*)::bigint as trade_count,
                        sum(quote_amount) filter (where trade_side='buy') as buy_volume,
                        sum(quote_amount) filter (where trade_side='sell') as sell_volume,
                        count(distinct trader_address) filter (where trade_side='buy')::bigint
                          as unique_buyers,
                        count(distinct trader_address) filter (where trade_side='sell')::bigint
                          as unique_sellers,
                        min(occurred_at) as first_trade_at,max(occurred_at) as last_trade_at,
                        (array_agg(unit_price order by occurred_at,block_number,tx_index,log_index)
                          filter (where unit_price is not null))[1] as first_price,
                        (array_agg(unit_price order by occurred_at desc,block_number desc,
                          tx_index desc,log_index desc) filter (where unit_price is not null))[1]
                          as last_price
                 from priced
                 group by launch_id
               ), cohorts as (
                 select facts.launch_id,
                        count(distinct facts.trader_address) filter (
                          where facts.trade_side='buy' and (
                            facts.trader_address=facts.creator_address or exists (
                              select 1
                              from protocol_scoped_clusters clusters
                              join protocol_scoped_cluster_members members
                                on members.cluster_id=clusters.cluster_id
                              where clusters.domain_id=facts.domain_id
                                and clusters.cluster_kind='issuer_cohort'
                                and clusters.subject_address=facts.creator_address
                                and clusters.status='active'
                                and members.address=facts.trader_address
                            )
                          )
                        )::bigint as issuer_buyers
                 from facts
                 group by facts.launch_id
               ), outcomes as (
                 select outcome.launch_id,
                        count(*) filter (
                          where outcome.result_status='verified'
                            and outcome.realized_result_quote>0
                        )::bigint as verified_winners,
                        count(*) filter (
                          where outcome.result_status='verified'
                            and outcome.realized_result_quote<0
                        )::bigint as verified_losers,
                        count(*) filter (
                          where outcome.result_status='estimated'
                            and outcome.realized_result_quote>0
                        )::bigint as estimated_winners,
                        count(*) filter (
                          where outcome.result_status='estimated'
                            and outcome.realized_result_quote<0
                        )::bigint as estimated_losers,
                        count(*) filter (where outcome.result_status='verified')::bigint
                          as verified_outcomes,
                        count(*)::bigint as evaluated_outcomes
                 from protocol_launch_participant_outcomes outcome
                 where outcome.launch_id=any($1) and outcome.window_key=$2
                   and outcome.measured_at=$4
                 group by outcome.launch_id
               )
               insert into protocol_launch_metric_snapshots (
                 launch_id,domain_id,chain_id,window_key,price_quote,price_change_percent,
                 net_flow_quote,inflow_velocity_quote_per_minute,buy_volume_quote,
                 sell_volume_quote,liquidity_quote,unique_buyers,unique_sellers,
                 issuer_cohort_buyers,external_buyers,unresolved_buyers,
                 verified_winning_participants,verified_losing_participants,
                 outcome_coverage_ratio,evidence_coverage_ratio,methodology_version,
                 limitations,evidence,measured_at,created_at
               )
               select selected.launch_id,selected.domain_id,selected.chain_id,$2,
                      aggregates.last_price,
                      case when aggregates.first_price>0 and aggregates.last_price is not null
                        then (aggregates.last_price/aggregates.first_price-1)*100 end,
                      coalesce(aggregates.buy_volume,0)-coalesce(aggregates.sell_volume,0),
                      case when aggregates.trade_count>0 then
                        (coalesce(aggregates.buy_volume,0)-coalesce(aggregates.sell_volume,0)) /
                        greatest(extract(epoch from
                          ($4::timestamptz-coalesce(aggregates.first_trade_at,$4)))/60,1)
                      end,
                      aggregates.buy_volume,aggregates.sell_volume,null,
                      aggregates.unique_buyers,aggregates.unique_sellers,
                      case when aggregates.trade_count>0 then coalesce(cohorts.issuer_buyers,0) end,
                      case when aggregates.trade_count>0 then 0 end,
                      case when aggregates.trade_count>0
                        then greatest(coalesce(aggregates.unique_buyers,0)-
                             coalesce(cohorts.issuer_buyers,0),0) end,
                      outcomes.verified_winners,outcomes.verified_losers,
                      case when coalesce(outcomes.evaluated_outcomes,0)>0
                        then outcomes.verified_outcomes::double precision /
                             outcomes.evaluated_outcomes::double precision end,
                      case when aggregates.trade_count>0 then 1 else 0 end,
                      $5,
                      array_remove(array[
                        case when aggregates.trade_count is null
                          then 'no canonical curve trades in this window' end,
                        case when selected.token_decimals is null
                          then 'token decimals unavailable' end,
                        case when selected.quote_decimals is null
                          then 'quote asset decimals unavailable' end,
                        'external participants remain unresolved without protocol-scoped evidence',
                        'estimated participant results exclude gas and are not verified profit'
                      ]::text[],null),
                      jsonb_build_object(
                        'method',$5::text,
                        'data_source','protocol_launch_trade_facts',
                        'trade_count',coalesce(aggregates.trade_count,0),
                        'first_trade_at',aggregates.first_trade_at,
                        'last_trade_at',aggregates.last_trade_at,
                        'estimated_winning_participants',coalesce(outcomes.estimated_winners,0),
                        'estimated_losing_participants',coalesce(outcomes.estimated_losers,0),
                        'estimated_evaluated_participants',coalesce(outcomes.evaluated_outcomes,0),
                        'profit_claim',false
                      ),$4,now()
               from selected
               left join aggregates on aggregates.launch_id=selected.launch_id
               left join cohorts on cohorts.launch_id=selected.launch_id
               left join outcomes on outcomes.launch_id=selected.launch_id
               on conflict (launch_id,window_key,measured_at) do update set
                 price_quote=excluded.price_quote,
                 price_change_percent=excluded.price_change_percent,
                 net_flow_quote=excluded.net_flow_quote,
                 inflow_velocity_quote_per_minute=excluded.inflow_velocity_quote_per_minute,
                 buy_volume_quote=excluded.buy_volume_quote,
                 sell_volume_quote=excluded.sell_volume_quote,
                 unique_buyers=excluded.unique_buyers,unique_sellers=excluded.unique_sellers,
                 issuer_cohort_buyers=excluded.issuer_cohort_buyers,
                 external_buyers=excluded.external_buyers,
                 unresolved_buyers=excluded.unresolved_buyers,
                 verified_winning_participants=excluded.verified_winning_participants,
                 verified_losing_participants=excluded.verified_losing_participants,
                 outcome_coverage_ratio=excluded.outcome_coverage_ratio,
                 evidence_coverage_ratio=excluded.evidence_coverage_ratio,
                 methodology_version=excluded.methodology_version,
                 limitations=excluded.limitations,evidence=excluded.evidence"#,
        )
        .bind(launch_ids)
        .bind(window_key)
        .bind(window_seconds)
        .bind(measured_at)
        .bind(METHOD_VERSION)
        .execute(&self.pool)
        .await
        .map_err(db_error)?;
        Ok(result.rows_affected())
    }
}

fn minute_bucket(value: DateTime<Utc>) -> DateTime<Utc> {
    value
        .with_second(0)
        .and_then(|value| value.with_nanosecond(0))
        .unwrap_or(value)
}

fn db_error(error: impl std::fmt::Display) -> LedgerError {
    LedgerError::Persistence(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn projection_windows_cover_product_ranges() {
        assert_eq!(WINDOWS.len(), 8);
        assert!(WINDOWS.iter().any(|window| window.key == "24h"));
        assert!(WINDOWS.iter().any(|window| window.key == "7d"));
        assert!(WINDOWS.iter().any(|window| window.key == "30d"));
        assert_eq!(
            WINDOWS
                .iter()
                .filter(|window| window.project_outcomes)
                .map(|window| window.key)
                .collect::<Vec<_>>(),
            vec!["24h"]
        );
    }

    #[test]
    fn snapshots_share_a_stable_minute_bucket() {
        let value = Utc.with_ymd_and_hms(2026, 9, 16, 5, 23, 41).unwrap();
        assert_eq!(
            minute_bucket(value),
            Utc.with_ymd_and_hms(2026, 9, 16, 5, 23, 0).unwrap()
        );
    }
}
