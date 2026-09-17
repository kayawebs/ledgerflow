use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;
use serde_json::{Value, json};
use sqlx::{PgPool, Row, types::Json};

use crate::{
    core::errors::{LedgerError, LedgerResult},
    domain::{entities::AddressAnalysisCandidate, interfaces::AnalysisRepository},
    indexer::{clickhouse::ClickHouseClient, models::CanonicalAddressObservation},
    repositories::sqlx::SqlxAnalysisRepository,
};

const SOURCE_KEY: &str = "canonical_address_facts";
const SOURCE_TABLE: &str = "clickhouse.canonical_transactions_and_logs";

#[derive(Clone, Debug)]
pub struct AddressFactProducerConfig {
    pub chain_id: u32,
    pub worker_id: String,
    pub batch_size: u32,
    pub reorg_lookback_blocks: u32,
    pub maximum_enqueued_priority: i32,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct AddressFactProducerIteration {
    pub min_available_block: Option<u32>,
    pub max_available_block: Option<u32>,
    pub start_block: Option<u32>,
    pub end_block: Option<u32>,
    pub observation_rows: u64,
    pub unique_addresses: u64,
    pub queued_addresses: u64,
    pub reorg_rewind: bool,
    pub caught_up: bool,
}

#[derive(Clone, Debug)]
struct ProducerCursor {
    last_processed_block: u32,
    last_processed_block_hash: String,
}

#[derive(Clone, Debug)]
struct PlannedRange {
    start_block: u32,
    end_block: u32,
    reorg_rewind: bool,
}

#[derive(Default)]
struct CandidateAggregate {
    priority: i32,
    first_seen_block: u32,
    last_seen_block: u32,
    observation_count: u64,
    roles: BTreeSet<String>,
    samples: Vec<Value>,
}

#[derive(Clone)]
pub struct AddressFactProducerService {
    repository: SqlxAnalysisRepository,
    pool: PgPool,
    clickhouse: ClickHouseClient,
    config: AddressFactProducerConfig,
}

impl AddressFactProducerService {
    pub fn new(
        repository: SqlxAnalysisRepository,
        clickhouse: ClickHouseClient,
        config: AddressFactProducerConfig,
    ) -> Self {
        let pool = repository.pool().clone();
        Self {
            repository,
            pool,
            clickhouse,
            config,
        }
    }

    pub async fn process_once(&self) -> LedgerResult<AddressFactProducerIteration> {
        let result = self.process_once_inner().await;
        if let Err(error) = &result {
            let _ = self.mark_cursor_error(&error.to_string()).await;
        }
        result
    }

    async fn process_once_inner(&self) -> LedgerResult<AddressFactProducerIteration> {
        let Some((min_block, max_block)) = self
            .clickhouse
            .canonical_block_bounds(self.config.chain_id)
            .await?
        else {
            return Ok(AddressFactProducerIteration::default());
        };
        let cursor = self.load_cursor().await?;
        let Some(range) = self
            .plan_range(cursor.as_ref(), min_block, max_block)
            .await?
        else {
            return Ok(AddressFactProducerIteration {
                min_available_block: Some(min_block),
                max_available_block: Some(max_block),
                caught_up: true,
                ..AddressFactProducerIteration::default()
            });
        };

        self.mark_cursor_running(&range).await?;
        let observations = self
            .clickhouse
            .canonical_address_observations(
                self.config.chain_id,
                range.start_block,
                range.end_block,
            )
            .await?;
        self.persist_observation_window(&range, &observations)
            .await?;
        let candidates = build_candidates(&observations);
        let queued_candidates =
            filter_candidates_for_queue(&candidates, self.config.maximum_enqueued_priority);
        let queued_addresses = self
            .repository
            .enqueue_address_analysis_candidates(
                u64::from(self.config.chain_id),
                &queued_candidates,
            )
            .await?;
        let end_hash = self
            .clickhouse
            .get_canonical_block(self.config.chain_id, range.end_block)
            .await?
            .map(|block| block.block_hash)
            .ok_or_else(|| {
                LedgerError::TransientIngestion(format!(
                    "canonical block {} disappeared before cursor commit",
                    range.end_block
                ))
            })?;
        self.mark_cursor_success(range.end_block, &end_hash, observations.len() as u64)
            .await?;

        Ok(AddressFactProducerIteration {
            min_available_block: Some(min_block),
            max_available_block: Some(max_block),
            start_block: Some(range.start_block),
            end_block: Some(range.end_block),
            observation_rows: observations.len() as u64,
            unique_addresses: candidates.len() as u64,
            queued_addresses,
            reorg_rewind: range.reorg_rewind,
            caught_up: range.end_block >= max_block,
        })
    }

    async fn plan_range(
        &self,
        cursor: Option<&ProducerCursor>,
        min_block: u32,
        max_block: u32,
    ) -> LedgerResult<Option<PlannedRange>> {
        let Some(cursor) = cursor else {
            return Ok(Some(self.range_from(min_block, max_block, false)));
        };
        if cursor.last_processed_block < min_block {
            return Ok(Some(self.range_from(min_block, max_block, false)));
        }
        let cursor_block = self
            .clickhouse
            .get_canonical_block(self.config.chain_id, cursor.last_processed_block)
            .await?;
        let cursor_is_canonical = cursor_block.as_ref().is_some_and(|block| {
            block
                .block_hash
                .eq_ignore_ascii_case(&cursor.last_processed_block_hash)
        });
        if cursor_is_canonical {
            if cursor.last_processed_block >= max_block {
                return Ok(None);
            }
            return Ok(Some(self.range_from(
                cursor.last_processed_block.saturating_add(1).max(min_block),
                max_block,
                false,
            )));
        }

        let rewind_target = cursor
            .last_processed_block
            .saturating_sub(self.config.reorg_lookback_blocks.saturating_sub(1))
            .max(min_block);
        let persisted_window_start = sqlx::query_scalar::<_, Option<i64>>(
            r#"
            select min(window_start_block)
            from address_observation_windows
            where worker_id = $1 and chain_id = $2 and window_end_block >= $3
            "#,
        )
        .bind(&self.config.worker_id)
        .bind(self.config.chain_id as i64)
        .bind(rewind_target as i64)
        .fetch_one(&self.pool)
        .await
        .map_err(persistence_error)?
        .map(|value| value.max(0) as u32)
        .unwrap_or(rewind_target)
        .max(min_block);
        Ok(Some(self.range_from(
            persisted_window_start,
            max_block,
            true,
        )))
    }

    fn range_from(&self, start_block: u32, max_block: u32, reorg_rewind: bool) -> PlannedRange {
        PlannedRange {
            start_block,
            end_block: max_block.min(
                start_block
                    .saturating_add(self.config.batch_size.max(1))
                    .saturating_sub(1),
            ),
            reorg_rewind,
        }
    }

    async fn persist_observation_window(
        &self,
        range: &PlannedRange,
        observations: &[CanonicalAddressObservation],
    ) -> LedgerResult<()> {
        let mut tx = self.pool.begin().await.map_err(persistence_error)?;
        if range.reorg_rewind {
            sqlx::query(
                r#"
                delete from address_observation_windows
                where worker_id = $1 and chain_id = $2 and window_start_block >= $3
                "#,
            )
            .bind(&self.config.worker_id)
            .bind(self.config.chain_id as i64)
            .bind(range.start_block as i64)
            .execute(&mut *tx)
            .await
            .map_err(persistence_error)?;
        } else {
            sqlx::query(
                r#"
                delete from address_observation_windows
                where worker_id = $1 and chain_id = $2 and window_start_block = $3
                "#,
            )
            .bind(&self.config.worker_id)
            .bind(self.config.chain_id as i64)
            .bind(range.start_block as i64)
            .execute(&mut *tx)
            .await
            .map_err(persistence_error)?;
        }

        if !observations.is_empty() {
            let payload = serde_json::to_value(observations)
                .map_err(|error| LedgerError::Persistence(error.to_string()))?;
            sqlx::query(
                r#"
                insert into address_observation_windows (
                  worker_id, chain_id, window_start_block, window_end_block,
                  address, observation_role, first_seen_block, last_seen_block,
                  observation_count, sample_block_hash, sample_tx_hash, sample_log_index,
                  source_key, metadata, observed_at, created_at, updated_at
                )
                select
                  $1, $2, $3, $4,
                  lower(item.address), item.observation_role,
                  item.first_seen_block, item.last_seen_block, item.observation_count,
                  lower(item.sample_block_hash), lower(item.sample_tx_hash), item.sample_log_index,
                  $5, jsonb_build_object('method_version', 'address_fact_producer_v1'),
                  now(), now(), now()
                from jsonb_to_recordset($6::jsonb) as item(
                  address text,
                  observation_role text,
                  first_seen_block bigint,
                  last_seen_block bigint,
                  observation_count bigint,
                  sample_block_hash text,
                  sample_tx_hash text,
                  sample_log_index bigint
                )
                on conflict (
                  worker_id, chain_id, window_start_block, address, observation_role
                ) do update set
                  window_end_block = excluded.window_end_block,
                  first_seen_block = excluded.first_seen_block,
                  last_seen_block = excluded.last_seen_block,
                  observation_count = excluded.observation_count,
                  sample_block_hash = excluded.sample_block_hash,
                  sample_tx_hash = excluded.sample_tx_hash,
                  sample_log_index = excluded.sample_log_index,
                  metadata = address_observation_windows.metadata || excluded.metadata,
                  observed_at = now(),
                  updated_at = now()
                "#,
            )
            .bind(&self.config.worker_id)
            .bind(self.config.chain_id as i64)
            .bind(range.start_block as i64)
            .bind(range.end_block as i64)
            .bind(SOURCE_KEY)
            .bind(Json(payload))
            .execute(&mut *tx)
            .await
            .map_err(persistence_error)?;

            let addresses = observations
                .iter()
                .map(|observation| observation.address.to_ascii_lowercase())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            sqlx::query(
                r#"
                insert into chain_addresses (
                  chain_id, address, address_kind, first_seen_block, last_seen_block,
                  first_seen_at, last_seen_at, metadata, created_at, updated_at
                )
                select
                  $1, windows.address, 'unknown', min(windows.first_seen_block),
                  max(windows.last_seen_block), now(), now(),
                  jsonb_build_object(
                    'observation_source', $3::text,
                    'observation_method_version', 'address_fact_producer_v1'
                  ),
                  now(), now()
                from address_observation_windows windows
                where windows.chain_id = $1 and windows.address = any($2)
                group by windows.address
                on conflict (chain_id, address) do update set
                  first_seen_block = case
                    when chain_addresses.first_seen_block is null then excluded.first_seen_block
                    else least(chain_addresses.first_seen_block, excluded.first_seen_block)
                  end,
                  last_seen_block = case
                    when chain_addresses.last_seen_block is null then excluded.last_seen_block
                    else greatest(chain_addresses.last_seen_block, excluded.last_seen_block)
                  end,
                  last_seen_at = now(),
                  metadata = chain_addresses.metadata || excluded.metadata,
                  updated_at = now()
                "#,
            )
            .bind(self.config.chain_id as i64)
            .bind(&addresses)
            .bind(SOURCE_KEY)
            .execute(&mut *tx)
            .await
            .map_err(persistence_error)?;
        }
        tx.commit().await.map_err(persistence_error)
    }

    async fn load_cursor(&self) -> LedgerResult<Option<ProducerCursor>> {
        let row = sqlx::query(
            r#"
            select last_processed_block, last_processed_block_hash
            from onchain_event_cursors
            where worker_id = $1 and chain_id = $2
            "#,
        )
        .bind(&self.config.worker_id)
        .bind(self.config.chain_id as i64)
        .fetch_optional(&self.pool)
        .await
        .map_err(persistence_error)?;
        Ok(row.and_then(|row| {
            Some(ProducerCursor {
                last_processed_block: row.get::<Option<i64>, _>("last_processed_block")?.max(0)
                    as u32,
                last_processed_block_hash: row
                    .get::<Option<String>, _>("last_processed_block_hash")
                    .unwrap_or_default(),
            })
        }))
    }

    async fn mark_cursor_running(&self, range: &PlannedRange) -> LedgerResult<()> {
        sqlx::query(
            r#"
            insert into onchain_event_cursors (
              worker_id, chain_id, source_table, scan_from_block, scan_to_block,
              lookback_blocks, status, metadata, created_at, updated_at
            )
            values ($1,$2,$3,$4,$5,$6,'running',$7,now(),now())
            on conflict (worker_id) do update set
              chain_id = excluded.chain_id,
              source_table = excluded.source_table,
              scan_from_block = excluded.scan_from_block,
              scan_to_block = excluded.scan_to_block,
              lookback_blocks = excluded.lookback_blocks,
              status = 'running',
              last_error = null,
              metadata = onchain_event_cursors.metadata || excluded.metadata,
              updated_at = now()
            "#,
        )
        .bind(&self.config.worker_id)
        .bind(self.config.chain_id as i64)
        .bind(SOURCE_TABLE)
        .bind(range.start_block as i64)
        .bind(range.end_block as i64)
        .bind(self.config.reorg_lookback_blocks as i64)
        .bind(Json(json!({
            "method_version": "address_fact_producer_v1",
            "reorg_rewind": range.reorg_rewind,
        })))
        .execute(&self.pool)
        .await
        .map_err(persistence_error)?;
        Ok(())
    }

    async fn mark_cursor_success(
        &self,
        end_block: u32,
        end_hash: &str,
        observation_rows: u64,
    ) -> LedgerResult<()> {
        sqlx::query(
            r#"
            update onchain_event_cursors
            set last_processed_block = $2,
                last_processed_block_hash = $3,
                status = 'idle',
                retry_count = 0,
                last_error = null,
                last_event_count = $4,
                last_success_at = now(),
                updated_at = now()
            where worker_id = $1
            "#,
        )
        .bind(&self.config.worker_id)
        .bind(end_block as i64)
        .bind(end_hash)
        .bind(observation_rows as i64)
        .execute(&self.pool)
        .await
        .map_err(persistence_error)?;
        Ok(())
    }

    async fn mark_cursor_error(&self, error: &str) -> LedgerResult<()> {
        sqlx::query(
            r#"
            update onchain_event_cursors
            set status = 'error', retry_count = retry_count + 1,
                last_error = $2, updated_at = now()
            where worker_id = $1
            "#,
        )
        .bind(&self.config.worker_id)
        .bind(truncate_text(error, 8_000))
        .execute(&self.pool)
        .await
        .map_err(persistence_error)?;
        Ok(())
    }
}

fn build_candidates(observations: &[CanonicalAddressObservation]) -> Vec<AddressAnalysisCandidate> {
    let mut by_address = BTreeMap::<String, CandidateAggregate>::new();
    for observation in observations {
        let entry = by_address
            .entry(observation.address.to_ascii_lowercase())
            .or_insert_with(|| CandidateAggregate {
                priority: priority_for_role(&observation.observation_role),
                first_seen_block: observation.first_seen_block,
                last_seen_block: observation.last_seen_block,
                ..CandidateAggregate::default()
            });
        entry.priority = entry
            .priority
            .min(priority_for_role(&observation.observation_role));
        entry.first_seen_block = entry.first_seen_block.min(observation.first_seen_block);
        entry.last_seen_block = entry.last_seen_block.max(observation.last_seen_block);
        entry.observation_count = entry
            .observation_count
            .saturating_add(observation.observation_count);
        entry.roles.insert(observation.observation_role.clone());
        entry.samples.push(json!({
            "role": observation.observation_role,
            "block_number": observation.first_seen_block,
            "block_hash": observation.sample_block_hash,
            "tx_hash": observation.sample_tx_hash,
            "log_index": observation.sample_log_index,
        }));
    }
    by_address
        .into_iter()
        .map(|(address, aggregate)| AddressAnalysisCandidate {
            address,
            priority: aggregate.priority,
            metadata: json!({
                "source": SOURCE_KEY,
                "roles": aggregate.roles,
                "first_seen_block": aggregate.first_seen_block,
                "last_seen_block": aggregate.last_seen_block,
                "observation_count": aggregate.observation_count,
                "evidence_samples": aggregate.samples,
                "method_version": "address_fact_producer_v1",
            }),
        })
        .collect()
}

fn priority_for_role(role: &str) -> i32 {
    match role {
        "contract_created" => 15,
        "log_emitter" => 50,
        "indexed_event_address" => 70,
        "transaction_sender" => 80,
        "transaction_recipient" => 90,
        _ => 100,
    }
}

fn filter_candidates_for_queue(
    candidates: &[AddressAnalysisCandidate],
    maximum_priority: i32,
) -> Vec<AddressAnalysisCandidate> {
    candidates
        .iter()
        .filter(|candidate| candidate.priority <= maximum_priority)
        .cloned()
        .collect()
}

fn persistence_error(error: sqlx::Error) -> LedgerError {
    LedgerError::Persistence(error.to_string())
}

fn truncate_text(value: &str, max_chars: usize) -> String {
    value.chars().take(max_chars).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn observation(address: &str, role: &str, count: u64) -> CanonicalAddressObservation {
        CanonicalAddressObservation {
            chain_id: 4663,
            address: address.to_owned(),
            observation_role: role.to_owned(),
            first_seen_block: 100,
            last_seen_block: 120,
            observation_count: count,
            sample_block_hash: "0x01".to_owned(),
            sample_tx_hash: "0x02".to_owned(),
            sample_log_index: None,
        }
    }

    #[test]
    fn candidates_merge_roles_and_keep_highest_priority() {
        let address = "0x1111111111111111111111111111111111111111";
        let candidates = build_candidates(&[
            observation(address, "transaction_recipient", 2),
            observation(address, "contract_created", 1),
        ]);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].priority, 15);
        assert_eq!(candidates[0].metadata["observation_count"], 3);
        assert_eq!(
            candidates[0].metadata["roles"],
            json!(["contract_created", "transaction_recipient"])
        );
    }

    #[test]
    fn generic_addresses_remain_indexed_but_are_not_queued_for_heavy_analysis() {
        let candidates = build_candidates(&[
            observation(
                "0x1111111111111111111111111111111111111111",
                "contract_created",
                1,
            ),
            observation(
                "0x2222222222222222222222222222222222222222",
                "transaction_sender",
                10,
            ),
            observation(
                "0x3333333333333333333333333333333333333333",
                "transaction_recipient",
                5,
            ),
        ]);
        let queued = filter_candidates_for_queue(&candidates, 20);
        assert_eq!(queued.len(), 1);
        assert_eq!(queued[0].priority, 15);
        assert_eq!(candidates.len(), 3, "fact collection remains complete");
    }
}
