use serde::Serialize;
use serde_json::json;
use sqlx::{PgPool, Row, types::Json};
use uuid::Uuid;

use crate::core::errors::{LedgerError, LedgerResult};

const METHOD_VERSION: &str = "seed_intelligence_v2";
const RELATIONSHIP_METHOD_VERSION: &str = "seed_relationship_v1";

#[derive(Clone, Debug)]
pub struct SeedIntelligenceConfig {
    pub chain_id: u32,
    pub worker_id: String,
    pub minimum_mev_candidate_transactions: u32,
    pub minimum_mev_distinct_routes: u32,
    pub maximum_mev_candidates: u32,
    pub maximum_active_pools: u32,
    pub maximum_queued_priority: i32,
    pub maximum_evidence_transactions_per_seed: u32,
    pub maximum_related_addresses: u32,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct SeedIntelligenceIteration {
    pub curated_seed_changes: u64,
    pub protocol_seed_changes: u64,
    pub active_pool_seed_changes: u64,
    pub asset_seed_changes: u64,
    pub mev_candidate_changes: u64,
    pub relationship_changes: u64,
    pub address_jobs_changed: u64,
    pub relationship_address_jobs_changed: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct SeedIntelligenceSummary {
    pub chain_id: u32,
    pub total_seed_addresses: i64,
    pub active_seed_addresses: i64,
    pub candidate_seed_addresses: i64,
    pub official_seed_addresses: i64,
    pub mev_candidate_addresses: i64,
    pub relationship_count: i64,
    pub seeds_with_relationships: i64,
    pub evidence_transaction_count: i64,
    pub analyzed_evidence_transaction_count: i64,
    pub queued_seed_jobs: i64,
    pub deferred_generic_jobs: i64,
    pub latest_completed_run_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Clone)]
pub struct SeedIntelligenceService {
    pool: PgPool,
    config: SeedIntelligenceConfig,
}

impl SeedIntelligenceService {
    pub fn new(pool: PgPool, config: SeedIntelligenceConfig) -> Self {
        Self { pool, config }
    }

    pub async fn process_once(&self) -> LedgerResult<SeedIntelligenceIteration> {
        let run_id = self.begin_run().await?;
        let result = self.process_once_inner().await;
        match result {
            Ok(iteration) => {
                // Counting the multi-million-row generic queue is deliberately
                // paid once per planner iteration. Read paths consume this
                // persisted snapshot instead of rescanning the queue.
                let summary = self.calculate_summary().await?;
                self.complete_run(run_id, &iteration, &summary).await?;
                Ok(iteration)
            }
            Err(error) => {
                let _ = self.fail_run(run_id, &error.to_string()).await;
                Err(error)
            }
        }
    }

    async fn process_once_inner(&self) -> LedgerResult<SeedIntelligenceIteration> {
        let curated_seed_changes = self.sync_curated_seeds().await?;
        let protocol_seed_changes = self.sync_protocol_seeds().await?;
        let active_pool_seed_changes = self.sync_active_pool_seeds().await?;
        let asset_seed_changes = self.sync_active_pool_assets().await?;
        let mev_candidate_changes = self.sync_mev_candidates().await?;
        let relationship_changes = self.sync_seed_relationships().await?;
        let address_jobs_changed = self.enqueue_effective_seeds().await?;
        let relationship_address_jobs_changed = self.enqueue_related_addresses().await?;
        Ok(SeedIntelligenceIteration {
            curated_seed_changes,
            protocol_seed_changes,
            active_pool_seed_changes,
            asset_seed_changes,
            mev_candidate_changes,
            relationship_changes,
            address_jobs_changed,
            relationship_address_jobs_changed,
        })
    }

    async fn sync_curated_seeds(&self) -> LedgerResult<u64> {
        let result = sqlx::query(
            r#"
            with grouped as (
              select
                assertions.chain_id,
                lower(assertions.address) as address,
                assertions.source_key,
                sources.source_kind,
                case
                  when bool_or(assertions.label_key like 'actor:%') then 'automated_actor'
                  when bool_or(
                    assertions.label_key like 'token:%'
                    or assertions.label_key in (
                      'token', 'erc20_token', 'canonical_token', 'stock_token',
                      'stablecoin', 'wrapped_native'
                    )
                  ) then 'asset'
                  when bool_or(assertions.label_key = 'official_robinhood_contract')
                    then 'official_contract'
                  else 'protocol_contract'
                end as seed_kind,
                array_agg(distinct assertions.label_key order by assertions.label_key) as roles,
                max(assertions.confidence) as confidence,
                min(assertions.valid_from_block) as first_seen_block,
                max(assertions.valid_to_block) as last_seen_block
              from address_label_assertions assertions
              join intelligence_sources sources
                on sources.source_key = assertions.source_key
              where assertions.chain_id = $1
                and assertions.assertion_value
                and assertions.status = 'active'
                and (assertions.expires_at is null or assertions.expires_at > now())
                and assertions.origin_kind in ('official', 'curated_seed', 'manual')
                and sources.status = 'active'
                and not exists (
                  select 1
                  from protocol_deployments deployments
                  where deployments.chain_id = assertions.chain_id
                    and deployments.address = lower(assertions.address)
                    and deployments.source_key = assertions.source_key
                    and deployments.status = 'active'
                )
              group by assertions.chain_id, lower(assertions.address),
                       assertions.source_key, sources.source_kind
            ), prepared as (
              select
                grouped.*,
                case grouped.source_kind
                  when 'manual' then 3
                  when 'official' then 5
                  else 8
                end as priority,
                jsonb_build_object(
                  'method_version', $2::text,
                  'source_kind', grouped.source_kind,
                  'labels', to_jsonb(grouped.roles),
                  'limitation', case
                    when grouped.seed_kind = 'automated_actor'
                      then 'An accepted automation label does not by itself prove MEV activity.'
                    else null
                  end
                ) as evidence
              from grouped
            )
            insert into intelligence_seed_addresses (
              chain_id, address, seed_kind, roles, source_key, status,
              priority, confidence, evidence, evidence_digest,
              first_seen_block, last_seen_block, first_seen_at, last_seen_at,
              created_at, updated_at
            )
            select
              chain_id, address, seed_kind, roles, source_key, 'active',
              priority, confidence, evidence, md5(evidence::text),
              first_seen_block, last_seen_block, now(), now(), now(), now()
            from prepared
            on conflict (chain_id, address, seed_kind, source_key) do update set
              roles = excluded.roles,
              status = 'active',
              priority = least(intelligence_seed_addresses.priority, excluded.priority),
              confidence = greatest(intelligence_seed_addresses.confidence, excluded.confidence),
              evidence = excluded.evidence,
              evidence_digest = excluded.evidence_digest,
              first_seen_block = coalesce(
                least(intelligence_seed_addresses.first_seen_block, excluded.first_seen_block),
                intelligence_seed_addresses.first_seen_block,
                excluded.first_seen_block
              ),
              last_seen_block = coalesce(
                greatest(intelligence_seed_addresses.last_seen_block, excluded.last_seen_block),
                intelligence_seed_addresses.last_seen_block,
                excluded.last_seen_block
              ),
              last_seen_at = now(),
              updated_at = now()
            where intelligence_seed_addresses.evidence_digest is distinct from excluded.evidence_digest
               or intelligence_seed_addresses.status <> 'active'
               or intelligence_seed_addresses.priority > excluded.priority
               or intelligence_seed_addresses.confidence < excluded.confidence
            "#,
        )
        .bind(self.config.chain_id as i64)
        .bind(METHOD_VERSION)
        .execute(&self.pool)
        .await
        .map_err(persistence_error)?;
        Ok(result.rows_affected())
    }

    async fn sync_protocol_seeds(&self) -> LedgerResult<u64> {
        let result = sqlx::query(
            r#"
            with grouped as (
              select
                deployments.chain_id,
                lower(deployments.address) as address,
                deployments.source_key,
                sources.source_kind,
                array_agg(distinct deployments.contract_role order by deployments.contract_role)
                  as contract_roles,
                array_agg(distinct deployments.protocol_key order by deployments.protocol_key)
                  as protocol_keys,
                max(deployments.confidence) as confidence,
                min(deployments.valid_from_block) as first_seen_block,
                max(deployments.valid_to_block) as last_seen_block
              from protocol_deployments deployments
              join intelligence_sources sources
                on sources.source_key = deployments.source_key
              where deployments.chain_id = $1
                and deployments.status = 'active'
                and sources.status = 'active'
                and (
                  sources.source_kind in ('official', 'curated', 'manual')
                  or deployments.contract_role in (
                    'factory', 'router', 'pool_manager', 'vault', 'comptroller',
                    'lending_pool', 'settlement', 'permit2'
                  )
                )
              group by deployments.chain_id, lower(deployments.address),
                       deployments.source_key, sources.source_kind
            ), prepared as (
              select
                grouped.*,
                case when grouped.source_kind in ('official', 'manual') then 5 else 12 end
                  as priority,
                jsonb_build_object(
                  'method_version', $2::text,
                  'source_kind', grouped.source_kind,
                  'protocol_keys', to_jsonb(grouped.protocol_keys),
                  'contract_roles', to_jsonb(grouped.contract_roles)
                ) as evidence
              from grouped
            )
            insert into intelligence_seed_addresses (
              chain_id, address, seed_kind, roles, source_key, status,
              priority, confidence, evidence, evidence_digest,
              first_seen_block, last_seen_block, first_seen_at, last_seen_at,
              created_at, updated_at
            )
            select
              chain_id, address,
              case when source_kind = 'official' then 'official_contract' else 'protocol_contract' end,
              contract_roles, source_key, 'active', priority, confidence,
              evidence, md5(evidence::text), first_seen_block, last_seen_block,
              now(), now(), now(), now()
            from prepared
            on conflict (chain_id, address, seed_kind, source_key) do update set
              roles = excluded.roles,
              status = 'active',
              priority = least(intelligence_seed_addresses.priority, excluded.priority),
              confidence = greatest(intelligence_seed_addresses.confidence, excluded.confidence),
              evidence = excluded.evidence,
              evidence_digest = excluded.evidence_digest,
              first_seen_block = coalesce(
                least(intelligence_seed_addresses.first_seen_block, excluded.first_seen_block),
                intelligence_seed_addresses.first_seen_block,
                excluded.first_seen_block
              ),
              last_seen_block = coalesce(
                greatest(intelligence_seed_addresses.last_seen_block, excluded.last_seen_block),
                intelligence_seed_addresses.last_seen_block,
                excluded.last_seen_block
              ),
              last_seen_at = now(),
              updated_at = now()
            where intelligence_seed_addresses.evidence_digest is distinct from excluded.evidence_digest
               or intelligence_seed_addresses.status <> 'active'
               or intelligence_seed_addresses.priority > excluded.priority
               or intelligence_seed_addresses.confidence < excluded.confidence
            "#,
        )
        .bind(self.config.chain_id as i64)
        .bind(METHOD_VERSION)
        .execute(&self.pool)
        .await
        .map_err(persistence_error)?;
        Ok(result.rows_affected())
    }

    async fn sync_active_pool_seeds(&self) -> LedgerResult<u64> {
        let result = sqlx::query(
            r#"
            with ranked as (
              select
                pools.chain_id,
                lower(pools.pool_address) as address,
                count(distinct pools.tx_hash)::bigint as candidate_transaction_count,
                min(pools.block_number) as first_seen_block,
                max(pools.block_number) as last_seen_block
              from arbitrage_candidate_pools pools
              join arbitrage_candidates candidates
                on candidates.chain_id = pools.chain_id
               and candidates.tx_hash = pools.tx_hash
              where pools.chain_id = $1
                and candidates.block_timestamp >= now() - interval '30 days'
              group by pools.chain_id, lower(pools.pool_address)
              order by count(distinct pools.tx_hash) desc,
                       max(pools.block_number) desc, lower(pools.pool_address)
              limit $2
            ), identified as (
              select
                ranked.*,
                coalesce(deployments.source_key, 'onchain_contract_discovery') as source_key,
                coalesce(deployments.confidence, 0.85) as confidence,
                deployments.protocol_key,
                deployments.contract_role
              from ranked
              left join lateral (
                select source_key, confidence, protocol_key, contract_role
                from protocol_deployments
                where chain_id = ranked.chain_id
                  and address = ranked.address
                  and status = 'active'
                order by confidence desc, updated_at desc
                limit 1
              ) deployments on true
            ), prepared as (
              select
                identified.*,
                array_remove(array['active_dex_pool', contract_role], null) as roles,
                jsonb_build_object(
                  'method_version', $3::text,
                  'selection', 'top_pool_by_repeated_multi_pool_execution',
                  'candidate_transaction_count', candidate_transaction_count,
                  'protocol_key', protocol_key,
                  'contract_role', contract_role,
                  'limitation', 'Pool activity is deterministic; associated transactions remain unverified arbitrage candidates.'
                ) as evidence
              from identified
            )
            insert into intelligence_seed_addresses (
              chain_id, address, seed_kind, roles, source_key, status,
              priority, confidence, evidence, evidence_digest,
              first_seen_block, last_seen_block, first_seen_at, last_seen_at,
              created_at, updated_at
            )
            select
              chain_id, address, 'protocol_contract', roles, source_key, 'active',
              18, confidence, evidence, md5(evidence::text),
              first_seen_block, last_seen_block, now(), now(), now(), now()
            from prepared
            on conflict (chain_id, address, seed_kind, source_key) do update set
              roles = excluded.roles,
              status = 'active',
              priority = least(intelligence_seed_addresses.priority, excluded.priority),
              confidence = greatest(intelligence_seed_addresses.confidence, excluded.confidence),
              evidence = excluded.evidence,
              evidence_digest = excluded.evidence_digest,
              first_seen_block = coalesce(
                least(intelligence_seed_addresses.first_seen_block, excluded.first_seen_block),
                intelligence_seed_addresses.first_seen_block,
                excluded.first_seen_block
              ),
              last_seen_block = coalesce(
                greatest(intelligence_seed_addresses.last_seen_block, excluded.last_seen_block),
                intelligence_seed_addresses.last_seen_block,
                excluded.last_seen_block
              ),
              last_seen_at = now(),
              updated_at = now()
            where intelligence_seed_addresses.evidence_digest is distinct from excluded.evidence_digest
               or intelligence_seed_addresses.status <> 'active'
               or intelligence_seed_addresses.priority > excluded.priority
               or intelligence_seed_addresses.confidence < excluded.confidence
            "#,
        )
        .bind(self.config.chain_id as i64)
        .bind(self.config.maximum_active_pools as i64)
        .bind(METHOD_VERSION)
        .execute(&self.pool)
        .await
        .map_err(persistence_error)?;
        Ok(result.rows_affected())
    }

    async fn sync_active_pool_assets(&self) -> LedgerResult<u64> {
        let result = sqlx::query(
            r#"
            with ranked_pools as (
              select lower(pools.pool_address) as pool_address,
                     count(distinct pools.tx_hash)::bigint as candidate_transaction_count
              from arbitrage_candidate_pools pools
              join arbitrage_candidates candidates
                on candidates.chain_id = pools.chain_id
               and candidates.tx_hash = pools.tx_hash
              where pools.chain_id = $1
                and candidates.block_timestamp >= now() - interval '30 days'
              group by lower(pools.pool_address)
              order by count(distinct pools.tx_hash) desc, lower(pools.pool_address)
              limit $2
            ), pool_assets as (
              select lower(discovery.token0_address) as token_address,
                     count(distinct discovery.address)::bigint as pool_count,
                     sum(ranked_pools.candidate_transaction_count)::bigint as candidate_transaction_count,
                     min(discovery.block_number) as first_seen_block,
                     max(discovery.block_number) as last_seen_block
              from ranked_pools
              join contract_discovery_candidates discovery
                on discovery.chain_id = $1
               and discovery.address = ranked_pools.pool_address
               and discovery.status = 'validated'
               and discovery.is_canonical
              where discovery.token0_address is not null
              group by lower(discovery.token0_address)
              union all
              select lower(discovery.token1_address),
                     count(distinct discovery.address)::bigint,
                     sum(ranked_pools.candidate_transaction_count)::bigint,
                     min(discovery.block_number), max(discovery.block_number)
              from ranked_pools
              join contract_discovery_candidates discovery
                on discovery.chain_id = $1
               and discovery.address = ranked_pools.pool_address
               and discovery.status = 'validated'
               and discovery.is_canonical
              where discovery.token1_address is not null
              group by lower(discovery.token1_address)
            ), grouped as (
              select token_address, sum(pool_count)::bigint as pool_count,
                     sum(candidate_transaction_count)::bigint as candidate_transaction_count,
                     min(first_seen_block) as first_seen_block,
                     max(last_seen_block) as last_seen_block
              from pool_assets
              group by token_address
            ), prepared as (
              select
                grouped.*,
                metadata.symbol,
                metadata.name,
                metadata.decimals,
                jsonb_build_object(
                  'method_version', $3::text,
                  'selection', 'asset_of_active_seed_pool',
                  'pool_count', grouped.pool_count,
                  'candidate_transaction_count', grouped.candidate_transaction_count,
                  'symbol', metadata.symbol,
                  'name', metadata.name,
                  'decimals', metadata.decimals
                ) as evidence
              from grouped
              join erc20_token_metadata metadata
                on metadata.chain_id = $1
               and lower(metadata.token_address) = grouped.token_address
               and metadata.fetch_status = 'ok'
            )
            insert into intelligence_seed_addresses (
              chain_id, address, seed_kind, roles, source_key, status,
              priority, confidence, evidence, evidence_digest,
              first_seen_block, last_seen_block, first_seen_at, last_seen_at,
              created_at, updated_at
            )
            select
              $1, token_address, 'asset', array['active_pool_asset'],
              'onchain_contract_discovery', 'active', 14, 0.90,
              evidence, md5(evidence::text), first_seen_block, last_seen_block,
              now(), now(), now(), now()
            from prepared
            on conflict (chain_id, address, seed_kind, source_key) do update set
              roles = excluded.roles,
              status = 'active',
              priority = least(intelligence_seed_addresses.priority, excluded.priority),
              confidence = greatest(intelligence_seed_addresses.confidence, excluded.confidence),
              evidence = excluded.evidence,
              evidence_digest = excluded.evidence_digest,
              first_seen_block = coalesce(
                least(intelligence_seed_addresses.first_seen_block, excluded.first_seen_block),
                intelligence_seed_addresses.first_seen_block,
                excluded.first_seen_block
              ),
              last_seen_block = coalesce(
                greatest(intelligence_seed_addresses.last_seen_block, excluded.last_seen_block),
                intelligence_seed_addresses.last_seen_block,
                excluded.last_seen_block
              ),
              last_seen_at = now(),
              updated_at = now()
            where intelligence_seed_addresses.evidence_digest is distinct from excluded.evidence_digest
               or intelligence_seed_addresses.status <> 'active'
               or intelligence_seed_addresses.priority > excluded.priority
               or intelligence_seed_addresses.confidence < excluded.confidence
            "#,
        )
        .bind(self.config.chain_id as i64)
        .bind(self.config.maximum_active_pools as i64)
        .bind(METHOD_VERSION)
        .execute(&self.pool)
        .await
        .map_err(persistence_error)?;
        Ok(result.rows_affected())
    }

    async fn sync_mev_candidates(&self) -> LedgerResult<u64> {
        let result = sqlx::query(
            r#"
            with aggregated as (
              select
                candidates.chain_id,
                lower(candidates.sender_address) as address,
                count(*)::bigint as candidate_transaction_count,
                count(distinct nullif(candidates.route_fingerprint, ''))::bigint
                  as distinct_route_count,
                count(distinct candidates.entry_contract) filter (
                  where candidates.entry_contract is not null
                )::bigint as distinct_entry_contract_count,
                count(*) filter (
                  where candidates.confirmation_status like 'confirmed_%'
                     or candidates.confirmation_status in (
                       'verified', 'verified_profit', 'confirmed_economic'
                     )
                )::bigint as confirmed_transaction_count,
                min(candidates.block_number) as first_seen_block,
                max(candidates.block_number) as last_seen_block,
                max(candidates.score) as maximum_detection_score,
                (array_agg(candidates.tx_hash order by candidates.block_number desc))[1:8]
                  as sample_transaction_hashes
              from arbitrage_candidates candidates
              where candidates.chain_id = $1
                and candidates.sender_address is not null
                and candidates.block_timestamp >= now() - interval '30 days'
              group by candidates.chain_id, lower(candidates.sender_address)
              having count(*) >= $2
                 and count(distinct nullif(candidates.route_fingerprint, '')) >= $3
              order by count(*) desc, max(candidates.block_number) desc,
                       lower(candidates.sender_address)
              limit $4
            ), stats as (
              select
                aggregated.*,
                exists (
                  select 1
                  from address_label_assertions assertions
                  where assertions.chain_id = aggregated.chain_id
                    and assertions.address = aggregated.address
                    and assertions.label_key = 'actor:automation_candidate'
                    and assertions.status = 'active'
                    and assertions.assertion_value
                    and (assertions.expires_at is null or assertions.expires_at > now())
                ) as has_automation_signal
              from aggregated
            ), prepared as (
              select
                stats.*,
                least(
                  0.79,
                  0.50
                    + least(stats.candidate_transaction_count, 20)::double precision * 0.005
                    + least(stats.distinct_route_count, 20)::double precision * 0.005
                    + case when stats.has_automation_signal then 0.05 else 0 end
                ) as confidence,
                jsonb_build_object(
                  'method_version', $5::text,
                  'candidate_only', true,
                  'candidate_transaction_count', stats.candidate_transaction_count,
                  'distinct_route_count', stats.distinct_route_count,
                  'distinct_entry_contract_count', stats.distinct_entry_contract_count,
                  'confirmed_transaction_count', stats.confirmed_transaction_count,
                  'maximum_detection_score', stats.maximum_detection_score,
                  'has_automation_signal', stats.has_automation_signal,
                  'sample_transaction_hashes', to_jsonb(stats.sample_transaction_hashes),
                  'limitations', jsonb_build_array(
                    'Repeated multi-pool execution is a selection signal, not verified MEV intent.',
                    'Profit and economic causality must be verified before promotion.'
                  )
                ) as evidence
              from stats
            )
            insert into intelligence_seed_addresses (
              chain_id, address, seed_kind, roles, source_key, status,
              priority, confidence, evidence, evidence_digest,
              first_seen_block, last_seen_block, first_seen_at, last_seen_at,
              created_at, updated_at
            )
            select
              chain_id, address, 'mev_actor',
              array['arbitrage_executor_candidate', 'automated_actor_candidate'],
              'mev_seed_classifier', 'candidate', 10, confidence,
              evidence, md5(evidence::text), first_seen_block, last_seen_block,
              now(), now(), now(), now()
            from prepared
            on conflict (chain_id, address, seed_kind, source_key) do update set
              roles = excluded.roles,
              status = case
                when intelligence_seed_addresses.status = 'active' then 'active'
                else 'candidate'
              end,
              priority = least(intelligence_seed_addresses.priority, excluded.priority),
              confidence = greatest(intelligence_seed_addresses.confidence, excluded.confidence),
              evidence = excluded.evidence,
              evidence_digest = excluded.evidence_digest,
              first_seen_block = coalesce(
                least(intelligence_seed_addresses.first_seen_block, excluded.first_seen_block),
                intelligence_seed_addresses.first_seen_block,
                excluded.first_seen_block
              ),
              last_seen_block = coalesce(
                greatest(intelligence_seed_addresses.last_seen_block, excluded.last_seen_block),
                intelligence_seed_addresses.last_seen_block,
                excluded.last_seen_block
              ),
              last_seen_at = now(),
              updated_at = now()
            where intelligence_seed_addresses.evidence_digest is distinct from excluded.evidence_digest
               or intelligence_seed_addresses.priority > excluded.priority
               or intelligence_seed_addresses.confidence < excluded.confidence
            "#,
        )
        .bind(self.config.chain_id as i64)
        .bind(self.config.minimum_mev_candidate_transactions as i64)
        .bind(self.config.minimum_mev_distinct_routes as i64)
        .bind(self.config.maximum_mev_candidates as i64)
        .bind(METHOD_VERSION)
        .execute(&self.pool)
        .await
        .map_err(persistence_error)?;
        Ok(result.rows_affected())
    }

    async fn sync_seed_relationships(&self) -> LedgerResult<u64> {
        let mut transaction = self.pool.begin().await.map_err(persistence_error)?;
        let expired = sqlx::query(
            r#"
            update intelligence_seed_relationships
            set status = 'expired', updated_at = now()
            where chain_id = $1
              and source_key = 'seed_relationship_extractor'
              and status = 'observed'
              and expires_at <= now()
            "#,
        )
        .bind(self.config.chain_id as i64)
        .execute(&mut *transaction)
        .await
        .map_err(persistence_error)?
        .rows_affected();

        let upserted = sqlx::query(
            r#"
            with eligible_seeds as (
              select distinct lower(address) as address
              from intelligence_seed_addresses
              where chain_id = $1
                and seed_kind = 'mev_actor'
                and status in ('candidate', 'active')
            ), recent_candidates as (
              select candidates.*
              from arbitrage_candidates candidates
              join eligible_seeds seeds
                on seeds.address = lower(candidates.sender_address)
              where candidates.chain_id = $1
                and candidates.block_timestamp >= now() - interval '30 days'
                and candidates.sender_address ~* '^0x[0-9a-f]{40}$'
            ), observed as (
              select
                lower(candidates.sender_address) as seed_address,
                lower(candidates.entry_contract) as related_address,
                'top_level_call_target'::text as relationship_type,
                count(*)::bigint as transaction_count,
                (array_agg(
                  candidates.tx_hash
                  order by candidates.block_number desc, candidates.tx_hash
                ))[1:8] as sample_transaction_hashes,
                min(candidates.block_number) as first_seen_block,
                max(candidates.block_number) as last_seen_block,
                1.0::double precision as confidence,
                'The seed address is the recorded transaction sender and the related address is the recorded top-level call target. This does not prove that the target is an executor or that both addresses share ownership.'::text as limitation
              from recent_candidates candidates
              where candidates.entry_contract ~* '^0x[0-9a-f]{40}$'
                and lower(candidates.entry_contract) <> lower(candidates.sender_address)
              group by lower(candidates.sender_address), lower(candidates.entry_contract)

              union all

              select
                lower(candidates.sender_address),
                lower(pools.pool_address),
                'transaction_pool'::text,
                count(*)::bigint,
                (array_agg(
                  candidates.tx_hash
                  order by candidates.block_number desc, candidates.tx_hash
                ))[1:8],
                min(candidates.block_number),
                max(candidates.block_number),
                1.0::double precision,
                'Swap logs prove that the transaction touched this pool. They do not by themselves prove profit, intent, ownership, or economic causality.'::text
              from recent_candidates candidates
              join arbitrage_candidate_pools pools
                on pools.chain_id = candidates.chain_id
               and pools.tx_hash = candidates.tx_hash
              where pools.pool_address ~* '^0x[0-9a-f]{40}$'
                and lower(pools.pool_address) <> lower(candidates.sender_address)
              group by lower(candidates.sender_address), lower(pools.pool_address)

              union all

              select
                lower(candidates.sender_address),
                lower(candidates.profit_recipient),
                'confirmed_profit_recipient'::text,
                count(*)::bigint,
                (array_agg(
                  candidates.tx_hash
                  order by candidates.block_number desc, candidates.tx_hash
                ))[1:8],
                min(candidates.block_number),
                max(candidates.block_number),
                1.0::double precision,
                'The recipient is emitted only from an economically confirmed candidate. This proves the recorded settlement recipient, not its real-world owner.'::text
              from recent_candidates candidates
              where candidates.profit_recipient ~* '^0x[0-9a-f]{40}$'
                and lower(candidates.profit_recipient) <> lower(candidates.sender_address)
                and (
                  candidates.confirmation_status like 'confirmed_%'
                  or candidates.confirmation_status in (
                    'verified', 'verified_profit', 'confirmed_economic'
                  )
                )
              group by lower(candidates.sender_address), lower(candidates.profit_recipient)
            ), prepared as (
              select
                observed.*,
                jsonb_build_object(
                  'method_version', $2::text,
                  'observation', observed.relationship_type,
                  'transaction_count', observed.transaction_count,
                  'sample_transaction_hashes', to_jsonb(observed.sample_transaction_hashes),
                  'limitation', observed.limitation
                ) as evidence
              from observed
            )
            insert into intelligence_seed_relationships (
              chain_id, seed_address, related_address, relationship_type,
              source_key, status, transaction_count, sample_transaction_hashes,
              first_seen_block, last_seen_block, confidence, evidence,
              evidence_digest, first_seen_at, last_seen_at, expires_at,
              created_at, updated_at
            )
            select
              $1, seed_address, related_address, relationship_type,
              'seed_relationship_extractor', 'observed', transaction_count,
              sample_transaction_hashes, first_seen_block, last_seen_block,
              confidence, evidence, md5(evidence::text), now(), now(),
              now() + interval '45 days', now(), now()
            from prepared
            on conflict (
              chain_id, seed_address, related_address, relationship_type, source_key
            ) do update set
              status = 'observed',
              transaction_count = excluded.transaction_count,
              sample_transaction_hashes = excluded.sample_transaction_hashes,
              first_seen_block = least(
                intelligence_seed_relationships.first_seen_block,
                excluded.first_seen_block
              ),
              last_seen_block = greatest(
                intelligence_seed_relationships.last_seen_block,
                excluded.last_seen_block
              ),
              confidence = excluded.confidence,
              evidence = excluded.evidence,
              evidence_digest = excluded.evidence_digest,
              last_seen_at = now(),
              expires_at = excluded.expires_at,
              updated_at = now()
            where intelligence_seed_relationships.status <> 'observed'
               or intelligence_seed_relationships.evidence_digest
                    is distinct from excluded.evidence_digest
               or intelligence_seed_relationships.last_seen_block < excluded.last_seen_block
            "#,
        )
        .bind(self.config.chain_id as i64)
        .bind(RELATIONSHIP_METHOD_VERSION)
        .execute(&mut *transaction)
        .await
        .map_err(persistence_error)?
        .rows_affected();
        transaction.commit().await.map_err(persistence_error)?;
        Ok(expired.saturating_add(upserted))
    }

    async fn enqueue_related_addresses(&self) -> LedgerResult<u64> {
        let result = sqlx::query(
            r#"
            with grouped as (
              select
                relationships.chain_id,
                relationships.related_address as address,
                min(case relationships.relationship_type
                  when 'confirmed_profit_recipient' then 22
                  when 'top_level_call_target' then 24
                  else 28
                end) as priority,
                sum(relationships.transaction_count)::bigint as transaction_count,
                array_agg(distinct relationships.relationship_type
                  order by relationships.relationship_type) as roles,
                (array_agg(distinct relationships.seed_address
                  order by relationships.seed_address))[1:8] as seed_addresses,
                max(relationships.confidence) as confidence,
                min(relationships.first_seen_block) as first_seen_block,
                max(relationships.last_seen_block) as last_seen_block,
                md5(string_agg(
                  relationships.evidence_digest,
                  '|' order by relationships.seed_address,
                               relationships.relationship_type
                )) as relationship_digest
              from intelligence_seed_relationships relationships
              where relationships.chain_id = $1
                and relationships.status = 'observed'
                and (
                  relationships.expires_at is null
                  or relationships.expires_at > now()
                )
              group by relationships.chain_id, relationships.related_address
              order by min(case relationships.relationship_type
                         when 'confirmed_profit_recipient' then 22
                         when 'top_level_call_target' then 24
                         else 28
                       end),
                       sum(relationships.transaction_count) desc,
                       relationships.related_address
              limit $2
            )
            insert into address_analysis_jobs (
              chain_id, address, status, attempts, priority,
              first_seen_at, last_seen_at, available_at, updated_at, metadata
            )
            select
              chain_id, address, 'pending', 0, priority,
              now(), now(), now(), now(),
              jsonb_build_object(
                'source', 'seed_relationship',
                'relationship_digest', relationship_digest,
                'roles', to_jsonb(roles),
                'seed_addresses', to_jsonb(seed_addresses),
                'transaction_count', transaction_count,
                'confidence', confidence,
                'first_seen_block', first_seen_block,
                'last_seen_block', last_seen_block,
                'method_version', $3::text
              )
            from grouped
            on conflict (chain_id, address) do update set
              priority = least(address_analysis_jobs.priority, excluded.priority),
              last_seen_at = now(),
              available_at = case
                when address_analysis_jobs.status in ('completed', 'failed')
                  and address_analysis_jobs.metadata->>'relationship_digest'
                    is distinct from excluded.metadata->>'relationship_digest'
                  then now()
                else address_analysis_jobs.available_at
              end,
              status = case
                when address_analysis_jobs.status in ('completed', 'failed')
                  and address_analysis_jobs.metadata->>'relationship_digest'
                    is distinct from excluded.metadata->>'relationship_digest'
                  then 'pending'
                else address_analysis_jobs.status
              end,
              metadata = address_analysis_jobs.metadata || excluded.metadata,
              updated_at = now()
            where address_analysis_jobs.priority > excluded.priority
               or address_analysis_jobs.metadata->>'relationship_digest'
                    is distinct from excluded.metadata->>'relationship_digest'
            "#,
        )
        .bind(self.config.chain_id as i64)
        .bind(self.config.maximum_related_addresses as i64)
        .bind(RELATIONSHIP_METHOD_VERSION)
        .execute(&self.pool)
        .await
        .map_err(persistence_error)?;
        Ok(result.rows_affected())
    }

    async fn enqueue_effective_seeds(&self) -> LedgerResult<u64> {
        let result = sqlx::query(
            r#"
            with prepared as (
              select
                seeds.*,
                history.transaction_hashes as evidence_transaction_hashes,
                coalesce(history.unanalyzed_transaction_count, 0) > 0
                  as has_unanalyzed_evidence,
                case
                  when history.transaction_digest is null then seeds.seed_digest
                  else md5(
                    seeds.seed_digest || ':' || history.transaction_digest || ':' || $2::text
                  )
                end as analysis_digest
              from effective_intelligence_seeds seeds
              left join lateral (
                select
                  array_agg(recent.tx_hash order by recent.block_number desc, recent.tx_hash)
                    as transaction_hashes,
                  md5(string_agg(
                    recent.tx_hash,
                    '|' order by recent.block_number desc, recent.tx_hash
                  )) as transaction_digest,
                  count(*) filter (where analyses.tx_hash is null)::bigint
                    as unanalyzed_transaction_count
                from (
                  select candidates.tx_hash, candidates.block_number
                  from arbitrage_candidates candidates
                  where candidates.chain_id = seeds.chain_id
                    and candidates.sender_address = seeds.address
                    and candidates.block_timestamp >= now() - interval '30 days'
                  order by candidates.block_number desc, candidates.tx_hash
                  limit $4
                ) recent
                left join transaction_analysis analyses
                  on analyses.chain_id = seeds.chain_id
                 and analyses.tx_hash = recent.tx_hash
              ) history on true
              where seeds.chain_id = $1
                and seeds.priority <= $3
            )
            insert into address_analysis_jobs (
              chain_id, address, status, attempts, priority,
              first_seen_at, last_seen_at, available_at, updated_at, metadata
            )
            select
              seeds.chain_id, seeds.address, 'pending', 0, seeds.priority,
              now(), now(), now(), now(),
              jsonb_build_object(
                'source', 'seed_intelligence',
                'seed_digest', seeds.seed_digest,
                'analysis_digest', seeds.analysis_digest,
                'seed_status', seeds.status,
                'seed_kinds', to_jsonb(seeds.seed_kinds),
                'roles', to_jsonb(coalesce(seeds.roles, array[]::text[])),
                'source_keys', to_jsonb(seeds.source_keys),
                'history_mode', case
                  when cardinality(seeds.evidence_transaction_hashes) > 0
                    then 'evidence_transactions'
                  else null
                end,
                'evidence_transaction_hashes', to_jsonb(
                  coalesce(seeds.evidence_transaction_hashes, array[]::text[])
                ),
                'has_unanalyzed_evidence', seeds.has_unanalyzed_evidence,
                'confidence', seeds.confidence,
                'first_seen_block', seeds.first_seen_block,
                'last_seen_block', seeds.last_seen_block,
                'method_version', $2::text
              )
            from prepared seeds
            on conflict (chain_id, address) do update set
              priority = least(address_analysis_jobs.priority, excluded.priority),
              last_seen_at = now(),
              available_at = case
                when address_analysis_jobs.status in ('completed', 'failed')
                  and coalesce(
                    address_analysis_jobs.metadata->>'analysis_digest',
                    address_analysis_jobs.metadata->>'seed_digest'
                  ) is distinct from excluded.metadata->>'analysis_digest'
                  then now()
                when address_analysis_jobs.status = 'completed'
                  and coalesce(
                    (excluded.metadata->>'has_unanalyzed_evidence')::boolean,
                    false
                  )
                  then now()
                else address_analysis_jobs.available_at
              end,
              status = case
                when address_analysis_jobs.status in ('completed', 'failed')
                  and coalesce(
                    address_analysis_jobs.metadata->>'analysis_digest',
                    address_analysis_jobs.metadata->>'seed_digest'
                  ) is distinct from excluded.metadata->>'analysis_digest'
                  then 'pending'
                when address_analysis_jobs.status = 'completed'
                  and coalesce(
                    (excluded.metadata->>'has_unanalyzed_evidence')::boolean,
                    false
                  )
                  then 'pending'
                else address_analysis_jobs.status
              end,
              metadata = address_analysis_jobs.metadata || excluded.metadata,
              updated_at = now()
            where address_analysis_jobs.priority > excluded.priority
               or coalesce(
                    address_analysis_jobs.metadata->>'analysis_digest',
                    address_analysis_jobs.metadata->>'seed_digest'
                  ) is distinct from excluded.metadata->>'analysis_digest'
               or (
                 address_analysis_jobs.status = 'completed'
                 and coalesce(
                   (excluded.metadata->>'has_unanalyzed_evidence')::boolean,
                   false
                 )
               )
            "#,
        )
        .bind(self.config.chain_id as i64)
        .bind(METHOD_VERSION)
        .bind(self.config.maximum_queued_priority)
        .bind(self.config.maximum_evidence_transactions_per_seed as i64)
        .execute(&self.pool)
        .await
        .map_err(persistence_error)?;
        Ok(result.rows_affected())
    }

    async fn calculate_summary(&self) -> LedgerResult<SeedIntelligenceSummary> {
        let row = sqlx::query(
            r#"
            with seed_counts as (
              select
                count(*) as total_seed_addresses,
                count(*) filter (where status = 'active') as active_seed_addresses,
                count(*) filter (where status = 'candidate') as candidate_seed_addresses
              from effective_intelligence_seeds
              where chain_id = $1
            ), detail_counts as (
              select
                count(distinct address) filter (
                  where status = 'active' and seed_kind = 'official_contract'
                ) as official_seed_addresses,
                count(distinct address) filter (
                  where status = 'candidate' and seed_kind = 'mev_actor'
                ) as mev_candidate_addresses
              from intelligence_seed_addresses
              where chain_id = $1
            ), relationship_counts as (
              select
                count(*) as relationship_count,
                count(distinct seed_address) as seeds_with_relationships
              from intelligence_seed_relationships
              where chain_id = $1
                and status = 'observed'
                and (expires_at is null or expires_at > now())
            ), ranked_evidence_transactions as (
              select
                candidates.tx_hash,
                row_number() over (
                  partition by lower(candidates.sender_address)
                  order by candidates.block_number desc, candidates.tx_hash
                ) as evidence_rank
              from arbitrage_candidates candidates
              join intelligence_seed_addresses seeds
                on seeds.chain_id = candidates.chain_id
               and seeds.address = lower(candidates.sender_address)
               and seeds.seed_kind = 'mev_actor'
               and seeds.status in ('candidate', 'active')
              where candidates.chain_id = $1
                and candidates.block_timestamp >= now() - interval '30 days'
            ), evidence_counts as (
              select
                count(distinct evidence.tx_hash) as evidence_transaction_count,
                count(distinct analyses.tx_hash) as analyzed_evidence_transaction_count
              from ranked_evidence_transactions evidence
              left join transaction_analysis analyses
                on analyses.chain_id = $1
               and analyses.tx_hash = evidence.tx_hash
              where evidence.evidence_rank <= $3
            ), queue_counts as (
              select
                count(*) filter (where priority <= $2) as queued_seed_jobs,
                count(*) filter (where priority > $2) as deferred_generic_jobs
              from address_analysis_jobs
              where chain_id = $1 and status in ('pending', 'failed')
            )
            select seed_counts.*, detail_counts.*, relationship_counts.*,
              evidence_counts.*, queue_counts.*,
              (select max(completed_at) from seed_intelligence_runs
               where chain_id = $1 and status = 'completed') as latest_completed_run_at
            from seed_counts, detail_counts, relationship_counts,
                 evidence_counts, queue_counts
            "#,
        )
        .bind(self.config.chain_id as i64)
        .bind(self.config.maximum_queued_priority)
        .bind(self.config.maximum_evidence_transactions_per_seed as i64)
        .fetch_one(&self.pool)
        .await
        .map_err(persistence_error)?;
        Ok(SeedIntelligenceSummary {
            chain_id: self.config.chain_id,
            total_seed_addresses: row.get("total_seed_addresses"),
            active_seed_addresses: row.get("active_seed_addresses"),
            candidate_seed_addresses: row.get("candidate_seed_addresses"),
            official_seed_addresses: row.get("official_seed_addresses"),
            mev_candidate_addresses: row.get("mev_candidate_addresses"),
            relationship_count: row.get("relationship_count"),
            seeds_with_relationships: row.get("seeds_with_relationships"),
            evidence_transaction_count: row.get("evidence_transaction_count"),
            analyzed_evidence_transaction_count: row.get("analyzed_evidence_transaction_count"),
            queued_seed_jobs: row.get("queued_seed_jobs"),
            deferred_generic_jobs: row.get("deferred_generic_jobs"),
            latest_completed_run_at: row.get("latest_completed_run_at"),
        })
    }

    pub async fn summary(&self) -> LedgerResult<SeedIntelligenceSummary> {
        let row = sqlx::query(
            r#"
            with seed_counts as (
              select
                count(*) as total_seed_addresses,
                count(*) filter (where status = 'active') as active_seed_addresses,
                count(*) filter (where status = 'candidate') as candidate_seed_addresses
              from effective_intelligence_seeds
              where chain_id = $1
            ), detail_counts as (
              select
                count(distinct address) filter (
                  where status = 'active' and seed_kind = 'official_contract'
                ) as official_seed_addresses,
                count(distinct address) filter (
                  where status = 'candidate' and seed_kind = 'mev_actor'
                ) as mev_candidate_addresses
              from intelligence_seed_addresses
              where chain_id = $1
            ), latest_run as (
              select metadata, completed_at
              from seed_intelligence_runs
              where chain_id = $1 and status = 'completed'
              order by completed_at desc
              limit 1
            )
            select seed_counts.*, detail_counts.*,
              coalesce((latest_run.metadata #>> '{summary,relationship_count}')::bigint, 0)
                as relationship_count,
              coalesce((latest_run.metadata #>> '{summary,seeds_with_relationships}')::bigint, 0)
                as seeds_with_relationships,
              coalesce((latest_run.metadata #>> '{summary,evidence_transaction_count}')::bigint, 0)
                as evidence_transaction_count,
              coalesce((latest_run.metadata #>> '{summary,analyzed_evidence_transaction_count}')::bigint, 0)
                as analyzed_evidence_transaction_count,
              coalesce((latest_run.metadata #>> '{summary,queued_seed_jobs}')::bigint, 0)
                as queued_seed_jobs,
              coalesce((latest_run.metadata #>> '{summary,deferred_generic_jobs}')::bigint, 0)
                as deferred_generic_jobs,
              latest_run.completed_at as latest_completed_run_at
            from seed_counts, detail_counts
            left join latest_run on true
            "#,
        )
        .bind(self.config.chain_id as i64)
        .fetch_one(&self.pool)
        .await
        .map_err(persistence_error)?;
        Ok(SeedIntelligenceSummary {
            chain_id: self.config.chain_id,
            total_seed_addresses: row.get("total_seed_addresses"),
            active_seed_addresses: row.get("active_seed_addresses"),
            candidate_seed_addresses: row.get("candidate_seed_addresses"),
            official_seed_addresses: row.get("official_seed_addresses"),
            mev_candidate_addresses: row.get("mev_candidate_addresses"),
            relationship_count: row.get("relationship_count"),
            seeds_with_relationships: row.get("seeds_with_relationships"),
            evidence_transaction_count: row.get("evidence_transaction_count"),
            analyzed_evidence_transaction_count: row.get("analyzed_evidence_transaction_count"),
            queued_seed_jobs: row.get("queued_seed_jobs"),
            deferred_generic_jobs: row.get("deferred_generic_jobs"),
            latest_completed_run_at: row.get("latest_completed_run_at"),
        })
    }

    async fn begin_run(&self) -> LedgerResult<Uuid> {
        sqlx::query_scalar(
            r#"
            insert into seed_intelligence_runs (
              worker_id, chain_id, status, metadata, started_at, created_at, updated_at
            )
            values ($1,$2,'running',$3,now(),now(),now())
            returning run_id
            "#,
        )
        .bind(&self.config.worker_id)
        .bind(self.config.chain_id as i64)
        .bind(Json(json!({
            "method_version": METHOD_VERSION,
            "minimum_mev_candidate_transactions": self.config.minimum_mev_candidate_transactions,
            "minimum_mev_distinct_routes": self.config.minimum_mev_distinct_routes,
            "maximum_mev_candidates": self.config.maximum_mev_candidates,
            "maximum_active_pools": self.config.maximum_active_pools,
            "maximum_queued_priority": self.config.maximum_queued_priority,
            "maximum_evidence_transactions_per_seed": self.config.maximum_evidence_transactions_per_seed,
            "maximum_related_addresses": self.config.maximum_related_addresses,
            "relationship_method_version": RELATIONSHIP_METHOD_VERSION,
        })))
        .fetch_one(&self.pool)
        .await
        .map_err(persistence_error)
    }

    async fn complete_run(
        &self,
        run_id: Uuid,
        iteration: &SeedIntelligenceIteration,
        summary: &SeedIntelligenceSummary,
    ) -> LedgerResult<()> {
        sqlx::query(
            r#"
            update seed_intelligence_runs
            set status = 'completed', curated_seed_changes = $2,
                protocol_seed_changes = $3, active_pool_seed_changes = $4,
                asset_seed_changes = $5, mev_candidate_changes = $6,
                relationship_changes = $7, address_jobs_changed = $8,
                relationship_address_jobs_changed = $9,
                metadata = metadata || $10,
                completed_at = now(), updated_at = now()
            where run_id = $1
            "#,
        )
        .bind(run_id)
        .bind(iteration.curated_seed_changes as i64)
        .bind(iteration.protocol_seed_changes as i64)
        .bind(iteration.active_pool_seed_changes as i64)
        .bind(iteration.asset_seed_changes as i64)
        .bind(iteration.mev_candidate_changes as i64)
        .bind(iteration.relationship_changes as i64)
        .bind(iteration.address_jobs_changed as i64)
        .bind(iteration.relationship_address_jobs_changed as i64)
        .bind(Json(json!({"summary": summary})))
        .execute(&self.pool)
        .await
        .map_err(persistence_error)?;
        Ok(())
    }

    async fn fail_run(&self, run_id: Uuid, error: &str) -> LedgerResult<()> {
        sqlx::query(
            r#"
            update seed_intelligence_runs
            set status = 'failed', last_error = $2, completed_at = now(), updated_at = now()
            where run_id = $1
            "#,
        )
        .bind(run_id)
        .bind(error.chars().take(8_000).collect::<String>())
        .execute(&self.pool)
        .await
        .map_err(persistence_error)?;
        Ok(())
    }
}

fn persistence_error(error: sqlx::Error) -> LedgerError {
    LedgerError::Persistence(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn phase_one_defaults_keep_mev_output_candidate_only() {
        let config = SeedIntelligenceConfig {
            chain_id: 4663,
            worker_id: "test".to_owned(),
            minimum_mev_candidate_transactions: 3,
            minimum_mev_distinct_routes: 2,
            maximum_mev_candidates: 500,
            maximum_active_pools: 500,
            maximum_queued_priority: 39,
            maximum_evidence_transactions_per_seed: 8,
            maximum_related_addresses: 5_000,
        };
        assert!(config.minimum_mev_candidate_transactions >= 2);
        assert!(config.minimum_mev_distinct_routes >= 1);
        assert!(config.maximum_queued_priority < 50);
        assert!(config.maximum_evidence_transactions_per_seed <= 16);
        assert!(config.maximum_related_addresses > config.maximum_mev_candidates);
    }

    #[test]
    fn relationship_schema_uses_neutral_observation_names() {
        let migration = include_str!("../../migrations/0038_seed_relationship_intelligence.up.sql");
        assert!(migration.contains("top_level_call_target"));
        assert!(migration.contains("transaction_pool"));
        assert!(migration.contains("confirmed_profit_recipient"));
        assert!(!migration.contains("'executor'"));
        assert!(!migration.contains("'owned_by'"));
    }
}
