# LedgerFlow Cold-Start Seed Intelligence S2A

Date: 2026-08-15 UTC

Timebox: one bounded development cycle, at most eight working hours. S2A
prioritizes seed enrichment over full transaction catch-up. Canonical block,
transaction, receipt, and log indexing continues independently; S2A does not
change or claim full historical Indexer coverage.

## Stage objective

S2A consumes the S1 database-backed seeds and existing deterministic candidate
facts to produce useful first-hop context without claiming identity, ownership,
intent, or profit:

```text
S1 effective seeds
        |
        +--> deterministic first-hop relationships
        |       +--> top-level call targets
        |       +--> pools touched by persisted swap logs
        |       `--> confirmed profit recipient only when already confirmed
        |
        +--> latest eight explicit evidence transactions per MEV seed
        |       `--> existing deterministic transaction analysis pipeline
        |
        `--> priority 22-28 analysis for related addresses
```

The bounded path is designed for cold start: processing a seed never calls the
unbounded address-history endpoint. Transaction history can lag while canonical
indexing and seed analysis continue.

## Acceptance target

- Migration-backed, chain-scoped relationship facts with evidence digests,
  samples, validity windows, and neutral relationship names.
- Existing candidate facts are the only S2A relationship input.
- Every selected MEV seed receives up to eight latest explicit evidence
  transaction hashes from the last 30 days.
- Existing transaction analysis is reused; already analyzed hashes are skipped.
- Any bounded evidence failure fails the address job and uses the existing retry
  path rather than silently completing.
- Completed jobs with missing evidence coverage are recoverably requeued by the
  seed planner.
- First-hop related addresses are queued at priority 22-28, capped by
  configuration, with relationship evidence in job metadata.
- Coverage and relationship APIs expose real persisted data and limitations.
- Metrics report relationship and evidence-analysis coverage.
- No automatic promotion from candidate to verified.
- The canonical Indexer and production Searcher are not restarted or modified.

## Relationship semantics

| Relationship type | Deterministic evidence | What it does not prove |
|---|---|---|
| `top_level_call_target` | Candidate sender and transaction `to`/entry contract | Executor role, ownership, identity, or intent |
| `transaction_pool` | Persisted candidate swap log names the pool | Profit, economic causality, ownership, or strategy |
| `confirmed_profit_recipient` | Only a pre-existing confirmed/verified economic settlement fact | Bot owner or broader organizational identity |

`confidence = 1.0` on the first two types means confidence in the narrow
observation, not confidence in MEV classification. The API also returns
`semantic_boundary: observed_relationship_only`.

## Scheduling

| Priority | Work |
|---:|---|
| 0-19 | Existing S1 seeds, including MEV candidates at priority 10 |
| 22 | Related top-level call target |
| 24 | Related confirmed profit recipient, when one exists |
| 28 | Related transaction pool |
| 40+ | Existing broad/generic backlog, still deferred by production worker |

Lower numbers run first. Priority is a scheduling decision, not evidence
confidence.

## Configuration

- `LEDGERFLOW_SEED_MAX_EVIDENCE_TRANSACTIONS_PER_SEED` defaults to `8` and is
  hard-bounded by the address worker to at most 16 hashes.
- `LEDGERFLOW_SEED_MAX_RELATED_ADDRESSES` defaults to `5000`.
- `LEDGERFLOW_SEED_MAX_QUEUED_PRIORITY` defaults to `39`.
- `LEDGERFLOW_ADDRESS_WORKER_MAX_PRIORITY` defaults to `39`.
- `LEDGERFLOW_INDEXER_API_BASE_URL` is required only for full address-history
  retrieval. Explicit S2A evidence hashes can still be analyzed without it.

## APIs and metrics

- `GET /intelligence/seeds/relationships`
- `GET /intelligence/seeds/coverage`
- `GET /intelligence/seeds/summary`
- `GET /metrics`

Relationship queries accept `chain_id`, `address`, `relationship_type`,
`status`, and `limit` where applicable. Coverage reports are explicit that
`full_history_coverage` is false.

Metrics added in S2A:

- `ledgerflow_seed_relationships`
- `ledgerflow_seeds_with_relationships`
- `ledgerflow_seed_evidence_transactions`
- `ledgerflow_seed_evidence_transactions_analyzed`

## Recovery behavior

The planner persists an analysis digest derived from the seed digest, bounded
evidence hashes, and method version. A changed digest requeues completed or
failed work. Independently, a completed seed job is requeued when any selected
evidence hash has no `transaction_analysis` row. A failed bounded analysis uses
the existing ten-minute retry backoff; successful hashes are skipped on retry.

## Explicit non-goals

- No full-history transaction catch-up.
- No guessed funding, internal-call, ownership, executor, or profit
  relationships.
- No automatic `Verified` event or MEV actor promotion.
- No new protocol classifier, arbitrage algorithm, liquidation logic, chain
  adapter, or UI redesign.
- No Searcher or canonical Indexer lifecycle change.

Measured deployment results are recorded in
[`cold-start-seed-intelligence-s2a-results.md`](cold-start-seed-intelligence-s2a-results.md).
