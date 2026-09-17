# LedgerFlow Cold-Start Seed Intelligence S1

Date: 2026-08-14 UTC

Timebox: one development cycle, at most eight working hours. The timebox covers
implementation, migration, automated tests, deployment to the existing
Robinhood data stack, and live verification. It does not claim that eight hours
of chain history or every queued address can be analyzed.

## Outcome

S1 changes LedgerFlow from an unbounded generic-address analyzer into a
seed-first intelligence pipeline:

```text
canonical blocks/transactions/receipts/logs
                 |
                 v
database-backed seeds with source and evidence
                 |
                 v
priority 0-39 targeted address analysis
                 |
                 v
protocol/asset/actor facts and selective tx history
```

Live canonical indexing remains complete from its activation block and is
independent. Historical coverage is selective and explicit; seed evidence uses
known transaction hashes and does not wait for a genesis-to-head backfill. A
transaction may be deferred for analysis without blocking live ingestion.

## S1 acceptance target

- Additive migration creates a chain-scoped seed registry and run history.
- Official/curated/manual labels become active high-priority seeds.
- Reviewed protocol contracts and factories become active seeds.
- The most active deterministically discovered pools and their assets become
  active seeds.
- Addresses with at least three multi-pool candidate transactions over at
  least two route fingerprints become `mev_actor` **candidates** for targeted
  analysis.
- No repeated multi-pool signal is promoted to verified MEV, arbitrage, profit,
  or real-world identity.
- Seed jobs use priority 0-39 and preempt the existing generic backlog.
- The canonical fact producer stops enqueueing ordinary sender, recipient, log
  emitter, and indexed-event addresses for heavy analysis by default.
- The generic backlog remains stored and recoverable, but the production
  address worker does not claim jobs above priority 39.
- `/intelligence/seeds`, `/intelligence/seeds/summary`, and `/metrics` expose
  seed counts, candidate counts, targeted queue depth, generic deferred depth,
  and latest successful seed run.
- Tests, type checking, formatting, migration, worker run, and live queries
  pass without restarting or modifying the canonical Indexer or Searcher.

## Seed tiers

| Priority | Source | Public meaning |
|---:|---|---|
| 0-9 | customer, manual, official, curated | High-trust seed |
| 10-19 | protocol factory, active pool/asset, repeated MEV-like actor | Targeted enrichment; actor remains candidate unless separately verified |
| 20-39 | deterministically discovered high-value contract | Targeted enrichment |
| 40+ | broad relationships and generic chain participants | Deferred by default |

Lower numbers run first. Priority affects scheduling only; it is not confidence.

## MEV candidate rule v1

The first rule is intentionally a selector, not a classifier conclusion:

- input is persisted `arbitrage_candidates` only;
- sender must appear in at least three candidate transactions in 30 days;
- at least two distinct non-empty route fingerprints are required;
- candidate count, routes, entry contracts, prior automation signal, sample
  transaction hashes, first/last block and limitations are persisted;
- confidence is capped below 0.80;
- status is always `candidate` unless a later independent review or economic
  verifier promotes it.

Frequency alone remains `actor:automation_candidate` and does not prove MEV.

## Explicit non-goals

- No all-transaction catch-up SLA.
- No automatic `verified_mev_actor` promotion.
- No guessed profit, protocol identity, bot owner or transaction intent.
- No new chain adapter, protocol classifier, billing, signup, Searcher change,
  or Indexer rewrite.
- No deletion of the existing generic address queue.

## Next bounded stage

S2 should consume the S1 seeds to build evidence-backed EOA/executor/profit
recipient/funding relationships and run deep transaction accounting only for
those seeds. Promotion to verified MEV must require repeatable economic and
ordering evidence, not just activity frequency.

The completed deployment evidence and measured results are recorded in
[`cold-start-seed-intelligence-s1-results.md`](cold-start-seed-intelligence-s1-results.md).
