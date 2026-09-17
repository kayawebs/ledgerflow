# Observed Address Intelligence v1 gap analysis

Date: 2026-08-20

Scope: Base (`8453`) and Robinhood Chain (`4663`) execution-oriented
addresses only: Searcher/Worker EOAs, executor contracts, high-frequency
trading addresses, and candidate execution clusters.

## 1. Current data flow

```text
Workspace + Observe form
  -> workspace_entities row
  -> redirect to /app/observations/:address
  -> Web requests /addresses/summary from the chain registry
  -> the API lazily enqueues address_analysis_jobs
  -> chain-specific address-worker discovers a bounded recent sample
  -> transaction_analysis / labels / profile
  -> Web combines the sample with material events and cluster graph rows
```

The public/private split and the `ObservedObject` UI abstraction already
exist. The persisted object is still `workspace_entities`; there is no durable
observation analysis contract or progress state.

## 2. Existing reusable modules

- Chain registry routes Base and Robinhood API/indexer requests to independent
  RPC, PostgreSQL, ClickHouse, queues, and workers.
- The live indexer stores canonical blocks, top-level transactions, receipts,
  logs, address endpoint references, and checkpoints.
- `AnalysisService` deterministically retrieves transaction/receipt/block,
  derives token/native flows, runs existing recognizers, and stores Evidence
  Packs.
- The address worker classifies code/type, probes pool-compatible contracts,
  discovers direct and ERC-20 Transfer-linked transactions, and creates
  evidence-backed label assertions.
- `economic_verification_results` contains strict verified / insufficient /
  dismissed outcomes for supported arbitrage candidates.
- Seed relationship extraction, cluster flow graph, pilot delivery, weekly
  reports, and notification settings are reusable internal modules.
- Existing Workspace, monitor, investigation, and report tables must remain.

## 3. Completed capability

- Base and Robinhood live indexing with restartable checkpoints.
- Strict worker chain-id isolation.
- Direct sender/recipient address lookup in indexed live ranges.
- Transfer-only activity discovery via indexed ERC-20 topics.
- Deterministic transaction analysis and persisted evidence.
- Candidate-versus-verified economic state separation.
- Workspace-owned observation routes and chain-specific API routing.

## 4. Partial capability

- Address history is bounded to a 50,000-block / 32-transaction sample. It is
  useful for cold-start evidence but is not a 24h/7d/30d contract.
- Address summaries expose analyzed count, coarse behavior, gas, and 25 recent
  rows, but do not distinguish subject-attributable gas or window coverage.
- Relationships are assembled from event participants or cluster graph edges;
  an object-specific relationship record with supporting transaction hashes is
  missing.
- The economic verifier is reliable for its explicit seeded arbitrage scope,
  but is not automatically scheduled for every observed object transaction.
- Existing weekly reports are project/event oriented rather than automatic
  object briefs.
- Monitoring can be enabled from the object page, but Observe does not enable
  it transactionally.

## 5. Missing capability

- Observation analysis state machine and observable progress.
- A durable activity ledger independent of Material Events.
- Exact 24h, 7d, and 30d deterministic summaries.
- Up-to-10,000 transaction coverage with explicit partial-coverage metadata.
- Object-specific behavior evidence, relationships, economics, baseline,
  change signals, and automatic briefs.
- Restartable discovery/analysis checkpoints and failed-run retry.
- Two-block incremental scheduling for ready observations.
- Workspace overview rows backed by per-object facts rather than global zeros.

## 6. Data quality risks

1. Standard RPC cannot enumerate arbitrary historical calls where an address is
   neither a top-level endpoint nor an indexed log participant. Coverage must
   say so explicitly.
2. A transfer recipient or balance increase is not profit. Only persisted
   deterministic verification may contribute to verified profit.
3. Gas belongs to the transaction sender. Recipient-only observations must not
   be charged the outer transaction gas.
4. Protocol names must come from protocol metadata/recognizer evidence; unknown
   call targets remain unknown contracts.
5. Trace availability varies. Missing traces reduce relationship/economic
   coverage rather than being replaced with inference.
6. High-frequency addresses may exceed 10,000 transactions; truncation must be
   newest-first, explicit, and resumable.
7. Historical index coverage is not genesis-complete. Activity discovery must
   report the indexed/RPC window actually examined.

## 7. Intelligence Contract v1 target

One chain-plane observation record owns:

- status: `queued`, `discovering`, `backfilling`, `classifying`,
  `verifying_economics`, `building_baseline`, `ready`, `stale`, `failed`;
- progress counters and timestamps;
- deterministic per-transaction activity rows;
- 24h/7d/30d activity summaries;
- evidence-backed behavior and relationship rows;
- verified/estimated/unknown economic summaries with exclusions and coverage;
- baseline comparison/change signals;
- generated 24h, weekly, and monthly briefs.

The chain-plane profile is reusable across customer workspaces. The private
workspace stores only the user's observation and notification preferences.

## 8. Implementation order

1. Add observation schema through migration `0046`.
2. Add typed Intelligence Contract v1 models and API endpoints.
3. Add a chain-specific, restartable observation worker.
4. Discover direct and Transfer-linked activity without full-chain replay.
5. Analyze batches and persist deterministic activity rows.
6. Aggregate behavior, relationships, economics, baseline, and briefs.
7. Make Observe enqueue analysis and enable internal monitoring defaults.
8. Render progress/activity-first object and workspace summaries.
9. Add fixed regression objects and deterministic tests.
10. Deploy one worker per chain and run real acceptance samples.

## 9. Expected modified files

- `migrations/0046_observed_address_intelligence_v1.{up,down}.sql`
- `src/services/observation_intelligence.rs`
- `src/bin/ledgerflow_observation_worker.rs`
- `src/api/mod.rs`
- `src/web/mod.rs`
- `src/web/execution_intelligence.rs`
- `src/services/mod.rs`, `src/lib.rs`, `Cargo.toml`, `Dockerfile`
- `docker-compose.yml`, `.env.example`, `config/pilots/example-base.env`
- `tests/observed_address_intelligence.rs`
- `tests/fixtures/observed_address_intelligence_v1.json`
- `docs/product/observed-address-intelligence-results.md`

## 10. Explicitly unchanged

- Public website, visual identity, product naming, billing, and authentication.
- Searcher execution logic and private Searcher infrastructure.
- Indexer storage model and existing production checkpoints.
- Existing Entity, Pilot, Monitor, Investigation, and Report tables.
- New chains, new protocol classifiers, generic wallet PnL, CEX accounting,
  and ownership attribution.
