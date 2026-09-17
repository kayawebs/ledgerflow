# LedgerFlow

LedgerFlow is an evidence-first Execution Intelligence workspace for onchain execution, MEV, liquidations, and transaction flow. This branch runs its primary web/control plane against Robinhood Chain (`chain_id=4663`) while retaining the isolated Base Pilot data plane (`chain_id=8453`) and keeping chain identity in runtime and database configuration.

The target multi-chain deployment uses a central web/control plane and one
isolated data plane per chain. See
[docs/multichain_architecture.md](docs/multichain_architecture.md) and
[ops/multichain/README.md](ops/multichain/README.md). The current runtime still
has single-upstream web assumptions, so follow the documented phased rollout
instead of pointing the web process directly at remote chain databases.

It ships with a separate `web` process that fronts the API with an operator UI: homepage, email-verified registration, password or email-code login, and an Execution Intelligence workspace organized by user tasks rather than database tables.

The credential-free public product has six primary destinations: **Home**, **Solutions**, **Research**, **Evidence Explorer**, **API**, and **About**. `/research` is the technical publication, `/evidence` exposes customer-safe execution evidence records, and `/pilot` accepts a scoped 10–30 address or contract request without creating billing or CRM state. Legacy `/reports`, `/cases`, and `/products` URLs redirect to the new information architecture. The access abstraction is `free`, `pilot`, or `enterprise`; existing and newly registered users default to `free`, while plan changes remain operator-managed.

The public site and private application have separate jobs:

- **Public:** Research, Evidence Explorer, and open Evidence Pack / Execution Event schemas.
- **Pilot:** Custom monitored entities, rules, Telegram alerts, private investigations, and weekly reports.
- **Enterprise:** Broader private datasets, operational intelligence, and scoped API integration.
- **Private application:** Workspace, Monitoring, Alerts, Investigations, Reports, and API. It is not the default public experience.

Public-product strategy, access boundaries, grant material, and the first 30-day technical content calendar live in:

- [Public product strategy](docs/product/public-product-strategy.md)
- [Pricing and access model](docs/product/pricing-and-access-model.md)
- [Grant package](docs/grants/ledgerflow-grant-package.md)
- [Twitter content plan](docs/marketing/twitter-content-plan.md)

The authenticated application is an Observation-Centric Workspace. New users create a workspace and observe one address or contract. LedgerFlow immediately queues deterministic 30-day discovery, shows real analysis progress, and enables continuous observation; notification channels are configured once in Settings. Returning users land on their workspace rather than a Global Explorer. The Pilot Workspace keeps four primary entries: **Overview**, **Observed Objects**, **Intelligence Briefs**, and **Settings**. Research stays on the public website; API appears only for Enterprise or Developer access.

The core private surfaces are:

- **Workspace Overview** — what changed across observed objects, including behavior, relationships, verified economic impact, and evidence-linked attention items.
- **Observed Objects** — the primary customer workflow. `/app/observations/:address` combines Overview, Behavior, Relationships, Economic Intelligence, Timeline, and continuous-observation actions without asserting ownership or total profit.
- **Object-contextual activity** — events and investigations start from an observed object or its evidence rather than separate primary navigation.
- **Continuous observation** — Observe creates the internal default watch and delivery route automatically. Monitor, rule, and destination records remain implementation details rather than setup steps.
- **Intelligence Briefs** — private object/protocol report snapshots under Reports. Public publications remain under `/research`.
- **Notification Settings** — Telegram is configured once per workspace and reused by active object watches; unavailable channels are labelled honestly.
- **API** — Enterprise-only scoped access and key management; versioned schemas remain public.

Legacy routes such as `/app/today`, `/app/bots`, `/app/watchlist`, `/app/alerts`, `/app/clusters`, `/app/address-analysis`, `/app/arbitrage`, and `/app/cluster-graph` redirect into the corresponding new task surface. Existing APIs remain unchanged.

Email verification supports real delivery through Postmark or Resend. If the web process starts without email credentials, it stays in explicit log-only mode instead of silently pretending mail is enabled; `/health` surfaces that operational status without exposing it in the account UI.

The current product surface is observation-first: add an address or contract, follow the historical-analysis state, then review deterministic activity, behavior, relationships, strict economic coverage, change signals, and automatically generated Intelligence Briefs. Entity remains an internal evidence model and relationships never imply proven ownership.

The v1 chain-plane contract, limitations, and live acceptance state are documented in [Observed Address Intelligence gap analysis](docs/product/observed-address-intelligence-gap-analysis.md) and [Observed Address Intelligence results](docs/product/observed-address-intelligence-results.md).

Execution-oriented observations also start the migration-backed Searcher Cluster
Intelligence worker. It classifies the seed, expands evidence-scored candidate
workers/executors/settlement recipients, reconstructs cluster executions, keeps
funding and settlement separate from verified profit, and materializes bounded
24h/7d/30d windows. See the [cluster gap analysis](docs/product/searcher-cluster-intelligence-gap-analysis.md)
and [runtime results](docs/product/searcher-cluster-intelligence-results.md).

The implementation and evidence boundaries are documented in [Observation-Centric Workspace v3](docs/product/observation-centric-workspace-v3.md). The previous [Entity Intelligence Workspace v2](docs/product/entity-intelligence-workspace-v2.md) remains as historical design context.

The repository also ships a standalone `ledgerflow indexer` binary for Robinhood Chain's canonical fact layer. It stores blocks, transactions, receipts, logs, and top-level `from` / `to` address references in ClickHouse, and exposes an address-to-tx query API without depending on the analysis API.

If the indexer API is protected, set `LEDGERFLOW_INDEXER_API_BEARER_TOKEN` in the analysis service so cluster backfill requests include `Authorization: Bearer <token>`.

The web portal also exposes an `Indexer API keys` page for operators who want to issue external client keys. Configure `LEDGERFLOW_INDEXER_INTERNAL_API_TOKEN` so the portal can call the indexer's admin endpoints, then create per-client keys from the dashboard.

For operators whose self-hosted node is bound only to localhost, the recommended setup is a host-level nginx reverse proxy or the configured Docker host gateway, followed by LedgerFlow's rate-limited `rpc-gateway`. See [docs/node_rpc_proxy.md](docs/node_rpc_proxy.md).

## What v0 does

- Ingests raw transaction, receipt, optional trace, and target bytecode evidence.
- Normalizes raw evidence into replayable execution facts.
- Derives basic native and ERC20 flows.
- Runs versioned recognizers over normalized facts instead of raw RPC payloads.
- Builds deterministic Evidence Packs with input digests and validation results.
- Persists analysis artifacts in PostgreSQL via SQLx.
- Exposes an internal Axum API for analysis, replay, evidence-pack fetch, recognizer listing, and health checks.
- Computes address-cluster spend summaries over a time window, separating gas burn, external outflows, internal transfers, and incoming transfers.
- Can queue cluster transaction discovery jobs from the standalone indexer, then analyze them into PostgreSQL-backed coverage and spend summaries.
- Automatically enqueues a historical backfill when a new cluster is created. A separate worker process drains that queue without blocking the API.
- Includes an optional trace monitor process that computes address-level and cluster-level net asset deltas from ERC20 logs plus trace native transfers, retaining only recent rows.
- Provides email-verified registration plus password or email-code login in the web process, with pluggable verification delivery.
- Continuously snapshots Robinhood's official asset and contract registries, validates deployments against the configured local RPC, and imports versioned token/protocol intelligence without code-level address constants.
- Discovers interface-verified DEX factories and pools from canonical events, projects ERC-8056 events, and enriches every discovered pool token through the local RPC.
- Continuously projects canonical transaction/log participants into PostgreSQL with persisted cursors, windowed evidence, first/last-seen blocks, and a deduplicated address-analysis queue.
- Produces expiring `automation_candidate` assertions from canonical statistical windows and exposes an auditable accept/reject/retract review queue; automatic rules never promote candidates directly to bot, searcher, or team identities.
- Runs a bounded seed-first Economic Verifier over representative MEV-candidate transactions. It reconciles receipt logs, trace-native flows, archive balances, and exact gas without requiring full historical indexing; incomplete evidence stays `insufficient_evidence`.
- Includes a documented Public / Free / Pilot / Enterprise access model without billing or automated plan upgrades.

## What v0 does not do

- Full protocol semantics.
- State-diff-based accounting precision.
- Opportunity-level P&L correctness.
- ABI/source verification as ground truth.
- Universal transaction understanding.
- Independent member-address monitoring after a candidate permanently stops
  interacting with its observation seed; current incremental cluster discovery
  is driven by newly processed seed evidence.

## Quick start

1. Copy `.env.example` to `.env`. Docker Compose reads `.env` as the runtime config file.
2. Point `LEDGERFLOW_RPC_URL`, `LEDGERFLOW_INDEXER_RPC_URL`, and the Compose RPC upstream at the local Robinhood Nitro RPC.
3. Start the full stack:

```bash
docker compose up -d --build
```

Open the public website at `http://127.0.0.1:3001/` or sign in at `http://127.0.0.1:3001/auth`. First login redirects to `/app/onboarding`; configured accounts land on `/app/overview`. The analysis API remains at `http://127.0.0.1:3000` and is not the browser UI.

The first startup applies database migrations, including the private Workspace, Workspace Entity, monitoring-goal, destination, Monitor, Investigation, `workspace_plan`, and bounded public Pilot-intake tables. Existing owned Entity Clusters are mapped to private workspaces without changing indexer or analysis data. Telegram credentials remain server-side and the UI stores only the destination reference. A saved Monitor is not shown as delivered until its delivery-history record confirms worker consumption.

4. Run checks locally:

```bash
make check
make test
```

For a practical operator walkthrough, read [docs/usage.md](docs/usage.md).

### Economic Monitoring Pilot v0.1

The Base-only Pilot adds database-backed projects, 10–30 monitored entities, continuous discovery through the existing indexer/backfill path, eight deterministic economic event types, versioned Evidence Packs, durable Telegram delivery, completed-week Markdown/HTML reports, and Pilot health/metrics/API routes. It runs in a Base data plane (`chain_id=8453`); the Robinhood services in this branch remain an optional, separate adapter and must not be used as the Pilot Base RPC.

```bash
docker compose --profile pilot up -d --build
```

Use [the example Pilot YAML](config/pilots/example-base-project.yaml), follow [startup and recovery](docs/operations/startup-recovery.md), and review the honest [Phase 1 acceptance results](docs/acceptance/phase1-results.md). Public Demo artifacts are under [demo/base-public](demo/base-public/README.md).

Public Evidence Explorer events are continuously published from explicitly opted-in projects. See [continuous public evidence publishing](docs/operations/public-evidence-publishing.md) for the authorization boundary, verification gates, bootstrap command, and health checks.

Default local endpoints:

- API: `http://127.0.0.1:3000`
- Web: `http://127.0.0.1:3001`
- Indexer API: `http://localhost:8090`
- Trace monitor API: `http://127.0.0.1:3010`

The intended exposure model is: web public, analysis API private. In local/dev this means the API stays on localhost or container-internal networking while operators use the web process.

## Standalone indexer

`ledgerflow indexer` maintains the canonical chain fact layer:

- the `live` worker follows the local node from a bounded head window
- the optional `history` worker freezes its upper bound from the persisted live range, then resumes from its own durable cursor
- blocks, transactions, receipt logs, L1 block references and address directions are stored in ClickHouse; the Robinhood history first pass intentionally defers receipt-derived fields and logs
- parent hashes are checked continuously; a reorg marks the old block hashes orphaned and rewinds the persisted cursor
- address lookup queues are consumed by the separately throttled `indexer-lazy` service
- `GET /progress` reports RPC head, live lag and worker cursors on a fast path; exact raw-table counts are opt-in diagnostics
- `GET /addresses/{address}/txs` and `/tx-hashes` return canonical references only

Run it with Docker:

```bash
docker compose up --build clickhouse rpc-gateway indexer indexer-lazy
```

Full history is deliberately not part of normal startup or a prerequisite for
seed intelligence. To run the diagnostic canonical/transaction-body pass,
explicitly opt in with `docker compose --profile full-history up -d
indexer-history`. That pass leaves receipt-derived status, gas, contract address
and logs unavailable (`null`/absent), so it is not complete economic evidence.
Address-triggered historical work must use explicit bounded block ranges.

Run it locally:

```bash
export LEDGERFLOW_INDEXER_CLICKHOUSE_URL=http://127.0.0.1:8123
export LEDGERFLOW_INDEXER_CLICKHOUSE_USER=ledgerflow
export LEDGERFLOW_INDEXER_CLICKHOUSE_PASSWORD=ledgerflow
export LEDGERFLOW_INDEXER_RPC_URL=http://127.0.0.1:8545
export LEDGERFLOW_INDEXER_WORKER_MODE=live
export LEDGERFLOW_INDEXER_WORKER_ID=robinhood_live
make run-indexer
```

Example query:

```bash
curl "http://localhost:8090/addresses/0x1111111111111111111111111111111111111111/txs?limit=100&direction=from"
```

```bash
curl "http://localhost:8090/addresses/0x1111111111111111111111111111111111111111/tx-hashes?limit=100"
```

For the full operational flow, read [docs/indexer.md](docs/indexer.md).

## Seed-first economic verification

`economic-verifier` consumes only database-backed `mev_actor` seeds and their
existing representative multi-pool candidate transactions. It never requests
genesis-to-head history. Each job produces one of three persisted outcomes:

- `verified`: exact positive token surplus for the explicit sender scope, with
  receipt, trace, archive-balance, and gas reconciliation;
- `insufficient_evidence`: a required deterministic input is missing or the
  accounting scope is ambiguous;
- `dismissed`: deterministic evidence contradicts the candidate.

The UI shows verified token surplus and gas separately. Net profit remains
`Unknown` when unlike assets cannot be converted deterministically. Start only
this bounded worker with:

```bash
docker compose up -d economic-verifier
```

Queue limits, retry/recovery controls, trust boundaries, and live acceptance
evidence are documented in
[the S2B plan](docs/product/cold-start-economic-verifier-s2b.md) and
[the S2B results](docs/product/cold-start-economic-verifier-s2b-results.md).

## Official registry synchronization

The `registry-sync` service reads its enabled sources, URLs, parser types, chain IDs, and schedules from PostgreSQL. Each response is stored before parsing with its headers and SHA-256 checksum. Accepted contract addresses must pass local-RPC bytecode checks; ERC-20 deployments also pass `symbol()`, `name()`, and `decimals()` checks. Failed validation remains auditable and is not promoted into the active intelligence dataset.

Run one due-source pass locally:

```bash
LEDGERFLOW_REGISTRY_SYNC_RUN_ONCE=true cargo run --bin ledgerflow_registry_sync
```

Docker Compose runs the same process continuously as `registry-sync`. Source schedules can be changed in `intelligence_source_endpoints` without rebuilding the application.

## On-chain discovery and address production

`discovery-worker` reads canonical event logs from ClickHouse. Pool candidates are promoted only after bytecode, token getters, factory relationship, and pool-variant interfaces agree with the creation event. Factory and pool identities are database-backed, versioned, and never compiled as chain addresses.

`address-producer` reads canonical transactions and ABI-known event address positions. It stores per-window evidence in `address_observation_windows`, updates `chain_addresses.first_seen_block` / `last_seen_block`, and queues each new address for RPC classification. Unknown indexed topics remain raw ClickHouse facts instead of being guessed as addresses.

`behavior-worker` aggregates recent canonical sender windows only after an address has been verified as an EOA. Every candidate includes the block window, counts, sample transaction hashes, rule version, confidence, and TTL. Its thresholds and rule version are environment configuration rather than address lists. Pending claims enter `behavior_assertion_reviews`; decisions create manual assertions or retractions while `behavior_assertion_review_events` preserves the audit trail.

Both workers persist their cursors in `onchain_event_cursors` and resume after restart. Run them with Docker Compose:

```bash
docker compose up -d discovery-worker address-producer address-worker behavior-worker
```

## Data migration

LedgerFlow stores analysis/web/monitor data in PostgreSQL and top-level indexer data in ClickHouse. Export, restore, and old-server cleanup scripts live in [ops/ledgerflow-data](ops/ledgerflow-data).

Typical migration flow:

```bash
./ops/ledgerflow-data/export.sh
scp -r ubuntu@<old-server>:/home/ubuntu/ledgerflow/ledgerflow-backups/ledgerflow-YYYYMMDDTHHMMSSZ ./
docker compose up -d db clickhouse
./ops/ledgerflow-data/restore.sh ledgerflow-backups/ledgerflow-YYYYMMDDTHHMMSSZ
docker compose up -d
```

## Protocol Market Intelligence web

The market-oriented LedgerFlow UI is a separate binary and service. It keeps
the existing public/workspace web unchanged and uses protocol-scoped domains
for launch participant relationships. Its first routes are:

- `/` market overview
- `/launches` and `/launches/:id` launch intelligence
- `/clusters` query-first address intelligence plus Token, Arbitrage, Liquidation, and Other protocol-scoped cluster filters
- `/clusters/:id` member roles, relationships, interval earnings and supporting transaction evidence
- `/arbitrage` per-asset verified earnings ranking plus execution evidence
- `/liquidations` verified public liquidation evidence
- `/seed-library` provenance-bearing protocol contract seeds

Import the official Pons seed manifest and start the independent service:

```bash
docker compose --profile market-bootstrap run --rm market-seed-import
docker compose up -d market-web protocol-launch-indexer protocol-cluster-projector
```

Open `http://127.0.0.1:3001`. Add `?lang=zh` for Chinese; language, network,
and time-window controls are available in the header. Public data is readable
without authentication. Wallet connection signs a nonce-bearing message and
creates a server-side session; it never submits an onchain transaction. There
is no payment or billing flow in this version.

The previous workspace web is retained as the opt-in Compose profile
`legacy-web`, but it is not started by the default stack because the market web
now owns port 3001.

Launch metric cells remain `Not indexed` until a deterministic protocol event
projector has produced price, flow, cohort, and participant-outcome snapshots.
Detected executions are not presented as verified profit.
The JSON ranking is available at `/api/market/arbitrage-rankings`; `window`
accepts `5m`, `15m`, `1h`, `6h`, `24h`, `7d`, `30d`, or `lifetime`. Rankings
use execution time and never combine unlike profit assets. Native gas remains a
separate exact cost unless an asset-equivalence rule can prove a conversion.

The protocol cluster projector consumes only LedgerFlow-owned database facts. A
Pons buyer becomes an issuer-cohort candidate only after buying launches from
the same creator across at least two distinct launches. Verified arbitrage
clusters start from deterministic execution results and preserve worker,
executor, settlement and known protocol-contract roles. These clusters never
assert common ownership outside their protocol domain.

## API

- `POST /analyses/tx`
- `GET /analyses/{tx_hash}?chain_id=8453`
- `GET /evidence-packs/{pack_id}`
- `GET /recognizers`
- `GET /behavior-reviews?chain_id=4663&status=pending&limit=50`
- `POST /behavior-reviews/{review_id}/decision`
- `POST /replay/tx`
- `POST /clusters`
- `POST /clusters/{cluster_id}/backfill`
- `GET /clusters/{cluster_id}/coverage`
- `GET /clusters/{cluster_id}/backfill-runs`
- `GET /clusters/{cluster_id}/spend-summary?start=...&end=...`
- `GET /health`
- `GET /ready`
- `GET /metrics`
- `GET /pilot/evidence-packs/{event_id}`
- `GET /pilot/projects/{project_id}/events`
- `GET /pilot/projects/{project_id}/deliveries`
- `GET /pilot/projects/{project_id}/weekly-reports/latest`

Example:

```bash
curl -X POST http://localhost:8180/analyses/tx \
  -H 'content-type: application/json' \
  -d '{"chain_id": 8453, "tx_hash": "0xabc"}'
```

## Project layout

```text
src/
  accounting/        Cost and rollup scaffolding
  api/               Axum routes and request/response schemas
  core/              Config, logging, errors, versioning
  domain/            Stable entities, enums, interfaces
  evidence_pack/     Deterministic export assembly and hashing
  flows/             Asset flow derivation
  ingestion/         RPC abstractions and fetchers
  indexer/           Standalone top-level address indexer
  jobs/              Background runner abstraction
  normalization/     Raw evidence -> normalized facts
  plugins/           Plugin registration surfaces
  recognition/       Recognizer framework and built-ins
  repositories/      SQLx and in-memory repositories
  scenarios/         Scenario adapters
  services/          Orchestration
  validation/        Replay and consistency checks
migrations/
docs/
tests/
```

## Design stance

LedgerFlow is scenario-extendable, not magical. It is designed to look more general over time by accumulating stable evidence facts and layering narrow recognizers and adapters safely.

Address labels, curated seeds, source trust, and versioned intelligence imports
are documented in [Address Intelligence V2](docs/address_intelligence_v2.md).

Cold-start intelligence is seed-first. Canonical block and top-level
transaction-body indexing remains continuous; historical receipt/log enrichment
and expensive address/transaction analysis may lag and are scheduled separately
from database-backed official, protocol, asset, customer, and MEV candidate
seeds. See
[`docs/product/cold-start-seed-intelligence-s1.md`](docs/product/cold-start-seed-intelligence-s1.md)
for the seed registry and priority scheduler, and
[`docs/product/cold-start-seed-intelligence-s2a.md`](docs/product/cold-start-seed-intelligence-s2a.md)
for deterministic first-hop relationships and bounded evidence-transaction
analysis.
