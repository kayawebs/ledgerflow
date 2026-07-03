# Bot and Cluster Analysis Redesign

Date: 2026-07-03

## Direction

LedgerFlow should treat the cluster as the product entry point:

1. A user enters a seed address.
2. Cluster discovery expands related addresses with evidence and confidence.
3. Cluster analysis explains what the actor does, how well it performs, and which routes/protocols/addresses matter.
4. A constrained report agent can answer custom questions from precomputed reports, not from raw RPC or traces.

The main product is not a generic chain analytics explorer. The target user is a bot operator, bot developer, or strategy researcher. They need targeted reports that answer:

- What is this address or cluster doing?
- Is it an arbitrage, liquidation, LP, position, collection, deployer, or mixed bot cluster?
- How much did it make?
- How much gas did it spend?
- What is the success rate and failure cost?
- Which protocols, token pairs, and routes does it use?
- Which addresses are executors, collectors, funders, contracts, or related operators?
- Which transactions are representative examples?

## Product Shape

The recommended product surface is:

- Overview: actor identity, primary behavior, top KPIs, status, sample evidence.
- PnL and Cost: estimated PnL, gas, failure gas, ROI/profit-per-gas, distribution.
- Strategies: arbitrage/liquidation/LP/position mix and trend.
- Arbitrage Report: route stats, protocol stats, token pairs, success rate, gas efficiency, samples.
- Liquidation Report: lending protocols, collateral/debt paths, success rate, PnL, post-liquidation route samples.
- Protocols: protocol contribution, dependency, and concentration.
- Routes: recurring paths, token pairs, success rate, PnL, sample tx hashes.
- Addresses: configured and inferred roles, evidence, tx share, PnL contribution.
- Ask: constrained report Q&A over precomputed report tables.

## Layering

### 1. Raw Data Layer

Input from indexer/backfill/trace services:

- transactions
- receipts
- logs
- token transfers
- decoded swap/liquidation/flashloan events where available
- traces when explicitly enabled
- token prices and gas prices

This layer is expensive and should not be queried directly by pages or agents.

### 2. Classified Fact Layer

Each transaction should produce structured facts:

- behavior tag: arbitrage, liquidation, LP, position, unknown
- strategy subtype: e.g. dex_dex_arbitrage, triangular_arbitrage, aave_liquidation
- protocol family/name
- token in/out
- amount in/out
- estimated value, gas, PnL
- route/path summary
- success/failure signal
- address role hint
- sample evidence refs

Current MVP input is `classified_transactions`. Future work should introduce richer `tx_strategy_facts` without breaking the current API.

### 3. Report Aggregation Layer

Cluster reports should be generated from classified facts and written/read as structured report objects:

- cluster behavior profile
- financial summary
- performance summary
- strategy breakdown
- arbitrage report
- liquidation report
- route stats
- protocol stats
- address role stats
- representative transactions

For performance, expensive aggregations should eventually be materialized by time bucket:

- cluster_id
- window_start/window_end
- behavior
- protocol
- route
- token pair
- address

### 4. API and UI Layer

The API should return typed report contracts. The web UI should render report-specific charts/tables instead of a generic transaction dump.

### 5. Constrained Agent Layer

The agent is a report interaction layer, not the analysis engine.

Allowed:

- query precomputed report APIs/tables
- filter by cluster, address, time window, strategy, protocol, route
- summarize report evidence and sample transactions

Not allowed by default:

- trigger RPC
- trigger backfills
- scan raw traces
- run unbounded SQL
- access secrets

Every agent query must have:

- cluster/address scope
- time window
- row limit
- query cost budget
- allowed table/API list

## Performance Boundaries

Do not compute heavy reports at request time once data volume grows. The target design is:

1. Ingestion writes raw data.
2. Classifier writes transaction-level facts.
3. Report worker incrementally refreshes materialized report tables.
4. API reads bounded report rows.
5. UI renders report payloads.
6. Agent reads the same report payloads with strict limits.

Initial MVP can aggregate from `classified_transactions` in memory for a cluster window, but should keep the API contract close to the materialized design so migration is straightforward.

## Development Phases

### Phase 1: Typed Cluster Report Contract

Goal: upgrade the current cluster strategy analysis API from generic stats to bot-oriented report sections.

Deliverables:

- cluster behavior profile
- bot performance summary
- arbitrage report
- liquidation report
- representative transactions
- tests using in-memory classified transactions

No RPC, Docker, or backfill required.

### Phase 2: Richer Strategy Facts

Goal: preserve more transaction-level structure for reports.

Deliverables:

- `tx_strategy_facts` design
- route/path normalization
- strategy subtype taxonomy
- protocol family/name split
- success/failure reason model

### Phase 3: Materialized Report Tables

Goal: move large aggregations off request path.

Deliverables:

- report table schemas
- incremental refresh worker
- backfill-safe refresh windows
- API reads from materialized tables where available

### Phase 4: Report UI

Goal: make cluster reports the primary UI experience.

Deliverables:

- overview KPI cards
- arbitrage charts/tables
- liquidation charts/tables
- route/protocol/address drilldowns
- sample transaction links

### Phase 5: Constrained Report Agent

Goal: custom natural-language reports over bounded precomputed data.

Deliverables:

- report query DSL
- allowlisted report APIs/tables
- query planner with limits
- report answer renderer with cited data rows

## Open Questions

- Is `estimated_pnl_usd` net of gas or pre-gas? The UI/API should label this precisely.
- Which protocols need first-class liquidation parsing first: Aave, Morpho, Compound, or others?
- Should route identity be token-symbol based, address based, or both?
- What time windows should be default: 24h, 7d, 30d, all time?
- Should failed transactions include reverted txs only, negative PnL txs, or both?
