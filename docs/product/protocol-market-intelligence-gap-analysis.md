# LedgerFlow Protocol Market Intelligence — Gap Analysis

Date: 2026-09-08  
Scope: a new, separately deployed web product. The existing LedgerFlow public site and private workspace remain available and are not rewritten.

## Product decision

LedgerFlow should lead with answers that are valuable before a user configures anything:

1. Which launch participants have evidence-backed gains or losses?
2. Which newly launched tokens are rising or falling, and how quickly is capital arriving?
3. Is that capital associated with the issuer cohort or with external participants?
4. Which arbitrageurs and liquidators are producing verified economic results?

The product is therefore narrowed from global entity intelligence to three connected datasets:

- **Launch Intelligence**: protocol-native launches, price movement, flow velocity, participant outcomes, funding provenance, and issuer/external cohorts.
- **Protocol-scoped Clusters**: relationships inferred from one protocol's deterministic events. A Pons cluster is valid in the Pons domain only and is never promoted into a global ownership claim.
- **Execution PnL**: the existing deterministic arbitrage and liquidation evidence, with candidate and verified results visually separated.

The reference Barker page is effective because it opens with market totals and a dense ranked opportunity table, gives every figure a time window, and keeps configuration secondary. LedgerFlow will adopt that hierarchy, not Barker's LP product or copy.

## Audited current state

### Reusable

- Rust/Axum server-rendered web stack and the existing LedgerFlow SVG mark.
- PostgreSQL migrations and provenance-aware address intelligence registry.
- `protocols`, `protocol_deployments`, `intelligence_sources`, `intelligence_datasets`, and `intelligence_seed_addresses`.
- Robinhood Chain live indexer and targeted/lazy history mechanisms.
- `arbitrage_candidates` for detected execution candidates.
- `economic_verification_results` for deterministic arbitrage outcomes.
- Base public Evidence Pack feed, including liquidation records.
- Token metadata, transaction explanation, trace, receipt, logs, Evidence Pack, and pricing infrastructure.

### Runtime facts observed on 2026-09-08

- Existing public/workspace web: port `3001`.
- Base public/workspace web: port `3201`.
- Robinhood live indexer lag: 2 blocks at audit time.
- Robinhood arbitrage candidates: 2,127,449 total; 120,308 in the latest 24-hour query window.
- Robinhood deterministic economic verification: 441 verified executions across 23 distinct profit recipients/subjects.
- Latest verified Robinhood execution in that table: 2026-08-25. This is stale and must be shown as such.
- Base public evidence contains real liquidation records, but it is a separate runtime data source.
- No current Pons protocol deployment/seed rows existed in PostgreSQL.
- No launch-specific price/flow/cohort/outcome projection existed.

### Partially complete

- Existing global/searcher cluster tables are evidence-aware, but their scope is chain/address based, not protocol-domain based.
- Existing seed registry stores provenance but does not say in which analytical domain a seed is valid.
- Existing price observations support deterministic historical prices for known assets, not a launch-native price and flow time series.
- Existing authentication is email/password/code based. It remains in the old web only.

### Missing

- Protocol-domain identity and strict database constraints preventing cross-domain cluster leakage.
- Launch, launch metric snapshot, participant outcome, and issuer/external cohort models.
- Pons V1/V2 seed manifests and import path.
- Wallet challenge/signature sessions for the new web.
- Bilingual market-oriented pages.
- A continuously running Pons launch projector. The new schema and read path are delivered first; ingestion remains a separately measurable worker.

## New information architecture

```text
Market Overview / 市场总览
├── Launch Radar / 发币雷达
├── Smart Money / 赚钱地址
├── Arbitrage / 套利
└── Liquidations / 清算
```

The header contains network, time window, language, search, and **Connect wallet**. Public intelligence is browsable without login. A wallet session is required only for later watchlists and personalization. V1 has no payment or billing.

## Page contract

### Market Overview

- launches observed in the selected window;
- rising versus falling launches;
- participants with verified gains versus verified losses;
- detected versus verified arbitrage executions;
- verified liquidation records;
- freshness and source status adjacent to the figures;
- ranked launch and execution tables.

### Launch Radar

Each row is designed for comparison:

- token and launch age;
- price change over the selected window;
- 5m/15m/1h net flow velocity;
- unique buyers and sellers;
- issuer-cohort versus external-participant capital;
- verified winning and losing participants;
- cluster/evidence coverage.

Unavailable fields are rendered as `Not indexed` / `尚未索引`, never zero.

### Launch detail

1. Price and flow timeline.
2. Who made or lost money, with realized/estimated/unknown separated.
3. Capital origin: issuer cohort, known related cohort, external, unresolved.
4. Protocol-scoped issuer and buyer relationships with evidence transaction links.
5. Liquidity/graduation state and limitations.

### Smart Money

- ranks protocol-scoped participant clusters;
- shows the protocol domain on every record;
- labels relationships as observed/candidate/inferred/unverified;
- never says “same team” or “owned by” without external proof;
- does not analyze unrelated activity outside the selected domain.

### Arbitrage and Liquidations

- candidate counts are labeled detected, never profitable;
- verified records show exact asset, raw amount, gas, confidence, evidence time, and link;
- assets with different token addresses are never summed into one fake total.

## Protocol-scoped clustering contract

```text
Protocol seed/event
  -> domain-specific participant edges
  -> evidence-scored protocol cluster
  -> launch/execution records in that domain
  -> domain-specific economic outcome
```

Strong Pons signals include factory launch creation, creator/deployer parameters, same-launch buys, direct funding transfers, settlement transfers, and repeated participation across Pons launches. Similar gas settings, timing, or route fingerprints remain weak signals and cannot independently establish cluster membership.

Pool, router, factory, and protocol contracts can be relationship nodes but are database-constrained out of controlled-entity economic scope.

## Metric truth policy

- **Verified realized result** requires deterministic buy/sell or execution accounting with gas deducted and inventory/funding excluded.
- **Observed flow** describes transfers or swaps; it is not profit.
- **Estimated** values are visibly labeled and include method/version.
- **Unknown** includes an explanation and coverage gap.
- Participant winners and losers count only rows whose outcome status is `verified`.
- Rising/falling counts require two deterministic price observations in the same configured window.
- All market totals include a source, window, and `last_updated_at`.

## Files in this slice

- `migrations/0051_protocol_market_intelligence_v1.*.sql`
- `config/intelligence/robinhood-market-protocols.v1.json`
- `src/services/protocol_market.rs`
- `src/bin/ledgerflow_market_seed_import.rs`
- `src/market_web/mod.rs`
- `src/bin/ledgerflow_market_web.rs`
- `Dockerfile`, `docker-compose.yml`, `.env.example`, `src/lib.rs`, `src/services/mod.rs`
- tests and `docs/product/protocol-market-intelligence-results.md`

## Explicitly unchanged

- Existing `web` binary and all of its routes.
- Existing email authentication tables and flows (they are not exposed by the new web).
- Production Searcher logic.
- Existing global/searcher cluster history, which remains available for research but is not used as a protocol ownership claim.
- Indexer architecture and supported chains.

## Implementation order

1. Add additive protocol-domain and wallet-session migrations.
2. Add a provenance-bearing Pons seed manifest and importer.
3. Add read-only market query adapters over real tables.
4. Build the separate bilingual market web and wallet signature flow.
5. Add launch/cluster/arb/liquidation pages with honest partial states.
6. Test migration constraints, wallet verification, route isolation, and production build.
7. Deploy on a new port and verify that ports 3001/3201 remain unchanged.

