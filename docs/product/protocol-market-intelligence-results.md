# LedgerFlow Protocol Market Intelligence — Results

Date: 2026-09-08  
Runtime: Robinhood Chain production data plus the existing Base public Evidence Pack feed  
New web: `http://127.0.0.1:3001`  
Legacy workspace web: stopped and retained under the opt-in `legacy-web` Compose profile  
Base web retained: `http://127.0.0.1:3201`

## Outcome

This slice creates a separate, bilingual LedgerFlow market product without replacing the existing web or changing the production Searcher. It changes the product hierarchy from “configure an entity monitor” to “see ranked market evidence first”:

1. protocol-native launch intelligence;
2. protocol-scoped participant clusters;
3. deterministic arbitrage and liquidation economics.

The Barker reference informed the information hierarchy: windowed totals, ranked comparable rows, and visible data freshness precede configuration. Its LP product and claims were not copied.

## Data flow

```text
official protocol source
  -> versioned JSON manifest
  -> seed importer
  -> intelligence source/dataset registry
  -> protocol intelligence domain + protocol seed library

targeted canonical factory logs (production worker)
  -> launch records
targeted Pons curve topics + direct ERC-20 calls (same LedgerFlow worker)
  -> curve buy/sell facts + token metadata
  -> protocol-only relationship edges
  -> protocol-scoped clusters
  -> price/flow/cohort snapshots
  -> verified participant outcomes
  -> Launch Radar and Address Intelligence

existing deterministic execution evidence
  -> Robinhood economic_verification_results
  -> Base public Evidence Packs
  -> Arbitrage and Liquidation pages
```

Global address similarity does not promote a member into a protocol cluster. Router, pool, factory, and protocol-contract roles are prevented by a database constraint from entering the controlled economic scope.

## New routes

| Route | Purpose | Data state |
|---|---|---|
| `/` | Market totals, freshness, ranked launch/execution evidence | Real execution data; launch cells fail closed |
| `/launches` | Launch/graduation lifecycle plus price movement, flow velocity, capital origin and outcomes | Real lifecycle rows; price/flow remain unavailable until market facts arrive |
| `/launches/:id` | Token lifecycle, price, flow, cohorts, scoped relationships and limitations | Real Pons lifecycle projection; downstream metrics pending |
| `/clusters` | Address lookup across protocol roles and category-filtered protocol-scoped clusters | Real Pons issuer/buyer and verified-arbitrage clusters; Token, Arbitrage, Liquidation and Other categories derive from database domain kinds |
| `/clusters/:id` | Cluster members, interval earnings, roles, edges and supporting transactions | Real database evidence; candidate roles remain explicit |
| `/arbitrage` | Per-asset interval earnings ranking and deterministically verified executions | Robinhood real data available; unlike assets and native gas are not combined |
| `/api/market/arbitrage-rankings` | Machine-readable interval earnings ranking | Uses transaction occurrence time; includes cluster link, exact raw values and verification coverage |
| `/liquidations` | Verified liquidation evidence | Base public feed available |
| `/seed-library` | Provenance-bearing protocol contracts | 8 active Pons domain seeds |
| `/auth/wallet/*` | Nonce, EIP-191 signature verification, session, logout | Implemented; no email or payment flow |
| `/health`, `/ready`, `/metrics` | Runtime operations | Live |

All routes accept `lang=en|zh`, `chain_id=4663|8453`, and a supported window. The header exposes both Robinhood Chain and Base. Wallet authentication accepts an EVM signature regardless of the wallet's currently selected chain, so a user on Ethereum mainnet is not blocked from signing in.

## Database migration

Migration `0051_protocol_market_intelligence_v1` adds only additive tables:

- `protocol_intelligence_domains`
- `protocol_seed_addresses`
- `protocol_cluster_runs`
- `protocol_scoped_clusters`
- `protocol_scoped_cluster_members`
- `protocol_scoped_cluster_edges`
- `protocol_token_launches`
- `protocol_launch_metric_snapshots`
- `protocol_launch_participant_outcomes`
- `market_wallet_accounts`
- `market_wallet_challenges`
- `market_wallet_sessions`

The migration is applied successfully in the Robinhood runtime database. Existing entity, observation, monitor, investigation, event, and authentication tables remain intact.

Migration `0052_protocol_launch_indexer_v1` adds lifecycle columns to `protocol_token_launches`, the deduplicated `protocol_launch_lifecycle_events` audit table, and the database seed role `launch_factory`. Migration `0053_protocol_launch_trade_facts_v1` adds LedgerFlow-direct Pons curve trade facts and bounded metadata-enrichment state. Migration `0054_protocol_cluster_projection_v1` adds incremental cluster projection checkpoints, and `0055_market_address_lookup_indexes` keeps address queries bounded as fact volume grows. All changes use SQLx migrations; no schema is changed manually.

## Seed library

The versioned manifest `config/intelligence/robinhood-market-protocols.v1.json` imports:

- Pons V1 and Pons V2 as separate analytical domains;
- 2 launch-factory addresses;
- the documented locker, DEX factory, position manager, router, quoter, and WETH dependency addresses;
- source URL, confidence, role, evidence and optional validity block for every row.

Import result: **2 domains, 8 domain seeds, 8 registry upserts**. Re-import keeps one row per `(domain, address, role)`. The general seed pipeline may additionally materialize the same address under another legitimate seed kind; the domain seed library remains deduplicated at 8.

Pons V2 live validation over blocks `58,000,000..58,004,999` decoded **262 TokenLaunched**, **3 LaunchSwept**, and **3 PoolGraduated** events. Repeating the same range retained exactly **268 distinct lifecycle keys**, proving idempotent projection. These are on-chain facts, not fixture rows.

The same range was validated through LedgerFlow's direct RPC curve adapter on 2026-09-09. It observed **7,127** exact-topic curve logs and retained **5,525** factory-proven Pons V2 curve facts: **3,171 buys** and **2,354 sells**, covering **168 launches**. Some unprojected emitters were confirmed to be legitimate Pons curves whose earlier factory launch events have not yet been reached by the lifecycle cursor; they remain fail-closed rather than being attributed without factory evidence. Metadata enrichment returned real ERC-20 names/symbols for the tested tokens. Curve history has an independent durable cursor so it can revisit historical ranges after lifecycle mappings exist without creating a silent trade-history gap.

The live incremental cluster projection on 2026-09-09 produced **128 Pons V2 issuer cohorts with 683 members** and **17 verified-arbitrage clusters with 40 members/nodes**. Twenty-three addresses belonged to more than one protocol-scoped cluster, which is why address lookup returns a list rather than forcing one global identity. Pons V1 currently has no qualifying direct curve-trade facts and therefore correctly projects zero clusters.

## Truth state at acceptance

- Robinhood live indexer: block `57,741,548` with chain head `57,741,550`; lag 2 blocks.
- Base live indexer: block `51,043,158` with chain head `51,043,161`; lag 3 blocks.
- Robinhood arbitrage signals in the current 24-hour query: approximately 117k; these are labeled **detected**, not profitable.
- Robinhood deterministic economic archive: **441 verified executions across 23 subjects**.
- Latest Robinhood verified execution: `2026-08-25 18:15 UTC`; the UI shows that timestamp rather than implying freshness.
- Launch lifecycle records are live. Price/flow snapshots are still absent, so price movement and participant outcomes render unavailable rather than `0`.
- Base liquidation page reads the existing public Evidence Pack feed. No private RPC URL or customer data is rendered.

## Authentication and access

The new web has no email signup/login route or email copy. Public market evidence is browsable without authentication. Connect Wallet performs:

```text
request account
  -> server-generated one-time challenge
  -> personal_sign (EIP-191)
  -> recovered-address verification
  -> hashed, HttpOnly, SameSite session cookie
```

Challenges are single-use and expiring. V1 has no checkout, token transfer, allowance, payment, or billing code.

## Performance and runtime checks

Warm-cache smoke measurements on the deployed container:

| Route | HTTP | Time |
|---|---:|---:|
| `/` | 200 | 284 ms |
| `/?lang=zh` | 200 | 131 ms |
| `/launches` | 200 | 3 ms |
| `/clusters` | 200 | 7 ms |
| `/arbitrage` | 200 | 8 ms |
| `/liquidations?chain_id=8453` | 200 | 8 ms |
| `/seed-library` | 200 | 4 ms |
| `/health` | 200 | 2 ms |
| `/ready` | 200 | 2 ms |

The Base public feed is warmed asynchronously and cached. A cold upstream request remains bounded by the 8-second HTTP timeout and fails closed with an upstream error instead of substituting data from another chain.

## Automated verification

- **PASS** — `cargo fmt --check`
- **PASS** — `cargo check --bin ledgerflow_market_web --bin ledgerflow_market_seed_import`
- **PASS** — `cargo test --all-targets`: 148 library tests plus all integration/binary target tests passed; no failures.
- **PASS** — wallet EIP-191 valid-signature and malformed-signature unit tests.
- **PASS** — manifest rejects duplicate/global domain keys and validates normalized addresses.
- **PASS** — CSS regression rejects font sizes below 12px.
- **PASS** — `docker compose config --quiet`.
- **PASS** — release Docker image built with both new binaries.
- **PASS** — Pons V1/V2 lifecycle and CurveBuy/CurveSell ABI parser tests.
- **PASS** — optional post-graduation Searcher pool-fact client rejects queries without an exact pool scope.
- **PASS** — Pons clusters require repeated purchases across at least two launches from the same creator; single-launch buyers are excluded.
- **PASS** — verified-arbitrage clusters distinguish candidate workers/executors, deterministic settlement recipients and known external protocol contracts.
- **PASS** — address lookup returns every matching cluster plus seed/deployment protocol roles; one address may match multiple clusters.
- **PASS** — migration `0052` applied and a 5,000-block real-chain range projected twice without duplicates.
- **PASS** — migration `0053` applied; the 5,000-block real-chain range projected 5,525 direct curve facts with exact raw amounts and evidence coordinates.
- **PASS** — bounded direct metadata calls complete or record an explicit error without blocking the launch cursor indefinitely.
- **PASS** — `protocol-launch-indexer` is running with durable per-domain checkpoints; observed memory was about 6 MiB and CPU below 1% during historical scanning.
- **PASS** — the new service is running on 3001; the previous Robinhood web is stopped, and Base 3201 remains healthy.
- **PASS** — production `robinhood-arb-*` containers remained running and were not modified.

## Screenshots

- `docs/product/screenshots/protocol-market-overview-en.png`
- `docs/product/screenshots/protocol-launch-radar-zh.png`

## Acceptance status

| Capability | Status | Evidence / limitation |
|---|---|---|
| Existing web preserved | PASS | Separate binary, Compose service, cookie and port |
| LedgerFlow name and existing mark | PASS | Reused in header and favicon |
| English and Chinese presentation | PASS | Server-rendered locale switch across all new pages |
| Base and Robinhood selector | PASS | Both are selectable and use distinct data adapters |
| Email registration removed from new web | PASS | Wallet-only auth routes; old web remains unchanged |
| Wallet challenge/signature/session | PASS | Recovery test plus live challenge smoke test |
| Real-wallet browser end-to-end | PARTIAL | Requires a user's injected wallet; no transaction is requested |
| No billing in V1 | PASS | No payment code or paid gate exists |
| Protocol/domain-scoped schema | PASS | Domain FKs and economic-scope role guard |
| Database-backed seed library | PASS | 8 active Pons domain seeds with provenance |
| Market-first page design | PASS | Windowed metrics and ranked-data contract precede operations |
| Real Robinhood arbitrage display | PASS | 441 deterministic verified archive records available |
| Real Base liquidation display | PASS | Existing public Evidence Pack adapter |
| Pons launch event projector | PASS | V1 TokenLaunched and V2 TokenLaunched/LaunchSwept/PoolGraduated; real-chain validation |
| Historical launch catch-up | PARTIAL | Running asynchronously from database deployment blocks; current lifecycle data is partial until cursors reach safe head |
| Pons V1 graduation state | NOT IMPLEMENTED | V1 has no equivalent graduation event; deterministic factory state probe is still required |
| Pons V2 V4 PoolId mapping | PARTIAL | Graduation/position are known; no pool address is invented and PoolId derivation remains pending |
| LedgerFlow-direct Pons curve facts | PASS | CurveBuy/CurveSell are read from LedgerFlow RPC and admitted only for factory-proven curves |
| LedgerFlow-direct token metadata | PASS | Bounded symbol/name/decimals calls are cached; unavailable metadata stays explicit |
| Optional Searcher post-graduation pool facts | NOT IMPLEMENTED | Not required for launch/curve indexing; narrow exact-pool contract is documented |
| Launch price/flow snapshots | NOT IMPLEMENTED | No values displayed until deterministic snapshots exist |
| Issuer/external cohort attribution | NOT IMPLEMENTED | Schema and UI contract only |
| Protocol cluster production worker | PASS | Incremental projector persists Pons repeat-buyer and verified-arbitrage clusters with transaction evidence |
| Query-first address intelligence | PASS | Address lookup returns multiple cluster memberships, protocol roles and indexed activity; cluster details expose members and evidence |
| Participant winner/loser accounting | NOT IMPLEMENTED | Requires inventory-aware realized outcome projection |
| Watchlists/personalization after wallet login | NOT IMPLEMENTED | Wallet identity exists; product action intentionally deferred |

## Next implementation sequence

1. Project 5m/15m/1h/6h/24h pre-graduation price and flow directly from the stored Pons curve facts.
2. Add Pons V1 graduation state probes and deterministic Pons V2 V4 PoolId derivation.
3. Optionally consume Searcher's already-decoded swaps only after LedgerFlow knows the exact graduated pool; otherwise add a direct pool-log adapter.
4. Build deterministic protocol edges: creator-to-launch, buyer-to-launch, direct funding and settlement; score clusters inside the Pons domain only.
5. Add inventory-aware realized participant outcomes, with estimated and unknown separated from verified.
6. Add liquidation economics freshness for Robinhood and improve Base verified-profit coverage.
7. Add wallet-linked watchlists only after the public feed contains enough useful launch data.

This order spends compute on the selected protocol's factories, tokens, pools and participants. It does not resume a full-chain backfill or the former global clustering strategy.
