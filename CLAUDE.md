# CLAUDE.md

LedgerFlow is a Rust (edition 2024, Axum + SQLx/PostgreSQL + ClickHouse) on-chain
intelligence platform. This worktree (`/home/ubuntu/ledgerflow-chainactors-intelligence-v2`)
is the active one; `/home/ubuntu/ledgerflow` is an older `main` checkout.

## Current focus: Market MVP

The public product is the **market web** (`src/market_web/mod.rs`, binary
`ledgerflow_market_web`, Compose service `market-web`, host port 3001). It covers
Robinhood Chain (chain_id 4663) Pons launches, arbitrage, liquidations and
protocol-scoped address clusters with their earnings.

- Plan: `docs/product/market-mvp-plan.md`
- Acceptance report (update it with measured evidence after every milestone):
  `docs/acceptance/market-mvp-acceptance.md`
- Branch `feat/market-mvp`; tag each milestone `market-mvp-mN`.

## Hard constraints

- **Data sources: local RPC only** (`robinhood-nitro` at `127.0.0.1:8547`, containers
  go through `rpc-gateway`) plus LedgerFlow's own databases. No third-party price,
  indexer or explorer APIs.
- Never touch the production `robinhood-arb-*` containers.
- Market MVP schema changes go in `migrations_market/` (schema `market`, own
  `_sqlx_migrations` table) so binaries built from older code never meet unknown
  public migrations. Never edit an applied migration; add a new one. Public
  schema changes still need additive migrations in `migrations/`.
- Request handlers must read precomputed tables; never scan
  `protocol_launch_trade_facts` (24M+ rows, 46 GB) on a page request.
- Every displayed amount carries a trust label: verified / estimated / unknown.
  Never sum different assets unless both were converted to USD.
- Deploying to port 3001 or pushing to the remote needs explicit user approval.

## Key data (PostgreSQL `ledgerflow`, container `ledgerflow-chainactors-v2-db-1`, host port 5532)

- `protocol_token_launches` – Pons V1/V2 launches (V1 inactive since 2026-08-12).
- `protocol_launch_trade_facts` – exact Pons V2 CurveBuy/CurveSell facts.
- `price_observations` – USD prices derived from local chain executions (price-worker).
- `arbitrage_candidates`, `economic_verification_results` – arbitrage detection / verification.
- `protocol_scoped_clusters` / `_members` / `_edges` – protocol-scoped clusters.
- Worker cursors live in `onchain_event_cursors`.

Query quickly: `sudo -n docker exec -i ledgerflow-chainactors-v2-db-1 psql -U postgres -d ledgerflow -c "..."`

## Commands

```bash
cargo check --bin ledgerflow_market_web          # fast compile check
cargo test --lib market                          # market unit tests
cargo fmt && cargo clippy --all-targets
cargo test --all-targets                         # full suite before tagging
```

The running stack is the Compose project `ledgerflow-chainactors-v2`
(`sudo docker compose -p ledgerflow-chainactors-v2 ...`). Docker needs `sudo -n`.
Git commits need `-c user.name=peter -c user.email=wcy19960411@gmail.com`
(no global identity is configured).

## Code map (market-related)

- `src/bin/ledgerflow_market_projector.rs` – runs all market tasks (prices, candles,
  stats, positions/leaderboards, arbitrage estimates, pool swaps, liquidations);
  see `docs/operations/market-projector.md`.
- `src/services/market_data.rs`, `market_pnl.rs`, `market_arbitrage.rs`,
  `market_pools.rs`, `market_liquidations.rs` – the projector stages.
- `src/services/market_read.rs` – read queries for the web (never scan trade facts).
- `src/market_web/{ui,pages,intel,execution}.rs` – components and pages.
- `scripts/market_mvp_acceptance.sql` – reproducible acceptance checks.
- `src/market_web/` – server-rendered pages, inline CSS/JS, wallet auth, JSON APIs.
- `src/services/protocol_market.rs` – read models used by the web + seed import.
- `src/services/protocol_launch_indexer.rs` – Pons factory/curve log ingestion via RPC.
- `src/services/protocol_market_projector.rs` – legacy per-minute launch snapshots.
- `src/services/protocol_cluster_projector.rs` – Pons issuer cohorts, arbitrage clusters.
- `src/services/economic_verifier.rs` – deterministic arbitrage verification.
- Operations docs: `docs/operations/protocol-*.md`.
