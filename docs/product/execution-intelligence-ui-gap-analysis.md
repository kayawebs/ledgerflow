# LedgerFlow Execution Intelligence UI v0.1 — Gap Analysis

Status: implementation baseline, 2026-08-12

## 1. Frontend architecture

LedgerFlow Web is a Rust/Axum server-rendered application. It does not contain a
Node, React, TypeScript, Vite, Playwright, Cypress, or Storybook application.
The UI, typed view models, route handlers, HTML rendering, and design tokens are
currently concentrated in `src/web/mod.rs`.

- Framework: Axum 0.8 + Tokio.
- Rendering: server-side HTML strings returned as `Html<String>`.
- UI library: no external component library. Reusable Rust render functions are
  the component boundary.
- State/query management: URL query structs plus server-side PostgreSQL/API
  loading. No client-side store or query hooks.
- API client: one pooled `reqwest::Client`; chain routing is resolved by
  `src/web/chain_registry.rs`.
- Design system: `shared_styles()` in `src/web/mod.rs`; the active product theme
  is the dark terminal palette with purple emphasis, dense cards, tables, pills,
  empty states, and responsive breakpoints.
- Authentication: `ledgerflow_session` cookie backed by `app_user_sessions`.
- Production data path: PostgreSQL, Analysis API, Indexer API, RPC gateway, and
  the configured external ClickHouse endpoint for bounded swap research.

The requested “typed frontend model” will therefore be implemented as Rust
enums/structs and explicit adapters. Introducing a parallel TypeScript SPA would
replace the framework and is outside the requested scope.

## 2. Current route map

### Product Web routes

| Current route | Current purpose | Current data |
|---|---|---|
| `/app/today` | Today intelligence dashboard | PostgreSQL aggregates, candidates, actors, protocols; API/RPC/indexer health |
| `/app/arbitrage` | Arbitrage candidates | `arbitrage_candidates` and pool/route tables |
| `/app/tx-analysis` | Transaction analysis/case view | Analysis API and evidence pack |
| `/app/address-analysis` | Address profile | Analysis API, Indexer API, address jobs |
| `/app/clusters` | Cluster creation/management | `app_user_clusters`, Analysis API |
| `/app/clusters/{id}` | Cluster analysis | coverage, tracking, transactions, metrics, alerts, strategy API |
| `/app/bots` | Automated-actor review queue | behavior review API |
| `/app/watchlist` | User watchlist | `user_watchlist_entries` |
| `/app/alerts` | Cluster alert inbox | `cluster_alerts` scoped through user clusters |
| `/app/cluster-graph` | Standalone relationship graph | classified cluster transactions |
| `/app/search` | Global search | clusters, labels, tokens, transactions, protocols |
| `/app/indexer/progress` | Indexer status | Indexer API |
| `/app/indexer/address-lookup` | Raw address transaction lookup | Indexer API |
| `/app/indexer/api-keys` | API key management | Indexer API |
| `/share/tx/{id}` | Public transaction analysis | `tx_analysis_shares` |

Legacy `/dashboard*` routes also exist and currently redirect or reuse handlers.

### Existing backend API routes used by Web

| API area | Routes/data |
|---|---|
| Analysis | `/analyses/tx`, `/analyses/{tx_hash}`, `/evidence-packs/{pack_id}` |
| Address intelligence | `/addresses/summary`, `/addresses/intelligence`, enqueue endpoint |
| Clusters | CRUD, coverage, backfill, tracking, classified transactions, metrics, strategy analysis, alerts |
| Behavior review | list and decision endpoints |
| Pilot monitoring | events, Evidence Pack, delivery history, latest weekly report |
| Health | `/health`, `/ready`, `/metrics` |
| Indexer | `/progress`, address tx refs/hashes, API keys |

## 3. Current data interface map

| Source | Real fields available | Important limitation |
|---|---|---|
| `arbitrage_candidates` | tx hash, block/time, sender, entry contract, score, swaps, pools, route, confirmation string, optional raw profit | Production records are `candidate` or `analyzed_multi_pool_unconfirmed`; these are not verified arbitrage conclusions |
| `transaction_analysis` | deterministic transaction/receipt-derived payload, status, addresses, timestamps, Evidence Pack linkage | Not every candidate has an analysis row; absent values must remain unknown |
| `pilot_events` | event UUID/type/severity/conclusion/Evidence Pack | Production Robinhood database currently has no rows; Base pilot data is isolated |
| `cluster_alerts` | severity, alert type, title/message, metric JSON, time | Production user clusters currently have no alerts |
| `user_watchlist_entries` | addresses, tx hashes, cluster IDs and labels | No rule/destination workflow |
| Address intelligence | labels, behavior, portfolio, recent tx refs, job coverage | Identity assertions are confidence-scoped and must not be promoted to facts |
| Cluster APIs | user-owned cluster, tracking, activity, metrics, strategy, relationships | “Cluster” is evidence-based grouping, not proof of a real team |
| Health/indexer/RPC | sync head, lag, worker cursors and online state | Best-effort probes can be unavailable independently |

No production UI mock data was found. Existing empty states generally avoid
fallback values, although the information hierarchy emphasizes internal
inventory metrics and candidates.

## 4. Reusable components and logic

- `render_app_sidebar`, `render_global_topbar`, and `render_app_workspace_page`
  provide the shared shell.
- Dark theme tokens and `ca-*` card/table/pill styles can be retained.
- `render_notice_block`, `render_error_block`, `render_json_details`, copyable
  code, relative time, confidence/status pills, and HTML escaping are reusable.
- `TodaySnapshot` cache already provides fast shared loading and background
  refresh.
- Transaction analysis already contains conclusion, economic accounting,
  storyline/timeline, participants, evidence, limitations, and share/export
  primitives.
- Address and cluster pages already contain profile, behavior, relationships,
  recent activity, tracking, and alert mechanics.
- Watchlist persistence can back “Monitored Entities”.
- Pilot project/rule/destination/report tables can be shown when present, but
  they cannot be assumed to exist in the active production workspace.

## 5. New-to-old navigation map

| New navigation | Existing capability reused |
|---|---|
| Overview / Execution Overview | `/app/today` snapshot and health data |
| Live Events | arbitrage candidates, pilot events when present, review queue |
| Monitors | watchlist, clusters/tracking, cluster alerts; lightweight UI rule/destination persistence |
| Investigations | transaction analysis plus new lightweight investigation records |
| Entities | address analysis, clusters, behavior reviews, protocol registry, contextual graph |
| Reports & Research | cluster strategy reports, pilot weekly reports, bounded public research links |
| API | existing Indexer API key and status surfaces |

Old terms are mapped as follows: Actors → Entities/Addresses; Teams →
Entities/Entity Clusters; Bots → Entities/Searchers; Candidates → Live
Events/Review Queue; Cases → Investigations; Watchlist + Alerts → Monitors;
Graph → contextual Relationships; Pro Research → Reports & Research.

## 6. Missing data and capabilities

- No canonical cross-source `ExecutionEvent` read model.
- No user-facing normalized event lifecycle. Backend-specific statuses need a
  fail-closed adapter to Detected/Verified/Investigating/Resolved/Dismissed.
- No persisted investigation model.
- No user-scoped monitor rule/destination model outside Base-only pilot config.
- No workspace domain model for Web. The first implementation must use a typed
  Global Explorer / user cluster abstraction and URL state.
- Candidate rows generally lack confirmed economic impact, protocol fee,
  rebate, flashblock timing, and net profit. UI must show Unknown or omit.
- No owner/comment model for investigations.
- No generic event-status mutation endpoint; only investigation creation and
  dismissal/review workflows that already exist can alter state.
- No browser test framework. Route/component behavior is tested through Rust
  unit/handler-source tests and authenticated HTTP smoke tests.

## 7. Files expected to change

- `src/web/mod.rs`: typed models/adapters, routes, loaders, render components,
  actions, redirects, tests, and incremental styles.
- `migrations/0033_execution_intelligence_workspace.*.sql`: lightweight investigation,
  monitor-rule, and notification-destination persistence.
- `README.md`: new IA, routes, and startup notes.
- `docs/product/execution-intelligence-ui-gap-analysis.md`: this audit.
- `docs/product/execution-intelligence-ui-results.md`: acceptance report.
- `docs/product/screenshots/execution-*.png`: verified production-like captures.

## 8. Explicitly unchanged modules

- Chain indexer, RPC gateway, Searcher, trace detection, decoding, accounting,
  pricing, classifiers, protocol recognizers, Base pilot workers, Telegram
  delivery worker, and report generator.
- Existing Analysis, Indexer, and Pilot API response contracts.
- Authentication/signup policy, billing, tenancy, and permissions.
- Existing public transaction share links.
- Existing reliable address/cluster/transaction analysis implementation; these
  are reorganized and linked, not rewritten.

## 9. Implementation order

1. Route/navigation restructuring.
2. Typed Workspace and ExecutionEvent adapters.
3. Execution Overview.
4. Execution Event detail.
5. Live Events and Review Queue.
6. Entities merge.
7. Monitors merge and four-step creation flow.
8. Investigations.
9. Reports & Research.
10. Redirects, tests, screenshots, and acceptance report.

The implementation will remain fail-closed: current production arbitrage rows
are displayed as `Detected / Unverified`; no profit, identity, protocol, or
purpose is inferred from absent evidence.
