# Multi-Chain Architecture

## Status

This document defines the target architecture for running LedgerFlow across
multiple EVM chains when each chain's data and RPC workload live on a separate
server.

The web BFF now resolves analysis and indexer upstreams through the
control-plane `chain_registry` table. Requests carry an explicit chain ID,
request ID, bounded connect/read timeouts, and an optional per-chain service
credential sourced from the deployment secret store. Legacy single-upstream
environment variables remain only as a zero-row development fallback.

The remaining migration boundary is the control database itself: existing
arbitrage SQL, cluster ownership joins, and the CryptoHouse diagnostics endpoint
still read legacy chain tables directly. Production must not move to the final
split topology until those reads are served by the versioned Chain API or a
central summary cache.

## Decision

Use a central control plane and one isolated data plane per chain.

```text
                         Public Internet
                               |
                        app.ledgerflow.io
                               |
                    +----------------------+
                    | Central control plane|
                    | Web + BFF            |
                    | Auth/control Postgres|
                    +----------+-----------+
                               |
                 private HTTPS / mTLS or VPN
            +------------------+------------------+
            |                  |                  |
     +------v------+     +-----v-------+    +-----v-------+
     | Base plane  |     | Arbitrum    |    | Other EVM   |
     | Chain API   |     | Chain API   |    | Chain API   |
     | PostgreSQL  |     | PostgreSQL  |    | PostgreSQL  |
     | ClickHouse  |     | ClickHouse  |    | ClickHouse  |
     | Workers/RPC |     | Workers/RPC |    | Workers/RPC |
     +-------------+     +-------------+    +-------------+
```

The central web process must never connect directly to a chain PostgreSQL,
ClickHouse, or RPC endpoint. It calls a versioned Chain API through its BFF.
This keeps chain failures and RPC pressure isolated and gives the browser one
stable public origin.

## Responsibilities

### Central control plane

The control plane owns:

- users, login sessions, verification codes, and product access
- the enabled-chain registry and chain capability metadata
- global cluster definitions and user ownership
- one chain projection record for every cluster analyzed on a chain
- saved reports, share links, UI preferences, and audit events
- cross-chain summary caches and asynchronous aggregation job state
- browser-facing HTML and `/api/v1/...` BFF routes

It does not own raw transactions, traces, logs, indexed blocks, chain-specific
analysis results, or RPC clients.

### Per-chain data plane

Every chain server owns:

- one immutable `chain_id` and one chain configuration
- the Chain API implementation
- chain analysis PostgreSQL
- chain indexer ClickHouse
- the RPC gateway and that chain's RPC/node connection
- lazy indexer, backfill, address, price, and trace/arbitrage workers
- chain-specific protocol metadata, token metadata, and pricing sources

A process in one data plane must reject a request whose `chain_id` does not
match its configured chain. This prevents a routing error from writing one
chain's data into another chain's database.

## Chain Registry

The control database is the source of truth for routing. Do not keep the
production registry only in environment variables.

Suggested fields:

| Field | Purpose |
| --- | --- |
| `chain_id` | Immutable EIP-155 chain ID and primary key |
| `slug` | Stable URL segment such as `base` or `arbitrum` |
| `display_name` | UI label |
| `native_symbol` | Native asset symbol |
| `explorer_base_url` | Transaction/address link base |
| `api_base_url` | Private Chain API URL |
| `api_audience` | Expected service-token audience |
| `enabled` | Whether the BFF routes user traffic to this plane |
| `capabilities` | Versioned JSON capability flags |
| `sort_order` | Main navigation order |
| `config_version` | Monotonic routing/config version |

Secrets and bearer tokens must live in the deployment secret store, keyed by
`chain_id`; they must not be stored in the registry table or returned to the
browser.

The current BFF reads service credentials from
`LEDGERFLOW_CHAIN_API_BEARER_TOKEN_<chain_id>` and
`LEDGERFLOW_CHAIN_INDEXER_BEARER_TOKEN_<chain_id>`. These values are applied
server-side after registry resolution and are never rendered into HTML.

Example capabilities:

```json
{
  "address_index": true,
  "cluster_analysis": true,
  "asset_changes": true,
  "arbitrage_monitor": true,
  "liquidation_analysis": false,
  "trace_analysis": true
}
```

The web UI hides unsupported features instead of sending requests that are
expected to fail.

## Chain API Contract

All data planes expose the same private, versioned HTTP contract:

```text
GET  /v1/status
GET  /v1/indexer/coverage
GET  /v1/addresses/{address}/summary
GET  /v1/addresses/{address}/transactions
POST /v1/analyses/transactions
POST /v1/clusters
GET  /v1/clusters/{chain_cluster_id}
PUT  /v1/clusters/{chain_cluster_id}
POST /v1/clusters/{chain_cluster_id}/backfills
GET  /v1/clusters/{chain_cluster_id}/coverage
GET  /v1/clusters/{chain_cluster_id}/asset-changes
GET  /v1/clusters/{chain_cluster_id}/strategy
GET  /v1/arbitrage/transactions
GET  /v1/arbitrage/pools
GET  /v1/arbitrage/routes
```

`GET /v1/status` is small and must not call RPC. It returns:

```json
{
  "chain_id": 8453,
  "service_version": "git-sha",
  "api_version": "v1",
  "config_version": 3,
  "capabilities": {},
  "database": "ok",
  "indexer": {
    "mode": "lazy",
    "last_observed_head": 123,
    "pending_requests": 0
  },
  "workers": {}
}
```

Every response includes `chain_id`. Every write accepts an idempotency key.
Long-running indexing and analysis requests return `202 Accepted` plus a job ID
instead of holding the BFF connection open.

The Chain API, not the central web process, reads chain PostgreSQL and
ClickHouse. Existing web-side arbitrage SQL and direct CryptoHouse queries must
move behind this API.

## Browser and BFF Routes

The selected chain is explicit in URLs:

```text
/chains/base/arbitrage
/chains/base/addresses/0x...
/chains/arbitrum/clusters/...
/api/v1/chains/8453/arbitrage/transactions
```

The BFF resolves the chain in the registry, checks user access and capability
flags, adds the internal service credential, then calls exactly one data plane.
The browser never receives a private Chain API URL or token.

Use these upstream controls:

- connect timeout: 1 second
- normal read timeout: 5 seconds
- report read timeout: 20 seconds
- bounded connection pool per chain
- circuit breaker after repeated failures
- response-size limits
- request IDs propagated to the data plane
- no automatic retry for non-idempotent requests

## Main Page Aggregation

The main page must not fan out expensive SQL or RPC-backed work on every page
load. Each data plane periodically produces a small summary snapshot, such as:

- indexed coverage and observed head
- transactions analyzed in the last hour/day
- confirmed arbitrage count and estimated profit
- hot pools and protocols
- worker lag and health

The control plane pulls or receives these snapshots every 30-60 seconds and
caches them. A page request reads only the central cache. Missing or stale
chains are marked degraded while healthy chains still render.

Cross-chain custom reports use an asynchronous fan-out job:

1. Central BFF creates a report job with a bounded chain set and cost budget.
2. One request is sent to each selected Chain API.
3. Results are persisted independently as they arrive.
4. The report returns partial results when one chain times out.
5. No cross-chain request can cause direct RPC calls without an explicit
   chain-side queued job and pressure budget.

## Cluster Model

An EVM address can be the same on multiple chains, but its behavior and
associations are chain-specific. Separate the user-facing cluster identity from
its chain analysis.

### Global cluster

Stored in the control database:

- `global_cluster_id`
- owner/user/team
- name and description
- seed addresses and manually managed membership intent
- created/updated timestamps

### Chain cluster projection

Stored as routing metadata in the control database, with analysis data in the
target data plane:

- `global_cluster_id`
- `chain_id`
- `chain_cluster_id`
- projection status and last successful sync time
- requested membership revision
- analyzed membership revision

The tuple `(global_cluster_id, chain_id)` is unique.

Creating a global cluster does not automatically start analysis on every chain.
The user selects chains. Central creates one projection per selected chain and
the data plane queues its own discovery/indexing work. Editing membership
increments the requested revision and tells the user that incremental analysis
is available; it does not automatically start a backfill.

## Data and Schema Isolation

Use one PostgreSQL and one ClickHouse database per chain server. Do not use a
single cross-server database cluster for the first multi-chain version.

Benefits:

- a hot chain cannot exhaust another chain's database connections
- schema migration and rollback can be performed one chain at a time
- backup/restore boundaries match chain ownership
- RPC and worker tuning remain chain-specific
- a chain can be moved to a larger server without changing public URLs

All existing analysis keys should retain `chain_id` even inside an isolated
database. It protects backups, exports, and accidental restores.

## Security Boundary

- Only the central web endpoint is public.
- Chain API, PostgreSQL, ClickHouse, indexer, and RPC gateway ports are private.
- Prefer WireGuard, Tailscale, or a private cloud network between servers.
- Use mTLS for Chain API traffic when the network is not fully private.
- Otherwise use short-lived audience-bound JWT service tokens with key
  rotation. A static per-chain bearer token is acceptable only as a migration
  step.
- Chain APIs must authenticate before reading request bodies for expensive
  operations.
- Apply user quotas in the control plane and RPC/job quotas again in the data
  plane.
- Keep RPC URLs, mail credentials, database passwords, and service tokens out of
  repository files and logs.

`frpc` should expose only the central web port. It should not publish chain
databases, ClickHouse, indexers, RPC gateways, or worker admin ports.

## RPC Pressure Isolation

Each chain data plane keeps its own RPC gateway and pressure budget. The
central control plane has no RPC URL.

Required controls per chain:

- one shared RPC gateway for all local workers
- global max in-flight and RPS limits at the gateway
- worker-specific concurrency and queue limits
- randomized worker startup delay to prevent restart bursts
- queued lazy indexing with block-level deduplication
- bounded backfill ranges and explicit backfill activation
- live monitors prioritized over historical jobs
- circuit breaking and exponential retry with jitter

Deploying a new chain starts with workers disabled. Enable read-only API health,
then the live monitor, then lazy indexer jobs, and finally optional historical
workloads.

## Versioning and Compatibility

Build one immutable image from a commit SHA and deploy that image to the control
and data planes. The image can contain several binaries, but each server starts
only its assigned role.

The BFF routes traffic only when:

- the Chain API major version is supported
- reported `chain_id` matches the registry
- required capability flags are present
- the chain is enabled

Database migrations are split into:

- control-plane migrations for users, registry, global clusters, shares, and
  summary cache
- data-plane migrations for transaction and analysis data
- ClickHouse migrations owned by the indexer

Do not let the web process run data-plane migrations during startup.

## Code Changes Required

### Phase 1: establish boundaries

1. Add a chain registry and router to the web/BFF state.
2. Make chain selection explicit in web and BFF routes.
3. Add `configured_chain_id` to the analysis API and reject mismatches.
4. Add version, capabilities, and non-RPC health data to Chain API status.
5. Move arbitrage, swap, and indexer-facing reads out of the web process.

### Phase 2: separate control data

1. Split auth/control migrations from chain-analysis migrations.
2. Replace `app_user_clusters -> address_clusters` local foreign-key ownership
   with global clusters and chain projections.
3. Keep saved reports and share snapshots in the control database.
4. Remove the web process's dependency on chain analysis tables.

### Phase 3: deploy the first split

1. Deploy Base as the first independent data plane.
2. Deploy the central control plane and route Base through the Chain API.
3. Verify that stopping Base leaves login and the main page available.
4. Verify that the central server has no RPC or chain database credentials.

### Phase 4: add a second chain

1. Deploy the same data-plane image with a different immutable `chain_id`.
2. Register the private endpoint and capabilities while disabled.
3. Run migration and bounded health validation.
4. Enable the chain for operators, then for users.
5. Validate partial failures, chain routing, and cluster projection isolation.

## Acceptance Criteria

The multi-chain architecture is complete when:

- the web server has no chain RPC, ClickHouse, or chain PostgreSQL credentials
- every user route includes or resolves an explicit chain
- the same transaction hash on two chains cannot collide
- one chain API cannot read or write another chain ID
- a stopped chain plane does not break login or other chains
- the main page performs no synchronous expensive cross-chain fan-out
- adding a chain requires configuration and deployment, not a code fork
- RPC concurrency is enforced independently on every chain server
