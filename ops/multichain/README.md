# Multi-Chain Deployment

This runbook describes the target deployment from
[Multi-Chain Architecture](../../docs/multichain_architecture.md). It is a
rollout specification, not a command to start the current stack. Do not switch
production to this topology until the code phases in that document are
complete.

## Recommended Topology

Use Docker Compose per server, immutable images, and a private server network.

```text
central-01
  reverse proxy or frpc -> web-bff:3001
  web-bff
  control-postgres

chain-base-01
  private ingress -> chain-api:3000
  chain-api
  chain-postgres
  chain-clickhouse
  rpc-gateway -> Base node
  indexer and workers

chain-arbitrum-01
  private ingress -> chain-api:3000
  chain-api
  chain-postgres
  chain-clickhouse
  rpc-gateway -> Arbitrum node
  indexer and workers
```

Use one Compose project name per server:

```text
ledgerflow-control
ledgerflow-base
ledgerflow-arbitrum
```

Do not run one cross-host Compose project. Compose dependencies and health
checks are host-local and should remain aligned with the failure boundary.

## Network

Recommended:

1. Create a WireGuard/Tailscale/private-cloud network for all LedgerFlow
   servers.
2. Bind each Chain API to its private interface or localhost behind a private
   reverse proxy.
3. Allow Chain API ingress only from central control-plane addresses.
4. Keep PostgreSQL, ClickHouse, indexer, RPC gateway, and worker ports
   unpublished.
5. Publish only central `web-bff:3001` through the existing public reverse
   proxy or `frpc`.

Example private names:

```text
ledgerflow-control.internal
ledgerflow-base.internal
ledgerflow-arbitrum.internal
```

The public DNS has one application origin, for example:

```text
app.ledgerflow.example -> central reverse proxy/frps -> web-bff:3001
```

Do not create public subdomains for chain RPC or database services.

## Image Build and Release

Build once in CI and push an immutable tag:

```text
ghcr.io/<owner>/ledgerflow:<git-sha>
```

Do not build from source independently on every production server. Record the
same image digest in the central and chain deployment manifests.

Recommended release flow:

1. Run static checks and tests in CI.
2. Build and scan the runtime image.
3. Push the commit-SHA tag.
4. Deploy control database migrations if required.
5. Deploy data-plane migrations to one chain canary.
6. Update Base, observe, then update remaining chains one at a time.
7. Update the central BFF after all required Chain API versions are available.

Rollback uses the previous image digest. Database migrations must be
backward-compatible for at least one release so the previous binary can run
during rollback.

## Central Server Configuration

The central server receives:

```text
LEDGERFLOW_ROLE=control
LEDGERFLOW_CONTROL_DATABASE_URL=<secret>
LEDGERFLOW_WEB_BIND_ADDR=0.0.0.0:3001
LEDGERFLOW_WEB_BASE_URL=https://app.ledgerflow.example
LEDGERFLOW_CHAIN_REGISTRY_SOURCE=database
LEDGERFLOW_CHAIN_API_CONNECT_TIMEOUT_MS=1000
LEDGERFLOW_CHAIN_API_READ_TIMEOUT_MS=5000
LEDGERFLOW_CHAIN_SUMMARY_TTL_SECONDS=90
```

It also receives mail and session secrets. It must not receive:

```text
LEDGERFLOW_RPC_URL
LEDGERFLOW_INDEXER_RPC_URL
LEDGERFLOW_INDEXER_CLICKHOUSE_URL
any chain PostgreSQL URL
```

Chain API credentials should be injected by the secret manager and selected by
`chain_id`.

## Chain Server Configuration

Address seeds are not loaded implicitly by any service. Apply the data-plane
migrations, then import a reviewed versioned intelligence dataset as described
in [Address Intelligence V2](../../docs/address_intelligence_v2.md). Each
dataset declares exactly one immutable `chain_id` and must be imported only
into that chain's PostgreSQL database.

Every chain server receives one immutable identity:

```text
LEDGERFLOW_ROLE=chain
LEDGERFLOW_CHAIN_ID=8453
LEDGERFLOW_CHAIN_SLUG=base
LEDGERFLOW_DATABASE_URL=<local chain PostgreSQL secret>
LEDGERFLOW_INDEXER_CHAIN_ID=8453
LEDGERFLOW_INDEXER_CLICKHOUSE_URL=http://clickhouse:8123
LEDGERFLOW_RPC_URL=http://rpc-gateway:8545
LEDGERFLOW_INDEXER_RPC_URL=http://rpc-gateway:8545
```

The following IDs must agree at startup:

```text
LEDGERFLOW_CHAIN_ID
LEDGERFLOW_DEFAULT_CHAIN_ID
LEDGERFLOW_INDEXER_CHAIN_ID
trace/arbitrage monitor chain_id
```

The process should fail closed if they disagree.

Workers remain independently configurable. A new chain is initially deployed
with backfill, continuous watch, and trace monitor disabled. Enable workloads
only after the API, databases, RPC gateway, and chain identity checks pass.

## Chain Registry Onboarding

Create a disabled registry row first:

```sql
insert into chain_registry (
  chain_id,
  slug,
  display_name,
  native_symbol,
  explorer_base_url,
  api_base_url,
  indexer_api_base_url,
  enabled,
  capabilities,
  sort_order,
  config_version
) values (
  8453,
  'base',
  'Base',
  'ETH',
  'https://basescan.org',
  'https://ledgerflow-base.internal',
  'https://ledgerflow-base.internal/indexer',
  false,
  '{"address_index":true,"cluster_analysis":true,"asset_changes":true,"arbitrage_monitor":true}'::jsonb,
  10,
  1
);
```

Inject service credentials separately as
`LEDGERFLOW_CHAIN_API_BEARER_TOKEN_8453` and
`LEDGERFLOW_CHAIN_INDEXER_BEARER_TOKEN_8453`. Never write either token to
`chain_registry`.

Before enabling it:

1. Call the private `/v1/status` endpoint from the central server.
2. Confirm returned and configured `chain_id` match.
3. Confirm the service image/API versions are supported.
4. Confirm no database or RPC port is public.
5. Run a bounded read-only address query.
6. Confirm request IDs and metrics appear on both servers.
7. Enable only for operator accounts.
8. Enable for all users after the observation window.

## Database Deployment

Maintain separate migration commands for control and data schemas:

```text
ledgerflow migrate control
ledgerflow migrate chain
ledgerflow_indexer migrate
```

These commands are target interfaces to add during the code split. Until they
exist, do not run the current combined migration set against an empty central
database because it also creates chain-analysis tables.

Back up independently:

- central database: users, sessions, cluster definitions, shares, reports
- chain PostgreSQL: analysis and worker state for one chain
- chain ClickHouse: indexed block and address references for one chain

Restore tests must verify the database's expected `chain_id` before a chain
service starts.

## Service Management

Use one systemd unit around each host-local Compose project:

```text
ledgerflow-control.service
ledgerflow-chain-base.service
ledgerflow-chain-arbitrum.service
```

The unit should pin:

- Compose file path
- project name
- environment/secret file path outside the repository
- image digest or immutable tag

Deploy chain by chain. Do not restart all data planes simultaneously, because
that creates synchronized RPC and indexing bursts.

Recommended chain restart order:

1. database migrations
2. Chain API
3. live trace/arbitrage monitor with randomized startup delay
4. lazy indexer
5. address and price workers
6. historical/backfill workers last

## Observability

Attach these labels to every log and metric:

```text
service
role
chain_id
chain_slug
version
instance
```

Minimum alerts:

- Chain API unavailable from central
- reported chain ID differs from registry
- indexer/monitor lag exceeds chain threshold
- pending jobs grow continuously
- RPC gateway throttling or error rate rises
- PostgreSQL or ClickHouse disk exceeds threshold
- central summary snapshot is stale

The main page should display stale or unavailable chain summaries without
failing the whole response.

## `frpc`

Keep the existing host-level `frpc` service on the central server and map the
public application route to central `web-bff:3001`.

Do not add `frpc` mappings for:

- chain RPC gateways
- Chain API admin endpoints
- PostgreSQL
- ClickHouse
- indexer APIs
- trace monitor ports

Use the private network for central-to-chain traffic. This avoids turning the
public tunnel into both the user ingress and the internal service mesh.

## Rollout Checklist

- [ ] Web/BFF routes by explicit chain and has no direct chain SQL
- [ ] Chain API includes arbitrage, swap, cluster, address, and indexer reads
- [ ] Control and data migrations are split
- [ ] Global cluster and chain projection tables exist
- [ ] Chain ID mismatch fails startup or requests
- [ ] Private network and service authentication are configured
- [ ] Main-page summaries are cached and tolerate partial failure
- [ ] Base works through the BFF before adding another chain
- [ ] Second-chain failure does not affect Base or login
- [ ] RPC limits are configured and verified separately per chain
