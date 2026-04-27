# LedgerFlow

LedgerFlow is a Rust backend starter for evidence-first EVM transaction analysis, initially focused on Base (`chain_id=8453`). It is intentionally narrow in semantic coverage for v0, but the architecture is built so recognizers, protocol adapters, validators, and accounting policies can expand steadily without breaking the stable core schema.

It now ships with a separate `web` process that fronts the API with a product-shaped operator UI: homepage, pricing page, passwordless email auth, and a dashboard split across cluster management, cluster analysis, and single-tx analysis.

Passwordless auth supports real email delivery through Resend. If the web process starts without email credentials, it now stays in explicit log-only mode instead of silently pretending mail is enabled; the auth page and `/health` surface that status.

The current product surface is cluster-first: define an address cluster for a person, bot fleet, or wallet family, then compute spend summaries over the already analyzed transactions that touch that cluster.

The repository also ships a standalone `ledgerflow indexer` binary for top-level Base address indexing. It scans blocks, writes `from` / `to` address references into ClickHouse, and exposes an address-to-tx query API without depending on the analysis API.

If the indexer API is protected, set `LEDGERFLOW_INDEXER_API_BEARER_TOKEN` in the analysis service so cluster backfill requests include `Authorization: Bearer <token>`.

For operators using a self-hosted Base node that is bound only to `127.0.0.1:8545`, the recommended setup is a host-level nginx reverse proxy that exposes a Docker-only RPC entrypoint on `18545`. See [docs/node_rpc_proxy.md](docs/node_rpc_proxy.md).

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
- Provides email-code registration and login in the web process, with pluggable verification delivery.
- Includes a pricing surface set to `0.5 ETH / month`, while the current private beta remains free.

## What v0 does not do

- Full protocol semantics.
- State-diff-based accounting precision.
- Opportunity-level P&L correctness.
- ABI/source verification as ground truth.
- Universal transaction understanding.
- Fully automatic continuous cluster monitoring after onboarding.

## Quick start

1. Copy `.env.example` to `.env`. Docker Compose now reads `.env` as the runtime config file.
2. Start Postgres, the API, the backfill worker, and the web process:

```bash
docker-compose up --build db app worker web
```

3. Run checks locally:

```bash
make check
make test
```

For a practical operator walkthrough, read [docs/usage.md](docs/usage.md).

Default local endpoints:

- API: `http://127.0.0.1:3000` when run directly on the host, `http://127.0.0.1:8180` in Docker
- Web: `http://127.0.0.1:3001` when run directly on the host, `http://127.0.0.1:3000` in Docker
- Indexer API: `http://localhost:8090`

The intended exposure model is: web public, analysis API private. In local/dev this means the API stays on localhost or container-internal networking while operators use the web process.

## Standalone indexer

`ledgerflow indexer` is intentionally narrow. It does one job:

- scan top-level Base transactions
- emit one `address_tx_refs` row for `from`
- emit one `address_tx_refs` row for `to` when present
- store the rows in ClickHouse with a `direction`
- serve `GET /addresses/{address}/txs`
- serve `GET /addresses/{address}/tx-hashes`

Run it with Docker:

```bash
docker-compose up --build clickhouse indexer
```

Run it locally:

```bash
export LEDGERFLOW_INDEXER_CLICKHOUSE_URL=http://127.0.0.1:8123
export LEDGERFLOW_INDEXER_CLICKHOUSE_USER=ledgerflow
export LEDGERFLOW_INDEXER_CLICKHOUSE_PASSWORD=ledgerflow
export LEDGERFLOW_INDEXER_RPC_URL=http://127.0.0.1:8545
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

## API

- `POST /analyses/tx`
- `GET /analyses/{tx_hash}?chain_id=8453`
- `GET /evidence-packs/{pack_id}`
- `GET /recognizers`
- `POST /replay/tx`
- `POST /clusters`
- `POST /clusters/{cluster_id}/backfill`
- `GET /clusters/{cluster_id}/coverage`
- `GET /clusters/{cluster_id}/backfill-runs`
- `GET /clusters/{cluster_id}/spend-summary?start=...&end=...`
- `GET /health`

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
