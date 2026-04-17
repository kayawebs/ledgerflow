# LedgerFlow

LedgerFlow is a Rust backend starter for evidence-first EVM transaction analysis, initially focused on Base (`chain_id=8453`). It is intentionally narrow in semantic coverage for v0, but the architecture is built so recognizers, protocol adapters, validators, and accounting policies can expand steadily without breaking the stable core schema.

It now ships with a separate `web` process that fronts the API with a lightweight operator UI, so the stack can be presented as an external-facing product shape without entangling web delivery concerns with the analysis service.

The current product surface is cluster-first: define an address cluster for a person, bot fleet, or wallet family, then compute spend summaries over the already analyzed transactions that touch that cluster.

The repository also ships a standalone `ledgerflow indexer` binary for top-level Base address indexing. It scans blocks, writes `from` / `to` address references into ClickHouse, and exposes an address-to-tx query API without depending on the analysis API.

## What v0 does

- Ingests raw transaction, receipt, optional trace, and target bytecode evidence.
- Normalizes raw evidence into replayable execution facts.
- Derives basic native and ERC20 flows.
- Runs versioned recognizers over normalized facts instead of raw RPC payloads.
- Builds deterministic Evidence Packs with input digests and validation results.
- Persists analysis artifacts in PostgreSQL via SQLx.
- Exposes an Axum API for analysis, replay, evidence-pack fetch, recognizer listing, and health checks.
- Computes address-cluster spend summaries over a time window, separating gas burn, external outflows, internal transfers, and incoming transfers.

## What v0 does not do

- Full protocol semantics.
- State-diff-based accounting precision.
- Opportunity-level P&L correctness.
- ABI/source verification as ground truth.
- Universal transaction understanding.
- Automatic historical transaction discovery for every address in a cluster.

## Quick start

1. Copy `.env.example` to `.env` if needed.
2. Start Postgres and the app:

```bash
docker-compose up --build
```

3. Run checks locally:

```bash
make check
make test
```

For a practical operator walkthrough, read [docs/usage.md](docs/usage.md).

Default local endpoints:

- API: `http://localhost:8180`
- Web: `http://localhost:3000`
- Indexer API: `http://localhost:8090`

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
- `GET /clusters/{cluster_id}/spend-summary?start=...&end=...`
- `GET /health`
- `GET /addresses/{address}/txs?limit=1000&from_block=&to_block=&direction=from|to`
- `GET /addresses/{address}/tx-hashes?limit=1000&from_block=&to_block=&direction=from|to`

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
