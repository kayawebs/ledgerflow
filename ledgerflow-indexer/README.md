# LedgerFlow Indexer

Standalone Base top-level transaction indexer and developer API.

This project is extracted from the main LedgerFlow analysis platform. It focuses on one narrow primitive:

- index top-level `from` / `to` address references for Base transactions
- answer address-to-transaction queries quickly
- expose a developer API suitable for bots, operators, analytics teams, and protocol projects
- provide product scaffolding for users, API keys, usage tracking, and Base ETH subscription payments

It does not attempt to replace the evidence-first transaction analysis engine. The analysis engine should call this service to discover candidate transaction hashes, then analyze those transactions separately.

## Reusing Existing ClickHouse Data

The current production-like data is in the parent LedgerFlow compose stack's ClickHouse volume. Do not create a new empty ClickHouse volume if you want to keep using already indexed blocks.

Start the existing ClickHouse service from the parent repository:

```bash
cd ..
sudo docker compose up -d clickhouse
```

Then run this service locally against that ClickHouse HTTP port:

```bash
cd ledgerflow-indexer
cp .env.example .env
cargo run
```

The important settings are:

```text
LEDGERFLOW_INDEXER_CLICKHOUSE_URL=http://127.0.0.1:8123
LEDGERFLOW_INDEXER_CLICKHOUSE_DATABASE=ledgerflow_indexer
LEDGERFLOW_INDEXER_WORKER_ID=base_mainnet_top_level
```

The table names remain `address_tx_refs` and `indexer_progress`, so the service can continue from the existing progress row.

## Public API Examples

```bash
curl -s http://127.0.0.1:8090/health | jq
curl -s http://127.0.0.1:8090/v1/chains/base/progress | jq
curl -s "http://127.0.0.1:8090/v1/base/address/0x1111111111111111111111111111111111111111/tx-hashes?limit=20" | jq
curl -s "http://127.0.0.1:8090/v1/base/address/0x1111111111111111111111111111111111111111/txs?direction=from&limit=20" | jq
```

Batch lookup:

```bash
curl -s http://127.0.0.1:8090/v1/base/addresses/txs \
  -H 'content-type: application/json' \
  -d '{
    "addresses": [
      "0x1111111111111111111111111111111111111111",
      "0x2222222222222222222222222222222222222222"
    ],
    "from_block": 30000000,
    "limit": 100
  }' | jq
```

## Admin API Examples

Start email verification:

```bash
curl -s http://127.0.0.1:8090/admin/auth/email/start \
  -H 'content-type: application/json' \
  -d '{"email":"operator@example.com"}' | jq
```

Verify email:

```bash
curl -s http://127.0.0.1:8090/admin/auth/email/verify \
  -H 'content-type: application/json' \
  -d '{"email":"operator@example.com","code":"123456"}' | jq
```

Create an API key after verification:

```bash
curl -s http://127.0.0.1:8090/admin/api-keys \
  -H 'content-type: application/json' \
  -d '{"email":"operator@example.com","name":"production"}' | jq
```

## Git Strategy

Keep this as a subdirectory in the current repository for now. Avoid a Git submodule at this stage. Submodules make cross-cutting migrations, local development, and CI unnecessarily brittle.

Once the API, billing model, and deployment boundary stabilize, split `ledgerflow-indexer/` into a standalone repository using subtree or history filtering.
