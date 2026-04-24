# LedgerFlow Indexer

Standalone Base top-level transaction indexer and developer API.

This project is extracted from the main LedgerFlow analysis platform. It focuses on one narrow primitive:

- index top-level `from` / `to` address references for Base transactions
- answer address-to-transaction queries quickly
- expose a developer API suitable for bots, operators, analytics teams, and protocol projects
- provide product scaffolding for users, API keys, usage tracking, and Base ETH subscription payments

It does not attempt to replace the evidence-first transaction analysis engine. The analysis engine should call this service to discover candidate transaction hashes, then analyze those transactions separately.

## Docs

- [docs/public_api.md](docs/public_api.md): full public API reference
- [docs/consumer_guide.md](docs/consumer_guide.md): how consumers should use the indexer as a discovery layer
- [docs/admin_and_billing.md](docs/admin_and_billing.md): dashboard auth, billing, quota, and usage model
- [docs/extraction.md](docs/extraction.md): extraction and storage notes

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
LEDGERFLOW_INDEXER_ENABLE_LEGACY_API=false
```

The table names remain `address_tx_refs` and `indexer_progress`, so the service can continue from the existing progress row.

Keep `LEDGERFLOW_INDEXER_ENABLE_LEGACY_API=false` for public deployments. The legacy `/addresses/...` endpoints are intended only for internal compatibility.

## Public API Examples

Create a verified user and API key first, then pass the key as a bearer token.

```bash
curl -s http://127.0.0.1:8090/health | jq
export LEDGERFLOW_API_KEY=lf_live_xxx
curl -s http://127.0.0.1:8090/v1/chains/base/progress \
  -H "Authorization: Bearer $LEDGERFLOW_API_KEY" | jq
curl -s "http://127.0.0.1:8090/v1/base/address/0x1111111111111111111111111111111111111111/tx-hashes?limit=20" \
  -H "Authorization: Bearer $LEDGERFLOW_API_KEY" | jq
curl -s "http://127.0.0.1:8090/v1/base/address/0x1111111111111111111111111111111111111111/txs?direction=from&limit=20" \
  -H "Authorization: Bearer $LEDGERFLOW_API_KEY" | jq
```

Batch lookup:

```bash
curl -s http://127.0.0.1:8090/v1/base/addresses/txs \
  -H "Authorization: Bearer $LEDGERFLOW_API_KEY" \
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

The customer dashboard is served from the same process:

```text
/               public landing page
/login          register, login, password reset
/dashboard      authenticated customer dashboard
/docs           public API docs
```

`/dashboard` requires a valid `ledgerflow_session` cookie. The login page supports password login, email-code registration, and password reset.

Start registration email verification:

```bash
curl -s http://127.0.0.1:8090/admin/auth/register/start \
  -H 'content-type: application/json' \
  -d '{"email":"operator@example.com"}' | jq
```

Complete registration and set a password:

```bash
curl -i -s http://127.0.0.1:8090/admin/auth/register/verify \
  -H 'content-type: application/json' \
  -d '{"email":"operator@example.com","code":"123456","password":"replace-with-a-long-password"}'
```

Password login:

```bash
curl -i -s http://127.0.0.1:8090/admin/auth/login \
  -H 'content-type: application/json' \
  -d '{"email":"operator@example.com","password":"replace-with-a-long-password"}'
```

Create an API key after verification:

```bash
curl -s http://127.0.0.1:8090/admin/api-keys \
  -H 'content-type: application/json' \
  -H 'cookie: ledgerflow_session=YOUR_SESSION_COOKIE' \
  -d '{"name":"production"}' | jq
```

Submit and verify a Base ETH subscription payment:

```bash
curl -s http://127.0.0.1:8090/admin/billing/base-eth/payments \
  -H 'content-type: application/json' \
  -H 'cookie: ledgerflow_session=YOUR_SESSION_COOKIE' \
  -d '{
    "plan_id": "launch",
    "tx_hash": "0xPAYMENT_TX_HASH",
    "from_address": "0xYOUR_PAYMENT_WALLET"
  }' | jq
```

The API key becomes usable for `/v1/...` requests after an active subscription exists.

## Git Strategy

Keep this as a subdirectory in the current repository for now. Avoid a Git submodule at this stage. Submodules make cross-cutting migrations, local development, and CI unnecessarily brittle.

Once the API, billing model, and deployment boundary stabilize, split `ledgerflow-indexer/` into a standalone repository using subtree or history filtering.
