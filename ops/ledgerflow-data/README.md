# LedgerFlow Data Migration

This folder contains operator scripts for moving LedgerFlow data to a new server.

The data set has two parts:

- PostgreSQL: analysis API, web auth, clusters, transaction analyses, monitor data, pricing metadata.
- ClickHouse: top-level address indexer data and indexer API keys.

The scripts use Docker containers directly. Defaults match the compose project names in this repository:

- Postgres container: `ledgerflow-db-1`
- ClickHouse container: `ledgerflow-clickhouse-1`
- Postgres DB: `ledgerflow`
- ClickHouse DB: `ledgerflow_indexer`

Override with env vars if your server differs:

```bash
POSTGRES_CONTAINER=ledgerflow-db-1
POSTGRES_DB=ledgerflow
POSTGRES_USER=postgres
CLICKHOUSE_CONTAINER=ledgerflow-clickhouse-1
LEDGERFLOW_INDEXER_CLICKHOUSE_DATABASE=ledgerflow_indexer
LEDGERFLOW_INDEXER_CLICKHOUSE_USER=ledgerflow
LEDGERFLOW_INDEXER_CLICKHOUSE_PASSWORD=ledgerflow
```

## Export On The Old Server

Stop LedgerFlow writers first if possible:

```bash
docker compose stop app web indexer backfill-worker price-worker address-worker trace-monitor
```

Then export:

```bash
./ops/ledgerflow-data/export.sh
```

The script writes a directory like:

```text
ledgerflow-backups/ledgerflow-20260601T120000Z/
  postgres/ledgerflow.dump
  clickhouse/address_tx_refs.native.gz
  clickhouse/indexer_progress.native.gz
  clickhouse/api_keys.native.gz
  manifest.txt
```

Download it to your local machine:

```bash
scp -r ubuntu@<old-server>:/home/ubuntu/ledgerflow/ledgerflow-backups/ledgerflow-YYYYMMDDTHHMMSSZ ./
```

## Restore On The New Server

Copy the backup folder to the new server, then start only the databases:

```bash
docker compose up -d db clickhouse
```

Restore:

```bash
./ops/ledgerflow-data/restore.sh ledgerflow-backups/ledgerflow-YYYYMMDDTHHMMSSZ
```

Start the full stack:

```bash
docker compose up -d
```

## Cleanup Old Server Data

After you have verified the downloaded backup and the new server restore:

```bash
./ops/ledgerflow-data/cleanup_server.sh --yes
```

This removes LedgerFlow Docker containers and compose volumes. It does not touch your Base node containers.
