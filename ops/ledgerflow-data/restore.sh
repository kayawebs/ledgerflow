#!/usr/bin/env bash
set -euo pipefail

BACKUP_DIR="${1:-}"
if [[ -z "$BACKUP_DIR" ]]; then
  echo "Usage: $0 <backup-dir>"
  exit 1
fi

POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-ledgerflow-db-1}"
POSTGRES_DB="${POSTGRES_DB:-ledgerflow}"
POSTGRES_USER="${POSTGRES_USER:-postgres}"

CLICKHOUSE_CONTAINER="${CLICKHOUSE_CONTAINER:-ledgerflow-clickhouse-1}"
CLICKHOUSE_DB="${LEDGERFLOW_INDEXER_CLICKHOUSE_DATABASE:-ledgerflow_indexer}"
CLICKHOUSE_USER="${LEDGERFLOW_INDEXER_CLICKHOUSE_USER:-ledgerflow}"
CLICKHOUSE_PASSWORD="${LEDGERFLOW_INDEXER_CLICKHOUSE_PASSWORD:-ledgerflow}"
DOCKER="${DOCKER:-docker}"

POSTGRES_DUMP="$BACKUP_DIR/postgres/ledgerflow.dump"
if [[ ! -f "$POSTGRES_DUMP" ]]; then
  echo "Missing Postgres dump: $POSTGRES_DUMP"
  exit 1
fi

echo "Restoring Postgres database '$POSTGRES_DB' into container '$POSTGRES_CONTAINER'..."
$DOCKER exec -i "$POSTGRES_CONTAINER" pg_restore \
  -U "$POSTGRES_USER" \
  -d "$POSTGRES_DB" \
  --clean \
  --if-exists \
  --no-owner \
  --no-acl \
  < "$POSTGRES_DUMP"

clickhouse_client() {
  $DOCKER exec -i "$CLICKHOUSE_CONTAINER" clickhouse-client \
    --user "$CLICKHOUSE_USER" \
    --password "$CLICKHOUSE_PASSWORD" \
    "$@"
}

echo "Preparing ClickHouse database '$CLICKHOUSE_DB'..."
clickhouse_client --query "CREATE DATABASE IF NOT EXISTS ${CLICKHOUSE_DB}"

clickhouse_client --query "
CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DB}.address_tx_refs (
  chain_id UInt32,
  address String,
  tx_hash String,
  block_number UInt32,
  tx_index UInt32,
  direction Enum8('from' = 1, 'to' = 2),
  from_address Nullable(String),
  to_address Nullable(String),
  inserted_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY intDiv(block_number, 1000000)
ORDER BY (chain_id, address, block_number, tx_index, direction, tx_hash)
"

clickhouse_client --query "
CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DB}.indexer_progress (
  chain_id UInt32,
  worker_id String,
  last_indexed_block UInt32,
  updated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (chain_id, worker_id)
"

clickhouse_client --query "
CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DB}.api_keys (
  key_id String,
  token_hash String,
  owner_user_id String,
  owner_email String,
  label String,
  token_prefix String,
  status Enum8('active' = 1, 'revoked' = 2),
  created_at DateTime,
  updated_at DateTime,
  last_used_at Nullable(DateTime)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (owner_user_id, key_id)
"

CLICKHOUSE_TABLES=(
  address_tx_refs
  indexer_progress
  api_keys
)

for table in "${CLICKHOUSE_TABLES[@]}"; do
  data_file="$BACKUP_DIR/clickhouse/${table}.native.gz"
  if [[ ! -f "$data_file" ]]; then
    echo "Skipping missing ClickHouse backup file: $data_file"
    continue
  fi
  echo "Restoring ClickHouse table: $CLICKHOUSE_DB.$table"
  clickhouse_client --query "TRUNCATE TABLE IF EXISTS ${CLICKHOUSE_DB}.${table}"
  gzip -dc "$data_file" \
    | clickhouse_client --query "INSERT INTO ${CLICKHOUSE_DB}.${table} FORMAT Native"
done

echo "Restore completed from: $BACKUP_DIR"
