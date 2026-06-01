#!/usr/bin/env bash
set -euo pipefail

OUT_DIR="${1:-ledgerflow-backups/ledgerflow-$(date -u +%Y%m%dT%H%M%SZ)}"

POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-ledgerflow-db-1}"
POSTGRES_DB="${POSTGRES_DB:-ledgerflow}"
POSTGRES_USER="${POSTGRES_USER:-postgres}"

CLICKHOUSE_CONTAINER="${CLICKHOUSE_CONTAINER:-ledgerflow-clickhouse-1}"
CLICKHOUSE_DB="${LEDGERFLOW_INDEXER_CLICKHOUSE_DATABASE:-ledgerflow_indexer}"
CLICKHOUSE_USER="${LEDGERFLOW_INDEXER_CLICKHOUSE_USER:-ledgerflow}"
CLICKHOUSE_PASSWORD="${LEDGERFLOW_INDEXER_CLICKHOUSE_PASSWORD:-ledgerflow}"

mkdir -p "$OUT_DIR/postgres" "$OUT_DIR/clickhouse"

echo "Exporting Postgres database '$POSTGRES_DB' from container '$POSTGRES_CONTAINER'..."
docker exec "$POSTGRES_CONTAINER" pg_dump \
  -U "$POSTGRES_USER" \
  -d "$POSTGRES_DB" \
  -Fc \
  --no-owner \
  --no-acl \
  > "$OUT_DIR/postgres/ledgerflow.dump"

clickhouse_client() {
  docker exec -i "$CLICKHOUSE_CONTAINER" clickhouse-client \
    --user "$CLICKHOUSE_USER" \
    --password "$CLICKHOUSE_PASSWORD" \
    "$@"
}

clickhouse_table_exists() {
  local table="$1"
  local result
  result="$(clickhouse_client --query "EXISTS TABLE ${CLICKHOUSE_DB}.${table}")"
  [[ "$result" == "1" ]]
}

CLICKHOUSE_TABLES=(
  address_tx_refs
  indexer_progress
  api_keys
)

echo "Exporting ClickHouse database '$CLICKHOUSE_DB' from container '$CLICKHOUSE_CONTAINER'..."
for table in "${CLICKHOUSE_TABLES[@]}"; do
  if ! clickhouse_table_exists "$table"; then
    echo "Skipping missing ClickHouse table: $CLICKHOUSE_DB.$table"
    continue
  fi
  echo "Exporting ClickHouse table: $CLICKHOUSE_DB.$table"
  clickhouse_client --query "SHOW CREATE TABLE ${CLICKHOUSE_DB}.${table}" \
    > "$OUT_DIR/clickhouse/${table}.schema.sql"
  clickhouse_client --query "SELECT * FROM ${CLICKHOUSE_DB}.${table} FORMAT Native" \
    | gzip -1 > "$OUT_DIR/clickhouse/${table}.native.gz"
done

cat > "$OUT_DIR/manifest.txt" <<EOF
created_at_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)
postgres_container=$POSTGRES_CONTAINER
postgres_db=$POSTGRES_DB
clickhouse_container=$CLICKHOUSE_CONTAINER
clickhouse_db=$CLICKHOUSE_DB
EOF

echo "Backup written to: $OUT_DIR"
echo
echo "Download from server example:"
echo "  scp -r ubuntu@<server>:$(pwd)/$OUT_DIR ./"
