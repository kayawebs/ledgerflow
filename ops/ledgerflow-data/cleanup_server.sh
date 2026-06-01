#!/usr/bin/env bash
set -euo pipefail

if [[ "${1:-}" != "--yes" ]]; then
  cat <<'EOF'
This removes LedgerFlow Docker containers and named volumes for the current compose project.

It is destructive for server-side LedgerFlow data:
  - Postgres volume
  - ClickHouse volume
  - LedgerFlow app/worker/indexer containers

Run an export first:
  ./ops/ledgerflow-data/export.sh

Then run:
  ./ops/ledgerflow-data/cleanup_server.sh --yes
EOF
  exit 1
fi

echo "Stopping LedgerFlow compose stack and removing compose volumes..."
docker compose down -v --remove-orphans

echo "Removing legacy LedgerFlow containers if they still exist..."
docker rm -f \
  ledgerflow-db-1 \
  ledgerflow-clickhouse-1 \
  ledgerflow-app-1 \
  ledgerflow-web-1 \
  ledgerflow-indexer-1 \
  ledgerflow-worker-1 \
  ledgerflow-backfill-worker-1 \
  ledgerflow-price-worker-1 \
  ledgerflow-address-worker-1 \
  ledgerflow-trace-monitor-1 \
  2>/dev/null || true

echo "Removing LedgerFlow named volumes if they still exist..."
docker volume ls --format '{{.Name}}' \
  | grep '^ledgerflow_' \
  | xargs -r docker volume rm

echo "LedgerFlow Docker data cleanup completed."
