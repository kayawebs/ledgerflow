#!/usr/bin/env bash
set -euo pipefail

: "${LEDGERFLOW_DATABASE_URL:?set LEDGERFLOW_DATABASE_URL to an isolated or approved Pilot database}"
: "${LEDGERFLOW_PILOT_CONFIG:?set LEDGERFLOW_PILOT_CONFIG to the Pilot YAML path}"
: "${LEDGERFLOW_PILOT_PROJECT_ID:?set LEDGERFLOW_PILOT_PROJECT_ID to the YAML project_id}"

pilot_query() {
  printf '%s\n' "$1" | psql "$LEDGERFLOW_DATABASE_URL" \
    -v project_id="$LEDGERFLOW_PILOT_PROJECT_ID" -At
}

cargo run --quiet --bin ledgerflow_pilot_config -- "$LEDGERFLOW_PILOT_CONFIG"
LEDGERFLOW_PILOT_RUN_ONCE=true cargo run --quiet --bin ledgerflow_pilot_event_worker
first_event_count="$(pilot_query "SELECT count(*) FROM pilot_events WHERE project_id=:'project_id';")"
LEDGERFLOW_PILOT_RUN_ONCE=true cargo run --quiet --bin ledgerflow_pilot_event_worker
second_event_count="$(pilot_query "SELECT count(*) FROM pilot_events WHERE project_id=:'project_id';")"
duplicate_count="$(pilot_query "SELECT count(*)-count(DISTINCT dedup_key) FROM pilot_events WHERE project_id=:'project_id';")"
failed_runs="$(pilot_query "SELECT count(*) FROM pilot_analysis_runs WHERE project_id=:'project_id' AND status='failed';")"

test "$first_event_count" = "$second_event_count"
test "$duplicate_count" = "0"
test "$failed_runs" = "0"
printf 'PASS project=%s events=%s duplicates=%s failed_runs=%s\n' \
  "$LEDGERFLOW_PILOT_PROJECT_ID" "$second_event_count" "$duplicate_count" "$failed_runs"
