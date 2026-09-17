#!/bin/sh
set -eu

SOAK_BASE_URL="${LEDGERFLOW_URL:-http://127.0.0.1:3000}"
SOAK_DURATION_SECONDS="${SOAK_DURATION_SECONDS:-604800}"
SOAK_INTERVAL_SECONDS="${SOAK_INTERVAL_SECONDS:-60}"
SOAK_OUTPUT_PATH="${SOAK_OUTPUT:-soak-results.csv}"

metric_value() {
  awk -v metric_name="$1" '$1 == metric_name { print $2 }'
}

printf 'timestamp,health_http,ready_http,health_ms,ready_ms,api_up,postgres_up,pilot_projects,active_monitors,monitor_errors,pilot_events,analysis_failures,queued,retries,dead_letters\n' > "$SOAK_OUTPUT_PATH"
soak_started_at="$(date +%s)"
soak_end=$((soak_started_at + SOAK_DURATION_SECONDS))
while [ "$(date +%s)" -lt "$soak_end" ]; do
  soak_timestamp="$(date -u +%FT%TZ)"
  health_sample="$(curl -sS --max-time 10 -o /dev/null -w '%{http_code},%{time_total}' "$SOAK_BASE_URL/health" || true)"
  ready_sample="$(curl -sS --max-time 10 -o /dev/null -w '%{http_code},%{time_total}' "$SOAK_BASE_URL/ready" || true)"
  metrics_body="$(curl -fsS --max-time 10 "$SOAK_BASE_URL/metrics" || true)"
  health_http="${health_sample%%,*}"
  health_seconds="${health_sample#*,}"
  ready_http="${ready_sample%%,*}"
  ready_seconds="${ready_sample#*,}"
  health_ms="$(awk -v value="${health_seconds:-0}" 'BEGIN { printf "%.3f", value * 1000 }')"
  ready_ms="$(awk -v value="${ready_seconds:-0}" 'BEGIN { printf "%.3f", value * 1000 }')"
  api_up="$(printf '%s\n' "$metrics_body" | metric_value ledgerflow_api_up)"
  postgres_up="$(printf '%s\n' "$metrics_body" | metric_value ledgerflow_postgres_up)"
  pilot_projects="$(printf '%s\n' "$metrics_body" | metric_value ledgerflow_pilot_projects)"
  active_monitors="$(printf '%s\n' "$metrics_body" | metric_value ledgerflow_pilot_active_monitors)"
  monitor_errors="$(printf '%s\n' "$metrics_body" | metric_value ledgerflow_pilot_monitor_errors)"
  pilot_events="$(printf '%s\n' "$metrics_body" | metric_value ledgerflow_pilot_events_total)"
  analysis_failures="$(printf '%s\n' "$metrics_body" | metric_value ledgerflow_pilot_analysis_failures)"
  queued="$(printf '%s\n' "$metrics_body" | metric_value ledgerflow_pilot_alert_queued)"
  retries="$(printf '%s\n' "$metrics_body" | metric_value ledgerflow_pilot_alert_retries)"
  dead_letters="$(printf '%s\n' "$metrics_body" | metric_value ledgerflow_pilot_alert_dead_letters)"
  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$soak_timestamp" "${health_http:-000}" "${ready_http:-000}" "$health_ms" "$ready_ms" \
    "${api_up:-0}" "${postgres_up:-0}" "${pilot_projects:-0}" "${active_monitors:-0}" \
    "${monitor_errors:-0}" "${pilot_events:-0}" "${analysis_failures:-0}" "${queued:-0}" \
    "${retries:-0}" "${dead_letters:-0}" >> "$SOAK_OUTPUT_PATH"
  sleep "$SOAK_INTERVAL_SECONDS"
done
