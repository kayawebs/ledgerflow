#!/usr/bin/env bash
# Measures market web TTFB for the main pages and fragments.
# Usage: scripts/market_web_latency.sh [base_url] [requests_per_route]
set -euo pipefail

BASE_URL="${1:-http://127.0.0.1:3401}"
REQUESTS="${2:-20}"

ROUTES=(
  "/?lang=zh"
  "/launches?lang=zh"
  "/launches?lang=zh&tab=gainers"
  "/launches?lang=zh&tab=volume&sort=mcap&dir=desc"
  "/clusters?lang=zh"
  "/clusters?lang=zh&period=7d"
  "/arbitrage?lang=zh"
  "/liquidations?lang=zh"
  "/fragments/ticker?lang=zh"
  "/fragments/overview/boards?lang=zh"
  "/fragments/overview/feed?lang=zh"
  "/fragments/launches?lang=zh&tab=hot"
  "/fragments/arbitrage/feed?lang=zh"
)

printf '%-52s %6s %8s %8s %8s\n' route status p50_ms p95_ms max_ms
for route in "${ROUTES[@]}"; do
  # Warm the route once so the numbers describe the steady state.
  curl -s -o /dev/null "${BASE_URL}${route}" || true
  samples=()
  status=""
  for _ in $(seq 1 "${REQUESTS}"); do
    read -r code ttfb < <(curl -s -o /dev/null -w '%{http_code} %{time_starttransfer}\n' "${BASE_URL}${route}")
    status="${code}"
    samples+=("$(awk -v t="${ttfb}" 'BEGIN { printf "%d", t * 1000 }')")
  done
  sorted=($(printf '%s\n' "${samples[@]}" | sort -n))
  count=${#sorted[@]}
  p50=${sorted[$(( (count - 1) * 50 / 100 ))]}
  p95=${sorted[$(( (count - 1) * 95 / 100 ))]}
  max=${sorted[$(( count - 1 ))]}
  printf '%-52s %6s %8s %8s %8s\n' "${route}" "${status}" "${p50}" "${p95}" "${max}"
done
