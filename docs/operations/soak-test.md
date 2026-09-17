# 7-day soak test

The isolated Base Pilot stack includes a `soak` profile that runs the sampler in a
dedicated container. Select the public example environment file explicitly so the
Pilot does not inherit another deployment's `.env`:

```bash
LEDGERFLOW_ENV_FILE=config/pilots/example-base.env \
LEDGERFLOW_IMAGE=ledgerflow:phase1-base \
LEDGERFLOW_RPC_GATEWAY_UPSTREAM_URL="$BASE_RPC_URL" \
LEDGERFLOW_ANALYSIS_COMPOSE_RPC_URL="$BASE_RPC_URL" \
LEDGERFLOW_PILOT_COMPOSE_RPC_URL="$BASE_RPC_URL" \
LEDGERFLOW_PILOT_CONFIG=/app/config/pilots/soak-base-project.yaml \
docker compose --env-file config/pilots/example-base.env \
  -p ledgerflow-base-pilot --profile pilot --profile soak up -d
```

The RPC URL is a runtime secret/configuration value and is deliberately not stored
in the repository. The sampler writes `reports/base-soak-results.csv`. It can also
be run directly against an existing API with `LEDGERFLOW_URL`,
`SOAK_DURATION_SECONDS`, `SOAK_INTERVAL_SECONDS`, and `SOAK_OUTPUT`.

`soak-results.csv` records health/readiness HTTP codes and latency plus API, Postgres, project, event, retry and DLQ gauges on every interval. In parallel, retain Compose restart counts, event-worker checkpoint age, RPC error rate, duplicate-key query, PostgreSQL disk growth and worker logs. The run passes only after all 10,080 one-minute samples are present, health/readiness remain successful, checkpoints advance, duplicates remain zero, and every retry/DLQ item is explained. The script does not claim a PASS by itself.

Current isolated run: `2026-08-12T00:16:17Z` through no earlier than `2026-08-19T00:16:17Z`. Its first final-image sample returned health/readiness 200/200 and zero monitor errors, analysis failures, retries or dead letters. Do not merge samples from an earlier image or promote this run before the elapsed-time and sample-count requirements are met.
