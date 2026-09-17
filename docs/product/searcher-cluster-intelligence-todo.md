# Searcher Cluster Intelligence — Follow-up TODO

Updated: 2026-08-21

This list records work intentionally deferred from Searcher Cluster Intelligence
v1. It is not an acceptance claim.

## P1 — high-volume cold-start benchmark

- Status: **TODO**
- Select a new, previously unobserved high-frequency Base searcher address.
- Preserve its transaction count and first/last activity block before the run.
- Measure time to earliest-block resolution, full history discovery, first 24h
  summary, first 20 relationships, initial economic sample, and complete
  discovery.
- Record provider request count/rate-limit behavior, RPC/trace load, PostgreSQL
  growth, checkpoint recovery, and impact on live indexer lag.
- Do not tune or accept this benchmark using the existing collector seed.

## P1 — external history reliability

- Add provider health/rate-limit metrics and a second discovery adapter.
- Evaluate the optional internal-transaction endpoint separately. It is disabled
  by default because the current Base provider did not return the high-volume
  collector query within 20 seconds. Canonical traces still reconstruct internal
  value movement for discovered execution transactions.
- Add a scheduled reconciliation that compares provider-discovered hashes with
  canonical RPC receipts/logs. Third-party fields must never become economic
  facts directly.

## P1 — economic verification throughput

- Drain and measure the deep-reconstruction and deterministic-verification
  backlog for the existing Base collector.
- Separate `pending` coverage from terminal evidence exclusions in product
  metrics.
- Add adaptive bounded concurrency that observes RPC/indexer lag.

## P1 — ground-truth acceptance

- Add an observable Base internal Searcher fixture.
- Select an active Robinhood internal seed whose onchain activity falls within
  available address-history coverage, or configure a compatible full-address
  history provider for Robinhood.
- Re-run worker recall >= 90%, executor recall >= 95%, router/pool false member
  rate = 0, and funding-as-profit error = 0.

