# LedgerFlow Pilot v0.1 — remaining acceptance plan

Updated: 2026-08-12 UTC. This plan separates the two remaining external/elapsed-time gates from the protocol and profit gates completed with the supplied Base archive RPC. Core Phase 1 implementation, migrations, configuration, deterministic classification, Evidence Packs, alert mechanics, reports, API, regression fixtures and release image are complete in `codex/chainactors-intelligence-v2`.

## Gate A — real Telegram Pilot-group UAT

Owner input: an approved bot token environment variable and a test/Pilot chat ID. Never commit either value.

1. Add one YAML destination whose `secret_ref` names the token environment variable.
2. Import the config into an approved non-production Pilot database.
3. Deliver one real event, verify every required alert field and the public explanation link, then replay the event.
4. Confirm one Telegram message only, `sent` delivery status, stored Telegram message ID, and zero duplicate outbox rows.
5. Simulate an invalid chat/token in a test destination and confirm bounded retry followed by DLQ without affecting the valid destination.

Exit evidence: redacted delivery row, message screenshot/hash, dedup query and retry/DLQ log. Only then change “Real Telegram delivery” from `NOT IMPLEMENTED` to `PASS`.

## Completed gate B — independent protocol-recognition precision

Implemented evidence: `tests/fixtures/base_protocol_ground_truth.jsonl` is generated from independently reviewed fixture labels and reviewed deployment evidence, never from recognizer output.

1. The corpus contains 95 Base transactions: 86 positives and 9 ordinary-token negative controls.
2. Positives cover Uniswap V2/V3/V4 and Aerodrome (10 each), plus PancakeSwap (10), Balancer, Morpho, Aave and Moonwell (9 each).
3. `tests/protocol_precision.rs` runs the actual database-evidence recognizer; `scripts/protocol_precision_report.mjs` emits the machine-readable metrics.
4. Result: 86 true positives, 0 false positives, 0 false negatives, precision 1.0 and recall 1.0. Live API replay also matched one representative for every required protocol and rejected one negative control.

Status: `PASS` for direct top-level calls to reviewed deployments. Proxy variants, internal-only paths and newly deployed contracts are outside this corpus and remain explicit unknown/refresh work rather than inferred labels.

## Completed gate C — numeric arbitrage profit reliability

Infrastructure used: the supplied Base archive RPC supports historical receipts, logs, `debug_traceTransaction` and balance snapshots.

1. `src/pilot/profit.rs` reconciles receipt transfers against archive `balanceOf` snapshots for the explicit searcher/executor scope and rejects same-block ambiguity.
2. Native trace boundary flow plus exact execution gas and Base L1 fee must reconcile against archive native balances.
3. Profit is emitted only with one positive token, no negative token or native principal delta, multiple swap-pool emitters and complete accounting; otherwise it remains null with a reason.
4. `tests/fixtures/base_arbitrage_profit_cases.jsonl` contains 6 real Base cases: 3 exact verified profits and 3 native-mismatch/incomplete cases that fail closed.

Status: `PASS` for the supported complete single-settlement-asset scope. Multi-asset, fee-on-transfer and unreconciled scopes remain null; expanding those scopes requires new reviewed fixtures, not heuristic guessing.

## Gate D — seven-day soak

Run an isolated Base Pilot deployment with production-like finality, RPC limits and a non-customer Telegram test destination:

```bash
SOAK_DURATION_SECONDS=604800 SOAK_INTERVAL_SECONDS=60 \
  LEDGERFLOW_URL=http://127.0.0.1:3000 ./scripts/soak_test.sh
```

Preserve 10,080 one-minute samples, Compose restart counts, logs, checkpoint age, RPC error rate, duplicate query, DLQ explanations and PostgreSQL growth. Seven elapsed days, advancing checkpoints, successful readiness, zero duplicate events and explained delivery failures are required. A short smoke cannot be promoted to `PASS`.

## Remaining order

1. Complete Telegram UAT when an approved non-customer bot token/chat ID is supplied.
2. Let the already prepared isolated Base soak run for seven uninterrupted elapsed days, then adjudicate its 10,080 samples and operational counters.

None of these gates requires billing, self-service signup, a multi-tenant dashboard, arbitrary-chain support, an AI chatbot or changes to the production Searcher.
