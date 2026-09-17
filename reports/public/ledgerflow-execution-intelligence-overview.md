# Execution Evidence Framework

LedgerFlow turns low-level EVM transaction evidence into reproducible execution intelligence. The objective is not to replace an explorer. It is to explain what happened economically, which evidence supports the conclusion, and what remains unknown.

## Evidence path

1. Retrieve transaction, receipt, block, logs, and trace when available.
2. Normalize status, exact gas, token transfers, and native flows.
3. Decode reviewed protocol interactions.
4. Reconstruct participants and boundary asset flows.
5. Classify material execution events with deterministic rules.
6. Persist a versioned Evidence Pack with confidence and limitations.

Language is downstream of facts. Missing evidence remains null and is never completed by an LLM.

## Public validation artifacts

- A fixed corpus of 100 Base transactions contains exact status, gas, Base L1 fee, and Transfer-log values.
- A reviewed direct-deployment corpus covers 95 rows: 86 positive protocol interactions and 9 negative controls.
- The public Demo contains 39 events across 36 transactions and a weekly report containing 27 events in its displayed UTC week.
- Six Base arbitrage accounting cases verify the fail-closed profit method: three exact positive token deltas and three null outcomes.

These are bounded validation results, not claims of full-chain recall.

## Product outputs

- Execution Event records
- Transaction explanations
- Versioned Evidence Pack JSON
- Monitored-entity rules
- Telegram alert delivery infrastructure
- Weekly Markdown and HTML reports

## Known boundaries

Protocol proxies, internal-only calls, missing traces, historical price gaps, cross-asset settlement, and unproven address ownership can reduce coverage. LedgerFlow records those limitations rather than presenting an unsupported conclusion.
