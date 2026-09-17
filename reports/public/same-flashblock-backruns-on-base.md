# Same-Flashblock Backruns on Base

## Research status

LedgerFlow does not currently publish a count of same-Flashblock backruns on Base.

The public Base corpus contains transaction, receipt, log, protocol, and selected trace evidence. It does not yet contain a canonical Flashblock position and ordering record for every trigger and response transaction. Without that ordering evidence, labeling an execution as a same-Flashblock backrun would exceed what the public evidence proves.

## What the current public evidence establishes

- The fixed Base regression corpus contains exactly 100 RPC-verified transactions.
- The public Pilot artifact contains 39 deterministic event records across 36 transactions.
- One public arbitrage-executor interaction is retained as unverified because profit and ordering evidence are incomplete.
- Six separate Base accounting cases test deterministic surplus verification: three reconcile exactly and three fail closed to null.

These facts support methodology development. They do not support a population estimate for backruns.

## Publication threshold

A future same-Flashblock result must include:

1. Canonical trigger and response transaction hashes.
2. Block, transaction, and Flashblock ordering evidence.
3. Shared-pool or state-transition causality.
4. Searcher execution and settlement reconstruction.
5. Deterministic profit and cost accounting, or an explicit null.
6. A documented false-positive review set.

Until those inputs are present, LedgerFlow will use `Detected` rather than `Verified` and will leave intent and profit unknown.

## Reproducibility

The current public inputs are the 100-transaction Base regression dataset, the public Pilot Evidence Packs, and the six-case arbitrage profit reliability corpus included with LedgerFlow.
