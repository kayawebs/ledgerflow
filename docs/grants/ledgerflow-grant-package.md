# LedgerFlow Grant Package

Date: 2026-08-16

## Project

**LedgerFlow — Execution Intelligence and Evidence Layer for EVM ecosystems**

## Problem

Existing explorers are effective at showing transaction inputs, receipts, logs, and balances. They generally do not explain the economic execution context:

- what triggered a material interaction;
- how execution moved through protocols and pools;
- which asset flows and costs are attributable to the execution;
- whether an MEV or liquidation conclusion is verified or only detected;
- how another researcher can reproduce the conclusion.

Teams repeatedly rebuild this analysis with private scripts, inconsistent labels, and screenshots that do not preserve evidence boundaries.

## Solution

LedgerFlow provides:

- a versioned execution event schema;
- deterministic transaction normalization and flow reconstruction;
- an Evidence Pack framework with confidence and limitations;
- MEV, liquidation, failure, and material-flow research paths;
- reproducible reports and public evidence records;
- private monitored-entity rules and alert delivery for Pilot customers.

LedgerFlow does not use an LLM to guess a protocol, address identity, transaction purpose, price, or profit. Language generation remains downstream of transaction, receipt, logs, trace, balance, and deterministic accounting evidence.

## Existing proof

- Fixed 100-transaction Base regression dataset with exact status, gas, Base L1 fee, and Transfer-log values.
- Public Base Demo with 39 events across 36 transactions and a 27-event weekly report.
- Reviewed protocol corpus with 86 positive interactions and 9 negative controls across required Base protocol families.
- Six reviewed Base arbitrage accounting cases: three exact positive token deltas and three incomplete cases that return null.
- Robinhood Chain live canonical Indexer with current near-head operation.
- Versioned Evidence Pack JSON Schema, Pilot monitoring code, Telegram delivery infrastructure, and weekly report generation.

These are bounded artifacts. LedgerFlow does not present them as full-chain precision or recall measurements.

## Public goods

Grant-supported public outputs can include:

1. Research reports with transaction-level evidence and limitations.
2. Public Evidence Explorer records and selected Evidence Packs.
3. Open event and Evidence Pack schemas.
4. Bounded sample and regression datasets.
5. Methodology for deterministic profit, gas, and asset-flow validation.
6. Protocol coverage and false-positive review reports.
7. Operational documentation for reproducible indexing and analysis.

## Commercial sustainability

The commercial layer funds ongoing operations without restricting the public methodology:

- private address and contract monitoring;
- customer-specific rules and thresholds;
- Telegram alerts and delivery history;
- private investigations and Evidence Packs;
- weekly reports;
- scoped API access and service commitments.

Public research demonstrates the method. Paid Pilots apply it continuously to a customer-defined scope.

## Proposed grant milestones

### Milestone 1 — Public evidence library

- Publish the Research and Evidence Explorer surfaces.
- Publish at least three methodology or ecosystem reports using existing evidence.
- Expose selected customer-safe Evidence Packs and schemas.

### Milestone 2 — Reproducible ecosystem coverage

- Expand reviewed public datasets without adding unsupported chain claims.
- Publish precision, recall, and limitation reports for each scoped corpus.
- Add canonical ordering evidence where the target ecosystem exposes it.

### Milestone 3 — Pilot validation

- Onboard a small set of ecosystem or protocol Pilot scopes.
- Measure monitored-transaction completeness, alert delivery, and report generation.
- Publish anonymized operational findings only with customer approval.

## Evaluation metrics

- Number of public reports and reproducible evidence records
- Evidence Packs passing schema validation
- Reviewed transactions and negative controls
- Detector-to-verified conversion with limitation breakdown
- Monitored transaction completeness in bounded Pilot scopes
- Alert deduplication, delivery, retry, and dead-letter rates
- Weekly report generation reliability
- Public methodology citations or ecosystem reuse

## Security and privacy

Public artifacts exclude private keys, private RPC endpoints, Searcher infrastructure, Telegram credentials, and customer data. Public evidence records use public chain evidence and explicitly selected Demo configuration only.

## Funding use categories

- Public dataset review and evidence QA
- Protocol and ecosystem research
- Indexing and archive RPC infrastructure
- Public Research and Evidence Explorer distribution
- Documentation and reproducibility tooling
- Pilot operational validation

Budget amount, duration, ecosystem-specific deliverables, and reporting cadence should be completed for each grant program rather than hard-coded into this reusable package.

## Links in this repository

- Evidence Pack schema: `schemas/evidence-pack-v1.schema.json`
- Execution Event schema: `schemas/execution-event-v1.schema.json`
- Public Base Demo: `demo/base-public/`
- Base regression dataset: `tests/fixtures/base_regression_dataset.jsonl`
- Acceptance evidence: `docs/acceptance/phase1-results.md`
- Public product strategy: `docs/product/public-product-strategy.md`
