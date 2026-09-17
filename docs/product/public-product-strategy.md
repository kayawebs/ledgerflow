# LedgerFlow Public + Pilot Product Strategy v1

Date: 2026-08-16

## Positioning

LedgerFlow is the **Execution Intelligence and Evidence Layer for EVM ecosystems**.

It helps chains, protocols, searchers, and infrastructure teams understand how a transaction executed, how value moved, which actors and protocols participated, what can be concluded economically, and which evidence supports that conclusion.

LedgerFlow is not:

- another block explorer optimized for raw transaction lookup;
- another wallet tracker centered on balances and labels;
- another DefiLlama-style aggregate dashboard;
- a generic security SOC or compliance product.

## Core differentiation

1. **Transaction-level causality** — connect trigger, protocol interactions, asset flows, failures, settlement, and cost without replacing missing evidence with a guess.
2. **Execution reconstruction** — normalize transaction, receipt, logs, trace, and exact gas into one execution narrative.
3. **MEV, liquidation, and searcher intelligence** — identify execution patterns and separate candidates from economically verified facts.
4. **Deterministic evidence** — make every material conclusion reproducible from public chain inputs and a versioned ruleset.
5. **Economic event analysis** — organize activity as actionable events rather than exposing database inventories.

## Audience and jobs

| Audience | Primary job |
|---|---|
| Chain and ecosystem teams | Understand execution quality, actor changes, MEV patterns, and ecosystem-level events |
| DEX and lending protocols | Monitor material transactions, searcher activity, liquidations, failures, and fee/revenue paths |
| Searchers, solvers, and market makers | Review execution evidence, failed gas, counterparties, and strategy behavior |
| Builders, sequencers, and OFA teams | Study ordering, attribution, reverts, settlement, and orderflow outcomes |
| Research and risk teams | Produce reproducible cases and reports with explicit limitations |

## Product loop

```text
Discover a material execution event
        ↓
Reconstruct the economic path
        ↓
Inspect participants and evidence
        ↓
Monitor related entities and rules
        ↓
Distribute an alert, evidence record, or report
```

## Public product

The public surface establishes credibility through evidence:

- research reports;
- a public Evidence Explorer with transaction-level evidence records;
- selected verified events;
- open Evidence Pack schema and methodology;
- bounded datasets and reproducible validation results;
- a limited explorer entry into public transaction explanations.

Public content must distinguish measured facts, bounded test results, detector candidates, and unsupported questions. A report can publish a null finding or an evidence gap; it cannot turn an internal hypothesis into a public claim.

## Commercial Pilot

The commercial layer applies the same evidence system to a customer-defined scope:

- 10–30 monitored addresses and contracts;
- roles and labels supplied or explicitly reviewed by the customer;
- custom materiality and failure rules;
- Telegram alert delivery;
- private investigations;
- weekly reports and Evidence Packs;
- scoped API access and operational support.

The Pilot does not require billing or a full self-service multi-tenant dashboard. Initial onboarding is operator-assisted and configuration-driven.

## Claim standard

- `Detected` means a deterministic signal exists but the required economic evidence is incomplete.
- `Verified` means the stated conclusion, not a broader identity or intent claim, reconciles against required evidence.
- Unknown amounts remain null.
- Address relationships use narrow semantics such as call target, pool interaction, or settlement recipient.
- LLMs may improve language but may not supply facts, protocols, identities, prices, or profit.

## Distribution strategy

1. Publish one rigorous research note per week, including methodology or negative results.
2. Publish two evidence-backed records per week with transaction links and limitations.
3. Publish three short technical observations per week that explain methods, not profit opportunities.
4. Use the public site as the canonical source; social content links back to Research and Evidence Explorer records.
5. Use public artifacts in grant applications and ecosystem partnership conversations.

## Success measures

- Public reports are reproducible and never require private infrastructure.
- Every public evidence record exposes status, confidence, and limitations.
- A protocol or ecosystem team can understand the Pilot in under five minutes.
- A qualified team can submit a scoped Pilot request without an account or CRM integration.
- Paid access is represented by a stable permission abstraction before billing exists.
