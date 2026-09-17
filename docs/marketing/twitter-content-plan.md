# LedgerFlow Twitter Content System — First 30 Days

Date: 2026-08-16

## Editorial standard

Tone: technical, research-oriented, concise, and evidence-first, in the tradition of rigorous protocol-research publishing. Do not imitate another team's voice or imply affiliation.

Every factual claim must link to or be traceable to a public report, case, schema, or dataset. Detector signals are described as detected, not verified. Missing profit, ownership, ordering, or price evidence remains unknown.

Avoid:

- profit flexing;
- opportunity advertising;
- exaggerated MEV language;
- actor identity claims from weak relationships;
- totals without a reproducible denominator;
- implying that a bounded regression corpus measures full-chain recall.

## Cadence

- Research thread: 1 per week
- Case study: 2 per week
- Short technical insight: 3 per week
- Remaining days: reply, distribution, source review, or no post

## 30-day calendar

| Day | Format | Topic |
|---:|---|---|
| 1 | Research thread | What Execution Intelligence adds beyond an explorer |
| 2 | Short insight | Detected versus Verified |
| 3 | Case study | Deterministic USDC surplus on Base |
| 4 | Short insight | Why gas and token surplus remain separate |
| 5 | Case study | Aave liquidation evidence |
| 6 | Short insight | Evidence Pack limitations are a feature |
| 7 | Distribution | Summarize and answer methodology questions |
| 8 | Research thread | Same-Flashblock backrun publication threshold |
| 9 | Short insight | Ordering evidence versus shared-pool correlation |
| 10 | Case study | Complex Universal Router asset flow |
| 11 | Short insight | Raw amounts cannot be compared across token decimals |
| 12 | Case study | Safe null outcome in profit accounting |
| 13 | Short insight | What archive balance reconciliation proves |
| 14 | Source review | Review claims and prepare next report |
| 15 | Research thread | Bounded Base searcher landscape evidence |
| 16 | Short insight | Address relationship is not ownership |
| 17 | Case study | Exact WETH surplus accounting case |
| 18 | Short insight | Why a multi-swap is only a candidate |
| 19 | Case study | Reverted transaction and exact gas evidence |
| 20 | Short insight | Protocol evidence and proxy limitations |
| 21 | Distribution | Publish weekly evidence index |
| 22 | Research thread | How the 100-transaction regression corpus is used |
| 23 | Short insight | Negative controls in protocol recognition |
| 24 | Case study | Morpho liquidation classification |
| 25 | Short insight | Transaction-local roles versus real-world identity |
| 26 | Case study | Large flow with unknown USD value |
| 27 | Short insight | Why LedgerFlow fails closed |
| 28 | Distribution | Invite ecosystem methodology feedback |
| 29 | Case study | Evidence Pack anatomy |
| 30 | Short insight | Next public research inputs and open limitations |

## First 20 tweets

1. Explorers tell you what a transaction contains. Execution intelligence asks a different question: what happened economically, which evidence supports it, and what remains unknown? LedgerFlow reconstructs that path from transaction, receipt, logs, trace, and balances. `/research/execution-intelligence-overview`

2. `Detected` is not a weaker visual style for `Verified`. It is a different evidence state. A detector can identify a multi-pool pattern while profit, intent, actor ownership, or ordering remain unresolved.

3. Base evidence record: receipt transfers, archive token balances, trace-derived native flows, and exact gas reconciled to a positive 3,896 raw-unit USDC delta for an explicit accounting scope. The scope relationship and cross-asset net profit remain separate questions. `/evidence/base-deterministic-usdc-surplus`

4. Gas and token surplus are facts in different assets. Subtracting one from the other requires an accepted historical conversion. LedgerFlow leaves net profit unknown when that conversion is not deterministic.

5. A liquidation classification should preserve the evidence boundary. This Base record has high-confidence liquidation-like evidence, but its public artifact has conflicting Aave recognizer and Morpho subject attribution. It stays Detected, not Verified. `/evidence/base-aave-liquidation-evidence`

6. Limitations are part of an Evidence Pack, not an appendix added after the conclusion. Missing trace, historical price, ordering, or ownership evidence changes what the product is allowed to say.

7. LedgerFlow is an Execution Intelligence and Evidence Layer for EVM ecosystems: transaction-level causality, execution reconstruction, MEV and liquidation research, and deterministic economic event analysis.

8. We are not publishing a same-Flashblock backrun count for Base yet. The current public corpus lacks canonical Flashblock position evidence for every trigger and response pair. The report explains the threshold required before such a claim is publishable. `/research/same-flashblock-backruns-on-base`

9. Two transactions touching the same pool is correlation. A backrun conclusion also needs ordering, a causal state transition, execution reconstruction, and settlement evidence.

10. This Base Universal Router transaction contains six exact Transfer-derived legs across USDC, WETH, and another token. LedgerFlow reconstructs the flow without turning it into an unsupported profit or intent claim. `/evidence/base-complex-asset-flow`

11. A raw token amount is exact but not automatically comparable. `10^18` raw units can mean one token for one asset and a very different quantity for another. Decimals and asset identity must travel with the value.

12. A safe null is a useful research result. In the six-case Base accounting set, three cases reconciled to exact positive token deltas and three stayed null because native trace and archive balance evidence did not reconcile.

13. Archive balance snapshots can validate whether receipt-derived token flows match the state transition. They do not, by themselves, prove real-world ownership of every address in the accounting scope.

14. Public research should make negative results reusable. LedgerFlow publishes the evidence required for a conclusion, the observed result, and the conditions that kept a stronger claim null.

15. The current Base searcher material is a bounded evidence set, not a census: six reviewed accounting cases and one public Demo executor interaction. No market share, total-profit, or actor-count estimate is claimed. `/research/base-searcher-landscape`

16. `EOA → contract` proves a top-level call. It does not automatically prove that the contract is a private executor or that both addresses share an owner.

17. One reviewed Base case reconciled to a positive 3,631,844,744,188 raw-unit WETH delta. The published fact is the token delta. Any USD valuation or organizational attribution needs additional evidence.

18. Multi-swap detection is useful for triage. It is not equivalent to arbitrage verification. Verification needs an explicit scope, settlement accounting, exact gas, and evidence that principal flows are not being mislabeled as profit.

19. Reverted transactions still contain economic evidence: status, gas used, effective gas price, Base L1 fee, sender, target, and attempted protocol interaction. A failed execution is not an empty event.

20. Protocol recognizers are strongest on reviewed deployments and direct evidence. Proxies, internal-only calls, and new deployments are separate recall problems and should remain explicit limitations.

## Workflow

1. Draft from a published artifact, never from an internal dashboard screenshot.
2. Copy the exact conclusion and limitations into a claim worksheet.
3. Shorten language without dropping the evidence state.
4. Review transaction hashes and numeric units.
5. Publish the site artifact first, then the social post.
6. Record post URL, source artifact, date, and any correction in a simple editorial log.
