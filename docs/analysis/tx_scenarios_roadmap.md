# Common Transaction Scenario Roadmap

This document tracks the next scenario-recognition work for single-transaction analysis.

## Current Direction

The recognizer system should keep expanding through narrow, evidence-bound scenario recognizers.

Current examples now include:

- selector presence
- native transfer activity
- ERC20 transfer activity
- ERC20 approval activity
- swap-like activity
- approval-then-swap-like activity
- bridge-like asset exit
- deposit-like / withdraw-like activity
- failed execution
- arbitrage-like activity
- liquidation-like activity

## Next Priority Scenarios

## Priority 1

- liquidation execution
  - identify repay asset
  - identify seized asset
  - identify liquidator vs protocol counterparty
- arbitrage execution
  - detect route length
  - estimate gross in/out by asset
  - distinguish from ordinary swaps
- bridge deposit / bridge claim
  - separate outbound lock / burn from inbound mint / release

## Priority 2

- flash-loan-assisted execution
- vault deposit / withdrawal
- borrow / repay
- LP add / remove liquidity
- batch router / aggregator path classification

## Priority 3

- liquidation retry / failed liquidation
- sandwich-like routing warnings
- self-shuffling / value recycling patterns
- treasury rebalance patterns

## Implementation Rules

- prefer `*_like` labels until protocol-specific confidence is strong
- attach ambiguity reasons and unresolved fields explicitly
- keep protocol classification separate from intent classification
- always link scenario outputs back to evidence refs, flows, or normalized facts

## Required Supporting Work

These scenario recognizers depend on:

- stronger trace facts
- a call-stack timeline projection, not only an asset-flow timeline
- event/token-flow attachment to call frames where evidence allows it
- richer token delta summaries
- touched-contract and protocol-family hints
- protocol / venue intelligence registry
- revert / failure location hints
- scenario composition rather than one giant recognizer

## Protocol Intelligence Priority

The first implementation target should be DEX / venue intelligence because it directly improves swap and arbitrage interpretation.

DEX coverage should include:

- known routers and aggregators
- V2-style pairs
- V3-style pools
- Aerodrome/Solidly-style pools
- swap events and pool token metadata

This should still be implemented as a general protocol intelligence system, not a DEX-specific shortcut. The same adapter pattern must later support lending markets, liquidation venues, vaults, bridges, LP positions, and other DeFi protocol families.

## Timeline Semantics

The transaction timeline should represent execution order.

- A call step may have no token transfer.
- A token transfer should be rendered as evidence attached to a call step when possible.
- When trace is available, the first row should be the top-level transaction call from initiator to target/executor.
- When trace is unavailable, the timeline can fall back to confirmed asset flows only and must label that limitation.
