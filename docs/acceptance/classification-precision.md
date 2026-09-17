# Classification precision and profit reliability

Run date: 2026-08-11 UTC.

The fixed fixture contains 100 Base transactions: Aave 12; Aerodrome 10; Balancer 9; Moonwell 9; Morpho 9; PancakeSwap 10; Uniswap V2/V3/V4 10 each; ordinary USDC transfers 9; multi-protocol special cases 2. Fixture v2 records exact RPC native value, status, gas and all receipt Transfer logs with raw values. A Transfer topic alone cannot distinguish an ERC-20 amount from an ERC-721 token ID, so that limitation is stored per log. Discovery labels identify the contract/address bucket used to select transactions; they are not treated as classifier ground truth.

## Checked ground-truth cases

| Case | Expected | Observed | Result |
|---|---:|---:|---|
| Configured public arbitrage executor | 1 | 1 `dex_arbitrage` | PASS |
| Fixed Aave liquidation hash | 1 | 1 `liquidation` | PASS |
| Fixed reverted hashes | 2 | 2 `high_cost_failure` | PASS |
| Fixed complex asset-flow hash | 1 | 1 `large_asset_flow` | PASS |
| Fixture transactions persisted | 100 | 100 | PASS |
| Evaluable configured protocol-path label agreement | 34 | 34 | PASS (100%) |

The run produced 39 events across 36 transactions. 35/39 events have a non-empty protocol path sourced from an actually involved, customer-configured protocol entity; 34 were evaluable against a single-protocol fixture bucket and all 34 agreed. Four of 39 also have independent recognizer-derived protocol evidence. Configured identity is deterministic input, not an LLM guess, but it is not independent human ground truth. Full event/no-event precision, recall, false-positive rate and broad recognizer accuracy are `NOT IMPLEMENTED`, not estimated.

## Profit validation

The fixed arbitrage case calls a configured public arbitrage executor, so it is emitted with 0.75 confidence. Complete trace/state accounting was unavailable. `profit_raw` and `profit_token_address` therefore remain null and the Evidence Pack records the limitation. The safe-null policy is tested and passes; numeric arbitrage profit reliability is `NOT IMPLEMENTED` until complete inputs and reviewed ground truth are available.
