# Base Searcher Landscape

## Scope

This is a bounded evidence note, not a census of Base searchers.

The current public material includes one configured arbitrage-executor interaction in the Pilot Demo and six separately reviewed accounting transactions. It is sufficient to demonstrate verification behavior, but not to estimate the number, market share, or total profit of Base searchers.

## Reviewed accounting cases

The six-case reliability set produced:

- Three exact positive token deltas.
- Three safe null outcomes because native trace flows and exact gas did not reconcile with archive balance changes.
- Zero numeric profit claims for the incomplete cases.

Verified positive deltas are recorded for:

- `0x1ae5…8737`: 5,119 raw USDC units.
- `0x921b…92dc`: 3,631,844,744,188 raw WETH units.
- `0xe529…f915`: 3,896 raw USDC units.

Gas remains a separate native-asset fact unless a deterministic historical conversion is accepted.

## What this says about searcher analysis

Searcher intelligence requires more than detecting a multi-swap transaction. A reliable result needs an explicit accounting scope, receipt and trace evidence, before/after balances, exact gas, and a settlement path. Address ownership and organizational identity are separate claims and are not inferred from a shared transaction alone.

## What is not claimed

- Total Base searcher count
- Searcher market share
- Aggregate Base MEV profit
- Same-Flashblock or next-Flashblock distribution
- Real-world ownership of the accounting addresses

Those conclusions require broader canonical ordering and relationship evidence than the current public corpus provides.
