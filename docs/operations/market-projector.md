# Market Projector Operations

`ledgerflow_market_projector` builds every read model behind the market web.
It uses only LedgerFlow's PostgreSQL facts and the local RPC (through
`rpc-gateway`); no third-party data source is involved.

## Schema and migrations

All tables live in the `market` schema. Its migrations are in
`migrations_market/` and are recorded in `market._sqlx_migrations`, separate
from the public migrator. Binaries built from older code therefore never see
unknown migrations. The projector and the market web both apply them at
startup. Every table is derived, so the schema can be rebuilt from scratch:

```sql
truncate market.projector_checkpoints, market.asset_usd_prices,
  market.token_candles_1m, market.token_stats, market.wallet_positions,
  market.wallet_leaderboard, market.cluster_leaderboard;
-- candles wait for pool swaps: keep or re-seed pool_swaps:<chain>
```

(`market.arbitrage_*`, `market.lending_*` and `market.price_assets` can be
kept; they carry RPC-sourced data that is expensive to refetch.)

## Tasks inside the binary

| Task | Cursor (`market.projector_checkpoints`) | Output |
|---|---|---|
| USD prices | `usd_prices:<chain>` (price bucket) | `asset_usd_prices`: hourly medians; pool snapshots preferred, swap observations outside 1.6× of the batch anchor dropped |
| Candles | `candles:<chain>` (block) | `token_candles_1m`; a 1,500-block trailing window is re-derived when caught up |
| Stats | `stats:<chain>` (candle `updated_at`, µs) | `token_stats`; windowed metrics also refresh every 60 s while a token has 24h trades |
| Total supply | — | `token_stats.total_supply` via `totalSupply()` |
| Positions | `positions:<chain>` (block, 600 blocks behind candles) | `wallet_positions`, incremental and transactional with its cursor |
| Leaderboards | — | `wallet_leaderboard`, `cluster_leaderboard` (24h every 60 s, 7d every 5 min, 30d every 15 min) |
| Arbitrage | `arbitrage_estimates:<chain>` (block time, µs) | `arbitrage_estimates`, `arbitrage_estimate_deltas`, `arbitrage_bot_leaderboard` |
| Price assets | — | new tokens in `price_assets` receive 30 days of price history |
| Liquidations | `liquidations:<chain>` (block) | `lending_markets`, `lending_liquidations` |
| Graduated pools | `pool_swaps:<chain>` (block) | `token_pools` (V4 pool id from the graduation receipt's `Initialize`), `pool_swap_facts`; candles never pass this cursor |

## Run

```bash
docker compose up -d market-projector
# or locally
LEDGERFLOW_DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5532/ledgerflow \
LEDGERFLOW_MARKET_RPC_URL=http://127.0.0.1:18555 \
cargo run --release --bin ledgerflow_market_projector
```

Key environment variables (defaults in parentheses):
`LEDGERFLOW_MARKET_CHAIN_ID` (4663), `LEDGERFLOW_MARKET_RPC_URL` (unset
disables supply, arbitrage and liquidation tasks),
`LEDGERFLOW_MARKET_CANDLE_BLOCK_BATCH` (100000),
`LEDGERFLOW_MARKET_STATS_BATCH_SIZE` (1000),
`LEDGERFLOW_MARKET_ARBITRAGE_BACKFILL_DAYS` (2),
`LEDGERFLOW_MARKET_ARBITRAGE_RECEIPT_CONCURRENCY` (4),
`LEDGERFLOW_MARKET_LIQUIDATION_START_BLOCK` (0),
`LEDGERFLOW_MARKET_MIN_WALLET_COST_USD` (50).

## Health checks

```sql
select checkpoint_key, last_position, last_success_at, last_error
from market.projector_checkpoints order by checkpoint_key;

-- candle lag behind trade facts, in blocks
select (select max(block_number) from public.protocol_launch_trade_facts where chain_id = 4663)
     - (select last_position from market.projector_checkpoints where checkpoint_key = 'candles:4663');

select window_key, count(*), max(computed_at) from market.wallet_leaderboard group by 1;
select status, count(*) from market.arbitrage_estimates
where block_time > now() - interval '24 hours' group by 1;
```

## Accuracy notes

- Candle prices are curve execution prices in quote units; USD uses the
  quote asset's hourly price. `token_stats.price_grade = 'reliable'` means the
  quote asset is native/wrapped native or a registry stablecoin; only reliable
  positions enter earnings rankings.
- Wallet earnings use average cost, include fees, exclude gas and ignore
  positions that sold more than they bought on the curve.
- Arbitrage estimates net ERC-20 transfers over sender + entry contract; profit
  settled elsewhere is not counted and unpriced tokens make a result unknown.
- Liquidation bonus is seized collateral value minus repaid debt value, before
  gas.
- Rankings use realized profit; unrealized inventory is shown for context only.
- Post-graduation V4 swaps feed prices, volume and candles; their `sender` is
  usually a router, so wallet positions remain curve-only.
- Hourly USD prices anchor on the 7-day median of stored prices and drop hours
  backed by one swap-derived observation.
