# Optional Searcher post-graduation pool facts

Status: optional consumer contract only; no Searcher endpoint is required for launch indexing.

## Ownership boundary

LedgerFlow directly calls its configured chain RPC and owns:

- Pons factory discovery and `TokenLaunched` decoding;
- Pons V2 `CurveBuy`, `CurveSell`, `LaunchSwept`, and `PoolGraduated` decoding;
- ERC-20 symbol/name/decimals calls;
- pre-graduation curve price and flow derivation;
- launch lifecycle, token-to-curve, and token-to-pool mapping;
- protocol-scoped cohorts, attribution, rankings, and user-facing conclusions.

Searcher is not expected to know Pons, launch platforms, token cohorts, or launch economics. It may
optionally expose swaps it already decoded for a **specific post-graduation pool**. LedgerFlow must
discover and validate that pool before querying it. If this endpoint is absent, LedgerFlow can add a
direct pool-log adapter later without changing the launch indexer.

```text
Pons factory/curve logs ── LedgerFlow RPC ──> launch and pre-graduation facts
graduated pool identity ── LedgerFlow ──────> optional Searcher pool query
decoded pool swaps ─────── Searcher ────────> LedgerFlow market projection
```

The browser never calls Searcher.

## Optional endpoints

### `GET /v1/pool-facts/capabilities`

```json
{
  "schema_version": "searcher.pool-facts.v1",
  "service_version": "git-or-release-id",
  "chain_ids": [4663],
  "datasets": ["swaps"],
  "retention_from_block": 0,
  "max_page_size": 10000
}
```

### `POST /v1/pool-facts/swaps/query`

```json
{
  "chain_id": 4663,
  "from_block": 58000000,
  "to_block": 58000100,
  "pool_addresses": ["0x..."],
  "pool_ids": [],
  "after": null,
  "limit": 10000
}
```

At least one exact pool address or PoolId is mandatory. Token, platform, creator, and factory
queries are intentionally unsupported.

Each fact contains the deterministic chain coordinates, exact signed raw pool amounts, pool
address or PoolId, token pair, optional emitted price state, canonical/finality flags, and decoder
version. Results are strictly sorted by `(block_number, tx_index, log_index, fact_id)` and paginated
using that tuple.

LedgerFlow rejects wrong-chain, out-of-range, duplicate, unordered, non-canonical, or unfinalized
responses. The typed optional client is `src/services/searcher_pool_facts.rs`.

## Explicit exclusions

Searcher does not provide or decide:

- launches, graduations, curve trades, or token metadata;
- which tokens LedgerFlow should follow;
- price windows, capital flow, profitability, issuer groups, or trader cohorts;
- private RPC details, signing/bundle endpoints, strategy state, or Searcher PnL.

No polling worker is enabled until a compatible read-only pool-facts endpoint exists. This does not
block the LedgerFlow-owned Pons indexer.
