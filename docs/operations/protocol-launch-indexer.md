# Protocol Launch Indexer Operations

The protocol launch indexer is owned by LedgerFlow. It performs targeted factory-log scans and does not require the full historical transaction indexer.

## Ownership

- Database seed manifests define launch domains, factory addresses, and valid deployment blocks.
- Code adapters define ABI event signatures and deterministic field decoding.
- `protocol_launch_lifecycle_events` is the immutable/auditable lifecycle projection.
- `protocol_launch_trade_facts` stores exact Pons V2 CurveBuy/CurveSell facts.
- `protocol_token_launches` is the current lifecycle projection used by the market web.
- ERC-20 metadata is read directly through LedgerFlow's configured RPC and cached in the database.
- Searcher is not used for launch discovery, curve trading, token metadata, or pre-graduation data.

## Start

```bash
docker compose up -d market-seed-import
docker compose up -d protocol-launch-indexer
```

The default Robinhood settings scan 5,000 blocks per active launch domain per iteration. Each domain has a durable lifecycle cursor named `protocol_launch_rpc:<domain_key>`. Pons V2 also has an independent curve cursor named `protocol_launch_rpc_curve:<domain_key>` so deploying a new decoder never silently skips ranges that the lifecycle projector already passed.

## Recovery

The cursor advances only after all decoded rows in the range commit. Restarting the worker resumes at the next block. Lifecycle facts are deduplicated by chain, transaction hash, log index, and event type. Curve facts are deduplicated by chain, transaction hash, and log index. Launch projections are deduplicated by domain and token.

At the safe chain head, the worker periodically rescans a 128-block lookback. RPC or decoding failures retain the old successful cursor and increment `retry_count`.

`LEDGERFLOW_PROTOCOL_LAUNCH_START_BLOCK_OVERRIDE` is only an operational floor for validation or an intentional partial bootstrap. Leave it unset for normal full history from every database seed's `valid_from_block`.

## Health checks

```sql
select worker_id,last_processed_block,status,retry_count,last_error,last_success_at
from onchain_event_cursors
where worker_id like 'protocol_launch_rpc:%'
   or worker_id like 'protocol_launch_rpc_curve:%';

select protocol_version,lifecycle_phase,launch_status,count(*)
from protocol_token_launches
group by 1,2,3;

select trade_side,count(*),max(occurred_at)
from protocol_launch_trade_facts
group by 1;

select metadata_status,count(*)
from protocol_token_launches
group by 1;
```

The worker is lightweight because it requests only factory lifecycle topics and the two Pons V2 curve-trade topics. Curve emitters are accepted only when a factory launch event already mapped that address to the domain. It does not retrieve every block body, receipt, trace, or unrelated token transfer.

`LEDGERFLOW_PROTOCOL_LAUNCH_METADATA_FETCH_LIMIT` bounds direct ERC-20 calls per iteration. The default is eight tokens, fetched concurrently with an individual timeout, so historical catch-up cannot create an unbounded metadata burst or block factory-log progress.

## Known lifecycle limitation

Pons V2 has explicit `LaunchSwept` and `PoolGraduated` events. The graduation event supplies a position identifier but not a V4 pool contract address, so LedgerFlow leaves `pool_address` null until a deterministic PoolId mapping is available.

Pons V1 launches directly into a V3 pool. Its later graduation state requires a factory state call rather than a distinct graduation event; launch discovery is implemented, while that state probe remains pending.
