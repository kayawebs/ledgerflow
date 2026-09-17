# Protocol-scoped Cluster Projector

The market cluster projector turns existing LedgerFlow facts into queryable,
domain-scoped address groups. It does not call Searcher and does not run a
global address-similarity model.

## Inputs

- Pons: `protocol_token_launches` and LedgerFlow-direct
  `protocol_launch_trade_facts`.
- Arbitrage: deterministically verified `economic_verification_results` joined
  to stored top-level `transactions`.
- Protocol roles: `protocol_seed_addresses` and `protocol_deployments`.

## Pons rule

An issuer cohort is emitted only when a buyer purchased tokens from the same
creator across at least two distinct launches. The creator fact is observed;
the buyer association remains candidate. Single-launch buyers are excluded.
The relation is valid only inside the Pons domain and is not an ownership or
team claim.

## Verified-arbitrage rule

The transaction sender is a candidate worker/self-executing account. Repeated
top-level contract targets are executor candidates. Deterministically
attributed profit recipients are settlement recipients. Indexed routers,
pools, factories and other protocol contracts remain external protocol nodes
and are excluded from controlled-entity economic scope.

## Incremental operation

`protocol_cluster_projection_checkpoints` stores a timestamp high-water mark
per domain and source. Each iteration recomputes only creators or execution
subjects whose source facts changed, while rebuilding their complete scoped
cluster idempotently.

```bash
docker compose up -d protocol-cluster-projector
```

Environment:

- `LEDGERFLOW_PROTOCOL_CLUSTER_MIN_REPEATED_LAUNCHES` (default `2`)
- `LEDGERFLOW_PROTOCOL_CLUSTER_MIN_VERIFIED_EXECUTIONS` (default `2`)
- `LEDGERFLOW_PROTOCOL_CLUSTER_POLL_INTERVAL_MS` (default `60000`)

## Health queries

```sql
select d.domain_key,p.source_kind,p.status,p.last_source_updated_at,
       p.last_run_at,p.last_error
from protocol_cluster_projection_checkpoints p
join protocol_intelligence_domains d using (domain_id);

select d.domain_key,c.cluster_kind,count(*)
from protocol_scoped_clusters c
join protocol_intelligence_domains d using (domain_id)
where c.status='active'
group by 1,2;
```
