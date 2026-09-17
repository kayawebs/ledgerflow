# Base Runtime Data-Source Audit

Date: 2026-08-20

## Conclusion

Base product traffic is routed to a real, dedicated Base data plane. It is not reading Robinhood Chain, a demo fixture, or the regression dataset. At audit time the Base live indexer was two blocks behind its RPC chain head.

The Base data plane is not a global full-history index. Its continuous live
range began at block `49,850,596`. For newly observed addresses, a configurable
Blockscout-compatible discovery adapter can now resolve the earliest activity
and page the address's normal transactions and token transfers without replaying
every historical block. Every discovered hash is re-fetched from canonical RPC;
the provider is not an accounting source. Product coverage remains
object-specific and must not claim global historical completeness.

## Evidence

| Check | Result |
|---|---|
| Base API configured chain | `8453` |
| Base RPC chain | `8453` |
| Base indexer chain | `8453` |
| Live indexed range at audit | `49,850,596` through `50,240,310` |
| RPC head at audit | `50,240,312` |
| Live ingestion lag | `2` blocks |
| Base full-history worker | Not configured |
| Historical policy | Per-address full discovery via optional Blockscout v2 keyset adapter; bounded fallback when unavailable |
| Sample observed transaction | Same hash and block resolved independently from Base RPC |
| Robinhood cross-read | Observation unavailable on Robinhood data plane |
| Wrong chain query on Base plane | Observation unavailable when queried as `4663` |

## Web routing audit

The control-plane chain registry has independent routes for Base and Robinhood Chain. The Web request layer resolves the selected chain before constructing API/indexer requests and rejects an explicit response `chain_id` mismatch.

## Implemented remediation

Searcher Cluster Intelligence v1 now returns a fail-closed source attestation containing:

```text
chain_id
data_source
indexed_from_block
indexed_to_block
chain_head
ingestion_lag
last_updated_at
history_mode
available
limitation
```

The read path validates the selected chain, API plane, indexer plane, and RPC
chain before returning intelligence. A Base request sent to the Robinhood data
plane, and a Robinhood request sent to the Base plane, return HTTP `503` with a
network-specific unavailable message. They do not return empty or cross-chain
results.

The source object is rendered as chain, indexed range, head, lag, history mode,
and update time. URLs, credentials, hostnames, and private infrastructure
identifiers are excluded from the contract and UI.

## Post-implementation runtime verification

Point-in-time values on 2026-08-21:

| Network | Live indexed through | Chain head | Lag | Result |
|---|---:|---:|---:|---|
| Base | 50,242,127 | 50,242,130 | 3 blocks | Available, address-lazy history |
| Robinhood Chain | 41,866,260 | 41,866,263 | 3 blocks | Available, partial historical range |

The current external Base seed snapshot reports
`data_source=live_canonical_index_with_address_lazy_history` and explicitly says
that global full history is not contiguous. Base queried as chain `4663`
returned HTTP `503`: `Robinhood Chain live intelligence is not available on the
current data source`.
