# Extraction Notes

## Current Decision

`ledgerflow-indexer/` starts as a subdirectory in the existing repository.

This is preferred over a Git submodule for now because:

- current extraction still needs cross-project refactors
- local development is simpler
- one commit can change both the analysis integration and indexer API client
- CI is easier to bootstrap

Later, split this directory into a separate repository once the API and deployment model stabilize.

## Reusing Existing Data

The existing indexed data is reusable if the new service points to the same ClickHouse database and tables:

```text
database: ledgerflow_indexer
tables: address_tx_refs, indexer_progress
worker_id: base_mainnet_top_level
```

If running ClickHouse from the parent compose stack, keep using the parent named volume. Do not run `docker compose down -v` in the parent project unless you intentionally want to delete indexed data.

## Runtime Options

Option A, safest during extraction:

1. Parent project runs ClickHouse.
2. `ledgerflow-indexer` runs locally and connects to `http://127.0.0.1:8123`.
3. Existing worker progress continues from `indexer_progress`.

Option B, later:

1. Move ClickHouse into dedicated infrastructure.
2. Point both old and new services at the same ClickHouse host.
3. Stop the old indexer worker.
4. Start the new worker with the same `worker_id`.
