# Phase 1 architecture

```mermaid
flowchart LR
    RPC[Base RPC chain 8453] --> IDX[Existing indexer]
    IDX --> WATCH[Existing backfill worker and Pilot-scoped watcher]
    WATCH --> ANALYSIS[Existing deterministic analysis pipeline]
    ANALYSIS --> TX[(transaction_analysis / flows)]
    YAML[Pilot YAML] --> IMPORT[Config validator/importer]
    IMPORT --> CFG[(projects / entities / rules / destinations)]
    TX --> EVENTS[Pilot event worker]
    CFG --> EVENTS
    EVENTS --> PACK[Deterministic Evidence Pack v1 validation]
    PACK --> STORE[(pilot_events / checkpoints / outbox)]
    STORE --> API[Health, metrics, events, evidence and report API]
    STORE --> ALERT[Telegram delivery worker]
    ALERT --> TG[Telegram]
    STORE --> REPORT[Completed UTC week report worker]
    REPORT --> FILES[Markdown / HTML]
    REPORT --> DBREPORT[(weekly report archive)]
```

The config importer bridges each enabled continuous Pilot to an existing address-cluster tracking row with auto-discovery disabled, so the customer-provided 10–30 address boundary stays exact. The existing indexer/backfill path performs range splitting, deduplication, checkpointing and analysis; the Pilot event worker then reads Base facts from the analysis tables. The LLM boundary is after deterministic facts and is optional language rendering only. It does not select protocols, identities, amounts, status, profit, purpose, or flows. Phase 1 workers do not participate in Searcher execution.
