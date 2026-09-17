# LedgerFlow Entity Intelligence Workspace v2

## Product center

The private application is organized around an entity, not a collection of unrelated features:

```text
Entity → Behavior → Relationships → Economic Intelligence → Timeline → Monitoring / Reports
```

Workspace navigation is `Overview`, `Entity Intelligence`, `Events`, `Reports`, `Monitors`, and `Investigations`. Developer and account navigation remain separate.

## Routes

- `/app/overview` — changes across tracked entities.
- `/app/entities` — workspace entity directory and type filters.
- `/app/entities/:address` — primary intelligence profile.
- `/app/entities?add=1` — add an intelligence target before enabling monitoring.
- `/app/events` — entity-related workspace event stream.
- `/app/reports?entity=:address` — entity-scoped report context and generated snapshots.
- `/app/monitors` — all active monitoring rules and delivery state.
- `/app/investigations` — advanced evidence workflow started from an entity or event.

## EntityProfile adapter

The frontend `EntityProfile` projection is assembled without changing existing APIs:

| Section | Existing source | Evidence boundary |
|---|---|---|
| Identity | workspace role/label, address profile, label assertions | Workspace-entered roles are `Unverified`; registry/human labels are `Observed`; other label assertions are `Inferred`. |
| Behavior | completed address-analysis behavior rows | Percentages describe analyzed samples, not lifetime activity. |
| Relationships | Evidence Pack participants and persisted asset-flow edges | `Associated with` or `Observed interaction`; never `owned by`. |
| Economic intelligence | verified Execution Events and completed sample gas | USD impact is summed only for Verified/Resolved events; unknown values stay `Unknown`. |
| Timeline | workspace-scoped Execution Events | Detector candidates remain `Inferred` / `Detected · Unverified`. |

If the address-summary service is slow or unavailable, the page still renders workspace identity and related event evidence and marks address-level profile/behavior as unavailable.

## Onboarding and demo

Workspace targets are Protocol, Chain, Searcher, Wallet, or Custom. The four steps add initial entities, select evidence focus, and optionally configure monitoring. Completion opens the first entity profile.

The public-data demo is named **Base Searcher Intelligence Demo** and opens the configured public arbitrage executor. It uses checked-in public Evidence Packs and does not expose customer data, RPC configuration, or internal infrastructure.

## Current limitations

- Relationship categories can be empty when no participant-role or asset-flow evidence exists.
- A workspace role is configuration, not an identity proof.
- Verified total profit remains `Unknown` unless deterministic net-profit evidence exists.
- Existing report workers produce persisted snapshots; the UI does not fabricate a new entity report while generation is pending.
- Monitoring supports existing event types only; this refactor adds no classifiers or protocols.
