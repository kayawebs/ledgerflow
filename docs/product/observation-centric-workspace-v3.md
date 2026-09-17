# LedgerFlow Observation-Centric Workspace v3

## Product center

The private product exposes one recurring customer concept:

```text
Observed Object → Intelligence Profile → Continuous Observation → Alerts / Briefs
```

`Entity`, monitor records, rules, destinations, events, and evidence remain internal implementation objects. They are not separate setup requirements in the primary user flow.

## Navigation

- **Workspace:** Overview, Observed Objects, Reports.
- **Research:** Research.
- **Developer:** API.
- **Account:** Settings.

Events and investigations remain reachable from the related object or evidence context. Monitoring is enabled from an Intelligence Profile. Notification channels are configured once in Settings.

## Routes

- `/app/overview` — meaningful changes across the workspace's observed objects.
- `/app/observations` — observed-object directory and `+ Observe` flow.
- `/app/observations/:address` — primary Intelligence Profile.
- `POST /app/observations/:address/monitoring` — enables evidence-backed default monitoring without a rule wizard.
- `/app/reports?entity=:address` — object-scoped Intelligence Brief context.
- `/app/settings` — workspace and notification-channel configuration.
- `/research` — public technical research.

Legacy `/app/entities` and entity-detail URLs redirect to the observation routes. Existing event, monitor, and investigation handlers remain available for contextual deep links and compatibility, but are absent from primary navigation.

## ObservedObject adapter

The typed frontend model wraps the existing deterministic model:

```text
ObservedObject
  └─ EntityProfile
      ├─ Execution Events
      └─ Evidence
```

The adapter records the target, object type, workspace, observed-since timestamp, monitoring state, and existing `EntityProfile`. It does not duplicate chain facts or change indexer/API contracts.

## Observation and monitoring behavior

Adding an address or contract persists the existing workspace entity and immediately opens its profile. Protocols and pools currently require a contract address. A transaction hash opens the existing one-time transaction analysis because an immutable transaction is not presented as continuously watchable.

`Enable Monitoring` creates a system-owned default policy for supported existing event types:

- material asset flow;
- DEX arbitrage activity;
- liquidations;
- repeated failed attempts;
- unknown material interactions.

No notification destination is required. When a Telegram channel is connected in Settings, it is applied to all active object watches in that workspace. Email and Webhook remain explicitly unavailable.

## Evidence boundary

- Workspace-entered names and roles remain `Unverified`.
- Completed transaction-analysis samples are `Observed` behavior, not lifetime coverage.
- Relationships mean observed interaction, associated evidence role, or possible relationship; never ownership.
- Economic totals include only deterministic, verified values. Missing values remain `Unknown`.
- Detector candidates remain `Detected · Unverified`.

## Known limitations

- Continuous observation supports existing event classifiers only.
- Arbitrary protocol names are not resolved; a protocol contract address is required.
- Transaction observations are one-time analyses rather than continuous watches.
- On-demand Intelligence Brief generation still depends on the existing report worker.
- Relationship and lifetime economic coverage remain bounded by available deterministic evidence.
