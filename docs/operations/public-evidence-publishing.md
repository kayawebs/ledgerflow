# Continuous public evidence publishing

LedgerFlow publishes Evidence Packs through an explicit, fail-closed path:

```text
Base indexed transaction
  -> deterministic transaction analysis
  -> Pilot classifier
  -> schema-valid Evidence Pack
  -> public project authorization
  -> event-specific verification gate
  -> /public/evidence/events.json
  -> Evidence Explorer (15 second cache)
```

## Authorization boundary

`project.public_publish` defaults to `false`. A project is returned by the
unauthenticated feed only when its stored configuration explicitly contains:

```yaml
project:
  public_publish: true
```

Customer projects must not enable this flag. `enabled` and
`continuous_monitoring` do not grant publication permission.

The current public producer is `ledgerflow-public-base-demo` in
`config/pilots/demo-base-public.yaml`. It contains public protocol, token, and
demonstration addresses only.

## Publication gates

- `large_asset_flow` is Verified only when the receipt succeeded and every
  published flow has an exact unsigned raw amount and evidence reference.
- `high_cost_failure` is Verified only when the receipt reverted and exact raw
  gas cost is present.
- `dex_arbitrage` is Verified only when deterministic accounting is complete,
  exact raw profit exists, and exact raw gas cost exists.
- Other event types are currently published as Detected / Unverified. Detector
  confidence alone never promotes an event.
- Missing values remain unknown. Raw token values are not converted to USD
  without deterministic historical pricing.

Every pack is validated against Evidence Pack v1. Publication fails closed if
the JSON contains credential or customer-identity fields such as `private_key`,
`rpc_url`, `secret_ref`, `chat_id`, `api_key`, or `customer_id`.

## Bootstrap

Create the internal web-to-feed network once per Docker host:

```bash
docker network create ledgerflow-public-evidence
```

Import the public project and fixed regression corpus idempotently:

```bash
docker compose --env-file config/pilots/example-base.env \
  -p ledgerflow-base-pilot run --rm --no-deps pilot-event-worker \
  ledgerflow_pilot_config /app/config/pilots/demo-base-public.yaml \
  --events /app/demo/base-public/all_events.json
```

After bootstrap, `ledgerflow_pilot_event_worker` writes newly classified events
through the normal persistence path. No recurring corpus import is required.

## Endpoints and verification

- Producer feed: `http://127.0.0.1:3201/public/evidence/events.json`
- Public site proxy/feed: `http://127.0.0.1:3001/public/evidence/events.json`
- Explorer: `http://127.0.0.1:3001/evidence`

Check the public scope and count:

```bash
curl -fsS http://127.0.0.1:3001/public/evidence/events.json | \
  jq '{count:length, projects:(map(.project_id)|unique)}'
```

The project list must contain only explicitly public projects. The Explorer uses
a 15-second cache and falls back to the embedded 39-event public corpus if the
live producer is unavailable. The page names the active source so an operator
can distinguish live data from fallback data.

