# LedgerFlow Public Product Website v1 — Results

Date: 2026-08-17

## Outcome

The credential-free surface now presents LedgerFlow as the **Execution Intelligence and Evidence Layer for EVM ecosystems**. It separates public research and evidence from the scoped private product without changing the internal application, indexer, analysis API, or Searcher logic.

## Public route map

| Route | Purpose | Status |
|---|---|---|
| `/` | Positioning, evidence model, public/private boundary, Pilot CTA | PASS |
| `/solutions` | Protocol, Chain, Searcher, and Evidence API solutions | PASS |
| `/research` | Technical publication with categories, dates, and reading time | PASS |
| `/research/{slug}` | Markdown-backed research article | PASS |
| `/evidence` | Filterable public economic-execution evidence records | PASS |
| `/evidence/{slug}` | Conclusion, impact, participants, timeline, evidence, entities, limitations | PASS |
| `/api` | Open schemas separated from commercial API access | PASS |
| `/about` | Positioning, public goods, commercial layer, evidence principles | PASS |
| `/pilot` | Scoped professional-engagement request | PASS |

Compatibility redirects:

- `/reports` → `/research`
- `/cases` → `/evidence`
- `/products` → `/solutions`
- detail and Markdown deep links preserve their slug
- `/pricing` → `/solutions#engagement-model`

## Access boundary

### Public

- Research
- Evidence Explorer
- Evidence Pack and Execution Event schemas

### Pilot

- 10–30 monitored entities
- rules and Telegram alerts
- private investigations
- weekly reports

### Enterprise

- scoped API
- private datasets
- broader operational intelligence

No billing, automated upgrade, CRM, or new authentication system was added.

## Evidence policy

The explorer is backed by existing fixed Base regression and public Evidence Pack artifacts. Unknown dates, prices, profit, protocols, intent, and ownership are not fabricated.

One liquidation-like record contains conflicting Aave recognizer and Morpho configured-subject attribution. It is published as `Detected / Unverified` with the conflict stated as a limitation. The two records whose narrow conclusions reconcile against their required evidence are labeled `Verified`.

## Verification

| Check | Result |
|---|---|
| `cargo fmt --check` | PASS |
| `cargo check --all-targets` | PASS |
| Web unit tests | PASS — 26/26 |
| `cargo clippy --all-targets --no-deps` | PASS with pre-existing non-blocking warnings |
| JSON syntax: both schemas and public event corpus | PASS |
| Docker production build | PASS |
| New public routes | PASS — HTTP 200 |
| Legacy public redirects | PASS — HTTP 308 |
| Migration 0041 | PASS — `organization` and `contact` present |
| Web-only deployment | PASS |
| App / Indexer continuity | PASS — neither container restarted |

## Known limitations

- The Evidence Explorer currently publishes three selected records, not the entire 39-event Demo corpus.
- Public evidence is Base-only. Robinhood Chain remains an internal live-indexing/data-plane capability.
- A transaction timestamp is unavailable in the fixed deterministic USDC accounting fixture and is displayed as unavailable.
- Pilot provisioning and plan changes remain operator-managed.
- Commercial API scope is explained but not self-provisioned.
