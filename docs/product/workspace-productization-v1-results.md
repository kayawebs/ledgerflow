# LedgerFlow Workspace Productization v1 Results

Date: 2026-08-18

## Product boundary

| Requirement | Result | Evidence |
|---|---|---|
| Public pages prove capability without customer intelligence | PASS | Public navigation remains Home, Solutions, Research, Evidence Explorer, API, About. |
| Logged-in users do not default to Global Explorer | PASS | Login resolves to onboarding or the first owned private/demo workspace. No Global workspace is created. |
| Reports separate public Research from Customer Reports | PASS | `/research` is public; `/app/reports` contains only owned workspace outputs or the explicitly public Demo artifact. |
| Entities are monitored intelligence | PASS | `/app/entities` lists only `workspace_entities`; an unmonitored address returns not found. |
| Events are workspace-specific | PASS | Event lists and details require participant intersection with an owned workspace entity. |
| Evidence export respects workspace access | PASS | Authenticated Evidence JSON checks membership in at least one owned workspace. |

## Onboarding and customer workflow

| Requirement | Result | Evidence |
|---|---|---|
| First login enters onboarding | PASS | Accounts without a completed workspace redirect to `/app/onboarding`. |
| Four onboarding steps | PASS | Workspace type, entities, monitoring goals, and notification destination are persisted. |
| Create first workspace and add entities | PASS | `intelligence_workspaces` and `workspace_entities` are migration-backed. |
| Create initial monitor rules | PASS | Pilot/Enterprise onboarding materializes goals as `user_monitor_rules`; Free retains a non-operational draft. |
| Demo Workspace | PASS | Uses `config/pilots/demo-base-public.yaml` and `demo/base-public/events.json`; includes 19 entities, 27 evidence records, rules, one investigation, and the generated weekly report. |
| Telegram/Webhook/Email abstraction | PASS | Preferences support all three. Only supported Pilot/Enterprise Telegram configuration is marked operational. |

## Access model

| Plan | Result |
|---|---|
| Free | Public Evidence Layer plus read-only public-data Demo Workspace; custom configuration is saved as a draft. |
| Pilot | Private workspace, monitors, rules, Telegram alerts, investigations, and customer reports. |
| Enterprise | Pilot features plus scoped private API and SLA abstraction. |

Billing, payment, new chains, new protocol classifiers, indexer changes, and Searcher changes were not implemented.

## Engineering and deployment verification

| Check | Result | Evidence |
|---|---|---|
| Rust formatting | PASS | `cargo fmt --all -- --check` completed successfully. |
| Type/build check | PASS | `cargo check --all-targets` completed successfully; legacy dead-code warnings remain. |
| Automated tests | PASS | `cargo test --all-targets` completed with zero failures, including 112 library tests and the fixed 100-transaction Base regression suite. |
| Production image | PASS | The release Docker image built successfully. |
| Database migration | PASS | SQLx migration 42 is recorded successful and all four workspace tables exist. |
| Runtime health | PASS | The deployed v2 Web process reports `status: ok` on `127.0.0.1:3001/health`. |
| Public/private route boundary | PASS | Public routes return 200; unauthenticated `/app` access redirects to `/auth`. |
| Minimum UI font size | PASS | No Web stylesheet declaration uses a pixel font size below 12px. |

Only the `ledgerflow-chainactors-v2-web-1` service was recreated for this release. Existing indexers, analysis workers, and Searcher processes were not restarted.

## Known limitations

- Webhook and email destinations are configuration abstractions only; they are explicitly marked non-operational.
- New customer reports still depend on the existing report generator and its persisted snapshots.
- Workspace historical comparison currently reports deterministic 30-day same-type event counts; it does not invent baselines when data is absent.
- The Demo Workspace is a fixed public evidence sample for product evaluation. The public Evidence Explorer remains the continuously updating public stream.
- The legacy public-home aggregate query took about 2.1 seconds during the final cold runtime check; it is outside the private Workspace request path but remains a public-page performance follow-up.
