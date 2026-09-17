# LedgerFlow Pricing and Access Model v1

Date: 2026-08-16

This document defines product access, not prices or billing enforcement. LedgerFlow v1 does not implement checkout, invoices, subscriptions, Stripe, or automated plan upgrades.

## Access surfaces

### Public

No account required.

- Research reports
- Public Evidence Explorer records
- Selected verified event examples
- Public Evidence Pack and methodology artifacts
- Limited explorer and public transaction explanations

### Free

`workspace_plan = free`

- Public Research, Evidence Explorer, and open schemas
- Workspace onboarding may save a draft configuration
- Read-only Base Execution Intelligence Demo Workspace
- Demo events, monitored entities, report, rules, and investigation use public evidence only
- No private customer monitoring or private intelligence delivery
- No SLA

### Pilot

`workspace_plan = pilot`

Pilot applications are free. Access is reviewed and manually released in limited evaluation cohorts; payment is not required for the current Pilot trial.

- Customer-defined monitored addresses and contracts
- Telegram alerts
- Custom rules and thresholds
- Private investigations
- Scheduled weekly reports
- Operator-assisted onboarding and a Pilot service target

### Enterprise

`workspace_plan = enterprise`

- Pilot capabilities
- Scoped private API access
- Higher scoped limits
- Multiple destinations and workspace configurations
- Custom retention and export support
- Agreed SLA and escalation path
- Integration planning for private infrastructure, subject to a separate scope

## Feature matrix

| Capability | Public | Free | Pilot | Enterprise |
|---|:---:|:---:|:---:|:---:|
| Research and Evidence Explorer | Yes | Yes | Yes | Yes |
| Selected public Evidence Packs | Yes | Yes | Yes | Yes |
| Public Evidence Explorer | Yes | Yes | Yes | Yes |
| Read-only Demo Workspace | No | Yes | Yes | Yes |
| Custom monitored entities | No | No | Yes | Yes |
| Custom alert rules | No | No | Yes | Yes |
| Telegram delivery | No | No | Yes | Yes |
| Private investigations | No | No | Yes | Yes |
| Scheduled weekly reports | No | Sample | Yes | Yes |
| Private API | No | No | No | Scoped |
| SLA | No | No | Pilot target | Contracted |

## Permission abstraction

The application stores one of:

```text
free
pilot
enterprise
```

The Rust `WorkspacePlan` model maps a plan to capabilities. The abstraction is additive and deliberately separate from payment status. A future billing system may request a plan change, but it must not become the source of evidence or customer configuration.

Initial migration behavior assigns existing and newly registered users to `free`. Pilot trial activation and revocation are operator-managed database changes during v1. Automated trial expiry is not implemented yet, so operators must review grants on a schedule.

## Enforcement in Workspace Productization v1

- Login resolves to onboarding when the account has no completed workspace.
- Free custom workspaces remain saved drafts and render an explicit activation gate.
- The Demo Workspace is read-only and is populated only from the checked-in public Base Pilot configuration and Evidence Packs.
- Pilot enables private monitored entities, rules, Telegram delivery, investigations, and customer reports.
- Enterprise enables scoped private API key management and contracted integration scope.
- Private event lists, entity profiles, searches, Evidence JSON, reports, monitors, and investigations are constrained by an owned `workspace_id` and/or monitored-entity intersection.

The plan abstraction remains independent of billing. No payment, checkout, or automated plan-upgrade system exists. Pilot access currently represents an approved evaluation grant, not proof of payment.
