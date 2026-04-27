# Analysis Web Site Map

This document defines the intended information architecture for the analysis web product.

## Public

- `/`
  - positioning
  - product framing
  - primary CTA
- `/pricing`
  - plan
  - beta pricing state
- `/auth`
  - request signup code
  - request login code
- `/verify`
  - code verification

## Authenticated App

- `/app`
  - redirect to `/app/clusters`

- `/app/clusters`
  - cluster list
  - create cluster
  - cluster onboarding state

- `/app/clusters/{cluster_id}`
  - cluster coverage
  - spend summary
  - recent backfill runs
  - future subnavigation:
    - overview
    - flows
    - counterparties
    - graph

- `/app/cluster-graph`
  - graph roadmap today
  - later becomes graph workspace entry

- `/app/tx-analysis`
  - single tx analyze / load / replay
  - future tx search / recent tx list

## Future Expansion

- `/app/clusters/{cluster_id}/flows`
- `/app/clusters/{cluster_id}/counterparties`
- `/app/clusters/{cluster_id}/graph`
- `/app/tx-analysis/{tx_hash}`
- `/app/recognizers`
- `/app/jobs`

## Navigation Principles

- cluster management and tx analysis should be separate top-level routes
- cluster detail should be path-based, not only query-tab-based
- graph work should have a dedicated route and page identity
- the internal analysis API remains private; the web app is the product surface
