# Cluster Graph Roadmap

This document scopes the cluster relationship graph work for the analysis product.

## Product Goal

Given a cluster and its analyzed history, build a relationship graph that helps an operator answer:

- which external counterparties recur around this cluster
- which cluster addresses repeatedly act together
- which protocols, routers, vaults, or liquidators are central
- how value movement changes over time
- which subgraphs suggest bot coordination, routing, liquidations, arbitrage, or treasury fan-out

The end goal may include a 3D graph view, but rendering is not the first milestone.

## Constraints

- graph data must come from analysis outputs, not raw explorer labels
- graph extraction must remain replayable and versioned
- graph generation must scale across large transaction histories
- the first versions must prioritize queryable graph facts over flashy rendering

## Proposed Delivery Phases

## Phase 1: Graph-Ready Facts

Goal: make graph inputs explicit in the analysis pipeline.

Deliver:

- counterparty extraction per tx
- cluster internal-transfer edges
- repeated protocol-touch edges
- sender -> target -> token motifs
- time-bucketed edge counts
- edge provenance via tx hashes / evidence refs

Output shape:

- nodes: addresses, clusters, contracts, tokens, protocol families
- edges: transfer, interaction, approval, repeated co-occurrence, inferred role relationship

## Phase 2: Aggregation Layer

Goal: build a server-side graph projection that is fast to query.

Deliver:

- graph snapshot builder for a cluster + time window
- weighted edges
- edge categories
- node centrality / degree summaries
- server-side filtering by:
  - time range
  - edge type
  - min edge weight
  - protocol family

Storage direction:

- start with PostgreSQL-backed graph projection tables or materialized JSON snapshots
- only move to a graph-native or columnar companion store if query pressure proves it necessary

## Phase 3: 2D Graph Workspace

Goal: validate operator interaction patterns before 3D work.

Deliver:

- dedicated cluster graph page
- node and edge filtering
- drill-down from graph node -> tx list -> tx analysis
- cluster-to-counterparty neighborhood view
- timeline slicing

Why 2D first:

- cheaper to build
- easier to validate usefulness
- avoids GPU / rendering complexity before data semantics are stable

## Phase 4: 3D Graph

Goal: add a performant 3D graph scene once the graph model is stable.

Deliver:

- GPU-backed rendering
- level-of-detail controls
- server-side graph thinning
- focus mode around a selected address / contract / token
- time animation or temporal scrubber

Performance notes:

- do not ship naive client-side force layout over large raw graphs
- pre-aggregate edges and pre-score importance server-side
- cap default node counts aggressively
- load neighborhoods and expansions incrementally

## Engineering Steps

Recommended implementation order:

1. Add graph-oriented facts to transaction summaries.
2. Add graph projection builder service.
3. Add graph projection API for one cluster + time window.
4. Add a non-3D graph workspace route in the web product.
5. Measure real cluster sizes and interaction density.
6. Only then prototype a 3D renderer.

## Non-Goals For V1

- universal graph of all Base addresses
- real-time global mempool graph
- 3D rendering before graph data quality is proven
- replacing tx analysis with graph-only heuristics
