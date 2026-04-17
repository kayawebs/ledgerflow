# Architecture

## Goal

LedgerFlow is a Rust modular monolith for evidence-first transaction analysis. It treats raw EVM evidence as the source of truth, derives stable facts from that evidence, and only then applies recognizers and scenario interpretation.

## Pipeline

1. `ingestion`
   Fetch raw transaction, receipt, optional trace, and bytecode.
2. `normalization`
   Convert raw payloads into normalized execution facts plus evidence refs.
3. `flows`
   Derive native and ERC20 flows from transaction value, traces, and logs.
4. `recognition`
   Run deterministic recognizers against normalized facts and flows.
5. `scenarios`
   Produce attempt/opportunity hints from recognizer output.
6. `accounting`
   Emit rollup-ready accounting scaffolding.
7. `evidence_pack`
   Build replayable normalized export artifacts.
8. `validation`
   Enforce replay metadata and sanity checks.
9. `repositories`
   Persist evidence metadata, analyses, and evidence packs.

## Stable core vs extension surfaces

- Stable fields live in Rust domain structs and explicit SQL columns.
- Scenario-specific or experimental data lives in `serde_json::Value` extension fields.
- Recognizers append labels and unresolved details instead of mutating the core schema.

## Determinism

Determinism is enforced through:

- canonical JSON hashing for `input_digest`
- explicit `ruleset_id`, `pipeline_version`, and `flow_derivation_version`
- per-recognizer version capture
- deterministic recognizer ordering
- replay endpoints that reject unavailable rulesets or recognizers

## Why scenario-driven instead of pretending to be fully generic

LedgerFlow is meant to become pseudo-general through layered recognition, not by claiming to understand every transaction up front. Narrow recognizers and adapters are safer, easier to version, and easier to replay.

