# Replay And Versioning

## Replay inputs

A deterministic replay depends on:

- raw transaction payload
- raw receipt payload
- optional trace payload
- optional code payload
- `ruleset_id`
- `pipeline_version`
- `flow_derivation_version`
- recognizer versions
- `config_version`

## Input digest

`input_digest` is SHA-256 over canonical JSON built from:

- `tx`
- `receipt`
- `trace`
- `target_code`
- `version_bundle`

Canonicalization sorts object keys recursively before serialization, so unordered map differences do not change the digest.

## Replay endpoint behavior

- rejects rulesets that are not installed
- rejects recognizers that are not installed
- re-runs the pipeline using the requested installed recognizer subset

## Why this matters

LedgerFlow is intended to be a reproducible facts platform. Analysis must be explainable from evidence refs and explicit versions, not from hidden mutable heuristics in the running process.

