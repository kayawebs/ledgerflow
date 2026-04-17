# Plugin Development

## Extension points

A plugin can add:

- recognizers
- validators
- later: protocol classifiers, scenario adapters, accounting policies

## Current registration pattern

Rust does not do Python-style runtime module import here. v0 uses explicit registration by plugin id:

1. Add a module under `src/plugins/`.
2. Expose a `register(&mut RecognizerRegistry)` function.
3. Return a `PluginManifest` plus validator instances.
4. Add the plugin id to `LEDGERFLOW_PLUGINS`.

## Recognizer rules

- Consume normalized facts and derived flows, not raw RPC payloads.
- Return confidence, evidence refs, unresolved points, and warnings.
- Never mutate raw evidence.
- Keep recognizer logic deterministic and versioned.

## Safe evolution

Prefer shipping a new recognizer or adapter over silently changing the meaning of an old label. If a recognizer meaning changes materially, bump its version and keep replay outputs traceable.

