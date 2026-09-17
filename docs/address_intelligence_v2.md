# Address Intelligence V2

Address Intelligence V2 treats every label as a provenance-bearing assertion.
A curated seed, an official registry entry, an on-chain probe, and a model
classification use the same assertion model. They differ by source, trust,
method version, evidence, validity, and lifecycle.

## Data model

The data-plane database for each chain owns these records:

```text
chain_addresses                  normalized chain-scoped identity
address_contract_states          observed bytecode state and code history
intelligence_sources             source identity and trust policy
label_definitions                label taxonomy and exclusivity rules
intelligence_datasets            imported dataset versions and checksums
address_label_assertions         raw, provenance-preserving claims
assertion_evidence               evidence observations for each claim
effective_address_labels         current policy-resolved read projection
protocols                        protocol catalog
protocol_deployments             chain-specific protocol contract relations
erc20_token_metadata             structured token attributes
```

`address_profiles`, `address_labels`, and `address_protocol_metadata` remain as
V1 compatibility tables during the rolling migration. New analysis writes both
the V2 source of truth and the V1 compatibility tables. Reads use
`effective_address_labels`.

## Important boundaries

- `chain_addresses.address_kind` is physical identity: `eoa`, `contract`,
  `system`, `precompile`, or `unknown`.
- Token, router, pool, protocol, searcher, whale, and behavioral classification
  are semantic labels or structured relations, not physical address kinds.
- Token symbol, name, and decimals remain structured token metadata.
- A protocol deployment is a structured relation between a protocol and a
  chain address. Labels are a convenient projection, not its only storage.
- An identical hexadecimal address on two chains is not automatically the same
  actor. Cross-chain actor ownership belongs in the control plane.

## Assertion lifecycle

Assertions use one of these states:

- `active`: eligible for the effective-label projection
- `superseded`: retained for audit but replaced by a newer dataset or method
- `retracted`: explicitly withdrawn because it was incorrect or unsafe

An effective score is currently calculated as:

```text
assertion confidence × source trust score
```

For an exclusive label group, only the highest-scoring active assertion is
projected. Non-exclusive labels can coexist. Raw assertions are never deleted
when the projection changes.

## Versioned dataset import

Services no longer insert address seeds at startup. Import reviewed data with:

```bash
export LEDGERFLOW_DATABASE_URL=postgres://...
cargo run --bin ledgerflow_intelligence_import -- \
  config/intelligence/base-core.v1.json
```

The importer:

1. validates chain ID, EVM addresses, address kinds, confidence, and checksum;
2. records the source and complete dataset manifest;
3. imports labels as `curated_seed` assertions;
4. records per-assertion evidence;
5. imports token attributes and protocol deployments;
6. supersedes older versions without deleting their history;
7. performs the entire import in one PostgreSQL transaction.

`config/intelligence/robinhood-mainnet.template.json` is intentionally empty
and marked as a template. The importer rejects templates. Populate it only
with verified Robinhood mainnet addresses, review it, remove the template
marker, and then import it into the Robinhood chain database.

## Chain data-plane configuration

Each chain server uses the same schema but a different database and immutable
chain ID. Native asset pricing must also be chain-local:

```text
LEDGERFLOW_DEFAULT_CHAIN_ID=4663
LEDGERFLOW_DATABASE_URL=postgres://.../ledgerflow_robinhood
LEDGERFLOW_RPC_URL=http://127.0.0.1:8545
LEDGERFLOW_NATIVE_TOKEN_SYMBOL=ETH
```

The wrapped-native address comes from the active `wrapped_native` database
assertion produced by registry sync. Native balances are excluded from USD
portfolio valuation until that assertion exists; no chain address is embedded
in application configuration or code.

## Rolling migration

Migration `0019_address_intelligence_v2` is additive and backfills current V1
records. Deploy it before the new binaries. During the compatibility window:

1. migrate one chain database;
2. deploy the new address worker;
3. confirm assertions and evidence are growing;
4. compare V1 labels with `effective_address_labels`;
5. deploy remaining workers and APIs;
6. remove V1 dual writes only in a later release.

Useful checks:

```sql
select source_key, source_kind, trust_score from intelligence_sources order by source_key;

select status, count(*)
from address_label_assertions
group by status;

select chain_id, address, label, source, confidence
from effective_address_labels
order by updated_at desc
limit 50;

select dataset_key, dataset_version, chain_id, status, checksum_sha256
from intelligence_datasets
order by imported_at desc;
```
