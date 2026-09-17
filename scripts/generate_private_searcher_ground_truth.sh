#!/usr/bin/env bash
set -euo pipefail

# Generates an ignored regression input from the live Searcher configuration.
# It never prints keys or addresses. The resulting file must not be committed.
source_env="${DEXMONEY_ENV_FILE:-/home/ubuntu/dexmoney/.env}"
output="${LEDGERFLOW_CLUSTER_GROUND_TRUTH_FILE:-config/intelligence/searcher-cluster-ground-truth.local.json}"

read_env() {
  local key="$1"
  awk -F= -v key="$key" '$1 == key {sub(/^[^=]*=/, ""); print; exit}' "$source_env"
}

normalize_address() {
  printf '%s' "$1" | tr '[:upper:]' '[:lower:]'
}

fund_address="$(normalize_address "$(read_env EOA_ADDRESS_1)")"
fund_key="$(read_env EOA_PRIVATE_KEY_1)"
pool_size="$(read_env EXECUTION_EOA_POOL_SIZE)"
pool_size="${pool_size:-1}"

workers='[]'
if [[ "$pool_size" -le 1 ]]; then
  workers="$(jq -n --arg value "$fund_address" '[$value]')"
else
  for ((index=0; index<pool_size; index++)); do
    worker=""
    for ((salt=0; salt<128; salt++)); do
      derived_key="$(cast keccak "dexmoney-worker-eoa-v1:${fund_key}:${index}:${salt}")"
      if worker="$(cast wallet address --private-key "$derived_key" 2>/dev/null)"; then
        break
      fi
    done
    [[ -n "$worker" ]] || { echo "worker derivation failed" >&2; exit 1; }
    worker="$(normalize_address "$worker")"
    workers="$(jq --arg value "$worker" '. + [$value] | unique' <<<"$workers")"
  done
fi

executor_keys=(
  EXECUTOR_CONTRACT VAULT_EXECUTOR_CONTRACT FLASH_CYCLE_EXECUTOR_LITE_CONTRACT
  FLASH_CYCLE_EXECUTOR_THREE_POOL_LITE_CONTRACT
  FLASH_CYCLE_EXECUTOR_V3_THREE_POOL_LITE_CONTRACT
  FLASH_CYCLE_EXECUTOR_V4_LITE_CONTRACT
  FLASH_CYCLE_EXECUTOR_V4_THREE_POOL_LITE_CONTRACT
  FLASH_CYCLE_EXECUTOR_V3_FOUR_POOL_LITE_CONTRACT
  FLASH_CYCLE_EXECUTOR_V4_FOUR_POOL_LITE_CONTRACT
  FLASH_CYCLE_EXECUTOR_V3_V4_LITE_CONTRACT
  FLASH_CYCLE_EXECUTOR_V3_V4_RETURN_LITE_CONTRACT
  FLASH_CYCLE_EXECUTOR_V3_V2_LITE_CONTRACT
  FLASH_CYCLE_EXECUTOR_V2_V2_LITE_CONTRACT
  FLASH_CYCLE_EXECUTOR_V2_V3_V3_LITE_CONTRACT
)
executors='[]'
for key in "${executor_keys[@]}"; do
  value="$(normalize_address "$(read_env "$key")")"
  if [[ "$value" =~ ^0x[0-9a-f]{40}$ && "$value" != "0x0000000000000000000000000000000000000000" ]]; then
    executors="$(jq --arg value "$value" '. + [$value] | unique' <<<"$executors")"
  fi
done

settlements='[]'
for key in FLASH_LITE_PROFIT_RECIPIENT VAULT_PROFIT_RECIPIENT; do
  value="$(normalize_address "$(read_env "$key")")"
  if [[ "$value" =~ ^0x[0-9a-f]{40}$ && "$value" != "0x0000000000000000000000000000000000000000" ]]; then
    settlements="$(jq --arg value "$value" '. + [$value] | unique' <<<"$settlements")"
  fi
done

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT
jq -n \
  --arg seed "$fund_address" \
  --argjson workers "$workers" \
  --argjson executors "$executors" \
  --argjson settlements "$settlements" \
  '{cases:[{
    name:"robinhood-internal-searcher",
    chain_id:4663,
    api_base_url:"http://127.0.0.1:3000",
    seed_address:$seed,
    known_workers:$workers,
    known_executors:$executors,
    known_settlement_recipients:$settlements
  }]}' >"$tmp_dir/ground-truth.json"
install -m 600 "$tmp_dir/ground-truth.json" "$output"
printf 'Generated private ground truth: 1 case, %s workers, %s executors, %s settlement recipients.\n' \
  "$(jq '.cases[0].known_workers|length' "$output")" \
  "$(jq '.cases[0].known_executors|length' "$output")" \
  "$(jq '.cases[0].known_settlement_recipients|length' "$output")"
