#!/usr/bin/env node

import { readFile } from "node:fs/promises";

const fixtures = JSON.parse(
  await readFile(new URL("../tests/fixtures/observed_address_intelligence_v1.json", import.meta.url), "utf8"),
);
const apiByChain = new Map([
  [8453, process.env.LEDGERFLOW_BASE_API_URL ?? "http://127.0.0.1:3200"],
  [4663, process.env.LEDGERFLOW_ROBINHOOD_API_URL ?? "http://127.0.0.1:3000"],
]);
const allowInProgress = process.env.LEDGERFLOW_OBSERVATION_ALLOW_IN_PROGRESS === "1";
const allowedStates = new Set([
  "queued",
  "discovering",
  "backfilling",
  "classifying",
  "verifying_economics",
  "building_baseline",
  "ready",
  "stale",
  "failed",
]);
const errors = [];
const results = [];

function check(condition, message) {
  if (!condition) errors.push(message);
}

for (const fixture of fixtures) {
  const api = apiByChain.get(fixture.chain_id);
  const url = `${api}/observations/${fixture.address}?chain_id=${fixture.chain_id}&timeline_limit=500`;
  let snapshot;
  try {
    const response = await fetch(url, { signal: AbortSignal.timeout(10_000) });
    check(response.ok, `${fixture.fixture_id}: HTTP ${response.status}`);
    if (!response.ok) continue;
    snapshot = await response.json();
  } catch (error) {
    errors.push(`${fixture.fixture_id}: ${error.message}`);
    continue;
  }

  const status = snapshot.object?.current_status;
  const progress = snapshot.object?.progress ?? {};
  const windows = new Set((snapshot.activity_windows ?? []).map((window) => window.window));
  const hasCompletedProfile = ["24h", "7d", "30d"].every((window) => windows.has(window))
    && (snapshot.briefs ?? []).length >= 3
    && snapshot.object?.last_incremental_update != null;
  check(snapshot.contract_version === "observed-address-intelligence.v1", `${fixture.fixture_id}: contract version`);
  check(snapshot.chain_id === fixture.chain_id, `${fixture.fixture_id}: chain isolation`);
  check(snapshot.address === fixture.address, `${fixture.fixture_id}: normalized address`);
  check(allowedStates.has(status), `${fixture.fixture_id}: invalid status ${status}`);
  check(snapshot.object?.monitoring_enabled === true, `${fixture.fixture_id}: observation not enabled`);

  if (status === "ready" || status === "stale" || hasCompletedProfile) {
    for (const window of ["24h", "7d", "30d"]) {
      check(windows.has(window), `${fixture.fixture_id}: missing ${window} activity`);
    }
    for (const relationship of snapshot.relationships ?? []) {
      check(
        Array.isArray(relationship.supporting_transactions) && relationship.supporting_transactions.length > 0,
        `${fixture.fixture_id}: relationship without transaction evidence`,
      );
    }
    for (const economic of snapshot.economics ?? []) {
      check(["verified", "estimated", "unknown"].includes(economic.result_status), `${fixture.fixture_id}: economic state`);
      check(economic.coverage_ratio >= 0 && economic.coverage_ratio <= 1, `${fixture.fixture_id}: economic coverage`);
    }
    for (const change of snapshot.changes ?? []) {
      check(
        Array.isArray(change.supporting_transactions) && change.supporting_transactions.length > 0,
        `${fixture.fixture_id}: change signal without transaction evidence`,
      );
    }
    check(
      !(snapshot.limitations ?? []).some((value) => /owned by|same team/i.test(value)),
      `${fixture.fixture_id}: unsupported ownership attribution`,
    );
  } else if (!allowInProgress) {
    errors.push(`${fixture.fixture_id}: analysis is ${status}, expected ready or stale`);
  }

  results.push({
    fixture_id: fixture.fixture_id,
    chain_id: fixture.chain_id,
    address: fixture.address,
    status,
    percent: progress.percent ?? null,
    discovered: progress.transactions_discovered ?? snapshot.object?.discovered_transaction_count ?? null,
    analyzed: progress.transactions_analyzed ?? snapshot.object?.analyzed_transaction_count ?? null,
    coverage_is_partial: snapshot.object?.coverage_is_partial ?? null,
    timeline_rows_returned: snapshot.timeline?.length ?? 0,
    relationship_count: snapshot.relationships?.length ?? 0,
    brief_count: snapshot.briefs?.length ?? 0,
    completed_profile_available: hasCompletedProfile,
  });
}

console.log(JSON.stringify({ checked_at: new Date().toISOString(), allow_in_progress: allowInProgress, results, errors }, null, 2));
if (errors.length > 0) process.exitCode = 1;
