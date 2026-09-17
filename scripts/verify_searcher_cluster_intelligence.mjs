#!/usr/bin/env node

import { readFile } from "node:fs/promises";

const configPath = process.env.LEDGERFLOW_CLUSTER_GROUND_TRUTH_FILE;
if (!configPath) {
  throw new Error("LEDGERFLOW_CLUSTER_GROUND_TRUTH_FILE is required");
}
const config = JSON.parse(await readFile(configPath, "utf8"));
const cases = Array.isArray(config.cases) ? config.cases : [];
if (cases.length === 0) throw new Error("ground-truth file has no cases");

const normalize = (value) => String(value || "").toLowerCase();
const set = (items) => new Set((items || []).map(normalize));
const ratio = (found, known) => (known.size === 0 ? null : found / known.size);
const percentage = (value) => (value === null ? "N/A" : `${(value * 100).toFixed(1)}%`);
const containsAny = (member, roles) => roles.has(member.candidate_role);

let failed = false;
const results = [];
for (const item of cases) {
  const baseUrl = String(item.api_base_url || "").replace(/\/$/, "");
  const response = await fetch(
    `${baseUrl}/observations/${encodeURIComponent(item.seed_address)}/cluster?chain_id=${item.chain_id}&execution_limit=500`,
  );
  if (!response.ok) {
    results.push({ name: item.name, status: "FAIL", error: `HTTP ${response.status}` });
    failed = true;
    continue;
  }
  const snapshot = await response.json();
  if (Number(snapshot.chain_id) !== Number(item.chain_id)) {
    results.push({ name: item.name, status: "FAIL", error: "chain mismatch" });
    failed = true;
    continue;
  }
  if (snapshot.cluster?.status !== "ready") {
    results.push({
      name: item.name,
      chain_id: item.chain_id,
      status: "NOT_READY",
      stage: snapshot.cluster?.status || "unavailable",
      error: snapshot.cluster?.error || "candidate cluster has not completed",
    });
    failed = true;
    continue;
  }
  const activeMembers = (snapshot.members || []).filter(
    (member) => member.membership_state !== "excluded",
  );
  const discoveredWorkers = set(
    activeMembers
      .filter((member) => member.candidate_role === "worker_candidate")
      .map((member) => member.address),
  );
  const discoveredExecutors = set(
    activeMembers
      .filter((member) => member.candidate_role === "executor_candidate")
      .map((member) => member.address),
  );
  const discoveredSettlements = set(
    activeMembers
      .filter((member) =>
        containsAny(
          member,
          new Set(["settlement_recipient_candidate", "profit_recipient_candidate", "seed"]),
        ),
      )
      .map((member) => member.address),
  );
  const knownWorkers = set(item.known_workers);
  const knownExecutors = set(item.known_executors);
  const knownSettlements = set(item.known_settlement_recipients);
  const found = (known, discovered) => [...known].filter((address) => discovered.has(address)).length;
  const workerRecall = ratio(found(knownWorkers, discoveredWorkers), knownWorkers);
  const executorRecall = ratio(found(knownExecutors, discoveredExecutors), knownExecutors);
  const settlementRecall = ratio(found(knownSettlements, discoveredSettlements), knownSettlements);
  const infrastructure = set(
    (snapshot.relationships || [])
      .filter((relation) => ["pool", "router", "protocol"].includes(relation.relationship_type))
      .map((relation) => relation.related_address),
  );
  const falseInfrastructureMembers = activeMembers.filter((member) =>
    infrastructure.has(normalize(member.address)),
  ).length;
  const fundingAsProfit = (snapshot.executions || []).filter(
    (execution) =>
      ["funding", "settlement"].includes(execution.execution_type) &&
      execution.economic_verification_status === "verified",
  ).length;
  const verifiedWithoutEvidence = (snapshot.executions || []).filter(
    (execution) =>
      execution.economic_verification_status === "verified" &&
      !(execution.evidence_refs || []).some((ref) => ref.kind === "economic_verification"),
  ).length;
  const checks = {
    worker_recall: percentage(workerRecall),
    executor_recall: percentage(executorRecall),
    settlement_recall: percentage(settlementRecall),
    router_pool_false_membership: falseInfrastructureMembers,
    funding_as_profit_errors: fundingAsProfit,
    verified_without_evidence: verifiedWithoutEvidence,
  };
  const pass =
    (workerRecall === null || workerRecall >= 0.9) &&
    (executorRecall === null || executorRecall >= 0.95) &&
    falseInfrastructureMembers === 0 &&
    fundingAsProfit === 0 &&
    verifiedWithoutEvidence === 0;
  if (!pass) failed = true;
  results.push({
    name: item.name,
    chain_id: item.chain_id,
    status: pass ? "PASS" : "FAIL",
    checks,
    discovered_counts: {
      workers: discoveredWorkers.size,
      executors: discoveredExecutors.size,
      settlement_recipients: discoveredSettlements.size,
    },
  });
}

process.stdout.write(`${JSON.stringify({ method: "searcher-cluster-ground-truth.v1", results }, null, 2)}\n`);
if (failed) process.exitCode = 1;
