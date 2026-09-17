#!/usr/bin/env node

import fs from 'node:fs/promises';

const fixturePath = process.argv[2] || 'tests/fixtures/base_protocol_ground_truth.jsonl';
const datasets = await Promise.all([
  'config/intelligence/base-core.v1.json',
  'config/intelligence/base-phase1-protocols.v1.json',
].map(async path => JSON.parse(await fs.readFile(path, 'utf8'))));
const deployments = new Map();
for (const dataset of datasets) {
  for (const entry of dataset.addresses || []) {
    for (const deployment of entry.protocol_deployments || []) {
      deployments.set(entry.address.toLowerCase(), deployment.protocol_key);
    }
  }
}
const rows = (await fs.readFile(fixturePath, 'utf8')).trim().split('\n').map(line => JSON.parse(line));
let truePositive = 0, falsePositive = 0, falseNegative = 0;
const perProtocol = {};
for (const row of rows) {
  const predicted = deployments.get(row.evidence_address.toLowerCase()) || null;
  if (predicted && predicted === row.expected_protocol) truePositive++;
  else if (predicted) falsePositive++;
  else if (row.expected_protocol) falseNegative++;
  if (row.expected_protocol) {
    const stats = perProtocol[row.expected_protocol] ||= {reviewed: 0, correct: 0};
    stats.reviewed++;
    if (predicted === row.expected_protocol) stats.correct++;
  }
}
const precision = truePositive / Math.max(1, truePositive + falsePositive);
const recall = truePositive / Math.max(1, truePositive + falseNegative);
process.stdout.write(`${JSON.stringify({dataset_rows: rows.length, true_positive: truePositive, false_positive: falsePositive, false_negative: falseNegative, precision, recall, per_protocol: perProtocol}, null, 2)}\n`);
