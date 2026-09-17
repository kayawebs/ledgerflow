#!/usr/bin/env node

import fs from 'node:fs/promises';

const regressionPath = process.argv[2] || 'tests/fixtures/base_regression_dataset.jsonl';
const outputPath = process.argv[3] || 'tests/fixtures/base_protocol_ground_truth.jsonl';
const datasetPaths = [
  'config/intelligence/base-core.v1.json',
  'config/intelligence/base-phase1-protocols.v1.json',
];

const deployments = new Map();
const protocolAliases = new Map([
  ['morpho', 'morpho_blue'],
]);
for (const path of datasetPaths) {
  const dataset = JSON.parse(await fs.readFile(path, 'utf8'));
  for (const entry of dataset.addresses || []) {
    for (const deployment of entry.protocol_deployments || []) {
      deployments.set(entry.address.toLowerCase(), {
        protocol: deployment.protocol_key,
        dataset_key: dataset.dataset_key,
        dataset_version: dataset.dataset_version,
      });
    }
  }
}

const rows = (await fs.readFile(regressionPath, 'utf8'))
  .trim()
  .split('\n')
  .map(line => JSON.parse(line));
const groundTruth = [];
for (const row of rows) {
  const to = row.to?.toLowerCase();
  const source = row.source_address?.toLowerCase();
  if (row.expected_protocol === 'transfer') {
    groundTruth.push({
      schema_version: 'ledgerflow.protocol-ground-truth.v1',
      chain_id: 8453,
      tx_hash: row.tx_hash,
      block_number: row.block_number,
      evidence_address: to,
      expected_protocol: null,
      review_status: 'reviewed_negative',
      review_method: 'ordinary_token_contract_call_not_a_protocol_deployment',
      independent_from: 'recognizer_output',
    });
    continue;
  }
  if (!source || !to || source !== to || row.expected_protocol === 'multi_protocol') continue;
  const deployment = deployments.get(to);
  if (!deployment) throw new Error(`reviewed deployment missing for ${to} (${row.expected_protocol})`);
  const expectedProtocol = protocolAliases.get(row.expected_protocol) || row.expected_protocol;
  if (deployment.protocol !== expectedProtocol) {
    throw new Error(`independent label mismatch for ${to}: fixture=${expectedProtocol}, deployment=${deployment.protocol}`);
  }
  groundTruth.push({
    schema_version: 'ledgerflow.protocol-ground-truth.v1',
    chain_id: 8453,
    tx_hash: row.tx_hash,
    block_number: row.block_number,
    evidence_address: to,
    expected_protocol: expectedProtocol,
    source_expected_protocol: row.expected_protocol,
    review_status: 'reviewed_positive',
    review_method: 'direct_top_level_call_to_reviewed_deployment',
    deployment_dataset: `${deployment.dataset_key}@${deployment.dataset_version}`,
    independent_from: 'recognizer_output',
  });
}
if (groundTruth.length !== 95) throw new Error(`expected 95 reviewed rows, got ${groundTruth.length}`);
await fs.writeFile(outputPath, groundTruth.map(row => JSON.stringify(row)).join('\n') + '\n');
process.stdout.write(`wrote ${groundTruth.length} reviewed protocol ground-truth rows to ${outputPath}\n`);
