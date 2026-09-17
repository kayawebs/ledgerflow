#!/usr/bin/env node

import readline from 'node:readline';

const rpcUrl = process.env.BASE_RPC_URL;
if (!rpcUrl) throw new Error('BASE_RPC_URL is required');

const transferTopic = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef';

async function rpc(method, params, timeoutMs = 8_000) {
  const response = await fetch(rpcUrl, {
    method: 'POST',
    headers: {'content-type': 'application/json'},
    body: JSON.stringify({jsonrpc: '2.0', id: 1, method, params}),
    signal: AbortSignal.timeout(timeoutMs),
  });
  const body = await response.json();
  if (body.error) throw new Error(`${method}: ${body.error.message}`);
  return body.result;
}

function addressFromTopic(topic) {
  return `0x${topic.slice(-40).toLowerCase()}`;
}

function tokenNets(receipt, scope) {
  const nets = new Map();
  for (const log of receipt.logs || []) {
    if (log.topics?.length !== 3 || log.topics[0]?.toLowerCase() !== transferTopic || !/^0x[0-9a-fA-F]{64}$/.test(log.data || '')) continue;
    const from = addressFromTopic(log.topics[1]);
    const to = addressFromTopic(log.topics[2]);
    const token = log.address.toLowerCase();
    const amount = BigInt(log.data);
    let net = nets.get(token) || 0n;
    if (scope.has(to) && !scope.has(from)) net += amount;
    if (scope.has(from) && !scope.has(to)) net -= amount;
    nets.set(token, net);
  }
  return nets;
}

function nativeBoundaryNet(frame, scope) {
  if (!frame || frame.error) return 0n;
  const from = frame.from?.toLowerCase();
  const to = frame.to?.toLowerCase();
  const amount = BigInt(frame.value || '0x0');
  let net = 0n;
  if (to && scope.has(to) && !scope.has(from)) net += amount;
  if (from && scope.has(from) && !scope.has(to)) net -= amount;
  for (const child of frame.calls || []) net += nativeBoundaryNet(child, scope);
  return net;
}

const input = readline.createInterface({input: process.stdin, crlfDelay: Infinity});
for await (const line of input) {
  const [txHash, sender, entryContract, swapCount, poolCount] = line.trim().split('|');
  if (!txHash || !sender || !entryContract) continue;
  const scope = new Set([sender.toLowerCase(), entryContract.toLowerCase()]);
  try {
    const receipt = await rpc('eth_getTransactionReceipt', [txHash]);
    const nets = tokenNets(receipt, scope);
    const positives = [...nets].filter(([, value]) => value > 0n);
    const negatives = [...nets].filter(([, value]) => value < 0n);
    if (positives.length !== 1 || negatives.length !== 0) continue;
    const trace = await rpc('debug_traceTransaction', [txHash, {tracer: 'callTracer'}]);
    const nativeNet = nativeBoundaryNet(trace, scope);
    if (nativeNet !== 0n) continue;
    process.stdout.write(`${JSON.stringify({
      tx_hash: txHash,
      sender,
      entry_contract: entryContract,
      swap_log_count: Number(swapCount),
      distinct_pool_count: Number(poolCount),
      profit_token_address: positives[0][0],
      receipt_scope_surplus_raw: positives[0][1].toString(),
      trace_native_net_raw: nativeNet.toString(),
      status: 'requires_archive_balance_reconciliation',
    })}\n`);
  } catch (error) {
    process.stderr.write(`${txHash} skipped: ${error.message}\n`);
  }
}
