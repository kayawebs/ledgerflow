#!/usr/bin/env node
import fs from 'node:fs/promises';

const rpc = process.env.BASE_RPC_URL || 'https://mainnet.base.org';
const args = process.argv.slice(2);
const refreshExisting = args.includes('--refresh-existing');
const output = args.find(value => !value.startsWith('--')) || 'tests/fixtures/base_regression_dataset.jsonl';
const transferTopic = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef';
const sources = [
  ['uniswap_v2','0x4752ba5dbc23f44d87826276bf6fd6b1c372ad24'],
  ['uniswap_v3','0x2626664c2603336e57b271c5c0b26f421741e481'],
  ['uniswap_v4','0x498581ff718922c3f8e6a244956af099b2652b2b'],
  ['aerodrome','0xcf77a3ba9a5ca399b7c97c74d54e5b1beb874e43'],
  ['pancakeswap','0x678aa4bf4e210cf2166753e054d5b7c31cc7fa86'],
  ['balancer','0xba1333333333a1ba1108e8412f11850a5c319ba9'],
  ['morpho','0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb'],
  ['aave','0xa238dd80c259a72e81d7e4664a9801593f98d1c5'],
  ['moonwell','0xfbb21d0380bee3312b33c4353c8936a0f13ef26c'],
  ['transfer','0x833589fcd6edb6e08f4c7c32d4f71b54bda02913'],
];
const fixed = [
  ['0x0a0309ba7b2d9268a30a7022f3ea33b6db3998628364dad56ddeb7ddc25887d1','multi_protocol','dex_arbitrage'],
  ['0x6f4c472b846a2d5530ece4596a39c5cb9f088f0ad2e3804f80f95c8b2289d40a','aave','liquidation'],
  ['0xb46c6a9f36c797d6fe10d3cd87103c3fd825e2974608bf971a6e818e4ea82d13','aave','reverted_transaction'],
  ['0x3cc4dedd31b2ee4e2458f65f6a9ac2ee66d1805f60718aafaea82b9e443b8b55','aave','reverted_transaction'],
  ['0x57b5c3bddf71686b4b435930c075412e43783a283d6ed894350fa787f4ccba03','multi_protocol','complex_asset_flow'],
];

async function getJson(url, options) {
  for (let attempt=0; attempt<8; attempt++) {
    const response=await fetch(url,{...options,signal:AbortSignal.timeout(20_000)});
    if (response.ok) return response.json();
    await new Promise(resolve=>setTimeout(resolve,750*Math.min(16,2**attempt)));
  }
  throw new Error(`request failed: ${url}`);
}
async function rpcCall(method, params) {
  const body=await getJson(rpc,{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({jsonrpc:'2.0',id:1,method,params})});
  if (body.error) throw new Error(`${method}: ${body.error.message}`);
  return body.result;
}
let candidates=[];
if (refreshExisting) {
  const existing=(await fs.readFile(output,'utf8')).trim().split('\n').map(line=>JSON.parse(line));
  candidates=existing.map(record=>({hash:record.tx_hash,protocol:record.expected_protocol,kind:record.expected_case,source_address:record.source_address}));
} else {
  const seen=new Set();
  for (const [hash,protocol,kind] of fixed) { seen.add(hash); candidates.push({hash,protocol,kind,source_address:null}); }
  for (const [sourceIndex,[protocol,address]] of sources.entries()) {
    process.stderr.write(`discover ${protocol}\n`);
    const page=await getJson(`https://base.blockscout.com/api/v2/addresses/${address}/transactions`);
    const quota=sourceIndex<5?10:9; let added=0;
    for (const item of page.items || []) {
      const hash=item.hash?.toLowerCase(); if (!hash || seen.has(hash)) continue;
      seen.add(hash); candidates.push({hash,protocol,kind:protocol==='transfer'?'ordinary_transfer':'protocol_interaction',source_address:address}); added++;
      if (added>=quota) break;
    }
    await new Promise(resolve=>setTimeout(resolve,750));
  }
}
const records=[];
for (const candidate of candidates) {
  if (records.length>=100) break;
  try {
    const [tx,receipt]=await Promise.all([rpcCall('eth_getTransactionByHash',[candidate.hash]),rpcCall('eth_getTransactionReceipt',[candidate.hash])]);
    if (!tx || !receipt) continue;
    const gasUsed=BigInt(receipt.gasUsed); const gasPrice=BigInt(receipt.effectiveGasPrice || tx.gasPrice || '0x0');
    const executionGasCost=gasUsed*gasPrice; const l1Fee=BigInt(receipt.l1Fee || '0x0');
    const transferLogs=(receipt.logs || []).filter(log=>log.topics?.length===3 && log.topics[0]?.toLowerCase()===transferTopic && /^0x[0-9a-fA-F]{64}$/.test(log.data || '')).map(log=>({log_index:Number(BigInt(log.logIndex)),token_address:log.address.toLowerCase(),from_address:`0x${log.topics[1].slice(-40).toLowerCase()}`,to_address:`0x${log.topics[2].slice(-40).toLowerCase()}`,value_raw:BigInt(log.data).toString()}));
    records.push({schema_version:'ledgerflow.base-regression.v3',chain_id:8453,tx_hash:candidate.hash,block_number:Number(BigInt(receipt.blockNumber)),transaction_index:Number(BigInt(receipt.transactionIndex)),from:tx.from.toLowerCase(),to:tx.to?.toLowerCase() || null,value_raw:BigInt(tx.value).toString(),input_selector:(tx.input || '0x').slice(0,10),status:BigInt(receipt.status)===1n?'success':'failure',gas_used_raw:gasUsed.toString(),effective_gas_price_raw:gasPrice.toString(),execution_gas_cost_raw:executionGasCost.toString(),l1_fee_raw:l1Fee.toString(),gas_cost_raw:(executionGasCost+l1Fee).toString(),log_count:receipt.logs.length,transfer_logs:transferLogs,expected_protocol:candidate.protocol,expected_case:candidate.kind,source_address:candidate.source_address,evidence_sources:['eth_getTransactionByHash','eth_getTransactionReceipt']});
    if (records.length%10===0) process.stderr.write(`verified ${records.length}/100\n`);
    await new Promise(resolve=>setTimeout(resolve,40));
  } catch (error) { process.stderr.write(`skip ${candidate.hash}: ${error.message}\n`); }
}
if (records.length!==100) throw new Error(`expected 100 verified Base transactions, got ${records.length}`);
for (const required of ['uniswap_v2','uniswap_v3','uniswap_v4','aerodrome','pancakeswap','balancer','morpho','aave','moonwell','transfer']) {
  if (!records.some(record=>record.expected_protocol===required)) throw new Error(`missing required protocol ${required}`);
}
if (!records.some(v=>v.expected_case==='dex_arbitrage') || !records.some(v=>v.expected_case==='liquidation') || !records.some(v=>v.status==='failure')) throw new Error('missing required special cases');
await fs.mkdir(output.split('/').slice(0,-1).join('/') || '.',{recursive:true});
await fs.writeFile(output,records.map(record=>JSON.stringify(record)).join('\n')+'\n');
process.stdout.write(`wrote ${records.length} RPC-verified Base transactions to ${output}\n`);
