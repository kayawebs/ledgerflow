# Public Base Demo

This directory is a reproducible, credential-free Pilot artifact generated from public Base transaction/receipt/log evidence on 2026-08-11. It contains 27 events in the displayed UTC week and 39 total events across the captured run.

- Config: `config/pilots/demo-base-public.yaml`
- Weekly Markdown: `demo/base-public/weekly_report.md`
- Weekly HTML: `demo/base-public/weekly_report.html`
- Weekly Evidence Packs: `demo/base-public/events.json`
- All Demo Evidence Packs: `demo/base-public/all_events.json`
- Fixed 100-transaction input facts, including raw Transfer-log values: `tests/fixtures/base_regression_dataset.jsonl`

Named cases:

| Case | Base transaction hash |
|---|---|
| Arbitrage executor interaction; profit intentionally null | `0x0a0309ba7b2d9268a30a7022f3ea33b6db3998628364dad56ddeb7ddc25887d1` |
| Aave liquidation | `0x6f4c472b846a2d5530ece4596a39c5cb9f088f0ad2e3804f80f95c8b2289d40a` |
| Complex asset flow | `0x57b5c3bddf71686b4b435930c075412e43783a283d6ed894350fa787f4ccba03` |
| Reverted transactions | `0xb46c6a9f36c797d6fe10d3cd87103c3fd825e2974608bf971a6e818e4ea82d13`, `0x3cc4dedd31b2ee4e2458f65f6a9ac2ee66d1805f60718aafaea82b9e443b8b55` |

When the local stacks are running, the continuously published feed is available at `http://127.0.0.1:3001/public/evidence/events.json` and the Evidence Explorer at `http://127.0.0.1:3001/evidence`. The embedded files remain the fail-safe bootstrap corpus. No private key, private RPC, Searcher infrastructure, Telegram credential or customer address is included.
