#!/usr/bin/env python3
"""Execution-level backtest of the 60-second launch signal.

Entry and exit are filled against the real trade tape: we can only buy what the
next real demand bought, and we can only sell into buying that actually showed
up afterwards. Fees and gas are charged on both legs.
"""
import sys
from collections import defaultdict

S = "/tmp/claude-1000/-home-ubuntu-ledgerflow/02e0d6c6-34c5-4865-bd0b-4f4f1d752f90/scratchpad"
WEI = 10**18

# --- parameters -------------------------------------------------------------
SIZES_ETH = [0.05, 0.2, 0.8]          # ~$124 / $496 / $1,984 at $2,480/ETH
SIGNAL_DELAY_S = 60                   # signal fires 60s after launch
FILL_WINDOW_S = 60                    # give up if we cannot fill within a minute
PARTICIPATION = 0.25                  # we may take a quarter of each buy when exiting
GAS_ETH = 0.00005                     # ~3c of gas per transaction, rounded up
CURVE_FEE = 0.01                      # observed 1% curve fee
POOL_FEE = 0.0                        # V4 pools for these launches are fee 0
WINDOW_S = 7200                       # tape covers 2h after launch

def load():
    launches = {}
    trades = defaultdict(list)
    with open(f"{S}/backtest_tape.tsv") as handle:
        for line in handle:
            parts = line.rstrip("\n").split("\\t")
            if len(parts) != 9:
                continue
            lid, launched, grad, t60, ts, side, quote, token, fee = parts
            launches.setdefault(lid, {
                "launched": int(launched),
                "graduated": int(grad),
                "trades60": int(t60),
            })
            trades[lid].append((int(ts), side, int(quote) / WEI, int(token), "curve"))
    with open(f"{S}/backtest_pool.tsv") as handle:
        for line in handle:
            parts = line.rstrip("\n").split("|")
            if len(parts) != 5:
                continue
            lid, ts, side, quote, token = parts
            if lid not in launches:
                continue
            trades[lid].append((int(ts), side, int(quote) / WEI, abs(int(token)), "pool"))
    for lid in trades:
        trades[lid].sort(key=lambda row: row[0])
    return launches, trades


def fee_of(venue):
    return CURVE_FEE if venue == "curve" else POOL_FEE


def fill_entry(tape, t_signal, size):
    """Buy `size` ETH at the price the next real demand paid."""
    spent = 0.0
    tokens = 0.0
    for ts, side, quote, token, venue in tape:
        if ts <= t_signal:
            continue
        if ts > t_signal + FILL_WINDOW_S:
            break
        if side != "buy" or quote <= 0 or token <= 0:
            continue
        take = min(quote, size - spent)
        tokens += token * (take / quote)
        spent += take
        if spent >= size * 0.999:
            return spent, tokens * (1 - fee_of(venue)), ts
    return (spent, tokens * (1 - CURVE_FEE), None) if spent > 0 else (0.0, 0.0, None)


def sell_into(tape, start_ts, tokens, end_ts):
    """Sell `tokens` into buying that actually happened, taking at most
    PARTICIPATION of each trade. Returns (proceeds, tokens_left)."""
    left = tokens
    proceeds = 0.0
    for ts, side, quote, token, venue in tape:
        if ts < start_ts or ts > end_ts:
            continue
        if side != "buy" or quote <= 0 or token <= 0:
            continue
        capacity_tokens = token * PARTICIPATION
        sold = min(left, capacity_tokens)
        price = quote / token
        proceeds += sold * price * (1 - fee_of(venue))
        left -= sold
        if left <= tokens * 1e-9:
            break
    return proceeds, left


def price_at(tape, ts):
    last = None
    for row in tape:
        if row[0] > ts:
            break
        if row[2] > 0 and row[3] > 0:
            last = row[2] / row[3]
    return last


def run(size):
    launches, trades = load()
    strategies = {
        "hold_5m": None,
        "hold_15m": None,
        "hold_30m": None,
        "hold_60m": None,
        "target_2x_or_30m": None,
        "at_graduation": None,
    }
    results = {name: [] for name in strategies}
    filled = 0
    skipped = 0
    for lid, meta in launches.items():
        tape = trades.get(lid, [])
        if not tape:
            skipped += 1
            continue
        t_launch = meta["launched"]
        t_signal = t_launch + SIGNAL_DELAY_S
        end = t_launch + WINDOW_S
        spent, tokens, entry_ts = fill_entry(tape, t_signal, size)
        if entry_ts is None or tokens <= 0 or spent < size * 0.5:
            skipped += 1
            continue
        filled += 1
        entry_price = spent / tokens
        cost = spent + GAS_ETH

        for name in strategies:
            if name.startswith("hold_"):
                minutes = int(name.split("_")[1].rstrip("m"))
                trigger = entry_ts + minutes * 60
            elif name == "at_graduation":
                if not meta["graduated"]:
                    trigger = entry_ts + 30 * 60
                else:
                    trigger = meta["graduated"]
            else:
                # Sell on a double, else after 30 minutes.
                trigger = entry_ts + 30 * 60
                target = entry_price * 2
                for ts, side, quote, token, _venue in tape:
                    if ts <= entry_ts or ts > end:
                        continue
                    if token > 0 and quote > 0 and (quote / token) >= target:
                        trigger = ts
                        break
            proceeds, left = sell_into(tape, trigger, tokens, end)
            # Anything still held when the tape ends is written off: there was
            # no demand left to sell it into.
            pnl = proceeds - cost - GAS_ETH
            results[name].append((pnl, pnl / size, left / tokens))
    return results, filled, skipped


def summarize(name, rows, size):
    if not rows:
        return f"{name:>18}  no fills"
    pnls = sorted(r[0] for r in rows)
    rets = sorted(r[1] for r in rows)
    total = sum(pnls)
    wins = sum(1 for p in pnls if p > 0)
    stuck = sum(1 for r in rows if r[2] > 0.05)
    mid = len(rets) // 2
    return (
        f"{name:>18}  trades={len(rows):5d}  win={100*wins/len(rows):5.1f}%  "
        f"median={100*rets[mid]:7.1f}%  mean={100*sum(rets)/len(rets):7.1f}%  "
        f"total={total:+9.3f} ETH on {size*len(rows):8.1f} ETH deployed  "
        f"stuck={100*stuck/len(rows):4.1f}%"
    )


if __name__ == "__main__":
    for size in SIZES_ETH:
        results, filled, skipped = run(size)
        print(f"\n=== size {size} ETH  (filled {filled}, skipped {skipped}) ===")
        for name, rows in results.items():
            print(summarize(name, rows, size))
