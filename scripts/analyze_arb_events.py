#!/usr/bin/env python3
"""Summarize depth-arb forensics JSONL (opportunities + executions).

Usage:
  python3 scripts/analyze_arb_events.py logs/arb_events_*.jsonl
  python3 scripts/analyze_arb_events.py logs/arb_events_123.jsonl --top 20
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable


def load_events(paths: Iterable[Path]) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for path in paths:
        with path.open() as f:
            for line_no, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    events.append(json.loads(line))
                except json.JSONDecodeError as e:
                    print(f"warn: {path}:{line_no}: {e}", file=sys.stderr)
    return events


def pct(n: int, d: int) -> str:
    if d == 0:
        return "n/a"
    return f"{100.0 * n / d:.1f}%"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("files", nargs="+", type=Path, help="arb_events JSONL files")
    parser.add_argument("--top", type=int, default=15, help="Top N pairs to show")
    args = parser.parse_args()

    events = load_events(args.files)
    if not events:
        print("No events found.")
        return 1

    opps = [e for e in events if e.get("event") == "arb_opportunity"]
    execs = [e for e in events if e.get("event") == "arb_execution"]
    momentum = [e for e in events if e.get("event") == "momentum_execution"]

    print(f"Files: {len(args.files)}")
    print(
        f"Events: {len(events)}  opportunities={len(opps)}  "
        f"executions={len(execs)}  momentum={len(momentum)}"
    )
    print()

    if momentum:
        outcomes = Counter(e.get("outcome", "?") for e in momentum)
        print("=== Momentum outcomes ===")
        for key, count in outcomes.most_common():
            print(f"  {key:20s}  {count:6d}  ({pct(count, len(momentum))})")
        pnls_m = [
            float(e["realized_pnl"]) for e in momentum if e.get("realized_pnl") is not None
        ]
        if pnls_m:
            print(
                f"  realized_pnl: sum={sum(pnls_m):.4f}  "
                f"mean={sum(pnls_m)/len(pnls_m):.4f}"
            )
        holds = [(float(e.get("hold_ms", 0)), float(e.get("realized_pnl", 0))) for e in momentum]
        if holds:
            print("  hold_ms vs pnl (bucketed):")
            buckets: defaultdict[str, list[float]] = defaultdict(list)
            for hold_ms, pnl in holds:
                if hold_ms < 10.0:
                    label = "1-10ms"
                elif hold_ms < 100.0:
                    label = "10-100ms"
                else:
                    label = "100-1000ms"
                buckets[label].append(pnl)
            for label in ("1-10ms", "10-100ms", "100-1000ms"):
                vals = buckets.get(label)
                if vals:
                    print(
                        f"    {label:12s}  n={len(vals):5d}  "
                        f"mean_pnl={sum(vals)/len(vals):.4f}"
                    )
        print()

    # --- Funnel / decisions ---
    decisions = Counter(e.get("decision", "?") for e in opps)
    print("=== Opportunity decisions (funnel) ===")
    for key, count in decisions.most_common():
        print(f"  {key:20s}  {count:6d}  ({pct(count, len(opps))})")
    print()

    stop_reasons = Counter(e.get("stop_reason", "?") for e in opps)
    print("=== Walk stop_reason ===")
    for key, count in stop_reasons.most_common():
        print(f"  {key:24s}  {count:6d}  ({pct(count, len(opps))})")
    print()

    triggers = Counter(e.get("trigger", "?") for e in opps)
    print("=== trigger ===")
    for key, count in triggers.most_common():
        print(f"  {key:24s}  {count:6d}  ({pct(count, len(opps))})")
    print()

    # --- Expected economics ---
    pnls = [float(e["expected_pnl"]) for e in opps if e.get("expected_pnl") is not None]
    vols = [float(e["depth_volume"]) for e in opps if e.get("depth_volume") is not None]
    if pnls:
        pnls_sorted = sorted(pnls)
        mid = pnls_sorted[len(pnls_sorted) // 2]
        print("=== Expected PnL (from opportunities) ===")
        print(f"  count={len(pnls)}  sum={sum(pnls):.4f}  mean={sum(pnls)/len(pnls):.4f}  median={mid:.4f}")
        print(f"  min={pnls_sorted[0]:.4f}  max={pnls_sorted[-1]:.4f}")
        print()
    if vols:
        print("=== Depth volume ===")
        print(f"  mean={sum(vols)/len(vols):.6f}  max={max(vols):.6f}")
        print()

    # --- Top pairs by opportunity count / expected pnl ---
    by_pair_count: Counter[str] = Counter()
    by_pair_pnl: defaultdict[str, float] = defaultdict(float)
    for e in opps:
        pair = f"{e.get('pair1','?')}->{e.get('pair2','?')}"
        by_pair_count[pair] += 1
        by_pair_pnl[pair] += float(e.get("expected_pnl") or 0.0)

    print(f"=== Top {args.top} pairs by opportunity count ===")
    for pair, count in by_pair_count.most_common(args.top):
        print(f"  {count:5d}  pnl_sum={by_pair_pnl[pair]:8.4f}  {pair}")
    print()

    # --- Executions / expected vs actual ---
    if execs:
        outcomes = Counter(e.get("outcome", "?") for e in execs)
        print("=== Execution outcomes ===")
        for key, count in outcomes.most_common():
            print(f"  {key:20s}  {count:6d}  ({pct(count, len(execs))})")
        print()

        opp_by_id = {e.get("opportunity_id"): e for e in opps if "opportunity_id" in e}
        matched = 0
        expected_sum = 0.0
        realized_sum = 0.0
        buy_slip = []
        sell_slip = []
        for ex in execs:
            oid = ex.get("opportunity_id")
            opp = opp_by_id.get(oid)
            if opp is not None:
                matched += 1
                expected_sum += float(opp.get("expected_pnl") or 0.0)
            realized_sum += float(ex.get("realized_pnl") or 0.0)
            if ex.get("buy_slippage_bps") is not None:
                buy_slip.append(float(ex["buy_slippage_bps"]))
            if ex.get("sell_slippage_bps") is not None:
                sell_slip.append(float(ex["sell_slippage_bps"]))

        print("=== Expected vs actual (joined on opportunity_id) ===")
        print(f"  executions with matching opportunity: {matched}/{len(execs)}")
        print(f"  sum expected_pnl (matched): {expected_sum:.4f}")
        print(f"  sum realized_pnl (all execs): {realized_sum:.4f}")
        if buy_slip:
            print(f"  mean buy_slippage_bps:  {sum(buy_slip)/len(buy_slip):.2f}")
        if sell_slip:
            print(f"  mean sell_slippage_bps: {sum(sell_slip)/len(sell_slip):.2f}")
        print()
    else:
        print("=== Executions ===")
        print("  (none — expected in evaluation-only mode)")
        print()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
