#!/usr/bin/env python3
"""Summarize depth-arb forensics JSONL (opportunities + executions).

Usage:
  python3 scripts/analyze_arb_events.py logs/arb_events_*.jsonl
  python3 scripts/analyze_arb_events.py logs/arb_events_123.jsonl --top 20
  python3 scripts/analyze_arb_events.py logs/arb_events_*.jsonl --since 2026-07-18T20:00:00Z

Win rate denominator: outcomes in {filled, partial_buy}; win = realized_pnl > 0.
PnL residual (pnl_error) = realized_pnl - expected_pnl.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

TRADED_OUTCOMES = frozenset({"filled", "partial_buy"})


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


def parse_time_arg(value: str) -> float:
    """Parse ISO-8601 or unix seconds into unix seconds (float)."""
    raw = value.strip()
    try:
        return float(raw)
    except ValueError:
        pass
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    dt = datetime.fromisoformat(raw)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def event_ts_s(event: dict[str, Any]) -> float | None:
    """Per-event clock in unix seconds, or None if missing (old logs)."""
    kind = event.get("event")
    if kind == "arb_opportunity":
        ts = event.get("eval_ts_ns")
    else:
        ts = event.get("event_ts_ns")
    if ts is None:
        return None
    try:
        return float(ts) / 1e9
    except (TypeError, ValueError):
        return None


def in_window(
    ts: float | None, since: float | None, until: float | None
) -> bool:
    if ts is None:
        # Old logs without timestamps: drop when a window is active.
        return since is None and until is None
    if since is not None and ts < since:
        return False
    if until is not None and ts > until:
        return False
    return True


def mean(vals: list[float]) -> float:
    return sum(vals) / len(vals) if vals else float("nan")


def rmse(vals: list[float]) -> float:
    if not vals:
        return float("nan")
    return math.sqrt(sum(v * v for v in vals) / len(vals))


def is_win(event: dict[str, Any]) -> bool:
    return float(event.get("realized_pnl") or 0.0) > 0.0


def traded(event: dict[str, Any]) -> bool:
    return event.get("outcome") in TRADED_OUTCOMES


def bin_data_age_ms(age: float) -> str:
    if age < 1.5:
        return "0-1.5ms"
    if age < 5.0:
        return "1.5-5ms"
    if age < 10.0:
        return "5-10ms"
    return "10ms+"


def bin_blended_roi(roi: float) -> str:
    if roi < 1.002:
        return "<1.002"
    if roi < 1.005:
        return "1.002-1.005"
    if roi < 1.01:
        return "1.005-1.01"
    return ">=1.01"


def bin_depth_multiplier(m: float) -> str:
    if m <= 1.0:
        return "<=1x"
    if m <= 2.0:
        return "1-2x"
    if m <= 5.0:
        return "2-5x"
    return ">5x"


def bin_hold_ms(hold: float) -> str:
    if hold < 10.0:
        return "1-10ms"
    if hold < 100.0:
        return "10-100ms"
    return "100-1000ms"


def print_win_rate_table(title: str, rows: list[tuple[str, list[dict[str, Any]]]]) -> None:
    print(f"=== {title} ===")
    print(f"  (win = realized_pnl > 0 among {sorted(TRADED_OUTCOMES)})")
    for label, group in rows:
        n = len(group)
        wins = sum(1 for e in group if is_win(e))
        print(f"  {label:16s}  n={n:5d}  wins={wins:5d}  win_rate={pct(wins, n)}")
    print()


def print_residual_bins(
    title: str,
    bins: list[tuple[str, list[dict[str, Any]]]],
    *,
    with_slippage: bool = False,
) -> None:
    print(f"=== {title} ===")
    print("  pnl_error = realized_pnl - expected_pnl")
    for label, group in bins:
        errors = [float(r["pnl_error"]) for r in group if r.get("pnl_error") is not None]
        n = len(group)
        line = f"  {label:16s}  n={n:5d}"
        if errors:
            line += f"  mean_err={mean(errors):+.4f}  rmse={rmse(errors):.4f}"
        if with_slippage:
            buy = [float(r["buy_slippage_bps"]) for r in group if r.get("buy_slippage_bps") is not None]
            sell = [
                float(r["sell_slippage_bps"]) for r in group if r.get("sell_slippage_bps") is not None
            ]
            if buy:
                line += f"  buy_slip={mean(buy):+.2f}bps"
            if sell:
                line += f"  sell_slip={mean(sell):+.2f}bps"
        print(line)
    print()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("files", nargs="+", type=Path, help="arb_events JSONL files")
    parser.add_argument("--top", type=int, default=15, help="Top N pairs to show")
    parser.add_argument(
        "--since",
        type=str,
        default=None,
        help="Keep events at/after this time (ISO-8601 or unix seconds)",
    )
    parser.add_argument(
        "--until",
        type=str,
        default=None,
        help="Keep events at/before this time (ISO-8601 or unix seconds)",
    )
    args = parser.parse_args()

    since = parse_time_arg(args.since) if args.since else None
    until = parse_time_arg(args.until) if args.until else None

    all_events = load_events(args.files)
    if not all_events:
        print("No events found.")
        return 1

    kept: list[dict[str, Any]] = []
    dropped = 0
    for e in all_events:
        if in_window(event_ts_s(e), since, until):
            kept.append(e)
        else:
            dropped += 1

    window_desc = "all time"
    if since is not None or until is not None:
        since_s = args.since or "-inf"
        until_s = args.until or "+inf"
        window_desc = f"{since_s} .. {until_s}"

    print(f"Files: {len(args.files)}")
    print(f"Time window: {window_desc}  kept={len(kept)}  dropped={dropped}")

    opps = [e for e in kept if e.get("event") == "arb_opportunity"]
    execs = [e for e in kept if e.get("event") == "arb_execution"]
    momentum = [e for e in kept if e.get("event") == "momentum_execution"]

    print(
        f"Events: {len(kept)}  opportunities={len(opps)}  "
        f"executions={len(execs)}  momentum={len(momentum)}"
    )
    print()

    # --- Momentum summary (incl. win rate vs hold) ---
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
        print()

        traded_m = [e for e in momentum if traded(e)]
        hold_groups: dict[str, list[dict[str, Any]]] = {
            "1-10ms": [],
            "10-100ms": [],
            "100-1000ms": [],
        }
        for e in traded_m:
            hold_groups[bin_hold_ms(float(e.get("hold_ms") or 0.0))].append(e)
        print_win_rate_table(
            "Momentum win rate vs hold_ms",
            [(label, hold_groups[label]) for label in hold_groups],
        )
        print("=== Momentum mean realized_pnl vs hold_ms ===")
        for label in hold_groups:
            vals = [float(e.get("realized_pnl") or 0.0) for e in hold_groups[label]]
            if vals:
                print(f"  {label:16s}  n={len(vals):5d}  mean_pnl={mean(vals):.4f}")
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
        print(
            f"  count={len(pnls)}  sum={sum(pnls):.4f}  "
            f"mean={sum(pnls)/len(pnls):.4f}  median={mid:.4f}"
        )
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

    # --- Executions / joins ---
    opp_by_id = {e.get("opportunity_id"): e for e in opps if "opportunity_id" in e}

    if execs:
        outcomes = Counter(e.get("outcome", "?") for e in execs)
        print("=== Execution outcomes ===")
        for key, count in outcomes.most_common():
            print(f"  {key:20s}  {count:6d}  ({pct(count, len(execs))})")
        print()

        matched = 0
        expected_sum = 0.0
        realized_sum = 0.0
        buy_slip: list[float] = []
        sell_slip: list[float] = []
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

        # Joined traded rows for residual / win-rate analyses.
        joined: list[dict[str, Any]] = []
        for ex in execs:
            if not traded(ex):
                continue
            opp = opp_by_id.get(ex.get("opportunity_id"))
            expected = float(opp.get("expected_pnl") or 0.0) if opp else None
            realized = float(ex.get("realized_pnl") or 0.0)
            row = {
                **ex,
                "trigger": (opp or {}).get("trigger", "?"),
                "blended_roi": (opp or {}).get("blended_roi", (opp or {}).get("bbo_roi")),
                "bbo_roi": (opp or {}).get("bbo_roi"),
                "depth_multiplier": (opp or {}).get("depth_multiplier"),
                "expected_pnl": expected,
                "pnl_error": (realized - expected) if expected is not None else None,
            }
            joined.append(row)

        # 1. Win rate vs trigger
        by_trigger: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in joined:
            by_trigger[str(row.get("trigger") or "?")].append(row)
        print_win_rate_table(
            "Win rate vs trigger",
            sorted(by_trigger.items(), key=lambda kv: (-len(kv[1]), kv[0])),
        )

        # 2. Slippage + residual vs data_age_ms
        age_order = ("0-1.5ms", "1.5-5ms", "5-10ms", "10ms+")
        by_age: dict[str, list[dict[str, Any]]] = {k: [] for k in age_order}
        for row in joined:
            age = row.get("data_age_ms")
            if age is None:
                continue
            by_age[bin_data_age_ms(float(age))].append(row)
        print_residual_bins(
            "Slippage + PnL residual vs data_age_ms",
            [(k, by_age[k]) for k in age_order],
            with_slippage=True,
        )

        # 3. Residual vs expected ROI (blended_roi)
        roi_order = ("<1.002", "1.002-1.005", "1.005-1.01", ">=1.01")
        by_roi: dict[str, list[dict[str, Any]]] = {k: [] for k in roi_order}
        for row in joined:
            roi = row.get("blended_roi")
            if roi is None:
                continue
            by_roi[bin_blended_roi(float(roi))].append(row)
        print_residual_bins(
            "PnL residual vs blended_roi",
            [(k, by_roi[k]) for k in roi_order],
        )

        # 4. Residual vs depth_multiplier
        depth_order = ("<=1x", "1-2x", "2-5x", ">5x")
        by_depth: dict[str, list[dict[str, Any]]] = {k: [] for k in depth_order}
        for row in joined:
            mult = row.get("depth_multiplier")
            if mult is None:
                continue
            by_depth[bin_depth_multiplier(float(mult))].append(row)
        print_residual_bins(
            "PnL residual vs depth_multiplier",
            [(k, by_depth[k]) for k in depth_order],
        )
    else:
        print("=== Executions ===")
        print("  (none — expected in evaluation-only mode)")
        print()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
