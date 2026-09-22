#!/usr/bin/env python3
"""Per-query medians and workload aggregates from the raw compilation measurements.

Reads `cedardb_compilation_raw_SF<n>.csv` and writes two files:

  cedardb_compilation_per_query_SF<n>.csv   one row per query: the median PREPARE and EXPLAIN
                                            time in every compilation mode, and the derived
                                            compilation time per mode
  cedardb_compilation_summary_SF<n>.csv     one row per suite and mode: n, median, mean, p90,
                                            max and sum of the derived compilation time

The derivation is per query, never on aggregates:

    compilation(query, mode) = median PREPARE(query, mode) - median PREPARE(query, Interpreted)

A median of differences is not a difference of medians in general; here both are taken over the
same five repetitions of the same statement, and the per-query subtraction is the one that keeps
the pairing. Aggregates are then formed over the per-query differences.

Negative values are possible on the cheapest queries, where the difference is at the level of the
measurement floor. They are kept, not clamped: clamping would bias the mean upward, and the floor
is reported beside the aggregates so the reader can see the scale.
"""
from __future__ import annotations

import argparse
import collections
import csv
import statistics
from pathlib import Path

MODE_ORDER = ["i", "A", "d", "c", "o"]
MODE_NAMES = {"i": "Interpreted", "A": "Auto (default)", "d": "DirectEmit",
              "c": "Cheap", "o": "Optimized"}
SUITE_ORDER = {"prodds": 0, "tpcds": 1}


def p90(values: list[float]) -> float:
    s = sorted(values)
    return s[int(0.9 * (len(s) - 1))]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--scale", default="100")
    ap.add_argument("--raw", default=None)
    args = ap.parse_args()
    sf = args.scale
    raw = Path(args.raw or f"cedardb_compilation_raw_SF{sf}.csv")

    # samples[(suite, query, mode, stage)] -> list of ms
    samples: dict[tuple, list[float]] = collections.defaultdict(list)
    floors: dict[tuple, list[float]] = collections.defaultdict(list)
    bad = []
    for r in csv.DictReader(raw.open(encoding="utf-8")):
        key = (r["suite"], r["query_id"], r["mode"], r["stage"])
        if r["status"] != "ok":
            bad.append((r["suite"], r["query_id"], r["mode"], r["status"]))
            continue
        ms = float(r["elapsed_ms"])
        (floors if r["query_id"] == "__floor__" else samples)[key].append(ms)

    queries = sorted({(s, q) for (s, q, _, _) in samples}, key=lambda x: (SUITE_ORDER[x[0]], x[1]))
    modes = [m for m in MODE_ORDER if any(k[2] == m for k in samples)]

    med = {k: statistics.median(v) for k, v in samples.items()}
    reps = max((len(v) for v in samples.values()), default=0)

    per_query = []
    for suite, q in queries:
        row = {"suite": suite, "query_id": q, "n_repetitions": reps}
        base = med.get((suite, q, "i", "prepare"))
        for m in modes:
            row[f"explain_{m}_ms"] = f"{med[(suite, q, m, 'explain')]:.3f}"
            row[f"prepare_{m}_ms"] = f"{med[(suite, q, m, 'prepare')]:.3f}"
        for m in modes:
            if m == "i" or base is None:
                continue
            row[f"compilation_{m}_ms"] = f"{med[(suite, q, m, 'prepare')] - base:.3f}"
        per_query.append(row)

    out_pq = Path(f"cedardb_compilation_per_query_SF{sf}.csv")
    with out_pq.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(per_query[0]))
        w.writeheader()
        w.writerows(per_query)

    summary = []
    for suite in sorted({s for s, _ in queries}, key=lambda s: SUITE_ORDER[s]):
        for m in modes:
            if m == "i":
                continue
            vals = [float(r[f"compilation_{m}_ms"]) for r in per_query if r["suite"] == suite]
            prep = [float(r[f"prepare_{m}_ms"]) for r in per_query if r["suite"] == suite]
            summary.append([suite, m, MODE_NAMES[m], len(vals),
                            f"{statistics.median(vals):.1f}", f"{statistics.fmean(vals):.1f}",
                            f"{p90(vals):.1f}", f"{max(vals):.1f}", f"{min(vals):.1f}",
                            f"{sum(vals) / 1000:.2f}", f"{statistics.median(prep):.1f}",
                            sum(1 for v in vals if v > 1000)])
    out_sum = Path(f"cedardb_compilation_summary_SF{sf}.csv")
    with out_sum.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["suite", "mode", "mode_name", "n_queries", "median_compilation_ms",
                    "mean_compilation_ms", "p90_compilation_ms", "max_compilation_ms",
                    "min_compilation_ms", "sum_compilation_s", "median_prepare_ms",
                    "n_queries_over_1s"])
        w.writerows(summary)

    print(f"{len(per_query)} queries x {len(modes)} modes x {reps} repetitions")
    print(f"  {out_pq}\n  {out_sum}")
    if bad:
        print(f"  {len(bad)} measurements not ok: {bad[:5]}")
    print("\n  round-trip floor, PREPARE ... AS SELECT 1, median over repetitions:")
    for m in modes:
        vals = [v for k, v in floors.items() if k[2] == m for v in [statistics.median(v)]]
        if vals:
            print(f"    mode {m} ({MODE_NAMES[m]:14s}): {statistics.median(vals):7.3f} ms")

    print()
    for row in summary:
        print(f"  {row[0]:7s} {row[2]:14s}  n={row[3]:3d}  median {row[4]:>8s} ms  "
              f"mean {row[5]:>8s}  p90 {row[6]:>8s}  max {row[7]:>9s}  "
              f"sum {row[9]:>7s} s  >1s: {row[11]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
