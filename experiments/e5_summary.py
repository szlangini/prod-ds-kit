#!/usr/bin/env python3
"""Summarise the E5 sparsity/skew arms the way the paper text reports them.

`export_paper_csv.py` writes two E5 files: the per-query medians and the bootstrap median
delta per arm. The prose, however, quotes the sum over the workload, the median per query,
the p90, the worst query and how many queries moved in each direction. Those were being
recomputed by hand every time. This writes them out, one row per (scale, tier, engine, arm),
so every number in the write-up is traceable to a file.

All deltas are against that engine's and tier's own no-injection baseline, over the queries
both the baseline and the arm completed.

Usage:
    python3 experiments/e5_summary.py --results-dir .reproduce/sf100/results_keyskew \\
        --out experiments/data/paper_csv/E5_summary_SF100.csv
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import statistics as st
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional

REPO = Path(__file__).resolve().parent.parent
ARMS = ["sparsity_only", "skew_only", "keyskew_only", "skew_all", "combined", "full"]
ARM_LABEL = {
    "sparsity_only": "NULL sparsity only",
    "skew_only": "value skew only (MCV)",
    "keyskew_only": "key skew only",
    "skew_all": "value + key skew",
    "combined": "NULL + value skew",
    "full": "NULL + value + key (default)",
}


def template_map(sf: str) -> Dict[int, int]:
    """workload position -> TPC-DS template number, from the generator's permutation."""
    for engine in ("cedardb", "duckdb", "monetdb"):
        p = REPO / f".reproduce/sf{sf}/queries/{engine}/prodds/_permutation.json"
        if p.exists():
            return {int(v): int(k) for k, v in json.loads(p.read_text()).items()}
    return {}


def label(query: str, tmpl: Dict[int, int]) -> str:
    m = re.fullmatch(r"query_(\d+)", query)
    if m and int(m.group(1)) in tmpl:
        return f"Q{tmpl[int(m.group(1))]}"
    return query.replace("query_", "")


def load(per_query_csv: Path):
    data: Dict = defaultdict(lambda: defaultdict(dict))
    with per_query_csv.open(newline="", encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            try:
                ok, ms = int(r["n_success"] or 0), float(r["median_ms"])
            except (TypeError, ValueError):
                continue
            if ok > 0:
                data[(r["engine"], r["tier"])][r["variant"]][r["query_id"]] = ms
    return data


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--results-dir", type=Path, required=True,
                    help="only used to derive the scale factor and locate the per-query CSV")
    ap.add_argument("--per-query", type=Path, default=None,
                    help="default: experiments/data/paper_csv/E5_per_query_SF<N>.csv")
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()

    m = re.search(r"sf(\d+)", str(args.results_dir))
    sf = m.group(1) if m else "NA"
    per_query = args.per_query or REPO / f"experiments/data/paper_csv/E5_per_query_SF{sf}.csv"
    if not per_query.exists():
        print(f"missing {per_query}; run export_paper_csv.py first")
        return 1
    tmpl = template_map(sf)
    data = load(per_query)

    rows: List[List] = []
    for (engine, tier) in sorted(data, key=lambda k: (k[1], k[0])):
        base = data[(engine, tier)].get("baseline")
        if not base:
            continue
        for arm in ARMS:
            var = data[(engine, tier)].get(arm)
            if not var:
                continue
            common = [q for q in base if q in var]
            if not common:
                continue
            deltas = sorted(((var[q] / base[q] - 1) * 100.0, q) for q in common)
            total = (sum(var[q] for q in common) / sum(base[q] for q in common) - 1) * 100.0
            p90 = deltas[int(0.9 * (len(deltas) - 1))][0]
            rows.append([
                f"SF{sf}", tier, engine, arm, ARM_LABEL[arm], len(common),
                f"{sum(base[q] for q in common) / 1000.0:.1f}",
                f"{total:+.1f}", f"{st.median(d for d, _ in deltas):+.1f}",
                f"{p90:+.1f}", f"{deltas[-1][0]:+.1f}", label(deltas[-1][1], tmpl),
                f"{deltas[0][0]:+.1f}", label(deltas[0][1], tmpl),
                sum(1 for d, _ in deltas if d < -2), sum(1 for d, _ in deltas if d > 2),
            ])

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["scale", "tier", "engine", "arm", "arm_label", "n_queries",
                    "baseline_total_s", "sum_delta_pct", "median_delta_pct", "p90_delta_pct",
                    "worst_delta_pct", "worst_query", "best_delta_pct", "best_query",
                    "n_faster_gt2pct", "n_slower_gt2pct"])
        w.writerows(rows)
    print(f"[e5-summary] wrote {args.out} ({len(rows)} rows)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
