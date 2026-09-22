#!/usr/bin/env python3
"""Threshold table (paper Fig 3b metric) from WorkloadLens data_metrics.jsonl.

Formula matches paper/signals/aggregate_signals.py: max_count / row_count
(NULLs in the denominator). Prints MCV + NULL rows for each given jsonl,
alongside the Redshift fleet reference and the committed paper rows.
Usage: wl_mcv_table.py NAME=path.jsonl [NAME=path.jsonl ...] [--top N]
"""
import json
import sys

THRESHOLDS = (0.01, 0.10, 0.30, 0.50, 0.70, 0.90)
REFERENCE = [
    ("tpcds (paper, SF10)", [60.14, 36.13, 18.65, 13.75, 11.42, 8.62]),
    ("prodds ohne K (paper, SF10)", [59.21, 40.33, 26.57, 22.38, 14.69, 9.79]),
    ("REDSHIFT FLEET", [73, 60, 49, 41, 33, 25]),
]
NULL_REF = [("REDSHIFT FLEET (NULL)", [25, 20, 16, 13.5, 11, 7])]


def load(path):
    mcv, nul, cols = [], [], []
    for line in open(path):
        r = json.loads(line)
        if r.get("record_type") != "data_column_stats":
            continue
        n = r.get("row_count") or 0
        if n <= 0:
            continue
        share = (r.get("max_count") or 0) / n
        mcv.append(share)
        nul.append((r.get("null_count") or 0) / n)
        cols.append((share, f"{r['table']}.{r['column']}"))
    return mcv, nul, cols


def row(name, values):
    cells = [f"{sum(1 for x in values if x >= t) / len(values) * 100:6.2f}" for t in THRESHOLDS]
    return f"{name:<32} " + " ".join(cells) + f"   n={len(values)}"


def main():
    args = [a for a in sys.argv[1:] if "=" in a]
    top_n = 0
    if "--top" in sys.argv:
        top_n = int(sys.argv[sys.argv.index("--top") + 1])
    def ref_row(name, vals):
        return f"{name:<32} " + " ".join(f"{v:6.2f}" for v in vals) + "   n=ref"

    header = f"{'bench':<32} " + " ".join(f">={t:4.2f}" for t in THRESHOLDS)
    print("== MCV share (max_count/row_count) ==")
    print(header)
    for name, vals in REFERENCE:
        print(ref_row(name, vals))
    for arg in args:
        name, path = arg.split("=", 1)
        mcv, nul, cols = load(path)
        print(row(name, mcv))
        if top_n:
            for share, col in sorted(cols, reverse=True)[:top_n]:
                print(f"    {share:6.3f}  {col}")
    print("\n== NULL fraction ==")
    print(header)
    for name, vals in NULL_REF:
        print(ref_row(name, vals))
    for arg in args:
        name, path = arg.split("=", 1)
        _, nul, _ = load(path)
        print(row(name, nul))


if __name__ == "__main__":
    main()
