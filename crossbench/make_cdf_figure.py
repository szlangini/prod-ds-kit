#!/usr/bin/env python3
"""Render the cross-benchmark runtime CDF and its summary table from the pilot CSV.

The figure is an empirical CDF of per-query runtime, one curve per suite, every query
weighted equally, on a logarithmic second axis. Queries that failed or timed out are not
in the curves -- a failed query has no runtime -- but they are never silently dropped: the
legend carries the success count against the total, and the summary table has its own
failure columns.

Usage:
    .venv/bin/python make_cdf_figure.py [--results results] [--out-dir figures]
"""
from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path
from typing import Dict, List

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

HERE = Path(__file__).resolve().parent

# Prod-DS keeps the paper's orange; the others are distinguishable in greyscale by dash and
# marker as well as by colour, the same rule the paper figures now follow.
STYLE = {
    "prodds":   dict(label="Prod-DS (default)", color="#FF8C00", dashes=(None, None), marker="o", z=5),
    "tpcds":    dict(label="TPC-DS",            color="#2A9D8F", dashes=(None, None), marker="s", z=4),
    "dsb":      dict(label="DSB",               color="#9BC53D", dashes=(1.3, 1.3),   marker="D", z=3),
    "job":      dict(label="JOB",               color="#E07A5F", dashes=(5.5, 1.5, 1.0, 1.5), marker="P", z=3),
    "redbench": dict(label="RedBench (writes)", color="#F2C744", dashes=(2.4, 1.2),   marker="X", z=2),
}
ORDER = ["prodds", "prodds_templates", "tpcds", "dsb", "job", "redbench"]
# Prod-DS's tail is dominated by eight amplification queries (the J* join and U* union
# micro-suite). Showing the 99 templates on their own keeps that visible instead of letting
# one design decision carry the whole comparison.
STYLE["prodds_templates"] = dict(label="Prod-DS, 99 templates only", color="#FF8C00",
                                 dashes=(3.0, 1.6), marker=None, z=5)
MICRO_PREFIXES = ("query_join_", "query_union_")


def load(results: Path):
    rows = list(csv.DictReader(open(results / "latencies_pilot.csv", newline="", encoding="utf-8")))
    manifest = json.loads((results / "manifest_pilot.json").read_text(encoding="utf-8"))
    per_suite: Dict[str, List[float]] = defaultdict(list)
    fails: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for r in rows:
        if r["status"] == "ok":
            per_suite[r["suite"]].append(float(r["latency_ms"]))
            if r["suite"] == "prodds" and not r["query_id"].startswith(MICRO_PREFIXES):
                per_suite["prodds_templates"].append(float(r["latency_ms"]))
        else:
            fails[r["suite"]][r["status"]] += 1
    return per_suite, fails, manifest


def pct(vals: List[float], q: float) -> float:
    return float(np.percentile(np.asarray(vals), q)) if vals else float("nan")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--results", type=Path, default=HERE / "results")
    ap.add_argument("--out-dir", type=Path, default=HERE / "figures")
    args = ap.parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)

    per_suite, fails, manifest = load(args.results)
    suites = [s for s in ORDER if s in per_suite]

    plt.rcParams.update({
        "font.family": "sans-serif", "font.sans-serif": ["DejaVu Sans"],
        "font.size": 9, "axes.labelsize": 9, "xtick.labelsize": 8, "ytick.labelsize": 8,
        "legend.fontsize": 8, "axes.linewidth": 0.6, "pdf.fonttype": 42, "ps.fonttype": 42,
        "figure.dpi": 150, "savefig.dpi": 200, "savefig.bbox": "tight",
    })
    fig, ax = plt.subplots(figsize=(7.0, 3.6))

    for name in suites:
        vals = sorted(v / 1000.0 for v in per_suite[name])          # ms -> s
        n_ok = len(vals)
        n_fail = sum(fails[name].values())
        y = np.arange(1, n_ok + 1) / n_ok
        st = STYLE[name]
        kw = dict(color=st["color"], linewidth=1.1 if name.endswith("_templates") else 1.7,
                  zorder=st["z"], alpha=0.85 if name.endswith("_templates") else 1.0)
        if st["marker"]:
            kw.update(marker=st["marker"], markersize=4.0, markevery=0.12,
                      markeredgecolor="white", markeredgewidth=0.6)
        if st["dashes"][0] is not None:
            kw["dashes"] = st["dashes"]
        lbl = f"{st['label']}  (n={n_ok}"
        if n_fail:
            lbl += f", {n_fail} failed"
        lbl += ")"
        ax.plot(vals, y, label=lbl, **kw)

    ax.set_xscale("log")
    ax.set_xlabel("Query runtime (s, log scale)")
    ax.set_ylabel("Fraction of queries")
    ax.set_ylim(0, 1.02)
    ax.grid(True, which="major", linestyle=":", linewidth=0.4, color="#aaaaaa", alpha=0.7)
    ax.grid(True, which="minor", linestyle=":", linewidth=0.25, color="#cccccc", alpha=0.5)
    ax.set_axisbelow(True)
    leg = ax.legend(loc="lower right", framealpha=0.95, edgecolor="#bbbbbb")
    leg.set_zorder(10)

    # The pilot label is part of the figure on purpose: these are single-repetition numbers.
    # It sits below the axes so it can never cover a curve.
    fig.text(0.5, -0.06,
             f"PILOT — one timed repetition per query, not the final protocol.  "
             f"DuckDB {manifest['engine'].split()[-1]}, {manifest['threads']} threads, warm "
             f"(one untimed warmup pass; the write suite gets none).  "
             f"Failed queries have no runtime and are not in the curves; they are counted in "
             f"the legend and in the summary table.",
             ha="center", va="top", fontsize=7.2, color="#444444", wrap=True)

    for ext in ("pdf", "png"):
        fig.savefig(args.out_dir / f"cdf_crossbench_pilot.{ext}")
    plt.close(fig)
    print(f"[cdf] wrote {args.out_dir/'cdf_crossbench_pilot.pdf'} and .png")

    # ── summary table ───────────────────────────────────────────
    hdr = ["suite", "kind", "data_logical_gib", "data_on_disk_gib", "n_queries", "n_ok",
           "n_error", "n_timeout", "median_s", "p90_s", "p99_s", "max_s", "min_s", "spread_x"]
    out_rows = []
    for name in suites:
        vals = [v / 1000.0 for v in per_suite[name]]
        base = "prodds" if name == "prodds_templates" else name
        info = dict(manifest["suites"][base])
        if name == "prodds_templates":
            info["kind"] = "read (subset)"
            info["n_queries"] = len(vals)
        size = info["source_data_bytes"]
        med, p90, p99 = pct(vals, 50), pct(vals, 90), pct(vals, 99)
        mx, mn = max(vals), min(vals)
        out_rows.append([
            name, info["kind"],
            f"{size['logical']/2**30:.2f}",
            f"{(size['on_disk'] or 0)/2**30:.2f}",
            info["n_queries"], len(vals),
            fails[name].get("error", 0), fails[name].get("timeout", 0),
            f"{med:.4f}", f"{p90:.4f}", f"{p99:.4f}", f"{mx:.4f}", f"{mn:.4f}",
            f"{mx/mn:.0f}" if mn > 0 else "",
        ])
    with (args.out_dir / "cdf_crossbench_summary.csv").open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(hdr)
        w.writerows(out_rows)

    md = ["| suite | kind | data (GiB, logical / on disk) | queries | ok | err | t/o | median | P90 | P99 | max | spread |",
          "|---|---|---|---|---|---|---|---|---|---|---|---|"]
    for r in out_rows:
        md.append(f"| {r[0]} | {r[1]} | {r[2]} / {r[3]} | {r[4]} | {r[5]} | {r[6]} | {r[7]} | "
                  f"{float(r[8])*1000:.0f} ms | {float(r[9])*1000:.0f} ms | {float(r[10])*1000:.0f} ms | "
                  f"{float(r[11]):.1f} s | {r[13]}x |")
    (args.out_dir / "cdf_crossbench_summary.md").write_text("\n".join(md) + "\n", encoding="utf-8")
    print("\n".join(md))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
