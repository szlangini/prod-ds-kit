#!/usr/bin/env python3
"""Cross-benchmark per-query latency CDF, in the SAME paper style as plot_results.py
(imports apply_style/save_fig/HEIGHT_SCALE). One step-line per benchmark SUITE, Prod-DS
emphasised. Two figures: SF10 (all 8 suites, matched ~2-15 GB) and SF100 (5 scalable suites).
Fed from the s7_cdf latency CSVs + the E1 SF100 prodds/tpcds summaries.

Usage: plot_cdf_crossbench.py --output-dir <dir>
"""
import argparse, csv, glob, sys
from pathlib import Path
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "experiments"))
from plot_results import apply_style, save_fig, HEIGHT_SCALE, safe_float  # noqa: E402

# The other suites are measured outside this pipeline; commit the small per-query
# latency CSVs under experiments/data/s7_cdf/ to make this figure self-contained.
DEFAULT_CDF_DIR = REPO / "experiments" / "data" / "s7_cdf"
DEFAULT_E1_DIR = REPO / ".reproduce" / "sf100" / "results" / "E1"

# Prod-DS emphasised (bold crimson); others a calm distinct palette.
SUITE_COLOR = {
    "prodds": "#d62728", "tpcds": "#1f77b4", "tpch": "#2ca02c", "jcch": "#9467bd",
    "dsb": "#8c564b", "clickbench": "#ff7f0e", "job": "#17becf", "redbench": "#7f7f7f",
}
SUITE_LABEL = {
    "prodds": "Prod-DS", "tpcds": "TPC-DS", "tpch": "TPC-H", "jcch": "JCC-H",
    "dsb": "DSB", "clickbench": "ClickBench", "job": "JOB", "redbench": "RedBench",
}


def _load(path, suites):
    d = {}
    for r in csv.DictReader(open(path)):
        s = r["suite"].replace("100", "")
        if s in suites and r.get("status") in ("ok", "empty") and r.get("latency_ms"):
            v = safe_float(r["latency_ms"])
            if v and v > 0:
                d.setdefault(s, []).append(v)
    return d


def _e1(e1_dir, key):
    out = []
    for f in glob.glob(f"{e1_dir}/{key}/**/summary.csv", recursive=True):
        for r in csv.DictReader(open(f)):
            v = safe_float(r.get("median_execution_ms") or r.get("median_ms"))
            if v and v > 0:
                out.append(v)
    return out


def plot_cdf(data, order, output_dir, fname, title):
    fig, ax = plt.subplots(figsize=(6, 3.2 * HEIGHT_SCALE))  # flattened to ~fig13 aspect (not taller than the STR sweep)
    plotted = False
    for s in order:
        vals = data.get(s, [])
        if not vals:
            continue
        times = sorted(v / 1000.0 for v in vals)          # ms -> seconds, like fig9
        cdf_y = np.arange(1, len(times) + 1) / len(times)
        lw = 2.6 if s == "prodds" else 1.5
        ax.plot(times, cdf_y, color=SUITE_COLOR.get(s, "#888888"),
                linewidth=lw, label=SUITE_LABEL.get(s, s),
                solid_capstyle="round", solid_joinstyle="round",
                alpha=1.0 if s == "prodds" else 0.85)
        plotted = True
    if not plotted:
        plt.close(fig)
        return
    ax.set_xscale("log")
    ax.set_xlabel("Per-query latency (s, log scale)")
    ax.set_ylabel("CDF")
    ax.set_title(title)
    ax.set_ylim(0, 1.02)
    ax.legend(loc="lower right", ncol=2, fontsize=7)
    fig.tight_layout()
    save_fig(fig, output_dir, fname)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-dir", required=True, type=Path)
    ap.add_argument("--cdf-input-dir", type=Path, default=DEFAULT_CDF_DIR,
                    help="Dir with cross-benchmark latency CSVs (s7_latencies_sf10.csv / _sf100.csv)")
    ap.add_argument("--e1-dir", type=Path, default=DEFAULT_E1_DIR,
                    help="E1 results dir for Prod-DS/TPC-DS (SF100)")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    apply_style()

    csv10 = args.cdf_input_dir / "s7_latencies_sf10.csv"
    csv100 = args.cdf_input_dir / "s7_latencies_sf100.csv"
    if not csv10.exists() and not csv100.exists():
        print(f"[skip] cross-bench latency CSVs not found under {args.cdf_input_dir} "
              "(other-suite latencies are an external input; commit them there to enable this figure)")
        return

    # SF10 — all 8 suites (matched scale)
    if csv10.exists():
        a = _load(str(csv10), set(SUITE_LABEL))
        plot_cdf(a, ["prodds", "clickbench", "tpcds", "redbench", "job", "tpch", "jcch", "dsb"],
                 args.output_dir, "fig_cdf_latency_sf10.png",
                 "Per-query latency CDF across benchmarks")

    # SF100 — 5 scalable suites
    if csv100.exists():
        sf = _load(str(csv100), {"tpch", "jcch", "dsb"})
        b = {"prodds": _e1(args.e1_dir, "duckdb_prodds"), "tpcds": _e1(args.e1_dir, "duckdb_tpcds"),
             "tpch": sf.get("tpch", []), "jcch": sf.get("jcch", []), "dsb": sf.get("dsb", [])}
        plot_cdf(b, ["prodds", "tpch", "jcch", "tpcds", "dsb"],
                 args.output_dir, "fig_cdf_latency_sf100_scalable.png",
                 "Per-query latency CDF (scalable benchmarks)")
    print("done")


if __name__ == "__main__":
    main()
