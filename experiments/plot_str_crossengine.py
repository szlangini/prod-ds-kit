#!/usr/bin/env python3
"""Cross-engine stringification figure (E4X) — in-repo, self-contained.

Reads the E4X results produced by the pipeline
(``<results-dir>/E4X/<engine>_str<N>/.../workload_compare/raw.jsonl``) and, per
engine, plots the median per-query runtime at each STR level normalised to that
engine's lowest STR level -> shows which engine degrades worst as stringification
rises. Degrades gracefully (placeholder page) if E4X is absent/empty, e.g. on a
single-engine smoke run.

Usage:
    python experiments/plot_str_crossengine.py \
        --results-dir .reproduce/sf100/results \
        --output-dir  eab_artifact/figures
"""
from __future__ import annotations

import argparse
import glob
import json
import re
import statistics as st
import sys
from collections import defaultdict
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

try:
    from experiments.plot_results import (  # noqa: E402
        ENGINE_COLORS,
        ENGINE_LABELS,
        apply_style,
    )

    apply_style()
except Exception:  # pragma: no cover - styling is best-effort
    ENGINE_COLORS = {"duckdb": "#FF6B00", "cedardb": "#2196F3", "monetdb": "#4CAF50", "postgres": "#00897B"}
    ENGINE_LABELS = {"duckdb": "DuckDB", "cedardb": "CedarDB", "monetdb": "MonetDB", "postgres": "PostgreSQL"}


def load_jsonl(path: str) -> list:
    with open(path) as fh:
        return [json.loads(line) for line in fh if line.strip()]


def med(xs):
    return st.median(xs) if xs else None


def collect(results_dir: Path) -> dict:
    base = results_dir / "E4X"
    eng_lvl: dict = defaultdict(dict)  # engine -> str -> {qid: median_ms}
    for d in glob.glob(str(base / "*_str*")):
        m = re.search(r"/([a-z]+)_str(\d+)$", d)
        if not m:
            continue
        eng, lvl = m.group(1), int(m.group(2))
        fs = glob.glob(d + "/*/*/workload_compare/raw.jsonl")
        if not fs:
            continue
        byq = defaultdict(list)
        for r in load_jsonl(sorted(fs)[-1]):
            if r.get("status") == "success" and r.get("wall_time_ms_total"):
                byq[r["query_id"]].append(r["wall_time_ms_total"])
        eng_lvl[eng][lvl] = {q: med(v) for q, v in byq.items() if v}
    return eng_lvl


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--results-dir", required=True, type=Path,
                    help="Per-scale results dir, e.g. .reproduce/sf100/results")
    ap.add_argument("--output-dir", required=True, type=Path,
                    help="Where to write fig_str_crossengine.{png,pdf}")
    args = ap.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    out_png = args.output_dir / "fig_str_crossengine.png"

    data = collect(args.results_dir)
    fig, ax = plt.subplots(figsize=(7, 2.96))  # flat aspect matching fig13
    plotted = False
    for eng in ["duckdb", "cedardb", "monetdb", "postgres"]:
        if eng not in data or len(data[eng]) < 2:
            continue
        levels = sorted(data[eng])
        base = levels[0]
        ys = []
        for lvl in levels:
            common = set(data[eng][base]) & set(data[eng][lvl])
            ratios = [data[eng][lvl][q] / data[eng][base][q] for q in common if data[eng][base][q] > 0]
            ys.append(med(ratios) if ratios else None)
        xs = [l for l, y in zip(levels, ys) if y]
        ys = [y for y in ys if y]
        if len(xs) < 2:
            continue
        ax.plot(xs, ys, "o-", color=ENGINE_COLORS.get(eng, "#888"), lw=2, ms=6,
                label=f"{ENGINE_LABELS.get(eng, eng)} (norm STR{base})")
        plotted = True

    if plotted:
        ax.axhline(1.0, color="#999", ls="--", lw=0.8)
        ax.set_xlabel("Stringification level (STR type coverage)")
        ax.set_ylabel("Median per-query runtime\n(normalized to each engine's lowest STR)")
        ax.set_title("Stringification cross-engine — who degrades worst", pad=8)
        ax.legend(loc="upper left")
        fig.text(0.5, -0.01,
                 "Per-query median ratio vs the engine's lowest STR level (common-query set). "
                 "Higher slope = more string-sensitive engine.",
                 ha="center", fontsize=7.3, color="#777")
        fig.tight_layout()
        fig.savefig(out_png, bbox_inches="tight")
        fig.savefig(out_png.with_suffix(".pdf"), bbox_inches="tight")
        print("saved", out_png)
        for eng in data:
            if len(data[eng]) >= 2:
                levels = sorted(data[eng])
                base, top = levels[0], levels[-1]
                common = set(data[eng][base]) & set(data[eng][top])
                r = [data[eng][top][q] / data[eng][base][q] for q in common if data[eng][base][q] > 0]
                print(f"  {eng}: STR{base}->STR{top} median x{med(r):.2f}" if r else f"  {eng}: n/a")
    else:
        ax.text(0.5, 0.5, "E4X cross-engine stringification —\nnot available yet",
                ha="center", va="center", fontsize=12, color="#999")
        ax.axis("off")
        fig.savefig(out_png, bbox_inches="tight")
        fig.savefig(out_png.with_suffix(".pdf"), bbox_inches="tight")
        print("saved placeholder", out_png)


if __name__ == "__main__":
    main()
