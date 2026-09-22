#!/usr/bin/env python3
"""
plot_results.py -- Generate paper figures from Prod-DS Kit experiment results.

Reads JSONL / CSV output from reproduce.sh and produces PNG plots
matching the figures in Section 6 of the paper.

Usage:
    python3 experiments/plot_results.py \
        --results-dir .reproduce/results \
        --output-dir  .reproduce/results/plots
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np

# ── Colour palette (engine branding) ────────────────────────────
# Engine line/bar colours — sampled from the published paper figures
# (benchmark_paper_new.pdf Fig 8/9/11/12) so regenerated plots match it exactly.
ENGINE_COLORS: Dict[str, str] = {
    "duckdb":  "#58b4dd",   # paper blue
    "cedardb": "#ff9800",   # paper orange
    "monetdb": "#96bf3d",   # paper green
    "postgres": "#9c27b0",  # purple — not in the paper (3-engine figs); distinct 4th
}
ENGINE_ORDER = ["duckdb", "cedardb", "monetdb", "postgres"]
ENGINE_LABELS: Dict[str, str] = {
    "duckdb":  "DuckDB",
    "cedardb": "CedarDB",
    "monetdb": "MonetDB",
    "postgres": "PostgreSQL",
}

SUITE_COLORS = {
    "tpcds":  "#9E9E9E",
    "prodds": "#E65100",
}
SUITE_LABELS = {
    "tpcds":  "TPC-DS",
    "prodds": "Prod-DS",
}

# Error categories shown in Fig 10
# Same taxonomy as the Table 4 CSV (export_paper_csv.py), so figure and table agree.
ERROR_CATEGORIES = ["success", "dialect", "failure", "oom", "timeout"]
ERROR_COLORS = {
    "success": "#4CAF50",
    "dialect": "#F44336",
    "failure": "#795548",
    "oom":     "#9C27B0",
    "timeout": "#FF9800",
    # legacy runner statuses, kept so the fallback path below still renders
    "error":             "#F44336",
    "timeout_planning":  "#FF9800",
    "timeout_execution": "#FFC107",
    "engine_crash":      "#795548",
}
ERROR_LABELS = {
    "success": "Success",
    "dialect": "Unsupported SQL / dialect",
    "failure": "Other failure",
    "oom":     "Out of memory",
    "timeout": "Timeout",
    "error":             "Error",
    "timeout_planning":  "Plan Timeout",
    "timeout_execution": "Exec Timeout",
    "engine_crash":      "Crash",
}
CAUSE_TO_CATEGORY = {
    "syntax / dialect": "dialect",
    "unsupported SQL feature": "dialect",
    "out of memory": "oom",
    "timeout": "timeout",
    "engine crash": "failure",
    "engine resource limit": "failure",
    "other error": "failure",
}


# ── IO helpers ──────────────────────────────────────────────────
def load_jsonl(path: Path) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def load_csv(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            rows.append(row)
    return rows


def safe_float(val: Any) -> Optional[float]:
    if val is None or val == "" or val == "None":
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _summarize_records(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Inline summary: group by query_id+suite, compute median/min/max."""
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for rec in records:
        key = f"{rec.get('query_id', '')}|{rec.get('suite', '')}"
        grouped[key].append(rec)

    summaries: List[Dict[str, Any]] = []
    for _, items in grouped.items():
        sample = items[0]
        total = len(items)
        successes = [i for i in items if i.get("status") == "success"]
        success_times = [
            i[RAW_TIME_FIELD] for i in successes
            if i.get(RAW_TIME_FIELD) is not None
        ]
        planning_times = [
            i["wall_time_ms_planning"] for i in successes
            if i.get("wall_time_ms_planning") is not None
        ]
        execution_times = [
            i["wall_time_ms_execution"] for i in successes
            if i.get("wall_time_ms_execution") is not None
        ]

        summaries.append({
            "query_id": sample.get("query_id"),
            "suite": sample.get("suite"),
            "runs_total": total,
            "runs_success": len(successes),
            "runs_failed": total - len(successes),
            "failure_rate": (total - len(successes)) / total if total else 0,
            SUMMARY_TIME_COL: float(np.median(success_times)) if success_times else None,
            "min_ms": min(success_times) if success_times else None,
            "max_ms": max(success_times) if success_times else None,
            "median_planning_ms": float(np.median(planning_times)) if planning_times else None,
            "median_execution_ms": float(np.median(execution_times)) if execution_times else None,
        })
    return summaries


def engine_color(name: str) -> str:
    return ENGINE_COLORS.get(name.lower(), "#888888")


def engine_label(name: str) -> str:
    return ENGINE_LABELS.get(name.lower(), name)


# Paper figures rendered ~20% shorter (compact for the revision). Tune here.
HEIGHT_SCALE = 0.8

# MonetDB join planning isn't in the harness raw (adapter skipped the EXPLAIN stage); a separate
# plan-only probe writes this {level: planning_ms} JSON, overlaid as MonetDB's planning line in fig11.
_MONET_JOIN_PLANNING_JSON = Path(__file__).resolve().parent / "monet_join_planning.json"


# ── Figure titles ───────────────────────────────────────────────
# Reviewer D3(d) of the July 2026 round: "All plots have a header, presumably
# from the plotting tool, *and* caption, presumably from the LaTeX (sub)figure,
# and the two say almost the same thing. Remove the header and make the caption
# more complete if the additional information is useful."
#
# Titles are therefore OFF by default. Set PLOT_TITLES=1 in the environment (it
# is honoured by every generator in this repo and by the WorkloadLens paper
# figures) or pass --titles to a single generator to restore them for slides and
# for eyeballing intermediate results.
# ── Runtime measure (author decision, 17 September 2026) ─────────────────────
# Every measured value is TWO client processes: one running `EXPLAIN <query>` and one running the
# query. `wall_time_ms_total` is their sum, which is what every figure used until now.
#
# The approved measure for E1-E5 is the EXECUTION CLIENT ALONE. Client startup, connection or
# database opening, parse, optimisation, execution and output handling inside that client all
# remain included -- this is NOT engine-internal execution time. The separately executed EXPLAIN
# client is excluded.
#
# RUNTIME_MEASURE=total restores the historical EXPLAIN-plus-execution figures.
_MEASURE = os.environ.get("RUNTIME_MEASURE", "execution").strip().lower()
if _MEASURE not in ("execution", "total"):
    raise SystemExit(f"RUNTIME_MEASURE must be 'execution' or 'total', got {_MEASURE!r}")
RAW_TIME_FIELD = "wall_time_ms_execution" if _MEASURE == "execution" else "wall_time_ms_total"
SUMMARY_TIME_COL = "median_execution_ms" if _MEASURE == "execution" else "median_ms"
MEASURE_LABEL = ("execution client only" if _MEASURE == "execution"
                 else "EXPLAIN client + execution client (historical)")
# Ordinate for the per-query time plots. Under either measure this is a CLIENT wall time, so it
# is never labelled "end-to-end runtime" or "execution time" without saying which client.
CLIENT_TIME_YLABEL = ("Median execution-client wall time (s)" if _MEASURE == "execution"
                      else "Median EXPLAIN+execution client wall time (s)")
# Same quantity, short enough to sit in a column-width panel without losing its unit. Which
# client is timed still has to be said; "wall time" is the part that can go.
CLIENT_TIME_YLABEL_SHORT = ("Execution-client time (s)" if _MEASURE == "execution"
                            else "EXPLAIN+execution client time (s)")
# The two ladder panels (Fig 11, Fig 12) carry ONE shared ordinate, at the author's direction:
# the same quantity is drawn in both, so the same words name it. This is a display title and
# nothing more. It does not redefine what is measured -- Fig 12's series and Fig 11's solid series
# are the execution client's `median_execution_ms`, Fig 11's dashed series is the separately
# executed EXPLAIN client, and the shared title neither turns EXPLAIN wall time into isolated
# optimiser time nor restores the historical sum of the two clients. The timer boundaries live in
# docs/08-measurement-contract.md and evidence/timing-boundaries/. RUNTIME_MEASURE still selects
# what is read (RAW_TIME_FIELD, SUMMARY_TIME_COL); it no longer changes these two words.
RUNTIME_YLABEL = "Median Query Runtime (s)"
UNION_YLABEL = RUNTIME_YLABEL
JOIN_YLABEL = RUNTIME_YLABEL
# Fig 11 draws two clients on one scale; the key below the curves names which is which.
JOIN_SERIES_LABELS = ("Execution", "EXPLAIN")

_TRUTHY = {"1", "true", "yes", "on"}
# PLOT_LEGACY_STYLE=1 renders the figures the way they looked before the July 2026 review
# round (in-plot titles on). It exists so the same data can be shown in the old and the new
# style side by side; it is never used for the paper itself.
LEGACY_STYLE: bool = os.environ.get("PLOT_LEGACY_STYLE", "0").strip().lower() in _TRUTHY
SHOW_TITLES: bool = LEGACY_STYLE or os.environ.get("PLOT_TITLES", "0").strip().lower() in _TRUTHY


def set_titles_enabled(flag: bool) -> None:
    """Override the PLOT_TITLES default for this process."""
    global SHOW_TITLES
    SHOW_TITLES = bool(flag)


def add_title_argument(parser: argparse.ArgumentParser) -> None:
    """Add --titles/--no-titles; leaves the PLOT_TITLES default when neither is given."""
    parser.add_argument("--titles", dest="titles", action="store_true", default=None,
                        help="draw the in-plot title (default: off, the LaTeX caption carries it)")
    parser.add_argument("--no-titles", dest="titles", action="store_false",
                        help="force the in-plot title off even when PLOT_TITLES=1")


def apply_title_argument(args: argparse.Namespace) -> None:
    """Apply --titles/--no-titles if the user passed one of them."""
    if getattr(args, "titles", None) is not None:
        set_titles_enabled(args.titles)


def set_title(target: Any, text: str, **kwargs: Any) -> None:
    """Set an Axes/Figure title only when titles are enabled (see PLOT_TITLES)."""
    if not SHOW_TITLES:
        return
    setter = getattr(target, "set_title", None) or getattr(target, "suptitle")
    setter(text, **kwargs)


# ── Style setup ─────────────────────────────────────────────────
def apply_style() -> None:
    # Calibrated to benchmark_paper_new.pdf (Fig 8-13): sans-serif, 10pt bold
    # titles, 9pt labels, 8pt ticks, thin 0.6 spines, subtle dotted y-grid.
    plt.rcParams.update({
        "figure.facecolor":   "white",
        "axes.facecolor":     "white",
        "axes.edgecolor":     "#333333",
        "axes.labelcolor":    "#222222",
        "font.family":        "sans-serif",
        "font.sans-serif":    ["DejaVu Sans", "Arial", "Helvetica", "Liberation Sans"],
        "font.size":          9,
        "axes.titlesize":     10,
        "axes.titleweight":   "bold",
        "axes.labelsize":     9,
        "xtick.color":        "#333333",
        "ytick.color":        "#333333",
        "xtick.labelsize":    8,
        "ytick.labelsize":    8,
        "legend.fontsize":    8,
        "legend.framealpha":  0.95,
        "legend.edgecolor":   "#bbbbbb",
        "axes.linewidth":     0.6,
        "xtick.major.width":  0.5,
        "ytick.major.width":  0.5,
        "xtick.major.size":   3.0,
        "ytick.major.size":   3.0,
        "axes.grid":          True,
        "axes.grid.axis":     "y",
        "axes.axisbelow":     True,
        "grid.linestyle":     ":",
        "grid.linewidth":     0.4,
        "grid.color":         "#aaaaaa",
        "grid.alpha":         0.7,
        "pdf.fonttype":       42,
        "ps.fonttype":        42,
        "axes.unicode_minus": False,
        "figure.dpi":         150,
        "savefig.dpi":        200,
        "savefig.bbox":       "tight",
        "savefig.pad_inches": 0.05,
    })
# ── Keeping failure callouts off the curves ─────────────────────────────────────────────
# The ladder callouts are anchored at the failure point and offset a few points to the right,
# which is clear space on most of these plots and not on all of them. These helpers score a
# candidate position against everything already drawn and take the first clear one, trying
# the position the callout already has FIRST — so a callout that is clear never moves, and
# the anchor never moves at all.
def _axes_obstacles(ax, exclude_text=None):
    """Everything a label must not cover, in axes coordinates.

    Curves come back as a densified point cloud with one owner id per series, because a
    polyline is thin and a label that grazes it is as bad as one that sits on it. Bars, other
    annotations (with the leader line back to the point they mark), filled bands and the
    legends already drawn come back as rectangles or paths.
    """
    to_axes = (ax.transData + ax.transAxes.inverted()).transform
    step = 0.004
    pts_all, owner_all = [], []
    for idx, line in enumerate(ax.lines):
        if not line.get_visible():
            continue
        xs = np.asarray(line.get_xdata(), dtype=float)
        ys = np.asarray(line.get_ydata(), dtype=float)
        ok = np.isfinite(xs) & np.isfinite(ys)
        xs, ys = xs[ok], ys[ok]
        if xs.size == 0:
            continue
        pts = np.asarray(to_axes(np.column_stack([xs, ys])), dtype=float)
        dense = [pts[:1]]
        for i in range(1, len(pts)):
            a, b = pts[i - 1], pts[i]
            n = int(max(abs(b[0] - a[0]), abs(b[1] - a[1])) / step)
            if n > 1:
                t = np.linspace(0.0, 1.0, min(n, 4000) + 1)[1:, None]
                dense.append(a + (b - a) * t)
            else:
                dense.append(b[None, :])
        pts = np.vstack(dense)
        pts_all.append(pts)
        owner_all.append(np.full(len(pts), idx, dtype=int))
    cloud = ((np.vstack(pts_all), np.concatenate(owner_all)) if pts_all
             else (np.zeros((0, 2)), np.zeros(0, dtype=int)))

    fig = ax.figure
    fig.canvas.draw()
    rend = fig.canvas.get_renderer()
    inv = ax.transAxes.inverted()
    rects = []
    for patch in ax.patches:
        if type(patch).__name__ != "Rectangle" or not patch.get_visible():
            continue
        (x0, y0), (x1, y1) = to_axes([
            (patch.get_x(), patch.get_y()),
            (patch.get_x() + patch.get_width(), patch.get_y() + patch.get_height())])
        rects.append((min(x0, x1), min(y0, y1), max(x0, x1), max(y0, y1)))
    for txt in ax.texts:
        if txt is exclude_text or not txt.get_visible() or not txt.get_text().strip():
            continue
        bb = txt.get_window_extent(rend).transformed(inv)
        rects.append((bb.x0, bb.y0, bb.x1, bb.y1))
        xy = getattr(txt, "xy", None)
        if xy is not None:
            ax_x, ax_y = to_axes([xy])[0]
            rects.append((min(bb.x0, ax_x), min(bb.y0, ax_y),
                          max(bb.x1, ax_x), max(bb.y1, ax_y)))
    from matplotlib.legend import Legend  # noqa: WPS433
    for child in list(ax.get_children()) + ([ax.legend_] if ax.legend_ is not None else []):
        if isinstance(child, Legend):
            bb = child.get_window_extent(rend).transformed(inv)
            rects.append((bb.x0, bb.y0, bb.x1, bb.y1))

    paths = []
    for coll in ax.collections:
        if not coll.get_visible():
            continue
        trans = coll.get_transform() + ax.transAxes.inverted()
        for path in coll.get_paths():
            if len(path.vertices):
                paths.append(path.transformed(trans))
    return cloud[0], cloud[1], (rects, paths)


def _label_score(rect, pts, owners, obstacles) -> float:
    """How many distinct things a box at ``rect`` would cover."""
    rects, paths = obstacles
    score = 0.0
    for path in paths:
        verts = path.vertices
        inside = ((verts[:, 0] >= rect[0]) & (verts[:, 0] <= rect[2])
                  & (verts[:, 1] >= rect[1]) & (verts[:, 1] <= rect[3]))
        if inside.any():
            score += 1.0
            continue
        gx = np.linspace(rect[0], rect[2], 7)
        gy = np.linspace(rect[1], rect[3], 7)
        if path.contains_points(np.array([(x, y) for x in gx for y in gy])).any():
            score += 1.0
    for other in rects:
        if (other[0] < rect[2] and other[2] > rect[0]
                and other[1] < rect[3] and other[3] > rect[1]):
            score += 1.0
    if len(pts):
        inside = ((pts[:, 0] >= rect[0]) & (pts[:, 0] <= rect[2])
                  & (pts[:, 1] >= rect[1]) & (pts[:, 1] <= rect[3]))
        if inside.any():
            score += float(np.unique(owners[inside]).size)
            score += min(int(inside.sum()), 999) / 1000.0
    return score


def _connect_callout(ax, target, min_points: float = 26.0) -> None:
    """Hairline from a callout that had to travel back to the marker it describes.

    A callout a few points off its cross needs no help; one that had to cross the plot does.
    The line stops at the edge of the text box, so it never runs under the words.
    """
    fig = ax.figure
    fig.canvas.draw()
    rend = fig.canvas.get_renderer()
    dx, dy = target.get_position()
    if abs(dx) <= min_points and abs(dy) <= min_points:
        return
    anchor = ax.transData.transform(target.xy)
    box = target.get_window_extent(rend).expanded(1.12, 1.25)
    near = (min(max(anchor[0], box.x0), box.x1), min(max(anchor[1], box.y0), box.y1))
    (x0, y0), (x1, y1) = ax.transData.inverted().transform([anchor, near])
    ax.plot([x0, x1], [y0, y1], "-", color=target.get_color(), linewidth=0.5,
            alpha=0.75, zorder=3, clip_on=False, label="_nolegend_")


def place_failure_callouts(ax) -> None:
    """Move each failure callout to the nearest clear offset, or leave it where it is."""
    fig = ax.figure
    targets = [t for t in ax.texts
               if getattr(t, "anncoords", None) == "offset points"
               and t.get_visible() and t.get_text().strip()]
    if not targets:
        return
    step = 6.3
    offsets = []
    for dx, ha in ((7, "left"), (-7, "right")):
        for dy in (0.0, 1.6 * step, -1.6 * step, 3.2 * step, -3.2 * step):
            offsets.append((dx, dy, ha, "center" if dy == 0 else ("bottom" if dy > 0 else "top")))
    offsets += [(0, 2.2 * step, "center", "bottom"), (0, -2.2 * step, "center", "top")]
    for dx, ha in ((7, "left"), (-7, "right"), (26, "left"), (-26, "right"),
                   (52, "left"), (-52, "right")):
        for dy in (5 * step, -5 * step, 8 * step, -8 * step, 12 * step, -12 * step, 0.0):
            offsets.append((dx, dy, ha, "center" if dy == 0 else ("bottom" if dy > 0 else "top")))
    for target in targets:
        start = (target.get_position(), target.get_ha(), target.get_va())
        candidates = [(start[0][0], start[0][1], start[1], start[2])] + offsets
        pts, owners, obstacles = _axes_obstacles(ax, exclude_text=target)
        best = None
        for dx, dy, ha, va in candidates:
            target.set_position((dx, dy))
            target.set_ha(ha)
            target.set_va(va)
            fig.canvas.draw()
            rend = fig.canvas.get_renderer()
            bb = target.get_window_extent(rend).transformed(ax.transAxes.inverted())
            rect = (bb.x0, bb.y0, bb.x1, bb.y1)
            score = _label_score(rect, pts, owners, obstacles)
            if bb.x0 < 0.004 or bb.x1 > 0.996 or bb.y0 < 0.004 or bb.y1 > 0.996:
                score += 10.0
            if best is None or score < best[0] - 1e-9:
                best = (score, (dx, dy), ha, va)
            if score <= 0.0:
                break
        target.set_position(best[1])
        target.set_ha(best[2])
        target.set_va(best[3])
        _connect_callout(ax, target)
        ax.__dict__.setdefault("_pd_callout_scores", []).append(
            (target.get_text().replace("\n", " / "), round(best[0], 3)))
    if os.environ.get("PD_LEGEND_DEBUG"):
        for text, score in ax.__dict__.get("_pd_callout_scores", []):
            print(f"  [callout] {text!r} covers {score}", flush=True)


def save_fig(fig: plt.Figure, output_dir: Path, name: str) -> None:
    path = output_dir / name
    fig.savefig(str(path))
    if name.endswith(".png"):                       # also emit a vector PDF for the paper
        fig.savefig(str(path)[:-4] + ".pdf")
    plt.close(fig)
    print(f"  saved {path}")


def _fmt_seconds(v: float) -> str:
    """Compact second-value label for bar tops (paper-style)."""
    if v >= 100:
        return f"{v:.0f}"
    if v >= 10:
        return f"{v:.1f}"
    if v >= 1:
        return f"{v:.2f}"
    return f"{v:.3f}"


# ── Data aggregation helpers ────────────────────────────────────
def _detect_engine(dirname: str) -> str:
    """Extract engine name from a directory name like 'duckdb_tpcds'."""
    low = dirname.lower()
    for eng in ENGINE_ORDER:
        if eng in low:
            return eng
    return low


def _detect_suite(dirname: str) -> str:
    """Extract suite name from a directory name like 'duckdb_prodds'."""
    low = dirname.lower()
    if "prodds" in low:
        return "prodds"
    if "tpcds" in low:
        return "tpcds"
    return low


def _find_latest_file(base_dir: Path, filename: str) -> Optional[Path]:
    """Find *filename* inside *base_dir*, searching recursively.

    The results hierarchy is <base_dir>/<timestamp>/<engine>/workload_compare/<file>.
    When multiple timestamp dirs exist, use the latest (sorted descending).
    Also handles flat layout where *filename* sits directly in *base_dir*.
    """
    direct = base_dir / filename
    if direct.is_file():
        return direct
    # Search recursively — pick the latest timestamp directory's file
    candidates = sorted(base_dir.rglob(filename), reverse=True)
    return candidates[0] if candidates else None


def _collect_e1_summaries(e1_dir: Path) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
    """Return {engine: {suite: [summary_rows]}} from E1 subdirectories."""
    result: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
    if not e1_dir.is_dir():
        return result

    for sub in sorted(e1_dir.iterdir()):
        if not sub.is_dir():
            continue
        engine = _detect_engine(sub.name)
        suite = _detect_suite(sub.name)

        summary_path = _find_latest_file(sub, "summary.csv")
        raw_path = _find_latest_file(sub, "raw.jsonl")

        rows: List[Dict[str, Any]] = []
        if summary_path:
            rows = load_csv(summary_path)
        elif raw_path:
            records = load_jsonl(raw_path)
            rows = _summarize_records(records)

        if rows:
            result.setdefault(engine, {}).setdefault(suite, []).extend(rows)

    return result


def _collect_e1_raw(e1_dir: Path) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
    """Return {engine: {suite: [raw_records]}} from E1 subdirectories."""
    result: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
    if not e1_dir.is_dir():
        return result

    for sub in sorted(e1_dir.iterdir()):
        if not sub.is_dir():
            continue
        engine = _detect_engine(sub.name)
        suite = _detect_suite(sub.name)

        raw_path = _find_latest_file(sub, "raw.jsonl")
        if raw_path:
            records = load_jsonl(raw_path)
            result.setdefault(engine, {}).setdefault(suite, []).extend(records)
    return result


# ── E1 Plots (Figs 8, 9, 10) ───────────────────────────────────

def _restrict_to_common_subset(summaries: Dict[str, Dict[str, List[Dict[str, Any]]]],
                               path: Path) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
    """Keep only the queries of the per-suite common subset (experiments/common_subset.py).

    The protocol reports E1 runtimes over the queries every audited engine completes
    (minus near-timeouts); a suite missing from the JSON is left untouched.
    """
    import json as _json
    try:
        spec = _json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        print(f"common subset: cannot read {path}: {exc} -- using all queries", file=sys.stderr)
        return summaries
    restricted: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
    for engine, suites in summaries.items():
        for suite, rows in suites.items():
            common = (spec.get("suites") or {}).get(suite, {}).get("common")
            if common is None:
                kept = rows
            else:
                allowed = set(common)
                kept = [r for r in rows if str(r.get("query_id")) in allowed]
            print(f"common subset: {engine}/{suite}: {len(kept)}/{len(rows)} queries")
            restricted.setdefault(engine, {})[suite] = kept
    return restricted


def plot_fig8_bar(summaries: Dict[str, Dict[str, List[Dict[str, Any]]]],
                  output_dir: Path, agg: str, fname: str, title: str,
                  ylabel: str = "Query runtime (s)") -> None:
    """Bar chart comparing runtime aggregates per engine, TPC-DS vs Prod-DS.

    ``agg="sum"`` is the TOTAL WORKLOAD RUNTIME and needs its own y-axis label: it is the sum
    over queries of each query's median, not a per-query runtime, and not the wall time of a
    measured workload pass -- the harness runs repetitions query-major, so no complete pass was
    ever timed (docs/08-measurement-contract.md).
    """
    engines = [e for e in ENGINE_ORDER if e in summaries]
    if not engines:
        return

    suites = ["tpcds", "prodds"]
    bar_width = 0.35
    x = np.arange(len(engines))

    fig, ax = plt.subplots(figsize=(6, 3.2 * HEIGHT_SCALE))  # wide+flat (~fig13 aspect), so it isn't taller than the STR sweep when column-scaled

    for i, suite in enumerate(suites):
        vals = []
        for eng in engines:
            rows = summaries.get(eng, {}).get(suite, [])
            times = [t for r in rows
                     if (t := safe_float(r.get(SUMMARY_TIME_COL))) is not None and t > 0]
            if not times:
                vals.append(0.0)
                continue
            # Paper plots SECONDS, not milliseconds.
            if agg == "median":
                vals.append(float(np.median(times)) / 1000.0)
            elif agg == "mean":
                vals.append(float(np.mean(times)) / 1000.0)
            elif agg == "sum":
                vals.append(float(np.sum(times)) / 1000.0)
            else:
                vals.append(0.0)

        # Paper convention: colour = ENGINE; the SUITE is shown by hatch
        # (TPC-DS hatched, Prod-DS solid), NOT by colour.
        offset = (i - 0.5) * bar_width
        hatch = "////" if suite == "prodds" else ""   # STRIPED = Prod-DS (consistent w/ other figs)
        # TPC-DS = lighter solid shade of each engine's colour; Prod-DS = full colour + hatch.
        def _light(c, f=0.55):
            import matplotlib.colors as mc
            r, g, b = mc.to_rgb(c)
            return (r + (1 - r) * f, g + (1 - g) * f, b + (1 - b) * f)
        cols = [(_light(engine_color(e)) if suite == "tpcds" else engine_color(e))
                for e in engines]
        bars = ax.bar(x + offset, vals, bar_width,
                      color=cols, edgecolor="#333333", linewidth=0.5, hatch=hatch)
        for rect, v in zip(bars, vals):
            if v > 0:
                ax.text(rect.get_x() + rect.get_width() / 2, v, _fmt_seconds(v),
                        ha="center", va="bottom", fontsize=5.5)

    ax.set_yscale("log")
    ax.set_ylabel(ylabel)
    set_title(ax, title)
    ax.set_xticks(x)
    ax.set_xticklabels([engine_label(e) for e in engines])
    # Legend keys the hatch to the suite (engine colour is read off the x-axis).
    from matplotlib.patches import Patch
    suite_handles = [
        Patch(facecolor="#d9d9d9", edgecolor="#333333", label="TPC-DS"),
        Patch(facecolor="#d9d9d9", edgecolor="#333333", hatch="////", label="Prod-DS"),
    ]
    ax.yaxis.set_minor_locator(ticker.LogLocator(subs="auto", numticks=20))
    ax.yaxis.set_minor_formatter(ticker.NullFormatter())
    ax.set_ylim(top=ax.get_ylim()[1] * 2.5)  # headroom for value labels + legend
    ax.legend(handles=suite_handles, loc="upper left")
    fig.tight_layout()
    save_fig(fig, output_dir, fname)


def plot_fig9_cdf(summaries: Dict[str, Dict[str, List[Dict[str, Any]]]],
                  output_dir: Path) -> None:
    """CDF of per-query median runtime on Prod-DS, one line per engine."""
    fig, ax = plt.subplots(figsize=(6, 3.2 * HEIGHT_SCALE))  # flattened to ~fig13 aspect
    plotted = False

    for eng in ENGINE_ORDER:
        rows = summaries.get(eng, {}).get("prodds", [])
        times = sorted(t / 1000.0 for r in rows
                       if (t := safe_float(r.get(SUMMARY_TIME_COL))) is not None and t > 0)
        if not times:
            continue
        cdf_y = np.arange(1, len(times) + 1) / len(times)
        ax.step(times, cdf_y, where="post", label=engine_label(eng),
                color=engine_color(eng), linewidth=1.8)
        plotted = True

    if not plotted:
        plt.close(fig)
        return

    ax.set_xscale("log")
    ax.set_xlabel("Per-query median runtime (s, log scale)")
    ax.set_ylabel("CDF")
    set_title(ax, "Per-query runtime CDF on Prod-DS")
    ax.set_ylim(0, 1.02)
    ax.legend(loc="lower right")
    fig.tight_layout()
    save_fig(fig, output_dir, "fig9_runtime_cdf.png")


def plot_fig9b_cdf_engines(summaries: Dict[str, Dict[str, List[Dict[str, Any]]]],
                           output_dir: Path) -> None:
    """CDF of per-query median runtime per ENGINE — TPC-DS (solid) vs Prod-DS (dashed),
    coloured by engine (same palette as Fig 8). Shows the per-engine workload shift."""
    fig, ax = plt.subplots(figsize=(6, 3.2 * HEIGHT_SCALE))  # flattened to ~fig13 aspect
    plotted = False
    for eng in ENGINE_ORDER:
        for suite, ls in (("tpcds", "-"), ("prodds", "--")):
            rows = summaries.get(eng, {}).get(suite, [])
            times = sorted(t / 1000.0 for r in rows
                           if (t := safe_float(r.get(SUMMARY_TIME_COL))) is not None and t > 0)
            if not times:
                continue
            cdf_y = np.arange(1, len(times) + 1) / len(times)
            ax.plot(times, cdf_y, ls, color=engine_color(eng), linewidth=1.8,
                    label=(engine_label(eng) if suite == "tpcds" else "_nolegend_"))
            plotted = True
    if not plotted:
        plt.close(fig)
        return
    ax.set_xscale("log")
    ax.set_xlabel("Per-query median runtime (s, log scale)")
    ax.set_ylabel("CDF")
    set_title(ax, "Per-query runtime CDF: TPC-DS vs Prod-DS")
    ax.set_ylim(0, 1.02)
    # Two legends: engine colour (lower-right) + suite line-style key (upper-left).
    from matplotlib.lines import Line2D
    eh, en = ax.get_legend_handles_labels()
    leg1 = ax.legend(eh, en, loc="lower right", fontsize=8, framealpha=0.9)
    ax.add_artist(leg1)
    style_h = [Line2D([0], [0], color="#555555", linestyle="-", label="TPC-DS"),
               Line2D([0], [0], color="#555555", linestyle="--", label="Prod-DS")]
    ax.legend(handles=style_h, loc="upper left", fontsize=8, framealpha=0.9)
    fig.tight_layout()
    save_fig(fig, output_dir, "fig9b_cdf_tpcds_vs_prodds.png")


def _audit_error_counts(results_dir: Path, timeout_s: float) -> Dict[str, Dict[str, int]]:
    """Per-engine Prod-DS outcome counts from the E0 audit, with the Table 4 taxonomy.

    Under the audit-first protocol E1 runs the common subset only, where every query
    succeeds by construction, so counting E1 statuses would report zero failures for every
    engine. The failures live in the E0 audit pass, which runs the full query set once per
    engine; that is what both this figure and E1_error_breakdown.csv report.
    """
    if not (results_dir / "E0").is_dir():
        return {}
    try:
        sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
        from experiments.common_subset import analyse as cs_analyse  # noqa: WPS433
        rep = cs_analyse(results_dir, None, timeout_s, timeout_s * 0.033, experiment="E0")
    except Exception:                                              # noqa: BLE001
        return {}
    suite = (rep.get("suites") or {}).get("prodds")
    if not suite:
        return {}
    out: Dict[str, Dict[str, int]] = {}
    for engine, info in suite["engines"].items():
        counts: Dict[str, int] = defaultdict(int)
        counts["success"] = info["success"]
        for _q, why in info["failed"].items():
            counts[CAUSE_TO_CATEGORY.get(why["cause"], "failure")] += 1
        out[engine] = dict(counts)
    return out


def plot_fig10_errors(raw: Dict[str, Dict[str, List[Dict[str, Any]]]],
                      output_dir: Path,
                      audit_counts: Optional[Dict[str, Dict[str, int]]] = None) -> None:
    """Stacked bar: error breakdown per engine on Prod-DS."""
    if audit_counts:
        engines = [e for e in ENGINE_ORDER if e in audit_counts]
        engine_counts = {e: audit_counts[e] for e in engines}
        if not engines:
            return
    else:
        engines = [e for e in ENGINE_ORDER if e in raw and "prodds" in raw[e]]
        if not engines:
            return
        # Fallback for result trees without an audit pass: count E1 statuses per query.
        engine_counts = {}
        for eng in engines:
            per_query: Dict[str, str] = {}
            for rec in raw[eng].get("prodds", []):
                per_query[rec.get("query_id", "")] = rec.get("status", "error")
            counts: Dict[str, int] = defaultdict(int)
            for status in per_query.values():
                counts[status if status in ERROR_COLORS else "error"] += 1
            engine_counts[eng] = dict(counts)

    # Determine which categories actually appear
    cats_present = [c for c in ERROR_CATEGORIES
                    if any(engine_counts[e].get(c, 0) > 0 for e in engines)]
    if not cats_present:
        return

    fig, ax = plt.subplots(figsize=(5, 2.7))   # wide + short, like the paper
    x = np.arange(len(engines))
    bar_width = 0.55
    # Percentage-stacked (Share %), like the paper (Fig 10).
    totals = {e: max(sum(engine_counts[e].values()), 1) for e in engines}
    bottoms = np.zeros(len(engines))

    small_slot = defaultdict(int)          # per engine: how many thin labels already placed
    for cat in cats_present:
        counts = np.array([engine_counts[e].get(cat, 0) for e in engines], dtype=float)
        vals = np.array([engine_counts[e].get(cat, 0) / totals[e] * 100 for e in engines],
                        dtype=float)
        ax.bar(x, vals, bar_width, bottom=bottoms,
               label=ERROR_LABELS.get(cat, cat),
               color=ERROR_COLORS.get(cat, "#888"),
               edgecolor="white", linewidth=0.5)
        # A failure category is often a sliver: 1 of 107 queries is under one percent of the
        # bar and invisible. Write the query count next to it so the figure still reports it.
        if cat != "success":
            for xi, (n, v, b) in enumerate(zip(counts, vals, bottoms)):
                if n <= 0:
                    continue
                if v >= 4.0:
                    ax.text(xi, b + v / 2, f"{int(n)}", ha="center", va="center",
                            fontsize=7.5, color="white", fontweight="bold")
                else:
                    # Stagger thin labels so two slivers on the same bar do not collide, and
                    # send them DOWNWARD once the sliver is near the top of the bar: pointing
                    # up from there puts the callout, and its leader, into the legend band.
                    near_top = (b + v / 2) > 82.0
                    slot = small_slot[xi]
                    dy = (-(11 + 11 * slot)) if near_top else (11 + 11 * slot)
                    small_slot[xi] += 1
                    ax.annotate(f"{int(n)}", xy=(xi + bar_width / 2, b + v / 2),
                                xytext=(11, dy), textcoords="offset points",
                                ha="left", va="center", fontsize=7.5,
                                color=ERROR_COLORS.get(cat, "#888"), fontweight="bold",
                                arrowprops=dict(arrowstyle="-", linewidth=0.6,
                                                color=ERROR_COLORS.get(cat, "#888"),
                                                shrinkA=0, shrinkB=1))
        bottoms += vals

    for xi, e in enumerate(engines):
        ax.text(xi, 1.5, f"{engine_counts[e].get('success', 0)}/{totals[e]}",
                ha="center", va="bottom", fontsize=7.5, color="white", fontweight="bold")

    ax.set_ylabel("Share (%)")
    ax.set_ylim(0, 100)
    ax.set_xlim(-0.6, len(engines) - 0.25)
    set_title(ax, "Prod-DS error breakdown", pad=22)
    ax.set_xticks(x)
    ax.set_xticklabels([engine_label(e) for e in engines])
    # Legend ABOVE the axes (horizontal), like the paper — never over a bar. It gets a band
    # of its own: the sliver callouts sit just outside the bars and their leader lines reach
    # further still, so anchoring the legend flush at 1.0 put the two on top of each other.
    ax.legend(loc="lower center", bbox_to_anchor=(0.5, 1.045),
              ncol=len(cats_present), frameon=False, fontsize=7.5,
              handlelength=1.1, columnspacing=1.3)
    fig.tight_layout()
    save_fig(fig, output_dir, "fig10_error_breakdown.png")


# ── E2 Plots (Fig 11) ──────────────────────────────────────────

def _collect_e2(e2_dir: Path) -> Dict[str, List[Dict[str, Any]]]:
    """Return {engine: [records]} from E2 subdirectories."""
    result: Dict[str, List[Dict[str, Any]]] = {}
    if not e2_dir.is_dir():
        return result
    for sub in sorted(e2_dir.iterdir()):
        if not sub.is_dir():
            continue
        engine = _detect_engine(sub.name)
        raw_path = _find_latest_file(sub, "raw.jsonl")
        if raw_path:
            result.setdefault(engine, []).extend(load_jsonl(raw_path))
    return result


def _extract_join_level(rec: Dict[str, Any]) -> Optional[int]:
    """Get join count from either the field or the query_id."""
    jc = rec.get("join_count")
    if jc is not None:
        try:
            return int(jc)
        except (ValueError, TypeError):
            pass
    qid = rec.get("query_id", "")
    m = re.search(r"J(\d+)", qid)
    if m:
        return int(m.group(1))
    return None


def _aggregate_by_level(records: List[Dict[str, Any]],
                        level_fn, time_field: str,
                        require_success: bool = True
                        ) -> Tuple[List[int], List[float]]:
    """Group records by level, compute median of the given time field.

    require_success=True (default) keeps only completed queries — correct for
    EXECUTION time, where a failed query's recorded time is a partial abort
    (e.g. CedarDB J256 = 89 s time-to-OOM), not a result. Pass require_success
    =False for PLANNING time: planning completes even when execution later
    OOMs/times out (CedarDB J256 plans in 107 ms, then OOMs in execution), so
    that planning point is a valid measurement and belongs on the planning line."""
    groups: Dict[int, List[float]] = defaultdict(list)
    for rec in records:
        if require_success and rec.get("status") != "success":
            continue
        lvl = level_fn(rec)
        t = safe_float(rec.get(time_field))
        if lvl is not None and t is not None and t > 0:
            groups[lvl].append(t)

    levels = sorted(groups.keys())
    medians = [float(np.median(groups[lvl])) for lvl in levels]
    return levels, medians


def _failure_point(records: List[Dict[str, Any]], level_fn):
    """First level where an engine fully fails for a REAL reason (ignoring ladder-abandon
    placeholders). Returns (level, kind) with kind in {OOM, cell-limit, timeout, error}, else None.
    Honest distinction: CedarDB hits hard OOM / cell-limit; MonetDB merely times out."""
    by_level: Dict[int, List[Dict[str, Any]]] = {}
    for r in records:
        lv = level_fn(r)
        if lv is not None:
            by_level.setdefault(lv, []).append(r)
    for lv in sorted(by_level):
        recs = by_level[lv]
        if any(r.get("status") == "success" for r in recs):
            continue
        # Exclude DuckDB's "max expression depth" error: that's a config limit we raised in the
        # adapter (not an engine capability limit), so it must NOT be drawn as a real failure point.
        real = [r for r in recs
                if "skipped" not in str(r.get("error_type") or "")
                and "expression depth" not in str(r.get("error_message") or "").lower()]
        if not real:
            continue
        msgs = " ".join(str(r.get("error_message") or "") for r in real).lower()
        ets = " ".join(str(r.get("error_type") or "") for r in real).lower()
        if "cell count" in msgs:
            return lv, "cell-limit"
        if "out of memory" in msgs or "oom" in ets:
            return lv, "OOM"
        if "timeout" in ets:
            return lv, "timeout"
        return lv, "error"
    return None


def _annotate_failures(ax, data, level_fn, time_field: str, unit: str = "J", skip=None) -> None:
    """Mark each engine's first failure ('stop @<unit>=<level> (<kind>)') to the RIGHT of its
    last point, staggered so engines failing at the same level don't overlap (or hit the title).
    Engines in `skip` are omitted (e.g. timeouts already drawn as measured-duration points)."""
    skip = skip or set()
    fails = []
    for eng in ENGINE_ORDER:
        if eng not in data or eng in skip:
            continue
        fp = _failure_point(data[eng], level_fn)
        if not fp:
            continue
        levels, meds = _aggregate_by_level(data[eng], level_fn, time_field)
        if not levels:
            continue
        fails.append((eng, fp[0], fp[1], levels[-1], meds[-1] / 1000.0))
    for i, (eng, lv, kind, x, y) in enumerate(fails):
        ax.annotate(f"stop @{unit}={lv} ({kind})", xy=(x, y), xytext=(8, -3 - i * 13),
                    textcoords="offset points", fontsize=6.3, fontweight="bold",
                    color=engine_color(eng), ha="left", va="top", clip_on=False)


def _plot_failure_points(ax, data, level_fn, time_field: str, unit: str = "J",
                         label_dy: int = 0, label_va: str = "center") -> None:
    """Mark EVERY engine's first hard failure with an × at the failure level, placed at the measured
    time-to-failure when the engine reported one (timeout duration, or time-until-OOM/refusal),
    connected to the last success by a dotted riser. The label states the kind AND the time, e.g.
    'J=32: timeout (1.0 h)', 'J=256: OOM (89 s)', 'U=128: cell-limit'. The × is a LOWER bound on the
    true cost (the query was killed/refused, not finished). Honest per-failure-mode marking."""
    for eng in ENGINE_ORDER:
        if eng not in data:
            continue
        fp = _failure_point(data[eng], level_fn)
        if not fp:
            continue
        lv, kind = fp
        # measured time of the (non-skipped) failing query at that level, if the engine reported one
        tsec = None
        for r in data[eng]:
            if level_fn(r) == lv and "skipped" not in str(r.get("error_type") or ""):
                t = safe_float(r.get(time_field))
                if t is not None and t > 0:
                    tsec = t / 1000.0
                    break
        sl, sm = _aggregate_by_level(data[eng], level_fn, time_field)
        last_y = sm[-1] / 1000.0 if sm else None
        y = tsec if tsec is not None else last_y
        if y is None:
            continue
        col = engine_color(eng)
        if sl:  # dotted riser from the last successful point to the failure marker
            ax.plot([sl[-1], lv], [last_y, y], ":", color=col, linewidth=1.3, alpha=0.85, zorder=4)
        ax.plot([lv], [y], marker="x", color=col, markersize=9, markeredgewidth=2.2,
                linestyle="none", zorder=5)
        if tsec is not None:
            t_lbl = (f"{tsec / 3600:.1f} h" if tsec >= 3600
                     else f"{tsec / 60:.0f} min" if tsec >= 120 else f"{tsec:.0f} s")
            txt = f"{unit}={lv}: {kind}\n({t_lbl})"
        else:
            txt = f"{unit}={lv}: {kind}"
        ax.annotate(txt, xy=(lv, y), xytext=(7, label_dy), textcoords="offset points",
                    fontsize=6.3, fontweight="bold", color=col, ha="left", va=label_va,
                    clip_on=False)


def plot_fig11(e2_data: Dict[str, List[Dict[str, Any]]], output_dir: Path) -> None:
    """One log-log diagram per engine (colour): the wall time of the EXECUTION CLIENT (solid)
    and of the separately executed EXPLAIN CLIENT (dashed) against join level.

    Neither series is an engine-internal phase. `wall_time_ms_planning` is one client process
    running `EXPLAIN <query>` end to end -- spawn, connect, parse, optimise, print, exit -- so it
    is labelled "EXPLAIN client", never "planning time" (docs/08-measurement-contract.md). Both
    share one y-axis, so the two clients are read off the same scale."""
    engines = [e for e in ENGINE_ORDER if e in e2_data]
    if not engines:
        return

    fig, ax = plt.subplots(figsize=(6, 3.3 * HEIGHT_SCALE))
    plotted = False
    for eng in engines:
        col = engine_color(eng)
        ex_levels, ex_meds = _aggregate_by_level(
            e2_data[eng], _extract_join_level, "wall_time_ms_execution")
        if ex_levels:
            ax.plot(ex_levels, [m / 1000.0 for m in ex_meds], "o-", color=col,
                    linewidth=1.8, markersize=5, label=engine_label(eng))
            plotted = True
        # require_success=False: the EXPLAIN client is logged even when the execution client
        # fails, so CedarDB's J256 EXPLAIN point (107 ms) extends the dashed line one notch past
        # its solid line — the OOM happens in the execution client, not while planning.
        pl_levels, pl_meds = _aggregate_by_level(
            e2_data[eng], _extract_join_level, "wall_time_ms_planning",
            require_success=False)
        if pl_levels:
            ax.plot(pl_levels, [m / 1000.0 for m in pl_meds], "s--", color=col,
                    linewidth=1.5, markersize=4, label="_nolegend_")
            plotted = True

    # Overlay MonetDB's EXPLAIN-only probe (the harness logged None for MonetDB: its adapter skips
    # the EXPLAIN stage). Same instrument as the dashed lines — one client running EXPLAIN. Spans
    # ALL levels incl. execution-failed ones: EXPLAIN returns even when the execution client dies.
    if _MONET_JOIN_PLANNING_JSON.is_file():
        try:
            import json as _json
            pj = _json.loads(_MONET_JOIN_PLANNING_JSON.read_text())
            pts = sorted((int(k), v / 1000.0) for k, v in pj.items() if v)
            if pts:
                ax.plot([p[0] for p in pts], [p[1] for p in pts], "s--",
                        color=engine_color("monetdb"), linewidth=1.5, markersize=4,
                        label="_nolegend_", alpha=0.9)
                plotted = True
        except Exception:
            pass

    if not plotted:
        plt.close(fig)
        return

    # Mark every hard failure with an × at its measured time-to-failure (OOM / timeout-with-time) —
    # drawn before the y-headroom so autoscale includes the (often highest) timeout points.
    _plot_failure_points(ax, e2_data, _extract_join_level, "wall_time_ms_execution", "J")

    ax.set_xscale("log", base=2)
    ax.set_yscale("log")
    ax.set_xlabel("Join level")
    ax.set_ylabel(JOIN_YLABEL)
    set_title(ax, "Join scaling: execution client vs EXPLAIN client")
    ax.xaxis.set_major_formatter(ticker.ScalarFormatter())
    ax.xaxis.set_minor_formatter(ticker.NullFormatter())

    # Two legends: engine colours (upper-left) + a line-style key (lower-right) telling the
    # execution client (solid o) from the EXPLAIN client (dashed s) — they share each engine's
    # colour, so the style key is what disambiguates the two series.
    from matplotlib.lines import Line2D
    engine_handles, engine_names = ax.get_legend_handles_labels()
    style_handles = [
        Line2D([0], [0], color="#555555", linestyle="-", marker="o",
               markersize=5, label=JOIN_SERIES_LABELS[0]),
        Line2D([0], [0], color="#555555", linestyle="--", marker="s",
               markersize=4, label=JOIN_SERIES_LABELS[1]),
    ]
    # Headroom at the top, before the legends are placed: placement is decided in axes
    # coordinates, so the y range has to be final first.
    ax.set_ylim(top=ax.get_ylim()[1] * 4)
    if engine_handles:
        leg_engines = ax.legend(engine_handles, engine_names, loc="upper right",
                                fontsize=8, framealpha=0.9)
        ax.add_artist(leg_engines)
    ax.legend(handles=style_handles, loc="lower right", fontsize=8, framealpha=0.9)
    place_failure_callouts(ax)

    # (OE-8 done: MonetDB's SF100 join is protocol-faithful + version-specific, not a
    #  pending artifact — no figure footnote; discussed in the text instead.)
    # Honest failure markers: CedarDB hard-fails (OOM @ J256), MonetDB merely times out.
    fig.tight_layout()
    save_fig(fig, output_dir, "fig11_join_exec_planning.png")


# ── E3 Plot (Fig 12) ───────────────────────────────────────────

def _collect_e3(e3_dir: Path) -> Dict[str, List[Dict[str, Any]]]:
    result: Dict[str, List[Dict[str, Any]]] = {}
    if not e3_dir.is_dir():
        return result
    for sub in sorted(e3_dir.iterdir()):
        if not sub.is_dir():
            continue
        engine = _detect_engine(sub.name)
        raw_path = _find_latest_file(sub, "raw.jsonl")
        if raw_path:
            result.setdefault(engine, []).extend(load_jsonl(raw_path))
    return result


def _extract_union_level(rec: Dict[str, Any]) -> Optional[int]:
    uc = rec.get("union_count")
    if uc is not None:
        try:
            return int(uc)
        except (ValueError, TypeError):
            pass
    qid = rec.get("query_id", "")
    m = re.search(r"U(\d+)", qid)
    if m:
        return int(m.group(1))
    return None


def plot_fig12(e3_data: Dict[str, List[Dict[str, Any]]], output_dir: Path) -> None:
    """Log-log: end-to-end runtime vs UNION fan-in level."""
    engines = [e for e in ENGINE_ORDER if e in e3_data]
    if not engines:
        return

    fig, ax = plt.subplots(figsize=(6, 3.3 * HEIGHT_SCALE))
    plotted = False

    for eng in engines:
        levels, meds = _aggregate_by_level(
            e3_data[eng], _extract_union_level, RAW_TIME_FIELD)
        if not levels:
            continue
        mk = {"duckdb": "o", "cedardb": "s", "monetdb": "^", "postgres": "D"}.get(eng, "o")
        col = engine_color(eng)
        # CedarDB and MonetDB have near-identical UNION-ALL runtime for U2..U64 (both columnar,
        # scan-bound on the same base) so their lines coincide. Draw CedarDB as OPEN markers
        # lifted above MonetDB (zorder) so both series stay legible where they overlap — the
        # green triangle shows through the hollow orange square; they only diverge at the
        # failure boundary (CedarDB OOM @ U128 vs MonetDB surviving to U128, timing out @ U256).
        if eng == "cedardb":
            ax.plot(levels, [m / 1000.0 for m in meds], mk + "-", label=engine_label(eng),
                    color=col, linewidth=1.8, markersize=8, markerfacecolor="none",
                    markeredgecolor=col, markeredgewidth=1.6, zorder=6)
        else:
            ax.plot(levels, [m / 1000.0 for m in meds], mk + "-", label=engine_label(eng),
                    color=col, linewidth=1.8, markersize=5)
        plotted = True

    if not plotted:
        plt.close(fig)
        return

    # Mark every hard failure with an × at its measured time-to-failure (cell-limit / OOM / timeout
    # with time) — drawn before the y-headroom so autoscale includes the timeout points.
    _plot_failure_points(ax, e3_data, _extract_union_level, RAW_TIME_FIELD, "U",
                         label_dy=-8, label_va="top")  # push the U256 timeout label below its × so it clears the top frame

    ax.set_xscale("log", base=2)
    ax.set_yscale("log")
    ax.set_xlabel("UNION ALL fan-in level")
    ax.set_ylabel(UNION_YLABEL)
    set_title(ax, "UNION ALL scaling")
    ax.legend()
    ax.xaxis.set_major_formatter(ticker.ScalarFormatter())
    ax.xaxis.set_minor_formatter(ticker.NullFormatter())
    place_failure_callouts(ax)
    fig.tight_layout()
    save_fig(fig, output_dir, "fig12_union_runtime.png")


# ── E4 Plot (Fig 13) ───────────────────────────────────────────

def _collect_e4(e4_dir: Path) -> List[Dict[str, Any]]:
    """Collect all raw records from E4 str1..str15 subdirectories."""
    records: List[Dict[str, Any]] = []
    if not e4_dir.is_dir():
        return records
    for sub in sorted(e4_dir.iterdir()):
        if not sub.is_dir():
            continue
        raw_path = _find_latest_file(sub, "raw.jsonl")
        if raw_path:
            for rec in load_jsonl(raw_path):
                if rec.get("string_level") is None:
                    m = re.fullmatch(r"str(\d+)", sub.name)
                    if m:
                        rec["string_level"] = int(m.group(1))
                records.append(rec)
    return records


def plot_fig13(e4_records: List[Dict[str, Any]], output_dir: Path) -> None:
    """Quantile fan: normalized median runtime vs STR level."""
    # Group by (query_id, string_level)
    groups: Dict[Tuple[str, int], List[float]] = defaultdict(list)
    for rec in e4_records:
        if rec.get("status") != "success":
            continue
        qid = rec.get("query_id", "")
        sl = rec.get("string_level")
        t = safe_float(rec.get(RAW_TIME_FIELD))
        if sl is not None and t is not None and t > 0:
            groups[(qid, int(sl))].append(t)

    if not groups:
        return

    # Compute per-query median at each level
    query_level_median: Dict[str, Dict[int, float]] = defaultdict(dict)
    for (qid, sl), times in groups.items():
        query_level_median[qid][sl] = float(np.median(times))

    # Normalize each query by its value at the lowest STR level present
    all_levels = sorted({sl for _, sl in groups.keys()})
    if len(all_levels) < 2:
        return

    base_level = all_levels[0]
    normalized: Dict[int, List[float]] = defaultdict(list)
    for qid, level_map in query_level_median.items():
        base_val = level_map.get(base_level)
        if base_val is None or base_val <= 0:
            continue
        for sl in all_levels:
            val = level_map.get(sl)
            if val is not None:
                normalized[sl].append(val / base_val)

    if not normalized:
        return

    levels = sorted(normalized.keys())
    p10, p25, p50, p75, p90 = [], [], [], [], []
    for sl in levels:
        vals = sorted(normalized[sl])
        n = len(vals)
        if n < 2:
            p10.append(vals[0]); p25.append(vals[0]); p50.append(vals[0])
            p75.append(vals[0]); p90.append(vals[0])
            continue
        p10.append(float(np.percentile(vals, 10)))
        p25.append(float(np.percentile(vals, 25)))
        p50.append(float(np.percentile(vals, 50)))
        p75.append(float(np.percentile(vals, 75)))
        p90.append(float(np.percentile(vals, 90)))

    fig, ax = plt.subplots(figsize=(7, 3.7 * HEIGHT_SCALE))
    levels_arr = np.array(levels)

    ax.fill_between(levels_arr, p10, p90, alpha=0.15, color="#58b4dd", label="P10\u2013P90")
    ax.fill_between(levels_arr, p25, p75, alpha=0.30, color="#58b4dd", label="P25\u2013P75")
    ax.plot(levels_arr, p50, "o-", color="#58b4dd", linewidth=2, markersize=5, label="Median")

    ax.axhline(1.0, color="#999999", linestyle="--", linewidth=0.8)
    ax.set_xlabel("Stringification level (STR)")
    ax.set_ylabel(f"Normalized runtime (vs STR={base_level})")
    set_title(ax, "Stringification sweep: runtime by STR level")
    ax.set_xticks(levels)
    ax.legend(loc="upper left")
    fig.tight_layout()
    save_fig(fig, output_dir, "fig13_str_quantile.png")


# ── E5 Plot (Table 3) ──────────────────────────────────────────

def _collect_e5(e5_dir: Path) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
    """Return {variant: {engine: [records]}} from E5 subdirectories.

    Directory names follow the pattern: <variant>_<engine>
    e.g. baseline_duckdb, sparsity_only_duckdb, combined_duckdb
    """
    result: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
    if not e5_dir.is_dir():
        return result

    for sub in sorted(e5_dir.iterdir()):
        if not sub.is_dir():
            continue
        engine = _detect_engine(sub.name)
        # Strip engine name to get variant
        variant = sub.name
        for eng in ENGINE_ORDER:
            variant = variant.replace(f"_{eng}", "").replace(eng, "")
        variant = variant.strip("_") or sub.name

        raw_path = _find_latest_file(sub, "raw.jsonl")
        if raw_path:
            result.setdefault(variant, {}).setdefault(engine, []).extend(
                load_jsonl(raw_path))
    return result


def plot_table3(e5_data: Dict[str, Dict[str, List[Dict[str, Any]]]],
                output_dir: Path) -> None:
    """Bar chart showing delta-% vs baseline for each variant and engine."""
    if "baseline" not in e5_data:
        return

    # Compute per-engine total successful runtime for each variant
    variants = [v for v in ["sparsity_only", "skew_only", "keyskew_only", "skew_all", "combined", "full"]
                if v in e5_data]
    if not variants:
        return

    all_engines = set()
    for vdata in e5_data.values():
        all_engines.update(vdata.keys())
    engines = [e for e in ENGINE_ORDER if e in all_engines]
    if not engines:
        return

    # Common-success-set method: compare only queries that succeed in BOTH baseline
    # and the variant (per engine), aggregated to per-query median over reps. Avoids
    # the timeout-confounding of a naive sum (esp. MonetDB) — matches the revision's
    # S5 analysis (otherwise MonetDB looked like +19% on sparsity instead of ~-10%).
    def _success_map(records: List[Dict[str, Any]]) -> Dict[str, float]:
        per_q: Dict[str, List[float]] = {}
        for r in records:
            if r.get("status") != "success":
                continue
            t = safe_float(r.get(RAW_TIME_FIELD))
            q = r.get("query_id")
            if t is not None and t > 0 and q:
                per_q.setdefault(q, []).append(t)
        return {q: float(np.median(ts)) for q, ts in per_q.items()}

    baseline_maps = {eng: _success_map(e5_data["baseline"].get(eng, []))
                     for eng in engines}

    fig, ax = plt.subplots(figsize=(6, 3.2 * HEIGHT_SCALE))  # flattened to ~fig13 aspect
    n_variants = len(variants)
    n_engines = len(engines)
    bar_width = 0.8 / max(n_engines, 1)
    x = np.arange(n_variants)

    for j, eng in enumerate(engines):
        deltas = []
        for var in variants:
            var_map = _success_map(e5_data.get(var, {}).get(eng, []))
            base_map = baseline_maps.get(eng, {})
            common = set(base_map) & set(var_map)
            base_total = sum(base_map[q] for q in common)
            if common and base_total > 0:
                var_total = sum(var_map[q] for q in common)
                deltas.append((var_total - base_total) / base_total * 100)
            else:
                deltas.append(0)

        offset = (j - (n_engines - 1) / 2) * bar_width
        ax.bar(x + offset, deltas, bar_width, label=engine_label(eng),
               color=engine_color(eng), edgecolor="white", linewidth=0.5)

    ax.axhline(0, color="#333333", linewidth=0.8)
    # NOT "Δ% total runtime": no complete workload pass was ever timed. This is the change in
    # the SUM of per-query medians over the queries that succeed in BOTH the baseline and the
    # arm — a percentage change of a sum, not a median of per-query percentages. Two lines,
    # because one line of this at the placed size runs past both ends of the axis.
    ax.set_ylabel("$\\Delta$ summed query\nmedians (%)")
    set_title(ax, "Sparsity & skew sensitivity")
    ax.set_xticks(x)
    # Two lines each: at the placed size the one-line names of neighbouring categories run
    # into each other.
    # Named by mechanism, two lines, so neighbouring categories do not touch (`docs/05`).
    variant_labels = {
        "sparsity_only": "NULL\nonly",
        "skew_only":     "MCV\nonly",
        "keyskew_only":  "Keys\nonly",
        "skew_all":      "MCV +\nkeys",
        "combined":      "NULL +\nMCV",
        "full":          "All\nthree",
    }
    ax.set_xticklabels([variant_labels.get(v, v) for v in variants])
    ax.legend()
    fig.tight_layout()
    save_fig(fig, output_dir, "table3_sparsity_skew.png")


# ── Main ────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate paper figures from experiment results.")
    parser.add_argument("--results-dir", required=True, type=Path,
                        help="Base results directory (e.g. .reproduce/results)")
    parser.add_argument("--output-dir", required=True, type=Path,
                        help="Directory to save PNG plots")
    parser.add_argument("--common-subset", type=Path, default=None,
                        help="common_subset.json written by experiments/common_subset.py; restricts the "
                             "E1 runtime figures (Figs 8/9) to the common success set per suite "
                             "(the error breakdown, Fig 10, keeps every query)")
    add_title_argument(parser)
    args = parser.parse_args()
    apply_title_argument(args)

    results_dir: Path = args.results_dir
    output_dir: Path = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    apply_style()

    if not results_dir.is_dir():
        print(f"Results directory does not exist: {results_dir}", file=sys.stderr)
        sys.exit(1)

    e1_dir = results_dir / "E1"
    e2_dir = results_dir / "E2"
    e3_dir = results_dir / "E3"
    e4_dir = results_dir / "E4"
    e5_dir = results_dir / "E5"

    generated = 0

    # ── E1 ──────────────────────────────────────────────────────
    if e1_dir.is_dir():
        print("E1: generating Figs 8, 9, 10 ...")
        summaries = _collect_e1_summaries(e1_dir)
        if args.common_subset:
            summaries = _restrict_to_common_subset(summaries, args.common_subset)
        raw = _collect_e1_raw(e1_dir)

        if summaries:
            plot_fig8_bar(summaries, output_dir, "median",
                          "fig8a_median_runtime.png",
                          "Median query runtime: TPC-DS vs Prod-DS")
            plot_fig8_bar(summaries, output_dir, "mean",
                          "fig8b_average_runtime.png",
                          "Average query runtime: TPC-DS vs Prod-DS")
            plot_fig8_bar(summaries, output_dir, "sum",
                          "fig8c_total_runtime.png",
                          "Total workload runtime: TPC-DS vs Prod-DS",
                          ylabel="Total workload runtime (s)")
            plot_fig9_cdf(summaries, output_dir)
            plot_fig9b_cdf_engines(summaries, output_dir)
            generated += 4

        audit_counts = _audit_error_counts(results_dir, 1800.0)
        if raw or audit_counts:
            plot_fig10_errors(raw, output_dir, audit_counts)
            generated += 1
    else:
        print("E1: skipped (directory not found)")

    # ── E2 ──────────────────────────────────────────────────────
    if e2_dir.is_dir():
        print("E2: generating Fig 11 ...")
        e2_data = _collect_e2(e2_dir)
        if e2_data:
            plot_fig11(e2_data, output_dir)
            generated += 1
    else:
        print("E2: skipped (directory not found)")

    # ── E3 ──────────────────────────────────────────────────────
    if e3_dir.is_dir():
        print("E3: generating Fig 12 ...")
        e3_data = _collect_e3(e3_dir)
        if e3_data:
            plot_fig12(e3_data, output_dir)
            generated += 1
    else:
        print("E3: skipped (directory not found)")

    # ── E4 ──────────────────────────────────────────────────────
    if e4_dir.is_dir():
        print("E4: generating Fig 13 ...")
        e4_records = _collect_e4(e4_dir)
        if e4_records:
            plot_fig13(e4_records, output_dir)
            generated += 1
    else:
        print("E4: skipped (directory not found)")

    # ── E5 ──────────────────────────────────────────────────────
    if e5_dir.is_dir():
        print("E5: generating Table 3 plot ...")
        e5_data = _collect_e5(e5_dir)
        if e5_data:
            plot_table3(e5_data, output_dir)
            generated += 1
    else:
        print("E5: skipped (directory not found)")

    print(f"\nDone. {generated} plot(s) generated in {output_dir}")


if __name__ == "__main__":
    main()
