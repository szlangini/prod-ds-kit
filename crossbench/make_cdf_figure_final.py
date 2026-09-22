#!/usr/bin/env python3
"""Cross-benchmark runtime CDF and summary tables — final protocol.

Aggregation, stated once and applied everywhere:

  * a query's runtime is the MEDIAN of its successful TIMED executions (10 passes);
  * the MAIN CDF contains only queries that succeeded in ALL ten timed passes. A query that
    succeeded in some passes is reported separately as a partial success, never averaged in;
  * the warmup pass is untimed and never enters any statistic, but its failures are reported;
  * percentiles use numpy's default LINEAR interpolation on the per-query medians;
  * every curve is over QUERY INSTANCES. Where a suite declares instance multiplicities
    (Redbench/Krid: 8,784 instances over 763 distinct files), each measured median is repeated
    by its multiplicity. A distinct-SQL view is reported alongside it. Re-weighting measured
    medians is NOT a sequential replay of the original order and does not reproduce whatever
    cache behaviour that order would have produced.

Prod-DS appears twice from the SAME run records: all 107 queries, and the 99 templates without
the eight join/union amplification micro-queries.

Usage:
    .venv/bin/python make_cdf_figure_final.py [--results results/final] [--out-dir figures]
"""
from __future__ import annotations

import argparse, csv, json, statistics
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

HERE = Path(__file__).resolve().parent
MICRO_PREFIXES = ("query_join_", "query_union_")

STYLE = {
    "prodds":  dict(label="Prod-DS (default, 107)", color="#FF8C00", dashes=(None, None), marker="o", z=6),
    "prodds_templates": dict(label="Prod-DS, 99 templates", color="#FF8C00", dashes=(3.0, 1.6), marker=None, z=6),
    "tpcds":   dict(label="TPC-DS", color="#2A9D8F", dashes=(None, None), marker="s", z=5),
    "dsb":     dict(label="DSB", color="#9BC53D", dashes=(1.3, 1.3), marker="D", z=4),
    "job":     dict(label="JOB", color="#E07A5F", dashes=(5.5, 1.5, 1.0, 1.5), marker="P", z=4),
    "redbench_krid": dict(label="Redbench (Krid et al.)", color="#6A4C93", dashes=(4.0, 1.4), marker="^", z=3),
    "sqlbarber": dict(label="SQLBarber", color="#1D7DBF", dashes=(2.0, 1.0, 0.5, 1.0), marker="v", z=3),
    "redbench_wehrstein_reads": dict(label="Redbench (Wehrstein et al.), reads", color="#F2C744", dashes=(2.4, 1.2), marker="X", z=2),
    "redbench_wehrstein_writes": dict(label="Redbench (Wehrstein et al.), writes", color="#B08968", dashes=(1.0, 1.0), marker=None, z=1),
}
# Short display names for the one-column paper export. The full-width response-letter figure
# keeps the longer labels in STYLE. What must survive the shortening, and does: both Prod-DS
# populations stay distinguishable, both RedBench author labels stay, the read-only qualifier on
# the Wehrstein curve stays, and Krid's instance-against-distinct-SQL distinction stays. The
# populations are read from the data, never written here, so a label cannot drift from its curve.
PAPER_NAME = {
    "prodds": "Prod-DS", "prodds_templates": "Prod-DS", "tpcds": "TPC-DS", "dsb": "DSB",
    "job": "JOB", "redbench_krid": "Redbench, Krid et al.", "sqlbarber": "SQLBarber",
    "redbench_wehrstein_reads": "Redbench, Wehrstein et al., reads",
}
# An extra word inside the count parenthesis, so "Prod-DS (99 templates)" reads as a population
# rather than repeating it. This is what removes the doubled 107 of the default label.
PAPER_QUALIFIER = {"prodds_templates": " templates"}

# The two exports. Same curves, same data, same axes: only the canvas and the legend composition
# differ. `final` is the accepted response-letter figure and its numbers must not move.
# `paper` is the one-column figure: 5.9 x 2.44 in, which placed at the 3.337 in manuscript column
# is about 1.38 in tall, the height of fig9b_cdf_tpcds_vs_prodds (1.359) and
# fig11_join_exec_planning (1.404). Its legend stays inside the axes, in the empty lower-right
# corner that no curve enters, in one column so that it never reaches left into the rising curves.
FIGURE_VARIANTS = {
    "cdf_crossbench_final": dict(
        figsize=(7.2, 4.0), legend_fontsize=7.2, short_labels=False,
        legend_kw=dict(loc="lower right", framealpha=0.95, edgecolor="#bbbbbb")),
    "cdf_crossbench_paper": dict(
        figsize=(6.8, 2.40), legend_fontsize=7.0, short_labels=True,
        legend_kw=dict(loc="lower right", framealpha=0.95, edgecolor="#bbbbbb", ncol=1,
                       borderpad=0.30, labelspacing=0.22, handlelength=1.5,
                       handletextpad=0.4, borderaxespad=0.3)),
}


# Order for figure and tables. The write suite is deliberately last and is NOT analytical.
ORDER = ["prodds", "prodds_templates", "tpcds", "dsb", "job", "redbench_krid", "sqlbarber",
         "redbench_wehrstein_reads", "redbench_wehrstein_writes"]
IN_MAIN_FIGURE = set(ORDER) - {"redbench_wehrstein_writes"}


def weights_for(suite: str) -> Dict[str, int]:
    sel = HERE / "queries" / suite / "_selection.json"
    if not sel.exists():
        return {}
    d = json.loads(sel.read_text(encoding="utf-8"))
    return {q["query_name"]: int(q.get("instances", 1)) for q in d.get("queries", [])
            if int(q.get("instances", 1)) > 1}


def load(results: Path):
    rows = list(csv.DictReader((results / "latencies.csv").open(newline="", encoding="utf-8")))
    manifest = json.loads((results / "manifest.json").read_text(encoding="utf-8"))
    n_passes = int(manifest.get("timed_passes", 10))

    # per suite -> per query -> list of (pass_id, ms, status)
    attempts: Dict[str, Dict[str, List[Tuple[int, float, str]]]] = defaultdict(lambda: defaultdict(list))
    warm_fail: Dict[str, List[Tuple[str, str]]] = defaultdict(list)
    for r in rows:
        p = int(r["pass_id"])
        rec = (p, float(r["latency_ms"]), r["status"])
        if p == 0:
            if r["status"] != "ok":
                warm_fail[r["suite"]].append((r["query_name"], r["status"]))
            continue
        attempts[r["suite"]][r["query_name"]].append(rec)
    return attempts, warm_fail, manifest, n_passes, rows


def classify(attempts, n_passes, manifest=None):
    """-> per suite: complete (all of THAT suite's passes ok), partial, failed; medians and IQR.

    "All passes" is per suite, not global: a write suite runs a single timed pass by design
    (its mutations are not idempotent), so judging it against the read suites' ten would mark
    every one of its statements a partial success.
    """
    out = {}
    for suite, qs in attempts.items():
        expected = n_passes
        if manifest:
            expected = int((manifest.get("suites", {}).get(suite, {}) or {})
                           .get("passes_timed", n_passes))
        complete, partial, failed = {}, {}, {}
        for q, recs in qs.items():
            ok = [ms for _, ms, st in recs if st == "ok"]
            n_timed = len(recs)
            if len(ok) == n_timed == expected:
                complete[q] = ok
            elif ok:
                partial[q] = (ok, n_timed)
            else:
                failed[q] = [st for _, _, st in recs]
        out[suite] = dict(complete=complete, partial=partial, failed=failed)
    return out


def instance_values(suite: str, complete: Dict[str, List[float]]) -> List[float]:
    """Per-query medians, repeated by instance multiplicity where the suite declares one."""
    w = weights_for(suite)
    vals = []
    for q, ok in complete.items():
        vals.extend([statistics.median(ok)] * w.get(q, 1))
    return vals


def pct(vals, q):
    return float(np.percentile(np.asarray(vals), q)) if vals else float("nan")


def draw_cdf(ax, suites, cls, short_labels: bool) -> Dict[str, np.ndarray]:
    """Draw the eight curves on `ax` and return exactly what was plotted, keyed by suite.

    Both exports call this, so they cannot diverge in what they show: the only argument that
    differs between them is the label vocabulary, which no curve depends on. main() compares the
    returned arrays across the two variants and refuses to finish if they are not identical.
    """
    plotted: Dict[str, np.ndarray] = {}
    for name in suites:
        if name not in IN_MAIN_FIGURE:
            continue
        vals = sorted(v / 1000.0 for v in instance_values(name, cls[name]["complete"]))
        if not vals:
            continue
        st = STYLE[name]
        y = np.arange(1, len(vals) + 1) / len(vals)
        kw = dict(color=st["color"], zorder=st["z"],
                  linewidth=1.1 if name.endswith("_templates") else 1.6,
                  alpha=0.85 if name.endswith("_templates") else 1.0)
        if st["marker"]:
            kw.update(marker=st["marker"], markersize=3.8, markevery=0.13,
                      markeredgecolor="white", markeredgewidth=0.6)
        if st["dashes"][0] is not None:
            kw["dashes"] = st["dashes"]
        nq = len(cls[name]["complete"])
        npart = len(cls[name]["partial"]) + len(cls[name]["failed"])
        if short_labels:
            count = f"{len(vals):,} inst / {nq} SQL" if len(vals) != nq else f"{len(vals):,}"
            lbl = f"{PAPER_NAME[name]} ({count}{PAPER_QUALIFIER.get(name, '')}"
        else:
            lbl = f"{st['label']}  (n={len(vals):,}" + (
                f" inst / {nq} SQL" if len(vals) != nq else "")
        if npart:
            lbl += f", {npart} excl."
        lbl += ")"
        ax.plot(vals, y, label=lbl, **kw)
        plotted[name] = np.column_stack([vals, y])
    return plotted


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--results", type=Path, default=HERE / "results/final")
    ap.add_argument("--out-dir", type=Path, default=HERE / "figures")
    args = ap.parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)

    attempts, warm_fail, manifest, n_passes, raw_rows = load(args.results)
    cls = classify(attempts, n_passes, manifest)

    # Prod-DS's 99-template view, from the SAME records
    if "prodds" in cls:
        p = cls["prodds"]
        cls["prodds_templates"] = dict(
            complete={q: v for q, v in p["complete"].items() if not q.startswith(MICRO_PREFIXES)},
            partial={q: v for q, v in p["partial"].items() if not q.startswith(MICRO_PREFIXES)},
            failed={q: v for q, v in p["failed"].items() if not q.startswith(MICRO_PREFIXES)})

    suites = [s for s in ORDER if s in cls and cls[s]["complete"]]

    # ── figures ──────────────────────────────────────────────────
    # Both exports are drawn from the same `cls`, in the same order, with the same styles, so the
    # curves cannot diverge between them. Only the canvas and the legend composition differ; the
    # plotted coordinates, the log scale, the limits and the ticks are identical by construction.
    plotted: Dict[str, Dict[str, np.ndarray]] = {}
    axes_state: Dict[str, tuple] = {}
    for stem, spec in FIGURE_VARIANTS.items():
        plt.rcParams.update({
            "font.family": "sans-serif", "font.sans-serif": ["DejaVu Sans"],
            "font.size": 9, "axes.labelsize": 9, "xtick.labelsize": 8, "ytick.labelsize": 8,
            "legend.fontsize": spec["legend_fontsize"], "axes.linewidth": 0.6,
            "pdf.fonttype": 42, "ps.fonttype": 42,
            "figure.dpi": 150, "savefig.dpi": 200, "savefig.bbox": "tight",
        })
        fig, ax = plt.subplots(figsize=spec["figsize"])
        plotted[stem] = draw_cdf(ax, suites, cls, spec["short_labels"])

        ax.set_xscale("log")
        ax.set_xlabel("Query runtime (s, log scale)")
        ax.set_ylabel("Fraction of query instances")
        ax.set_ylim(0, 1.02)
        ax.grid(True, which="major", linestyle=":", linewidth=0.4, color="#aaaaaa", alpha=0.7)
        ax.grid(True, which="minor", linestyle=":", linewidth=0.25, color="#cccccc", alpha=0.5)
        ax.set_axisbelow(True)
        ax.legend(**spec["legend_kw"]).set_zorder(10)
        axes_state[stem] = (ax.get_xlim(), ax.get_ylim(),
                            tuple(ax.get_xticks()), tuple(ax.get_yticks()))
        # No plot title and no footer on either export. The bottom description was removed at the
        # author's direction: the scope it carried (engine build, thread count, pass protocol,
        # which queries the curves contain, and RedBench's read/write split) belongs in the
        # response letter, not under the axes. With savefig.bbox="tight" the page simply loses
        # the band that text needed.
        for ext in ("pdf", "png"):
            fig.savefig(args.out_dir / f"{stem}.{ext}")
        plt.close(fig)
        print(f"[cdf] wrote {args.out_dir / (stem + '.pdf')} and .png")

    # The two exports must be the same picture at two sizes. Comparing what was handed to
    # matplotlib is stronger than comparing the rendered pages, which differ in canvas by design:
    # it catches a divergence in the data path, which a page comparison cannot see.
    stems = list(plotted)
    ref = plotted[stems[0]]
    for other in stems[1:]:
        if set(plotted[other]) != set(ref):
            raise SystemExit(f"{other} and {stems[0]} do not carry the same suites")
        for name in ref:
            if not np.array_equal(plotted[other][name], ref[name]):
                raise SystemExit(f"{other}: plotted coordinates differ from {stems[0]} at {name}")
        if axes_state[other] != axes_state[stems[0]]:
            raise SystemExit(f"{other}: axis limits or ticks differ from {stems[0]}")
    print(f"[cdf] {len(ref)} curves, {sum(len(v) for v in ref.values()):,} plotted points, "
          f"identical coordinates, limits and ticks across: {', '.join(stems)}")

    # ── per-query summary ────────────────────────────────────────
    qrows = []
    for name in [s for s in ORDER if s in cls]:
        w = weights_for(name)
        for q, ok in sorted(cls[name]["complete"].items()):
            a = np.asarray(ok)
            qrows.append([name, q, "complete", len(ok), w.get(q, 1),
                          f"{statistics.median(ok):.3f}", f"{a.min():.3f}", f"{a.max():.3f}",
                          f"{np.percentile(a,75)-np.percentile(a,25):.3f}"])
        for q, (ok, n) in sorted(cls[name]["partial"].items()):
            a = np.asarray(ok)
            qrows.append([name, q, f"partial_{len(ok)}_of_{n}", len(ok), w.get(q, 1),
                          f"{statistics.median(ok):.3f}", f"{a.min():.3f}", f"{a.max():.3f}",
                          f"{np.percentile(a,75)-np.percentile(a,25):.3f}"])
        for q in sorted(cls[name]["failed"]):
            qrows.append([name, q, "failed", 0, w.get(q, 1), "", "", "", ""])
    with (args.results / "query_summary.csv").open("w", newline="", encoding="utf-8") as fh:
        wr = csv.writer(fh)
        wr.writerow(["suite", "query_name", "outcome", "n_successful_timed_runs",
                     "instance_weight", "median_ms", "min_ms", "max_ms", "iqr_ms"])
        wr.writerows(qrows)
    print(f"  wrote query_summary.csv ({len(qrows)} rows)")

    # ── failures ─────────────────────────────────────────────────
    frows = []
    for r in raw_rows:
        if r["status"] != "ok":
            frows.append([r["suite"], r["pass_id"],
                          "warmup" if r["pass_id"] == "0" else "timed",
                          r["query_name"], r["status"], r["latency_ms"], r["error"]])
    with (args.results / "failures.csv").open("w", newline="", encoding="utf-8") as fh:
        wr = csv.writer(fh)
        wr.writerow(["suite", "pass_id", "pass_kind", "query_name", "status",
                     "elapsed_ms", "error"])
        wr.writerows(frows)
    print(f"  wrote failures.csv ({len(frows)} rows)")

    # ── suite summary ────────────────────────────────────────────
    srows = []
    for name in [s for s in ORDER if s in cls]:
        c = cls[name]
        vals = [v / 1000.0 for v in instance_values(name, c["complete"])]
        base = "prodds" if name == "prodds_templates" else name
        info = dict(manifest["suites"].get(base, {}))
        size = info.get("source_data_bytes", {}) or {}
        n_attempt = len(c["complete"]) + len(c["partial"]) + len(c["failed"])
        micro_share = ""
        if name == "prodds":
            tot = sum(statistics.median(v) for v in c["complete"].values())
            mic = sum(statistics.median(v) for q, v in c["complete"].items()
                      if q.startswith(MICRO_PREFIXES))
            micro_share = f"{100 * mic / tot:.2f}" if tot else ""
        # A derived subset has no wall time of its own: the runner timed a pass over the whole
        # suite. Carrying the parent's 107-query wall time on a 99-query row would be wrong, so it
        # is marked unavailable and the sum over that subset's own query timers is given instead.
        derived = name.endswith("_templates")
        passes = {} if derived else info.get("measured_pass_wall_s", {})
        own_pass_sums = defaultdict(float)
        for q, recs in attempts.get(base, {}).items():
            if derived and q.startswith(MICRO_PREFIXES):
                continue
            for p_, ms_, st_ in recs:
                if p_ > 0 and st_ == "ok":
                    own_pass_sums[p_] += ms_
        srows.append([
            name, STYLE[name]["label"], info.get("kind", ""),
            f"{size.get('logical', 0) / 2**30:.2f}" if size else "",
            f"{(size.get('on_disk') or 0) / 2**30:.2f}" if size else "",
            n_attempt, len(c["complete"]), len(c["partial"]), len(c["failed"]),
            len(vals), len(warm_fail.get(base, [])),
            f"{pct(vals,50):.4f}", f"{pct(vals,90):.4f}", f"{pct(vals,95):.4f}",
            f"{pct(vals,99):.4f}", f"{max(vals):.4f}" if vals else "",
            f"{min(vals):.4f}" if vals else "",
            f"{max(vals)/min(vals):.0f}" if vals and min(vals) > 0 else "",
            f"{statistics.median(passes.values()):.2f}" if passes else "unavailable",
            f"{statistics.median(own_pass_sums.values()) / 1000:.3f}" if own_pass_sums else "",
            micro_share,
        ])
    hdr = ["suite", "label", "kind", "data_logical_gib", "data_on_disk_gib",
           "queries_attempted", "complete_all_passes", "partial_success", "failed_all_passes",
           "instances_in_cdf", "warmup_failures", "median_s", "p90_s", "p95_s", "p99_s",
           "max_s", "min_s", "spread_x", "median_measured_pass_wall_s",
           "median_sum_of_own_query_timers_per_pass_s",
           "amplification_share_of_total_pct"]
    for dest in (args.out_dir / "cdf_crossbench_summary_final.csv",
                 args.results / "suite_summary.csv"):
        with dest.open("w", newline="", encoding="utf-8") as fh:
            wr = csv.writer(fh); wr.writerow(hdr); wr.writerows(srows)
    print(f"  wrote suite_summary.csv ({len(srows)} rows)")

    def ms(v):        # a suite with no complete query has empty statistics, not zero ones
        try:
            return f"{float(v) * 1000:.0f} ms"
        except (TypeError, ValueError):
            return "—"

    def sec(v):
        try:
            return f"{float(v):.1f} s"
        except (TypeError, ValueError):
            return "—"

    md = ["| suite | kind | data GiB | attempted | complete | partial | failed | instances | median | P90 | P99 | max | spread |",
          "|---|---|---|---|---|---|---|---|---|---|---|---|---|"]
    for r in srows:
        md.append(f"| {r[1]} | {r[2]} | {r[3]} | {r[5]} | {r[6]} | {r[7]} | {r[8]} | {r[9]:,} | "
                  f"{ms(r[11])} | {ms(r[12])} | {ms(r[14])} | {sec(r[15])} | "
                  f"{r[17] + 'x' if r[17] else '—'} |")
    (args.out_dir / "cdf_crossbench_summary_final.md").write_text("\n".join(md) + "\n",
                                                                  encoding="utf-8")
    print("\n".join(md))

    # ── per-pass workload totals ─────────────────────────────────
    # Two numbers per pass: the wall time the runner measured for the whole pass, and the sum of
    # its per-query latencies. They differ by the harness overhead between queries, which is what
    # makes the pair worth reporting rather than either alone.
    pass_rows = []
    for name in [s for s in ORDER if s in attempts and not s.endswith("_templates")]:
        wall = (manifest["suites"].get(name, {}) or {}).get("measured_pass_wall_s", {})
        sums = defaultdict(float)
        cnt = defaultdict(int)
        for q, recs in attempts[name].items():
            for p_, ms_, st_ in recs:
                if st_ == "ok":
                    sums[p_] += ms_
                    cnt[p_] += 1
        for p_ in sorted(sums):
            w = wall.get(str(p_))
            pass_rows.append([name, p_, cnt[p_], f"{sums[p_] / 1000:.3f}",
                              f"{w:.3f}" if w is not None else "",
                              f"{w - sums[p_] / 1000:.3f}" if w is not None else ""])
    with (args.results / "pass_totals.csv").open("w", newline="", encoding="utf-8") as fh:
        wr = csv.writer(fh)
        wr.writerow(["suite", "pass_id", "queries_ok", "sum_of_query_latencies_s",
                     "measured_pass_wall_s", "harness_overhead_s"])
        wr.writerows(pass_rows)
    print(f"  wrote pass_totals.csv ({len(pass_rows)} rows)")

    # ── tail: slowest queries per suite ──────────────────────────
    trows = []
    for name in [s for s in ORDER if s in cls]:
        c = cls[name]["complete"]
        if not c:
            continue
        tot = sum(statistics.median(v) for v in c.values())
        for q, v in sorted(c.items(), key=lambda kv: -statistics.median(kv[1]))[:10]:
            m = statistics.median(v)
            trows.append([name, q, f"{m/1000:.4f}", f"{100*m/tot:.2f}"])
    with (args.out_dir / "cdf_crossbench_tail_final.csv").open("w", newline="", encoding="utf-8") as fh:
        wr = csv.writer(fh)
        wr.writerow(["suite", "query_name", "median_s", "share_of_suite_total_pct"])
        wr.writerows(trows)
    print(f"  wrote cdf_crossbench_tail_final.csv ({len(trows)} rows)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
