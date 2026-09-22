#!/usr/bin/env python3
"""Export every E1 timing sample, and summarise the workload under each named measure.

The harness records three numbers per (query, repetition):

  wall_time_ms_planning    wall time of a whole client process running `EXPLAIN <query>`
  wall_time_ms_execution   wall time of a second client process running the query
  wall_time_ms_total       the sum of the two -- this is what every delivered figure uses

Each part spans process spawn, client start, connection or database open, parse, optimise, and for
the execution stage the run itself and printing the complete result. So the "total" is the cost of
running the query once AND explaining it once, each from a cold client.

This exports the samples so the author can choose a statistic from evidence rather than argument,
and summarises the workload under each measure computed the same way: per query take the median
over its repetitions OF THAT MEASURE, then sum over queries. It never subtracts one median from
another -- a median total minus a median EXPLAIN time is not a median execution time.

Usage:
    python experiments/export_timing_samples.py --sf 100 --out-dir <handoff>/data
"""
from __future__ import annotations

import argparse, csv, json, re, statistics
from pathlib import Path
from typing import Any, Dict, List, Optional

REPO = Path(__file__).resolve().parent.parent
POS_RE = re.compile(r"^query_(\d+)$")


def load_jsonl(p: Path) -> List[Dict[str, Any]]:
    return [json.loads(l) for l in p.read_text(encoding="utf-8").splitlines() if l.strip()]


def latest_raw(unit: Path) -> Optional[Path]:
    c = sorted(unit.glob("*/*/*/raw.jsonl"))
    return c[-1] if c else None


def permutation(sf: str, suite: str) -> Dict[int, int]:
    for eng in ("duckdb", "cedardb", "monetdb"):
        for nm in (f"{suite}_run", suite):
            p = REPO / f".reproduce/sf{sf}/queries/{eng}/{nm}/_permutation.json"
            if p.exists():
                return {int(v): int(k) for k, v in json.loads(p.read_text()).items()}
    return {}


def write_csv(path: Path, header: List[str], rows: List[List[Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(header)
        w.writerows(rows)
    print(f"  wrote {path.name} ({len(rows)} rows)")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sf", default="100")
    ap.add_argument("--experiment", default="E1")
    ap.add_argument("--results-dir", type=Path, default=None)
    ap.add_argument("--out-dir", type=Path,
                    default=Path("/home/jvs34/prodds-revision-handoff/data"))
    args = ap.parse_args()
    results = args.results_dir or REPO / f".reproduce/sf{args.sf}/results_keyskew"
    exp = results / args.experiment
    if not exp.is_dir():
        raise SystemExit(f"no {args.experiment} under {results}")

    samples: List[List[Any]] = []
    per_measure: Dict[tuple, Dict[str, List[float]]] = {}

    for unit in sorted(p for p in exp.iterdir() if p.is_dir()):
        raw = latest_raw(unit)
        if raw is None:
            continue
        engine, suite = unit.name.split("_", 1)
        perm = permutation(args.sf, suite)
        for r in load_jsonl(raw):
            if r.get("status") != "success":
                continue
            q = r["query_id"]
            m = POS_RE.match(q)
            tmpl = f"Q{perm[int(m.group(1))]}" if (m and int(m.group(1)) in perm) else ""
            pl, ex, to = (r.get("wall_time_ms_planning"), r.get("wall_time_ms_execution"),
                          r.get("wall_time_ms_total"))
            samples.append([suite, engine, q, tmpl, r.get("repetition_index"),
                            pl if pl is not None else "", ex if ex is not None else "",
                            to if to is not None else ""])
            d = per_measure.setdefault((suite, engine, q), {"explain": [], "execution": [], "total": []})
            if isinstance(pl, (int, float)):
                d["explain"].append(float(pl))
            if isinstance(ex, (int, float)):
                d["execution"].append(float(ex))
            if isinstance(to, (int, float)):
                d["total"].append(float(to))

    write_csv(args.out_dir / f"{args.experiment}_timing_samples_SF{args.sf}.csv",
              ["suite", "engine", "query_id", "template", "repetition",
               "explain_client_ms", "execution_client_ms", "reported_total_ms"], samples)

    # workload summaries, each measure treated identically: median per query, then sum
    agg: Dict[tuple, Dict[str, float]] = {}
    for (suite, engine, q), d in per_measure.items():
        a = agg.setdefault((suite, engine), {"explain": 0.0, "execution": 0.0, "total": 0.0, "n": 0})
        for k in ("explain", "execution", "total"):
            if d[k]:
                a[k] += statistics.median(d[k])
        a["n"] += 1
    rows = []
    for (suite, engine), a in sorted(agg.items()):
        rows.append([suite, engine, a["n"],
                     f"{a['total'] / 1000:.3f}", f"{a['execution'] / 1000:.3f}",
                     f"{a['explain'] / 1000:.3f}",
                     f"{100 * a['explain'] / a['total']:.2f}" if a["total"] else ""])
    write_csv(args.out_dir / f"{args.experiment}_workload_by_measure_SF{args.sf}.csv",
              ["suite", "engine", "n_queries",
               "sum_of_median_reported_total_s", "sum_of_median_execution_client_s",
               "sum_of_median_explain_client_s", "explain_share_of_reported_total_pct"], rows)

    # ratios under each measure
    by = {(s, e): a for (s, e), a in agg.items()}
    rr = []
    for engine in ("duckdb", "cedardb", "monetdb"):
        t, p = by.get(("tpcds", engine)), by.get(("prodds", engine))
        if not t or not p:
            continue
        rr.append([engine,
                   f"{p['total'] / t['total']:.4f}", f"{p['execution'] / t['execution']:.4f}",
                   f"{p['explain'] / t['explain']:.4f}"])
    write_csv(args.out_dir / f"{args.experiment}_ratio_by_measure_SF{args.sf}.csv",
              ["engine", "ratio_reported_total", "ratio_execution_client_only",
               "ratio_explain_client_only"], rr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
