#!/usr/bin/env python3
"""Export the paper's per-figure/table CSV bundle (experiments/data/paper_csv/) from a results tree.

Reads ``.reproduce/sf<N>/results[_<tag>]/E*/...`` (raw.jsonl, summary.csv, manifest.json) and
writes, for the scale factor of that tree, the files REPRODUCIBILITY.md lists:

  E1_total_workload_runtime_SF<N>.csv    suite,engine,run_idx,n_queries,total_time_s
  E1_per_query_runtime_SF<N>.csv         suite,engine,query_name,median_s,n_success
  E1_compilation_time_SF<N>.csv         suite,engine,n_queries,median/mean/p90/max_planning_ms,
                                         sum_planning_s,planning_share_of_runtime_pct  (reviewer D1)
  E1_error_breakdown_SF<N>.csv           engine,success,dialect,failure,oom,timeout,total   (Prod-DS)
  E1_error_per_query_SF<N>.csv           engine,query,category,cause                        (Prod-DS)
  E2_join_scaling_SF<N>.csv              engine,join_level,median_planning_ms,... ,n_success,n_fail,attempted
  E3_union_fanin_SF<N>.csv               engine,union_level,median_planning_ms,... ,n_success,n_fail,attempted
                                         `attempted=no` marks a level the runner skipped after a
                                         lower one failed; those rows have n_fail=0, not 1.
  E4_stringification_sweep_SF<N>_duckdb.csv   str_level,query_id,median_ms,runs_success
  E5_per_query_SF<N>.csv                 variant,tier,engine,query_id,median_ms,n_success
  E5_sparsity_skew_SF<N>.csv             variant,tier,engine,delta_pct,se_pct,n_common
  run_provenance_SF<N>.csv               one row per run: experiment, unit, timestamp, engine version,
                                         harness git hash, host, threads/reps/timeout/warmup

E1 totals and per-query medians honour ``<results-dir>/common_subset.json`` (written by
experiments/common_subset.py) when present: the paper reports E1 runtimes over the common success
subset. Only files whose experiment has results are written; existing files are overwritten.

Usage:
    python experiments/export_paper_csv.py --results-dir .reproduce/sf100/results_keyskew \
        [--out-dir experiments/data/paper_csv] [--timeout-s 1800]
"""
from __future__ import annotations

import argparse
import os
import csv
import json
import re
import statistics
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

REPO = Path(__file__).resolve().parents[1]

# ── Runtime measure (author decision, 17 September 2026) ─────────────────────
# Every measured value is TWO client processes: one running `EXPLAIN <query>` and one running the
# query. `wall_time_ms_total` is their sum, which is what every export used until now.
#
# The approved measure for E1-E5 is the EXECUTION CLIENT ALONE. Client startup, connection or
# database opening, parse, optimisation, execution and output handling inside that client all
# remain included -- this is NOT engine-internal execution time. The separately executed EXPLAIN
# client is excluded.
#
# RUNTIME_MEASURE=total restores the historical EXPLAIN-plus-execution exports.
_MEASURE = os.environ.get("RUNTIME_MEASURE", "execution").strip().lower()
if _MEASURE not in ("execution", "total"):
    raise SystemExit(f"RUNTIME_MEASURE must be 'execution' or 'total', got {_MEASURE!r}")
RAW_TIME_FIELD = "wall_time_ms_execution" if _MEASURE == "execution" else "wall_time_ms_total"
SUMMARY_TIME_COL = "median_execution_ms" if _MEASURE == "execution" else "median_ms"
MEASURE_LABEL = ("execution client only" if _MEASURE == "execution"
                 else "EXPLAIN client + execution client (historical)")
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))
from experiments.common_subset import analyse as cs_analyse, query_sort_key  # noqa: E402

ENGINE_ORDER = ["duckdb", "cedardb", "monetdb", "postgres"]
CAUSE_TO_CATEGORY = {
    "syntax / dialect": "dialect",
    "unsupported SQL feature": "dialect",
    "out of memory": "oom",
    "timeout": "timeout",
    "engine crash": "failure",
    "engine resource limit": "failure",
    "other error": "failure",
}


# ── helpers ──────────────────────────────────────────────────────────────────
def load_jsonl(path: Path) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                try:
                    out.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    return out


def latest_run(unit_dir: Path) -> Optional[Tuple[Path, str]]:
    """(workload_compare dir, engine) of the latest timestamped run under an E*/<unit>/ dir."""
    cands = sorted(unit_dir.glob("*/*/workload_compare/raw.jsonl"))
    if not cands:
        return None
    wc = cands[-1].parent
    return wc, wc.parent.name


def engine_sort(e: str) -> Tuple[int, str]:
    return (ENGINE_ORDER.index(e) if e in ENGINE_ORDER else len(ENGINE_ORDER), e)


def per_query_times(records: Iterable[Dict[str, Any]], suite: Optional[str] = None) -> Dict[str, Dict[str, Any]]:
    acc: Dict[str, Dict[str, Any]] = {}
    for r in records:
        if suite and r.get("suite") not in (None, suite):
            continue
        q = r.get("query_id")
        if not q:
            continue
        a = acc.setdefault(q, {"ms": [], "runs": 0, "per_rep": {}})
        a["runs"] += 1
        if r.get("status") == "success" and isinstance(r.get(RAW_TIME_FIELD), (int, float)):
            a["ms"].append(float(r[RAW_TIME_FIELD]))
            a["per_rep"][int(r.get("repetition_index") or len(a["per_rep"]) + 1)] = float(r[RAW_TIME_FIELD])
    return acc


def write_csv(path: Path, header: List[str], rows: List[List[Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(header)
        w.writerows(rows)
    print(f"  wrote {path} ({len(rows)} rows)")


def level_of(rec: Dict[str, Any], field: str, pattern: str) -> Optional[int]:
    v = rec.get(field)
    if isinstance(v, int):
        return v
    m = re.search(pattern, str(rec.get("query_id") or ""))
    return int(m.group(1)) if m else None


def ladder_rows(exp_dir: Path, field: str, pattern: str) -> List[List[Any]]:
    rows: List[List[Any]] = []
    for unit in sorted(p for p in exp_dir.iterdir() if p.is_dir()):
        run = latest_run(unit)
        if not run:
            continue
        wc, engine = run
        # `skipped` must be kept apart from `fail`: once a level fails, ladder_abandon writes a
        # placeholder record at every higher level with status `skipped_ladder_abandon`. Counting
        # those as failures made the exported CSVs report n_fail=1 at levels never attempted.
        by: Dict[int, Dict[str, List[float]]] = defaultdict(
            lambda: {"plan": [], "exec": [], "total": [], "fail": [], "skipped": []})
        for r in load_jsonl(wc / "raw.jsonl"):
            lvl = level_of(r, field, pattern)
            if lvl is None:
                continue
            if r.get("status") == "success":
                for key, fld in (("plan", "wall_time_ms_planning"), ("exec", "wall_time_ms_execution"), ("total", "wall_time_ms_total")):
                    v = r.get(fld)
                    if isinstance(v, (int, float)):
                        by[lvl][key].append(float(v))
            elif "skipped" in str(r.get("status") or ""):
                by[lvl]["skipped"].append(1.0)
            else:
                by[lvl]["fail"].append(1.0)
        for lvl in sorted(by):
            t = by[lvl]["total"]
            rows.append([
                engine, lvl,
                f"{statistics.median(by[lvl]['plan']):.3f}" if by[lvl]["plan"] else "",
                f"{statistics.median(by[lvl]['exec']):.3f}" if by[lvl]["exec"] else "",
                f"{statistics.median(t):.3f}" if t else "",
                f"{statistics.mean(t):.3f}" if t else "",
                f"{(statistics.stdev(t) if len(t) > 1 else 0.0):.3f}" if t else "",
                len(t), len(by[lvl]["fail"]),
                "no" if (by[lvl]["skipped"] and not t and not by[lvl]["fail"]) else "yes",
            ])
    rows.sort(key=lambda r: (engine_sort(r[0]), r[1]))
    return rows


def bootstrap_delta(base: Dict[str, float], var: Dict[str, float], n_boot: int = 2000, seed: int = 0):
    """(median %, bootstrap-SE %, n) of per-query relative delta over the common success set."""
    import random
    common = sorted(q for q in base if q in var and base[q] > 0)
    if not common:
        return None
    deltas = [(var[q] - base[q]) / base[q] * 100.0 for q in common]
    med = statistics.median(deltas)
    rng = random.Random(seed)
    meds = []
    for _ in range(n_boot):
        sample = [deltas[rng.randrange(len(deltas))] for _ in deltas]
        meds.append(statistics.median(sample))
    se = statistics.pstdev(meds) if len(meds) > 1 else 0.0
    return med, se, len(common)


# ── exporters ────────────────────────────────────────────────────────────────
def export_e1(results_dir: Path, out: Path, sf: str, timeout_s: float, margin_s: float) -> None:
    e1 = results_dir / "E1"
    if not e1.is_dir():
        return
    cs_path = results_dir / "common_subset.json"
    common: Dict[str, Optional[set]] = {}
    if cs_path.exists():
        spec = json.loads(cs_path.read_text(encoding="utf-8"))
        for suite, s in (spec.get("suites") or {}).items():
            common[suite] = set(s.get("common") or [])
        print(f"  E1: common subset from {cs_path.name}: " + ", ".join(f"{k}={len(v)}" for k, v in common.items()))
    totals: List[List[Any]] = []
    perq: List[List[Any]] = []
    for unit in sorted(p for p in e1.iterdir() if p.is_dir()):
        run = latest_run(unit)
        if not run:
            continue
        wc, engine = run
        suite = unit.name[len(engine) + 1:] if unit.name.startswith(engine + "_") else unit.name.rsplit("_", 1)[-1]
        facts = per_query_times(load_jsonl(wc / "raw.jsonl"), suite)
        keep = common.get(suite)
        if keep is None:
            keep = {q for q, f in facts.items() if f["ms"] and len(f["ms"]) == f["runs"]}
        qs = sorted((q for q in facts if q in keep), key=query_sort_key)
        for q in qs:
            f = facts[q]
            if f["ms"]:
                perq.append([suite, engine, f"{q}.sql", f"{statistics.median(f['ms']) / 1000.0:.6f}", len(f["ms"])])
        reps = sorted({rep for q in qs for rep in facts[q]["per_rep"]})
        for rep in reps:
            vals = [facts[q]["per_rep"][rep] for q in qs if rep in facts[q]["per_rep"]]
            totals.append([suite, engine, rep, len(vals), f"{sum(vals) / 1000.0:.6f}"])
    totals.sort(key=lambda r: (r[0], engine_sort(r[1]), r[2]))
    perq.sort(key=lambda r: (r[0], engine_sort(r[1]), query_sort_key(r[2][:-4])))
    write_csv(out / f"E1_total_workload_runtime_SF{sf}.csv", ["suite", "engine", "run_idx", "n_queries", "total_time_s"], totals)
    write_csv(out / f"E1_per_query_runtime_SF{sf}.csv", ["suite", "engine", "query_name", "median_s", "n_success"], perq)

    # Error breakdown (Prod-DS), attributed by experiments/common_subset.py. With the
    # audit-first protocol the failures live in the E0 audit (E1 runs only the common
    # subset), so E0 is the source whenever it exists.
    exp = "E0" if (results_dir / "E0").is_dir() else "E1"
    try:
        rep = cs_analyse(results_dir, None, timeout_s, margin_s, experiment=exp)
    except SystemExit:
        return
    suite_rep = (rep.get("suites") or {}).get("prodds")
    if not suite_rep:
        return
    breakdown: List[List[Any]] = []
    per_query_err: List[List[Any]] = []
    for engine in sorted(suite_rep["engines"], key=engine_sort):
        info = suite_rep["engines"][engine]
        counts = {"dialect": 0, "failure": 0, "oom": 0, "timeout": 0}
        for q, why in info["failed"].items():
            cat = CAUSE_TO_CATEGORY.get(why["cause"], "failure")
            counts[cat] += 1
            msg = (why.get("message") or why.get("error_type") or why["cause"]).replace("\n", " ").strip()
            per_query_err.append([engine, q, cat.upper(), msg[:200]])
        breakdown.append([engine, info["success"], counts["dialect"], counts["failure"], counts["oom"], counts["timeout"], info["queries"]])
    write_csv(out / f"E1_error_breakdown_SF{sf}.csv", ["engine", "success", "dialect", "failure", "oom", "timeout", "total"], breakdown)
    write_csv(out / f"E1_error_per_query_SF{sf}.csv", ["engine", "query", "category", "cause"], per_query_err)


def export_e1_compilation(results_dir: Path, out: Path, sf: str) -> None:
    """Per-engine compilation (planning) time on the E1 workloads.

    Reviewer D1 of the July 2026 round asked for compilation times, since CedarDB/Umbra
    compile queries. The runner already separates planning from execution for every query
    and every engine, so this is a view on the E1 runs, not a new measurement. Restricted
    to the common subset when one is present, so the engines are compared on one query set.
    """
    e1 = results_dir / "E1"
    if not e1.is_dir():
        return
    cs_path = results_dir / "common_subset.json"
    common: Dict[str, Optional[set]] = {}
    if cs_path.exists():
        spec = json.loads(cs_path.read_text(encoding="utf-8"))
        for suite, sp in (spec.get("suites") or {}).items():
            common[suite] = set(sp.get("common") or [])
    rows: List[List[Any]] = []
    for unit in sorted(p for p in e1.iterdir() if p.is_dir()):
        run = latest_run(unit)
        if not run:
            continue
        wc, engine = run
        suite = unit.name[len(engine) + 1:] if unit.name.startswith(engine + "_") else unit.name.rsplit("_", 1)[-1]
        summary = wc / "summary.csv"
        if not summary.exists():
            continue
        keep = common.get(suite)
        plan: List[float] = []
        total: List[float] = []
        with summary.open(newline="", encoding="utf-8") as fh:
            for r in csv.DictReader(fh):
                qid = (r.get("query_id") or "").removesuffix(".sql")
                if keep is not None and qid not in keep:
                    continue
                try:
                    pm = float(r["median_planning_ms"])
                    tm = float(r[SUMMARY_TIME_COL])
                except (TypeError, ValueError, KeyError):
                    continue
                plan.append(pm)
                total.append(tm)
        if not plan:
            continue
        plan_sorted = sorted(plan)
        p90 = plan_sorted[int(0.9 * (len(plan_sorted) - 1))]
        share = 100.0 * sum(plan) / sum(total) if sum(total) else 0.0
        rows.append([
            suite, engine, len(plan),
            f"{statistics.median(plan):.1f}",
            f"{statistics.fmean(plan):.1f}",
            f"{p90:.1f}",
            f"{max(plan):.1f}",
            f"{sum(plan) / 1000.0:.2f}",
            f"{share:.2f}",
        ])
    if not rows:
        return
    rows.sort(key=lambda r: (r[0], engine_sort(r[1])))
    write_csv(out / f"E1_compilation_time_SF{sf}.csv",
              ["suite", "engine", "n_queries", "median_planning_ms", "mean_planning_ms",
               "p90_planning_ms", "max_planning_ms", "sum_planning_s", "planning_share_of_runtime_pct"],
              rows)


def export_ladders(results_dir: Path, out: Path, sf: str) -> None:
    hdr = ["median_planning_ms", "median_execution_ms", "median_total_ms", "mean_total_ms",
           "stddev_total_ms", "n_success", "n_fail", "attempted"]
    if (results_dir / "E2").is_dir():
        rows = ladder_rows(results_dir / "E2", "join_count", r"_J(\d+)$")
        if rows:
            write_csv(out / f"E2_join_scaling_SF{sf}.csv", ["engine", "join_level"] + hdr, rows)
    if (results_dir / "E3").is_dir():
        rows = ladder_rows(results_dir / "E3", "union_count", r"_U(\d+)$")
        if rows:
            write_csv(out / f"E3_union_fanin_SF{sf}.csv", ["engine", "union_level"] + hdr, rows)


def export_e4(results_dir: Path, out: Path, sf: str) -> None:
    e4 = results_dir / "E4"
    if not e4.is_dir():
        return
    rows: List[List[Any]] = []
    engines = set()

    def level_key(name: str) -> Tuple[int, int]:
        m = re.match(r"str(\d+)(?:_len(\d+))?$", name)
        return (int(m.group(1)), int(m.group(2) or 0)) if m else (999, 0)

    for unit in sorted((p for p in e4.iterdir() if p.is_dir()), key=lambda p: level_key(p.name)):
        run = latest_run(unit)
        if not run:
            continue
        wc, engine = run
        engines.add(engine)
        facts = per_query_times(load_jsonl(wc / "raw.jsonl"))
        for q in sorted(facts, key=query_sort_key):
            f = facts[q]
            if f["ms"]:
                rows.append([unit.name, q, f"{statistics.median(f['ms']):.1f}", len(f["ms"])])
    if rows:
        tag = "_".join(sorted(engines)) or "duckdb"
        write_csv(out / f"E4_stringification_sweep_SF{sf}_{tag}.csv", ["str_level", "query_id", "median_ms", "runs_success"], rows)


def export_e5(results_dir: Path, out: Path, sf: str) -> None:
    tiers = [("low", results_dir / "E5_low"), ("medium", results_dir / "E5"), ("high", results_dir / "E5_high")]
    perq: List[List[Any]] = []
    deltas: List[List[Any]] = []
    for tier, d in tiers:
        if not d.is_dir():
            continue
        maps: Dict[Tuple[str, str], Dict[str, float]] = {}
        for unit in sorted(p for p in d.iterdir() if p.is_dir()):
            run = latest_run(unit)
            if not run:
                continue
            wc, engine = run
            variant = unit.name[: -(len(engine) + 1)] if unit.name.endswith("_" + engine) else unit.name
            facts = per_query_times(load_jsonl(wc / "raw.jsonl"))
            med = {q: statistics.median(f["ms"]) for q, f in facts.items() if f["ms"] and len(f["ms"]) == f["runs"]}
            maps[(variant, engine)] = med
            for q in sorted(facts, key=query_sort_key):
                f = facts[q]
                if f["ms"]:
                    perq.append([variant, tier, engine, q, f"{statistics.median(f['ms']):.1f}", len(f["ms"])])
        engines = sorted({e for (_v, e) in maps}, key=engine_sort)
        variants = [v for v in ["sparsity_only", "skew_only", "keyskew_only", "skew_all", "combined", "full"]
                    if any((v, e) in maps for e in engines)]
        for v in variants:
            for e in engines:
                base = maps.get(("baseline", e))
                var = maps.get((v, e))
                if not base or not var:
                    continue
                res = bootstrap_delta(base, var)
                if res:
                    med, se, n = res
                    deltas.append([v, tier, e, f"{med:+.1f}", f"{se:.1f}", n])
    if perq:
        write_csv(out / f"E5_per_query_SF{sf}.csv", ["variant", "tier", "engine", "query_id", "median_ms", "n_success"], perq)
    if deltas:
        write_csv(out / f"E5_sparsity_skew_SF{sf}.csv", ["variant", "tier", "engine", "delta_pct", "se_pct", "n_common"], deltas)


def export_provenance(results_dir: Path, out: Path, sf: str) -> None:
    rows: List[List[Any]] = []
    for m in sorted(results_dir.glob("E*/*/*/manifest.json")):
        try:
            d = json.loads(m.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        rel = m.relative_to(results_dir).parts  # E1 / unit / ts / manifest.json
        g = (d.get("config") or {}).get("global") or {}
        host = d.get("host") or {}
        systems = d.get("systems") or {}
        rows.append([
            rel[0], rel[1], d.get("timestamp") or rel[2],
            "; ".join(f"{k}={v}" for k, v in systems.items()),
            (d.get("git_hashes") or {}).get("harness"),
            g.get("threads"), g.get("repetitions"), g.get("timeout_seconds_execution"), g.get("warmup_queries"),
            g.get("memory_limit_bytes"),
            host.get("cpu_model"), host.get("cpu_count_physical"), host.get("memory_total_bytes"),
            host.get("os_release"), host.get("python"),
        ])
    if rows:
        write_csv(out / f"run_provenance_SF{sf}.csv",
                  ["experiment", "unit", "timestamp", "engine_version", "harness_git_hash", "threads", "repetitions",
                   "timeout_s", "warmup_queries", "memory_limit_bytes", "cpu_model", "cpu_physical", "memory_total_bytes",
                   "os_release", "python"], rows)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--results-dir", required=True, type=Path)
    ap.add_argument("--out-dir", type=Path, default=REPO / "experiments" / "data" / "paper_csv")
    ap.add_argument("--timeout-s", type=float, default=1800.0)
    ap.add_argument("--margin-s", type=float, default=60.0)
    args = ap.parse_args()
    results_dir = args.results_dir.resolve()
    m = re.search(r"sf(\d+)", str(results_dir))
    sf = m.group(1) if m else "NA"
    if not results_dir.is_dir():
        sys.exit(f"results dir not found: {results_dir}")
    print(f"[export] {results_dir} -> {args.out_dir} (SF{sf})")
    export_e1(results_dir, args.out_dir, sf, args.timeout_s, args.margin_s)
    export_e1_compilation(results_dir, args.out_dir, sf)
    export_ladders(results_dir, args.out_dir, sf)
    export_e4(results_dir, args.out_dir, sf)
    export_e5(results_dir, args.out_dir, sf)
    export_provenance(results_dir, args.out_dir, sf)


if __name__ == "__main__":
    main()
