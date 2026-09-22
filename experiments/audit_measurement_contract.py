#!/usr/bin/env python3
"""Audit what the measurement harness actually measured, and export the evidence.

Answers three questions the revision needs settled before any number is quoted:

  1. What does a "run" mean?  The runner's loop is query-major: for each query it runs
     ``repetitions`` back-to-back executions (experiments/runner.py, _run_queries).
     ``run_index`` is a global counter over (query, repetition) pairs and
     ``repetition_index`` is the position within that per-query loop.  There is therefore
     no such thing as a complete sequential workload pass in these results, and a total
     formed by grouping equal repetition indices is a SYNTHETIC total, not a measured pass.

  2. What is inside a timer?  ``wall_time_ms_total = planning + execution`` where each of
     the two is a separate client process invocation (experiments/adapters/base.py,
     run_sql -> _execute -> experiments/process.py, run_command).  The timer therefore
     spans process spawn, client start, connection/database open, parse, optimise,
     execute, print the full result, and process exit -- twice per measured value, once
     for the ``EXPLAIN`` stage and once for the execution stage.

  3. Which queries are in which population?  E0 audits the full set, E1 measures only the
     common subset.  Query files are named by STREAMS position, not by TPC-DS template
     number; the mapping comes from the generator's _permutation.json.

Usage:
    python experiments/audit_measurement_contract.py --sf 100 --out-dir <handoff>
"""
from __future__ import annotations

import argparse
import csv
import os
import hashlib
import json
import re
import statistics
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

REPO = Path(__file__).resolve().parent.parent

# Runtime measure, author decision 17 September 2026: the execution client alone.
# RUNTIME_MEASURE=total restores the historical EXPLAIN-plus-execution totals.
_MEASURE = os.environ.get("RUNTIME_MEASURE", "execution").strip().lower()
RAW_TIME_FIELD = "wall_time_ms_execution" if _MEASURE == "execution" else "wall_time_ms_total"
MEASURE_LABEL = ("execution client only" if _MEASURE == "execution"
                 else "EXPLAIN client + execution client (historical)")
sys.path.insert(0, str(REPO))

from experiments.common_subset import analyse as cs_analyse  # noqa: E402

MICRO_RE = re.compile(r"query_(join_J|union_U)(\d+)")
POS_RE = re.compile(r"^query_(\d+)$")


def load_jsonl(path: Path) -> List[Dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def latest_raw(unit: Path) -> Optional[Path]:
    """Newest raw.jsonl under a result unit (<unit>/<stamp>/<engine>/<exp>/raw.jsonl)."""
    cands = sorted(unit.glob("*/*/*/raw.jsonl"))
    return cands[-1] if cands else None


def permutation(sf: str, suite: str) -> Dict[int, int]:
    """position -> TPC-DS template number, inverted from the generator's map."""
    for engine in ("duckdb", "cedardb", "monetdb"):
        for name in (f"{suite}_run", suite):
            p = REPO / f".reproduce/sf{sf}/queries/{engine}/{name}/_permutation.json"
            if p.exists():
                raw = json.loads(p.read_text(encoding="utf-8"))
                return {int(v): int(k) for k, v in raw.items()}
    return {}


def template_of(query_id: str, perm: Dict[int, int]) -> str:
    m = POS_RE.match(query_id)
    if m:
        t = perm.get(int(m.group(1)))
        return f"Q{t}" if t else ""
    m = MICRO_RE.match(query_id)
    if m:
        return f"micro-{m.group(1).rstrip('_')}{m.group(2)}"
    return ""


def kind_of(query_id: str) -> str:
    if query_id.startswith("query_join_"):
        return "join_amplification"
    if query_id.startswith("query_union_"):
        return "union_amplification"
    return "tpcds_template"


def sql_hash(sf: str, engine: str, suite: str, query_id: str) -> str:
    for name in (f"{suite}_run", suite):
        p = REPO / f".reproduce/sf{sf}/queries/{engine}/{name}/{query_id}.sql"
        if p.exists():
            return hashlib.sha256(p.read_bytes()).hexdigest()[:16]
    return ""


def write_csv(path: Path, header: List[str], rows: List[List[Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(header)
        w.writerows(rows)
    print(f"  wrote {path.name} ({len(rows)} rows)")


# ── 1.1  total workload runtime, both aggregations ────────────────────────────
def e1_totals(results: Path, sf: str, out: Path) -> None:
    e1 = results / "E1"
    if not e1.is_dir():
        print("  no E1 results — skipped")
        return
    cs = json.loads((results / "common_subset.json").read_text(encoding="utf-8"))
    rows: List[List[Any]] = []
    stat: Dict[str, Dict[str, float]] = {}
    for unit in sorted(p for p in e1.iterdir() if p.is_dir()):
        raw = latest_raw(unit)
        if raw is None:
            continue
        engine, suite = unit.name.split("_", 1)
        perq: Dict[str, Dict[int, float]] = {}
        for r in load_jsonl(raw):
            if r.get("status") == "success" and isinstance(r.get(RAW_TIME_FIELD), (int, float)):
                perq.setdefault(r["query_id"], {})[int(r["repetition_index"])] = float(r[RAW_TIME_FIELD])
        if not perq:
            continue
        qs = sorted(perq)
        reps = sorted({rp for q in qs for rp in perq[q]})
        # A "rep total" groups the r-th execution of every query. Those executions did not
        # happen contiguously: the loop is query-major, so this is a synthetic total.
        rep_tot = {rp: sum(perq[q][rp] for q in qs if rp in perq[q]) / 1000.0 for rp in reps}
        med_of_sums = statistics.median(rep_tot.values())
        sum_of_meds = sum(statistics.median(perq[q].values()) for q in qs) / 1000.0
        stat[f"{engine}_{suite}"] = {"med_of_sums": med_of_sums, "sum_of_meds": sum_of_meds}
        rows.append([
            suite, engine, len(qs), len(reps),
            *[f"{rep_tot.get(r, float('nan')):.3f}" for r in range(1, 11)],
            f"{med_of_sums:.3f}", f"{statistics.mean(rep_tot.values()):.3f}",
            f"{sum_of_meds:.3f}",
            f"{100.0 * (med_of_sums - sum_of_meds) / sum_of_meds:+.2f}",
            f"{rep_tot.get(1, float('nan')):.3f}", f"{statistics.median([rep_tot[r] for r in reps if r > 1]):.3f}",
            f"{100.0 * (rep_tot[1] / statistics.median([rep_tot[r] for r in reps if r > 1]) - 1):+.1f}" if len(reps) > 1 else "",
        ])
    hdr = (["suite", "engine", "n_queries", "n_repetitions"]
           + [f"synthetic_total_rep{i}_s" for i in range(1, 11)]
           + ["median_of_rep_totals_s", "mean_of_rep_totals_s", "sum_of_per_query_medians_s",
              "pct_diff_median_vs_sum", "rep1_total_s", "median_rep2_to_10_s", "rep1_penalty_pct"])
    write_csv(out / f"E1_total_workload_summary_SF{sf}.csv", hdr, rows)

    # ratios, both definitions, so the paper can quote either without recomputing
    rr: List[List[Any]] = []
    for engine in ("duckdb", "cedardb", "monetdb"):
        a, b = stat.get(f"{engine}_tpcds"), stat.get(f"{engine}_prodds")
        if not a or not b:
            continue
        rr.append([engine,
                   f"{b['med_of_sums'] / a['med_of_sums']:.3f}",
                   f"{b['sum_of_meds'] / a['sum_of_meds']:.3f}",
                   f"{a['med_of_sums']:.3f}", f"{b['med_of_sums']:.3f}",
                   f"{a['sum_of_meds']:.3f}", f"{b['sum_of_meds']:.3f}"])
    write_csv(out / f"E1_total_ratio_SF{sf}.csv",
              ["engine", "ratio_median_of_rep_totals", "ratio_sum_of_per_query_medians",
               "tpcds_median_of_rep_totals_s", "prodds_median_of_rep_totals_s",
               "tpcds_sum_of_per_query_medians_s", "prodds_sum_of_per_query_medians_s"], rr)


# ── 1.3  per-query audit and inventory ────────────────────────────────────────
def e0_audit(results: Path, sf: str, out: Path, ev: Path) -> None:
    rep = cs_analyse(results, None, 1800.0, 60.0, experiment="E0")
    rows: List[List[Any]] = []
    inv: List[List[Any]] = []
    for suite, srep in rep["suites"].items():
        perm = permutation(sf, suite)
        common = set(srep.get("common", []))
        for engine, info in sorted(srep["engines"].items()):
            med = info.get("median_s_over_success", {})
            for q in sorted(set(info["success_queries"]) | set(info["failed"].keys())):
                f = info["failed"].get(q)
                rows.append([
                    suite, engine, q, template_of(q, perm), kind_of(q),
                    sql_hash(sf, engine, suite, q),
                    "failed" if f else "success",
                    (f or {}).get("cause", ""), (f or {}).get("status", ""),
                    (f or {}).get("failed_runs", 0), (f or {}).get("runs", info.get("reps", "")),
                    f"{med[q]:.4f}" if q in med else "",
                    ((f or {}).get("message") or "").replace("\n", " ")[:400],
                    "yes" if q in common else "no",
                ])
            for q in sorted(set(info["success_queries"]) | set(info["failed"].keys())):
                reason = ""
                if q not in common:
                    why = []
                    for e2, i2 in srep["engines"].items():
                        if q in i2["failed"]:
                            why.append(f"{e2}: {i2['failed'][q]['cause']}")
                    reason = "; ".join(why) or "not run by every engine"
                inv.append(["E1", f"SF{sf}", suite, q, template_of(q, perm), kind_of(q),
                            "included" if q in common else "excluded", reason])
    write_csv(out / f"E0_audit_per_query_SF{sf}.csv",
              ["suite", "engine", "query_id", "template", "kind", "sql_sha256_16", "status",
               "cause", "runner_status", "failed_runs", "runs", "median_s", "message",
               "in_E1_common_subset"], rows)
    # de-duplicate inventory rows (one per suite/query, not per engine)
    seen, inv2 = set(), []
    for r in inv:
        k = (r[0], r[1], r[2], r[3])
        if k not in seen:
            seen.add(k)
            inv2.append(r)
    write_csv(out / f"_inventory_E1_SF{sf}.csv",
              ["experiment", "scale", "suite", "query_id", "template", "kind",
               "membership", "exclusion_reason"], inv2)
    # full original error text, one file per engine
    ev.mkdir(parents=True, exist_ok=True)
    for suite, srep in rep["suites"].items():
        for engine, info in srep["engines"].items():
            if not info["failed"]:
                continue
            perm = permutation(sf, suite)
            lines = [f"# {engine} / {suite} / SF{sf} — every failure, full recorded text", "",
                     f"run: {info['run_dir']}", ""]
            for q, why in info["failed"].items():
                lines += [f"## {q}  ({template_of(q, perm) or 'micro-query'})",
                          f"- cause class : {why['cause']}",
                          f"- runner status: {why['status']}  (error_type {why.get('error_type')})",
                          f"- failed runs : {why['failed_runs']} of {why['runs']}",
                          "", "```", (why.get("message") or "").strip(), "```", ""]
            (ev / f"{engine}_{suite}_SF{sf}_failures.md").write_text("\n".join(lines), encoding="utf-8")
    print(f"  wrote per-engine failure texts to {ev}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sf", default="100")
    ap.add_argument("--results-dir", type=Path, default=None)
    ap.add_argument("--out-dir", type=Path, default=Path("/home/jvs34/prodds-revision-handoff"))
    args = ap.parse_args()
    results = args.results_dir or REPO / f".reproduce/sf{args.sf}/results_keyskew"
    data, ev = args.out_dir / "data", args.out_dir / "evidence/engine-failures"
    print(f"[audit] SF{args.sf} from {results}")
    e1_totals(results, args.sf, data)
    e0_audit(results, args.sf, data, ev)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
