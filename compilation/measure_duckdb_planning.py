#!/usr/bin/env python3
"""A side measurement: what DuckDB's own profiler says its planning costs.

**This is not the answer to D1** — that is `measure_cedardb_compilation.py`, because CedarDB is the
engine that compiles. DuckDB does not compile at all: it interprets a vectorised physical plan.
This script exists because the same campaign field that mis-describes CedarDB's compilation also
supplies the paper's DuckDB planning claims, and those were left contradicted but unquantified in
the revision's own review as contradicted but unquantified. It costs minutes, so the number is
here rather than absent.

DuckDB reports the phases itself, in its profiler output, so nothing here is timed by a client:
`planner` (which already contains `planner_binding`), `cumulative_optimizer_timing` and
`physical_planner`. `profiling_mode='detailed'` is what exposes them; the default STANDARD mode
carries only latency and operator metrics.

Two passes are taken. `--explain-only` plans each query without running it; the default executes
them, which is what the campaign did. Reporting both is the guard: if they disagreed, neither could
be quoted.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import statistics
import subprocess
import time
from pathlib import Path

# The checkout this script sits in. `.reproduce/` under it holds the generated data and
# the engine binaries; neither is redistributed, so PRODDS_ROOT can point elsewhere.
REPO = Path(os.environ.get("PRODDS_ROOT", Path(__file__).resolve().parents[1]))
DUCKDB_BIN = REPO / ".reproduce/engines/duckdb/duckdb"
PROFILE_METRICS = ("PLANNER", "PLANNER_BINDING", "PHYSICAL_PLANNER",
                   "CUMULATIVE_OPTIMIZER_TIMING", "LATENCY", "QUERY_NAME")


def to_ms(value: str, unit: str) -> float:
    return float(value) * (1000.0 if unit == "s" else 1.0)


def load_queries(qdir: Path, ids: list[str]) -> list[tuple[str, str]]:
    out = []
    for qid in ids:
        text = (qdir / f"{qid}.sql").read_text(encoding="utf-8")
        stripped = "\n".join(re.sub(r"--.*$", "", ln) for ln in text.splitlines())
        parts = [p.strip() for p in stripped.split(";") if p.strip()]
        if len(parts) != 1:
            raise SystemExit(f"{qid}: expected one statement, found {len(parts)}")
        out.append((qid, parts[0]))
    return out


def run_duckdb(db: Path, queries, reps: int, work: Path, explain_only: bool) -> list[list]:
    """DuckDB reports the phases itself, so nothing here is timed by a client.

    `profiling_mode='detailed'` is what exposes them; the default STANDARD mode carries only
    latency and operator metrics. `profiling_coverage='ALL'` is needed for EXPLAIN statements.
    `planner` already contains `planner_binding`, so the pre-execution total is
    planner + optimizers + physical planner, and binding is carried separately as a component.
    """
    rows = []
    for rep in range(1, reps + 1):
        pdir = work / f"profiles_rep{rep}"
        pdir.mkdir(parents=True, exist_ok=True)
        lines = ["SET profiling_mode='detailed';", "SET profiling_coverage='ALL';",
                 "SET enable_profiling='json';",
                 # untimed warm-up: the first statement in a session pays one-time setup
                 f"SET profiling_output='{pdir / '__warmup__.json'}';", "SELECT 1;"]
        for qid, sql in queries:
            lines.append(f"SET profiling_output='{pdir / (qid + '.json')}';")
            lines.append(f"EXPLAIN {sql};" if explain_only else f"{sql};")
        script = work / f"duckdb_rep{rep}.sql"
        script.write_text("\n".join(lines) + "\n", encoding="utf-8")
        p = subprocess.run([str(DUCKDB_BIN), str(db), "-f", str(script)],
                           capture_output=True, text=True)
        if p.returncode != 0:
            raise SystemExit(f"duckdb failed: {p.stderr[:400]}")
        for qid, _ in queries:
            f = pdir / f"{qid}.json"
            if not f.exists():
                rows.append(["duckdb", qid, rep, "", "", "", "", "missing_profile"])
                continue
            prof = json.loads(f.read_text(encoding="utf-8"))
            g = lambda k: float(prof.get(k) or 0.0) * 1000.0  # DuckDB reports seconds
            rows.append(["duckdb", qid, rep, f"{g('planner'):.4f}",
                         f"{g('planner_binding'):.4f}",
                         f"{g('cumulative_optimizer_timing'):.4f}",
                         f"{g('physical_planner'):.4f}", "ok"])
        print(f"  rep {rep} done", flush=True)
    return rows


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--suite", default="prodds")
    ap.add_argument("--scale", default="100")
    ap.add_argument("--reps", type=int, default=5)
    ap.add_argument("--work", default="/tmp")
    ap.add_argument("--out", default=None)
    ap.add_argument("--explain-only", action="store_true",
                    help="DuckDB: plan the queries without executing them")
    args = ap.parse_args()

    base = REPO / f".reproduce/sf{args.scale}"
    subset = json.loads((base / "results_keyskew/common_subset.json").read_text())["suites"]
    ids = sorted(subset[args.suite]["common"])
    queries = load_queries(base / "queries/duckdb" / args.suite, ids)
    work = Path(args.work) / "planning_duckdb"
    work.mkdir(parents=True, exist_ok=True)
    label = (f"prodds_sf{args.scale}_str5" if args.suite == "prodds"
             else f"tpcds_sf{args.scale}")
    print(f"[duckdb/{args.suite}] SF{args.scale}: {len(queries)} queries", flush=True)

    t0 = time.monotonic()
    rows = run_duckdb(base / "databases/duckdb" / f"{label}.duckdb",
                      queries, args.reps, work, args.explain_only)
    header = ["engine", "query_id", "repetition", "planner_ms", "planner_binding_ms",
              "optimizer_ms", "physical_planner_ms", "status"]
    print(f"  {time.monotonic() - t0:.1f} s", flush=True)

    out = Path(args.out or f"duckdb_planning_{args.suite}_SF{args.scale}.csv")
    with out.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(header)
        w.writerows(rows)
    per = {}
    for r in rows:
        if r[-1] != "ok":
            continue
        # r[4] is planner_binding, already inside r[3]; it is a component, not a term
        total = sum(float(x) for i, x in enumerate(r[3:7], start=3) if x and i != 4)
        per.setdefault(r[1], []).append(total)
    meds = sorted(statistics.median(v) for v in per.values())
    print(f"{len(rows)} rows -> {out}")
    print(f"  engine-side pre-execution time, median over {len(meds)} queries: "
          f"{statistics.median(meds):.2f} ms  (min {meds[0]:.2f}, max {meds[-1]:.2f})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
