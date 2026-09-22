#!/usr/bin/env python3
"""What the generated code buys, measured against the same query run without it.

A compilation cost is only interesting next to what it saves, so this runs a small sample of the
Prod-DS common subset three ways in one session per mode, on the same SF100 database:

  Interpreted     no machine code at all -- the bytecode VM executes the plan
  Auto (default)  what the campaign ran: a cheap PREPARE, then the engine decides during
                  execution whether the query is worth compiling
  Optimized       all code generated at PREPARE time, so EXECUTE is pure execution

PREPARE and EXECUTE are timed separately, which is the whole point: in `Optimized` the compilation
is in the first number and nowhere else, so `PREPARE + EXECUTE` decomposes the query without any
subtraction between modes.

**The selection rule was fixed in writing before anything was timed**, in the manner of
`docs/10-crossbench-benchmarks.md`. Of the 97 Prod-DS queries in the E1 common subset, rank by
CedarDB's own campaign median at SF100 (`data/E1_per_query_runtime_SF100.csv`) and take the
queries at the 10th, 30th, 50th, 70th and 90th percentile of that ranking, plus the slowest query
that still falls under the cap. The cap is 30 s of campaign median and exists for tractability:
interpreted execution runs several times slower, so including the 1,322 s tail query would mean
hours in the interpreted arm alone. The cap is a stated limit of this probe, not a finding -- it
means the sample says nothing about the amplification tail.
"""
from __future__ import annotations

import argparse
import csv
import json
import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from measure_cedardb_compilation import (  # noqa: E402
    MODE_NAMES, REPO, load_queries, run_session, start_server, stop_server)

# The campaign medians the selection rule ranks by, as published in this repository.
RUNTIME_CSV = REPO / "experiments/data/paper_csv/E1_per_query_runtime_SF100.csv"
PERCENTILES = [10, 30, 50, 70, 90]
CAP_S = 30.0


def select_queries(ids: set[str]) -> list[tuple[str, float]]:
    ranked = []
    for r in csv.DictReader(RUNTIME_CSV.open(encoding="utf-8")):
        if r["engine"] != "cedardb" or r["suite"] != "prodds":
            continue
        qid = r["query_name"].removesuffix(".sql")
        if qid in ids and float(r["median_s"]) <= CAP_S:
            ranked.append((qid, float(r["median_s"])))
    ranked.sort(key=lambda x: x[1])
    picked: list[tuple[str, float]] = []
    for p in PERCENTILES:
        cand = ranked[int(round(p / 100 * (len(ranked) - 1)))]
        if cand not in picked:
            picked.append(cand)
    if ranked[-1] not in picked:
        picked.append(ranked[-1])
    return picked


def build(mode: str, rep: int, queries: list[tuple[str, str]], timeout_ms: int):
    lines = ["\\timing on", "\\o /dev/null",
             f"SET statement_timeout = {timeout_ms};",
             f"SET debug.compilationmode = '{mode}';",
             f"SET debug.compilationmode.prepared = '{mode}';",
             "PREPARE warmup AS SELECT 1;"]
    labels: dict[int, tuple[str, str]] = {}

    def emit(qid: str, stage: str, stmt: str) -> None:
        lines.append(f"\\echo @@{qid}|{stage}")
        lines.append(stmt)
        labels[len(lines)] = (qid, stage)

    for qid, sql in queries:
        emit(qid, "prepare", f"PREPARE x_{qid}_r{rep} AS {sql};")
        emit(qid, "execute", f"EXECUTE x_{qid}_r{rep};")
    return "\n".join(lines) + "\n", labels


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--modes", default="i,A,o")
    ap.add_argument("--timeout-s", type=int, default=900)
    ap.add_argument("--work", default="/tmp")
    ap.add_argument("--out", default="cedardb_execution_payoff_SF100.csv")
    args = ap.parse_args()

    base = REPO / ".reproduce/sf100"
    subset = json.loads((base / "results_keyskew/common_subset.json").read_text())["suites"]
    ids = set(subset["prodds"]["common"])
    picked = select_queries(ids)
    print("selected by the pre-declared rule (campaign median, CedarDB, Prod-DS SF100):")
    for qid, s in picked:
        print(f"  {qid:22s} {s:8.2f} s")

    queries = load_queries(base / "queries/cedardb/prodds", [q for q, _ in picked])
    work = Path(args.work) / "cedar_payoff"
    work.mkdir(parents=True, exist_ok=True)
    rows = []
    proc = start_server(base / "databases/cedardb/prodds_sf100_str5", work / "server.log")
    try:
        for mode in args.modes.split(","):
            for rep in range(1, args.reps + 1):
                script, labels = build(mode, rep, queries, args.timeout_s * 1000)
                for qid, stage, ms, status in run_session("prodds_sf100_str5", script, labels, work):
                    rows.append(["prodds", "100", mode, MODE_NAMES.get(mode, mode), rep,
                                 qid, stage, f"{ms:.3f}", status])
                print(f"  mode {mode} rep {rep} done", flush=True)
    finally:
        stop_server(proc)

    with open(args.out, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["suite", "scale_factor", "mode", "mode_name", "repetition",
                    "query_id", "stage", "elapsed_ms", "status"])
        w.writerows(rows)

    med: dict[tuple, float] = {}
    for key in {(r[2], r[5], r[6]) for r in rows}:
        vals = [float(r[7]) for r in rows if (r[2], r[5], r[6]) == key and r[8] == "ok"]
        if vals:
            med[key] = statistics.median(vals)
    print(f"\n{len(rows)} measurements written to {args.out}\n")
    print(f"  {'query':22s} {'interp exec':>12s} {'default exec':>13s} "
          f"{'opt prepare':>12s} {'opt exec':>10s} {'interp/opt':>11s}")
    for qid, _ in picked:
        i_e, a_e = med.get(("i", qid, "execute")), med.get(("A", qid, "execute"))
        o_p, o_e = med.get(("o", qid, "prepare")), med.get(("o", qid, "execute"))
        ratio = f"{i_e / o_e:.1f}x" if i_e and o_e else "-"
        print(f"  {qid:22s} {i_e or 0:11.1f}m {a_e or 0:12.1f}m {o_p or 0:11.1f}m "
              f"{o_e or 0:9.1f}m {ratio:>11s}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
