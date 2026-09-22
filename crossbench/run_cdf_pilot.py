#!/usr/bin/env python3
"""Pilot: per-query runtime distributions across benchmark suites on one engine.

Background: reviewer R6 (W1/D1) asks whether Prod-DS exposes system behaviour that other
recent production-oriented benchmarks do not, and wants that shown experimentally rather
than only through workload statistics. This measures per-query runtime for several suites
on the same machine and the same engine, so their distributions can be compared directly.

PILOT PROTOCOL — not the final numbers.
  * one untimed warmup pass over the suite, then one timed pass (read-only suites);
  * the write suite gets a single timed pass and no warmup, because a warmup would apply
    its mutations twice;
  * the final figure is to use ten timed repetitions with the same warmup rule.

WHAT IS MEASURED: wall-clock from submitting the query to having its complete result
materialised in the client, i.e. execute() plus fetchall(), on a warm database. This is
end-to-end query latency as a client sees it. It includes result transfer and excludes
connection setup and query-file reading.

FAILURES ARE NOT DROPPED: every query is written to the CSV with a status of ok, error or
timeout, and the summary counts them separately from the successful runs.

Usage:
    .venv/bin/python run_cdf_pilot.py                 # every configured suite
    .venv/bin/python run_cdf_pilot.py --suites tpcds,prodds
    .venv/bin/python run_cdf_pilot.py --timeout 300 --out-dir results
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import platform
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Dict, List, Optional

import duckdb

HERE = Path(__file__).resolve().parent
REPO = Path(os.environ.get("PRODDS_ROOT", Path(__file__).resolve().parents[1]))
# Comparator sources and their loaded databases live outside the repository: they are
# other projects' data, not ours to redistribute. These are the locations used for the
# recorded run; override them to re-measure elsewhere.
BENCH = Path(os.environ.get("CROSSBENCH_SOURCES",
                            Path.home() / "Prod.DS.Revision/Benchmarks"))
S7DBS = Path(os.environ.get("CROSSBENCH_DBS",
                            Path.home() / "PROD-DS-Revision-Results/s7_cdf/dbs"))

THREADS = 56
MEMORY_LIMIT = "700GB"
DEFAULT_TIMEOUT_S = 300.0

# Each suite names the database it runs against and the query set, both pinned to a path so
# the run is reproducible. `kind` is read or write; write suites mutate the database, so they
# run against a scratch copy and get no warmup pass.
SUITES: Dict[str, Dict] = {
    "prodds": dict(
        db=REPO / ".reproduce/sf10/databases/duckdb/prodds_sf10_str5.duckdb",
        queries=REPO / ".reproduce/sf10/queries/duckdb/prodds",
        data=REPO / ".reproduce/sf10/data/prodds_sf10_str5",
        kind="read",
        note="Prod-DS current default at SF10: STR=5, NULL sparsity, MCV skew and key skew all on.",
    ),
    "tpcds": dict(
        db=REPO / ".reproduce/sf10/databases/duckdb/tpcds_sf10.duckdb",
        queries=REPO / ".reproduce/sf10/queries/duckdb/tpcds",
        data=REPO / ".reproduce/sf10/data/tpcds_sf10",
        kind="read",
        note="Vanilla TPC-DS SF10 from the same generator run as the Prod-DS database.",
    ),
    "dsb": dict(
        db=S7DBS / "sf10_dsb.duckdb",
        queries=BENCH / "dsb/queries",
        data=BENCH / "dsb/data",
        kind="read",
        note="DSB at its own SF10. Database built June 2026 by the S7 runner, reused unchanged.",
    ),
    "job": dict(
        db=S7DBS / "sf10_job.duckdb",
        queries=BENCH / "job/queries",
        data=BENCH / "job/data",
        kind="read",
        note="Join Order Benchmark on the full IMDb dataset. Not scale-factor driven.",
    ),
    "redbench": dict(
        db=S7DBS / "sf10_redbench.duckdb",
        queries=BENCH / "redbench/queries",
        data=BENCH / "redbench/data",
        kind="write",
        note=("RedBench: 1,000 single-row DELETE+INSERT statements over the IMDb database that "
              "JOB also uses (its data directory is a symlink to JOB's). This is a write "
              "workload, not an analytical one -- see the README before comparing its curve "
              "with the others."),
    ),
}


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def dir_bytes(path: Path) -> Dict[str, Optional[int]]:
    """Logical and on-disk size of a dataset.

    The measurement host uses a compressing filesystem, so the bytes an engine reads (the
    logical size, what st_size reports) and the bytes the dataset occupies (what du reports)
    differ by more than a factor of two. Both are recorded: the logical size is the one to
    compare suites by, the on-disk size explains the storage footprint.
    """
    logical = 0
    if path.is_dir():
        for p in path.rglob("*"):
            try:
                if p.is_file():
                    logical += p.stat().st_size
            except OSError:
                pass
    elif path.exists():
        logical = path.stat().st_size
    on_disk: Optional[int] = None
    try:
        out = subprocess.run(["du", "-sb", "--apparent-size", "--dereference", str(path)],
                             capture_output=True, text=True, timeout=300)
        du = subprocess.run(["du", "-sB1", "--dereference", str(path)],
                            capture_output=True, text=True, timeout=300)
        if du.returncode == 0:
            on_disk = int(du.stdout.split()[0])
        if out.returncode == 0 and not logical:
            logical = int(out.stdout.split()[0])
    except (OSError, ValueError, subprocess.SubprocessError):
        pass
    return {"logical": logical, "on_disk": on_disk}


def query_files(qdir: Path) -> List[Path]:
    return sorted(qdir.glob("*.sql"))


def sha(path: Path) -> str:
    h = hashlib.sha256()
    h.update(path.read_bytes())
    return h.hexdigest()[:16]


def run_suite(name: str, cfg: Dict, timeout_s: float, out_dir: Path) -> List[List]:
    qfiles = query_files(Path(cfg["queries"]))
    if not qfiles:
        log(f"{name}: no queries at {cfg['queries']} — skipped")
        return []
    db = Path(cfg["db"])
    if not db.exists():
        log(f"{name}: database missing at {db} — skipped")
        return []

    read_only = cfg["kind"] == "read"
    if not read_only:
        # A write suite mutates its database. Work on a scratch copy so the run is repeatable
        # and the original stays intact.
        scratch = out_dir / f"scratch_{name}.duckdb"
        if scratch.exists():
            scratch.unlink()
        log(f"{name}: copying {db.name} to a scratch database ({db.stat().st_size/2**30:.1f} GiB)")
        shutil.copy2(db, scratch)
        db = scratch

    con = duckdb.connect(str(db), read_only=read_only)
    con.execute(f"PRAGMA threads={THREADS}")
    try:
        con.execute(f"SET memory_limit='{MEMORY_LIMIT}'")
    except duckdb.Error:
        pass

    def timed(sql: str):
        """Run one query with a watchdog; return (seconds, status, rows, error)."""
        fired = threading.Event()
        timer = threading.Timer(timeout_s, lambda: (fired.set(), con.interrupt()))
        timer.start()
        t0 = time.perf_counter()
        try:
            rows = con.execute(sql).fetchall()
            dt = time.perf_counter() - t0
            return dt, "ok", len(rows), ""
        except Exception as exc:                                   # noqa: BLE001
            dt = time.perf_counter() - t0
            if fired.is_set():
                return dt, "timeout", 0, f"interrupted after {timeout_s:.0f}s"
            return dt, "error", 0, str(exc).replace("\n", " ")[:300]
        finally:
            timer.cancel()

    rows_out: List[List] = []
    if read_only:
        log(f"{name}: warmup pass over {len(qfiles)} queries (untimed)")
        for qf in qfiles:
            timed(qf.read_text(encoding="utf-8", errors="replace"))
    else:
        log(f"{name}: write suite — no warmup pass (a warmup would apply the mutations twice)")

    log(f"{name}: timed pass over {len(qfiles)} queries")
    t_suite = time.perf_counter()
    for qf in qfiles:
        sql = qf.read_text(encoding="utf-8", errors="replace")
        dt, status, nrows, err = timed(sql)
        rows_out.append([name, qf.stem, f"{dt*1000:.3f}", status, nrows, err])
        if status != "ok":
            log(f"  {qf.stem}: {status} — {err[:90]}")
    log(f"{name}: done in {time.perf_counter()-t_suite:.1f}s")
    con.close()
    if not read_only:
        db.unlink(missing_ok=True)
    return rows_out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--suites", default=",".join(SUITES),
                    help="comma-separated subset, default all")
    ap.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_S,
                    help="per-query cap in seconds (default 300)")
    ap.add_argument("--out-dir", type=Path, default=HERE / "results")
    args = ap.parse_args()

    wanted = [s.strip() for s in args.suites.split(",") if s.strip()]
    unknown = [s for s in wanted if s not in SUITES]
    if unknown:
        print(f"unknown suite(s): {', '.join(unknown)}", file=sys.stderr)
        return 2
    args.out_dir.mkdir(parents=True, exist_ok=True)

    log(f"DuckDB {duckdb.__version__}, threads={THREADS}, memory_limit={MEMORY_LIMIT}, "
        f"per-query cap {args.timeout:.0f}s")

    manifest = {
        "experiment": "cross-benchmark per-query runtime CDF (pilot)",
        "protocol": ("read suites: 1 untimed warmup pass then 1 timed pass; "
                     "write suites: 1 timed pass, no warmup"),
        "measured": ("wall clock from submitting the query to the complete result being "
                     "materialised in the client (execute + fetchall), warm database"),
        "repetitions": 1,
        "engine": f"duckdb {duckdb.__version__}",
        "threads": THREADS,
        "memory_limit": MEMORY_LIMIT,
        "per_query_timeout_s": args.timeout,
        "started": time.strftime("%Y-%m-%d %H:%M:%S %Z"),
        "host": platform.node(),
        "kernel": platform.release(),
        "cpu_count": os.cpu_count(),
        "python": platform.python_version(),
        "suites": {},
    }

    all_rows: List[List] = []
    for name in wanted:
        cfg = SUITES[name]
        qfiles = query_files(Path(cfg["queries"]))
        db = Path(cfg["db"])
        manifest["suites"][name] = {
            "kind": cfg["kind"],
            "note": cfg["note"],
            "database": str(db),
            "database_bytes": db.stat().st_size if db.exists() else None,
            "source_data": str(cfg["data"]),
            "source_data_bytes": dir_bytes(Path(cfg["data"])),
            "note_size": "logical = bytes the engine reads; on_disk = filesystem footprint (compressed)",
            "query_dir": str(cfg["queries"]),
            "n_queries": len(qfiles),
        }
        # Pin the exact query set: name and content hash of every file.
        qlist = args.out_dir / f"queries_{name}.txt"
        qlist.write_text("".join(f"{sha(q)}  {q.name}\n" for q in qfiles), encoding="utf-8")
        all_rows += run_suite(name, cfg, args.timeout, args.out_dir)

    manifest["finished"] = time.strftime("%Y-%m-%d %H:%M:%S %Z")
    out_csv = args.out_dir / "latencies_pilot.csv"
    with out_csv.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["suite", "query_id", "latency_ms", "status", "n_rows", "error"])
        w.writerows(all_rows)
    (args.out_dir / "manifest_pilot.json").write_text(
        json.dumps(manifest, indent=2), encoding="utf-8")
    log(f"wrote {out_csv} ({len(all_rows)} rows) and manifest_pilot.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
