#!/usr/bin/env python3
"""Cross-benchmark per-query runtime distributions on one engine — final protocol.

For the meta-review's MR2 and reviewer R6's W1/D1: whether Prod-DS exposes system behaviour
that other production-oriented benchmarks do not, shown by running them on the same machine
and the same engine and comparing their per-query runtime distributions.

WHICH INSTANCE OF EACH BENCHMARK, AND WHICH QUERIES: docs/10-crossbench-benchmarks.md in the
`queries/<suite>/_selection.json`. The selection rules there were fixed before this script ran.

PROTOCOL
  * one complete UNTIMED warmup pass over the suite, then ten TIMED sequential passes;
  * passes are pass-major: pass p runs every query once, in a fixed order, before pass p+1
    starts. Each pass is therefore a real workload pass, and a pass total is measured rather
    than assembled from separately timed queries;
  * one connection per suite, reused across passes; session settings are applied once,
    outside any timed query, and verified to have taken effect;
  * write suites are the exception: their mutations are not idempotent, so they get a single
    timed pass on a scratch copy of the database and no warmup. They are never part of the
    analytical CDF.

WHAT IS MEASURED: wall clock from submitting the query to having its complete result
materialised in the client, i.e. execute() plus fetchall(). It includes result transfer and
client-side materialisation; it excludes connection setup and reading the query file.

FAILURES ARE NOT DROPPED: every attempt, warmup included, is written to the CSV with a status
of ok, error or timeout, its elapsed time, and a pointer into the error log. A timed-out query
is a censored observation, never a successful 300-second query.

Usage:
    .venv/bin/python run_cdf_final.py                      # everything, 10 passes
    .venv/bin/python run_cdf_final.py --suites tpcds,dsb --passes 1
"""
from __future__ import annotations

import argparse, csv, hashlib, json, os, platform, shutil, socket, subprocess, sys, threading, time
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple

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
JOB_DB = S7DBS / "sf10_job.duckdb"

# `queries` is either a directory of .sql files or a path to a directory frozen by the
# selection step (which additionally carries _selection.json). `weights` names a selection
# file whose entries give each query an instance multiplicity.
SUITES: Dict[str, Dict] = {
    "prodds": dict(
        db=REPO / ".reproduce/sf10/databases/duckdb/prodds_sf10_str5.duckdb",
        queries=REPO / ".reproduce/sf10/queries/duckdb/prodds",
        data=REPO / ".reproduce/sf10/data/prodds_sf10_str5", kind="read",
        label="Prod-DS (default)",
        note="Prod-DS SF10 current default: STR=5, NULL sparsity, MCV value skew, key skew."),
    "tpcds": dict(
        db=REPO / ".reproduce/sf10/databases/duckdb/tpcds_sf10.duckdb",
        queries=REPO / ".reproduce/sf10/queries/duckdb/tpcds",
        data=REPO / ".reproduce/sf10/data/tpcds_sf10", kind="read", label="TPC-DS",
        note="Vanilla TPC-DS SF10 from the same generator run as the Prod-DS database."),
    "dsb": dict(
        db=S7DBS / "sf10_dsb.duckdb", queries=BENCH / "dsb/queries",
        data=BENCH / "dsb/data", kind="read", label="DSB",
        note="microsoft/DSB ec9a156, 52 templates x STREAMS=2 = 104 instances, SF10."),
    "job": dict(
        db=JOB_DB, queries=BENCH / "job/queries", data=BENCH / "job/data", kind="read",
        label="JOB", patched_dir=HERE / "queries/patched/job",
        note="Join Order Benchmark on the full IMDb dataset. Not scale-factor driven."),
    "redbench_krid": dict(
        db=JOB_DB, queries=HERE / "queries/redbench_krid", data=BENCH / "job/data",
        kind="read", label="Redbench (Krid et al.)",
        weights=HERE / "queries/redbench_krid/_selection.json",
        note="30 released workloads over IMDb (CEB+JOB); 8,784 instances, 763 distinct SQL."),
    "sqlbarber": dict(
        db=JOB_DB, queries=HERE / "queries/sqlbarber", data=BENCH / "job/data", kind="read",
        label="SQLBarber",
        note="1,000 queries selected from the released 7,524-query pool by the authors' "
             "target cost distribution; see docs/10."),
    "redbench_wehrstein_reads": dict(
        db=JOB_DB, queries=HERE / "queries/redbench_wehrstein_reads", data=BENCH / "job/data",
        kind="read", label="Redbench (Wehrstein et al.), reads only",
        note="Read-only projection: the 62 SELECT queries of a MIXED read/write workload."),
    "redbench_wehrstein_writes": dict(
        db=S7DBS / "sf10_redbench.duckdb", queries=HERE / "queries/redbench_wehrstein_writes",
        data=BENCH / "redbench/data", kind="write",
        label="Redbench (Wehrstein et al.), writes only",
        note="The 938 write statements of the same mixed workload. Single pass on a scratch "
             "copy; not part of the analytical CDF."),
}


def log(m: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def dir_bytes(path: Path) -> Dict[str, Optional[int]]:
    """Logical (st_size) and on-disk (du) size. ZFS compresses, so they differ by >2x."""
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
    on_disk = None
    try:
        du = subprocess.run(["du", "-sB1", "--dereference", str(path)],
                            capture_output=True, text=True, timeout=600)
        if du.returncode == 0:
            on_disk = int(du.stdout.split()[0])
    except (OSError, ValueError, subprocess.SubprocessError):
        pass
    return {"logical": logical, "on_disk": on_disk}


def query_set(cfg: Dict) -> List[Tuple[str, str, str]]:
    """(query_name, sql, sha256_16), in a fixed order that every pass reuses."""
    qdir = Path(cfg["queries"])
    patched = cfg.get("patched_dir")
    out = []
    for p in sorted(qdir.glob("*.sql")):
        src = p
        if patched and (Path(patched) / p.name).exists():
            src = Path(patched) / p.name
        sql = src.read_text(encoding="utf-8", errors="replace")
        out.append((p.stem, sql, hashlib.sha256(sql.encode()).hexdigest()[:16]))
    return out


def weights_for(cfg: Dict) -> Dict[str, int]:
    wf = cfg.get("weights")
    if not wf or not Path(wf).exists():
        return {}
    sel = json.loads(Path(wf).read_text(encoding="utf-8"))
    return {q["query_name"]: int(q.get("instances", 1)) for q in sel.get("queries", [])}


class Session:
    """One DuckDB connection per suite, with verified settings and recoverable state."""

    def __init__(self, db: Path, read_only: bool, errlog):
        self.db, self.read_only, self.errlog = db, read_only, errlog
        self.reconnects = 0
        self.con = None
        self._connect()

    def _connect(self):
        if self.con is not None:
            try:
                self.con.close()
            except Exception:                                          # noqa: BLE001
                pass
        self.con = duckdb.connect(str(self.db), read_only=self.read_only)
        self.con.execute(f"PRAGMA threads={THREADS}")
        self.con.execute(f"SET memory_limit='{MEMORY_LIMIT}'")
        self.settings = self.verify()

    def verify(self) -> Dict[str, str]:
        """Read the settings back. A silently ignored setting must not pass unnoticed."""
        got = {}
        for k in ("threads", "memory_limit"):
            got[k] = str(self.con.execute(f"SELECT current_setting('{k}')").fetchone()[0])
        if int(got["threads"]) != THREADS:
            raise SystemExit(f"threads did not take effect: asked {THREADS}, got {got['threads']}")
        log(f"    settings verified: threads={got['threads']} memory_limit={got['memory_limit']}")
        return got

    def healthy(self) -> bool:
        try:
            self.con.execute("SELECT 1").fetchall()
            return True
        except Exception:                                              # noqa: BLE001
            return False

    def recover(self, why: str):
        """After a timeout or error, make sure the session is usable again. The reconnect
        itself is never inside a query timer."""
        if self.healthy():
            return False
        self.reconnects += 1
        log(f"    session unusable after {why} — reconnecting (#{self.reconnects})")
        self.errlog.write(f"\n=== RECONNECT #{self.reconnects} after {why} ===\n")
        self._connect()
        return True


def run_suite(name: str, cfg: Dict, passes: int, timeout_s: float, out_dir: Path,
              errlog, sink=None) -> Tuple[List[List], Dict]:
    qs = query_set(cfg)
    if not qs:
        log(f"{name}: no queries at {cfg['queries']} — skipped")
        return [], {}
    db = Path(cfg["db"])
    if not db.exists():
        log(f"{name}: database missing at {db} — skipped")
        return [], {}

    read_only = cfg["kind"] == "read"
    scratch = None
    if not read_only:
        scratch = out_dir / f"scratch_{name}.duckdb"
        if scratch.exists():
            scratch.unlink()
        log(f"{name}: copying {db.name} to a scratch database ({db.stat().st_size / 2**30:.1f} GiB)")
        shutil.copy2(db, scratch)
        db = scratch

    sess = Session(db, read_only, errlog)
    rows: List[List] = []
    pass_totals: Dict[int, float] = {}

    def timed(sql: str) -> Tuple[float, str, int, str]:
        fired = threading.Event()
        timer = threading.Timer(timeout_s, lambda: (fired.set(), sess.con.interrupt()))
        timer.start()
        t0 = time.perf_counter()
        try:
            res = sess.con.execute(sql).fetchall()
            return time.perf_counter() - t0, "ok", len(res), ""
        except Exception as exc:                                       # noqa: BLE001
            dt = time.perf_counter() - t0
            if fired.is_set():
                return dt, "timeout", 0, f"interrupted after {timeout_s:.0f}s"
            return dt, "error", 0, str(exc).replace("\n", " ")[:400]
        finally:
            timer.cancel()

    def one_pass(pidx: int, timed_pass: bool):
        tag = "warmup (untimed)" if pidx == 0 else f"pass {pidx}/{passes}"
        mark = len(rows)
        log(f"{name}: {tag} over {len(qs)} queries")
        t_start = time.perf_counter()
        for seq, (qname, sql, h) in enumerate(qs, start=1):
            dt, status, nrows, err = timed(sql)
            rows.append([name, pidx, seq, qname, h, f"{dt * 1000:.3f}", status, nrows,
                         1 if (pidx > 0 and timed_pass) else 0, err[:200]])
            if status != "ok":
                errlog.write(f"\n--- {name} pass={pidx} seq={seq} query={qname} "
                             f"status={status} elapsed_ms={dt * 1000:.1f}\n{err}\n")
                errlog.flush()
                if pidx == 0 or seq % 1 == 0:
                    log(f"    {qname}: {status} — {err[:80]}")
                sess.recover(f"{status} on {qname}")
        el = time.perf_counter() - t_start
        if pidx > 0:
            pass_totals[pidx] = el
        if sink is not None:                  # flush this pass before starting the next
            sink(rows[mark:])
        log(f"{name}: {tag} done in {el:.1f}s")

    if read_only:
        one_pass(0, False)
        for p in range(1, passes + 1):
            one_pass(p, True)
    else:
        log(f"{name}: write suite — single timed pass, no warmup (mutations are not idempotent)")
        one_pass(1, True)

    info = {
        "kind": cfg["kind"], "label": cfg["label"], "note": cfg["note"],
        "database": str(Path(cfg["db"])), "database_bytes":
            Path(cfg["db"]).stat().st_size if Path(cfg["db"]).exists() else None,
        "source_data": str(cfg["data"]), "source_data_bytes": dir_bytes(Path(cfg["data"])),
        "note_size": "logical = bytes the engine reads; on_disk = filesystem footprint (compressed)",
        "query_dir": str(cfg["queries"]), "n_queries": len(qs),
        "passes_timed": passes if read_only else 1,
        "warmup_passes": 1 if read_only else 0,
        "instance_weights": bool(cfg.get("weights")),
        "total_instances": sum(weights_for(cfg).values()) or len(qs),
        "measured_pass_wall_s": {str(k): round(v, 3) for k, v in pass_totals.items()},
        "settings_verified": sess.settings,
        "reconnects": sess.reconnects,
    }
    try:
        sess.con.close()
    except Exception:                                                   # noqa: BLE001
        pass
    if scratch is not None:
        scratch.unlink(missing_ok=True)
    return rows, info


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--suites", default=",".join(SUITES))
    ap.add_argument("--passes", type=int, default=10)
    ap.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_S)
    ap.add_argument("--out-dir", type=Path, default=HERE / "results/final")
    args = ap.parse_args()

    wanted = [s.strip() for s in args.suites.split(",") if s.strip()]
    unknown = [s for s in wanted if s not in SUITES]
    if unknown:
        print(f"unknown suite(s): {', '.join(unknown)}", file=sys.stderr)
        return 2
    args.out_dir.mkdir(parents=True, exist_ok=True)

    log(f"DuckDB {duckdb.__version__}, threads={THREADS}, memory_limit={MEMORY_LIMIT}, "
        f"per-query cap {args.timeout:.0f}s, {args.passes} timed passes + 1 warmup pass")

    manifest = {
        "experiment": "cross-benchmark per-query runtime CDF (final)",
        "benchmark_identification": "docs/10-crossbench-benchmarks.md (fixed before this run)",
        "protocol": (f"read suites: 1 untimed warmup pass, then {args.passes} timed sequential "
                     "passes, pass-major (every query once per pass, fixed order); "
                     "write suites: 1 timed pass on a scratch copy, no warmup"),
        "measured": ("wall clock from submitting the query to the complete result being "
                     "materialised in the client (execute + fetchall)"),
        "aggregation": ("per query: median over the timed passes in which it succeeded; "
                        "the main CDF keeps only queries that succeeded in ALL timed passes"),
        "timed_passes": args.passes, "warmup_passes": 1,
        "engine": f"duckdb {duckdb.__version__}", "threads": THREADS,
        "memory_limit": MEMORY_LIMIT, "per_query_timeout_s": args.timeout,
        "timeout_semantics": "censored observation; never recorded as a successful run",
        "started": time.strftime("%Y-%m-%d %H:%M:%S %Z"),
        "host": socket.gethostname(), "kernel": platform.release(),
        "cpu_count": os.cpu_count(), "python": platform.python_version(),
        "suites": {},
    }

    all_rows: List[List] = []
    errpath = args.out_dir / "errors.log"
    out_csv = args.out_dir / "latencies.csv"
    csv_fh = out_csv.open("w", newline="", encoding="utf-8")
    wcsv = csv.writer(csv_fh)
    wcsv.writerow(["suite", "pass_id", "seq_in_pass", "query_name", "sql_sha256_16",
                   "latency_ms", "status", "n_rows", "is_timed", "error"])
    csv_fh.flush()

    def sink(batch):
        wcsv.writerows(batch)
        csv_fh.flush()
        os.fsync(csv_fh.fileno())

    with errpath.open("w", encoding="utf-8") as errlog:
        errlog.write(f"# cross-benchmark final run, started {manifest['started']}\n")
        for name in wanted:
            cfg = SUITES[name]
            qs = query_set(cfg)
            qlist = args.out_dir / f"queries_{name}.txt"
            w = weights_for(cfg)
            qlist.write_text("".join(
                f"{h}  {n}" + (f"  instances={w[n]}" if n in w else "") + "\n" for n, _, h in qs),
                encoding="utf-8")
            rows, info = run_suite(name, cfg, args.passes, args.timeout, args.out_dir,
                                   errlog, sink=sink)
            if info:
                manifest["suites"][name] = info
            all_rows += rows
            # refresh the manifest after every suite so a partial run is still described
            manifest["finished"] = time.strftime("%Y-%m-%d %H:%M:%S %Z") + " (in progress)"
            (args.out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2),
                                                        encoding="utf-8")

    csv_fh.close()
    manifest["finished"] = time.strftime("%Y-%m-%d %H:%M:%S %Z")
    (args.out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    log(f"wrote {out_csv} ({len(all_rows)} attempts), manifest.json and errors.log")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
