#!/usr/bin/env python3
"""Engine-side compilation time for CedarDB, measured inside one session.

Reviewer R2's D1 asked for compilation times, since CedarDB and Umbra compile queries. The
campaign cannot answer it. Its `wall_time_ms_planning` is the wall time of a whole `psql` process
running `EXPLAIN` -- about 32 of CedarDB's 36 ms are the client starting up and connecting -- and
whatever code generation the engine does happens in the execution stage, which that field excludes.
measured directly on the measurement host; see README.md beside this file.

CedarDB exposes its compilation mode as a session setting, and naming an illegal value makes the
server enumerate the legal ones:

    SET debug.compilationmode = 'zzinvalid';
    ERROR:  invalid value for setting "compilationmode": "zzinvalid".
    Available values for The default compilation mode:
      Auto:'A', Interpreted:'i', C:'C', DirectEmit:'d', Adaptive:'a', Cheap:'c', Optimized:'o'

That is the Umbra compilation ladder. `Interpreted` executes in the bytecode VM and generates no
machine code; the others generate it by increasingly expensive routes. `PREPARE` is where the code
generation happens, which the mode sensitivity establishes directly -- on `query_77` at SF100 the
same `PREPARE` costs 2.6 ms interpreted, 4.8 ms DirectEmit, 39 ms Cheap, 156 ms Optimized and
1,073 ms through a real C compiler -- and `EXECUTE` then carries none of it.

So, per query, in one session, with no process start anywhere inside the measured interval:

    compilation(mode) = PREPARE(mode) - PREPARE(Interpreted)

Both terms contain parse, binding and optimisation, and the difference is the **estimated
compilation cost under forced optimized compilation** -- not a measurement of pure code
generation, since what else differs between the two modes is not established here. The subtrahend
is measured per query, not taken as a global constant.

**The caveat that must travel with every number produced here.** The campaign ran the *default*
mode, `Auto`. Its `PREPARE` is cheap -- 4.3 ms on `query_77` against 156 ms for `Optimized` --
because it defers the decision and then compiles *during* execution if the query turns out to be
worth compiling. Measured end to end on `query_77` at SF100: `Auto` executes in 4.2 s and
`Interpreted` in 21.6 s, so `Auto` plainly does compile, just not at prepare time. These forced-mode
numbers therefore say **what compilation costs**, not where the campaign's recorded runtime went.

Timing is psql's own `\timing`, i.e. one client-server round trip over the unix socket in a session
that is already open. `PREPARE ... AS SELECT 1` is measured in every session as the floor for that
round trip; it runs around 0.1-0.3 ms, three orders below the optimized-mode compilations, and the
summary reports it rather than subtracting it.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import signal
import subprocess
import sys
import time
from pathlib import Path

# The checkout this script sits in. `.reproduce/` under it holds the generated data and
# the engine binaries; neither is redistributed, so PRODDS_ROOT can point elsewhere.
REPO = Path(os.environ.get("PRODDS_ROOT", Path(__file__).resolve().parents[1]))
CEDAR_BIN = REPO / ".reproduce/engines/cedardb/cedar/cedardb"
PORT = 5433
SOCKET_DIR = "/tmp"

# 'C' (a real C compiler) is deliberately not swept: it costs about a second per query and is a
# research mode nobody runs. It is quoted once in the docstring from a single-query observation.
MODES = ["i", "A", "d", "c", "o"]
MODE_NAMES = {"i": "Interpreted", "A": "Auto (default)", "d": "DirectEmit",
              "c": "Cheap", "o": "Optimized", "a": "Adaptive", "C": "C"}
TIME_RE = re.compile(r"^Time:\s+([0-9.]+)\s+ms")
ERR_RE = re.compile(r"^psql:[^:]+:(\d+):\s*ERROR:\s*(.*)")


def wait_ready(timeout: int = 60) -> bool:
    for _ in range(timeout):
        p = subprocess.run(["psql", "-X", "-q", "-h", SOCKET_DIR, "-p", str(PORT),
                            "-U", "postgres", "-d", "postgres", "-c", "SELECT 1;"],
                           capture_output=True)
        if p.returncode == 0:
            return True
        time.sleep(1)
    return False


def start_server(db_dir: Path, log_path: Path):
    """Read-only, so a measurement pass cannot alter a delivered database."""
    log = log_path.open("w")
    proc = subprocess.Popen([str(CEDAR_BIN), "-readonly", "-address=127.0.0.1",
                             f"-port={PORT}", str(db_dir)],
                            stdout=log, stderr=log, stdin=subprocess.DEVNULL,
                            preexec_fn=os.setsid)
    if not wait_ready():
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        raise SystemExit(f"CedarDB did not become ready for {db_dir}")
    return proc


def stop_server(proc) -> None:
    os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
    try:
        proc.wait(timeout=60)
    except subprocess.TimeoutExpired:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        proc.wait(timeout=30)


def build_session(mode: str, rep: int, queries: list[tuple[str, str]]) -> tuple[str, dict[int, tuple[str, str]]]:
    """The session script, plus the map from its line numbers back to (query_id, stage).

    Statement names carry the repetition, so no `PREPARE` in a pass ever repeats a name, and each
    query is prepared exactly once per session -- preparing the same text twice in one session is
    measurably cheaper (163 ms then 139 ms on query_77), so repetitions are separate sessions.
    """
    lines = ["\\timing on", "\\o /dev/null",
             f"SET debug.compilationmode = '{mode}';",
             f"SET debug.compilationmode.prepared = '{mode}';",
             # Untimed warm-up: the first compilation in a fresh session pays one-time setup.
             "PREPARE warmup AS SELECT 1;"]
    labels: dict[int, tuple[str, str]] = {}
    order: list[tuple[str, str]] = []

    def emit(qid: str, stage: str, stmt: str) -> None:
        lines.append(f"\\echo @@{qid}|{stage}")
        order.append((qid, stage))
        lines.append(stmt)
        labels[len(lines)] = (qid, stage)

    emit("__floor__", "floor", f"PREPARE floor_r{rep} AS SELECT 1;")
    for qid, sql in queries:
        emit(qid, "explain", f"EXPLAIN {sql};")
        emit(qid, "prepare", f"PREPARE p_{qid}_r{rep} AS {sql};")
    return "\n".join(lines) + "\n", labels


def run_session(dbname: str, script: str, labels: dict[int, tuple[str, str]],
                work: Path) -> list[tuple[str, str, float, str]]:
    path = work / "session.sql"
    path.write_text(script, encoding="utf-8")
    p = subprocess.run(["psql", "-X", "-q", "-h", SOCKET_DIR, "-p", str(PORT),
                        "-U", "postgres", "-d", dbname, "-f", str(path)],
                       capture_output=True, text=True)
    failed: dict[tuple[str, str], str] = {}
    for line in p.stderr.splitlines():
        m = ERR_RE.match(line)
        if m and int(m.group(1)) in labels:
            failed[labels[int(m.group(1))]] = m.group(2).strip()[:200]

    out: list[tuple[str, str, float, str]] = []
    pending: tuple[str, str] | None = None
    for line in p.stdout.splitlines():
        if line.startswith("@@"):
            qid, _, stage = line[2:].partition("|")
            pending = (qid, stage)
            continue
        m = TIME_RE.match(line)
        if m and pending:
            msg = failed.get(pending)
            out.append((pending[0], pending[1], float(m.group(1)), msg or "ok"))
            pending = None
    return out


def load_queries(qdir: Path, ids: list[str]) -> list[tuple[str, str]]:
    queries = []
    for qid in ids:
        text = (qdir / f"{qid}.sql").read_text(encoding="utf-8")
        # The generated files carry `-- start query` / `-- end query` banners and the union ones
        # close with the statement terminator, a banner, and then a second, empty statement. Line
        # comments come off first, then the file must hold exactly one non-empty statement:
        # PREPARE takes a single query, and an embedded semicolon would end it early and silently
        # measure a fragment. No query in either suite puts `--` inside a string literal, which is
        # the one case this simplification would get wrong.
        stripped = "\n".join(re.sub(r"--.*$", "", ln) for ln in text.splitlines())
        parts = [part.strip() for part in stripped.split(";")]
        parts = [part for part in parts if part]
        if len(parts) != 1:
            raise SystemExit(f"{qid}: expected one statement, found {len(parts)}")
        queries.append((qid, parts[0]))
    return queries


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scale", default="100", help="scale factor label, e.g. 100 or 10")
    ap.add_argument("--suites", default="prodds,tpcds")
    ap.add_argument("--modes", default=",".join(MODES))
    ap.add_argument("--reps", type=int, default=5)
    ap.add_argument("--out", default="cedardb_compilation_raw.csv")
    ap.add_argument("--work", default=None, help="scratch directory for session scripts")
    args = ap.parse_args()

    sf = args.scale
    base = REPO / f".reproduce/sf{sf}"
    # Both scales keep the audited subset in the key-skew tree; the older tree is the fallback.
    subset_path = next(p for p in (base / "results_keyskew/common_subset.json",
                                   base / "results/common_subset.json") if p.exists())
    subset = json.loads(subset_path.read_text(encoding="utf-8"))["suites"]
    work = Path(args.work or os.environ.get("TMPDIR", "/tmp")) / "cedar_compile"
    work.mkdir(parents=True, exist_ok=True)

    rows: list[list] = []
    for suite in args.suites.split(","):
        ids = sorted(subset[suite]["common"])
        qdir = base / "queries/cedardb" / suite
        queries = load_queries(qdir, ids)
        db_label = f"prodds_sf{sf}_str5" if suite == "prodds" else f"tpcds_sf{sf}"
        db_dir = base / "databases/cedardb" / db_label
        print(f"[{suite}] SF{sf}: {len(queries)} queries, database {db_label}", flush=True)

        proc = start_server(db_dir, work / f"server_{suite}_sf{sf}.log")
        try:
            for mode in args.modes.split(","):
                for rep in range(1, args.reps + 1):
                    t0 = time.monotonic()
                    script, labels = build_session(mode, rep, queries)
                    for qid, stage, ms, status in run_session(db_label, script, labels, work):
                        rows.append([suite, sf, mode, MODE_NAMES.get(mode, mode), rep,
                                     qid, stage, f"{ms:.3f}", status])
                    print(f"  mode {mode:1s} ({MODE_NAMES.get(mode, mode):14s}) rep {rep}: "
                          f"{time.monotonic() - t0:6.1f} s", flush=True)
        finally:
            stop_server(proc)

    with open(args.out, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["suite", "scale_factor", "mode", "mode_name", "repetition",
                    "query_id", "stage", "elapsed_ms", "status"])
        w.writerows(rows)
    bad = sum(1 for r in rows if r[-1] != "ok")
    print(f"\n{len(rows)} measurements written to {args.out} ({bad} not ok)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
