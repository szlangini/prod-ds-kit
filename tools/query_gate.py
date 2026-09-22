#!/usr/bin/env python3
"""107-query non-empty gate for a Prod-DS data variant.

Loads the variant into a fresh DuckDB (schema WITH primary keys), runs every
query in the given query dir with a per-query timeout, and reports
errors / empty results / timings. For each empty or failed query it re-runs
that query against the baseline variant, so the DELTA (newly-empty vs
already-empty-at-baseline) is explicit — that delta is the gate metric.

Usage: query_gate.py <variant_data_dir> <baseline_data_dir> <queries_dir> <out.json>
"""
import json
import os
import re
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

import duckdb

VARIANT = Path(sys.argv[1])
BASELINE = Path(sys.argv[2])
QUERIES = Path(sys.argv[3])
OUT = Path(sys.argv[4])
# Per-query timeout; override for large scale factors (the union/join micro-suite
# at SF100 legitimately runs for minutes) via QUERY_GATE_TIMEOUT_S.
TIMEOUT_S = int(os.environ.get("QUERY_GATE_TIMEOUT_S", "180"))

TABLE_RE = re.compile(r"^(?P<t>[a-z_]+)\.dat$")


def load(data_dir: Path) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    schema_sql = (data_dir / "_schema.sql").read_text()
    con.execute(schema_sql)
    for f in sorted(data_dir.glob("*.dat")):
        m = TABLE_RE.match(f.name)
        if not m:
            continue
        with tempfile.NamedTemporaryFile(suffix=".dat") as tmp:
            subprocess.run(["iconv", "-f", "ISO-8859-1", "-t", "UTF-8", str(f)],
                           stdout=tmp, check=True)
            tmp.flush()
            con.execute(
                f"COPY {m.group('t')} FROM '{tmp.name}' "
                "(DELIMITER '|', HEADER false, NULL '', AUTO_DETECT false)")
    return con


def _real_statements(sql: str):
    """Split on ';' and drop chunks that contain only comments/whitespace."""
    out = []
    for chunk in sql.split(";"):
        body = "\n".join(
            line for line in chunk.splitlines()
            if line.strip() and not line.strip().startswith("--")
        )
        if body.strip():
            out.append(chunk)
    return out


def run_query(con, sql: str):
    """Returns (status, rows, seconds). status: ok|empty|error:<msg>.

    The per-query timeout is enforced with DuckDB's interrupt() from a watchdog
    thread; an interrupted query is reported as an error.
    """
    start = time.perf_counter()
    watchdog = threading.Timer(TIMEOUT_S, con.interrupt)
    watchdog.daemon = True
    watchdog.start()
    try:
        rows = 0
        for stmt in _real_statements(sql):
            body = "\n".join(
                line for line in stmt.splitlines() if not line.strip().startswith("--")
            ).strip()
            if body.split(None, 1)[0].lower() in ("select", "with"):
                # Count inside the engine, exactly like the experiment runner's
                # validation path (experiments/runner.py). fetchall() on a 28.8M-row,
                # 70-column join result builds ~2e9 Python objects and was what got
                # the SF100 gate OOM-killed -- the engine itself needs a few GB.
                rel = con.execute(f"SELECT COUNT(*) FROM ({body}) AS _q")
                rows = int(rel.fetchone()[0])
            else:
                con.execute(stmt)
        dt = time.perf_counter() - start
        return ("ok" if rows > 0 else "empty"), rows, dt
    except Exception as e:  # noqa: BLE001
        dt = time.perf_counter() - start
        if dt >= TIMEOUT_S:
            return f"error:Timeout:{TIMEOUT_S}s", 0, dt
        return f"error:{type(e).__name__}:{str(e)[:200]}", 0, dt
    finally:
        watchdog.cancel()


def open_variant(data_dir: Path, db_env: str) -> duckdb.DuckDBPyConnection:
    """Open the data: a prebuilt DuckDB file (read-only) when $<db_env> names one,
    else load the .dat directory into a fresh in-memory database.

    A file-backed database can spill to disk, which the 200-way join / 2048-way
    union micro-suite needs at SF100; an in-memory database cannot and gets the
    process killed. QUERY_GATE_MEMORY_LIMIT / QUERY_GATE_TEMP_DIR tune spilling.
    """
    db_path = os.environ.get(db_env)
    if db_path:
        print(f"[gate] opening {db_path} (read-only, ${db_env}) for {data_dir}", flush=True)
        con = duckdb.connect(db_path, read_only=True)
    else:
        print(f"[gate] loading {data_dir} ...", flush=True)
        con = load(data_dir)
    con.execute("SET threads TO 16")
    # Same session pragma the benchmark adapter uses (experiments/adapters/duckdb.py):
    # the U-fanin queries exceed the default expression-depth limit.
    con.execute("SET max_expression_depth TO 1000000")
    memory_limit = os.environ.get("QUERY_GATE_MEMORY_LIMIT")
    if memory_limit:
        con.execute(f"SET memory_limit = '{memory_limit}'")
    temp_dir = os.environ.get("QUERY_GATE_TEMP_DIR")
    if temp_dir:
        Path(temp_dir).mkdir(parents=True, exist_ok=True)
        con.execute(f"SET temp_directory = '{temp_dir}'")
    return con


def main():
    con = open_variant(VARIANT, "QUERY_GATE_VARIANT_DB")
    results = {}
    empties, errors = [], []
    files = sorted(QUERIES.glob("*.sql"))
    print(f"[gate] running {len(files)} queries ...", flush=True)
    for i, qf in enumerate(files, 1):
        sql = qf.read_text(encoding="utf-8")
        status, rows, dt = run_query(con, sql)
        results[qf.name] = {"status": status, "rows": rows, "seconds": round(dt, 2)}
        if status == "empty":
            empties.append(qf.name)
        elif status.startswith("error"):
            errors.append(qf.name)
        if i % 20 == 0 or status != "ok":
            print(f"[gate] {i}/{len(files)} {qf.name}: {status} rows={rows} {dt:.1f}s",
                  flush=True)
    con.close()

    baseline_status = {}
    if empties or errors:
        print(f"[gate] re-checking {len(empties) + len(errors)} queries on baseline ...",
              flush=True)
        bcon = open_variant(BASELINE, "QUERY_GATE_BASELINE_DB")
        for name in empties + errors:
            sql = (QUERIES / name).read_text(encoding="utf-8")
            status, rows, dt = run_query(bcon, sql)
            baseline_status[name] = {"status": status, "rows": rows}
            print(f"[gate]   baseline {name}: {status} rows={rows}", flush=True)
        bcon.close()

    newly_empty = [n for n in empties if baseline_status.get(n, {}).get("status") == "ok"]
    summary = {
        "variant": str(VARIANT),
        "queries": len(files),
        "ok": sum(1 for r in results.values() if r["status"] == "ok"),
        "empty": empties,
        "errors": errors,
        "newly_empty_vs_baseline": newly_empty,
        "baseline_status": baseline_status,
        "total_seconds": round(sum(r["seconds"] for r in results.values()), 1),
        "slowest": sorted(
            ((r["seconds"], n) for n, r in results.items()), reverse=True)[:10],
        "results": results,
    }
    OUT.write_text(json.dumps(summary, indent=1))
    print(f"[gate] DONE ok={summary['ok']}/{len(files)} empty={len(empties)} "
          f"errors={len(errors)} NEWLY-EMPTY={len(newly_empty)} -> {OUT}", flush=True)


if __name__ == "__main__":
    main()
