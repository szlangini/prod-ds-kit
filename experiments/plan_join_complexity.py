#!/usr/bin/env python3
"""Quantify how much of a query's written join complexity survives optimisation.

Reviewer D4 of the July 2026 round, on Section 3.2.3:

    "Section 3.2.3 notes that optimizer simplifications may eliminate some generated joins.
     It would be helpful to quantify this effect and discuss whether the intended join
     complexity is still achieved at a plan level. Section 5.2.3 could also include such an
     analysis alongside the join-scaling results."

For every query this compares two counts:

  * ``sql_joins``  — joins in the query as written, taken from the WorkloadLens AST coverage
                     (``operators.join`` in ``coverage.jsonl``), i.e. the number the paper
                     reports as the workload's join complexity;
  * ``plan_joins`` — join operators the engine actually puts in its plan, obtained with
                     EXPLAIN. No query is executed, so this is cheap and side-effect free.

A ratio below 1 means the optimiser removed joins (unnesting a subquery into a semi-join that
collapses, eliminating a join whose key is a foreign key with a guaranteed match, folding a
constant side away). A ratio above 1 means the engine introduced joins the SQL does not write
(a duplicate-eliminating DELIM_JOIN for a correlated subquery, a mark join for IN/EXISTS).
Both directions are interesting for D4 and both occur.

The engines disagree on what counts as a join operator, so the per-engine operator sets below
are the contract; they are listed in the output header comment so a reader can check them.

Usage:
    python3 experiments/plan_join_complexity.py \\
        --engine duckdb --sf 10 \\
        --coverage ~/keyskew_work/campaign/wl_sf10/analyses/prodds/coverage.jsonl \\
        --out experiments/data/paper_csv/plan_join_complexity_SF10_duckdb.csv

    # all three engines, writing one CSV each (engines must be running for cedardb/monetdb)
    for e in duckdb cedardb monetdb; do python3 experiments/plan_join_complexity.py --engine $e --sf 100 ...; done
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

REPO = Path(__file__).resolve().parent.parent

# ── What counts as a join, per engine ───────────────────────────────────────
# DuckDB physical operator names (EXPLAIN (FORMAT JSON) -> "name").
DUCKDB_JOINS = {
    "HASH_JOIN", "NESTED_LOOP_JOIN", "BLOCKWISE_NL_JOIN", "PIECEWISE_MERGE_JOIN",
    "IE_JOIN", "ASOF_JOIN", "POSITIONAL_JOIN", "CROSS_PRODUCT", "DELIM_JOIN",
}
# Postgres node types, for a real PostgreSQL server.
POSTGRES_JOINS = {"Hash Join", "Nested Loop", "Merge Join"}
# CedarDB speaks the Postgres wire protocol but emits its OWN EXPLAIN JSON: a tree of
# {"operator": <logical>, "physicalOperator": <physical>, ...}. The logical name is the
# one comparable to the AST count; the physical name goes into the audit histogram
# (hashjoin / indexnljoin / singletonjoin — the last is CedarDB's scalar-subquery form).
CEDARDB_JOINS = {"join", "groupjoin", "multiwayjoin"}
# MonetDB relational-plan operators. In 11.55 the relational plan comes from EXPLAIN
# (the PLAN keyword is rejected by this server's parser); the plan prints operators as
# "join (", "left outer join (", "semijoin (" and so on.
MONETDB_JOIN_RE = re.compile(
    r"\b(left outer join|right outer join|full outer join|semijoin|antijoin|crossproduct|join)\s*\(",
    re.IGNORECASE,
)


def die(msg: str) -> None:
    print(f"[plan-joins] FATAL: {msg}", file=sys.stderr)
    raise SystemExit(1)


def read_sql(path: Path) -> str:
    return path.read_text(encoding="utf-8").strip().rstrip(";")


def ast_join_counts(coverage: Path) -> Dict[str, int]:
    """query name -> join count in the SQL, from the WorkloadLens AST coverage."""
    out: Dict[str, int] = {}
    if not coverage.exists():
        die(f"coverage file not found: {coverage}")
    for line in coverage.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        rec = json.loads(line)
        ident = str(rec.get("id") or "")
        name = ident.split("#", 1)[0].removesuffix(".sql")
        ops = rec.get("operators")
        if isinstance(ops, str):
            try:
                ops = json.loads(ops.replace("'", '"'))
            except json.JSONDecodeError:
                ops = None
        if not isinstance(ops, dict) or not name:
            continue
        out[name] = int(ops.get("join") or 0)
    return out


# ── Per-engine plan extraction ──────────────────────────────────────────────
def _count_json_nodes(node: Any, key: str, wanted: set,
                      hist: Optional[Dict[str, int]] = None) -> int:
    """Walk an EXPLAIN JSON tree counting nodes whose ``key`` is in ``wanted``.

    ``hist``, when given, collects which operator each hit was, so the number in the
    CSV can be audited against the plan rather than taken on trust.
    """
    total = 0
    if isinstance(node, dict):
        val = node.get(key)
        if isinstance(val, str) and val in wanted:
            total += 1
            if hist is not None:
                hist[val] = hist.get(val, 0) + 1
        for v in node.values():
            total += _count_json_nodes(v, key, wanted, hist)
    elif isinstance(node, list):
        for v in node:
            total += _count_json_nodes(v, key, wanted, hist)
    return total


def _json_after_first_bracket(text: str) -> Optional[Any]:
    for opener in ("[", "{"):
        idx = text.find(opener)
        if idx >= 0:
            try:
                return json.loads(text[idx:])
            except json.JSONDecodeError:
                continue
    return None


def explain_duckdb(sql: str, cfg: Dict[str, Any]) -> Tuple[Optional[int], str, str]:
    cmd = [cfg["cli_path"], "-readonly", "-noheader", "-list", cfg["db_path"]]
    proc = subprocess.run(cmd, input=f"EXPLAIN (FORMAT JSON) {sql};\n",
                          capture_output=True, text=True, timeout=cfg.get("timeout", 120))
    if proc.returncode != 0:
        return None, (proc.stderr or proc.stdout).strip()[:200], ""
    tree = _json_after_first_bracket(proc.stdout)
    if tree is None:
        return None, "could not parse EXPLAIN JSON", ""
    hist: Dict[str, int] = {}
    n = _count_json_nodes(tree, "name", DUCKDB_JOINS, hist)
    return n, "", " ".join(f"{k}={v}" for k, v in sorted(hist.items()))


def explain_postgres(sql: str, cfg: Dict[str, Any]) -> Tuple[Optional[int], str, str]:
    cmd = [cfg.get("client_path", "psql"), "-X", "-q", "-t", "-A",
           "-h", str(cfg["host"]), "-p", str(cfg["port"]),
           "-U", str(cfg["user"]), "-d", str(cfg["dbname"]),
           "-v", "ON_ERROR_STOP=1",
           "-c", f"EXPLAIN (FORMAT JSON) {sql}"]
    env = {"PGPASSWORD": str(cfg.get("password", ""))}
    proc = subprocess.run(cmd, capture_output=True, text=True,
                          timeout=cfg.get("timeout", 120), env={**cfg.get("env", {}), **env})
    if proc.returncode != 0:
        return None, (proc.stderr or proc.stdout).strip()[:200], ""
    tree = _json_after_first_bracket(proc.stdout)
    if tree is None:
        return None, "could not parse EXPLAIN JSON", ""
    hist: Dict[str, int] = {}
    n = _count_json_nodes(tree, "Node Type", POSTGRES_JOINS, hist)
    return n, "", " ".join(f"{k}={v}" for k, v in sorted(hist.items()))


def explain_cedardb(sql: str, cfg: Dict[str, Any]) -> Tuple[Optional[int], str, str]:
    cmd = [cfg.get("client_path", "psql"), "-X", "-q", "-t", "-A",
           "-h", str(cfg["host"]), "-p", str(cfg["port"]),
           "-U", str(cfg["user"]), "-d", str(cfg["dbname"]),
           "-v", "ON_ERROR_STOP=1",
           "-c", f"EXPLAIN (FORMAT JSON) {sql}"]
    env = {"PGPASSWORD": str(cfg.get("password", ""))}
    proc = subprocess.run(cmd, capture_output=True, text=True,
                          timeout=cfg.get("timeout", 120), env={**cfg.get("env", {}), **env})
    if proc.returncode != 0:
        return None, (proc.stderr or proc.stdout).strip()[:200], ""
    tree = _json_after_first_bracket(proc.stdout)
    if tree is None:
        return None, "could not parse EXPLAIN JSON", ""
    n = _count_json_nodes(tree, "operator", CEDARDB_JOINS)
    phys: Dict[str, int] = {}
    _collect_physical_for_joins(tree, phys)
    return n, "", " ".join(f"{k}={v}" for k, v in sorted(phys.items()))


def _collect_physical_for_joins(node: Any, hist: Dict[str, int]) -> None:
    """Histogram of ``physicalOperator`` on the nodes counted as joins."""
    if isinstance(node, dict):
        if node.get("operator") in CEDARDB_JOINS:
            key = str(node.get("physicalOperator") or node.get("operator"))
            hist[key] = hist.get(key, 0) + 1
        for v in node.values():
            _collect_physical_for_joins(v, hist)
    elif isinstance(node, list):
        for v in node:
            _collect_physical_for_joins(v, hist)


def explain_monetdb(sql: str, cfg: Dict[str, Any]) -> Tuple[Optional[int], str, str]:
    cmd = [cfg.get("mclient_path", "mclient"), "-d", str(cfg["dbname"]),
           "-h", str(cfg["host"]), "-p", str(cfg["port"]),
           "-f", "raw", "-s", f"EXPLAIN {sql}"]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=cfg.get("timeout", 120))
    if proc.returncode != 0:
        return None, (proc.stderr or proc.stdout).strip()[:200], ""
    hits = [m.lower() for m in MONETDB_JOIN_RE.findall(proc.stdout)]
    hist: Dict[str, int] = {}
    for h in hits:
        hist[h] = hist.get(h, 0) + 1
    return len(hits), "", " ".join(f"{k.replace(' ', '_')}={v}" for k, v in sorted(hist.items()))


EXPLAINERS = {"duckdb": explain_duckdb, "cedardb": explain_cedardb,
              "postgres": explain_postgres, "monetdb": explain_monetdb}


def load_engine_cfg(engine: str, config: Optional[Path], sf: str, db: Optional[str]) -> Dict[str, Any]:
    cfg: Dict[str, Any] = {}
    if config and config.exists():
        try:
            import yaml  # noqa: WPS433
            doc = yaml.safe_load(config.read_text(encoding="utf-8")) or {}
            native = ((doc.get("engines") or {}).get(engine) or {}).get("native") or {}
            cfg.update({k: v for k, v in native.items() if v is not None})
        except ImportError:
            print("[plan-joins] pyyaml missing — falling back to defaults", file=sys.stderr)
    if engine == "duckdb":
        cfg.setdefault("cli_path", str(REPO / ".reproduce/engines/duckdb/duckdb"))
        cfg.setdefault("db_path", db or str(REPO / f".reproduce/sf{sf}/databases/duckdb/prodds_sf{sf}_str5.duckdb"))
    elif engine in ("cedardb", "postgres"):
        cfg.setdefault("host", "/tmp"); cfg.setdefault("port", 5433)
        cfg.setdefault("user", "postgres"); cfg.setdefault("password", "postgres")
        cfg.setdefault("dbname", db or f"prodds_sf{sf}_str5")
    elif engine == "monetdb":
        cfg.setdefault("host", "localhost"); cfg.setdefault("port", 50000)
        cfg.setdefault("dbname", db or f"prodds_sf{sf}_str5")
    return cfg


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--engine", required=True, choices=sorted(EXPLAINERS))
    ap.add_argument("--sf", default="100")
    ap.add_argument("--queries-dir", type=Path, default=None,
                    help="default: .reproduce/sf<SF>/queries/<engine>/prodds")
    ap.add_argument("--coverage", type=Path, required=True,
                    help="WorkloadLens coverage.jsonl for the same query set (AST join counts)")
    ap.add_argument("--config", type=Path, default=None, help="engine config YAML (connection details)")
    ap.add_argument("--db", default=None, help="database name / path override")
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--timeout", type=float, default=120.0, help="per-EXPLAIN timeout in seconds")
    args = ap.parse_args()

    qdir = args.queries_dir or REPO / f".reproduce/sf{args.sf}/queries/{args.engine}/prodds"
    if not qdir.is_dir():
        die(f"queries dir not found: {qdir}")
    cfg = load_engine_cfg(args.engine, args.config, args.sf, args.db)
    cfg["timeout"] = args.timeout
    explain = EXPLAINERS[args.engine]
    ast = ast_join_counts(args.coverage)

    rows: List[List[Any]] = []
    files = sorted(qdir.glob("*.sql"))
    print(f"[plan-joins] {args.engine} SF{args.sf}: {len(files)} queries from {qdir}")
    for path in files:
        name = path.stem
        sql = read_sql(path)
        plan_joins, err, ops = explain(sql, cfg)
        sql_joins = ast.get(name)
        ratio = ""
        if plan_joins is not None and sql_joins:
            ratio = f"{plan_joins / sql_joins:.3f}"
        rows.append([args.engine, name,
                     "" if sql_joins is None else sql_joins,
                     "" if plan_joins is None else plan_joins,
                     ratio, ops, err])
        if err:
            print(f"    {name}: EXPLAIN failed — {err}")

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["engine", "query", "sql_joins", "plan_joins", "plan_over_sql", "plan_join_operators", "error"])
        w.writerows(rows)

    ok = [r for r in rows if r[3] != "" and r[2] != ""]
    if ok:
        tot_sql = sum(int(r[2]) for r in ok)
        tot_plan = sum(int(r[3]) for r in ok)
        fewer = sum(1 for r in ok if int(r[3]) < int(r[2]))
        same = sum(1 for r in ok if int(r[3]) == int(r[2]))
        more = sum(1 for r in ok if int(r[3]) > int(r[2]))
        print(f"[plan-joins] {len(ok)}/{len(rows)} queries explained; "
              f"joins written {tot_sql}, in plan {tot_plan} ({tot_plan / tot_sql:.2f}x); "
              f"fewer {fewer}, same {same}, more {more}")
    print(f"[plan-joins] wrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
