#!/usr/bin/env python3
"""Classify column roles across a generated query set and emit the
literal-filter exclusion list for role-scoped MCV eligibility.

A column is written to the output list iff it appears somewhere in the
workload inside a comparison against a literal (WHERE/HAVING/ON/CASE
conditions with =, <>, <, <=, >, >=, IN, BETWEEN, LIKE). Concentrating such a
column can collapse predicate selectivity and empty queries, so MCV injection
must never touch it. Columns referenced only as join predicates, GROUP BY /
ORDER BY keys, aggregate inputs, or plain projections are NOT listed:
concentrating them shrinks groups or changes aggregate values but cannot empty
a result, so a role-scoped MCV profile (query_exclusion_scope: filtered) may
skew them.

Known approximation: alias-laundered filters (a CTE projects `d_year AS y`,
the outer query filters `y = 2000`) attribute the literal to the alias, not
the source column. Across the full workload the columns reached that way are
also filtered directly elsewhere, and the 107-query non-empty gate is the
empirical backstop for the shipped configuration.

Usage:
    python tools/analyze_query_column_roles.py \
        --queries-dir .reproduce/sf10/queries/duckdb/prodds \
        [--queries-dir ...] \
        --out config/query_filter_columns.txt
"""
from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from pathlib import Path

try:
    import sqlglot
    from sqlglot import exp
except ImportError as exc:  # pragma: no cover
    raise SystemExit("This tool needs sqlglot (pip install sqlglot).") from exc

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from workload.dsdgen import stringify  # noqa: E402

CONDITION_TYPES = (
    exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE,
    exp.In, exp.Between, exp.Like, exp.ILike,
)


def collect_roles(sql_files: list[Path], dialect: str) -> dict[str, set[str]]:
    roles: dict[str, set[str]] = defaultdict(set)
    parsed = failed = 0
    for sql_file in sql_files:
        text = sql_file.read_text(encoding="utf-8")
        try:
            trees = sqlglot.parse(text, read=dialect)
        except Exception as e:
            failed += 1
            print(f"[roles] PARSE FAIL {sql_file.name}: {e}", file=sys.stderr)
            continue
        parsed += 1
        for tree in trees:
            if tree is None:
                continue
            for cond in tree.find_all(*CONDITION_TYPES):
                has_literal = any(True for _ in cond.find_all(exp.Literal))
                role = "filter_literal" if has_literal else "join_pred"
                for column in cond.find_all(exp.Column):
                    roles[column.name.lower()].add(role)
            for group in tree.find_all(exp.Group):
                for column in group.find_all(exp.Column):
                    roles[column.name.lower()].add("group")
            for order in tree.find_all(exp.Order):
                for column in order.find_all(exp.Column):
                    roles[column.name.lower()].add("order")
            for agg in tree.find_all(exp.AggFunc):
                for column in agg.find_all(exp.Column):
                    roles[column.name.lower()].add("agg_input")
    print(f"[roles] parsed={parsed} failed={failed} names={len(roles)}")
    return roles


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--queries-dir", action="append", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--dialect", default="duckdb")
    args = parser.parse_args()

    sql_files: list[Path] = []
    for qdir in args.queries_dir:
        found = sorted(qdir.glob("*.sql"))
        if not found:
            raise SystemExit(f"No .sql files in {qdir}")
        sql_files.extend(found)

    roles = collect_roles(sql_files, args.dialect)

    schema = stringify._schema_cache()
    schema_columns = {
        column.lower()
        for meta in schema.values()
        for column in (meta.get("columns") or [])
    }
    filtered = sorted(
        name for name, r in roles.items()
        if "filter_literal" in r and name in schema_columns
    )

    lines = [
        "# Column names compared against a LITERAL anywhere in the query workload",
        "# (WHERE/HAVING/ON/CASE with =, <>, <, <=, >, >=, IN, BETWEEN, LIKE).",
        "# MCV injection with query_exclusion_scope: filtered excludes exactly this",
        "# list (plus keys and the curated exclusions); columns referenced only as",
        "# join predicates, GROUP BY / ORDER BY keys, aggregate inputs, or plain",
        "# projections stay eligible. Regenerate with:",
        "#   python tools/analyze_query_column_roles.py \\",
        "#     --queries-dir .reproduce/sf10/queries/duckdb/prodds --out config/query_filter_columns.txt",
    ]
    lines.extend(filtered)
    args.out.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"[roles] wrote {len(filtered)} literal-filtered column names -> {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
