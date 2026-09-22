#!/usr/bin/env python3
"""Post-generation distribution sanity check for Prod-DS data variants.

Guards against the degenerate-injection failure class: a column that carried
information in the baseline must never end up with a single distinct value
after injection (count(DISTINCT) == 1 makes aggregate-comparison queries
unsatisfiable -- see the retention-floor rationale in workload/dsdgen/
stringify.py). Run it after every data generation; it is wired into the
calibration finisher.

Usage:
    validate_distributions.py <variant_dir> <baseline_dir> [--min-nonnull N]

Exit code 0 = clean; 1 = degenerate columns found (listed on stdout).
Requires the duckdb python module (PYTHONPATH to a pip --target works).
"""
from __future__ import annotations

import argparse
import re
import subprocess
import sys
import tempfile
from pathlib import Path

import duckdb

TABLE_RE = re.compile(r"^([a-z_]+)\.(?:dat|tbl)$")


def column_stats(con, data_dir: Path, min_nonnull: int) -> dict[str, int]:
    """qualified column -> distinct count, for columns with >= min_nonnull values."""
    schema_sql = (data_dir / "_schema.sql").read_text()
    con.execute("BEGIN")
    con.execute(schema_sql)
    out: dict[str, int] = {}
    for f in sorted(data_dir.iterdir()):
        m = TABLE_RE.match(f.name)
        if not m:
            continue
        table = m.group(1)
        col_types = con.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            f"WHERE table_name='{table}' ORDER BY ordinal_position").fetchall()
        if not col_types:
            continue
        cols = [c for c, _ in col_types]
        # read_csv needs a literal columns struct (no subqueries in table
        # functions); dsdgen output is ISO-8859-1, so convert like the loaders do.
        columns_literal = ", ".join(f"'{c}': '{t}'" for c, t in col_types)
        selects = ", ".join(
            f"count({c}) AS nn_{i}, count(DISTINCT {c}) AS d_{i}"
            for i, c in enumerate(cols)
        )
        with tempfile.NamedTemporaryFile(suffix=".dat") as tmp:
            subprocess.run(
                ["iconv", "-f", "ISO-8859-1", "-t", "UTF-8", str(f)],
                stdout=tmp, check=True,
            )
            tmp.flush()
            row = con.execute(
                f"SELECT {selects} FROM read_csv('{tmp.name}', delim='|', "
                f"header=false, nullstr='', columns={{{columns_literal}}})"
            ).fetchone()
        for i, c in enumerate(cols):
            nn, distinct = row[2 * i], row[2 * i + 1]
            if nn >= min_nonnull:
                out[f"{table}.{c}"] = distinct
    con.execute("ROLLBACK")
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("variant_dir", type=Path)
    parser.add_argument("baseline_dir", type=Path)
    parser.add_argument("--min-nonnull", type=int, default=1000)
    args = parser.parse_args()

    con = duckdb.connect()
    con.execute("SET threads TO 16")
    variant = column_stats(con, args.variant_dir, args.min_nonnull)
    baseline = column_stats(con, args.baseline_dir, args.min_nonnull)

    degenerate = sorted(
        qualified for qualified, distinct in variant.items()
        if distinct == 1 and baseline.get(qualified, 0) > 1
    )
    if degenerate:
        print(f"DEGENERATE ({len(degenerate)}) -- multi-valued in baseline, "
              "single-valued in variant:")
        for qualified in degenerate:
            print(f"  {qualified}")
        return 1
    print(f"OK: no degenerate columns ({len(variant)} columns checked, "
          f">= {args.min_nonnull} non-null)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
