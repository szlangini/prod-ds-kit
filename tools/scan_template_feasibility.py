#!/usr/bin/env python3
"""Feasibility scan: which TPC-DS templates suit JCC-H-style MCV parameter binding.

Per Sebastian's note §7/§8.4, for every base template report:
  (a) which SKEWED FK edges the query traverses (join predicates whose column is
      in the key-skew column set) -- without a skewed edge, a normal/skewed
      parameter pair has nothing to hit;
  (b) literal-filter columns per dimension table (the candidates for binding a
      parameter to hot-key attributes) -- also the effort proxy (~JCC-H had
      10-40 special-case lines per query);
  (c) whether the .tpl has HARDCODED date/year literals in the body (blocker for
      a calendar-skew layer, per JCC-H Q7/Q8) vs parameterized defines.

Ranking: needs >=1 skewed edge AND >=1 literal filter on a dimension of that
edge; fewer distinct filter columns = less special-casing.
"""
import json
import re
import sys
from collections import defaultdict
from pathlib import Path

REPO = Path("/home/jvs34/prod-ds-cleanup")
sys.path.insert(0, str(REPO))

import sqlglot
from sqlglot import exp

from workload.dsdgen.stringify import KEY_SKEW_CHANNELS, _schema_cache

QUERIES = REPO / ".reproduce/sf1/queries/duckdb/prodds"
TEMPLATES = REPO / "query_templates"

# skewed FK column -> (channel, domain)
SKEWED_FK = {}
for ch in KEY_SKEW_CHANNELS:
    for dom, cfg in ch["domains"].items():
        for table, col in [cfg["canonical"], *cfg["mirrors"]]:
            SKEWED_FK[col.lower()] = (ch["channel"], dom)

# dimension table joined by each domain (for matching filters to edges)
DOMAIN_DIM = {
    "date": "date_dim", "ship_date": "date_dim", "return_date": "date_dim",
    "time": "time_dim", "return_time": "time_dim",
    "customer": "customer", "cdemo": "customer_demographics",
    "hdemo": "household_demographics", "addr": "customer_address",
    "store": "store", "promo": "promotion", "warehouse": "warehouse",
    "ship_mode": "ship_mode", "reason": "reason", "call_center": "call_center",
    "catalog_page": "catalog_page", "web_page": "web_page", "web_site": "web_site",
}

schema = _schema_cache()
COLUMN_TABLE = {}
for table, meta in schema.items():
    for col in meta.get("columns") or []:
        COLUMN_TABLE.setdefault(col.lower(), table.lower())

CONDS = (exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE,
         exp.In, exp.Between, exp.Like, exp.ILike)

DATE_LIT = re.compile(r"\b(19|20)\d\d\b|'\d{4}-\d{2}-\d{2}'")


def scan_sql(path: Path):
    text = path.read_text(encoding="utf-8")
    edges = set()
    filters = defaultdict(set)  # table -> filter columns
    try:
        trees = sqlglot.parse(text, read="duckdb")
    except Exception:
        return None
    for tree in trees:
        if tree is None:
            continue
        for cond in tree.find_all(*CONDS):
            cols = [c.name.lower() for c in cond.find_all(exp.Column)]
            has_lit = any(True for _ in cond.find_all(exp.Literal))
            if has_lit:
                for name in cols:
                    t = COLUMN_TABLE.get(name)
                    if t:
                        filters[t].add(name)
            else:
                for name in cols:
                    if name in SKEWED_FK:
                        edges.add(SKEWED_FK[name])
    return edges, filters


def scan_tpl(qnum: int):
    """Hardcoded date/year literals in the template BODY (outside define lines)."""
    for name in (f"query{qnum}_ext.tpl", f"query{qnum}.tpl"):
        p = TEMPLATES / name
        if p.exists():
            hard = []
            defines = 0
            for line in p.read_text(encoding="utf-8", errors="replace").splitlines():
                s = line.strip().lower()
                if s.startswith("define"):
                    defines += 1
                    continue
                if s.startswith("--"):
                    continue
                for m in DATE_LIT.finditer(line):
                    hard.append(m.group(0))
            return name, defines, sorted(set(hard))
    return None, 0, []


def main():
    rows = []
    for qf in sorted(QUERIES.glob("query_*.sql")):
        m = re.match(r"query_(\d+)\.sql$", qf.name)
        if not m:
            continue
        qnum = int(m.group(1))
        res = scan_sql(qf)
        if res is None:
            continue
        edges, filters = res
        # filters on dimensions belonging to a traversed skewed edge
        edge_dims = {DOMAIN_DIM[d] for _, d in edges if d in DOMAIN_DIM}
        bindable = {t: sorted(cs) for t, cs in filters.items() if t in edge_dims}
        effort = sum(len(cs) for cs in bindable.values())
        tpl, defines, hard = scan_tpl(qnum)
        rows.append({
            "q": qnum,
            "edges": sorted(f"{c}.{d}" for c, d in edges),
            "bindable_filters": bindable,
            "effort_cols": effort,
            "hardcoded_dates": hard,
            "defines": defines,
        })

    feasible = [r for r in rows if r["edges"] and r["bindable_filters"]]
    feasible.sort(key=lambda r: (bool(r["hardcoded_dates"]), r["effort_cols"], -len(r["edges"])))

    print(f"templates gescannt: {len(rows)}")
    print(f"mit skewed Edge:    {sum(1 for r in rows if r['edges'])}")
    print(f"bindbar (Edge + Dim-Literal-Filter): {len(feasible)}")
    print(f"davon ohne hartkodierte Daten:       {sum(1 for r in feasible if not r['hardcoded_dates'])}")
    print(f"\n== Top-Kandidaten (sortiert: keine Hardcoded-Dates, wenig Aufwand) ==")
    print(f"{'q':>4} {'edges':>2} {'filt':>4} {'hardDate':>8}  bindbare Filter (Tabelle: Spalten)")
    for r in feasible[:20]:
        bind = "; ".join(f"{t}: {','.join(cs)}" for t, cs in sorted(r["bindable_filters"].items()))
        print(f"q{r['q']:>3} {len(r['edges']):>2} {r['effort_cols']:>4} "
              f"{'JA' if r['hardcoded_dates'] else '-':>8}  {bind[:95]}")
    Path(sys.argv[1]).write_text(json.dumps(rows, indent=1))
    print(f"\nvoller Report -> {sys.argv[1]}")


if __name__ == "__main__":
    main()
