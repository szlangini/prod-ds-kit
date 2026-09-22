#!/usr/bin/env python3
"""Canonical query inventory: position, template, hash, and membership per experiment.

Query files are named by the dsqgen STREAMS position, not by TPC-DS template number, so
`query_41.sql` is template Q17. Every claim about "Q17" therefore depends on a mapping that is
easy to get silently wrong; this writes it out in both directions, with content hashes, and
records which experiment population each query belongs to and why it is in or out.

Usage:
    python experiments/export_query_inventory.py --out-dir <handoff>
"""
from __future__ import annotations

import argparse, csv, hashlib, json, re
from pathlib import Path
from typing import Any, Dict, List

REPO = Path(__file__).resolve().parent.parent
POS_RE = re.compile(r"^query_(\d+)$")
ENGINES = ("duckdb", "cedardb", "monetdb")


def write_csv(path: Path, header: List[str], rows: List[List[Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(header)
        w.writerows(rows)
    print(f"  wrote {path.name} ({len(rows)} rows)")


def kind_of(q: str) -> str:
    if q.startswith("query_join_"):
        return "join_amplification"
    if q.startswith("query_union_"):
        return "union_amplification"
    return "tpcds_template"


def micro_param(q: str) -> str:
    m = re.match(r"query_(join_J|union_U)(\d+)$", q)
    return m.group(2) if m else ""


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out-dir", type=Path, default=Path("/home/jvs34/prodds-revision-handoff"))
    args = ap.parse_args()
    data = args.out_dir / "data"

    # ── mapping, both directions, with hashes ────────────────────
    map_rows: List[List[Any]] = []
    for sf in ("10", "100"):
        for suite in ("prodds", "tpcds"):
            perm_path = None
            for eng in ENGINES:
                for nm in (f"{suite}_run", suite):
                    p = REPO / f".reproduce/sf{sf}/queries/{eng}/{nm}/_permutation.json"
                    if p.exists():
                        perm_path = p
                        break
                if perm_path:
                    break
            if not perm_path:
                continue
            perm = {int(k): int(v) for k, v in json.loads(
                perm_path.read_text(encoding="utf-8")).items()}
            for tmpl, pos in sorted(perm.items()):
                row = [f"SF{sf}", suite, f"Q{tmpl}", tmpl, f"query_{pos}", pos]
                for eng in ENGINES:
                    h = ""
                    for nm in (f"{suite}_run", suite):
                        f = REPO / f".reproduce/sf{sf}/queries/{eng}/{nm}/query_{pos}.sql"
                        if f.exists():
                            h = hashlib.sha256(f.read_bytes()).hexdigest()[:16]
                            break
                    row.append(h)
                map_rows.append(row)
    write_csv(data / "query_template_mapping.csv",
              ["scale", "suite", "template", "template_num", "query_file", "position",
               "sha256_duckdb", "sha256_cedardb", "sha256_monetdb"], map_rows)

    # ── inventory per experiment ─────────────────────────────────
    inv: List[List[Any]] = []
    for sf in ("10", "100"):
        cs_path = REPO / f".reproduce/sf{sf}/results_keyskew/common_subset.json"
        if not cs_path.exists():
            continue
        cs = json.loads(cs_path.read_text(encoding="utf-8"))
        for suite, srep in (cs.get("suites") or {}).items():
            perm = {}
            for eng in ENGINES:
                for nm in (f"{suite}_run", suite):
                    p = REPO / f".reproduce/sf{sf}/queries/{eng}/{nm}/_permutation.json"
                    if p.exists():
                        perm = {int(v): int(k) for k, v in json.loads(
                            p.read_text(encoding="utf-8")).items()}
                        break
                if perm:
                    break
            common = set(srep.get("common") or [])
            universe = set(common)
            for info in (srep.get("engines") or {}).values():
                universe |= set(info.get("success_queries") or [])
                universe |= set(info.get("failed") or {})
            for q in sorted(universe):
                m = POS_RE.match(q)
                tmpl = f"Q{perm[int(m.group(1))]}" if (m and int(m.group(1)) in perm) else ""
                reasons = []
                for eng, info in (srep.get("engines") or {}).items():
                    f = (info.get("failed") or {}).get(q)
                    if f:
                        reasons.append(f"{eng}: {f['cause']}")
                in_e0 = "included"
                in_e1 = "included" if q in common else "excluded"
                # E5 additionally drops the J* micro-suite and any U level above 200
                lvl = micro_param(q)
                dropped_micro = q.startswith("query_join_") or (
                    q.startswith("query_union_") and lvl.isdigit() and int(lvl) > 200)
                in_e5 = "excluded" if (q not in common or dropped_micro) else "included"
                e5_reason = ("; ".join(reasons) if q not in common
                             else ("micro-suite dropped (WORKLOAD_DROP_MICROSUITE=1)"
                                   if dropped_micro else ""))
                inv.append([f"SF{sf}", suite, q, tmpl, kind_of(q), micro_param(q),
                            in_e0, in_e1, "; ".join(reasons), in_e5, e5_reason])
    write_csv(data / "query_inventory.csv",
              ["scale", "suite", "query_file", "template", "kind", "micro_parameter",
               "in_E0_audit", "in_E1_common_subset", "E1_exclusion_reason",
               "in_E5_population", "E5_exclusion_reason"], inv)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
