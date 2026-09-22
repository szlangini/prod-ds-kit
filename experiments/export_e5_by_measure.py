#!/usr/bin/env python3
"""E5 skew deltas under each timer measure.

Every E5 record carries two separately timed client processes: one running `EXPLAIN <query>` and
one running the query. The delivered deltas are computed on their sum. For E5's queries that
`EXPLAIN` client is a large and near-constant share of the total -- 14-28 % for the tail family --
so it dilutes every percentage delta. This exports the delta under both measures so the effect is
visible rather than buried.

It changes no conclusion: direction and ordering are identical. It changes magnitudes.

Usage:
    python experiments/export_e5_by_measure.py --sf 100 --out-dir <handoff>/data
"""
from __future__ import annotations

import argparse, csv, glob, json, re, statistics
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List

REPO = Path(__file__).resolve().parent.parent
POS_RE = re.compile(r"^query_(\d+)$")
ENGINES = ("duckdb", "cedardb", "monetdb")


def permutation(sf: str) -> Dict[int, int]:
    for eng in ENGINES:
        for nm in ("prodds_run", "prodds"):
            p = REPO / f".reproduce/sf{sf}/queries/{eng}/{nm}/_permutation.json"
            if p.exists():
                return {int(v): int(k) for k, v in json.loads(p.read_text()).items()}
    return {}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sf", default="100")
    ap.add_argument("--out-dir", type=Path,
                    default=Path("/home/jvs34/prodds-revision-handoff/data"))
    args = ap.parse_args()
    perm = permutation(args.sf)
    base = REPO / f".reproduce/sf{args.sf}/results_keyskew"

    rows: List[List[Any]] = []
    for tierdir, tier in ((base / "E5", "medium"), (base / "E5_low", "low"),
                          (base / "E5_high", "high")):
        if not tierdir.is_dir():
            continue
        med: Dict[tuple, Dict[str, float]] = {}
        for unit in sorted(tierdir.iterdir()):
            if not unit.is_dir():
                continue
            arm, engine = unit.name.rsplit("_", 1)
            raws = sorted(glob.glob(str(unit / "*/*/*/raw.jsonl")))
            if not raws:
                continue
            acc = defaultdict(lambda: {"total": [], "exec": [], "plan": []})
            for line in open(raws[-1]):
                r = json.loads(line)
                if r.get("status") != "success":
                    continue
                a = acc[r["query_id"]]
                for key, fld in (("total", "wall_time_ms_total"),
                                 ("exec", "wall_time_ms_execution"),
                                 ("plan", "wall_time_ms_planning")):
                    v = r.get(fld)
                    if isinstance(v, (int, float)):
                        a[key].append(float(v))
            for q, a in acc.items():
                med[(engine, arm, q)] = {k: statistics.median(v) for k, v in a.items() if v}

        for (engine, arm, q), m in sorted(med.items()):
            if arm == "baseline":
                continue
            b = med.get((engine, "baseline", q))
            if not b:
                continue
            pm = POS_RE.match(q)
            tmpl = f"Q{perm[int(pm.group(1))]}" if (pm and int(pm.group(1)) in perm) else q
            def d(k):
                return f"{100 * (m[k] / b[k] - 1):+.2f}" if b.get(k) else ""
            rows.append([f"SF{args.sf}", tier, engine, arm, q, tmpl,
                         f"{b['total']:.1f}", f"{m['total']:.1f}", d("total"),
                         f"{b['exec']:.1f}", f"{m['exec']:.1f}", d("exec"),
                         f"{100 * b['plan'] / b['total']:.1f}" if b.get("total") else ""])

    args.out_dir.mkdir(parents=True, exist_ok=True)
    out = args.out_dir / f"E5_delta_by_measure_SF{args.sf}.csv"
    with out.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["scale", "tier", "engine", "arm", "query_id", "template",
                    "baseline_reported_total_ms", "arm_reported_total_ms", "delta_reported_pct",
                    "baseline_execution_client_ms", "arm_execution_client_ms", "delta_execution_pct",
                    "explain_share_of_baseline_total_pct"])
        w.writerows(rows)
    print(f"  wrote {out.name} ({len(rows)} rows)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
