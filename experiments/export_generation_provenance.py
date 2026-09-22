#!/usr/bin/env python3
"""Export the exact generation profile of each shipped dataset.

Reviewer-facing question: for the datasets the numbers were measured on, which columns were
touched, which key groups were made hot, what share was aimed at, and what share was actually
realised?  All of it is recorded by the generator in
``<data-dir>/stringification_data_manifest.json``; this reads those manifests and flattens
them so a claim can be checked without opening JSON.

A target share is an expectation over the injection, not a deterministic guarantee for any one
key or any one query's filter, and a realised share is measured over the whole column, not
after a query's predicates. Both caveats are printed into the CSV header comment of the
summary file and repeated in docs/08.

Usage:
    python experiments/export_generation_provenance.py --out-dir <handoff>
"""
from __future__ import annotations

import argparse, csv, json
from pathlib import Path
from typing import Any, Dict, List

REPO = Path(__file__).resolve().parent.parent


def write_csv(path: Path, header: List[str], rows: List[List[Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(header)
        w.writerows(rows)
    print(f"  wrote {path.name} ({len(rows)} rows)")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out-dir", type=Path, default=Path("/home/jvs34/prodds-revision-handoff"))
    args = ap.parse_args()
    data = args.out_dir / "data"
    cfg_out = args.out_dir / "configs"
    cfg_out.mkdir(parents=True, exist_ok=True)

    key_rows: List[List[Any]] = []
    prof_rows: List[List[Any]] = []
    col_rows: List[List[Any]] = []

    for sf in ("1", "10", "100"):
        for variant in sorted((REPO / f".reproduce/sf{sf}/data").glob("prodds_sf*")):
            man = variant / "stringification_data_manifest.json"
            if not man.exists():
                continue
            m = json.loads(man.read_text(encoding="utf-8"))
            tag = variant.name
            # a copy of the manifest travels with the handoff, unmodified
            (cfg_out / f"generation_manifest_{tag}.json").write_text(
                json.dumps(m, indent=1), encoding="utf-8")

            prof_rows.append([
                f"SF{sf}", tag,
                m.get("stringification_enabled"), m.get("stringification_level"),
                m.get("stringification_preset"), m.get("intensity"),
                m.get("null_profile"), m.get("null_seed"), m.get("null_tier_alias"),
                m.get("mcv_profile"), m.get("mcv_seed"), m.get("mcv_tier_alias"),
                m.get("key_skew_enabled"), m.get("key_skew_profile"), m.get("key_skew_seed"),
                m.get("key_skew_tier_alias"), m.get("include_hot_paths"),
                m.get("min_ndv_for_injection"), m.get("null_small_table_floor"),
                len(m.get("key_skew_pairs") or {}), m.get("touched_columns_count"),
                m.get("files_rewritten"), m.get("rows_rewritten"),
                len(m.get("mcv_ndv_guard_excluded") or []),
                len(m.get("null_ndv_guard_excluded") or []),
            ])

            for group, d in sorted((m.get("key_skew_pairs") or {}).items()):
                tgt, got = d.get("target"), d.get("f1")
                key_rows.append([
                    f"SF{sf}", tag, group, d.get("canonical"), d.get("granularity"),
                    d.get("value"), d.get("entity_anchor"),
                    f"{tgt:.6f}" if isinstance(tgt, (int, float)) else "",
                    f"{got:.6f}" if isinstance(got, (int, float)) else "",
                    f"{(got - tgt):+.6f}" if isinstance(tgt, (int, float))
                    and isinstance(got, (int, float)) else "",
                    len(d.get("columns") or []), ";".join(d.get("columns") or []),
                ])

            for c in sorted(m.get("touched_columns") or []):
                col_rows.append([f"SF{sf}", tag, c])

    write_csv(data / "keyskew_target_vs_realised.csv",
              ["scale", "dataset", "key_group", "canonical_column", "granularity",
               "hot_value", "entity_anchor", "target_share", "realised_share",
               "realised_minus_target", "n_columns", "columns"], key_rows)
    write_csv(data / "generation_profiles.csv",
              ["scale", "dataset", "stringification_enabled", "str_level", "str_preset",
               "intensity", "null_profile", "null_seed", "null_tier",
               "mcv_profile", "mcv_seed", "mcv_tier", "key_skew_enabled", "key_skew_profile",
               "key_skew_seed", "key_skew_tier", "include_hot_paths", "min_ndv_for_injection",
               "null_small_table_floor", "n_key_groups", "n_touched_columns",
               "files_rewritten", "rows_rewritten", "n_mcv_ndv_guard_excluded",
               "n_null_ndv_guard_excluded"], prof_rows)
    write_csv(data / "active_columns.csv", ["scale", "dataset", "column"], col_rows)
    print(f"  copied {len(prof_rows)} generation manifests to {cfg_out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
