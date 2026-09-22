#!/usr/bin/env python3
"""Export the join and union ladder records, with attempted/not-attempted made explicit.

The delivered ladder summaries report a failure count per level, which reads as "1 failure" even
for levels the runner never ran. Once a level fails, `ladder_abandon` skips every same-or-higher
level and writes a placeholder record with status `skipped_ladder_abandon`. That is not a failure
and must not be counted as one.

This writes one row per engine and level with:
  attempted            yes / no (no = skipped after a lower level failed)
  outcome              success | oom | timeout | error | not_attempted
  elapsed_ms           what the engine actually spent before failing, where it failed
  n_success/n_runs     repetitions that succeeded out of those actually run
  error_text_as_recorded   the message AS RECORDED, matching the JSON field
                       `error_message_as_recorded`. The harness stores `stderr or stdout` capped at
                       2,000 characters by `utils.truncate_error`, so this is recorded output, not
                       full stderr; `error_text_truncated` says when the cap was hit.
  configured_timeout_s / seconds_past_configured_timeout
                       a timeout's elapsed time is not the cap: MonetDB's U256 ran 1,873.985 s
                       against a configured 1,800 s.

Usage:
    python experiments/export_ladder_records.py --sf 100 --out-dir <handoff>/data
"""
from __future__ import annotations

import argparse, csv, glob, json, re, statistics
from pathlib import Path
from typing import Any, Dict, List

REPO = Path(__file__).resolve().parent.parent
LEVEL_RE = re.compile(r"query_(join_J|union_U)(\d+)")

OUTCOME = {"success": "success", "oom": "oom", "timeout_execution": "timeout",
           "timeout_planning": "timeout", "engine_crash": "engine_crash",
           "skipped_ladder_abandon": "not_attempted"}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sf", default="100")
    ap.add_argument("--out-dir", type=Path,
                    default=Path("/home/jvs34/prodds-revision-handoff/data"))
    args = ap.parse_args()
    base = REPO / f".reproduce/sf{args.sf}/results_keyskew"

    rows: List[List[Any]] = []
    evidence: Dict[str, Any] = {}
    for exp, ladder in (("E2", "join"), ("E3", "union")):
        d = base / exp
        if not d.is_dir():
            continue
        for unit in sorted(p for p in d.iterdir() if p.is_dir()):
            engine = unit.name
            raws = sorted(glob.glob(str(unit / "*/*/*/raw.jsonl")))
            if not raws:
                continue
            by: Dict[int, List[Dict[str, Any]]] = {}
            for line in open(raws[-1]):
                r = json.loads(line)
                m = LEVEL_RE.match(r.get("query_id") or "")
                if m:
                    by.setdefault(int(m.group(2)), []).append(r)
            for lvl in sorted(by):
                recs = by[lvl]
                statuses = [r.get("status") for r in recs]
                skipped = all(s == "skipped_ladder_abandon" for s in statuses)
                ok = [r for r in recs if r.get("status") == "success"]
                bad = [r for r in recs if r.get("status") not in ("success", "skipped_ladder_abandon")]
                outcome = ("not_attempted" if skipped else
                           "success" if ok and not bad else
                           OUTCOME.get(bad[0]["status"], bad[0]["status"]) if bad else "unknown")
                elapsed = ""
                if ok:
                    elapsed = f"{statistics.median([r['wall_time_ms_total'] for r in ok]):.0f}"
                elif bad and bad[0].get("wall_time_ms_total") is not None:
                    elapsed = f"{bad[0]['wall_time_ms_total']:.0f}"
                err = (bad[0].get("error_message") or "") if bad else ""
                # A timeout's elapsed time is NOT the configured cap: the harness notices the
                # deadline, signals the process group and waits for it to die, so the recorded
                # execution time runs past the limit. MonetDB's U256 is 1,873.985 s against a
                # configured 1,800 s. Keep both.
                timeout_s = 1800
                over = ""
                if bad and "timeout" in str(bad[0].get("status") or ""):
                    ex = bad[0].get("wall_time_ms_execution")
                    if isinstance(ex, (int, float)):
                        over = f"{ex / 1000 - timeout_s:+.3f}"
                rows.append([f"SF{args.sf}", ladder, engine, lvl,
                             "no" if skipped else "yes", outcome,
                             len(ok), 0 if skipped else len(recs), elapsed,
                             timeout_s if not skipped else "",
                             over,
                             (bad[0].get("return_code") if bad else ""),
                             "yes" if len(err) >= 2000 else "no",
                             err.replace("\n", " ")[:300]])
                # record every real failure, including one whose message is empty: a MonetDB
                # timeout is reaped by the harness and leaves no text, and the status is the
                # evidence. Skipping it would hide a failure.
                if bad:
                    evidence[f"{ladder}_{engine}_{lvl}"] = {
                        "experiment": exp, "engine": engine, "ladder": ladder, "level": lvl,
                        "status": bad[0]["status"], "error_type": bad[0].get("error_type"),
                        "return_code": bad[0].get("return_code"),
                        "wall_time_ms_total": bad[0].get("wall_time_ms_total"),
                        "wall_time_ms_planning": bad[0].get("wall_time_ms_planning"),
                        "wall_time_ms_execution": bad[0].get("wall_time_ms_execution"),
                        "peak_rss_bytes": bad[0].get("peak_rss_bytes"),
                        "error_message_as_recorded": bad[0].get("error_message"),
                        "error_message_truncated": len(bad[0].get("error_message") or "") >= 2000,
                        "error_message_note": ("the harness records `stderr or stdout` capped at "
                                               "2,000 characters by utils.truncate_error; this is "
                                               "recorded output, not full stderr"),
                        "configured_timeout_s": 1800,
                        "note": ("empty message: the harness reaped this run at the timeout, so the "
                                 "engine produced no error text" if not err else ""),
                    }

    args.out_dir.mkdir(parents=True, exist_ok=True)
    out = args.out_dir / f"ladder_records_SF{args.sf}.csv"
    with out.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["scale", "ladder", "engine", "level", "attempted", "outcome",
                    "n_success", "n_runs_attempted", "elapsed_ms",
                    "configured_timeout_s", "seconds_past_configured_timeout",
                    "return_code", "error_text_truncated", "error_text_as_recorded"])
        w.writerows(rows)
    print(f"  wrote {out.name} ({len(rows)} rows)")

    ev = Path("/home/jvs34/prodds-revision-handoff/evidence/engine-failures") / \
        f"ladder_failures_SF{args.sf}.json"
    ev.write_text(json.dumps(evidence, indent=2), encoding="utf-8")
    print(f"  wrote {ev.name} ({len(evidence)} failure records, as recorded)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
