#!/usr/bin/env python3
"""Common success subset and per-engine failure attribution for E1 (workload_compare).

Protocol (docs/experimental-protocol.md, "Timeout Policy" / "E1"): per suite, the common
subset is the set of queries that every audited engine completes successfully in all
timed repetitions; a query is additionally excluded when any engine completes it within
``--margin-s`` seconds of the per-query timeout (near-timeout rule). Every failure is
attributed per engine from the runner's raw records (status / error_type /
error_message) and grouped into coarse cause classes so the exclusions are explainable.

Usage:
    python experiments/common_subset.py --results-dir .reproduce/sf100/results_keyskew \
        [--timeout-s 1800] [--margin-s 60] [--engines duckdb,cedardb,monetdb]

Writes ``<results-dir>/common_subset.json`` (machine-readable, consumed by
``experiments/plot_results.py --common-subset``) and ``common_subset.md`` (the report),
and prints the report. With a single measured engine the subset is provisional; the
report says so.
"""
from __future__ import annotations

import argparse
import json
import re
import statistics
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

SUITES = ("tpcds", "prodds")
ENGINE_ORDER = ("duckdb", "cedardb", "monetdb", "postgres")

# (class label, statuses that map to it, error-text pattern) -- first match wins.
CAUSE_CLASSES: List[Tuple[str, set, str]] = [
    ("out of memory", {"oom"},
     r"out of memory|memory limit|cannot allocate|bad_alloc|OOM"),
    ("timeout", {"timeout"},
     r"timeout|timed out|cancel+ed|statement_timeout|interrupt"),
    ("engine crash", {"engine_crash", "crash"},
     r"server closed|connection (reset|refused|lost|closed)|broken pipe|crash|terminated|segmentation"),
    ("engine resource limit", set(),
     r"cell count|exceeded|too large|result set too big|max_result|limit exceeded|capacity"),
    ("unsupported SQL feature", set(),
     r"not supported|unsupported|not implemented|no such (function|type|operator)|does not exist|"
     r"undefined (function|column|type)|no function matches|cannot (be )?cast|type mismatch|"
     r"not allowed|invalid input|conversion failed|too many|nested aggregate"),
    ("syntax / dialect", set(),
     r"syntax error|parse error|parser|unexpected token|near \""),
]


def classify(status: str, error_type: Optional[str], message: Optional[str]) -> str:
    # The runner's status names (timeout_planning / timeout_execution / oom / engine_crash)
    # take part in the match, so a timeout with an empty message is still a timeout.
    text = f"{status or ''} {error_type or ''} {message or ''}"
    for label, statuses, pattern in CAUSE_CLASSES:
        if status in statuses or re.search(pattern, text, flags=re.IGNORECASE):
            return label
    return "other error"


def query_sort_key(qid: str) -> Tuple[int, int, str]:
    m = re.match(r"query_(\d+)$", qid)
    if m:
        return (0, int(m.group(1)), qid)
    m = re.match(r"query_(join_J|union_U)(\d+)$", qid)
    if m:
        return (1 if m.group(1).startswith("join") else 2, int(m.group(2)), qid)
    return (3, 0, qid)


def latest_raw(run_root: Path) -> Optional[Path]:
    """Latest timestamped run dir under <E1>/<engine>_<suite>/ that has a raw.jsonl."""
    candidates = sorted(run_root.glob("*/*/workload_compare/raw.jsonl"))
    return candidates[-1] if candidates else None


def load_records(path: Path) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return out


def per_query(records: Iterable[Dict[str, Any]], suite: str) -> Dict[str, Dict[str, Any]]:
    """Aggregate the raw runs of one engine into per-query success/failure facts."""
    acc: Dict[str, Dict[str, Any]] = {}
    for rec in records:
        if rec.get("suite") not in (None, suite):
            continue
        if rec.get("experiment_name") not in (None, "workload_compare"):
            continue
        qid = rec.get("query_id")
        if not qid:
            continue
        q = acc.setdefault(qid, {"runs": 0, "success": 0, "success_ms": [], "failures": []})
        q["runs"] += 1
        status = str(rec.get("status") or "error")
        if status == "success":
            q["success"] += 1
            t = rec.get("wall_time_ms_total")
            if isinstance(t, (int, float)):
                q["success_ms"].append(float(t))
        else:
            q["failures"].append({
                "status": status,
                "error_type": rec.get("error_type"),
                "message": (rec.get("error_message") or "")[:300],
                "cause": classify(status, rec.get("error_type"), rec.get("error_message")),
            })
    return acc


def analyse(results_dir: Path, engines: Optional[List[str]], timeout_s: float, margin_s: float,
            experiment: str = "E1") -> Dict[str, Any]:
    e1_dir = results_dir / experiment
    if not e1_dir.is_dir():
        raise SystemExit(f"no {experiment} results under {results_dir}")
    limit_ms = (timeout_s - margin_s) * 1000.0
    report: Dict[str, Any] = {
        "results_dir": str(results_dir),
        "timeout_s": timeout_s,
        "margin_s": margin_s,
        "rule": ("common subset = queries every measured engine completes in all timed repetitions, "
                 "minus queries any engine completes within margin_s of the timeout"),
        "suites": {},
    }
    for suite in SUITES:
        suite_engines: Dict[str, Dict[str, Any]] = {}
        for sub in sorted(e1_dir.iterdir()):
            if not sub.is_dir() or not sub.name.endswith(f"_{suite}"):
                continue
            engine = sub.name[: -len(f"_{suite}")]
            if engines and engine not in engines:
                continue
            raw = latest_raw(sub)
            if raw is None:
                continue
            facts = per_query(load_records(raw), suite)
            if not facts:
                continue
            success_set = {q for q, f in facts.items() if f["runs"] and f["success"] == f["runs"]}
            near = {q: max(f["success_ms"]) / 1000.0 for q, f in facts.items()
                    if f["success_ms"] and max(f["success_ms"]) >= limit_ms}
            failed: Dict[str, Dict[str, Any]] = {}
            for q, f in facts.items():
                if q in success_set:
                    continue
                first = f["failures"][0] if f["failures"] else {"status": "missing", "cause": "other error", "message": ""}
                failed[q] = {
                    "cause": first["cause"],
                    "status": first["status"],
                    "error_type": first.get("error_type"),
                    "message": first["message"],
                    "runs": f["runs"],
                    "failed_runs": len(f["failures"]),
                }
            suite_engines[engine] = {
                "run_dir": str(raw.parent.parent.parent.relative_to(results_dir)),
                "queries": len(facts),
                "success": len(success_set),
                "success_queries": sorted(success_set, key=query_sort_key),
                "failed": dict(sorted(failed.items(), key=lambda kv: query_sort_key(kv[0]))),
                "near_timeout": dict(sorted(near.items(), key=lambda kv: query_sort_key(kv[0]))),
                "median_s_over_success": {
                    q: statistics.median(f["success_ms"]) / 1000.0
                    for q, f in facts.items() if f["success_ms"]
                },
            }
        if not suite_engines:
            continue
        universe = set()
        for info in suite_engines.values():
            universe.update(info["success_queries"])
            universe.update(info["failed"].keys())
        # Common subset = intersection of the per-engine all-reps success sets; a query an
        # engine never ran (e.g. a ladder abandoned after an OOM) is excluded as well.
        common = set.intersection(*[set(info["success_queries"]) for info in suite_engines.values()])
        excluded: Dict[str, Dict[str, Any]] = {}
        for engine, info in suite_engines.items():
            ran = set(info["success_queries"]) | set(info["failed"].keys())
            for q in sorted(universe - ran, key=query_sort_key):
                ex = excluded.setdefault(q, {"reasons": []})
                ex["reasons"].append(f"{engine}: not run (skipped/abandoned by the runner)")
            for q, why in info["failed"].items():
                common.discard(q)
                ex = excluded.setdefault(q, {"reasons": []})
                ex["reasons"].append(f"{engine}: {why['cause']} ({why['status']}, {why['failed_runs']}/{why['runs']} runs)")
            for q, secs in info["near_timeout"].items():
                common.discard(q)
                ex = excluded.setdefault(q, {"reasons": []})
                ex["reasons"].append(f"{engine}: near-timeout ({secs:.0f} s of {timeout_s:.0f} s)")
        report["suites"][suite] = {
            "engines": suite_engines,
            "universe": sorted(universe, key=query_sort_key),
            "common": sorted(common, key=query_sort_key),
            "excluded": dict(sorted(excluded.items(), key=lambda kv: query_sort_key(kv[0]))),
            "provisional": len(suite_engines) < 2,
        }
    return report


def render_markdown(report: Dict[str, Any]) -> str:
    lines: List[str] = []
    lines.append(f"# Common subset — {report['results_dir']}")
    lines.append("")
    lines.append(f"Rule: {report['rule']}. Timeout {report['timeout_s']:.0f} s, margin {report['margin_s']:.0f} s.")
    lines.append("")
    for suite, s in report["suites"].items():
        engines = [e for e in ENGINE_ORDER if e in s["engines"]] + [e for e in s["engines"] if e not in ENGINE_ORDER]
        lines.append(f"## {suite}: common subset {len(s['common'])}/{len(s['universe'])}"
                     + ("  (PROVISIONAL: only one engine measured so far)" if s["provisional"] else ""))
        lines.append("")
        lines.append("| engine | queries | all-reps success | failed | near-timeout | run |")
        lines.append("|---|---|---|---|---|---|")
        for e in engines:
            info = s["engines"][e]
            not_run = len(s["universe"]) - info["queries"]
            lines.append(f"| {e} | {info['queries']}{f' (+{not_run} not run)' if not_run > 0 else ''} | {info['success']} | "
                         f"{len(info['failed'])} | {len(info['near_timeout'])} | `{info['run_dir']}` |")
        lines.append("")
        for e in engines:
            info = s["engines"][e]
            if not info["failed"] and not info["near_timeout"]:
                lines.append(f"- **{e}**: no failures, no near-timeouts.")
                continue
            by_cause: Dict[str, List[str]] = defaultdict(list)
            for q, why in info["failed"].items():
                msg = (why["message"] or "").replace("\n", " ").strip()
                msg = (msg[:140] + "…") if len(msg) > 140 else msg
                by_cause[why["cause"]].append(f"{q} [{why['failed_runs']}/{why['runs']} runs]" + (f": {msg}" if msg else ""))
            lines.append(f"- **{e}** failures by cause:")
            for cause, items in by_cause.items():
                lines.append(f"  - {cause} ({len(items)}):")
                for it in items:
                    lines.append(f"    - {it}")
            if info["near_timeout"]:
                lines.append(f"  - near-timeout ({len(info['near_timeout'])}): "
                             + ", ".join(f"{q} ({secs:.0f} s)" for q, secs in info["near_timeout"].items()))
        if s["excluded"]:
            lines.append("")
            lines.append(f"Excluded from the {suite} common subset ({len(s['excluded'])}):")
            for q, ex in s["excluded"].items():
                lines.append(f"- {q}: " + "; ".join(ex["reasons"]))
        lines.append("")
    return "\n".join(lines) + "\n"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--results-dir", required=True, type=Path,
                    help="results tree holding E1/<engine>_<suite>/<ts>/... (e.g. .reproduce/sf100/results_keyskew)")
    ap.add_argument("--timeout-s", type=float, default=1800.0, help="per-query timeout used in the runs (s)")
    ap.add_argument("--margin-s", type=float, default=60.0, help="near-timeout margin (s), protocol default 60")
    ap.add_argument("--engines", default="", help="comma-separated engines to audit (default: every engine with results)")
    ap.add_argument("--experiment", default="E1",
                    help="results subdirectory holding the <engine>_<suite> audit runs: E0 (audit pass, one run per "
                         "query) or E1 (timed runs); default E1")
    ap.add_argument("--out", type=Path, default=None, help="JSON output (default <results-dir>/common_subset.json)")
    args = ap.parse_args()

    engines = [e.strip() for e in args.engines.split(",") if e.strip()] or None
    report = analyse(args.results_dir, engines, args.timeout_s, args.margin_s, experiment=args.experiment)
    report["experiment"] = args.experiment
    if not report["suites"]:
        print(f"no workload_compare results found under {args.results_dir}/E1", file=sys.stderr)
        sys.exit(1)
    out_json = args.out or (args.results_dir / "common_subset.json")
    out_json.write_text(json.dumps(report, indent=2), encoding="utf-8")
    md = render_markdown(report)
    out_json.with_suffix(".md").write_text(md, encoding="utf-8")
    print(md)
    print(f"[common-subset] wrote {out_json} and {out_json.with_suffix('.md')}")


if __name__ == "__main__":
    main()
