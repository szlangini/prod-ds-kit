#!/usr/bin/env bash
# Reproduce the final cross-benchmark runtime CDF end to end.
#
# Which instance of each benchmark, and the rule that selected its queries, are pinned in
# ../prodds-revision-handoff/docs/10-crossbench-benchmarks.md BEFORE anything is timed. This
# script only executes and summarises; it never chooses queries.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

PY=.venv/bin/python
PASSES="${PASSES:-10}"
TIMEOUT="${TIMEOUT:-300}"

if [ ! -x "$PY" ]; then
  echo "creating .venv with the pinned engine"
  python3 -m venv .venv
  .venv/bin/pip install --quiet 'duckdb==1.4.4' matplotlib numpy
fi

# 1. Freeze the query sets (idempotent; rewrites the same files from the same sources).
#    Skipped when the frozen sets are already present, so a re-run cannot silently reselect.
for d in queries/sqlbarber queries/redbench_krid queries/redbench_wehrstein_reads; do
  [ -f "$d/_selection.json" ] || { echo "MISSING frozen query set: $d" >&2; exit 1; }
done

# 2. Measure. Serial, one engine, one suite at a time.
echo "== measuring: $PASSES timed passes + 1 warmup pass, ${TIMEOUT}s per-query cap =="
$PY run_cdf_final.py --passes "$PASSES" --timeout "$TIMEOUT"

# 3. Summarise and plot.
echo "== summarising =="
$PY make_cdf_figure_final.py

echo "done: figures/cdf_crossbench_final.pdf, results/final/{latencies,query_summary,failures}.csv"
