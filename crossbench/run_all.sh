#!/usr/bin/env bash
# Reproduce the cross-benchmark runtime CDF pilot end to end.
#
#   ./run_all.sh              # measure, then render figure + tables
#   ./run_all.sh --figure     # re-render from the existing results only
#
# The virtual environment pins DuckDB to 1.4.4, the version the paper's engine experiments
# use. Create it once with:
#   python3 -m venv .venv && .venv/bin/pip install duckdb==1.4.4 matplotlib
set -euo pipefail
cd "$(dirname "$0")"
PY=.venv/bin/python
[ -x "$PY" ] || { echo "create the venv first: python3 -m venv .venv && .venv/bin/pip install duckdb==1.4.4 matplotlib" >&2; exit 1; }
if [ "${1:-}" != "--figure" ]; then
  "$PY" run_cdf_pilot.py --timeout 300 --out-dir results
fi
"$PY" make_cdf_figure.py --results results --out-dir figures
echo "figures/ and results/ are up to date"
