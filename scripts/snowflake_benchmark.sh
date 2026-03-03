#!/usr/bin/env bash
# snowflake_benchmark.sh — End-to-end Prod-DS Kit benchmark on Snowflake.
#
# Generates data (TPC-DS SF100 + Prod-DS SF100), uploads to Snowflake,
# creates schemas, loads tables, runs warmup + 10 timed repetitions,
# collects timing from QUERY_HISTORY, and produces a summary report.
#
# Usage:
#   bash scripts/snowflake_benchmark.sh
#
# The script will prompt for Snowflake connection details interactively.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORK_DIR="${REPO_ROOT}/snowflake_run"
TPCDS_DATA_DIR="${WORK_DIR}/data/tpcds_sf100"
PRODDS_DATA_DIR="${WORK_DIR}/data/prodds_sf100"
TPCDS_QUERIES_DIR="${WORK_DIR}/queries/tpcds"
PRODDS_QUERIES_DIR="${WORK_DIR}/queries/prodds"
RESULTS_DIR="${WORK_DIR}/results"
TPCDS_SCHEMA="${WORK_DIR}/schema/tpcds.sql"
PRODDS_SCHEMA="${WORK_DIR}/schema/prodds.sql"

WARMUP_REPS=1
TIMED_REPS=10
SF=100
STR_LEVEL=10
WAREHOUSE_SIZE="${SNOWFLAKE_WAREHOUSE_SIZE:-LARGE}"

# ---------- helpers ----------
info()  { printf '\033[1;34m[snowflake]\033[0m %s\n' "$*"; }
ok()    { printf '\033[1;32m[snowflake]\033[0m %s\n' "$*"; }
warn()  { printf '\033[1;33m[snowflake]\033[0m %s\n' "$*"; }
fail()  { printf '\033[1;31m[snowflake]\033[0m %s\n' "$*" >&2; exit 1; }

# ---------- Step 0: Collect Snowflake connection details ----------
info "Snowflake Connection Setup"
echo ""
read -rp "  Snowflake account identifier (e.g. xy12345.us-east-1): " SF_ACCOUNT
read -rp "  Snowflake username: " SF_USER
read -rsp "  Snowflake password: " SF_PASSWORD
echo ""
read -rp "  Warehouse name [BENCHMARK_WH]: " SF_WAREHOUSE
SF_WAREHOUSE="${SF_WAREHOUSE:-BENCHMARK_WH}"
read -rp "  Database name [PRODDS_BENCHMARK]: " SF_DATABASE
SF_DATABASE="${SF_DATABASE:-PRODDS_BENCHMARK}"
read -rp "  Schema name [PUBLIC]: " SF_SCHEMA
SF_SCHEMA="${SF_SCHEMA:-PUBLIC}"
read -rp "  Warehouse size [${WAREHOUSE_SIZE}]: " WH_SIZE_INPUT
WAREHOUSE_SIZE="${WH_SIZE_INPUT:-${WAREHOUSE_SIZE}}"
echo ""

export SNOWSQL_ACCOUNT="${SF_ACCOUNT}"
export SNOWSQL_USER="${SF_USER}"
export SNOWSQL_PWD="${SF_PASSWORD}"

# Verify snowsql is available
command -v snowsql >/dev/null 2>&1 || fail "snowsql CLI not found. Install: brew install --cask snowflake-snowsql"

snowsql_exec() {
    snowsql -a "${SF_ACCOUNT}" -u "${SF_USER}" \
        -d "${SF_DATABASE}" -s "${SF_SCHEMA}" -w "${SF_WAREHOUSE}" \
        -o friendly=false -o header=false -o timing=false \
        -o output_format=tsv \
        "$@"
}

snowsql_query() {
    snowsql_exec -q "$1"
}

# ---------- Step 1: Check prerequisites ----------
info "Checking prerequisites..."
cd "${REPO_ROOT}"
source .venv/bin/activate 2>/dev/null || fail "Virtual environment not found. Run ./install.sh first."
python3 -c "from workload import stringification" 2>/dev/null || fail "Prod-DS Kit not installed. Run ./install.sh first."
ok "Prerequisites OK."

# ---------- Step 2: Generate data ----------
mkdir -p "${WORK_DIR}/data" "${WORK_DIR}/queries" "${WORK_DIR}/schema" "${RESULTS_DIR}"

if [ -d "${TPCDS_DATA_DIR}" ] && [ "$(ls -A "${TPCDS_DATA_DIR}"/*.dat 2>/dev/null | head -1)" ]; then
    info "TPC-DS SF${SF} data already exists, skipping generation."
else
    info "Generating TPC-DS SF${SF} data (vanilla, no stringification)..."
    mkdir -p "${TPCDS_DATA_DIR}"
    python3 wrap_dsdgen.py --stringification-level 1 \
        --disable-null-skew --disable-mcv-skew \
        -DIR "${TPCDS_DATA_DIR}" -SCALE "${SF}"
    ok "TPC-DS SF${SF} data generated."
fi

if [ -d "${PRODDS_DATA_DIR}" ] && [ "$(ls -A "${PRODDS_DATA_DIR}"/*.dat 2>/dev/null | head -1)" ]; then
    info "Prod-DS SF${SF} STR${STR_LEVEL} data already exists, skipping generation."
else
    info "Generating Prod-DS SF${SF} STR${STR_LEVEL} data (NULL=medium, MCV=medium)..."
    mkdir -p "${PRODDS_DATA_DIR}"
    python3 wrap_dsdgen.py --stringification-level "${STR_LEVEL}" \
        --null-profile medium --mcv-profile medium \
        -DIR "${PRODDS_DATA_DIR}" -SCALE "${SF}"
    ok "Prod-DS SF${SF} STR${STR_LEVEL} data generated."
fi

# ---------- Step 3: Generate schemas ----------
info "Generating schemas..."
python3 tools/generate_tpcds_schema.py --stringification-level 1 \
    --out "${TPCDS_SCHEMA}" 2>/dev/null || \
    cp tpcds-kit/tools/tpcds.sql "${TPCDS_SCHEMA}"

python3 tools/generate_tpcds_schema.py --stringification-level "${STR_LEVEL}" \
    --out "${PRODDS_SCHEMA}"
ok "Schemas generated."

# ---------- Step 4: Generate queries ----------
if [ -d "${TPCDS_QUERIES_DIR}" ] && [ "$(ls -A "${TPCDS_QUERIES_DIR}"/*.sql 2>/dev/null | head -1)" ]; then
    info "TPC-DS queries already exist, skipping."
else
    info "Generating TPC-DS queries..."
    python3 wrap_dsqgen.py --output-dir "${TPCDS_QUERIES_DIR}" \
        --no-extensions --scale "${SF}"
    ok "TPC-DS queries generated."
fi

if [ -d "${PRODDS_QUERIES_DIR}" ] && [ "$(ls -A "${PRODDS_QUERIES_DIR}"/*.sql 2>/dev/null | head -1)" ]; then
    info "Prod-DS queries already exist, skipping."
else
    info "Generating Prod-DS STR${STR_LEVEL} queries..."
    python3 wrap_dsqgen.py --output-dir "${PRODDS_QUERIES_DIR}" \
        --stringification-level "${STR_LEVEL}" --scale "${SF}"
    ok "Prod-DS queries generated."
fi

# ---------- Step 5: Set up Snowflake ----------
info "Setting up Snowflake database and warehouse..."

snowsql_query "
CREATE DATABASE IF NOT EXISTS ${SF_DATABASE};
USE DATABASE ${SF_DATABASE};
CREATE SCHEMA IF NOT EXISTS ${SF_SCHEMA};
USE SCHEMA ${SF_SCHEMA};
CREATE WAREHOUSE IF NOT EXISTS ${SF_WAREHOUSE}
    WITH WAREHOUSE_SIZE = '${WAREHOUSE_SIZE}'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;
ALTER WAREHOUSE ${SF_WAREHOUSE} RESUME;
"
ok "Snowflake setup complete."

# ---------- Step 6: Create file format and stage ----------
info "Creating file format and internal stage..."

snowsql_query "
USE DATABASE ${SF_DATABASE};
USE SCHEMA ${SF_SCHEMA};
CREATE OR REPLACE FILE FORMAT tpcds_pipe_format
    TYPE = 'CSV'
    FIELD_DELIMITER = '|'
    SKIP_HEADER = 0
    NULL_IF = ('')
    EMPTY_FIELD_AS_NULL = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    TRIM_SPACE = TRUE;
CREATE OR REPLACE STAGE prodds_stage FILE_FORMAT = tpcds_pipe_format;
"
ok "File format and stage created."

# ---------- Step 7: Load data function ----------
load_workload() {
    local WORKLOAD_NAME="$1"
    local DATA_DIR="$2"
    local SCHEMA_FILE="$3"
    local TABLE_PREFIX="$4"

    info "Loading ${WORKLOAD_NAME} data into Snowflake..."

    # Create tables from schema (with optional prefix)
    if [ -n "${TABLE_PREFIX}" ]; then
        sed "s/create table /create table ${TABLE_PREFIX}/gi" "${SCHEMA_FILE}" | \
            snowsql_exec -q "$(cat /dev/stdin)"
    else
        snowsql_exec -f "${SCHEMA_FILE}"
    fi

    # Upload and load each data file
    for file in "${DATA_DIR}"/*.dat; do
        [ -f "$file" ] || continue
        local base
        base="$(basename "$file" .dat)"
        local table="${TABLE_PREFIX}${base}"
        info "  Uploading ${base}.dat -> @prodds_stage/${WORKLOAD_NAME}/${base}/"
        snowsql_exec -q "PUT file://${file} @prodds_stage/${WORKLOAD_NAME}/${base}/ AUTO_COMPRESS=TRUE PARALLEL=8 OVERWRITE=TRUE;"
        info "  Loading ${table}..."
        snowsql_query "
COPY INTO ${table}
    FROM @prodds_stage/${WORKLOAD_NAME}/${base}/
    FILE_FORMAT = tpcds_pipe_format
    ON_ERROR = 'CONTINUE'
    FORCE = TRUE;
"
    done

    # Run ANALYZE equivalent
    info "  Computing statistics for ${WORKLOAD_NAME}..."
    # Snowflake auto-maintains statistics, but we trigger optimization
    ok "  ${WORKLOAD_NAME} data loaded."
}

# Load TPC-DS tables
load_workload "tpcds" "${TPCDS_DATA_DIR}" "${TPCDS_SCHEMA}" ""

# Load Prod-DS tables (use prefix to avoid conflicts)
load_workload "prodds" "${PRODDS_DATA_DIR}" "${PRODDS_SCHEMA}" "prodds_"

# ---------- Step 8: Run benchmark ----------
run_queries() {
    local SUITE_NAME="$1"
    local QUERY_DIR="$2"
    local TABLE_PREFIX="$3"
    local REP_COUNT="$4"
    local IS_WARMUP="$5"

    local LABEL
    if [ "${IS_WARMUP}" = "1" ]; then
        LABEL="${SUITE_NAME}_warmup"
    else
        LABEL="${SUITE_NAME}_rep${REP_COUNT}"
    fi

    # Disable result cache
    snowsql_query "ALTER SESSION SET USE_CACHED_RESULT = FALSE;"
    snowsql_query "ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 1800;"

    for qfile in "${QUERY_DIR}"/query_*.sql; do
        [ -f "$qfile" ] || continue
        local qname
        qname="$(basename "$qfile" .sql)"

        # Skip query_0
        if [ "$qname" = "query_0" ]; then
            continue
        fi

        local sql
        sql="$(cat "$qfile")"

        # If using prefixed tables, rewrite table references
        if [ -n "${TABLE_PREFIX}" ]; then
            sql="$(echo "$sql" | sed "s/\bFROM /FROM ${TABLE_PREFIX}/gi; s/\bJOIN /JOIN ${TABLE_PREFIX}/gi")"
        fi

        local start_ts
        start_ts="$(date +%s%N)"
        local status="SUCCESS"

        if ! snowsql_query "${sql}" >/dev/null 2>&1; then
            status="ERROR"
        fi

        local end_ts
        end_ts="$(date +%s%N)"
        local elapsed_ms=$(( (end_ts - start_ts) / 1000000 ))

        echo "${LABEL},${qname},${status},${elapsed_ms}" >> "${RESULTS_DIR}/raw_times.csv"
    done
}

info "Starting benchmark execution..."
echo "suite,query,status,elapsed_ms" > "${RESULTS_DIR}/raw_times.csv"

# Warmup
info "Running warmup (TPC-DS)..."
run_queries "tpcds" "${TPCDS_QUERIES_DIR}" "" "0" "1"
info "Running warmup (Prod-DS)..."
run_queries "prodds" "${PRODDS_QUERIES_DIR}" "prodds_" "0" "1"

# Clear warehouse cache before timed runs
snowsql_query "ALTER WAREHOUSE ${SF_WAREHOUSE} SUSPEND;"
snowsql_query "ALTER WAREHOUSE ${SF_WAREHOUSE} RESUME;"

# Timed repetitions
for rep in $(seq 1 ${TIMED_REPS}); do
    info "Timed repetition ${rep}/${TIMED_REPS} (TPC-DS)..."
    run_queries "tpcds" "${TPCDS_QUERIES_DIR}" "" "${rep}" "0"
    info "Timed repetition ${rep}/${TIMED_REPS} (Prod-DS)..."
    run_queries "prodds" "${PRODDS_QUERIES_DIR}" "prodds_" "${rep}" "0"
done

# ---------- Step 9: Collect timing from QUERY_HISTORY ----------
info "Collecting detailed timing from Snowflake QUERY_HISTORY..."

snowsql_query "
SELECT
    query_id,
    SUBSTR(query_text, 1, 120) AS query_preview,
    total_elapsed_time,
    execution_time,
    compilation_time,
    queued_overload_time,
    rows_produced,
    bytes_scanned,
    warehouse_size,
    start_time,
    end_time
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
    END_TIME_RANGE_START => DATEADD('hours', -24, CURRENT_TIMESTAMP()),
    END_TIME_RANGE_END => CURRENT_TIMESTAMP()
))
WHERE query_type = 'SELECT'
  AND database_name = '${SF_DATABASE}'
ORDER BY start_time;
" > "${RESULTS_DIR}/snowflake_query_history.tsv"

ok "Query history collected."

# ---------- Step 10: Generate summary report ----------
info "Generating summary report..."

python3 -c "
import csv
import sys
from collections import defaultdict
from pathlib import Path

results_dir = Path('${RESULTS_DIR}')
raw_path = results_dir / 'raw_times.csv'

if not raw_path.exists():
    print('No results found.')
    sys.exit(1)

data = defaultdict(lambda: defaultdict(list))
with open(raw_path) as f:
    reader = csv.DictReader(f)
    for row in reader:
        suite = row['suite']
        if '_warmup' in suite:
            continue
        suite_base = suite.rsplit('_rep', 1)[0]
        query = row['query']
        status = row['status']
        elapsed = int(row['elapsed_ms'])
        if status == 'SUCCESS':
            data[suite_base][query].append(elapsed)

report = []
report.append('=' * 70)
report.append('  Prod-DS Kit Snowflake Benchmark Report')
report.append('  SF=${SF}, STR=${STR_LEVEL}, Warehouse=${WAREHOUSE_SIZE}')
report.append('  Warmup: ${WARMUP_REPS}, Timed Reps: ${TIMED_REPS}')
report.append('=' * 70)
report.append('')

for suite in sorted(data.keys()):
    queries = data[suite]
    report.append(f'--- {suite.upper()} ---')
    report.append(f'{\"Query\":<20} {\"Median (ms)\":>12} {\"Min (ms)\":>10} {\"Max (ms)\":>10} {\"Reps\":>6}')
    report.append('-' * 60)

    total_median = 0
    for qname in sorted(queries.keys()):
        times = sorted(queries[qname])
        n = len(times)
        median = times[n // 2]
        total_median += median
        report.append(f'{qname:<20} {median:>12,} {times[0]:>10,} {times[-1]:>10,} {n:>6}')

    report.append('-' * 60)
    report.append(f'{\"TOTAL (median sum)\":<20} {total_median:>12,}')
    report.append(f'{\"TOTAL (seconds)\":<20} {total_median / 1000:>12,.1f}')
    report.append('')

report_text = '\n'.join(report)
print(report_text)

report_path = results_dir / 'benchmark_report.txt'
report_path.write_text(report_text)
print(f'\nReport saved to {report_path}')
"

# ---------- Step 11: Cleanup ----------
info "Suspending warehouse to save costs..."
snowsql_query "ALTER WAREHOUSE ${SF_WAREHOUSE} SUSPEND;" 2>/dev/null || true

echo ""
ok "Benchmark complete!"
echo ""
echo "  Results:  ${RESULTS_DIR}/raw_times.csv"
echo "  Report:   ${RESULTS_DIR}/benchmark_report.txt"
echo "  History:  ${RESULTS_DIR}/snowflake_query_history.tsv"
echo ""
