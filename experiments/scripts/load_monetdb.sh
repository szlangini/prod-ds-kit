#!/usr/bin/env bash
set -euo pipefail
set -x

# Environment variables
export PATH="$HOME/opt/monetdb/MonetDB/bin:$PATH"
DATA_DIR="${DATA_DIR:-}"
SF="${SF:-}"
STR="${STR:-10}"
DBNAME="${DBNAME:-tpcds_sf${SF}}"
MONETDB_FARM="${MONETDB_FARM:-$HOME/opt/monetdb/experiment0_patch_farm}"
SCHEMA_FILE="${SCHEMA_FILE:-}"

if [[ -z "${DATA_DIR}" ]]; then
  echo "DATA_DIR is required." >&2
  exit 1
fi

if [[ -z "${SCHEMA_FILE}" ]]; then
  if [[ "${STR}" -le 1 ]]; then
    SCHEMA_FILE="tools/tpcds.sql"
  else
    SCHEMA_FILE="tools/prodds.sql"
  fi
fi

echo "[load_monetdb] Settings:"
echo "  DATA_DIR=${DATA_DIR}"
echo "  DBNAME=${DBNAME}"
echo "  FARM=${MONETDB_FARM}"
echo "  SCHEMA=${SCHEMA_FILE}"

# Ensure monetdbd is running
if ! monetdbd status "${MONETDB_FARM}" >/dev/null 2>&1; then
    echo "[load_monetdb] Starting monetdbd on ${MONETDB_FARM}..."
    monetdbd start "${MONETDB_FARM}" || true
fi

# Threads: physical cores only (paper protocol §6.3 baseline = 56; MonetDB on scan/join-heavy
# columnar work is hurt by SMT oversubscription). Honor THREADS/MONETDB_NTHREADS if exported,
# else auto-detect physical cores (unique Core,Socket pairs); fall back to nproc/2.
NTHREADS="${MONETDB_NTHREADS:-${THREADS:-$(lscpu -p=Core,Socket 2>/dev/null | grep -v '^#' | sort -u | wc -l)}}"
case "$NTHREADS" in ''|*[!0-9]*|0) NTHREADS=$(( $(nproc 2>/dev/null || echo 2) / 2 )) ;; esac
echo "[load_monetdb] nthreads target = ${NTHREADS} (physical cores)"

# Always recreate database
echo "[load_monetdb] Recreating database ${DBNAME}..."
monetdb stop "${DBNAME}" || true
monetdb destroy -f "${DBNAME}" || true
monetdb create "${DBNAME}" || true
# Set nthreads BEFORE the db is started, so mserver5 launches with it (avoids the 112-SMT default).
monetdb set nthreads=${NTHREADS} "${DBNAME}" || echo "[load_monetdb] set nthreads unsupported; using engine default"
monetdb release "${DBNAME}" || true

# Ensure database is started
echo "[load_monetdb] Starting database ${DBNAME}..."
monetdb start "${DBNAME}" || true

# Setup authentication
DOTMONETDBFILE="$(pwd)/.monetdb_load_config"
export DOTMONETDBFILE
echo "user=monetdb" > "${DOTMONETDBFILE}"
echo "password=monetdb" >> "${DOTMONETDBFILE}"
SOCK_PATH="${MONETDB_FARM}/${DBNAME}/.mapi.sock"

# Helper function to run SQL
function run_sql() {
    mclient -l sql -q -h "${SOCK_PATH}" -d "${DBNAME}" -s "$1"
}

function run_file() {
    mclient -l sql -q -h "${SOCK_PATH}" -d "${DBNAME}" -i "$1"
}

# Wait for mapi socket readiness before first SQL command.
for _ in $(seq 1 120); do
  if mclient -l sql -q -h "${SOCK_PATH}" -d "${DBNAME}" -s "select 1;" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

# Apply Schema
echo "[load_monetdb] Applying schema from ${SCHEMA_FILE}..."
run_file "${SCHEMA_FILE}"

# Load Data
shopt -s nullglob
files=( "${DATA_DIR}"/*.dat "${DATA_DIR}"/*.tbl )
if [[ "${#files[@]}" -eq 0 ]]; then
  echo "No data files found in ${DATA_DIR}" >&2
  exit 1
fi

for file in "${files[@]}"; do
  base="$(basename "${file}")"
  table="${base%.*}"
  echo "[load_monetdb] Loading ${table} from ${file}..."
  
  # MonetDB COPY INTO syntax
  # Use absolute path for COPY INTO if possible, or stream stdin?
  # mclient COPY INTO ... FROM STDIN usually works differently or requires client-side protocol
  # Standard COPY INTO <table> FROM '/path/to/file' requires server to read file.
  # Since we are local, absolute path works.
  
  abs_path="$(realpath "${file}")"
  pipe_path="${abs_path}.pipe"
  
  # Create named pipe
  rm -f "${pipe_path}"
  mkfifo "${pipe_path}"
  
  # Stream converted data to pipe in background
  # Use ISO-8859-1 (Latin1) source, UTF-8 destination
  iconv -f ISO-8859-1 -t UTF-8 "${abs_path}" > "${pipe_path}" &
  ICONV_PID=$!
  
  echo "[load_monetdb] Loading ${table} via pipe ${pipe_path}..."
  
  # Load from pipe
  # Note: Removed ENCODING option as we are converting
  run_sql "COPY INTO ${table} FROM '${pipe_path}' USING DELIMITERS '|', '\n', '' NULL AS '' ;"
  
  # Cleanup
  wait ${ICONV_PID} || true
  rm -f "${pipe_path}"
done

echo "[load_monetdb] Data load complete."

# Refresh statistics BEFORE timed execution — paper protocol (§6.3): each engine runs its
# equivalent of ANALYZE. This was missing for MonetDB; without column stats the optimizer
# mis-orders the heavy join-scaling plans (the likely cause of the ~7x join slowdown vs paper).
echo "[load_monetdb] ANALYZE (refresh statistics, paper protocol)..."
run_sql "CALL sys.analyze();" || run_sql "ANALYZE sys;" || echo "[load_monetdb] ANALYZE unavailable; skipped"
echo "[load_monetdb] ANALYZE done."
