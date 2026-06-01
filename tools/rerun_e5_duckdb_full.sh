#!/usr/bin/env bash
set -euo pipefail

REPO="/home/jvs34/prodds-kit"
ART="$REPO/experiments/artifacts/E5"
DATA_CANON="/home/jvs34/data/prodds/sf10"
DATA_RUNS="/home/jvs34/data/prodds/sf10_runs"
DB_ROOT="/home/jvs34/data/duckdb"
PYTHONPATH_RUN="$REPO:${PYTHONPATH:-}"
RUN_TS="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_ROOT_DEFAULT="$DATA_RUNS/$RUN_TS"
RUN_ROOT_OVERRIDE="${RUN_ROOT_OVERRIDE:-}"
LOG_DIR="$ART/rerun_logs"

FORCE_REGEN_DATA="${FORCE_REGEN_DATA:-0}"
FORCE_REWRITE="${FORCE_REWRITE:-0}"
FORCE_REBUILD_DB="${FORCE_REBUILD_DB:-1}"
E5_MODE="${E5_MODE:-data_only}" # data_only | load_only | run_only | full_3phase
BASE_RAW_SOURCE="${BASE_RAW_SOURCE:-/home/jvs34/data/tpcds/sf10}"
STRINGIFY_MAX_WORKERS="${STRINGIFY_MAX_WORKERS:-$(nproc --all)}"
STRINGIFY_BACKEND="${STRINGIFY_BACKEND:-auto}"

choose_run_root() {
  if [[ -n "$RUN_ROOT_OVERRIDE" ]]; then
    echo "$RUN_ROOT_OVERRIDE"
    return
  fi
  if [[ "$E5_MODE" == "load_only" || "$E5_MODE" == "run_only" ]]; then
    local latest
    latest="$(ls -1dt "$DATA_RUNS"/20* 2>/dev/null | head -n1 || true)"
    if [[ -z "$latest" ]]; then
      echo "ERROR no prior run root found under $DATA_RUNS" >&2
      exit 1
    fi
    echo "$latest"
    return
  fi
  echo "$RUN_ROOT_DEFAULT"
}

RUN_ROOT="$(choose_run_root)"
MAIN_LOG="$LOG_DIR/e5_full_rerun_${RUN_TS}.log"
SCHEMA_ROOT="$RUN_ROOT/schemas"

mkdir -p "$LOG_DIR" "$RUN_ROOT" "$DATA_CANON"

log() {
  printf '%s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" | tee -a "$MAIN_LOG"
}

run_cmd() {
  log "cmd=$*"
  "$@" >>"$MAIN_LOG" 2>&1
}

count_dat_files() {
  local d="$1"
  find "$d" -maxdepth 1 -type f -name '*.dat' | wc -l | tr -d ' '
}

clone_base_into_level() {
  local base_dir="$1"
  local target_dir="$2"
  rm -rf "$target_dir"
  mkdir -p "$target_dir"
  if cp -a --reflink=auto "$base_dir/." "$target_dir/" 2>/dev/null; then
    return
  fi
  cp -a "$base_dir/." "$target_dir/"
}

assert_no_data_generation_active() {
  local active
  active="$(pgrep -af "wrap_dsdgen.py|/tools/dsdgen" || true)"
  if [[ -n "${active}" ]]; then
    log "ERROR data generation still active; refusing to start timed run"
    log "active_data_generation_processes:"
    while IFS= read -r line; do
      [[ -n "$line" ]] && log "  $line"
    done <<<"$active"
    exit 1
  fi
}

assert_data_ready_for_run() {
  local s
  for s in 1 2 3 4 5 6 7 8 9 10; do
    local d="$DATA_CANON/str$s"
    local n
    n="$(count_dat_files "$d" 2>/dev/null || echo 0)"
    if [[ "$n" -lt 24 ]]; then
      log "ERROR data for str$s not ready at $d (dat_files=$n)"
      exit 1
    fi
    if [[ ! -f "$d/stringification_data_manifest.json" ]]; then
      log "ERROR missing data manifest for str$s at $d/stringification_data_manifest.json"
      exit 1
    fi
  done
}

assert_schema_ready_for_run() {
  local s
  for s in 1 2 3 4 5 6 7 8 9 10; do
    local sf="$DATA_CANON/_schemas/str$s/tpcds_sf10_str$s.sql"
    if [[ ! -f "$sf" ]]; then
      log "ERROR missing schema for str$s at $sf"
      exit 1
    fi
  done
}

run_data_stage() {
  local base_raw="$RUN_ROOT/base_raw"
  mkdir -p "$base_raw"

  local base_count
  base_count="$(count_dat_files "$base_raw" 2>/dev/null || echo 0)"
  local src_count=0
  if [[ -n "$BASE_RAW_SOURCE" && -d "$BASE_RAW_SOURCE" ]]; then
    src_count="$(count_dat_files "$BASE_RAW_SOURCE" 2>/dev/null || echo 0)"
  fi
  if [[ "$FORCE_REGEN_DATA" == "1" || "$base_count" -lt 24 ]]; then
    if [[ -n "$BASE_RAW_SOURCE" && -d "$BASE_RAW_SOURCE" && "$src_count" -ge 24 ]]; then
      log "prepare_base_raw_from_source source=$BASE_RAW_SOURCE source_dat_files=$src_count dat_files_before=$base_count"
      clone_base_into_level "$BASE_RAW_SOURCE" "$base_raw"
      base_count="$(count_dat_files "$base_raw")"
      log "prepare_base_raw_from_source_done dat_files_after=$base_count"
    else
      log "generate_base_raw sf=10 dat_files_before=$base_count (no valid BASE_RAW_SOURCE)"
      run_cmd bash -lc "cd '$REPO/tools' && ./dsdgen -SCALE 10 -DIR '$base_raw' -FORCE"
      base_count="$(count_dat_files "$base_raw")"
      log "generate_base_raw_done dat_files_after=$base_count"
    fi
    if [[ "$base_count" -lt 24 ]]; then
      log "ERROR incomplete base raw data (dat_files=$base_count)"
      exit 1
    fi
  else
    log "skip_prepare_base_raw dat_files=$base_count"
  fi

  local s
  for s in 1 2 3 4 5 6 7 8 9 10; do
    local out_dir="$RUN_ROOT/str$s"
    local dat_n
    dat_n="$(count_dat_files "$out_dir" 2>/dev/null || echo 0)"
    if [[ "$FORCE_REGEN_DATA" == "1" || "$FORCE_REWRITE" == "1" || "$dat_n" -lt 24 || ! -f "$out_dir/stringification_data_manifest.json" ]]; then
      log "prepare_level_data str=$s dat_files_before=$dat_n"
      clone_base_into_level "$base_raw" "$out_dir"
      run_cmd bash -lc "cd '$REPO' && PYTHONPATH='$PYTHONPATH_RUN' STRINGIFY_BACKEND='$STRINGIFY_BACKEND' STRINGIFY_MAX_WORKERS='$STRINGIFY_MAX_WORKERS' python3 tools/rewrite_str_data_from_base.py --backend '$STRINGIFY_BACKEND' --output-dir '$out_dir' --stringification-level $s"
      local dat_n_after
      dat_n_after="$(count_dat_files "$out_dir")"
      log "prepare_level_data_done str=$s dat_files_after=$dat_n_after"
      if [[ "$dat_n_after" -lt 24 ]]; then
        log "ERROR incomplete data for str$s (dat_files=$dat_n_after)"
        exit 1
      fi
      if [[ ! -f "$out_dir/stringification_data_manifest.json" ]]; then
        log "ERROR missing data manifest for str$s"
        exit 1
      fi
    else
      log "skip_prepare_level_data str=$s dat_files=$dat_n"
    fi
  done

  for s in 1 2 3 4 5 6 7 8 9 10; do
    local target="$RUN_ROOT/str$s"
    local link="$DATA_CANON/str$s"
    ln -sfn "$target" "$link"
    log "symlink_set str=$s link=$link target=$target"
  done
}

run_load_stage() {
  mkdir -p "$SCHEMA_ROOT"
  local s
  for s in 1 2 3 4 5 6 7 8 9 10; do
    local schema_dir="$SCHEMA_ROOT/str$s"
    local schema_file="$schema_dir/tpcds_sf10_str$s.sql"
    mkdir -p "$schema_dir"
    log "generate_schema str=$s schema=$schema_file"
    run_cmd bash -lc "cd '$REPO' && PYTHONPATH='$PYTHONPATH_RUN' python3 tools/generate_tpcds_schema.py --stringification-level $s --out '$schema_file'"

    local db_path="$DB_ROOT/prodds_sf10_str$s.duckdb"
    if [[ "$FORCE_REBUILD_DB" == "1" || ! -f "$db_path" ]]; then
      log "build_duckdb str=$s db=$db_path schema=$schema_file"
      run_cmd bash -lc "cd '$REPO' && DATA_DIR='$DATA_CANON/str$s' SF=10 STR=$s SCHEMA_FILE='$schema_file' DUCKDB_PATH='$db_path' DUCKDB_ALLOW_OVERWRITE=1 ./experiments/scripts/load_duckdb.sh"
    else
      log "skip_build_duckdb str=$s db=$db_path"
    fi
  done
  ln -sfn "$SCHEMA_ROOT" "$DATA_CANON/_schemas"
  log "schema_symlink_set link=$DATA_CANON/_schemas target=$SCHEMA_ROOT"
}

run_benchmark_stage() {
  run_cmd bash -lc "cd '$REPO' && ./tools/preflight_one_engine.sh"
  assert_no_data_generation_active
  assert_data_ready_for_run
  assert_schema_ready_for_run

  log "run_e5_duckdb_start"
  run_cmd bash -lc "cd '$REPO' && PYTHONPATH='$PYTHONPATH_RUN' E5_SCHEMA_ROOT='$DATA_CANON/_schemas' E5_REQUIRE_LEVEL_SCHEMA=1 python3 tools/run_e5_duckdb.py"

  log "consolidate_e5_start"
  run_cmd bash -lc "cd '$REPO' && PYTHONPATH='$PYTHONPATH_RUN' python3 tools/consolidate_and_plot_e5_stringification.py --artifacts-root experiments/artifacts --out-root experiments/artifacts/E5 --engine-order duckdb,cedardb,monetdb,postgres --str-levels 1,2,3,4,5,6,7,8,9,10 --formats pdf,png"
}

log "E5 rerun start"
log "run_root=$RUN_ROOT"
log "force_regen_data=$FORCE_REGEN_DATA force_rewrite=$FORCE_REWRITE force_rebuild_db=$FORCE_REBUILD_DB"
log "mode=$E5_MODE"
log "stringify_max_workers=$STRINGIFY_MAX_WORKERS"
log "stringify_backend=$STRINGIFY_BACKEND"

case "$E5_MODE" in
  data_only)
    run_data_stage
    ;;
  load_only)
    run_load_stage
    ;;
  run_only)
    run_benchmark_stage
    ;;
  full_3phase)
    run_data_stage
    run_load_stage
    run_benchmark_stage
    ;;
  *)
    log "ERROR unsupported E5_MODE=$E5_MODE"
    exit 1
    ;;
esac

log "E5 rerun completed mode=$E5_MODE"
echo "$MAIN_LOG"
