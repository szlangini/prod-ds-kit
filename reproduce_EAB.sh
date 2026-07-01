#!/usr/bin/env bash
# =============================================================================
#  reproduce_EAB.sh  —  master reproducibility entry point for the Prod-DS paper
#  (VLDB 2027, "Experiments, Analyses & Benchmarks" category).
#
#  Single-entry master script per the PVLDB Reproducibility guidelines
#  (vldb.org/pvldb/reproducibility): it installs the engines, generates all data,
#  runs every experiment from the paper in a sensible order, then renders every
#  figure/table — all from THIS repository, with no author-local paths. It
#  orchestrates the per-experiment reproduce.sh machinery and adds the robustness
#  guards an unattended committee run needs.
#
#  ---- 1. Entry points (per-experiment targets too) --------------------------
#     ./reproduce_EAB.sh all              # full pipeline at the PAPER scales (E1-E3 SF100, E4/E5 SF10) -> figures
#     ./reproduce_EAB.sh --sf100 all      # force EVERY experiment to SF100 (multi-day)
#     ./reproduce_EAB.sh E1|E2|E3|E4|E4X|E5    # one experiment at its paper scale
#     ./reproduce_EAB.sh figures          # (re)render figures+tables from results
#     ./reproduce_EAB.sh clean            # wipe results + artifact (data kept)
#     ./reproduce_EAB.sh clean-data       # also free regenerable variant data (str_sweep+sparsity, all SF)
#     ./reproduce_EAB.sh purge            # full reset: all data+DBs+results+artifact (engine binaries kept)
#     ./reproduce_EAB.sh --quick all      # SMOKE: SF1, 1 rep, subset sweep, 120s timeout
#     ./reproduce_EAB.sh --quick E1       # fastest functional check (~15 min; or 'figures' ~1 min)
#
#  ---- 2. SCALE — per-experiment paper scales by default ---------------------
#     Defaults reproduce the paper: E1-E3 at SF100, E4/E4X/E5 at SF10 (their
#     reported scales). Per-experiment override via SF_E1.. env vars; `--sf100`
#     (alias `--full`) forces every experiment to SF100; `--quick` forces SF1.
#     SF100 variant data is DISK-HEAVY (one stringified variant ~= 0.6 * SF GB
#     ~= 61 GB at SF100); this script generates -> runs -> CLEANS one variant at
#     a time, so peak disk stays ~1 variant (+ its engine DB) instead of all
#     16 (E4) / 4-per-tier (E5) at once. Use --quick for a fast SF1 smoke.
#
#  ---- 3. Hardware tested ----------------------------------------------------
#     2x AMD EPYC 7453 (56 physical cores total), ~1 TiB RAM. Disk: >= 250 GiB
#     free (per-engine + per-variant cleanup keeps disk bounded; without it the
#     interleaved SF100 data would need > 2 TiB on disk at once).
#
#  ---- 4. Software stack (pinned) --------------------------------------------
#     Ubuntu 24.04 (kernel 6.x) · Python 3.12 (./.venv) ·
#     DuckDB 1.4.4 · CedarDB v2026-05-26 · MonetDB 11.55.5
#     (engine binaries fetched/built by `reproduce.sh --init` on first use; all
#      version-pinned: DuckDB + CedarDB from versioned download URLs, MonetDB
#      from a version-locked distro package with a logged best-effort fallback.)
#
#  ---- 5. Expected runtime — tested HW ---------------------------------------
#     The default reproduces the paper scales (E1-E3 SF100) and is multi-day,
#     data-generation dominated. Per-experiment SF100 timings —
#     At REPS=10: E1 ~5h · E2/E3 longer (scaling ladders x10) · E4 ~30-40h
#     (16 variant gens) · E4X ~10h · E5 ~30-40h (12 variant gens) · figures ~5min
#     => the SF100 run is multi-day (data-generation dominated). The --quick path
#     (SF1, REPS=1, subset sweep) completes unattended in a few hours — each variant
#     still runs the full 107-query workload, so it is bounded, not tiny. For a fast
#     functional check use `--quick E1` (~15 min) or `--quick figures` (~1 min).
#
#  ---- 6. Input data ---------------------------------------------------------
#     Generated (not downloaded): TPC-DS dsdgen + Prod-DS generator
#     (wrap_dsdgen.py: stringification / MCV-skew / NULL-sparsity). Idempotent.
#     The cross-benchmark CDF additionally needs small external latency CSVs for
#     the OTHER suites (experiments/data/s7_cdf/); that figure is skipped if absent.
#
#  ---- 7. Measurement protocol / determinism ---------------------------------
#     threads=56 · per-query timeout=1800 s · WARMUP=1 untimed pass + REPS=10
#     timed repetitions, MEDIAN reported. This is the paper protocol (Sec 6.2) and
#     matches REPRODUCIBILITY.md exactly. --quick forces REPS=1. Data-gen is
#     seeded. Committee criterion = BEHAVIORAL agreement (same trends / cliffs /
#     failure modes), not exact milliseconds.
#
#  ---- 8. Outputs (in-repo generator filenames) ------------------------------
#     Filenames are LEGACY and do NOT match the paper's numbering; the paper
#     crosswalk is in REPRODUCIBILITY.md. Experiment -> generated file [paper]:
#     E1 -> fig8a/b/c, fig9/fig9b (CDF), fig10 (errors), cross-bench CDF [Fig 7, Fig 8, Table 4]
#     E2 -> fig11 (join exec+planning, x-failure markers)               [Fig 10]
#     E3 -> fig12 (union pow2 U2..U2048, x-failure markers)             [Fig 11]
#     E4 -> fig13 (STR quantile fan + STRLEN), DuckDB at SF10           [Fig 9]
#     E4X-> fig_str_crossengine (cross-engine stringification; NOT in the paper)
#     E5 -> table3 (skew bars) + table_skew_nullity_tiers.tex          [Table 5]
#     All rendered by IN-REPO generators (experiments/plot_results.py,
#     plot_str_crossengine.py, make_skew_table.py, plot_cdf_crossbench.py) from
#     .reproduce/sf*/results/ into eab_artifact/{figures,tables}/ (PNG + PDF).
#
#  ---- 9. Robustness guards (lessons baked in) ------------------------------
#     * ONE engine at a time; after each engine: stop its server + delete its
#       (reloadable) DBs -> disk bounded, no accumulation.
#     * SF100 E4/E5: ONE data variant at a time (gen -> run -> delete variant
#       data) -> peak ~1 variant on disk.
#     * MonetDB daemons killed before every MonetDB run -> a stray monetdbd from
#       another scale can't hijack 'monetdb create' ("no such database").
#     * Setup -> Run -> Cleanup per unit; idempotent gen; disk-low auto-free.
#     * Continue-and-report: a failed unit is logged, the run continues, a
#       failure summary prints at the end (a non-zero unit can BE the result,
#       e.g. MonetDB timeout / CedarDB OOM).
# =============================================================================
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; cd "$ROOT"
REPRO="$ROOT/reproduce.sh"
PY="$ROOT/.venv/bin/python"; [ -x "$PY" ] || PY="python3"
EAB="$ROOT/eab_artifact"; LOGD="$EAB/logs"; FIGD="$EAB/figures"; TABD="$EAB/tables"
RUN_LOG="$EAB/run_summary.log"
RAW="$ROOT/.reproduce"                                   # namespace: sf<scale>/results/E<exp>/<engine>/<ts>/

ENGINES="${ENGINES:-duckdb cedardb monetdb}"
THREADS="${THREADS:-56}"
TIMEOUT="${TIMEOUT:-1800}"
WARMUP="${WARMUP:-1}"     # paper protocol: 1 untimed warmup pass
REPS="${REPS:-10}"       # paper protocol: 10 timed repetitions (MEDIAN reported)
SF_E1="${SF_E1:-100}"; SF_E2="${SF_E2:-100}"; SF_E3="${SF_E3:-100}"
SF_E4="${SF_E4:-10}"; SF_E4X="${SF_E4X:-10}"; SF_E5="${SF_E5:-10}"   # paper scales: E1-E3 SF100, E4/E4X/E5 SF10. Per-experiment override via SF_Ex env vars; --quick forces SF1.
QUICK=0; FAILS=()

M(){    echo "===== [$(date '+%F %T')] $* =====" | tee -a "$RUN_LOG"; }
note(){ echo "       $*" | tee -a "$RUN_LOG"; }
warn(){ echo "  [WARN] $*" | tee -a "$RUN_LOG"; }
die(){  echo "  [FATAL] $*" | tee -a "$RUN_LOG"; exit 1; }
freeg(){ df -BG --output=avail "$ROOT" 2>/dev/null | tail -1 | tr -dc 0-9; }
scale(){ [ "$QUICK" = 1 ] && { echo 1; return; }; eval "echo \${SF_$1}"; }

# ---- robustness guards ------------------------------------------------------
clean_monetdb_daemons(){ pkill -f '[m]server5' 2>/dev/null||true; pkill -f '[m]onetdbd' 2>/dev/null||true; sleep 2; }
stop_engine(){ case "$1" in monetdb) clean_monetdb_daemons;; cedardb) pkill -f '[c]edardb' 2>/dev/null||true; sleep 1;; esac; }
clean_engine_dbs(){ local sf="$1"
  rm -rf "$RAW/sf${sf}/databases/duckdb/"* 2>/dev/null || true
  rm -rf "$RAW/sf${sf}/databases/cedardb"  2>/dev/null || true
  rm -rf "$RAW/sf${sf}/databases/monetdb/farm/"sparsity_sf* \
         "$RAW/sf${sf}/databases/monetdb/farm/"prodds_sf*   \
         "$RAW/sf${sf}/databases/monetdb/farm/"str_sweep_sf* 2>/dev/null || true
}
free_disk_if_low(){ local need="$1" f; f=$(freeg); f=${f:-9999}
  if [ "$f" -lt "$need" ]; then warn "disk low (${f}G < ${need}G) — cleaning reloadable engine DBs"; clean_engine_dbs 10; clean_engine_dbs 100; fi
}

# ---- one guarded single-engine run: Setup -> Run -> Cleanup ------------------
run_one(){ local exp="$1" eng="$2" sf="$3" t0 t1; local lg="$LOGD/${exp}_sf${sf}_${eng}.log"
  M "RUN ${exp} | ${eng} | SF${sf} | timeout=${TIMEOUT}s warmup=${WARMUP} reps=${REPS}${E5_PROFILE:+ tier=$E5_PROFILE}"
  free_disk_if_low 70; clean_monetdb_daemons
  t0=$(date +%s)
  if "$REPRO" --init --experiment "$exp" --sf "$sf" --engines "$eng" \
        --threads "$THREADS" --timeout "$TIMEOUT" --warmup "$WARMUP" --reps "$REPS" >>"$lg" 2>&1; then
    t1=$(date +%s); note "${exp}/${eng}/SF${sf}: ok ($(( (t1-t0)/60 )) min)"
  else warn "${exp}/${eng}/SF${sf}: non-zero exit (see $lg)"; FAILS+=("${exp}/${eng}/SF${sf}"); fi
  stop_engine "$eng"; clean_engine_dbs "$sf"
}
# free one stringification/sparsity variant's DATA (reloadable; results kept)
free_variant_data(){ rm -rf "$RAW/sf${1}/data/str_sweep/${2}" "$RAW/sf${1}/data/sparsity/${2}" 2>/dev/null || true; }

# ---- experiment phases (sensible order) -------------------------------------
phase_E1(){ M "PHASE E1 — workload TPC-DS vs Prod-DS (fig8/9/9b/10)"
  local s; s=$(scale E1); for e in $ENGINES; do run_one E1 "$e" "$s"; done; }
phase_E2(){ M "PHASE E2 — join scaling J16..J2048 (fig11)"
  local s; s=$(scale E2); for e in $ENGINES; do run_one E2 "$e" "$s"; done; }
phase_E3(){ M "PHASE E3 — UNION ALL fan-in U2..U2048 pow2 (fig12)"
  local s; s=$(scale E3); for e in $ENGINES; do run_one E3 "$e" "$s"; done; }
phase_E4(){ M "PHASE E4 — stringification STR1-10 + STRLEN (fig13, DuckDB)"
  local s; s=$(scale E4); note "SF${s}: one variant at a time (gen->run->clean) to bound disk"
  for str in ${E4_STR:-1 2 3 4 5 6 7 8 9 10}; do
    export E4_LEVELS="$str" E4_STRLEN_LEVELS=""; run_one E4 duckdb "$s"; free_variant_data "$s" "str${str}"; done
  for len in ${E4_LEN:-1 3 5 7 9 10}; do
    export E4_LEVELS="" E4_STRLEN_LEVELS="$len"; run_one E4 duckdb "$s"; free_variant_data "$s" "str5_len${len}"; done
  unset E4_LEVELS E4_STRLEN_LEVELS
  rm -rf "$RAW/sf${s}/data/str_sweep" 2>/dev/null || true; }   # phase-end safety net: clear any variant residue (incl. the str5 base regenerated by STRLEN variants)
phase_E4X(){ M "PHASE E4X — cross-engine stringification (fig_str_crossengine)"
  local s; s=$(scale E4X)
  for str in ${E4X_STR:-1 4 7 10}; do
    export E4_LEVELS="$str"; for e in $ENGINES; do run_one E4X "$e" "$s"; done; free_variant_data "$s" "str${str}"; done
  unset E4_LEVELS; }
phase_E5(){ M "PHASE E5 — sparsity/skew intensity sweep low/medium/high (table3 + tier table)"
  local s; s=$(scale E5)
  if [ "$s" = 100 ] && [ "$(freeg)" -lt 300 ]; then
    warn "E5 @ SF100 wants ~300G free (4 variants/tier x ~61G); have $(freeg)G — per-tier cleanup is on; free disk if a tier aborts."
  fi
  for tier in ${E5_TIERS:-low medium high}; do
    M "  E5 tier=$tier"; export E5_PROFILE="$tier"
    for e in $ENGINES; do run_one E5 "$e" "$s"; done
    for v in baseline sparsity_only skew_only combined; do
      [ "$tier" = medium ] && free_variant_data "$s" "$v" || free_variant_data "$s" "${v}_${tier}"; done
  done; unset E5_PROFILE
  rm -rf "$RAW/sf${s}/data/sparsity" 2>/dev/null || true; }   # phase-end safety net: clear all sparsity variant residue across tiers

# ---- figures (all generators live IN THIS REPO; rendered from results) -------
phase_figures(){ M "PHASE FIGURES — render all figures + tables (in-repo) -> $FIGD / $TABD"; mkdir -p "$FIGD" "$TABD"
  local fl="$LOGD/figures.log"; : > "$fl"
  local sc rdir
  for sc in $(printf '%s\n' "$(scale E1)" "$(scale E2)" "$(scale E3)" "$(scale E4)" "$(scale E4X)" "$(scale E5)" | sort -un); do
    rdir="$RAW/sf${sc}/results"
    [ -d "$rdir" ] || rdir="$ROOT/experiments/data/results/sf${sc}"   # committed archive fallback (fresh clone → render figures with no re-run)
    [ -d "$rdir" ] || continue
    "$PY" experiments/plot_results.py         --results-dir "$rdir" --output-dir "$FIGD" >>"$fl" 2>&1 || warn "plot_results sf${sc}"
    "$PY" experiments/plot_str_crossengine.py --results-dir "$rdir" --output-dir "$FIGD" >>"$fl" 2>&1 || warn "str_crossengine sf${sc}"
    "$PY" experiments/make_skew_table.py      --repo "$ROOT" --sf "$sc" --tiers --out "$TABD/table_skew_nullity.tex" >>"$fl" 2>&1 || warn "skew tier table sf${sc}"
  done
  "$PY" experiments/plot_cdf_crossbench.py --output-dir "$FIGD" >>"$fl" 2>&1 || warn "cross-bench CDF (needs experiments/data/s7_cdf CSVs)"
  note "figures: $(ls "$FIGD"/*.pdf 2>/dev/null|wc -l) PDFs · tables: $(ls "$TABD"/*.tex 2>/dev/null|wc -l)"; }

# ---- main -------------------------------------------------------------------
run_all(){ phase_E1; phase_E2; phase_E3; phase_E4; phase_E4X; phase_E5; phase_figures; }
summary(){ M "RUN COMPLETE — artifact in $EAB"
  if [ "${#FAILS[@]}" -gt 0 ]; then warn "${#FAILS[@]} unit(s) non-zero:"; printf '         - %s\n' "${FAILS[@]}"|tee -a "$RUN_LOG"
    note "(a non-zero unit can BE the result: MonetDB timeout / CedarDB OOM — check its log)"
  else note "all units ok"; fi; }

mkdir -p "$LOGD" "$FIGD" "$TABD"
[ -x "$REPRO" ] || die "reproduce.sh not found/executable at $REPRO"
[ "${1:-}" = "--quick" ] && { QUICK=1; REPS=1; TIMEOUT=120
  E4_STR="${E4_STR:-1 5 10}"; E4_LEN="${E4_LEN:-5}"; E4X_STR="${E4X_STR:-1 10}"; E5_TIERS="${E5_TIERS:-medium}"
  shift; M "QUICK / SMOKE: SF1, reps 1, timeout 120s, subset sweep (E4 STR 1/5/10 +LEN5, E4X 1/10, E5 medium)"; }
case "${1:-}" in --sf100|--full) for v in E1 E2 E3 E4 E4X E5; do eval "SF_$v=100"; done; shift
    M "FULL / PAPER SCALE: SF100 for every experiment (the published configuration; multi-day)";; esac
TARGET="${1:-all}"
M "reproduce_EAB start — target=$TARGET quick=$QUICK engines='$ENGINES' warmup=$WARMUP reps=$REPS scales(E1-5)=$SF_E1/$SF_E2/$SF_E3/$SF_E4/$SF_E5 (free $(freeg)G)"
case "$TARGET" in
  all) run_all ;;
  E1) phase_E1 ;; E2) phase_E2 ;; E3) phase_E3 ;; E4) phase_E4 ;; E4X) phase_E4X ;; E5) phase_E5 ;;
  figures) phase_figures ;;
  clean) M "CLEAN — removing results + artifact (data kept)"; clean_monetdb_daemons; rm -rf "$RAW"/sf*/results "$EAB"; echo "       done"; exit 0 ;;
  clean-data) M "CLEAN-DATA — freeing regenerable variant data (str_sweep + sparsity, all SF; base + results kept)"; rm -rf "$RAW"/sf*/data/str_sweep "$RAW"/sf*/data/sparsity; echo "       done (base tpcds/prodds + results kept; variants regenerated on next --init)"; exit 0 ;;
  purge) M "PURGE — full reset: ALL data + databases + results + artifact (engine binaries kept)"; clean_monetdb_daemons; rm -rf "$RAW"/sf*/data "$RAW"/sf*/databases "$RAW"/sf*/results "$EAB"; echo "       done (everything regenerable; rerun with --init)"; exit 0 ;;
  *) die "unknown target '$TARGET' (valid: all E1 E2 E3 E4 E4X E5 figures clean clean-data purge; prefix --quick)" ;;
esac
summary
