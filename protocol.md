> Paths in this document use environment variables. See install.sh for setup.

# Prod-DS Experimental Protocol Freeze

Status: frozen draft for supervisor review (2026-02-11)  
Mode: mechanism-first, reproducible, one-engine-at-a-time

## Sources Of Truth
- `results/20260211T123144Z_experiment0_patch_monetdb/protocol_manifest.json`
- `results/20260211T123144Z_experiment0_patch_monetdb/engines_versions.json`
- `results/20260211T123144Z_experiment0_patch_monetdb/threads.json`
- `results/20260211T123144Z_experiment0_patch_monetdb/engines_control.md`
- `results/20260211T123144Z_experiment0_patch_monetdb/queries_inventory.md`
- `results/20260211T123144Z_experiment0_patch_monetdb/jan_TODO.md`
- `results/20260211T125633Z_preflight_guard_patch/guard_integration_notes.md`
- `results/20260211T125633Z_preflight_guard_patch/repo_data_cleanup.md`
- `results/20260211T125633Z_preflight_guard_patch/guard_validation_postgres_only.txt`
- `results/20260211T125633Z_preflight_guard_patch/guard_validation_postgres_plus_monetdb.txt`
- `results/20260211T125633Z_preflight_guard_patch/guard_validation_all_stopped.txt`

## Extensions (What Changed)
- Scope finalized to: `duckdb`, `postgres`, `cedardb`, `monetdb` (ClickHouse removed from benchmark scope).
- Temporary execution override (effective `2026-02-12`): `postgres` is paused/excluded due to runtime constraints and will be reintroduced for final evaluation. Active execution scope during this override: `duckdb`, `cedardb`, `monetdb`.
- Old policy removed:
  - fixed `32` threads baseline
  - fixed `512 GiB` memory cap baseline
  - fixed `30 min` timeout baseline
  - ClickHouse-specific benchmark settings
- One-engine-at-a-time moved from guideline to hard preflight enforcement via `tools/preflight_one_engine.sh`.
- In-repo duplicate data under `experiments/plan_runs/**/data` removed and forbidden by config/runtime validation.
- Union scaling level `1` deprecated and excluded from execution.

## Verification (Did Changes Work)
- Guard pass/fail behavior is evidenced:
  - pass with only Postgres active
  - fail with Postgres + MonetDB active (exit code `1`)
  - pass with all engines stopped
- Repo data cleanup evidenced:
  - before: `77` files, `1.7G`
  - after: `0` files, `1.0K`
- Config path validator rejects `experiments/plan_runs/**/data` roots.

## Environment (Final)
- OS: `Ubuntu 24.04.4 LTS` (accepted final)
- Kernel: `6.14.0-37-generic`
- CPU: `2 x AMD EPYC 7453`, `28` cores/socket, SMT `2`
- Logical CPUs: `112`
- RAM: `~1.0 TiB` (`Mem: 1.0Ti`)

## Engines (Final Scope + Pins)
- DuckDB: `v1.4.4 (Andium) 6ddac802ff` (`/usr/local/bin/duckdb`)
- PostgreSQL: `16.11` (`/usr/bin/psql`)
- CedarDB: `v2026-02-03 (2026-01-29, format 0)` (`/usr/local/bin/cedardb`)
- MonetDB: `11.55.1 (Dec2025)` (`/usr/bin/mserver5`, control `/usr/bin/monetdbd`)

## Data Policy
- Authoritative roots (absolute):
  - `$DATA_ROOT/tpcds/sf10`
  - `$DATA_ROOT/prodds/sf10/str1`
  - `$DATA_ROOT/prodds/sf10/str10`
- SF100 campaign roots (2026-02-27 refresh):
  - `$DATA_ROOT/prodds/sf100/str1` (TPC-DS baseline + PROD-DS STR1)
  - `$DATA_ROOT/prodds/sf100/str10`
- No data copying into repo.
- Any run/config path under `experiments/plan_runs/**/data` is invalid and must abort.

## Query Policy
- Canonical query root namespace: `$QUERY_DIR/...`
- Variants root namespace: `experiments/queries/<engine>/<suite>/...`
- Mapping file: `experiments/queries/query_mapping.yaml`
- Never execute `query_0.sql`.
- Scaling query locations:
  - Join: `$QUERY_DIR/prodds/generators/join`
  - Union: `$QUERY_DIR/prodds/generators/union`
- Scaling levels:
  - Join active: `[1,2,4,8,16,32,64,128,256,512,1024,2048]`
  - Union active: `[2,4,8,16,32,64,128,256,512,1024,2048]`
  - Union `1`: deprecated, excluded from execution

## Interim E1 Freeze (3 Engines)
- Freeze scope engines: `duckdb`, `cedardb`, `monetdb` (`postgres` paused for active execution).
- Frozen subset artifacts:
  - `experiments/artifacts/E1/common_subset_3engines/tpcds_sf10.json`
  - `experiments/artifacts/E1/common_subset_3engines/prodds_sf10_str10.json`
- TPCDS freeze rule:
  - include all `query_1.sql..query_99.sql` except `query_3.sql`
  - `query_0.sql` always excluded by protocol
  - `query_3.sql` excluded due MonetDB runtime conversion error
- ProdDS freeze rule:
  - include only 3-engine `SUCCESS` intersection (DuckDB + CedarDB + MonetDB rerun)
  - union/other non-common queries excluded with reason captured in the subset JSON
- Seed freeze artifacts:
  - shared seed overrides: `experiments/artifacts/E1/seeds/seed_overrides_sf10_str10.yml`
  - non-empty audit (frozen ProdDS subset, all 3 engines): `experiments/artifacts/E1/seeds/nonempty_audit_prodds_subset_3eng_summary.json`
  - result: no additional seed overrides required for the frozen subset

## Execution Model
- Single node.
- Single-query serial execution (no concurrent query workers).
- Exactly one engine active at a time, enforced by mandatory preflight script:
  - `tools/preflight_one_engine.sh`
- Warmup: `1` full pass.
- Timed repetitions: `5`.

## Statistics Collection Policy (Mandatory)
- Before any timed workload execution (including audit and calibration runs), a full statistics refresh MUST be executed on all tables.
- This phase is mandatory for optimizer fairness and cross-engine comparability.
- Per engine:
  - PostgreSQL:
    - command: `ANALYZE;`
    - execute once after data load and before any timing
    - enforce `jit=off`
  - DuckDB:
    - command: `ANALYZE;` (or `PRAGMA analyze;`)
    - execute explicitly after bulk load even if some stats are auto-maintained
  - MonetDB:
    - command: `ANALYZE;` (or `CALL sys.analyze();`)
    - execute explicitly before timing
  - CedarDB:
    - `ANALYZE;` is accepted through PostgreSQL wire compatibility
    - CedarDB uses adaptive statistics internally
    - still execute `ANALYZE;` for protocol symmetry (may be operationally redundant)
- Clarifications:
  - this is not `EXPLAIN ANALYZE`
  - statistics collection is not timed
  - completion of statistics collection must be logged
- All engines execute an explicit ANALYZE phase prior to workload timing, even if internally adaptive.

## Threading Policy
- Baseline mode (for E2 and default runs): use physical cores only, `56` threads/workers.
- Optional sensitivity mode: use logical CPUs, `112` threads/workers.
- DuckDB:
  - baseline: `PRAGMA threads=56;`
  - optional full-core: `PRAGMA threads=112;`
  - verify: `SELECT current_setting('threads');`
- PostgreSQL:
  - baseline session: `SET max_parallel_workers_per_gather=56; SET max_parallel_workers=56;`
  - baseline postmaster: `ALTER SYSTEM SET max_worker_processes=56;` (restart required)
  - optional full-core session: `SET max_parallel_workers_per_gather=112; SET max_parallel_workers=112;`
  - optional full-core postmaster: `ALTER SYSTEM SET max_worker_processes=112;` (restart required)
  - verify: `SHOW max_parallel_workers_per_gather; SHOW max_parallel_workers; SHOW max_worker_processes;`
- MonetDB:
  - baseline: `monetdb set nthreads=56 <db>`
  - optional full-core: `monetdb set nthreads=112 <db>`
  - verify target: `monetdb get nthreads <db>`
  - note: verification can require stable daemon/db state
- CedarDB:
  - target setting baseline: `parallel="56"` (or env equivalent)
  - optional full-core: `parallel="112"` (or env equivalent)
  - status: delegated pending vendor-authoritative controls and verification

## E2 Fast-Track Mode
- Active E2 scope: `duckdb`, `cedardb`, `monetdb` (PostgreSQL temporarily excluded due to runtime constraints).
- Timeout: fixed `1800s` per query.
- Common subset sources:
  - `experiments/artifacts/E1/common_subset_3engines/tpcds_sf10.json`
  - `experiments/artifacts/E1/common_subset_3engines/prodds_sf10_str10.json`
- Fast subset rule:
  - query is in fast subset iff it is in common subset and median execution time is `< 60s` on all three active engines
  - fast subset is an analysis slice and does not replace full common-subset evaluation
- Metric scope for E2:
  - Primary: end-to-end workload time, per-query execution time
  - Deferred to E3: join scaling, planning-time instrumentation
- Deep-dive engine for planning-time instrumentation in later phases: `DuckDB`

## E3 Join Scaling Micro-Suite
- Active E3 scope: `duckdb`, `cedardb`, `monetdb` (PostgreSQL temporarily excluded due to runtime constraints).
- Query root: `$QUERY_DIR/prodds/generators/join`
- Join levels: `1,2,4,8,16,32,64,128,256,512,1024,2048`
- Join-generator calibration is pinned in `workload/config/returns.yml::target_overrides` for all active levels.
  - Generator model is `J(k,m)=(m+1)*(b+k*(b+1))+m` with inline base-block replication.
  - With fixed base width `b=10`, very low labels (`J < 10`) are not exactly reachable and map to nearest calibrated overrides.
  - Canonical refresh command for `join_<J>.sql` files:
    - `python3 tools/regenerate_join_scaling_queries.py --levels 1,2,4,8,16,32,64,128,256,512,1024,2048`
  - E3/E7 runners must validate generated header metadata (`STRATEGY=target_override`, `k`, `m`) against `target_overrides` before timing/analysis.
- Threading: baseline `56` (physical cores), optional sensitivity `112` (logical CPUs).
- Timeout: fixed `1800s`.
- Planning collection policy:
  - DuckDB (required deep-dive): `EXPLAIN (ANALYZE, FORMAT JSON)` artifacts + planning proxy via `EXPLAIN` wall-clock.
  - CedarDB/MonetDB (best-effort): planning proxy via `EXPLAIN` wall-clock only.
- Derived metric:
  - `planning_ratio = planning_time_ms / total_time_ms`
- Canonical artifacts:
  - `experiments/artifacts/E3/overview.md`
  - `experiments/artifacts/E3/merged/per_level_medians.csv`
  - `experiments/artifacts/E3/join_levels.json`
  - `experiments/artifacts/E3/checksums_join_queries.tsv`

## Timeout And Subset Policy
- `T` is intentionally not numeric yet.
- E1 freezes `T` with rule: `T = max_successful_calibration_runtime + 60s`.
- Near-timeout exclusion rule:
  - if any audited engine completes a query within `60s` of `T`, exclude that query from common subset.
- E1 output must include:
  - `results/<ts>/common_subset.md` with columns:
    - query name
    - included (`yes/no`)
    - exclusion reason

## Error Taxonomy
- Primary status: `SUCCESS`, `TIMEOUT`, `ERROR`
- `ERROR` subclasses:
  - `OOM`
  - `FAILURE`
  - `DIALECT`
  - `PARSE`
  - `UNKNOWN` (fallback only if classification fails)

## Artifact Contract (Per Run)
- Required:
  - `raw_times.csv`
  - `failures.csv`
  - `audit.json`
  - `run_manifest.json`
  - `environment_snapshot.md`
  - `effective_config.json`
- Effective config capture requirements:
  - PostgreSQL: capture `SHOW ALL` output
  - DuckDB: capture version + effective PRAGMAs used
  - MonetDB: try `monetdb get all <db>`; if unavailable, capture `monetdb status`, `monetdb get nthreads`, and record limitation
  - CedarDB: capture effective config dump if available, else record delegated gap

## CedarDB Vendor Questions (Maximillian Bandle)
1. What is the authoritative method to set `parallel` at runtime vs static config file?
2. What is the authoritative method to verify effective parallelism during a running query?
3. What is the authoritative method to dump effective CedarDB configuration (all parameters)?

## Evaluation (System Response Plan)
- `E0` Setup (completed):
  - environment verification, install verification, scaling query materialization, manifests
- `E1` Audit + Timeout freeze (required):
  - run audit with provisional timeout process
  - freeze `T`
  - compute and publish `common_subset.md`
- `E2` End-to-end timing on common subset (required):
  - suites: `tpcds_sf10`, `prodds_sf10_str10`
  - report per-query medians + workload median totals
  - derive fast subset (`<60s` on all 3 active engines) for analysis-only slices
- `E3` Dedicated join experiment (required):
  - join scaling execution on `$QUERY_DIR/prodds/generators/join`
  - levels: `1..2048` (`[1,2,4,8,16,32,64,128,256,512,1024,2048]`)
  - scope: `duckdb`, `cedardb`, `monetdb` (`postgres` temporarily excluded)
  - baseline threads: `56`
- planning_ratio capture and reporting

## E5 DuckDB Stringification Sweep (SF10, STR1..STR10)
- Scope:
  - engine: `duckdb` only
  - workload: Prod-DS (SF10), stringification levels `STR1..STR10`
  - PostgreSQL/CedarDB/MonetDB are not part of E5 execution
- Query policy:
  - canonical Prod-DS query set (`query_1.sql..query_99.sql`)
  - `query_0.sql` always excluded
  - no semantic query rewrites
  - query generation is level-specific (`--stringification-level <S>`) and may activate level-dependent query-layer behavior
  - pure-data mode is not mandatory for E5
  - for every timed point, persist query provenance (`query_path`, `query_sha256`, normalized SQL hash) in run artifacts
- Data policy:
  - required data roots are `str1..str10` under `$DATA_ROOT/prodds/sf10/str<S>`
  - fallback mapping `STR>1 -> str10` is forbidden
  - if any required STR root is missing, stop run and record blocker
- Preflight phase (all STR levels):
  - run full query set once per STR to verify feasibility and non-empty outputs
  - classify non-success using protocol taxonomy (`TIMEOUT`, `PARSE`, `DIALECT`, `FAILURE`, `OOM`, `UNKNOWN`)
  - if unexpected empty results appear, treat as seed issue first and repair via seed-override tooling
- Timing phase (selected queries):
  - select exactly 3 stringification-sensitive queries using a documented rubric
  - for each selected query and STR level:
    - warmup `1`
    - timed repetitions `5`
    - report median runtime
- Threading and stats:
  - DuckDB fixed at `PRAGMA threads=56;` (physical cores baseline)
  - run `ANALYZE;` before timing work for each STR level
- Seed artifacts:
  - canonical E5 seed outputs under `experiments/artifacts/E5/seeds/`
  - include per-level overrides and consolidated manifest
- Plots:
  - combined normalized 3-query runtime plot (primary)
  - omit success-rate plots when all points are `100%`

## E5_new Extended Stringification Sweep (SF10, OPTIONAL)
- Scope:
  - engine: `duckdb` only
  - workload: Prod-DS (SF10), extended stringification levels `STR1..STR_MAX` (default `STR_MAX=15`)
  - E5 semantics remain unchanged; E5_new is additive.
- Query policy:
  - reuse E5 selected query set from `experiments/artifacts/E5/selected_queries.json`
  - optional additional sanity candidates may be included when documented in run manifest
  - query generation is level-specific for all STR levels (1-15)
- Data policy:
  - canonical roots: `$DATA_ROOT/prodds/sf10/str<S>`
  - missing STR levels are generated via postprocessing from canonical SF10 base data, outside repo
  - generated STR>10 levels must persist manifests with generator command, git SHA, timestamp, and STR+ config
- Timing policy:
  - warmup `1`, timed reps `5`, timeout follows protocol (`1800s` currently)
  - mandatory `ANALYZE;` before timed runs per STR level
  - one-engine-at-a-time guard must pass before timing
- Required artifacts under `experiments/artifacts/E5_new/`:
  - `merged/raw_times.csv`, `merged/per_str_medians.csv`, `merged/failures.csv`
  - `intensity_metrics.csv`, `run_manifest.json`, `environment_snapshot.md`, `effective_config/duckdb.json`
  - plots:
    - `e5new_normalized_total_vs_str_1_10_duckdb.{pdf,png}`
    - `e5new_normalized_total_vs_str_1_<STR_MAX>_duckdb.{pdf,png}`
    - `e5new_normalized_total_vs_str_1_10_duckdb_top3.{pdf,png}`
    - `e5new_normalized_total_vs_str_1_<STR_MAX>_duckdb_top3.{pdf,png}`

## E6 Result Verification (Post-E5, DuckDB-only)
- Scope:
  - engine: `duckdb` only
  - based on E5 selected queries from `experiments/artifacts/E5/selected_queries.json`
- Goal:
  - verify result consistency across `STR1..STR10`
  - detect semantic drift from seed overrides, stringification, or generator changes
- Deterministic wrapper:
  - execute `SELECT * FROM (<query>) q ORDER BY 1,2,...,N`
  - `N` discovered from `DESCRIBE SELECT * FROM (<query>) q`
  - if ordering fails due type constraints, cast outer projected columns to `VARCHAR` before ordering
- Fingerprinting:
  - capture full result set per `(query, STR)` as deterministic CSV (not printed to stdout)
  - compute `SHA256` over canonical CSV bytes
  - store `row_count`, `column_count`, `sha256` in matrix output
  - verification runtime probe: `1` warmup + `5` timed reps per point (median recorded)
- Classification:
  - `FORMAT_ONLY`, `ORDERING_ONLY`, `TRUE_SEMANTIC_CHANGE`
  - stop immediately if `TRUE_SEMANTIC_CHANGE` is detected
- Artifacts:
  - `experiments/artifacts/E6/overview.md`
  - `experiments/artifacts/E6/verification_matrix.csv`
  - `experiments/artifacts/E6/fingerprints/<query>/str*.sha256`
  - `experiments/artifacts/E6/diffs/<query>/strX_vs_strY.diff` (mismatch only)
  - `experiments/artifacts/E6/commands.log`
  - `experiments/artifacts/E6/progress.log`

- `E4` Stringification sweep (legacy slot):
  - retained for historical compatibility; superseded by dedicated E5 definition below
- `E5` DuckDB stringification sweep (required):
  - SF10, `STR1..STR10`, DuckDB-only
  - preflight full query-set feasibility + non-empty enforcement
  - selected-query timing with `1` warmup + `5` timed repetitions, median reporting
- `E6` Result verification (required):
  - DuckDB-only post-E5 consistency audit with deterministic wrapper + SHA256 fingerprints
- `E7` DuckDB SF100 sanity (OPTIONAL, disabled by default)
- `E8` Result verification on common subset (OPTIONAL, disabled by default)
- `E9` Plan capture audit (OPTIONAL, disabled by default):
  - store under `results/<ts>/plan_capture/<engine>/<suite>/<query>.json`
  - separate from timed runs

## SF100 Campaign (Paper Refresh, 2026-02-27)
- Campaign roots:
  - human plan: `experiments/artifacts_sf100/README.md`
  - machine plan: `experiments/artifacts_sf100/plan.yaml`
  - bootstrap copies: `experiments/artifacts_sf100/bootstrap/duckdb/`
- Existing SF100 data that is already copied into the campaign folder:
  - `E11_default_20260224T001642Z`
  - `E11_fullscope_20260224T112605Z`
- Scope (paper-only figures/tables):
  - median query runtime (common subset only)
  - workload total runtime (common subset only)
  - per-query median runtime distribution (CDF, common subset only)
  - ProdDS error breakdown (derived from timed-run failures; no separate audit run)
  - join execution time vs join level
  - join planning time vs join level
  - union runtime vs fan-in
  - normalized total runtime vs stringification level
- Current status:
  - done (bootstrap only): DuckDB SF100 end-to-end + per-query medians (E11 default/fullscope)
  - pending: 4-engine SF100 common-subset rerun for all paper metrics
  - pending: SF100 join/union scaling for `duckdb`, `postgres`, `cedardb`, `monetdb`
  - pending: SF100 DuckDB stringification sweep (`STR1..STR10`)
  - optional: Nality/Q rerun (not required for paper scope)
- Missing prerequisites before full SF100 execution:
  - SF100 sparse query mapping entries for non-DuckDB engines (reuse existing dialect rewrites where valid)
  - SF100 ProdDS query directories for `str1..str9` (currently only `str10` exists)
  - SF100 ProdDS data for `str2..str9` or stagewise generation workflow
- Disk-aware execution order:
  1. freeze bootstrap artifacts and manifests under `experiments/artifacts_sf100/`
  2. prepare SF100 sparse mapping (`experiments/queries/query_mapping.yaml`) for 4 engines
  3. run common-subset E2-style timing on all 4 engines with inline failure tracking
  4. derive error breakdown from timed-run `failures.csv` (no standalone audit run)
  5. run SF100 join scaling (execution + planning) on all 4 engines
  6. run SF100 union fan-in scaling on all 4 engines
  7. run DuckDB SF100 stringification stagewise (`STR1..STR10`) and clean transient levels after each level
  8. rebuild paper plots/tables from SF100 artifacts only
- Frozen standard set update (`2026-02-28`):
  - definition: base query is included if all 4 engines return `SUCCESS` or `TIMEOUT` (hard errors excluded)
  - PROD-DS SF100 STR10:
    - base success-only intersection: `80`
    - base no-error intersection (timeout included): `90`
    - standard microsuite add-ons: `query_join_J50.sql`, `query_join_J100.sql`, `query_join_J200.sql`,
      `query_union_U2.sql`, `query_union_U5.sql`, `query_union_U10.sql`, `query_union_U20.sql`, `query_union_U200.sql`
    - frozen standard set total: `98`
  - TPC-DS SF100 default run-set: base99 (`query_1.sql..query_99.sql`, `query_0.sql` excluded) = `99`
  - canonical freeze artifacts:
    - `experiments/artifacts_sf100/frozen/sf100_frozen_standard_set.md`
    - `experiments/artifacts_sf100/frozen/sf100_frozen_standard_set.json`
    - `experiments/artifacts_sf100/frozen/e2_subset_sf100_frozen_standard.json`
- Timing policy update (`2026-02-28`, applies to new SF100 timed runs):
  - warmup: `1` pass on all engines
  - timed repetitions: `10` for `duckdb`, `cedardb`, `monetdb`
  - timed repetitions: `3` for `postgres`
