# Experimental Protocol

> **Note.** This document is the frozen protocol used for the paper evaluation.
> All paths use environment variables (`$DATA_ROOT`, `$QUERY_DIR`, `$REPO_ROOT`).
> See `install.sh` for setup.

## Mode

Mechanism-first, reproducible, one-engine-at-a-time execution.

---

## Host Environment

| Property      | Value                                  |
|---------------|----------------------------------------|
| OS            | Ubuntu 24.04.4 LTS                     |
| Kernel        | 6.14.0-37-generic                      |
| CPU           | 2 x AMD EPYC 7453, 28 cores/socket    |
| SMT           | 2 (hyperthreading)                     |
| Logical CPUs  | 112                                    |
| RAM           | ~1.0 TiB                               |

## Engine Versions

| Engine     | Version                                     | Binary path                  |
|------------|---------------------------------------------|------------------------------|
| DuckDB     | v1.4.4 (Andium) `6ddac802ff`                | `/usr/local/bin/duckdb`      |
| PostgreSQL | 16.11                                       | `/usr/bin/psql`              |
| CedarDB    | v2026-02-03 (2026-01-29, format 0)          | `/usr/local/bin/cedardb`     |
| MonetDB    | 11.55.1 (Dec2025)                           | `/usr/bin/mserver5`          |

---

## Data Policy

Authoritative data roots:

- `$DATA_ROOT/tpcds/sf10`
- `$DATA_ROOT/prodds/sf10/str1`
- `$DATA_ROOT/prodds/sf10/str10`
- `$DATA_ROOT/prodds/sf100/str1` (SF100 campaign)
- `$DATA_ROOT/prodds/sf100/str10` (SF100 campaign)

No data is copied into the repository. Any run or config path under
`experiments/plan_runs/**/data` is invalid and must abort.

## Query Policy

- Canonical query root namespace: `$QUERY_DIR/...`
- Engine-specific variant root: `experiments/queries/<engine>/<suite>/...`
- Mapping file: `experiments/queries/query_mapping.yaml`
- `query_0.sql` is always excluded from execution.

Scaling query locations:

- Join: `$QUERY_DIR/prodds/generators/join`
- Union: `$QUERY_DIR/prodds/generators/union`

Active scaling levels:

- Join: `[1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048]`
- Union: `[2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048]` (level 1
  deprecated)

---

## Execution Invariants

| Invariant                 | Value                                       |
|---------------------------|---------------------------------------------|
| Node count                | 1 (single node)                             |
| Query concurrency         | Serial (no concurrent query workers)        |
| Engine concurrency        | Exactly 1 engine active at a time           |
| Enforcement               | `tools/preflight_one_engine.sh`             |
| Warmup passes             | 1                                           |
| Timed repetitions (SF10)  | 5                                           |
| Timed repetitions (SF100) | 10 (DuckDB, CedarDB, MonetDB); 3 (PostgreSQL) |

## Threading Policy

- **Baseline** (default): physical cores only, 56 threads/workers.
- **Optional sensitivity mode**: logical CPUs, 112 threads/workers.

Per-engine settings:

| Engine     | Baseline command                                           |
|------------|------------------------------------------------------------|
| DuckDB     | `PRAGMA threads=56;`                                       |
| PostgreSQL | `SET max_parallel_workers_per_gather=56;`                  |
| MonetDB    | `monetdb set nthreads=56 <db>`                             |
| CedarDB    | `parallel="56"` (or environment equivalent)                |

## Statistics Collection

Before any timed workload execution, a full statistics refresh is mandatory on all
tables for optimizer fairness and cross-engine comparability.

| Engine     | Command                           | Notes                        |
|------------|-----------------------------------|------------------------------|
| PostgreSQL | `ANALYZE;`                        | Execute after load, `jit=off`|
| DuckDB     | `ANALYZE;` or `PRAGMA analyze;`   | Explicit after bulk load     |
| MonetDB    | `ANALYZE;` or `CALL sys.analyze();` | Explicit before timing    |
| CedarDB    | `ANALYZE;`                        | Protocol symmetry; adaptive internally |

Statistics collection is not timed. Completion must be logged.

## Timeout Policy

- Per-query timeout: **1800 seconds** (30 minutes).
- Near-timeout exclusion rule: if any audited engine completes a query within 60
  seconds of the timeout, that query is excluded from the common subset.

---

## Experiments

### E0 -- Setup

Environment verification, install verification, scaling query materialization,
manifests.

### E1 -- Audit and Timeout Freeze

- Run audit with provisional timeout.
- Freeze timeout `T` using rule: `T = max_successful_calibration_runtime + 60s`.
- Compute and publish `common_subset.md`.

### E2 -- End-to-End Timing

- Suites: `tpcds_sf10`, `prodds_sf10_str10`.
- Report per-query medians and workload median totals.
- Derive fast subset (queries under 60 seconds on all active engines) for
  analysis-only slices.

### E3 -- Join Scaling Micro-Suite

- Query root: `$QUERY_DIR/prodds/generators/join`
- Join levels: `[1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048]`
- Generator model: `J(k,m) = (m+1)*(b + k*(b+1)) + m` with base width `b = 10`.
- Calibration pinned in `workload/config/returns.yml::target_overrides`.
- Planning collection: DuckDB deep-dive with `EXPLAIN (ANALYZE, FORMAT JSON)`;
  CedarDB/MonetDB best-effort via `EXPLAIN` wall-clock.
- Derived metric: `planning_ratio = planning_time_ms / total_time_ms`.

### E4 -- Stringification Sweep (Legacy)

Retained for historical compatibility; superseded by E5.

### E5 -- DuckDB Stringification Sweep (SF10, STR 1..10)

- Engine: DuckDB only.
- Workload: Prod-DS SF10, levels STR 1 through STR 10.
- Preflight: full query-set feasibility check with non-empty enforcement per
  level.
- Timing: 3 selected stringification-sensitive queries; 1 warmup, 5 timed
  repetitions, median reporting.

### E5_new -- Extended Stringification Sweep (STR 1..15, Optional)

- Extends E5 to STR+ levels (11--15).
- Reuses E5 selected query set.
- Uses `--stringification-level 11..15` directly (no extra flag needed).

### E6 -- Result Verification

- DuckDB-only post-E5 consistency audit.
- Deterministic wrapper: `SELECT * FROM (<query>) q ORDER BY 1, 2, ..., N`.
- SHA256 fingerprinting per `(query, STR)` pair.
- Classification: `FORMAT_ONLY`, `ORDERING_ONLY`, `TRUE_SEMANTIC_CHANGE`.

### E7 -- SF100 Sanity (Optional)

DuckDB SF100 sanity check. Disabled by default.

### E8 -- Result Verification on Common Subset (Optional)

Disabled by default.

### E9 -- Plan Capture Audit (Optional)

Plan artifacts stored under `results/<ts>/plan_capture/<engine>/<suite>/`.
Separate from timed runs. Disabled by default.

### E10 -- SF100 Campaign

- Scope: all 4 engines at SF100.
- Metrics: median query runtime, workload total, per-query CDF, error breakdown,
  join/union scaling, stringification sweep.
- Timing: 10 repetitions (DuckDB, CedarDB, MonetDB); 3 repetitions (PostgreSQL).

---

## Error Taxonomy

| Status    | Description                                  |
|-----------|----------------------------------------------|
| `SUCCESS` | Query completed within timeout               |
| `TIMEOUT` | Query exceeded per-query timeout             |
| `ERROR`   | Query failed (see subclasses below)          |

Error subclasses:

| Subclass  | Description                                  |
|-----------|----------------------------------------------|
| `OOM`     | Out-of-memory error                          |
| `FAILURE` | Engine-reported execution failure             |
| `DIALECT` | SQL dialect incompatibility                  |
| `PARSE`   | SQL parse error                              |
| `UNKNOWN` | Fallback if classification fails             |

## Artifact Contract (Per Run)

Every run must produce:

- `raw_times.csv`
- `failures.csv`
- `audit.json`
- `run_manifest.json`
- `environment_snapshot.md`
- `effective_config.json`
