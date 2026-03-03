# Prod-DS Experimental Protocol

Version: 1.0.0
Mode: mechanism-first, reproducible, one-engine-at-a-time

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

| Engine     | Version                                     |
|------------|---------------------------------------------|
| DuckDB     | v1.4.4 (Andium) `6ddac802ff`                |
| CedarDB    | v2026-02-03 (2026-01-29, format 0)          |
| MonetDB    | 11.55.1 (Dec2025)                           |

---

## Execution Invariants

| Invariant                 | Value                                       |
|---------------------------|---------------------------------------------|
| Node count                | 1 (single node)                             |
| Query concurrency         | Serial (no concurrent query workers)        |
| Engine concurrency        | Exactly 1 engine active at a time           |
| Warmup passes             | 1                                           |
| Timed repetitions         | 10                                          |
| Per-query timeout          | 1800 seconds (30 min)                       |

## Threading Policy

- **Baseline** (default): physical cores only, 56 threads/workers.
- **Optional sensitivity mode**: logical CPUs, 112 threads/workers.

| Engine     | Baseline command                                           |
|------------|------------------------------------------------------------|
| DuckDB     | `PRAGMA threads=56;`                                       |
| MonetDB    | `monetdb set nthreads=56 <db>`                             |
| CedarDB    | `parallel="56"` (or environment equivalent)                |

## Statistics Collection

Before any timed workload execution, a full statistics refresh is mandatory
on all tables for optimizer fairness and cross-engine comparability.

| Engine     | Command                              | Notes                           |
|------------|--------------------------------------|---------------------------------|
| DuckDB     | `ANALYZE;` or `PRAGMA analyze;`      | Explicit after bulk load        |
| MonetDB    | `ANALYZE;` or `CALL sys.analyze();`  | Explicit before timing          |
| CedarDB    | `ANALYZE;`                           | Protocol symmetry; adaptive internally |

Statistics collection is not timed. Completion must be logged.

---

## Experiments

### E1 -- Audit and Timeout Freeze

Run audit with provisional timeout. Freeze timeout `T` using rule:
`T = max_successful_calibration_runtime + 60s`. Compute common subset.

Runner: `python -m experiments run --experiment workload_compare`

### E2 -- End-to-End Timing

Suites: `tpcds` and `prodds` (STR10). Report per-query medians and
workload median totals.

Runner: `python -m experiments run --experiment workload_compare`

### E3 -- Join Scaling Micro-Suite

Join levels: `[1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048]`.
Generator model: `J(k,m) = (m+1)*(b + k*(b+1)) + m` with base width `b = 10`.
Calibration pinned in `workload/config/returns.yml::target_overrides`.

Runner: `python -m experiments run --experiment join_scaling`

### E5 -- Stringification Sweep (DuckDB, STR 1..10)

DuckDB-only. Prod-DS at SF10/SF100, stringification levels STR 1 through STR 10.
Preflight feasibility check per level, then selected-query timing.

Runner: `python -m experiments run --experiment string_sweep`

### E6 -- Result Verification

DuckDB-only post-E5 consistency audit. Deterministic wrapper with
`SELECT * FROM (<query>) q ORDER BY 1, 2, ..., N` and SHA256 fingerprinting.

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
