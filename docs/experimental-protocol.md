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
| Kernel        | 7.0.0-30-generic (revision campaign; the submitted evaluation ran 6.14.0-37-generic) |
| CPU           | 2 x AMD EPYC 7453, 28 cores/socket    |
| SMT           | 2 (hyperthreading)                     |
| Logical CPUs  | 112                                    |
| RAM           | ~1.0 TiB                               |

## Engine Versions

| Engine     | Version                                     |
|------------|---------------------------------------------|
| DuckDB     | v1.4.4 (Andium) `6ddac802ff`                |
| CedarDB    | v2026-05-26 (pinned versioned binary)       |
| MonetDB    | 11.55.7 Dec2025-SP3 (pinned distro package) |

These are the builds the **revision** campaign measured. The submitted paper's numbers came from
earlier MonetDB builds (11.55.1, then 11.55.5 for the June recheck), and the feasibility counts move
with that build — do not read a June number and a September number as the same experiment.

---

## Data Policy

Authoritative data roots:

- `$DATA_ROOT/tpcds/sf100`                   (vanilla baseline, STR1)
- `$DATA_ROOT/prodds/sf100/str5`             (default Prod-DS = production optimum)
- `$DATA_ROOT/prodds/sf10/str1` … `str10`    (E4 stringification type-coverage sweep; str10 = full).
  **This sweep runs at SF10, not SF100**: ten levels at SF100 would be roughly 500 GB of data
  (`reproduce.sh` says so at the generation step). E4X, the cross-engine variant, is SF10 too.

No data is copied into the repository.

## Query Policy

- Canonical query root namespace: `$QUERY_DIR/...`
- Engine-specific variant root: `experiments/queries/<engine>/<suite>/...`
- Mapping file: `experiments/queries/query_mapping.yaml`
- `query_0.sql` is always excluded from execution.

Active scaling levels. The **default workload** and the **micro-suite ladders** are different
populations and must not be mixed:

| | levels | where |
|---|---|---|
| default workload, joins | `J50`, `J100`, `J200` | part of the 107-query default set |
| default workload, unions | `U2`, `U5`, `U10`, `U20`, `U200` | `wrap_dsqgen.UNION_FANIN_TARGETS` |
| E2 join ladder | `[16, 32, 64, 128, 256, 512, 1024, 2048]` | `reproduce.sh --join-targets` |
| E3 union ladder | `[2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048]` | `--union-max-inputs 2048` |

The default set is therefore **107 queries**: 99 templates + 3 joins + 5 unions. The ladders are
generated into their own directories and are *not* part of it. A `J*` level is a **syntactic
target**, not an exact join count — the generator minimises absolute error to it (Appendix A), so
`J16` holds 21 joins and `J256` holds 252.

---

## Execution Invariants

| Invariant                 | Value                                       |
|---------------------------|---------------------------------------------|
| Node count                | 1 (single node)                             |
| Query concurrency         | Serial (no concurrent query workers)        |
| Engine concurrency        | Exactly 1 engine active at a time           |
| Warmup (untimed leading queries per suite)| 1                                           |
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

Before any timed workload execution, a full statistics refresh is mandatory on all
tables for optimizer fairness and cross-engine comparability.

| Engine     | Command                              | Notes                           |
|------------|--------------------------------------|---------------------------------|
| DuckDB     | `ANALYZE;` or `PRAGMA analyze;`      | Explicit after bulk load        |
| MonetDB    | `ANALYZE;` or `CALL sys.analyze();`  | Explicit before timing          |
| CedarDB    | `ANALYZE;`                           | Protocol symmetry; adaptive internally |

Statistics collection is not timed. Completion must be logged.

## Timeout Policy

- Per-query timeout: **1800 seconds** (30 minutes).
- Near-timeout exclusion rule: if any audited engine completes a query within 60
  seconds of the timeout, that query is excluded from the common subset.
  Implemented by `experiments/common_subset.py` (writes `common_subset.{json,md}`
  next to the results; `experiments/plot_results.py --common-subset` consumes it).

---

## Experiments

The identifiers below are the ones `reproduce.sh --experiment` takes, and the ones the paper and its
figures use. An earlier version of this document numbered them E1-E6, one off from the harness; that
numbering is withdrawn. `python -m experiments run --experiment <name>` is the low-level runner that
each step drives.

| | what it is | scale | runner experiment |
|---|---|---|---|
| **E0** | audit pass, one untimed run per query and engine | SF100 | `workload_compare` |
| **E1** | timed workload, TPC-DS against Prod-DS | SF100 | `workload_compare` |
| **E2** | join-amplification ladder | SF100 | `join_scaling` |
| **E3** | UNION ALL fan-in ladder | SF100 | `union_scaling` |
| **E4** | stringification sweep, DuckDB only | **SF10** | `string_sweep` |
| **E4X** | stringification, cross-engine | **SF10** | `string_sweep` |
| **E5** | sparsity and skew arms | SF10 and SF100 | `workload_compare` |

`--all` expands to `E1 E2 E3 E4 E5` and **does not include E0**. That matters: without
`common_subset.json`, `prepare_run_queries` falls back to the full query set silently, so E1 and E5
then measure a different population than the paper's. Run the audit first.

### E0 -- Audit and timeout freeze

One untimed run of every query on every engine (`REPS=1`, no warmup), then the common subset across
the engines audited so far. Timeout frozen by
`T = max_successful_calibration_runtime + 60 s`, capped at 1800 s.

`experiments/common_subset.py --experiment E0` writes `common_subset.{json,md}` beside the results;
the timed experiments read that file and run only the subset. Failures live here — the timed set
contains none by construction.

### E1 -- End-to-end timing

- Suites: `tpcds`, `prodds` (STR5, the default production optimum).
- Per-query medians and workload totals over the common subset.

### E2 -- Join-amplification ladder

- Levels `[16, 32, 64, 128, 256, 512, 1024, 2048]`, generator model
  `J(k,m) = (m+1)*(b + k*(b+1)) + m` with base width `b = 10`; calibration pinned in
  `workload/config/returns.yml::target_overrides`.
- **One shared query directory for all three engines** — the ladder is generated once at the parser
  default dialect and is not passed through the per-engine dialect fixes, so every engine executes
  byte-identical SQL. The union ladder is the exception: MonetDB runs CTE-inlined variants.
- Planning collected as a separately executed `EXPLAIN` client, not an engine-internal phase.

### E3 -- UNION ALL fan-in ladder

- Levels `[2, 4, 8, ..., 2048]`, one shared base CTE per branch, each branch filtering a different
  month.
- MonetDB runs **CTE-inlined** variants (`monetdb_inline_union_ctes`), because of an optimiser bug
  with CTE + JOIN + UNION ALL. Its query is therefore structurally different from the other two
  engines', which any comparison has to state.

### E4 -- Stringification sweep (DuckDB, STR 1..10)

- Engine: DuckDB only. Workload: Prod-DS at **SF10**, levels STR 1 through STR 10, plus the STRLEN
  length add-on at the default level.
- Preflight: full query-set feasibility check with non-empty enforcement per level.
- 1 untimed warmup, timed repetitions, median reporting.

### E4X -- Stringification, cross-engine

The same sweep restricted to selected levels and run on every engine, at SF10. Kept separate from
E4 because its query population differs.

### E5 -- Sparsity and skew arms

Baseline against the injected arms (NULL sparsity, MCV skew, key skew, and the joint arm), at SF10
and SF100, over the common subset. Reports per-arm deltas.

### Result verification -- specified, not run

A DuckDB-only consistency audit was specified: wrap each query as
`SELECT * FROM (<query>) q ORDER BY 1, 2, ..., N`, fingerprint the result per `(query, STR)` pair
with SHA-256, and classify differences as `FORMAT_ONLY`, `ORDERING_ONLY` or
`TRUE_SEMANTIC_CHANGE`. **It is not implemented in `reproduce.sh` and it was not run.** No
result-set comparison exists for any query in this benchmark, so a query that runs successfully is
not thereby shown to return the same answer as an earlier form of itself. The harness measures the
client's endpoint and discards rows; there is no stored fingerprint to compare against.

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
