# Reproducibility Guide

**Paper:** "Scaling Analytical Benchmarks to Production Complexity: A Data- and Query-Centric Extension to TPC-DS"
**Venue:** VLDB 2027
**Repository:** https://github.com/szlangini/prod-ds-kit

## Overview

Prod-DS Kit extends TPC-DS with stringification, join-graph amplification, NULL
sparsity, MCV skew, and 92 extended query templates. This package regenerates all
data and queries, re-runs the five experiments, and renders the paper's figures
and tables — all from this repository, with no author-local paths.

## Running the artifact

`./reproduce_EAB.sh` is the single entry point. It installs the engines, generates
data, runs the experiments in order — **the E0 audit first**, so E1 and E5 execute only
the common subset — and renders every figure and table into
`eab_artifact/{figures,tables,logs}/`. The audit runs at every scale a subset-consuming
experiment uses, which is E1's and E5's (SF100 and SF10 by default), because
`common_subset.json` is written per scale factor.

```bash
./reproduce_EAB.sh --quick E1   # fast smoke: SF1, 1 rep, single phase (~15 min)
./reproduce_EAB.sh all          # full run at the scales listed in the Experiments table
./reproduce_EAB.sh E0|E1|E2|E3|E4|E4X|E5  # one experiment (E0 = the audit pass)
./reproduce_EAB.sh figures      # (re)render figures + tables from existing/archived results
```

Each experiment runs at its reported scale (see the *Scale* column below); `--quick`
forces SF1 / 1 rep for a functional check. One engine runs at a time; a failed unit
is logged and the run continues. Regenerable variant data is cleaned per variant, so
peak disk stays ~1 variant (`clean-data` / `purge` reclaim space between runs).

**Measured queries are the generated ones.** `reproduce.sh` applies only parameter-neutral
dialect fixes (regex) to a generated query set. The hand-authored structural overlay files
under `experiments/queries/dialect_variants/` are legacy (authored March 2026 with that
generator's parameters) and are disabled by default; `DIALECT_OVERLAYS=1` re-enables them
for archaeology only, because they replace up to 41 Prod-DS and 71 TPC-DS queries with
stale variants and break the one-query-set-for-all-engines rule.

**Default workload is non-empty by construction.** Data-side injections use seed
`0`; query parameters use dsqgen's default seed plus the shipped per-scale-factor
overrides `configs/seed_overrides_sf{1,10,100}.yml`, which `wrap_dsqgen.py` applies
automatically for every stringification level and dialect. With them every query of
the 107-query default workload returns rows at SF10 and SF100 (SF1: 106/107, `query_4`
is a documented exception). The files are validated with `tools/query_gate.py`
against the default data and against the data without key skew (newly-empty check)
before a release; `tests/test_shipped_seed_overrides.py` guards them. See README,
section *Seeds*.

### Docker (recommended)

A pinned `Dockerfile` (Ubuntu 24.04, the engine versions below) gives a hermetic
environment:

```bash
docker build -t prod-ds-kit .
docker run --rm -it prod-ds-kit ./reproduce_EAB.sh --quick E1     # smoke
docker run --rm -it -v "$PWD/.reproduce:/opt/prod-ds-kit/.reproduce" \
    prod-ds-kit ./reproduce_EAB.sh all                            # full run
```

## Experiments

| ID | Description | Engines | Scale | Paper figure/table | Data file — `experiments/data/paper_csv/` |
|----|-------------|---------|-------|--------------------|-------------------------------------------|
| E1 | End-to-end TPC-DS vs Prod-DS | all 3 | SF100 | Fig. 7, Fig. 8, Table 4 | `E1_total_workload_runtime_SF100.csv` (Fig. 7) · `E1_per_query_runtime_SF100.csv` (Fig. 8) · `E1_error_breakdown_SF100.csv` + `E1_error_per_query_SF100.csv` (Table 4) |
| E2 | Join-scaling micro-suite (J=16..2048) | all 3 | SF100 | Fig. 10 | `E2_join_scaling_SF100.csv` |
| E3 | UNION ALL fan-in scaling (U=2..2048) | all 3 | SF100 | Fig. 11 | `E3_union_fanin_SF100.csv` |
| E4 | Stringification sweep (STR=1..10) + STRLEN | DuckDB | SF10 | Fig. 9 | `E4_stringification_sweep_SF10_duckdb.csv` |
| E4X | Stringification, cross-engine | all 3 | SF10 | — (revision material) | rendered by `plot_str_crossengine.py` |
| E5 | Sparsity and skew sensitivity | all 3 | SF10 and SF100 | Table 5 | `E5_sparsity_skew_SF10.csv` (Table 5, Δ%) · `E5_per_query_SF10.csv` (per-query medians) · `E5_sparsity_skew_SF100.csv` + `E5_per_query_SF100.csv` + `E5_summary_SF{10,100}.csv` (the SF100 arm, added for the revision) |

Not tied to a single figure, and shipped alongside: `E1_compilation_time_SF100.csv`
(the separately executed `EXPLAIN` client per engine and suite),
`plan_join_complexity_SF{10,100}_<engine>.csv` (joins written against joins planned, both
scales) and `run_provenance_SF{10,100}.csv` (engine versions, harness commit, threads,
repetitions, timeout, host). The **data-side seeds are not in the provenance CSV** — they
are recorded in `stringification_data_manifest.json` beside the generated data, so tying a
measurement to the seeds that produced its data means reading both.

Per-figure/table CSVs are in [`experiments/data/paper_csv/`](experiments/data/paper_csv/).
`./reproduce_EAB.sh figures` renders every figure and table from a completed run's
`.reproduce/sf*/results`.

> The in-repo figure generators emit legacy output filenames (e.g. `fig13_*`,
> `table3_*`) — match outputs to the paper by experiment/content per the table above,
> not by filename.

## Engine versions

- **DuckDB** v1.4.4
- **CedarDB** v2026-05-26 (pinned versioned binary)
- **MonetDB** v11.55.7 Dec2025-SP3 (version-locked distro package)

`--init` installs all three, version-pinned. If an engine cannot be installed, the
run logs it and continues with the rest; DuckDB alone covers E1–E5.

## Measurement protocol (paper Section 6.2)

- **Isolation:** one engine at a time.
- **Warmup:** one untimed execution of the first query of each suite before the timed
  repetitions (`--warmup N` = number of leading queries, default 1); the timed
  repetitions of every query then run back to back.
- **Repetitions:** 10 timed; median reported.
- **Timeout:** 1800 s per query.
- **Audit first (E0):** `./reproduce.sh --experiment E0` runs every query of both suites
  once, untimed, on each requested engine and writes the common subset
  (`.reproduce/sf<N>/results/common_subset.json`, with the per-engine failure report next to
  it). E1 and E5 then execute only that subset, so timed repetitions are never spent on
  queries some engine cannot run. Driving `reproduce.sh` directly, the databases loaded
  for the audit can be reused (`KEEP_ENGINE_DBS=1`); `reproduce_EAB.sh` does **not** set
  it and reloads per unit instead, trading load time for bounded peak disk.
- **Evaluation:** `experiments/common_subset.py` computes, per suite, the common success
  set across the audited engines with the near-timeout rule (60 s) and attributes every
  failure per engine; `./reproduce_EAB.sh figures` runs it first, restricts the E1
  runtime figures to that subset and publishes the report
  (`eab_artifact/tables/common_subset_sf<N>.md`).
- **Hardware:** 2× AMD EPYC 7453 (56 cores), ~1 TiB RAM, Ubuntu 24.04.4 LTS.

## Prerequisites

- **OS:** Ubuntu 22.04+ (tested on 24.04.4 LTS) · **Python** ≥ 3.9
- **Packages:** `curl`, `unzip`, `git`, `build-essential`
- **Disk:** ~50 GB (SF10); ~250 GB (SF100, with per-variant cleanup)
- **RAM:** 64 GB (SF10); ~1 TiB (SF100)

## Command reference (`reproduce.sh`)

`reproduce_EAB.sh` wraps the lower-level `reproduce.sh`, which can be driven directly:

| Flag | Description | Default |
|------|-------------|---------|
| `--init` | Install deps, build TPC-DS toolkit, generate data, load DBs | (first run) |
| `--experiment ID` | Run one experiment (E0 audit, E1–E5) | — |
| `--all` | `E1 E2 E3 E4 E5` — **not** E0 and **not** E4X. Run the E0 audit first, or E1/E5 fall back to the full query set instead of the common subset | — |
| `--sf N` | Scale factor (1, 10, 100) | 1 |
| `--engines LIST` | Comma-separated engines or `all` | duckdb |
| `--reps N` | Timed repetitions per query | 3 |
| `--warmup N` | Untimed leading queries per suite | 1 |
| `--timeout N` | Per-query timeout (s) | 1800 |
| `--plots` | Render plots from existing results | — |

```bash
./reproduce.sh --init --experiment E1 --sf 100 --engines all --reps 10
```

Results are written to `.reproduce/sf<N>/results/<E>/` as `raw.jsonl` (one record
per query execution) and `summary.csv` (per-query median / p5 / p95).

## Troubleshooting

- **TPC-DS toolkit build fails** — `sudo apt-get install build-essential`.
- **DuckDB download fails** — check network; the binary can be placed on `PATH` manually.
- **CedarDB / MonetDB unavailable** — skip them: `--engines duckdb`.
- **Disk errors** — SF100 needs ~250 GB; check `df -h` before starting.
- **Python errors** — `python3 -m venv .venv && source .venv/bin/activate && pip install -e ".[test]"`.

## License

Provided for research reproducibility. The TPC-DS toolkit is subject to the TPC End
User License Agreement; see `NOTICE.md` for third-party license information.
