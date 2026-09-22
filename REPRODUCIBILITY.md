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

| ID | Description | Engines | Scale | Paper location | Data file — `experiments/data/paper_csv/` |
|----|-------------|---------|-------|----------------|-------------------------------------------|
| E1 | End-to-end TPC-DS vs Prod-DS | all 3 | SF100 | Fig. 7 (§6.5), Fig. 8 (§6.5), Table 5 (§6.5) | `E1_total_workload_runtime_SF100.csv` (Fig. 7) · `E1_per_query_runtime_SF100.csv` (Fig. 8) · `E1_error_breakdown_SF100.csv` + `E1_error_per_query_SF100.csv` (Table 5) |
| CDF | Cross-benchmark runtime CDF, 8 curves over 6 comparator suites | DuckDB | SF100-class, matched data sizes | Fig. 9 (§6.5) | not in `paper_csv/` — see [`crossbench/`](crossbench/) |
| E2 | Join-scaling micro-suite (J=16..2048) | all 3 | SF100 | Fig. 11 (§6.7) | `E2_join_scaling_SF100.csv` |
| CMP | CedarDB compilation estimates under forced optimized compilation | CedarDB (DuckDB planner comparator) | SF100 and SF10 | paragraph in §6.7, **no figure** | not in `paper_csv/` — see [`compilation/`](compilation/) |
| E3 | UNION ALL fan-in scaling (U=2..2048) | all 3 | SF100 | Fig. 12 (§6.8) | `E3_union_fanin_SF100.csv` |
| E4 | Stringification sweep (STR=1..10) + STRLEN | DuckDB | SF10 | Fig. 10 (§6.6) | `E4_stringification_sweep_SF10_duckdb.csv` |
| E4X | Stringification, cross-engine | all 3 | SF10 | — (revision material) | rendered by `plot_str_crossengine.py` |
| E5 | Sparsity and skew sensitivity | all 3 | SF10 and SF100 | Table 6 (§6.9) | `E5_sparsity_skew_SF10.csv` (Table 6, Δ%) · `E5_per_query_SF10.csv` (per-query medians) · `E5_sparsity_skew_SF100.csv` + `E5_per_query_SF100.csv` + `E5_summary_SF{10,100}.csv` (the SF100 arm, added for the revision) |

**Table 4 (§6.1), the engine-properties survey, is prose**; no experiment in this repository
produces it.

**E5's three paper rows** are NULL sparsity, skew (value *and* key together), and their
combination: `experiments/make_skew_table.py` builds them from the arms `sparsity_only`,
`skew_all` and `full`. Six arms are measured and shipped — the three above plus `skew_only`
(value skew alone), `keyskew_only` and `combined` (NULL + value skew, key skew off) — and
`--all-arms` renders every one. Note that `combined` is **not** the paper's combination row;
`full` is.

**The feasibility/error overview (Table 5) is deliberately conservative and is not the raw E0
tally.** `experiments/export_paper_csv.py` attributes it from the **E0 audit** whenever an E0
tree exists, because E1 runs only the common subset and so cannot see the failures that defined
it; `E1_error_per_query_SF100.csv` carries the per-query cause class behind each count. Keep the
two apart when quoting: the audit outcome is what `E0_audit_per_query_SF<N>.csv` records.

Not tied to a single figure, and shipped alongside: `E1_compilation_time_SF100.csv`
(the separately executed `EXPLAIN` client per engine and suite — **this is not the source of
the §6.7 compilation estimates**; it times a whole client process running `EXPLAIN`, most of
which is client startup, and the §6.7 numbers come from [`compilation/`](compilation/)),
`plan_join_complexity_SF{10,100}_<engine>.csv` (joins written against joins planned, both
scales) and `run_provenance_SF{10,100}.csv` (engine versions, harness commit, threads,
repetitions, timeout, host). The **data-side seeds are not in the provenance CSV** — they
are recorded in `stringification_data_manifest.json` beside the generated data, so tying a
measurement to the seeds that produced its data means reading both.

Per-figure/table CSVs are in [`experiments/data/paper_csv/`](experiments/data/paper_csv/).
`./reproduce_EAB.sh figures` renders every figure and table from a completed run's
`.reproduce/sf*/results`.

> **Filenames are not paper numbers.** The in-repo figure generators emit legacy output
> filenames (`fig13_*` is Figure 10, `fig10_error_breakdown` is Table 5, `table3_*` is
> Table 6); for the two ladders the file numbers happen to coincide with the paper
> (`fig11_*` is Figure 11, `fig12_*` is Figure 12), which is a coincidence and not a rule.
> Match outputs to the paper by experiment and content per the table above, never by
> filename. Filenames are kept as they are so that earlier outputs stay comparable.

## Figure 9 and the §6.7 compilation estimates

Two results have their own self-contained directories, because neither is produced by
`reproduce.sh`. Both are reproducible **from a clean checkout with no database and no engine
install** — the measurement records travel with them.

### Figure 9 — cross-benchmark runtime CDF

```bash
git clone https://github.com/szlangini/prod-ds-kit.git
cd prod-ds-kit/crossbench
python3 make_cdf_figure_final.py        # both exports + the summary tables, from results/final/
python3 verify_cdf_exports.py           # checks them against each other and against the page
```

`figures/cdf_crossbench_paper.{pdf,png}` is **paper Figure 9**, one manuscript column wide.
`figures/cdf_crossbench_final.{pdf,png}` is the same plot on a full-width canvas, kept for the
response letter; the two are the same curves at two sizes and the renderer verifies that on
every run. Needs only Python with `matplotlib` and `numpy`.

> **Not to be confused with [`experiments/plot_cdf_crossbench.py`](experiments/plot_cdf_crossbench.py).**
> That is an **earlier, superseded experiment** kept for the record: a different pipeline over
> `experiments/data/s7_cdf/`, drawing SF10 across eight suites and SF100 across five scalable
> ones, not run under the ten-pass protocol. **It does not produce Figure 9** and its outputs
> are not the paper's. `crossbench/` is the accepted path.

### §6.7 — CedarDB compilation estimates

```bash
cd prod-ds-kit/compilation
python3 summarize_compilation.py --scale 100    # per-query medians and workload aggregates
python3 summarize_compilation.py --scale 10     # the second scale, same treatment
```

The raw records travel with the directory, so the summaries above regenerate without an engine.
Re-measuring needs CedarDB and a loaded SF100 database under `.reproduce/` and takes about
six minutes:

```bash
python3 measure_cedardb_compilation.py --scale 100 --suites prodds,tpcds \
        --modes i,A,d,c,o --reps 5 --out cedardb_compilation_raw_SF100.csv
```

**What the numbers are.** Per query, the difference between the median `PREPARE` time under
forced `Optimized` compilation and under `Interpreted`, each over five repetitions in its own
session, then summarised across queries. They are **estimated compilation costs under forced
optimized compilation**. They do not isolate pure code generation, and they are not the
compilation component of the default-mode campaign: the campaign ran CedarDB's default `Auto`
mode, whose `PREPARE` defers compilation into the execution stage. `compilation/README.md` has
the method, the caveats and the per-query data.

## Engine versions

- **DuckDB** v1.4.4
- **CedarDB** v2026-05-26 (pinned versioned binary)
- **MonetDB** v11.55.7 Dec2025-SP3 (version-locked distro package)

`--init` installs all three, version-pinned. If an engine cannot be installed, the
run logs it and continues with the rest; DuckDB alone covers E1–E5.

## Measurement protocol (paper Sections 6.2 and 6.4)

- **Isolation:** one engine at a time.
- **Warmup:** one untimed execution of the first query of each suite before the timed
  repetitions (`--warmup N` = number of leading queries, default 1); the timed
  repetitions of every query then run back to back.
- **Repetitions:** 10 timed; the **median of a query's ten executions** is that query's
  runtime, and total workload runtime is the **sum of those per-query medians** — not the
  wall time of any single pass. §6.2 is the hardware and execution environment; §6.4 is
  the methodology, including repetitions and aggregation.
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
