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
data, runs the experiments in order, and renders every figure and table into
`eab_artifact/{figures,tables,logs}/`.

```bash
./reproduce_EAB.sh --quick E1   # fast smoke: SF1, 1 rep, single phase (~15 min)
./reproduce_EAB.sh all          # full run at the scales listed in the Experiments table
./reproduce_EAB.sh E1|E2|E3|E4|E4X|E5   # one experiment
./reproduce_EAB.sh figures      # (re)render figures + tables from existing/archived results
```

Each experiment runs at its reported scale (see the *Scale* column below); `--quick`
forces SF1 / 1 rep for a functional check. One engine runs at a time; a failed unit
is logged and the run continues. Regenerable variant data is cleaned per variant, so
peak disk stays ~1 variant (`clean-data` / `purge` reclaim space between runs).

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
| E5 | Sparsity and skew sensitivity | all 3 | SF10 | Table 5 | `E5_sparsity_skew_SF10.csv` |

Per-figure/table CSVs are in [`experiments/data/paper_csv/`](experiments/data/paper_csv/);
full per-query raw runs (`raw.jsonl` + `summary.csv`, with a `manifest.json` per run)
are under `experiments/data/results/`. `./reproduce_EAB.sh figures` renders from
`.reproduce/sf*/results` when present and otherwise from the committed archive.

> The in-repo figure generators emit legacy output filenames (e.g. `fig13_*`,
> `table3_*`) — match outputs to the paper by experiment/content per the table above,
> not by filename.

## Engine versions

- **DuckDB** v1.4.4
- **CedarDB** v2026-05-26 (pinned versioned binary)
- **MonetDB** v11.55.5 (version-locked distro package)

`--init` installs all three, version-pinned. If an engine cannot be installed, the
run logs it and continues with the rest; DuckDB alone covers E1–E5.

## Measurement protocol (paper Section 6.2)

- **Isolation:** one engine at a time.
- **Warmup:** 1 untimed pass.
- **Repetitions:** 10 timed; median reported.
- **Timeout:** 1800 s per query.
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
| `--experiment ID` | Run one experiment (E1–E5) | — |
| `--all` | Run all experiments | — |
| `--sf N` | Scale factor (1, 10, 100) | 1 |
| `--engines LIST` | Comma-separated engines or `all` | duckdb |
| `--reps N` | Timed repetitions per query | 3 |
| `--warmup N` | Warmup passes | 1 |
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
