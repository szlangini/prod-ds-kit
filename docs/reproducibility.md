# Running the Reproducibility Script

[`reproduce_EAB.sh`](../reproduce_EAB.sh) (at the repository root) is the **single
entry point** that reproduces every experiment in the paper *"Scaling Analytical
Benchmarks to Production Complexity: A Data- and Query-Centric Extension to
TPC-DS"* (PVLDB, Experiment · Analysis & Benchmark). It installs the engines,
generates all data, runs the experiments, and renders every figure and table —
all from this repository, with no author-local paths.

- **Repository:** <https://github.com/szlangini/prod-ds-kit>
- **Script:** [`reproduce_EAB.sh`](../reproduce_EAB.sh) (repo root)
- **Full guide:** [`REPRODUCIBILITY.md`](../REPRODUCIBILITY.md) — prerequisites,
  hardware, engine versions, output structure, and troubleshooting.

## Getting it

```bash
git clone https://github.com/szlangini/prod-ds-kit.git
cd prod-ds-kit
./reproduce_EAB.sh --quick E1      # ~15 min sanity check (bootstraps everything on first run)
```

The first invocation bootstraps a Python virtual environment, builds the TPC-DS
toolkit, and installs the **pinned** engines (DuckDB 1.4.4, CedarDB v2026-05-26,
MonetDB 11.55.5) — no manual setup. If an engine cannot be installed, the run
logs it and continues with the rest.

## Choosing the scale factor

The scale factor is selected with a **single prefix flag**. The **default is
SF10** (fast enough to iterate); the **published paper results are at SF100**.

| Command | Scale | Use it for |
|---|---|---|
| `./reproduce_EAB.sh --quick all` | SF1 | smoke test — every phase, subset sweep, 1 rep (a few hours) |
| `./reproduce_EAB.sh all` | **SF10** *(default)* | fast full run — same trends/cliffs/failure modes at 1/10 the data |
| `./reproduce_EAB.sh --sf100 all` | SF100 | **reproduce the published paper results** (multi-day) |

`--full` is an alias for `--sf100`. To override a *single* experiment's scale,
set its env var, e.g. `SF_E4=100 ./reproduce_EAB.sh E4`.

> **Reproducing the paper:** use `--sf100`. The SF10 default reproduces the same
> qualitative behaviour (trends, cliffs, failure modes) at a tenth of the data
> size; only the absolute runtimes differ.

## What to run (targets)

| Target | What it does | Paper output |
|---|---|---|
| `all` | every experiment (E1–E5 + E4X) → all figures & tables | — |
| `E1` | end-to-end TPC-DS vs Prod-DS | Fig. 7, Fig. 8, Table 4 |
| `E2` | join-scaling micro-suite, J = 16…2048 | Fig. 10 |
| `E3` | UNION ALL fan-in, U = 2…2048 | Fig. 11 |
| `E4` | stringification sweep STR = 1…10 + STRLEN (DuckDB) | Fig. 9 |
| `E4X` | stringification sweep, cross-engine | — |
| `E5` | sparsity & skew sensitivity (low/medium/high) | Table 5 |
| `figures` | (re)render figures + tables from existing results | — |
| `clean` | remove results + rendered artifact (keep generated data) | — |
| `clean-data` | also free regenerable variant data (keep base + results) | — |
| `purge` | full reset: all data + DBs + results (keep engine binaries) | — |

Flags combine with targets, e.g. `./reproduce_EAB.sh --sf100 E5` or
`./reproduce_EAB.sh --quick figures`.

## Measurement protocol

Paper-faithful by default: **1 warmup pass + 10 timed repetitions, median
reported**, 1800 s per-query timeout, one engine at a time. `--quick` drops to a
single repetition. Hardware reference: 2× AMD EPYC 7453 (56 physical cores),
~1 TiB RAM, Ubuntu 24.04.

## Tuning (environment variables)

| Variable | Default | Meaning |
|---|---|---|
| `ENGINES` | `duckdb cedardb monetdb` | which engines to run |
| `REPS` | `10` | timed repetitions (median reported) |
| `THREADS` | `56` | engine threads |
| `TIMEOUT` | `1800` | per-query timeout (seconds) |
| `SF_E1` … `SF_E5`, `SF_E4X` | `10` | per-experiment scale override |

Example — DuckDB only, 3 repetitions, default SF10:

```bash
ENGINES=duckdb REPS=3 ./reproduce_EAB.sh all
```

## Output

Rendered figures and tables are collected under `eab_artifact/`:

```
eab_artifact/
  figures/         # every paper figure (PNG + vector PDF)
  tables/          # every paper table (.tex)
  logs/            # per-experiment run logs
  run_summary.log  # one line per unit + a failure summary
```

Disk stays **bounded**: each stringification/sparsity variant is generated,
measured, and deleted one at a time, with a safety sweep at the end of phases
E4 and E5. Use `clean-data` or `purge` to reclaim regenerable data between runs.

## Robustness

The script is built for an unattended committee run: it runs **one engine at a
time**, cleans each engine's database afterwards, keeps MonetDB daemons tidy, and
uses **continue-and-report** — a failing unit (e.g. a CedarDB out-of-memory or a
MonetDB timeout at SF100) is logged and the run proceeds, with a summary of any
non-zero units printed at the end. A non-zero unit can itself be a result.
