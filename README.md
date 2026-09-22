# Prod-DS Kit

Prod-DS Kit is a data- and query-centric extension to TPC-DS that adds
production-realistic string processing, join-graph amplification, NULL
sparsity, most-common-value (MCV) skew, and configurable query complexity
(LIMIT, GROUP BY, UNION ALL fan-in) to the standard benchmark. All extensions
operate at the logical level, enabling engine-agnostic evaluation across
analytical systems.

## Paper Appendices (Technical Documentation)

The full formal specification of every extension, including algorithms,
formulas, and worked examples from the paper appendices:

| Appendix | Document | Contents |
|----------|----------|----------|
| A | [docs/join-amplification.md](docs/join-amplification.md) | Join-graph growth model J(k,m), SQL patterns (LOD/segment CTE), LEFT JOIN rejoin, calibration tables |
| B | [docs/stringification-levels.md](docs/stringification-levels.md) | Two orthogonal knobs — STR (type coverage 1–10, default 5) and STRLEN (string length, default 0); per-level domain map, column counts, padding widths, prefixes |
| C | [docs/column-recast.md](docs/column-recast.md) | 131 recast candidates across 24 tables, semantic categories, selection ordering, usage statistics |
| D | [docs/null-profiles.md](docs/null-profiles.md) | Profile tuple P=(f,B,E), 4-step BLAKE2b assignment, three sparsity tiers with bucket definitions |
| E | [docs/mcv-profiles.md](docs/mcv-profiles.md) | Profile tuple M=(f,B_T,E), max-value target buckets, null-compensated + monotonic injection reusing the natural dominant value, fleet calibration |
| F | [docs/key-skew.md](docs/key-skew.md) | Join-key skew: FK→PK redirection, entity vs ticket granularity, cross-channel anchor, fleet calibration, tier caps |
| -- | [docs/experimental-protocol.md](docs/experimental-protocol.md) | Frozen evaluation protocol (E0-E5 plus E4X), engine versions, host spec, timeout policy, error taxonomy |
| -- | [docs/dialect-adaptations.md](docs/dialect-adaptations.md) | Per-engine SQL rewrites for DuckDB, CedarDB, MonetDB; adding a new dialect |
| -- | [docs/reproducibility.md](docs/reproducibility.md) | What `reproduce.sh` does step by step, and what each flag changes |

## Quick Start

```bash
git clone https://github.com/szlangini/prod-ds-kit.git
cd prod-ds-kit
./install.sh
source .venv/bin/activate
```

### Default Commands

The `--default` flag uses recommended settings so you can get started with a single flag.

```bash
# Generate data (STR=5, STRLEN=0, NULL=medium, MCV=medium, SF=10, output=./output)
python3 wrap_dsdgen.py --default

# Generate queries (STR=5, dialect=duckdb, output=./queries)
python3 wrap_dsqgen.py --default
```

You can override individual defaults:

```bash
# Default settings but at SF=1 (smaller, faster)
python3 wrap_dsdgen.py --default -SCALE 1

# Default settings but at SF=100
python3 wrap_dsdgen.py --default -SCALE 100
```

## For AI Agents

Read [`AGENTS.md`](AGENTS.md) for a structured project overview.

### Agent Prompt (Copy-Paste)

Give this prompt to an AI coding agent (e.g. Claude Code, Cursor, Copilot) to
set up and run Prod-DS Kit end-to-end with DuckDB:

> Clone the Prod-DS Kit repository from https://github.com/szlangini/prod-ds-kit.git
> and run `./install.sh` to build the TPC-DS toolkit. Activate the virtual
> environment with `source .venv/bin/activate`. Verify that `dsdgen` and `dsqgen`
> binaries exist in `tpcds-kit/tools/`. Check that `python3 -c "from workload
> import stringification"` succeeds. Then generate data using
> `python3 wrap_dsdgen.py --default -SCALE 1` (uses STR=5, NULL=medium,
> MCV=medium). Generate queries with `python3 wrap_dsqgen.py --default`.
> Verify that `./output/` contains `.dat` files and `./queries/` contains `.sql`
> files. Report any errors encountered.

## Extension Parameters

### Data generation (`wrap_dsdgen.py`)

| Flag | Values | Default | Effect |
|------|--------|---------|--------|
| `--stringification-level` | 1-10 | 5 | Type coverage — which numeric `_sk` domains are recast to strings (atomic whole-domain, mass-ordered; STR=5 = production optimum, STR=10 = full) |
| `--strlen` | 0, 1, 2, … | 0 | String length — appends `STRLEN×2` filler chars to each stringified value (orthogonal to STR; 0 = natural length) |
| `--stringification-preset` | `vanilla` (STR 1), `low` (STR 3), `medium`/`production` (STR 5), `high` (STR 8), `full` (STR 10) | none | Named shortcut for stringification level |
| `--null-profile` | `low`, `medium`, `high` | none (no NULLs) | Fleet-derived NULL sparsity tier injected into eligible columns |
| `--mcv-profile` | `low`, `medium`, `high` | none (no MCV) | Fleet-derived MCV skew tier injected into eligible columns |
| `-SCALE` | integer | 1 | TPC-DS scale factor (1, 10, 100, ...) |
| `-DIR` | path | required | Output directory for `.dat` files |

### Query generation (`wrap_dsqgen.py`)

| Flag | Values | Default | Effect |
|------|--------|---------|--------|
| `--default` | flag | off | Use recommended defaults: STR=5, dialect=duckdb, output=./queries |
| `--output-dir` | path | required | Output directory for generated SQL files (optional with `--default`) |
| `--stringification-level` | 1-10 | none | Activates extended templates and literal post-processing for the given STR level |
| `--strlen` | 0, 1, 2, … | 0 | String-length amplification of stringified literals (orthogonal to STR) |
| `--no-extensions` | flag | off | Use base TPC-DS templates only (skip `*_ext.tpl`) |
| `--dialect` | `ansi`, `duckdb` | `ansi` | SQL dialect for dsqgen output |
| `--join` / `--no-join` | flag | on | Include/exclude join-amplified queries |
| `--join-targets` | comma-separated ints | `50,100,200` | Target effective join counts for generated join queries |
| `--union` / `--no-union` | flag | on | Include/exclude UNION ALL fan-in queries |
| `--union-max-inputs` | integer | no cap | Cap maximum UNION ALL fan-in branches |
| `--pure-data-mode` | flag | off | Disable query-layer rewrites (for data-only stringification evaluation) |
| `--scale` | integer | `1` | Scale factor passed to dsqgen |

### Seeds (the default configuration is fully pinned)

Prod-DS is deterministic end to end; the shipped defaults are part of the
benchmark definition, so everyone tests the same thing out of the box:

- **Query parameters:** dsqgen's default seed (`19620718`) plus the shipped
  per-scale-factor seed overrides `configs/seed_overrides_sf{SF}.yml`.
  `wrap_dsqgen.py` applies the file for the requested `--scale` automatically,
  for every stringification level and every dialect (a parameter draw is empty
  because of the *data*, not the SQL flavour), so the default workload works
  out of the box — no flag, no search. The overrides replace only the handful
  of parameter draws that return empty results on the skewed default data:

  | SF | pinned templates | gate (`tools/query_gate.py`, 107-query workload) |
  |----|------------------|--------------------------------------------------|
  | 1  | 9 (`query_8/24/30/41/44/54/68/82/91`) | 106/107 — `query_4` (T4) stays empty: its three-channel repeat-customer population is 3 rows at 1 GB (documented exception) |
  | 10 | 5 (`query_8/24/30/44/68`)      | **107/107**, 0 errors, 0 newly empty vs. the data without key skew |
  | 100 | 3 (`query_24/37/44`)          | **107/107**, 0 errors, 1 newly empty vs. the data without key skew (`query_24`, pinned) |

  This is the **no-empty guarantee**: with the shipped defaults every query of
  the workload returns rows at SF10 and SF100. Without the overrides (pure
  default draw) the key/MCV/NULL skew shifts selectivity for a few templates per
  scale factor (5 at SF10, 3 at SF100) — that is a property of the skew, not a defect, and the pinned
  seeds make it invisible to users. The files are kept minimal (only templates
  whose default draw is empty at that scale factor) and are regenerated with
  `tools/find_nonempty_seeds.py --no-base-fallback` (an `_ext` template that
  stays empty across the seed budget is a template defect, not a seed problem);
  `tests/test_shipped_seed_overrides.py` guards the shipped files. Per-STR or
  per-dialect files (`seed_overrides_sf{SF}_str{STR}[_{dialect}].yml`) are still
  honoured on top for experiments, but none are shipped. dsqgen permutes
  templates into output positions — `_permutation.json` in each query dir maps
  template → file.
- **Data-side injections:** NULL, MCV, and key skew each use seed `0` by
  default (overridable via `--null-seed` / `--mcv-seed` / `--key-skew-seed`).
  A fourth seed, `stats_seed`, pins the length-statistics sampling inside
  stringification; it is fixed at `0` and has no flag. All four are written into
  `stringification_data_manifest.json` beside the generated data, so what a
  dataset was built with can be read off the dataset itself.
  Identical inputs produce byte-identical data, on both the Python and C++
  backends.

If you change any seed, re-validate non-emptiness for your setup
(`tools/query_gate.py`, `tools/find_nonempty_seeds.py`).

### Profile tiers

| Dimension | Low | Medium (default) | High | Config file |
|-----------|-----|-------------------|------|-------------|
| NULL sparsity | ~5% columns, light rates | ~30% columns, fleet-derived rates | ~60% columns, heavy rates | `config/null_profiles.yml` |
| MCV skew | milder targets, role-scoped | fleet-calibrated targets, role-scoped (jointly with key skew: mean gap ~4pp to the Redshift curve) | strong targets, extreme tail | `config/mcv_profiles.yml` |
| Join-key skew | mild, capped 0.80 | fleet-shaped, capped 0.80, date-family FKs vanilla | uncapped extremes (stress) | `config/key_skew_profiles.yml` |
| Stringification | vanilla (STR 1): 0 columns recast | production (STR 5): 47 columns | full (STR 10): 131 columns | `config/string_profiles.yml` |

### Benchmark runner (`python -m experiments run`)

| Flag | Values | Default | Effect |
|------|--------|---------|--------|
| `--config` | path | required | YAML config file (see `experiments/config.example.yaml`) |
| `--experiment` | `workload_compare`, `join_scaling`, `union_scaling`, `string_sweep` | required | Which experiment to execute |
| `--system` | `duckdb`, `cedardb`, `monetdb` | required | Target engine |

See `experiments/config.example.yaml` for the full configuration schema
including threading, memory limits, timeouts, and repetition counts.

## What Prod-DS Adds to TPC-DS

### Stringification (STR 1–10 + STRLEN)

Recasts up to 131 integer columns (surrogate keys, demographic keys, time
keys, codes) to variable-length strings. Forces hash joins, disables integer
fast-paths, and inflates working-set sizes. STR=1 is vanilla TPC-DS; STR=5
(the default) is the production-realistic optimum; STR=10 covers all 131
columns. A separate STRLEN knob (≥1) extends per-value string length
independently, without changing which columns are recast.

### Join-Graph Amplification

Generates CTE-based queries with tuneable join counts (1 to 2048+). Uses a
two-level design: base blocks replicated across LOD and segment branches, with
a LEFT JOIN rejoin pattern. The join count follows the formula
J(k,m) = (m+1) * J0(k) + m, where J0(k) = b + k*(b+1).

### UNION ALL Fan-In Scaling

Generates standalone UNION ALL queries with 2, 5, 10, 20, or 200 branches
over a shared base CTE (store_sales/date_dim/store), each branch filtering
on a different month. Stresses materialisation buffers, hash-table sizing,
and scan concurrency at scale.

### NULL Sparsity Injection

Injects NULL values into eligible non-key columns using fleet-derived
probability distributions. Three tiers (low, medium, high) control the
fraction of affected columns and per-cell NULL rates. Assignment is
deterministic via BLAKE2b hashing. A **small-dimension floor** keeps at least
4 naturally non-NULL rows per nulled column (exact hash threshold, Python/C++
identical): a size-independent 0.9 probability would otherwise leave 0–1 rows
on 5–15-row dimensions such as `warehouse` and empty every predicate on them.
See [docs/null-profiles.md](docs/null-profiles.md).

### MCV Skew Injection

Amplifies each eligible column's **natural dominant value** up to a target
max-value share drawn from the profile, reproducing the production
"Maximum MCV frequency" curve. The injection is **null-compensated** (it targets
the share over *total* rows, undoing null dilution) and **monotonic** (it only
raises a column's max-value share, never lowers it). It is **query-safe by
role**: only columns the workload compares against literals are excluded
(`config/query_filter_columns.txt`) — concentrating those could empty results —
while pure GROUP BY / ORDER BY / aggregate-input columns are skewable (their
concentration shrinks groups but cannot empty a query). Jointly calibrated with
the join-key axis, the realized curve tracks the Redshift fleet across the whole
range (mean gap ≈ 4 pp at SF10). Three tiers (low, medium, high) set the target
distribution. Execution order: NULL injection, then key skew, then
stringification, then MCV injection. See
[docs/mcv-profiles.md](docs/mcv-profiles.md).

### Join-Key Skew Injection

Redirects a fleet-calibrated share of fact-table **date/customer/store foreign
keys** to each channel's natural dominant key, creating join-fan-out,
aggregation, and order-by skew — the key columns the NULL/MCV profiles must
exclude. FK→PK redirection preserves referential integrity and can never empty
an equi-join; redirect decisions hash the ticket/order identity shared between
sales and returns files, so cross-fact joins and naturally-equal `sr_*`/`ss_*` columns stay
coherent. Customer keys decide per customer rather than per ticket: a share of
customers is absorbed by the hot key in every channel while the rest keep their
complete purchase histories, so year-over-year and cross-channel customer
queries keep their populations. Enabled by default like NULL/MCV (`--disable-key-skew` turns it off,
`--key-skew-profile low|medium|high` selects the tier) and freely composable
with the other axes. See [docs/key-skew.md](docs/key-skew.md).

### Extended Query Templates

92 extended templates (*_ext.tpl) that rewrite GROUP BY, LIMIT, and filter
predicates to reference stringified columns where appropriate. Each template
is automatically selected based on the active stringification level.

Literal predicates in the templates are drawn from dsqgen's distributions the
same way dsdgen draws the data (store/warehouse geography from the
scale-dependent `active_counties` / `active_cities` prefixes, customer cities
from the weighted `cities` distribution, countries from `countries`, brand
prefixes from `brand_syllables`), never hand-written: values such as `'CA'`,
`'Seattle'`, `'United States'` or `'Brand#1%'` do not occur in dsdgen output at
any scale factor and would silently empty a query.
`tests/test_ext_template_literals.py` enforces this.

## Reproducing Paper Experiments

A single script reproduces the experiments (E0–E5, plus E4X) from the paper:

```bash
# Quick validation (SF=1, ~15 min, DuckDB only)
./reproduce.sh --init --experiment E0 --sf 1
./reproduce.sh --all --sf 1

# Full reproduction (SF=10, all engines)
./reproduce.sh --init --experiment E0 --sf 10 --engines all
./reproduce.sh --all --sf 10 --engines all --reps 10
```

**Run E0 first.** `--all` expands to `E1 E2 E3 E4 E5` and does **not** include the audit pass.
E0 writes `common_subset.json`, the intersection of queries every engine can run; without that file
E1 and E5 silently fall back to the full query set and measure a different population than the
paper. E4 and E4X run at SF10 by design — ten stringification levels at SF100 would be ~500 GB.

This generates data, queries, dialect-specific fixes, runs the experiments, and produces the paper
figures.

See [REPRODUCIBILITY.md](REPRODUCIBILITY.md) for the full reviewer guide.

## Supported Engines

DuckDB, CedarDB, MonetDB.

Engine adapters are in `experiments/adapters/`. Adding a new engine requires
implementing the adapter interface in `experiments/adapters/base.py`.

## Running Tests

```bash
source .venv/bin/activate
pytest tests/ -v
```

Tests that require the TPC-DS toolkit (built via `install.sh`) are marked
with `@pytest.mark.needs_tpcds_tools` and will be skipped automatically
if the toolkit is not present. Tests requiring a C++ compiler are marked
with `@pytest.mark.needs_cpp`.

## License

Prod-DS Kit is released under an academic non-commercial license.
See [LICENSE](LICENSE) for the full terms.

The underlying TPC-DS toolkit is subject to the
[TPC End User License Agreement](https://www.tpc.org/tpc_documents_current_versions/pdf/tpc_eula_v2.2.0.pdf)
and is fetched separately during installation. See [NOTICE.md](NOTICE.md)
for third-party attributions.
