# Appendix E: MCV Skew Profiles

This appendix details the Most-Common-Value (MCV) injection mechanism used by
Prod-DS to model value-skew patterns observed in production datasets.

## Overview

Production datasets exhibit strong MCV dominance: for many columns a single value
occupies a large fraction of all rows. The Redshift fleet ("Why TPC Is Not
Enough", Fig 9) shows roughly **41 %** of columns with a most-common value covering
≥50 % of rows, and ~25 % covering ≥90 %; TPC-DS is far flatter (~14 % and ~9 %).
Prod-DS injects configurable skew so the generated data reproduces this
heavy-tailed behavior.

## The metric being matched

The calibration target is the production **"Maximum MCV frequency"** curve, defined
per column as

```
max_value_share = count(single most-frequent NON-NULL value) / row_count(total)
```

i.e. the dominant value's share of **all** rows, including nulls. NULLs are *not* a
MCV candidate (they are tracked separately by the NULL profile), but they remain in
the denominator — so injecting nulls into a column mechanically dilutes its
`max_value_share`. The injection compensates for this (see below).

## Profile Tuple

An MCV profile is defined as a tuple:

```
M = (f, B_T, E)
```

where:

- `f` -- the column selection fraction.
- `B_T` -- weighted buckets for the **target top-1 (max-value) share** `T`: the
  fraction of *total* rows the single most-common value should occupy after
  injection. This is exactly the quantity the production curve measures.
- `E` -- the exclusion set of query-critical columns (low-cardinality filter
  categoricals and aggregation measures). For MCV this is **always applied** — see
  Eligibility — because concentrating these columns empties queries.

> Earlier revisions used a `(f, B_20, B_r, E)` tuple that targeted a *top-20*
> replacement mass `f_20` and a dominance ratio `r` with **synthetic** values. That
> scheme (a) was not null-compensated, so the realized share fell to `f_1·(1−null)`,
> and (b) overwrote naturally-skewed columns with lower-share synthetic values,
> which could *reduce* a column's max-value share below the TPC-DS baseline. Both
> are fixed by the model below. The legacy `top20_buckets` key is still accepted as
> an alias for `max_value_buckets`.

## Eligibility

A column is MCV-eligible iff:

1. Not a primary key, foreign key, or other key-like / NOT NULL column.
2. **Not in the exclusion set `E`** (`exclude_tables` + `exclude_qualified_columns`).
   Unlike NULL, MCV applies `E` in **both** pools — the hot-path flag does not drop
   it. Concentrating a query-critical column (e.g. `item.i_category`, a filter
   categorical, or `store_sales.ss_ext_sales_price`, an aggregation measure) wipes
   out the values queries select and induces empty results (paper §5.2.6); NULL only
   dilutes such columns, so it can safely include them, but MCV cannot.
3. **Not referenced by the query workload.** Any column named in a WHERE/JOIN/
   GROUP BY/HAVING/aggregation across the 107 queries is excluded
   (`config/query_referenced_columns.txt`, loaded automatically). MCV therefore only
   ever concentrates *payload* columns no query depends on — guaranteeing that value
   skew can never empty a query. This is the decisive lever for the §5.2.6
   constraint: it is *why* the realized curve stays below the production fleet (you
   cannot reach the fleet mid-tail without skewing query-predicate columns).
4. (If an NDV reference DuckDB is supplied) NDV ≥ `--min-ndv-for-injection`
   (default 50). Without a reference DB the cardinality guard is skipped.

MCV-eligible columns are non-key, so they are never stringified — the recast
(STR) axis and the MCV axis are orthogonal.

## Per-Column Assignment

For each eligible column `c`:

### Step 1: Column selection

Hash-based selection with the token `"select-mcv"`, independent of the NULL
profile's selection.

### Step 2: Target draw

Draw a bucket from `B_T` and interpolate to obtain the target top-1 share `T`
(fraction of total rows the dominant value should occupy).

### Step 3: Natural baseline

From a one-time scan of the base data, the column's natural dominant **value** and
its share among non-null cells `s` are known, together with its natural null rate.
Let `g = (1 − null_rate) · (1 − f_null)` be the final non-null fraction of total
rows, where `f_null` is the NULL profile's injected probability for the column.

### Step 4: Solve for the injection rate

The per-cell contract is: a fraction `f_1` of non-null cells is set to the natural
dominant value. The realized max-value share over total rows is then
`g·(f_1 + (1 − f_1)·s)`. Setting this equal to `T` and solving:

```
f_1 = ( T/g − s ) / ( 1 − s )      (clamped to [0, 1])
```

- **Null-compensated:** the `T/g` term cancels the dilution from injected (and
  natural) nulls, so the realized total-row share lands on `T`.
- **Monotonic:** if `T/g ≤ s` the column already meets/exceeds the target once
  nulls are accounted for, `f_1 ≤ 0`, and the column is left untouched — injection
  can only *raise* a column's max-value share, never lower it.
- **Capped:** a column cannot exceed `g` (its non-null fraction); targets above
  `g` saturate at `g`. High-skew targets therefore land on low-null columns.

### Step 5: Per-cell replacement

For each non-null cell with hash value `h`: if `h < f_1`, set it to the natural
dominant value. (The secondary band collapses to `f_1`, so only the single
dominant value is amplified — the natural tail of other values is preserved, and
no synthetic values are introduced.)

## Execution Order

```
NULL injection  -->  Stringification  -->  MCV injection
```

NULL runs first; MCV skips already-nulled cells and compensates for the null
fraction via the `T/g` term above, so the two injections no longer fight each
other. NULL behavior is unchanged from prior revisions.

## Skew Tiers

Prod-DS ships three MCV profiles. They differ in selection fraction `f` and in the
target-share buckets `B_T`. The **medium** tier (`mcv_fleet_default`) is the
calibrated default.

Target top-1 share buckets `B_T` (weights):

| Range `T` [lo, hi) | Low  | Med (default) | High |
|--------------------|-----:|--------------:|-----:|
| [0.01, 0.10)       | 0.30 | 0.03          | 0.01 |
| [0.10, 0.30)       | 0.30 | 0.05          | 0.03 |
| [0.30, 0.50)       | 0.25 | 0.12          | 0.06 |
| [0.50, 0.70)       | 0.12 | 0.35          | 0.25 |
| [0.70, 0.90)       | 0.03 | 0.25          | 0.30 |
| [0.90, 0.99)       | --   | 0.20          | 0.35 |

| Parameter           | Low  | Med (default) | High |
|---------------------|-----:|--------------:|-----:|
| Column fraction `f` | 0.45 | 1.0           | 1.0  |

Only the **medium/default** tier is calibrated to the production fleet; `mcv_low`
and `mcv_high` are heuristic softer/harder variants.

## Calibration target (SF10, STR=5 default)

Share of columns with `max_value_share ≥` threshold. Because MCV only skews
query-untouched columns (Eligibility #3), the realized curve lifts the mid-tail
materially above the TPC-DS base and fixes the old high-end regression, while
staying **deliberately below the production fleet** — you cannot reach the fleet
mid-tail without concentrating query-predicate columns, which empties queries
(paper §5.2.6). This is the intended conservative operating point.

| max-value ≥ | TPC-DS base | old (broken) | Prod-DS (default, measured) | Redshift fleet |
|-------------|------------:|-------------:|----------------------------:|---------------:|
| ≥0.01       | 60.1        | 65.5         | 59.4                        | 73             |
| ≥0.10       | 36.1        | 44.1         | 39.9                        | 60             |
| ≥0.30       | 18.6        | 23.1         | 27.4                        | 49             |
| ≥0.50       | 13.8        | 14.0         | 22.6                        | 41             |
| ≥0.70       | 11.4        | 8.4          | 14.9                        | 33             |
| ≥0.90       | 8.6         | 2.8          | 9.7                         | 25             |

Measured with WorkloadLens on regenerated SF10 STR=5 data. The "old (broken)"
column shows the prior synthetic, non-null-compensated injection, which collapsed
≥0.70/≥0.90 *below* the TPC-DS base. The new injection keeps every value-skewed
column query-safe: all 107 workload queries return non-empty results.

## Configuration Reference

The MCV profile tiers are defined in:

- **`config/mcv_profiles.yml`** -- three shipped tiers:
  - `mcv_low` (low skew)
  - `mcv_fleet_default` (medium/default, calibrated to the fleet)
  - `mcv_high` (high skew)

Each profile specifies `column_selection_fraction`, `max_value_buckets` (target
top-1 share; `top20_buckets` accepted as a legacy alias), excluded tables, and
excluded qualified columns (conservative pool only).

## Implementation Reference

- **`workload/dsdgen/stringify.py`** -- `MCVInjector` (target solve in
  `_resolve_target`, rule build in `_build_rules`), the natural-value scan
  (`_scan_natural_mcv_stats` / `_load_natural_mcv_stats`, cached), BLAKE2b-based
  selection/bucket draws, and per-cell replacement. The C++ backend
  (`stringify_cpp.cpp`) applies the identical per-cell contract from the serialized
  rules.
