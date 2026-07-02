# Appendix C: Column Recast Justification

This appendix describes the rationale, selection criteria, and categorization of
the 131 candidate columns eligible for stringification in Prod-DS.

## Motivation

Large-scale workload studies consistently report that columns which TPC-DS defines
as integers -- surrogate keys, foreign keys, and coded identifiers -- are stored
as strings in production schemas. Variable-length strings are the dominant column
type in production, commonly encoding surrogate keys (UUIDs, ISBNs) and values
that could be typed as integers or booleans (e.g., "Y"/"N", "0"/"1").

In contrast, TPC-DS defines comparable domains as integers. To close this gap,
Prod-DS introduces a schema-level stringification pass that recasts a curated set
of categorical and identifier-like attributes from integer to `varchar`.

## Candidate Selection

### Scope

We identify every TPC-DS column whose base type is numeric but could plausibly be
a text type in a production schema (e.g., `varchar(32)`). This yields **131
candidates across 24 tables**.

These 131 are the *candidate pool*, not the set recast at every level. The active
`STR` (type-coverage) level selects a cumulative subset — whole join-domains at a
time — so the default `STR = 5` recasts **47 of the 131** and full coverage
(`STR = 10`) recasts all 131. String *length* is controlled separately by the
orthogonal `STRLEN` knob (it never changes *which* columns are recast). See
[Appendix B](stringification-levels.md) for the per-level counts.

### Inclusion rules

A column is included if it acts as an identifier or categorical code:

- Surrogate keys (`_sk` suffix columns)
- Foreign keys referencing dimension tables
- Classification and categorical codes (`_id` suffix columns for brand, class,
  category, manager, etc.)

### Exclusion rules

The following column types are excluded from recasting:

- **Additive numeric measures** (prices, quantities, profits) -- recasting would
  be semantically incorrect since they serve as aggregation operands.
- **Temporal columns** (`d_year`, `t_hour`, etc.) -- used as range-predicate
  targets and aggregation operands.

## Semantic Categories

The 131 recast candidates are grouped by semantic category (counts derived
directly from the schema; 120 surrogate/foreign `_sk` keys + 11 `_id` codes):

| Category             | Count | Examples                 |
|----------------------|------:|--------------------------|
| Surrogate key        |    40 | `s_store_sk`             |
| Demographic key      |    38 | `c_customer_sk`          |
| Time key             |    30 | `d_date_sk`              |
| Geographic key       |    12 | `ca_address_sk`          |
| Classification code  |     4 | `i_brand_id`             |
| Categorical code     |     7 | `i_manager_id`           |
| **Total**            |   131 |                          |

## Usage Statistics

Measured over the 107-query workload by classifying each query's AST (a candidate
is counted in a role if it appears in that role in at least one query), of the 131
recast candidates:

- **72.5 %** (95 of 131) are referenced by the workload at all; the other 27.5 %
  are payload columns no query touches.
- **67.9 %** (89) appear as **equi-join keys** — in a `column = column` predicate.
- **9.2 %** (12) appear in a **filter predicate** — compared to a constant, or in
  an `IN` / `BETWEEN` / `LIKE` / `IS NULL` restriction.
- **19.1 %** (25) appear in a **`GROUP BY`** clause.

The recast set is thus dominated by **join keys**: stringifying it loads the
operators where surrogate and foreign keys actually do their work — equi-joins and
the hash/sort machinery behind them. Comparatively few candidates surface in
filters or `GROUP BY`, because most TPC-DS restriction and grouping predicates fall
on categorical and temporal columns, which are already strings or are excluded from
recasting (see Exclusion rules above).

## Selection Order

Candidates enter the recast set as **whole join domains** (not individual
columns), one domain per level, ordered by decreasing *join mass* -- how
frequently the domain's key participates in equi-joins across the workload. A
domain is the complete set of PK/FK columns that share a key space, so both
operands of every equi-join are recast together and joins never become
mixed-type:

1. **STR 2**: the `date` domain (`d_date_sk` plus every `*_date_sk` foreign key,
   including the fact-table date keys) -- the highest-mass domain and the largest
   single step (+23 columns).
2. **STR 3--5**: `item`, then `customer`, then `store` (STR 5 is the default,
   production-realistic optimum). Fact-table foreign keys are recast together with
   their domain, so the large fact tables are stringified early -- not deferred.
3. **STR 6--9**: `addr`, `hdemo`, `cdemo`, then `time` + `income_band`.
4. **STR 10**: all remaining domains (`reason`, `promo`, `ship_mode`,
   `call_center`, `catalog_page`, `web_*`, `warehouse`, and low-mass singletons)
   -- full coverage, 131 columns.

See [stringification-levels.md](stringification-levels.md) for the per-level
column counts and sample values. The full per-column candidate list is included
in the artifact and can be inspected via the `explain-stringification` CLI
command.

## Implementation Reference

- **`workload/dsdgen/stringify.py`** -- contains the curated column lists,
  custom prefix mappings (e.g., `BRAND_`, `TKT_`, `ORD_`), and domain-suffix
  prefix rules that ensure PK/FK columns sharing the same domain stringify to
  the same textual key space across tables.
- The **`explain-stringification`** CLI subcommand prints the full column
  selection for any given `STR` level, including the recast order, prefix
  assignments, and affected query templates.
