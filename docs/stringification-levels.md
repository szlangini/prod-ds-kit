# Appendix B: Stringification — Type Coverage (`STR`) and String Length (`STRLEN`)

This appendix describes the two orthogonal stringification knobs and their effect
on schema recasts, data post-processing, and query-template extensions.

Stringification is controlled by **two independent parameters**:

| Knob     | Range  | Default | Controls                                            |
|----------|--------|---------|-----------------------------------------------------|
| `STR`    | 1–10   | **5**   | *Type coverage* — **which** numeric `_sk` domains become `varchar`. |
| `STRLEN` | 0–…    | **0**   | *String length* — **how long** each stringified value is (payload amplification). |

The two axes are orthogonal: `STR` decides the set of columns that are converted
to text, and `STRLEN` independently scales the byte length of those text values
without changing which columns are affected. This replaces the previous single
`[1, 15]` axis (where type coverage and payload length were entangled and the
`STR 1 → 2` step caused a ~48 pp coverage cliff).

`STR = 5` is the **default** and corresponds to the production-realistic optimum
(it is the configuration evaluated in the paper). `STR = 1` is unmodified TPC-DS;
`STR = 10` is full type coverage. `STRLEN = 0` is the natural value length.

---

## 1. `STR` — Type-Coverage Axis (1–10)

### Atomic whole-domain selection

`STR` does **not** recast an arbitrary number of individual columns. Instead it
turns on **whole join domains**, one at a time, in order of decreasing *join
mass* (how frequently the domain's key participates in equi-joins across the
workload). A "domain" is the full set of PK/FK columns that share a key space —
e.g. the `date` domain is `d_date_sk` plus **every** `*_date_sk` foreign key in
every fact and dimension table.

Selecting domains atomically (all columns of a domain move together, on both
sides of every join) guarantees that the two operands of every equi-join always
have the **same type**. There are therefore **no mixed-type joins** — the
historical correctness bug that strict engines (DuckDB, Umbra, CedarDB) reject.

Domains are added cumulatively as `STR` increases:

| `STR` | Domain added at this level         | Cols (cum. /131) | Pad | Sample value (orig. `42`) |
|------:|------------------------------------|-----------------:|----:|---------------------------|
|     1 | *(none — unmodified TPC-DS)*       |                0 |  —  | `42`                      |
|     2 | `date` (`*_date_sk` → `d_date_sk`) |               23 |   4 | `D_0042`                  |
|     3 | `item`                             |               32 |   5 | `i00042`                  |
|     4 | `customer`                         |               44 |   5 | `c00042`                  |
|   **5** | **`store`  ← DEFAULT (production optimum)** |       47 |   6 | `s000042`                 |
|     6 | `addr` (address)                   |               59 |   6 | `c000042`                 |
|     7 | `hdemo` (household demographics)   |               71 |   7 | `h0000042`                |
|     8 | `cdemo` (customer demographics)    |               83 |   7 | `c0000042`                |
|     9 | `time` + `income_band`             |               92 |   8 | `T_00000042`, `i00000042` |
|    10 | *all remaining* (`reason`, `promo`, `ship_mode`, `call_center`, `catalog_page`, `web_*`, `warehouse`, low-mass singletons) — **full coverage** | 131 | 8 | `r00000042`, `p00000042`, … |

Notes on the table:

- **Cols** is the cumulative number of columns recast to `varchar`, out of 131
  candidate columns across 24 tables.
- **Pad** is the minimum zero-padding width applied to all stringified values at
  that level (values whose natural width exceeds the pad keep their width — e.g.
  `d_date_sk = 2451813` stays `D_2451813`). All stringified columns at a given
  level share that level's pad width.
- **Sample value** shows the *newly added* domain's textual form for an original
  integer `42`, using that domain's prefix. Each domain has a distinct prefix so
  that columns sharing a key space stringify into the same text key space and
  joins remain valid: `date → D_`, `time → T_`, `item → i`, `income_band → i`,
  `customer`/`addr`/`cdemo`/`call_center`/`catalog_page → c`, `store`/`ship_mode → s`,
  `hdemo → h`, `reason → r`, `promo → p`, `web_*`/`warehouse → w`.
- The `date` domain is the largest single jump (+23 columns) and is irreducible:
  every `*_date_sk` joins the single `d_date_sk`, so the whole domain must move as
  one unit. This is the smallest possible first step and is why the old
  `STR 1 → 2` cliff cannot be made arbitrarily small.

### Query-template extensions

At **every** `STR` level, all eligible query-template overrides (`queryN_ext.tpl`)
are enabled. The `_ext` templates are **SQL-correctness overrides** — they fix
reserved-word aliases (`at`, `returns`) and ambiguous unqualified columns that
strict parsers reject — and are therefore orthogonal to type coverage; they are
applied regardless of `STR`. What *does* vary with `STR` is the **literal
formatting** inside those queries: a predicate on a domain that is stringified at
the current level emits a quoted text literal (`r_reason_sk = 'r00000001'` at
`STR 10`), while the same predicate emits a numeric literal (`r_reason_sk = 1` at
`STR 5`, where `reason` is not yet stringified). The query set is thus identical
across levels; only the literal types track the schema.

---

## 2. `STRLEN` — String-Length Axis (0, 1, 2, …)

`STRLEN` independently amplifies the byte length of the stringified values
selected by `STR`. It appends a separator and filler characters **after** the
zero-padded value, leaving the join key (the prefix + padded number) intact, so
amplification never breaks joins or changes which columns are text.

The appended payload is `separator + marker × (STRLEN × pad_step)`, with defaults
`separator = "~"`, `marker = "X"`, `pad_step = 2`. Hence the extra width is
`STRLEN × 2` characters:

| `STRLEN` | Extra chars | Sample (`store`, orig. `42`, at `STR 5`) |
|---------:|------------:|-------------------------------------------|
|        0 |           0 | `s000042`               *(natural — DEFAULT)* |
|        1 |           2 | `s000042~XX`                              |
|        2 |           4 | `s000042~XXXX`                            |
|        3 |           6 | `s000042~XXXXXX`                          |
|        5 |          10 | `s000042~XXXXXXXXXX`                      |

The suffix is added to **only** the columns that `STR` has stringified; numeric
columns are untouched. For example at `STR 5, STRLEN 3` a `store_sales` row reads:

```
D_2451813~XXXXXX | 65495 | i034435~XXXXXX | c167006~XXXXXX | 591617 | … | s000040~XXXXXX | …
└ date (text+amp)   └ num   └ item (text+amp)  └ customer        └ num     └ store (text+amp)
```

> **Width limit:** stringified columns are declared `varchar(32)`. On
> length-enforcing engines this bounds `STRLEN` to the width left after the
> padded value (≈ 11 for the widest base keys); DuckDB does not enforce
> `varchar` length, so larger `STRLEN` still loads there.

`STRLEN` isolates the effect of string *payload size* from type *coverage*: hold
`STR` fixed and vary `STRLEN` to study how value length alone affects an engine
(hashing, comparison, storage), without changing the set of typed columns.

---

## 3. Choosing values

- **Reproducing the paper / production-realistic runs:** `STR = 5`, `STRLEN = 0`
  (the defaults). This is the production optimum on the type-mix distance metrics.
- **Unmodified TPC-DS baseline:** `STR = 1`.
- **Maximum type coverage:** `STR = 10`.
- **String-length sensitivity study:** fix `STR` (e.g. 5) and sweep
  `STRLEN ∈ {0, 1, 2, 3, …}`.

The presets map to levels as: `vanilla = 1`, `low = 3`, `medium = production = 5`,
`high = 8`, `full = 10`.

---

## 4. Implementation Reference

- **`workload/stringification.py`** — defines the type-coverage policy:
  `STR_LEVEL_DOMAINS_ADDED` (the per-level domain map above),
  `_level_schema_selection()` (cumulative atomic-domain selection;
  `STR ≥ 10` ⇒ all candidates), `PRESET_LEVELS` (`production = 5`, `full = 10`),
  and `build_stringification_config(level=…, strlen=…)` which sets
  `amplification_extra_pad = STRLEN × pad_step`.
- **`workload/dsdgen/stringify.py`** — applies the recasts, zero-padding, domain
  prefixes (`DOMAIN_SUFFIX_PREFIXES`, `CUSTOM_PREFIXES`), and the `STRLEN`
  filler-character suffix (`amplify_string()`) to generated `.dat` files.

The CLIs expose both knobs:

```bash
# data generator
python wrap_dsdgen.py   --stringification-level 5 --strlen 0 -SCALE 10 -DIR ./out
# query generator
python wrap_dsqgen.py   --stringification-level 5 --strlen 0 --scale 10 --dialect duckdb --output-dir ./q
# schema only
python tools/generate_tpcds_schema.py --stringification-level 5 --out schema.sql
```

`--stringification-level` also accepts a preset name (`--stringification-preset production`).
