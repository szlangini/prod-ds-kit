# Archived experiment results (Prod-DS Kit)

Measurement data behind the paper's figures/tables, so a fresh clone can inspect
and (where the per-query tree survives) re-render them without re-running the
multi-day experiments. Complements the generators + `reproduce_EAB.sh` and
addresses the PVLDB EA&B "all experimental data available" requirement.
Integrity: each scale directory carries a `checksums.sha256`.

## What backs each paper artefact

| Paper artefact | Exp. | Scale | Engines | Data here |
|----------------|------|-------|---------|-----------|
| Fig. 7 (total runtime), Fig. 8 (per-query CDF), Table 4 (errors) | E1 | SF100 | all 3 | `sf100/{prodds_common90,tpcds_common98}_*`, `sf100/error_categories_*` |
| **Fig. 9** (stringification sweep) | E4 | **SF10** | DuckDB | `sf10/E4` |
| Fig. 10 (join-scaling)             | E2 | SF100 | all 3 | `sf100/join_scaling_*` |
| Fig. 11 (UNION fan-in)             | E3 | SF100 | all 3 | `sf100/union_scaling_*` |
| Table 5 (sparsity & skew)          | E5 | SF100 | all 3 | **lost** — only an SF10/DuckDB remnant in `sf10/E5`; regenerate with `reproduce_EAB.sh --sf100 E5` |

The paper's **Table 3** ("limit magnitude") is a WorkloadLens / coverage artefact —
reproduced in the WorkloadLens repo, not here.

## `sf100/` — the SF100 paper data (3 engines)

Protocol-faithful aggregate CSVs (56 cores · 1 warmup + 10 timed reps · median) for
E1–E3 and the failure taxonomy. The per-query SF100 raw tree was not retained; these
aggregates (per-query medians, raw times, workload totals, error categories) are what
the SF100 figures were rendered from. Each CSV has an `engine` column covering DuckDB,
CedarDB and MonetDB. (Cross-bench CDF latencies for Fig. 8 are in
`../s7_cdf/s7_latencies_sf100.csv`.)

## `sf10/` — stringification (paper) + supplementary fast-repro

Verbatim harness output: one directory per experiment / variant / engine / run, each
leaf with `raw.jsonl` (one record per query × repetition) and `summary.csv`; each run
has a `manifest.json` (harness git hash, host, engine version, scale, protocol).
Protocol: **56 threads · 1 warmup · 10 timed reps · median · 1800 s timeout**.

- **`E4/`** — DuckDB stringification (STR1–10 + STRLEN) = the paper's **Fig. 9** (SF10).
- `E1/ E2/ E3/ E4X/ E5/` are **SF10 supplementary** data: what the default
  `reproduce_EAB.sh all` (SF10) produces for fast iteration. The paper reports these
  experiments at **SF100** (see `sf100/`), not from here. `E5/` is DuckDB-only and is
  the only surviving sparsity/skew remnant (the paper's Table 5 is SF100, 3 engines).

## Render

```bash
.venv/bin/python experiments/plot_results.py \
    --results-dir experiments/data/results/sf10 --output-dir /tmp/figs
# or:  ./reproduce_EAB.sh figures   (falls back to this archive when
#      .reproduce/sf*/results is absent — e.g. on a fresh clone)
```

Note: the in-repo figure generators emit legacy output filenames (`fig13_*`,
`table3_*`, …) that predate the paper's final numbering — match outputs to the paper
by experiment/content per the table above, not by filename.
