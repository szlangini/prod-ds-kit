# Paper Figure 9 — cross-benchmark runtime CDF

> **Where this sits.** This directory produces **paper Figure 9 (Section 6.5)** and the full-width
> export of the same plot. `REPRODUCIBILITY.md` in the repository root has the reproduction
> commands. It needs no database, no engine and no measurement run — the records travel with it:
> `python3 make_cdf_figure_final.py`, then `python3 verify_cdf_exports.py`.
>
> **`experiments/plot_cdf_crossbench.py` is a different, historical experiment** and does not
> produce this figure.

# Cross-benchmark runtime CDF

For the meta-review's MR2 and reviewer R6's W1/D1: whether Prod-DS exposes system behaviour that
other production-oriented benchmarks do not, shown by running them on the same machine and the same
engine and comparing their per-query runtime distributions.

**Which instance of each benchmark, and the rule that selected its queries, are in
`queries/<suite>/_selection.json` beside each frozen query set. Those were written and fixed before anything was
timed.** This file is the protocol, the results and the limits.

Run 16 September 2026 on helios, kernel 7.0.0-30, DuckDB 1.4.4, 56 threads, 700 GB memory limit,
1 h 39 min of machine time, **25,666 attempts, zero failures**. `./run_final.sh` reproduces it.

The earlier single-repetition pilot is preserved beside this run as `results/latencies_pilot.csv`
and `figures/cdf_crossbench_pilot.*`. Where the two disagree, the final run is right; the pilot's
errors are listed at the end.

## Protocol

- **Ten timed passes after one untimed warmup pass**, pass-major: every query runs once per pass,
  in a fixed order, before the next pass starts. Each pass is therefore a real workload pass.
- One connection per suite, reused across passes. Session settings are applied once, outside any
  timed query, and **read back to confirm they took effect**.
- **Measured**: wall clock from submitting the query to its complete result being materialised in
  the client — `execute()` plus `fetchall()`. It includes result transfer and client-side
  materialisation; it excludes connection setup and reading the query file.
- **Per query**: the median of its ten timed runs. **The CDF contains only queries that succeeded
  in all ten passes**; partial successes, errors and timeouts are reported separately and never
  averaged in. Percentiles use numpy's default linear interpolation.
- 300 s per-query cap. Timeouts would be censored observations, never successful 300 s queries.
  Nothing came close: the slowest query in the whole run is 168.5 s.
- The write suite is the exception. Its mutations are not idempotent, so it gets a **single** timed
  pass on a scratch copy of its database and no warmup, and it is not part of the analytical CDF.

## What was measured

| suite | kind | data GiB | queries | complete | instances in CDF | median | P90 | P99 | max | spread |
|---|---|---|---|---|---|---|---|---|---|---|
| **Prod-DS (default, 107)** | read | 11.43 | 107 | 107 | 107 | 106 ms | 491 ms | 91.6 s | 168.5 s | **9,081x** |
| **Prod-DS, 99 templates** | read | 11.43 | 99 | 99 | 99 | 103 ms | 312 ms | 1.10 s | 20.7 s | **1,113x** |
| TPC-DS | read | 11.36 | 99 | 99 | 99 | 76 ms | 285 ms | 790 ms | 0.94 s | 68x |
| DSB | read | 11.79 | 104 | 104 | 104 | 49 ms | 113 ms | 223 ms | 0.24 s | 22x |
| JOB | read | 3.61 | 113 | 113 | 113 | 60 ms | 128 ms | 201 ms | 0.49 s | 49x |
| Redbench (Krid et al.) † | read | 3.61 | 763 | 763 | **8,784** | 95 ms | 189 ms | 309 ms | 0.52 s | 30x |
| SQLBarber | read | 3.61 | 1000 | 1000 | 1,000 | 7 ms | 20 ms | 25 ms | 0.03 s | 20x |
| Redbench (Wehrstein), reads | read | 3.61 | 62 | 62 | 62 | 48 ms | 78 ms | 107 ms | 0.11 s | 4x |
| Redbench (Wehrstein), writes | **write** | 3.61 | 938 | 938 | — | 2 ms | 4 ms | 7 ms | 0.01 s | 8x |

† **Krid's curve is weighted per-query latencies, not a replay.** Each of the 763 distinct SQL
files was measured once under the common protocol, and the curve repeats each median by the number
of times that file appears across the 30 workloads. It reproduces the *distribution* over the 8,784
instances; it does **not** reproduce the original order, and so does not carry whatever cache
behaviour the real interleaving would produce. The distinct-file view is in
`results/final/query_summary.csv`, and `queries/redbench_krid/workload_order.csv` carries the
original workload identity, position and Redset query id for every one of the 8,784 instances, so a
sequential replay is possible later. None was done here. Avoid saying all seven workloads were
"replayed under a common protocol" — six were executed as declared, and this one was weighted.

`figures/cdf_crossbench_summary_final.csv` is the same table with every column;
`results/final/query_summary.csv` has one row per query with its median, min, max and IQR;
`results/final/pass_totals.csv` has all ten pass totals per suite; `results/final/failures.csv` is
empty, which is itself a result.

**JOB is 113/113 here.** In the pilot its queries 15a–d failed: they alias `aka_title AS at`, and
`at` is reserved in DuckDB. The syntax-only rename is in `patches/job_15_reserved_alias_at.patch`
and is applied to Redbench/Krid's copies of the same four queries too.

**Data volume, and why two numbers.** The host filesystem compresses, so the size of the source
files on disk and their uncompressed size differ by more than a factor of two; both are in
`results/final/manifest.json`. **Neither is bytes scanned by a query.** They are the size of the
generated `.dat`/source files, before loading; what an engine actually reads depends on its storage
format, its compression and the columns a query touches, and none of that was measured. Use them to
compare the *size of the datasets*, not the work a query does. Equal scale-factor labels also do
not mean equal size: DSB at SF10 is larger than TPC-DS at SF10, and **IMDb is not an SF10 database
at all** — JOB, both Redbenches and SQLBarber share it.

## What the distributions say

**In the body of the distribution the analytical suites are close, and Prod-DS is not the slowest.**
Medians: SQLBarber 7 ms, Wehrstein reads 48 ms, DSB 49 ms, JOB 60 ms, TPC-DS 76 ms, Redbench/Krid
95 ms, Prod-DS templates 103 ms. Ignoring SQLBarber, a factor of about two covers all of them.
Whatever distinguishes these benchmarks is not visible below the median.

**The discriminator is spread, and it is not close.** Minimum to maximum: Prod-DS spans **9,081x**,
and **1,113x even with its eight amplification queries removed**. No other suite exceeds **68x**.
That is a sixteen-fold difference in dynamic range against the widest comparator, before Prod-DS's
deliberate amplification is counted at all.

**The tail is the design element, and the figure shows both curves for that reason.** The eight
join and union amplification queries are **91.77 %** of Prod-DS's total runtime — the 200-way union
alone is 168.5 s and 40.8 % of the total. Note what the endpoint includes: the CDF measures
`execute()` plus `fetchall()`, so **result transfer and client-side materialisation are part of
every number**, and that union returns **87,390,461 rows**. These times therefore are not
engine-internal execution costs, and the tail claim is specific to these selected instances and
this endpoint. Without them Prod-DS's P99 is **1.40x** TPC-DS's, not
116x. The claim the data supports is that **Prod-DS reaches runtimes the other suites never reach,
and that this is a deliberate design element rather than an emergent property of its 99 templates**.

**The tail is longer even without the micro-suite, and the comparison there is paired.** The
slowest Prod-DS *template* is `query_46` — and `query_46` is the slowest TPC-DS query too. Both are
TPC-DS template **Q51**, generated by the same generator run, and the workloads use the same STREAMS
permutation. So this is one template measured under two data configurations:

| | TPC-DS | Prod-DS | factor |
|---|---|---|---|
| `query_46` (template Q51) | **0.944 s** | **20.66 s** | **21.9x** |

Unlike E1, which compares 99 TPC-DS queries against 97 Prod-DS ones, this is a like-for-like
comparison of the same query. The amplification queries extend a tail that already exists; they do
not create it. Note that `query_46` is a template, not an amplification query; the pilot's write-up
conflated the two.

**Prod-DS is the only suite whose tail is concentrated.** Share of a suite's total runtime taken by
its single slowest query:

| Prod-DS | TPC-DS | JOB | DSB | Wehrstein reads | Krid | SQLBarber |
|---|---|---|---|---|---|---|
| **40.8 %** | 7.2 % | 5.8 % | 3.9 % | 3.7 % | 0.8 % | 0.3 % |

Every comparator spreads its cost fairly evenly across its queries; the slowest query of the widest
of them takes 7 % of the total. Prod-DS's slowest takes **40.8 %**, and eight queries take 91.77 %.
That concentration is the design, and it is what a mean over the workload destroys — which is the
same reason the paper reports the median and the named tail rather than the sum.

**This SQLBarber instance's cost spread did not produce a runtime spread here.** It targets a
normal distribution of PostgreSQL optimiser cost over 0–10,000, and on DuckDB against this IMDb
database it is the fastest and among the narrowest suites measured: median 7 ms, P99 25 ms, spread
20x. That is a statement about **this instance, this engine and this endpoint** — one selected
1,000-query sample out of a configurable generator. It is not a demonstration that PostgreSQL cost
targeting fails to transfer to DuckDB in general; that would need several instances, several
targets, and ideally the authors' own PostgreSQL endpoint for comparison. What it does support is
describing this comparator as a **PostgreSQL-cost-targeted IMDb instance** rather than as a
reconstruction of a production log.

**The two Redbench projects behave differently, as they should.** Krid's 8,784 analytical instances
have the second-widest spread of the comparators (30x) and the second-highest median (95 ms), which
puts a Redset-derived workload closest to Prod-DS's templates in the body of the distribution. The
Wehrstein read projection is the narrowest thing measured (spread 4x over 62 JOB-derived queries),
and its 938 write statements median 2 ms — write latency, not analytical query latency, and not
comparable in kind. **Neither project is write-only**; the pilot said so and was wrong.

**Two things this comparison cannot show.** Similar curves below the median do not mean similar
coverage: DSB and JOB sit within 11 ms of each other yet differ markedly in join and GROUP BY
structure. And different curves do not by themselves identify different mechanisms — the figure
shows that Prod-DS reaches longer runtimes, not why. The why needs per-query and plan-level
evidence, which for Prod-DS is in the main revision folder.

**A consistency check, not a new finding.** The queries that form the runtime tail are the ones the
workload statistics already flagged: in the WorkloadLens signals Prod-DS is the only suite with
queries in the 51–100 and 101–500 join buckets, and the only one with UNION ALL fan-in above 16.
Those are exactly the amplification queries. The AST signals and the runtime distribution point at
the same handful of queries.

## Coverage, and the one gap

Five of the six suites reviewer R6 named were measured. **PBench was not**, and the reason is
specific: the ILP stage that selects a PBench workload needs per-query candidate metrics which
`HOW_TO_RUN.md` says ship in `src/Collect_metrics/metrics_witho/output/` and which have never
existed in the repository. Producing them means standing up Databend and Prometheus, loading five
databases and running a fresh profiling campaign. The per-suite `_selection.json` files have the full
account. PBench did not fail — it was never runnable from what is released.

## What the pilot got wrong

The single-repetition pilot is kept for comparison. Three of its statements do not survive:

1. **"RedBench is a write workload, 1,000 single-row DELETE plus INSERT statements."** It is a
   *mixed* workload from the Wehrstein project, and there is a second, purely analytical Redbench
   from Krid et al. that the manuscript also cites.
2. **"90.6 % of total runtime."** Recomputed from the pilot's own rows it was 91.66 %; on the final
   run it is **91.77 %**.
3. **JOB's four failures** were a DuckDB reserved-word collision, now patched, not a capability limit.

Its headline reading — close below the median, separated in the tail — is unchanged.

## The two figure exports

One run writes both. They are the same plot at two sizes, and the renderer proves it rather than
asserting it: `draw_cdf` is called once per export, and `main` compares the arrays it handed to
matplotlib across the two and stops if they differ. The confirmation is printed every run —
*8 curves, 10,368 plotted points, identical coordinates, limits and ticks*.

| export | page | placed at 3.337 in | for |
|---|---|---|---|
| `cdf_crossbench_final` | 6.231 x 3.679 in | 1.970 in | the response letter, across the text width |
| `cdf_crossbench_paper` | 5.921 x 2.453 in | **1.382 in** | the manuscript, one column |

The manuscript column is 3.337 in, where the accepted figure would stand 1.97 in tall — half again
the height of the engine figures it sits beside, `fig9b_cdf_tpcds_vs_prodds` (1.359 in) and
`fig11_join_exec_planning` (1.404 in). Uniform scaling cannot fix that, since it preserves the
proportions that are wrong. The paper export changes the canvas and recomposes the legend, and
changes nothing else: same curves, same log scale, same limits, same ticks, same palette, dash
patterns, markers and typeface, no plot title and no footer on either.

**The legend stays inside the axes**, in one column in the lower-right corner. That corner is the
only region a CDF leaves genuinely empty — its curves run from the lower left to the upper right,
so everything right of about 0.8 s and below 0.53 on the ordinate is free. A two-column legend was
tried first and rejected: it is wide enough to reach back into the rising curves and hide them.

Labels are shortened, and what had to survive the shortening did: both Prod-DS populations stay
distinguishable (`Prod-DS (107)` against `Prod-DS (99 templates)`, which also removes the doubled
107 of the old label), both RedBench author labels stay, the read-only qualifier on the Wehrstein
curve stays, and Krid's instance-against-distinct-SQL distinction stays. Every population is read
from the data, never typed into the label, so a label cannot drift from its curve.

The revision handoff additionally carries a sheet showing both exports beside the two engine
figures, each rasterised at its real placed width.

## Files

```
run_final.sh                  reproduce everything
run_cdf_final.py              measurement
make_cdf_figure_final.py      both figure exports and the summary tables
verify_cdf_exports.py         checks the exports against each other and against the delivered figure
queries/                      the frozen query sets and their _selection.json provenance
  sqlbarber/                  1,000 selected from the released 7,524-query pool
  redbench_krid/              763 distinct SQL behind 8,784 instances, with multiplicities
  redbench_wehrstein_reads/   the 62 SELECT queries and the full read/write split
  patched/                    JOB 15a-d with the reserved alias renamed
patches/                      the rename, as a diff
results/final/
  latencies.csv               one row per attempt: suite, pass, order, query, hash, ms, status, rows
  query_summary.csv           per query: outcome, median, min, max, IQR, instance weight
  pass_totals.csv             all ten pass totals, measured wall time and sum of query latencies
  failures.csv                empty
  manifest.json               engine, host, protocol, per-suite sizes, verified settings
  queries_<suite>.txt         the exact query set with content hashes
figures/cdf_crossbench_final.{pdf,png}    full width, for the response letter
figures/cdf_crossbench_paper.{pdf,png}    one column, for the manuscript
figures/cdf_crossbench_summary_final.{csv,md}
figures/cdf_crossbench_tail_final.csv    ten slowest queries per suite
results/latencies_pilot.csv, figures/cdf_crossbench_pilot.*   the superseded pilot
```

No paper text has been changed.
