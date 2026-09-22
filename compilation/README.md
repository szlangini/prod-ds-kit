# CedarDB compilation estimates (paper Sec 6.7)

> **Where this sits.** This directory carries the measurement behind the compilation paragraph in
> **Section 6.7** of the paper. `REPRODUCIBILITY.md` in the repository root has the reproduction
> commands. The summaries regenerate from the raw records here without an engine:
> `python3 summarize_compilation.py --scale 100`.

# Compilation estimates, measured inside the engines (22 September 2026)

Reviewer R2's **D1** asked the paper to survey the three systems' capabilities before comparing
them, and to **report compilation times**, since CedarDB and Umbra compile queries. The campaign
could not answer it:

> The campaign harness has no compilation field. Its `wall_time_ms_planning` times a whole client
> process running `EXPLAIN`, and whatever compilation the engine does happens in the execution
> stage that field excludes. **Nothing in the campaign separates them.**

That remains true of the campaign. This directory answers the question **beside** it, with a
separate measurement that does not touch a single delivered record.

## The sentence, ready to paste

> Under **forced optimized compilation**, CedarDB's estimated compilation cost is a median of
> **143 ms** per query on Prod-DS and **127 ms** on TPC-DS at SF100. It stays **below one second
> for every one of the 99 TPC-DS queries** (maximum **0.49 s**) and for 95 of the 97 Prod-DS
> queries; the two exceptions are the join-amplification queries, `J100` at **1.18 s** and `J200`
> at **2.36 s**. Under the **default** mode, the one the campaign ran, nothing of this appears
> before execution (median **0.2 ms**): the engine defers the decision and compiles during
> execution when the query is long enough to repay the cost.

A shorter form, if only one clause fits:

> Under forced optimized compilation, CedarDB's estimated compilation cost has a median of 143 ms
> per query and exceeds one second only for the two deepest join-amplification queries.

**What the sentence must not say.**

- **Not that compilation is constant in the data size.** The sweep was run at both scales and both
  sets of numbers are in this package, but no scale conclusion is drawn from them here.
- **Not that this isolates pure code generation.** It is a difference between two compilation
  modes; what that difference contains beyond code generation is not established.
- **Not that this is the campaign's compilation component.** The campaign ran the default mode,
  which defers compilation into execution.
- **Not that the `EXPLAIN`-stage field measures any of it.**
- **Not that compiling is always worth it**: on three of six sampled queries, forcing it costs
  more than it saves.

## Why the shipped "compilation time" file is not compilation time

`experiments/data/paper_csv/E1_compilation_time_SF100.csv` exports `wall_time_ms_planning`, which is the wall time of a
**whole `psql` process** that runs `EXPLAIN <query>` and exits. Roughly **32 of CedarDB's 36 ms are
the client starting up and connecting**, measured directly on this host, and
whatever code generation the engine does happens in the **execution** stage, which that field
excludes by construction. So the exported number over-counts at one end and misses the thing being
asked about at the other. It is a property of the harness, not of the engine.

## What CedarDB exposes

CedarDB carries its compilation mode as a session setting, and naming an illegal value makes the
server enumerate the legal ones:

```
$ psql … -c "SET debug.compilationmode = 'zzinvalid';"
ERROR:  invalid value for setting "compilationmode": "zzinvalid".
Available values for The default compilation mode:
  Auto:'A', Interpreted:'i', C:'C', DirectEmit:'d', Adaptive:'a', Cheap:'c', Optimized:'o'
```

That is the Umbra compilation ladder: `Interpreted` executes the plan in the bytecode VM and
generates **no machine code**; `DirectEmit` emits machine code without an optimising pass; `Cheap`
and `Optimized` run the code through increasingly expensive optimisation; `C` goes out to a real C
compiler. `Auto` is the default and the mode the whole campaign ran under.

**`PREPARE` is where the compilation work happens.** The mode sensitivity establishes that much directly —
the identical `PREPARE` of `query_77` against the SF100 Prod-DS database costs:

| mode | `PREPARE` |
|---|---:|
| Interpreted | 2.6 ms |
| DirectEmit | 4.8 ms |
| Cheap | 39.4 ms |
| Optimized | 155.5 ms |
| C (a real C compiler) | 1,073.4 ms |

A statement that only parsed and optimised could not vary by a factor of 400 with the setting that
selects a code generator. `EXECUTE` afterwards carries none of it.

## The measurement

For every query, in **one already-open session**, over the unix socket, with no process start
anywhere inside the measured interval:

```
compilation(query, mode) = median PREPARE(query, mode) - median PREPARE(query, Interpreted)
```

Both terms contain parse, binding and optimisation, and the difference is the **estimated
compilation cost under forced optimized compilation**. It is not a measurement of pure code
generation: what else differs between the two modes is not established here. The subtraction is
**per query** over the same five repetitions, never between aggregates.

- **Population**: the E1 common subset — 97 Prod-DS and 99 TPC-DS queries at SF100, the same set
  the runtime figures use, taken from `common_subset.json`.
- **Repetitions**: five, each a **fresh session** with a fresh statement name. Preparing the same
  text twice inside one session is measurably cheaper (163 ms then 139 ms on `query_77`), so
  repetitions must not share a session.
- **Warm-up**: one untimed `PREPARE … AS SELECT 1` per session, because the first compilation in a
  session pays one-time setup.
- **Floor**: `PREPARE … AS SELECT 1` is measured in every session. In `Interpreted` it is the bare
  client-server round trip; in `Optimized` it is *also* the cost of compiling a trivial statement,
  and the summary reports it rather than subtracting it.
- **Read-only**: the server is started with `-readonly`, so no pass can alter a delivered database.
- **Serial**: nothing else ran on the host. Load average before the first pass was 0.31.

## The caveat that must travel with every number here

The campaign ran the **default** mode, `Auto`, whose `PREPARE` is cheap — 4.3 ms on `query_77`
against 155.5 ms for `Optimized` — because it defers the decision and then compiles **during
execution** if the query turns out to be worth compiling. Measured end to end on `query_77` at
SF100: `Auto` executes in **4.2 s**, `Interpreted` in **21.6 s**. `Auto` plainly does compile; it
just does not do it at prepare time.

So these numbers say **what compilation costs**, not where the campaign's recorded runtime went.
Nothing here re-attributes a single millisecond of `E1`.

## What it costs — CedarDB, SF100, E1 common subset

Five repetitions per query, medians. `Auto` is the mode the campaign ran; `Optimized` is the most
expensive routine mode and therefore the upper end of what code generation can cost here.

| mode | suite | median | mean | p90 | max | sum over the workload | queries over 1 s |
|---|---|---:|---:|---:|---:|---:|---:|
| Auto (default) | Prod-DS (97) | **0.2 ms** | 0.2 | 0.3 | 1.6 | 0.02 s | 0 |
| Auto (default) | TPC-DS (99) | **0.2 ms** | 0.4 | 0.5 | 3.9 | 0.04 s | 0 |
| DirectEmit | Prod-DS | 0.2 ms | 0.1 | 0.3 | 1.2 | 0.01 s | 0 |
| DirectEmit | TPC-DS | 0.2 ms | 0.2 | 0.4 | 0.7 | 0.02 s | 0 |
| Cheap | Prod-DS | 21.3 ms | 29.8 | 40.6 | 388.9 | 2.89 s | 0 |
| Cheap | TPC-DS | 18.2 ms | 20.4 | 36.3 | 58.5 | 2.02 s | 0 |
| **Optimized** | **Prod-DS** | **142.5 ms** | 203.0 | 315.3 | **2,359.3** | 19.69 s | **2** |
| **Optimized** | **TPC-DS** | **127.2 ms** | 147.1 | 235.4 | **487.6** | 14.57 s | **0** |

The round-trip floor in the same sessions, `PREPARE … AS SELECT 1`: 0.205 ms interpreted, 0.191 ms
in `Auto`, 0.179 ms in `DirectEmit`, 1.463 ms in `Cheap`, 4.756 ms in `Optimized` — the last two
are not round trip, they are what it costs to compile a trivial statement.

**The two queries over a second are both Prod-DS, and both are join amplification**: `J200` at
2,359 ms and `J100` at 1,184 ms, with `J50` next at 673 ms. No TPC-DS query reaches 500 ms — the
worst is `query_39` at 488 ms. Prod-DS's deep join queries are the only thing in either suite that
puts CedarDB's code generator under real load, which is the same query family that drives the
runtime tail.

## The same sweep at SF10

The sweep was repeated at **SF10**, on the 196 queries the two scales share. Both sets of numbers
are recorded here as measurements; **no conclusion about how compilation behaves with data volume
is drawn from them**, and none should be quoted.

| | SF10 | SF100 |
|---|---:|---:|
| Prod-DS median, Optimized | 142.5 ms | 142.5 ms |
| Prod-DS max | 2,357.3 ms | 2,359.3 ms |
| TPC-DS median, Optimized | 129.3 ms | 127.2 ms |
| TPC-DS max | 490.2 ms | 487.6 ms |
| sum over the 196 shared queries | 34.03 s | 34.26 s |

Two queries move substantially between the scales: `query_75` by +54.8 % and `query_95` by
−52.0 %, in both cases because different statistics produce a different plan to compile.

## What it buys — and when it does not pay

Compilation cost is only meaningful against what it saves, so six Prod-DS queries were run all
three ways, three repetitions each, medians (`cedardb_execution_payoff_SF100.csv`). The selection
rule was fixed in writing before anything was timed: rank the 97 common-subset queries by
CedarDB's own campaign median at SF100, take the 10th, 30th, 50th, 70th and 90th percentile plus
the slowest query under a 30 s cap. The cap exists because the interpreted arm runs several times
slower; it means **this sample says nothing about the amplification tail**.

| query | compile | execute, interpreted | execute, default | execute, optimized | saved | return |
|---|---:|---:|---:|---:|---:|---:|
| `query_39` | 265.8 ms | 102.5 ms | 69.7 ms | 77.5 ms | 25.0 ms | **0.1x** |
| `query_95` | 242.0 ms | 289.9 ms | 106.7 ms | 124.5 ms | 165.4 ms | **0.7x** |
| `query_49` | 123.9 ms | 402.1 ms | 224.0 ms | 241.0 ms | 161.1 ms | 1.3x |
| `query_6` | 328.6 ms | 672.7 ms | 408.8 ms | 412.8 ms | 260.0 ms | **0.8x** |
| `query_54` | 133.1 ms | 10,400.8 ms | 3,209.2 ms | 3,122.7 ms | 7,278.1 ms | **54.7x** |
| `query_union_U5` | 54.6 ms | 2,208.4 ms | 2,017.5 ms | 2,038.0 ms | 170.3 ms | 3.1x |

Two things follow, and the second is the interesting one.

**Generated code runs 1.1x to 3.3x faster than the VM here** — and 5.1x on `query_77`, the query
used for the initial probe, where interpreted execution takes 21.6 s against 4.2 s. So compilation
is not a rounding error in what it delivers.

**On three of these six queries, forcing compilation costs more than it saves.** `query_39` pays
266 ms to save 25 ms. That is not a defect; it is the reason the engine's default mode does not
compile at prepare time at all. Across the sample the **default mode lands within a few percent of
forced-optimized execution** — 69.7 against 77.5 ms, 3,209 against 3,123 ms — while paying 0.2 ms
instead of 55-329 ms up front. Whatever the paper says about compilation, it should not imply that
compiling is unconditionally worth it: this engine decides per query, and on short queries it
decides not to.

The two runs cross-check: the raw `PREPARE` under `Optimized` agrees with the five-repetition sweep
to within 2.5 % on five of the six queries and 8.6 % on the sixth, across different sessions and
about an hour apart.

## A by-product: what DuckDB's planning actually costs

**Secondary, and not what D1 asked for** — DuckDB does not compile anything. It is here because the
same campaign field that mis-describes CedarDB also supplies the paper's DuckDB planning claims,
which the revision found contradicted but never quantified. DuckDB's
own profiler reports the phases, so this costs minutes:

| pass | suite | median | mean | p90 | max |
|---|---|---:|---:|---:|---:|
| planned only (`EXPLAIN`) | Prod-DS (97) | 3.07 ms | 5.14 | 6.93 | 84.55 |
| planned only (`EXPLAIN`) | TPC-DS (99) | 2.84 ms | 3.59 | 6.57 | 15.49 |
| executed | Prod-DS (97) | **1.81 ms** | 3.19 | 3.75 | 61.02 |
| executed | TPC-DS (99) | **1.63 ms** | 2.06 | 3.29 | 11.28 |

Planner plus optimisers plus physical planner, from `profiling_mode='detailed'`; `planner` already
contains `planner_binding`, so binding is carried as a component and not added twice. Both passes
are reported because agreement between them is what makes either quotable.

**So DuckDB's engine-side planning is about 1.8 ms, against the 240 ms the campaign's field
reports for the same queries.** The field is a second `psql`-equivalent process, ~199 ms of which
is the CLI opening a 40 GB database. Any sentence of the form "DuckDB spends x % of its runtime
planning" is about the harness; the engine spends roughly **1 %** of DuckDB's Prod-DS workload
time in its planner.

## Files

| file | what it is |
|---|---|
| `measure_cedardb_compilation.py` | **the measurement**: starts CedarDB read-only, sweeps the modes, writes the raw CSV |
| `summarize_compilation.py` | per-query medians and workload aggregates from the raw CSV |
| `measure_execution_payoff.py` | what the generated code buys, on the pre-declared sample |
| `measure_duckdb_planning.py` | the secondary DuckDB pass |
| `cedardb_compilation_raw_SF100.csv` | all 9,850 measurements: suite, mode, repetition, query, stage, ms, status |
| `cedardb_compilation_raw_SF10.csv` | the same at SF10, four modes |
| `cedardb_compilation_per_query_SF{100,10}.csv` | per query, the median in each mode and the derived compilation time |
| `cedardb_compilation_summary_SF{100,10}.csv` | per suite and mode: n, median, mean, p90, max, sum |
| `cedardb_execution_payoff_SF100.csv` | the six-query cost-against-benefit probe |
| `duckdb_planning_{prodds,tpcds}_SF100_{explain,executed}.csv` | the secondary DuckDB pass |

## What was not done

- **No campaign record was read, rewritten or re-attributed.** `E1` and its exports are untouched;
  this is a separate measurement in its own directory.
- **No MonetDB arm.** MonetDB does not compile either, and its own timer reports parse and
  MAL-optimiser time only for statements it actually executes — a pass over both suites at SF100
  costs about an hour, against seconds for DuckDB. It was started, judged out of scope for a
  question about CedarDB, and stopped; no MonetDB data is shipped and the database was left with
  its query log disabled and empty, as found.
- **No new figure**, no manuscript or caption edit, and nothing pushed to either repository.
- **The amplification tail is not in the payoff sample** — the 30 s cap excludes it.
