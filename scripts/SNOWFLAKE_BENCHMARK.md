# Snowflake Benchmark Script

End-to-end Prod-DS Kit benchmark on Snowflake. Generates data, uploads to
Snowflake, runs TPC-DS and Prod-DS workloads, collects timing, and produces
a summary report.

## Prerequisites

1. **Prod-DS Kit installed**: Run `./install.sh` from the repo root first.
2. **SnowSQL CLI**: Install via `brew install --cask snowflake-snowsql` (macOS)
   or from [Snowflake docs](https://docs.snowflake.com/en/user-guide/snowsql-install-config).
3. **Snowflake account**: You need an account identifier, username, and password.

## Usage

```bash
cd prod-ds-kit
source .venv/bin/activate
bash scripts/snowflake_benchmark.sh
```

The script will interactively ask for:

| Prompt | Example | Default |
|--------|---------|---------|
| Account identifier | `xy12345.us-east-1` | (required) |
| Username | `myuser` | (required) |
| Password | (hidden input) | (required) |
| Warehouse name | `BENCHMARK_WH` | `BENCHMARK_WH` |
| Database name | `PRODDS_BENCHMARK` | `PRODDS_BENCHMARK` |
| Schema name | `PUBLIC` | `PUBLIC` |
| Warehouse size | `LARGE` | `LARGE` |

## What it does

1. **Generate data** (SF100, default settings):
   - TPC-DS SF100 (vanilla, no stringification)
   - Prod-DS SF100 STR10 (NULL=medium, MCV=medium)
2. **Generate schemas** (vanilla + stringified DDL)
3. **Generate queries** (TPC-DS base + Prod-DS extended templates)
4. **Create Snowflake objects** (database, schema, warehouse, stage, file format)
5. **Upload data** via `PUT` to internal stage
6. **Load tables** via `COPY INTO` (pipe-delimited with trailing delimiter handling)
7. **Run warmup** (1 pass for each workload)
8. **Run 10 timed repetitions** (result cache disabled)
9. **Collect timing** from `INFORMATION_SCHEMA.QUERY_HISTORY`
10. **Generate report** with per-query median/min/max and workload totals

## Output

All output goes to `snowflake_run/results/`:

| File | Contents |
|------|----------|
| `raw_times.csv` | Per-query, per-rep timing (suite, query, status, elapsed_ms) |
| `benchmark_report.txt` | Human-readable summary with medians |
| `snowflake_query_history.tsv` | Detailed Snowflake-side metrics (compilation, execution, queue time) |

## Configuration

Override defaults via environment variables:

```bash
# Use a different warehouse size
SNOWFLAKE_WAREHOUSE_SIZE=XLARGE bash scripts/snowflake_benchmark.sh
```

## Cost Estimate

| Warehouse Size | Credits/Hour | Approx $/Hour |
|---------------|-------------|----------------|
| Medium | 4 | $8-16 |
| Large (default) | 8 | $16-32 |
| X-Large | 16 | $32-64 |

A full SF100 run (data load + warmup + 10 reps) on a LARGE warehouse typically
takes 2-4 hours. The warehouse is automatically suspended after completion.

## Troubleshooting

- **"snowsql not found"**: Install SnowSQL CLI first.
- **PUT failures**: Ensure you have sufficient local disk for SF100 (~100 GB).
- **COPY INTO errors**: The script uses `ON_ERROR = 'CONTINUE'` to skip
  problematic rows. Check `snowflake_run/results/` for details.
- **Timeout**: Default per-query timeout is 1800s (30 min). Increase via
  `STATEMENT_TIMEOUT_IN_SECONDS` in the script if needed.

## Dialect Notes

Snowflake SQL is highly ANSI-compatible. The main adaptation needed is date
arithmetic: Snowflake uses `DATEADD(day, N, date)` instead of `date + N`.
Most TPC-DS queries run without modification. The extended Prod-DS templates
(`*_ext.tpl`) use ANSI syntax that Snowflake supports natively.
