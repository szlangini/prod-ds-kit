# AGENTS.md

## Project Overview
Prod-DS Kit is a data- and query-centric extension to TPC-DS.
It adds stringification, join-graph amplification, NULL sparsity,
MCV skew, and extended query templates to the standard benchmark.

## Quick Install
./install.sh

## Entry Points
- `wrap_dsdgen.py` -- data generation (wraps TPC-DS dsdgen + our post-processing)
- `wrap_dsqgen.py` -- query generation (wraps TPC-DS dsqgen + our extensions)
- `prodds-kit` -- CLI utilities (explain-stringification, etc.)
- `python -m experiments run` -- benchmark harness

## Key Directories
- `workload/` -- core Prod-DS code (stringification, query generators)
- `config/` -- NULL/MCV/string profile configurations
- `configs/` -- per-SF seed overrides for deterministic generation
- `query_templates/*_ext.tpl` -- 92 extended query templates (our contribution)
- `experiments/` -- benchmark runner harness with engine adapters
- `tools/` -- utility scripts (schema generator, join validator, etc.)
- `docs/` -- paper appendix material and documentation
- `tpcds-kit/` -- TPC-DS toolkit (cloned by install.sh, gitignored)

## Workflow: Generate Data (Default)
python3 wrap_dsdgen.py --default
# Equivalent to: STR=10, NULL=medium, MCV=medium, SF=10, output=./output
# Override scale: python3 wrap_dsdgen.py --default -SCALE 1

## Workflow: Generate Queries
python3 wrap_dsqgen.py --output-dir /path/to/queries
# Uses *_ext.tpl templates by default

## Workflow: Run Benchmark
cp experiments/config.example.yaml experiments/config.yaml
# Edit config.yaml with your paths and engine settings
python -m experiments run --config experiments/config.yaml --experiment workload_compare --system duckdb

## Running Tests
pytest tests/ -v

## Profile Tiers
NULL: low / medium (default) / high -- see config/null_profiles.yml
MCV:  low / medium (default) / high -- see config/mcv_profiles.yml
STR:  vanilla (1), low (3), medium (5), high (7), production (10), plus STR+ (11-15)

## Supported Engines
DuckDB, CedarDB, MonetDB
