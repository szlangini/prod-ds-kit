# Pre-revision measurement files (not part of the revision artifact)

Files here were produced before the September 2026 revision campaign and must not be mixed
with the CSVs in `experiments/data/paper_csv/`, which are all regenerated from
`.reproduce/sf<N>/results_keyskew/` by `experiments/export_paper_csv.py`.

- `E4_stringification_sweep_SF100_duckdb.csv` — measured 15 July 2026 as a post-submission
  bonus run of E4 at SF100. It predates two corrections: the disabling of the stale March-2026
  dialect overlays (8 September, which changed 41 of 107 Prod-DS queries) and the key-skew
  work. The revision measures E4 at SF10 only, per the paper's scale assignment, so there is
  no current SF100 counterpart. Kept for reference; regenerate with
  `reproduce.sh --experiment E4 --sf 100` if an SF100 sweep is ever wanted.
