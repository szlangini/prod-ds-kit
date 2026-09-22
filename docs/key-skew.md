# Appendix F: Join-Key Skew Profiles

This appendix details the join-key skew axis: a data extension that introduces
skew into fact-table foreign keys — the columns the NULL and MCV profiles
deliberately exclude — so joins, aggregations, and order-bys stop being
uniform while structural non-emptiness is preserved: FK→PK equi-joins cannot
lose their join partners, and per-key retention is enforced in expectation with
a hard floor (`KEY_SKEW_MIN_RETENTION`); the shipped default seeds are
additionally gate-verified against empty results.

## Motivation

The MCV profile only concentrates payload columns no query references
(Appendix E, Eligibility #3): concentrating a *predicate* column can wipe out
the values queries select. Join keys, however, are different: an FK→PK
equi-join cannot become empty when the skewed FK values are existing dimension
keys, and redirection at rate `f1 < 1` leaves every key value with at least
`(1 − f1)` of its original occurrences. Filters on dimension attributes
therefore shrink proportionally but never collapse. This follows the JCC-H
insight (Boncz et al., TPCTC 2017) that key skew and query viability are
compatible when data generation controls where the mass sits — while the
*magnitudes* here come from the production fleet, not from JCC-H.

## Design

- **On by default in the CLI, switchable like NULL/MCV.** `wrap_dsdgen.py`
  applies the medium tier unless `--disable-key-skew` is given, so default
  Prod-DS data includes join-key skew; `--key-skew-profile low|medium|high`
  selects the tier, and the umbrella `--skew-profile` sets NULL+MCV+KEY
  together. The axis composes with the NULL and MCV profiles in the same
  rewrite pass (each can be toggled/tiered independently). Direct
  `rewrite_tbl_directory()` callers still opt in explicitly
  (`enable_key_skew=True`), keeping programmatic use and axis-isolating tests
  deliberate. In `reproduce.sh` the default prodds variants carry the axis;
  TPC-DS vanilla and the E5 axis-isolation arms disable it explicitly, and
  pre-recalibration data dirs are preserved as `*_preK`.
- **Calibrated cap.** The medium/default tier draws targets from the fleet
  bucket shape but is capped at **0.80**: fact-FK domains conjoin
  multiplicatively (a row escaping k hot domains survives with `∏(1-f1)`), and
  uncapped fleet draws emptied 15/113 queries. The fleet's extreme
  [0.90, 0.99) mass is carried by the MCV axis instead (non-conjoining
  columns). The cap is a declared no-empty constraint, not a fleet
  measurement; `key_high` keeps uncapped extremes as a documented stress tier.
  Per-pair blacklisting is available via `exclude_pairs: ["channel.domain"]`
  in the profile/overrides. Jointly calibrated result (SF10, all axes at
  medium): 79.5/66.2/50.1/45.0/31.9/19.4 vs fleet 73/60/49/41/33/25; the
  key-columns-only curve reaches 85.7/73.1/54.6/49.6/26.9 (WorkloadLens
  key-only signal), and the 107-query gate passes with 0 errors.
- **Targets.** Per (channel, domain) pair a target top-1 share `T` is drawn
  from `max_value_buckets` — the SAME fleet-calibrated bucket tables as the
  MCV tiers (Redshift "Maximum MCV frequency" curve, "Why TPC Is Not Enough"
  Fig 9). The key axis extends that calibration to the join-key columns the
  MCV profile must exclude.
- **Solve.** `f1 = (T/g − s) / (1 − s)` clamped to [0, 1], with `g` the
  natural non-null fraction (keys receive no injected NULLs) and `s` the
  natural dominant share — the same null-compensated monotone formula as the
  MCV profile. Targets above `g` saturate at `g`.
- **Hot key = natural dominant value** of the canonical column (reuse-natural-
  value): referential integrity is guaranteed, no synthetic keys ever.
- **Identity-hashed decisions.** A row redirects iff
  `h(seed, channel, domain, ticket/order_number) < f1`. The hash input is the
  logical row identity shared between a channel's sales and returns files —
  not the file, column, or row index — so both files independently reach the
  same decision and replacement value. The `(item_sk, ticket_number)`
  sales↔returns join stays intact and the redirect decisions agree per ticket
  (columns that were naturally equal across the two files stay equal; dsdgen
  itself only guarantees such equality partially, e.g. ~80% for `sr_customer_sk`)
  without any cross-file state.
- **Entity-granular customer keys.** Domains listed in the profile's
  `entity_domains` (default: `customer`) hash the key *value* instead of the
  ticket: `h(seed, domain, customer_sk) < f1`. A customer is therefore either
  absorbed by the hot key in every row of every channel (nested across channels
  by f1) or keeps its complete purchase history. Per-ticket thinning of
  customers had removed the multi-channel repeat customers the q4/q11/q38/q74
  family depends on (SF10: customers active in all three channels in two
  consecutive years 1,968 → 6); per-customer redirection keeps the same
  column-level hot-key share with intact histories. All other domains (store,
  demographics, address, promotion, warehouse, page/site, …) stay per ticket.
- **One hot key per entity domain, across channels.** An entity is a single
  dimension row, so its placeholder must be the same key in every fact table:
  the hot customer is the `entity_anchor_channel`'s (default `store`, the
  largest channel) natural dominant value, reused by the catalog and web
  pairs. With per-channel hot keys the whale held its store rows under one
  customer and its catalog/web rows under two others (SF100: c623592 /
  c556750 / c540493), so every cross-channel customer join (q4, q11, q25,
  q74, …) lost the whale's 60–76% of the traffic — q25 at SF100 went from 9
  rows to 0 for every seed. The manifest records the anchor per pair
  (`entity_anchor`).
- **Runs before stringification.** Decisions and replacement operate on raw
  dsdgen values; the hot key then receives the same STR domain prefix as every
  other cell, so stringified join-key spaces stay consistent at any STR level.

## Skewed columns

The axis covers **every fact-table FK domain that is safe to redirect** — 41
(channel, domain) pairs over 75 columns (`KEY_SKEW_CHANNELS` in
`workload/dsdgen/stringify.py` is the authoritative table):

- **store** (identity `ss/sr_ticket_number`): date, time, customer, cdemo,
  hdemo, addr, store, promo, return_date, return_time, reason.
- **catalog** (identity `cs/cr_order_number`): date, ship_date, time,
  customer (bill/ship/refunded/returning), cdemo, hdemo, addr (each ×4),
  call_center, catalog_page, ship_mode, warehouse, promo, return_date,
  return_time, reason.
- **web** (identity `ws/wr_order_number`): date, ship_date, time, customer,
  cdemo, hdemo, addr (each ×4), web_page, web_site, ship_mode, warehouse,
  promo, return_date, return_time, reason.

**Date-family domains are vanilla in every tier** (`domain_caps: 0.0`
for date/ship_date/return_date/return_time/time in all three profiles):
nearly every template filters a date window, and the buy→return→re-buy chain
templates hop several date FKs whose independent redirects multiply into empty
results. The no-empty design rule wins — not every domain needs skew; the
default key-skew story runs on customer/store/demo/address/promo/warehouse/
page keys, and the tiers change only the intensity, never the domain set. A
stress profile that also redirects dates can be built by overriding
`domain_caps` (a redirected return can then predate its sale — plausibility
caveat, no correctness impact).

**Deliberately not skewed:** `item` FKs (every sales/returns primary key is
`(item_sk, ticket/order_number)` — redirection creates duplicate composite
PKs engines reject at load) and all `inventory` FKs (its PK is
`(date_sk, item_sk, warehouse_sk)`, i.e. entirely FKs). Extending to item
keys would require constraint-free schemas or a collision-avoiding scheme
that breaks sales/returns independence — an open design decision.

## Interplay with role-scoped MCV eligibility

The same no-empty reasoning extends the MCV profile: with
`query_exclusion_scope: filtered`, MCV excludes only columns the workload
compares against literals (`config/query_filter_columns.txt`, generated by
`tools/analyze_query_column_roles.py`) instead of every referenced column —
GROUP BY / ORDER BY keys and aggregate inputs become skewable. Key axis (join
keys) + role-scoped MCV (aggregation/order keys and payloads) together cover
the full "skew in keys of joins, aggregations, and order bys" surface.

## Tiers

`config/key_skew_profiles.yml` ships `key_low`, `key_fleet_default`
(medium/default), and `key_high`; the bucket tables are identical to
`mcv_low` / `mcv_fleet_default` / `mcv_high` (Appendix E). Aliases:
low/medium/high.

## Usage

```
# default: STR level + NULL/MCV/key-skew all at medium
python wrap_dsdgen.py --stringification-level 5 -SCALE 10 -DIR out/

# tier override / axis off
python wrap_dsdgen.py --stringification-level 5 --key-skew-profile high -SCALE 10 -DIR out/
python wrap_dsdgen.py --stringification-level 5 --disable-key-skew -SCALE 10 -DIR out/
```

The data manifest records `key_skew_pairs` — hot value, drawn target, and
realized `f1` per (channel, domain) — which is also the hook for query-side
parameter curation that wants to deliberately hit or avoid the hot keys.

## Implementation Reference

- **`workload/dsdgen/stringify.py`** — `KeySkewInjector` (targets/solve in
  `_build_rules`, per-cell redirect in `apply_to_row`) and the
  `KEY_SKEW_CHANNELS` table.
- **`workload/dsdgen/stringify_cpp.cpp`** — the C++ fast path applies the
  identical per-cell contract from the serialized rules (order: nulls →
  key skew → stringify → mcv). Before touching real data, the python wrapper
  probes the binary's `key_skew_applied` summary capability on an empty
  directory; a stale binary triggers a python fallback (auto) or a hard error
  (explicit cpp), so the axis is never silently dropped.
- **`workload/dsdgen/config.py`** — `key_skew_rules()`, tier aliases.
- **`tests/test_key_skew.py`** — CLI/library defaults, sales/returns coherence,
  PK safety, target shares, value retention, determinism, STR interaction,
  python↔cpp byte parity.
