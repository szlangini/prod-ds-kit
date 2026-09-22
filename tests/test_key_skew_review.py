"""Adversarial review tests for the join-key skew axis ("Profil K").

Companion to tests/test_key_skew.py; the review report lives in
~/KEYSKEW_REVIEW_FINDINGS.md (finding ids referenced below: S*, C*, P*).

Every test is deterministic (fixed seeds / fixed hash inputs), builds synthetic
.tbl fixtures (no dsdgen run) and the whole file runs in a few seconds.

Tests marked ``xfail(strict=True)`` pin review findings: they FAIL on the
current tree and flip to XPASS once the fix lands (then remove the marker).
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import re
import subprocess
import sys
from collections import Counter
from pathlib import Path
from typing import Callable, Dict, List, Mapping, Sequence, Tuple

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from workload import stringification as stringification_cfg  # noqa: E402
from workload.dsdgen import config as config_mod  # noqa: E402
from workload.dsdgen import stringify  # noqa: E402
from workload.dsdgen import wrap_dsdgen  # noqa: E402
from workload.dsdgen.config import key_skew_rules  # noqa: E402

CPP_BIN = REPO_ROOT / "workload" / "dsdgen" / "stringify_cpp"
TPCDS_SQL = REPO_ROOT / "tpcds-kit" / "tools" / "tpcds.sql"
REPRODUCE_SH = REPO_ROOT / "reproduce.sh"

CHANNELS: Dict[str, Mapping] = {c["channel"]: c for c in stringify.KEY_SKEW_CHANNELS}
FACT_TABLES = (
    "store_sales", "store_returns",
    "catalog_sales", "catalog_returns",
    "web_sales", "web_returns",
)
# One degenerate bucket -> every materialised pair draws target T = 0.6.
# Mechanism-level fixture overrides: fixed target, and the shipped default
# profile's pair blacklist cleared (it excludes catalog.customer for the q81
# avg-poisoning; these tests exercise the full mechanism on every channel).
T60 = {
    "max_value_buckets": [{"weight": 1.0, "min": 0.6, "max": 0.6}],
    "seed": 7,
    "exclude_pairs": [],
}
NULLISH = ("", "\\N")

needs_tools = pytest.mark.needs_tpcds_tools
needs_cpp = pytest.mark.needs_cpp


# ---------------------------------------------------------------------------
# Fixture builder (schema-driven, all three channels)
# ---------------------------------------------------------------------------

POOL = {
    "date": 30, "ship_date": 30, "time": 25, "return_date": 30, "return_time": 25,
    "customer": 50, "cdemo": 40, "hdemo": 30, "addr": 45, "store": 7, "promo": 12,
    "call_center": 5, "catalog_page": 20, "ship_mode": 9, "warehouse": 6,
    "web_page": 15, "web_site": 8, "reason": 10,
}
BASE = {domain: 1000 * (i + 1) for i, domain in enumerate(POOL)}


def natural_value(domain: str, order: int) -> str:
    """Skewed pool draw: every third order sits on the domain's base value, so
    each column's natural dominant is str(BASE[domain]) with a ~35% share."""
    if order % 3 == 0:
        return str(BASE[domain])
    return str(BASE[domain] + order % POOL[domain])


def uniform_value(domain: str, order: int) -> str:
    return str(BASE[domain] + order % POOL[domain])


def schema() -> dict:
    return stringify._schema_cache()


def col(table: str, column: str) -> int:
    return schema()[table]["columns"].index(column)


def channel_of(table: str) -> Mapping:
    return next(c for c in stringify.KEY_SKEW_CHANNELS if table in c["identity_columns"])


def target_columns() -> List[Tuple[str, str, str, str, bool]]:
    """(channel, domain, table, column, is_canonical) for every target column."""
    out = []
    for cfg in stringify.KEY_SKEW_CHANNELS:
        for domain, dcfg in cfg["domains"].items():
            table, column = dcfg["canonical"]
            out.append((cfg["channel"], domain, table, column, True))
            for table, column in dcfg["mirrors"]:
                out.append((cfg["channel"], domain, table, column, False))
    return out


def make_channel(
    channel: str,
    orders: int,
    lines: int = 2,
    *,
    returns_every: int = 2,
    identity: Callable[[int], str] = str,
    value_fn: Callable[[str, int], str] = natural_value,
    mirror_jitter: bool = False,
) -> Dict[str, List[str]]:
    """Synthetic sales+returns rows for one channel.

    Every order has `lines` sales lines sharing the order's FK values (dsdgen
    ticket atomicity); every `returns_every`-th order has one return line whose
    shared-domain FKs mirror the sales side. With mirror_jitter, non-canonical
    columns of a domain differ from the canonical one on every 4th order
    (dsdgen: SR_SAME_CUSTOMER = 80%, i.e. 20% of returns name another customer).
    """
    cfg = CHANNELS[channel]
    identity_cols: Mapping[str, str] = cfg["identity_columns"]
    sales_table, returns_table = list(identity_cols)
    columns = {t: schema()[t]["columns"] for t in (sales_table, returns_table)}
    domain_of: Dict[str, Dict[str, Tuple[str, bool]]] = {sales_table: {}, returns_table: {}}
    for domain, dcfg in cfg["domains"].items():
        table, column = dcfg["canonical"]
        domain_of[table][column] = (domain, True)
        for table, column in dcfg["mirrors"]:
            domain_of[table][column] = (domain, False)
    item_col = {t: next(c for c in columns[t] if c.endswith("_item_sk")) for t in columns}
    rows: Dict[str, List[str]] = {sales_table: [], returns_table: []}
    for order in range(1, orders + 1):
        for line in range(lines):
            item = str(5000 + (order * lines + line) % 400)
            targets = [sales_table]
            if line == 0 and order % returns_every == 0:
                targets.append(returns_table)
            for table in targets:
                row = [str(100 + i) for i in range(len(columns[table]))]
                for i, column in enumerate(columns[table]):
                    if column == identity_cols[table]:
                        row[i] = identity(order)
                    elif column == item_col[table]:
                        row[i] = item
                    elif column in domain_of[table]:
                        domain, canonical = domain_of[table][column]
                        jitter = mirror_jitter and not canonical and order % 4 == 1
                        row[i] = value_fn(domain, order + 1 if jitter else order)
                rows[table].append("|".join(row) + "|\n")
    return rows


def write_tables(directory: Path, tables: Mapping[str, Sequence[str]]) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    for name, rows in tables.items():
        (directory / f"{name}.tbl").write_text(
            "".join(rows), encoding="utf-8", errors="surrogateescape"
        )


def read_rows(path: Path) -> List[List[str]]:
    text = path.read_text(encoding="utf-8", errors="surrogateescape")
    return [line.split("|") for line in text.splitlines()]


def split_rows(rows: Sequence[str]) -> List[List[str]]:
    return [r.rstrip("\n").split("|") for r in rows]


def rewrite(directory: Path, **kwargs) -> Tuple[int, int]:
    params = dict(
        max_workers=1,
        backend="python",   # explicit: `auto` silently picks the C++ path when the binary is built
        enable_stringify=False,
        stringification_level=None,
        enable_nulls=False,
        enable_mcv=False,
        min_ndv_for_injection=0,
        ndv_cache_dir=str(directory / "_cache"),
    )
    params.update(kwargs)
    return stringify.rewrite_tbl_directory(directory, **params)


def manifest(directory: Path) -> dict:
    path = directory / stringification_cfg.DATA_MANIFEST_NAME
    return json.loads(path.read_text(encoding="utf-8"))


def unit_hash(seed: int, *parts) -> float:
    return stringify._stable_unit_hash(seed, *parts)


def assert_documented_contract(
    directory: Path,
    before: Mapping[str, Sequence[str]],
    *,
    seed: int,
    null_marker: str = "",
    check_untouched: bool = True,
) -> int:
    """Check EVERY cell of every key-skew target column against the documented
    per-cell contract (docs/key-skew.md "Identity-hashed decisions" /
    "Entity-granular customer keys"):

        ticket domains:  cell := hot(pair)  iff  h(seed, "key-skew", channel, domain, identity) < f1
                                                 and identity, cell are non-null
        entity domains:  cell := hot(pair)  iff  h(seed, "key-skew-entity", domain, cell) < f1
                                                 and cell is non-null
        cell unchanged   otherwise

    with (hot, f1) taken from the manifest. Returns the number of redirect
    decisions taken. With check_untouched, every non-target cell must be
    byte-identical to the input (no other axis ran)."""
    pairs = manifest(directory)["key_skew_pairs"]
    redirected = 0
    for table, rows_before in before.items():
        after = read_rows(directory / f"{table}.tbl")
        pre = split_rows(rows_before)
        assert len(after) == len(pre), f"{table}: row count changed"
        identity_idx = col(table, channel_of(table)["identity_columns"][table])
        expectations = [
            (col(table, column), channel, domain, pairs.get(f"{channel}.{domain}"))
            for channel, domain, t, column, _ in target_columns()
            if t == table
        ]
        target_idx = {e[0] for e in expectations}
        for b, a in zip(pre, after):
            assert len(a) == len(b), f"{table}: field count changed"
            identity = b[identity_idx]
            for idx, channel, domain, pair in expectations:
                expected = b[idx]
                if pair is not None and b[idx] not in (null_marker, *NULLISH):
                    if pair.get("granularity") == "entity":
                        hit = unit_hash(seed, "key-skew-entity", domain, b[idx]) < pair["f1"]
                    else:
                        hit = identity not in (null_marker, *NULLISH) and (
                            unit_hash(seed, "key-skew", channel, domain, identity) < pair["f1"]
                        )
                    if hit:
                        expected = pair["value"]
                        redirected += 1
                assert a[idx] == expected, (
                    f"{table}[identity={identity!r}].{domain}: {a[idx]!r} != {expected!r}"
                )
            if check_untouched:
                for i, (bc, ac) in enumerate(zip(b, a)):
                    if i not in target_idx:
                        assert bc == ac, f"{table}: non-target column {i} changed"
    return redirected


# ---------------------------------------------------------------------------
# Invariant 3: PK safety / table integrity (programmatic, against tpcds.sql)
# ---------------------------------------------------------------------------

def _primary_keys(sql_path: Path) -> Dict[str, set]:
    text = sql_path.read_text(encoding="utf-8")
    pks: Dict[str, set] = {}
    for match in re.finditer(r"create\s+table\s+(\w+)\s*\((.*?)\n\s*\)\s*;", text, re.S | re.I):
        table = match.group(1).lower()
        pk = re.search(r"primary\s+key\s*\(([^)]*)\)", match.group(2), re.I)
        pks[table] = {c.strip().lower() for c in pk.group(1).split(",")} if pk else set()
    return pks


@needs_tools
class TestPkSafety:
    def test_no_target_column_is_in_any_primary_key(self):
        pks = _primary_keys(TPCDS_SQL)
        assert pks["store_sales"] == {"ss_item_sk", "ss_ticket_number"}  # parser sanity
        assert pks["inventory"] == {"inv_date_sk", "inv_item_sk", "inv_warehouse_sk"}
        assert len(pks) >= 24
        for channel, domain, table, column, _ in target_columns():
            assert column not in pks[table], f"{channel}.{domain}: {table}.{column} is a PK column"
            assert not column.endswith("_item_sk"), (table, column)
            assert table not in ("inventory", "item"), (table, column)

    def test_identity_columns_are_pk_members_not_null_and_never_targets(self):
        pks = _primary_keys(TPCDS_SQL)
        targets = {(t, c) for _, _, t, c, _ in target_columns()}
        for cfg in stringify.KEY_SKEW_CHANNELS:
            for table, identity in cfg["identity_columns"].items():
                assert identity in pks[table], (table, identity)
                assert (table, identity) not in targets
                not_nulls = {c["name"].lower() for c in schema()[table]["not_null_columns"]}
                assert identity.lower() in not_nulls, (
                    f"{table}.{identity} is not NOT NULL -> NULL axis could null identities"
                )

    def test_channel_table_covers_every_non_item_fact_fk_exactly_once(self):
        targets = target_columns()
        assert len(targets) == 75
        assert len({(ch, d) for ch, d, *_ in targets}) == 41
        prefix = {
            "store_sales": "ss_", "store_returns": "sr_", "catalog_sales": "cs_",
            "catalog_returns": "cr_", "web_sales": "ws_", "web_returns": "wr_",
        }
        for channel, domain, table, column, _ in targets:
            assert table.startswith(channel), (channel, table)
            assert column.startswith(prefix[table]), (table, column)
            assert column.endswith("_sk"), (table, column)
            assert column in schema()[table]["columns"], f"{table}.{column} not in schema"
        covered = Counter((t, c) for _, _, t, c, _ in targets)
        assert max(covered.values()) == 1, "a column is listed twice"
        for table in FACT_TABLES:
            key_like = {c["name"] for c in schema()[table]["key_like_columns"]}
            key_like -= {f"{prefix[table]}item_sk"}
            assert key_like == {c for (t, c) in covered if t == table}, table

    def test_every_domain_references_a_single_dimension(self):
        expected_dim = {
            "customer": "customer", "cdemo": "customer_demographics",
            "hdemo": "household_demographics", "addr": "customer_address",
            "store": "store", "promo": "promotion", "call_center": "call_center",
            "catalog_page": "catalog_page", "ship_mode": "ship_mode",
            "warehouse": "warehouse", "web_page": "web_page", "web_site": "web_site",
            "reason": "reason", "date": "date_dim", "ship_date": "date_dim",
            "return_date": "date_dim", "time": "time_dim", "return_time": "time_dim",
        }
        expected_suffix = {
            "customer": "_customer_sk", "cdemo": "_cdemo_sk", "hdemo": "_hdemo_sk",
            "addr": "_addr_sk", "store": "_store_sk", "promo": "_promo_sk",
            "call_center": "_call_center_sk", "catalog_page": "_catalog_page_sk",
            "ship_mode": "_ship_mode_sk", "warehouse": "_warehouse_sk",
            "web_page": "_web_page_sk", "web_site": "_web_site_sk", "reason": "_reason_sk",
            "date": "_date_sk", "ship_date": "_date_sk", "return_date": "_date_sk",
            "time": "_time_sk", "return_time": "_time_sk",
        }
        ri = dict(stringification_cfg._ri_fk_pairs())
        assert ri, "tools/tpcds_ri.sql not parsed"
        unconstrained = []
        for cfg in stringify.KEY_SKEW_CHANNELS:
            for domain, dcfg in cfg["domains"].items():
                columns = (dcfg["canonical"], *dcfg["mirrors"])
                for t, c in columns:
                    assert c.endswith(expected_suffix[domain]), (cfg["channel"], domain, t, c)
                refs = {ri.get(f"{t}.{c}") for t, c in columns}
                known = {r for r in refs if r is not None}
                unconstrained += [f"{t}.{c}" for t, c in columns if ri.get(f"{t}.{c}") is None]
                # Copying the canonical hot key into mirrors is RI-safe only if
                # every column of the domain references the SAME dimension.
                assert len(known) <= 1, (cfg["channel"], domain, refs)
                if known:
                    assert known.pop().split(".")[0] == expected_dim[domain], (cfg["channel"], domain)
        # tools/tpcds_ri.sql comments out exactly three of the 75 constraints
        # (present in tpcds-kit/tools/tpcds_ri.sql); the suffix check above
        # covers them. Fail loudly if that set ever grows.
        assert sorted(unconstrained) == [
            "catalog_returns.cr_returned_time_sk",
            "web_returns.wr_returning_hdemo_sk",
            "web_sales.ws_bill_hdemo_sk",
        ], unconstrained


# ---------------------------------------------------------------------------
# Invariant 1: sales<->returns coherence (identity hashing)
# ---------------------------------------------------------------------------

@needs_tools
class TestCoherence:
    @pytest.mark.parametrize("backend", ["python", pytest.param("cpp", marks=needs_cpp)])
    @pytest.mark.parametrize("channel", ["store", "catalog", "web"])
    def test_every_cell_follows_the_documented_decision_rule(self, tmp_path, channel, backend):
        rows = make_channel(channel, 150, mirror_jitter=True)
        write_tables(tmp_path, rows)
        rewrite(tmp_path, backend=backend, enable_key_skew=True, key_skew_overrides=T60)
        assert manifest(tmp_path)["rewrite_backend"] == backend
        pairs = manifest(tmp_path)["key_skew_pairs"]
        assert f"{channel}.customer" in pairs
        assert assert_documented_contract(tmp_path, rows, seed=7) > 0

    def test_all_customer_mirrors_follow_one_decision(self, tmp_path):
        # catalog.customer spans 4 columns over 2 files (bill/ship/refunded/returning).
        rows = make_channel("catalog", 150, mirror_jitter=True)
        write_tables(tmp_path, rows)
        rewrite(tmp_path, enable_key_skew=True, key_skew_overrides=T60)
        entry = manifest(tmp_path)["key_skew_pairs"]["catalog.customer"]
        hot, f1 = entry["value"], entry["f1"]
        mirrors = {
            "catalog_sales": ["cs_bill_customer_sk", "cs_ship_customer_sk"],
            "catalog_returns": ["cr_refunded_customer_sk", "cr_returning_customer_sk"],
        }
        assert sorted(entry["columns"]) == sorted(f"{t}.{c}" for t, cs in mirrors.items() for c in cs)
        # customer is ENTITY-granular: every customer column follows the decision
        # of ITS OWN value (h(seed, "key-skew-entity", "customer", value)) --
        # identically in every column, row, file and channel.
        seen = {True: 0, False: 0}
        for table, columns in mirrors.items():
            for before, after in zip(split_rows(rows[table]), read_rows(tmp_path / f"{table}.tbl")):
                for c in columns:
                    value = before[col(table, c)]
                    if value in NULLISH:
                        continue
                    redirected = unit_hash(7, "key-skew-entity", "customer", value) < f1
                    seen[redirected] += 1
                    assert after[col(table, c)] == (hot if redirected else value), (table, c, value)
        assert seen[True] > 0 and seen[False] > 0

    @pytest.mark.parametrize(
        "style,identity",
        [
            ("plain", str),
            ("leading-zeros", lambda o: f"{o:06d}"),
            ("leading-space", lambda o: f" {o}"),
            ("trailing-space", lambda o: f"{o} "),
            ("unit-separator", lambda o: f"{o}\x1f{o}"),
            ("non-ascii", lambda o: f"é{o}"),
            ("signed", lambda o: f"+{o}"),
        ],
    )
    @pytest.mark.parametrize("backend", ["python", pytest.param("cpp", marks=needs_cpp)])
    def test_byte_identical_odd_identities_stay_coherent(self, tmp_path, style, identity, backend):
        rows = make_channel("store", 90, identity=identity)
        write_tables(tmp_path, rows)
        rewrite(tmp_path, backend=backend, enable_key_skew=True, key_skew_overrides=T60)
        assert manifest(tmp_path)["rewrite_backend"] == backend
        assert assert_documented_contract(tmp_path, rows, seed=7) > 0
        # Without mirror jitter sr_* == ss_* holds naturally, so it must hold after.
        ss = {
            r[col("store_sales", "ss_ticket_number")]: r
            for r in read_rows(tmp_path / "store_sales.tbl")
        }
        for r in read_rows(tmp_path / "store_returns.tbl"):
            s = ss[r[col("store_returns", "sr_ticket_number")]]
            for domain in ("customer", "cdemo", "hdemo", "addr", "store"):
                ss_col = CHANNELS["store"]["domains"][domain]["canonical"][1]
                sr_col = CHANNELS["store"]["domains"][domain]["mirrors"][0][1]
                assert r[col("store_returns", sr_col)] == s[col("store_sales", ss_col)], (style, domain)

    def test_formatting_mismatch_between_files_breaks_coherence(self, tmp_path):
        """Documented limitation (not a dsdgen scenario): the hash input is the
        RAW identity string, nothing normalises leading zeros/whitespace. dsdgen
        prints sr_ticket_number = ss_ticket_number with the same integer format
        (w_store_returns.c: `r->sr_ticket_number = sale->ss_ticket_number`), so
        generated data cannot hit this; a formatting drift between files WOULD
        silently split the decision, as this test shows."""
        rows = make_channel("store", 200, lines=1)
        sr_id = col("store_returns", "sr_ticket_number")
        returns = []
        for line in rows["store_returns"]:
            f = line.rstrip("\n").split("|")
            f[sr_id] = f"{int(f[sr_id]):06d}"
            returns.append("|".join(f) + "\n")
        write_tables(tmp_path, {"store_sales": rows["store_sales"], "store_returns": returns})
        rewrite(tmp_path, enable_key_skew=True, key_skew_overrides=T60)
        # (store is ticket-granular; the entity-granular customer column does
        # not depend on the identity at all and stays coherent regardless)
        ss = {
            r[col("store_sales", "ss_ticket_number")]: r[col("store_sales", "ss_store_sk")]
            for r in read_rows(tmp_path / "store_sales.tbl")
        }
        mismatches = sum(
            1
            for r in read_rows(tmp_path / "store_returns.tbl")
            if ss[str(int(r[sr_id]))] != r[col("store_returns", "sr_store_sk")]
        )
        assert mismatches > 0

    def test_invalid_utf8_identity_bytes_stay_coherent_in_python(self, tmp_path):
        # A raw 0xFF byte survives the surrogateescape read/write and both files
        # (same bytes) reach the same decision.
        rows = make_channel("store", 80, lines=1, identity=lambda o: f"{o}\udcff")
        write_tables(tmp_path, rows)
        assert b"\xff" in (tmp_path / "store_sales.tbl").read_bytes()
        rewrite(tmp_path, backend="python", enable_key_skew=True, key_skew_overrides=T60)
        assert b"\xff" in (tmp_path / "store_sales.tbl").read_bytes()
        assert assert_documented_contract(tmp_path, rows, seed=7) > 0


# ---------------------------------------------------------------------------
# Invariant 4: pipeline order & axis interference
# ---------------------------------------------------------------------------

@needs_tools
class TestAxisInterference:
    @staticmethod
    def _protected_indexes(table: str) -> set:
        idx = {col(t, c) for _, _, t, c, _ in target_columns() if t == table}
        idx.add(col(table, channel_of(table)["identity_columns"][table]))
        idx.add(col(table, next(c for c in schema()[table]["columns"] if c.endswith("_item_sk"))))
        return idx

    def test_null_injector_never_targets_keys_or_identities(self):
        cfg = {
            "enabled": True, "seed": 1, "column_selection_fraction": 1.0,
            "selection_fraction_scope": "eligible",
            "buckets": [{"weight": 1.0, "min": 0.9, "max": 0.9}],
            "min_ndv_for_injection": 0, "include_hot_path_columns": True,
        }
        injector = stringify.NullInjector(schema(), cfg)
        assert injector.has_rules
        for table in FACT_TABLES:
            rule_idx = {r.index for r in injector.rules.get(table, [])}
            assert rule_idx, f"{table}: expected NULL rules on payload columns"
            assert not (rule_idx & self._protected_indexes(table)), table

    @pytest.mark.parametrize("scope", ["referenced", "filtered"])
    def test_mcv_injector_never_targets_keys_or_identities(self, scope):
        cfg = {
            "enabled": True, "seed": 1, "column_selection_fraction": 1.0,
            "selection_fraction_scope": "eligible",
            "max_value_buckets": [{"weight": 1.0, "min": 0.7, "max": 0.7}],
            "min_ndv_for_injection": 0, "query_exclusion_scope": scope,
        }
        injector = stringify.MCVInjector(schema(), cfg, natural_stats={})
        assert injector.has_rules
        for table in FACT_TABLES:
            rule_idx = {r.index for r in injector.rules.get(table, [])}
            assert not (rule_idx & self._protected_indexes(table)), table

    @pytest.mark.parametrize("level", [1, 5, 10])
    def test_hot_key_is_formatted_like_untouched_values_at_every_str_level(self, tmp_path, level):
        rows = make_channel("store", 90)
        plain, skewed = tmp_path / "plain", tmp_path / "skewed"
        write_tables(plain, rows)
        write_tables(skewed, rows)
        str_kwargs = dict(enable_stringify=None, stringification_level=level)
        rewrite(plain, **str_kwargs)
        rewrite(skewed, enable_key_skew=True, key_skew_overrides=T60, **str_kwargs)
        assert manifest(skewed)["key_skew_pairs"]
        assert manifest(skewed)["stringification_level"] == level
        for _, _, table, column, _ in target_columns():
            if not table.startswith("store"):
                continue
            idx = col(table, column)
            plain_vals = {r[idx] for r in read_rows(plain / f"{table}.tbl")}
            skew_vals = {r[idx] for r in read_rows(skewed / f"{table}.tbl")}
            # The redirected key is formatted exactly like the same raw value in
            # the plain run -> the skewed value space is a subset of the plain one.
            assert skew_vals <= plain_vals, (level, table, column, skew_vals - plain_vals)
        ss = {
            r[col("store_sales", "ss_ticket_number")]: r
            for r in read_rows(skewed / "store_sales.tbl")
        }
        stringified = False
        for r in read_rows(skewed / "store_returns.tbl"):
            s = ss[r[col("store_returns", "sr_ticket_number")]]
            sr_val = r[col("store_returns", "sr_customer_sk")]
            assert sr_val == s[col("store_sales", "ss_customer_sk")]
            stringified |= not sr_val.isdigit()
        assert stringified == (level >= 5), level

    def test_pipeline_order_null_key_str_mcv_leaves_key_contract_intact(self, tmp_path):
        rows = make_channel("store", 90, mirror_jitter=True)
        full, staged = tmp_path / "full", tmp_path / "staged"
        write_tables(full, rows)
        write_tables(staged, rows)
        null_overrides = {
            "column_selection_fraction": 1.0,
            "buckets": [{"weight": 1.0, "min": 0.5, "max": 0.5}], "seed": 21,
        }
        mcv_overrides = {
            "column_selection_fraction": 1.0,
            "max_value_buckets": [{"weight": 1.0, "min": 0.7, "max": 0.7}], "seed": 22,
        }
        rewrite(
            full, enable_stringify=None, stringification_level=5,
            enable_nulls=True, null_overrides=null_overrides,
            enable_mcv=True, mcv_overrides=mcv_overrides,
            enable_key_skew=True, key_skew_overrides=T60,
        )
        # Reference: key skew alone on raw rows (contract-checked), then STR5 alone.
        rewrite(staged, enable_key_skew=True, key_skew_overrides=T60)
        assert assert_documented_contract(staged, rows, seed=7) > 0
        staged_pairs = manifest(staged)["key_skew_pairs"]
        rewrite(staged, enable_stringify=None, stringification_level=5)
        # NULL never touches keys, so g and hence every (hot, f1) is identical.
        assert manifest(full)["key_skew_pairs"] == staged_pairs
        for table in rows:
            protected = self._protected_indexes(table)
            full_rows = read_rows(full / f"{table}.tbl")
            staged_rows = read_rows(staged / f"{table}.tbl")
            assert len(full_rows) == len(staged_rows)
            changed_elsewhere = 0
            for a, b in zip(full_rows, staged_rows):
                for i in protected:
                    assert a[i] == b[i], (table, i)   # NULL/MCV never touched a key/identity
                changed_elsewhere += sum(1 for i in range(len(a)) if i not in protected and a[i] != b[i])
            assert changed_elsewhere > 0, f"{table}: NULL/MCV did not run in the full pipeline"


# ---------------------------------------------------------------------------
# Invariant 5: f1 solve
# ---------------------------------------------------------------------------

def solve(share_nn: float, null_rate: float, target: float, **extra):
    cfg = {
        "enabled": True, "seed": 0,
        "max_value_buckets": [{"weight": 1.0, "min": target, "max": target}],
        **extra,
    }
    stats = {
        "store_sales.ss_customer_sk": stringify.NaturalColumnStat(
            value="1000", share_nn=share_nn, null_rate=null_rate
        )
    }
    injector = stringify.KeySkewInjector(schema(), cfg, natural_stats=stats)
    entry = injector.pair_targets.get("store.customer")
    return None if entry is None else entry["f1"]


@needs_tools
class TestSolve:
    @pytest.mark.parametrize(
        "s,nr,T,expected",
        [
            (0.2, 0.0, 0.6, 0.5),
            (0.0, 0.0, 0.6, 0.6),
            (0.2, 0.25, 0.6, 0.75),   # g=0.75 -> T/g=0.8 -> (0.8-0.2)/0.8
            (0.2, 0.0, 1.0, "CAP"),   # T = 1 -> f1 saturates (1.0 today; 1 - retention floor after S1)
            (0.2, 0.5, 0.6, "CAP"),   # T > g -> f1 saturates (see FINDING S1)
            (0.6, 0.0, 0.6, None),    # already at target -> monotone: no rule
            (0.61, 0.0, 0.6, None),
            (0.999, 0.0, 0.6, None),
            (1.0, 0.0, 0.6, None),    # constant column
            (0.2, 1.0, 0.6, None),    # all-NULL column (g = 0)
            (0.2, 0.0, 0.0, None),    # T = 0 (domain cap 0.0)
            (0.5, 0.0, 0.5, None),
        ],
    )
    def test_f1_solve_table(self, s, nr, T, expected):
        f1 = solve(s, nr, T)
        if expected is None:
            assert f1 is None
            return
        # Saturation cap: 1.0 on the current tree; 1 - KEY_SKEW_MIN_RETENTION once S1 lands.
        f1_cap = 1.0 - getattr(stringify, "KEY_SKEW_MIN_RETENTION", 0.0)
        if expected == "CAP":
            expected = f1_cap
        assert f1 == pytest.approx(expected, abs=1e-12)
        g = 1.0 - nr
        realized = g * (s + f1 * (1.0 - s))   # expected top-1 share over ALL rows
        assert realized == pytest.approx(min(T, g * (s + f1_cap * (1.0 - s))), abs=1e-12)

    def test_f1_is_monotone_in_target_and_bounded(self):
        s, nr = 0.3, 0.05
        prev = 0.0
        for T in (i / 100 for i in range(1, 100)):
            f1 = solve(s, nr, T)
            if f1 is None:
                assert T <= s * (1 - nr) + 1e-9, T   # no rule iff target below natural share
                continue
            assert 0.0 < f1 <= 1.0
            assert f1 >= prev - 1e-12
            prev = f1

    @pytest.mark.parametrize("caps,expected", [({"customer": 0.3}, 0.125), ({"customer": 0.0}, None)])
    def test_domain_caps_bound_the_target(self, caps, expected):
        f1 = solve(0.2, 0.0, 0.6, domain_caps=caps)
        assert (f1 is None) == (expected is None)
        if expected is not None:
            assert f1 == pytest.approx(expected)

    def test_saturation_keeps_a_retention_floor(self):
        f1 = solve(0.02, 0.045, 0.97)   # SF10 ss_store_sk-like column under key_high
        assert f1 is not None and f1 < 1.0

    def test_degenerate_tables_produce_no_rules(self, tmp_path):
        one_row = make_channel("store", 1, lines=1)["store_sales"]
        for name, rows in {"empty": [], "blank": ["\n", "\n"], "single": one_row}.items():
            d = tmp_path / name
            write_tables(d, {"store_sales": rows})
            rewrite(d, enable_key_skew=True, key_skew_overrides=T60)
            m = manifest(d)
            assert m["key_skew_pairs"] == {} and m["key_skew_enabled"] is False, name
            assert (d / "store_sales.tbl").read_text() == "".join(rows), name
        # two distinct rows: s = 0.5 -> f1 = (0.6 - 0.5) / 0.5 = 0.2
        d = tmp_path / "two"
        write_tables(d, {"store_sales": make_channel("store", 2, lines=1)["store_sales"]})
        rewrite(d, enable_key_skew=True, key_skew_overrides=T60)
        assert manifest(d)["key_skew_pairs"]["store.customer"]["f1"] == pytest.approx(0.2)


# ---------------------------------------------------------------------------
# Invariant 2 + 8: no invented keys, natural-stats cache, manifest honesty
# ---------------------------------------------------------------------------

@needs_tools
class TestManifestAndStats:
    @pytest.mark.parametrize("backend", ["python", pytest.param("cpp", marks=needs_cpp)])
    def test_realized_shares_match_the_manifest(self, tmp_path, backend):
        rows = make_channel("store", 4000, lines=1, mirror_jitter=True)
        write_tables(tmp_path, rows)
        rewrite(tmp_path, backend=backend, enable_key_skew=True, key_skew_overrides=T60)
        assert manifest(tmp_path)["rewrite_backend"] == backend
        pairs = manifest(tmp_path)["key_skew_pairs"]
        vanilla = {"date", "time", "return_date", "return_time"}   # medium tier caps 0.0
        assert sorted(pairs) == sorted(
            f"store.{d}" for d in CHANNELS["store"]["domains"] if d not in vanilla
        )
        for key, entry in pairs.items():
            ctable, ccol = entry["canonical"].split(".")
            before = [r[col(ctable, ccol)] for r in split_rows(rows[ctable])]
            non_null = [v for v in before if v not in NULLISH]
            natural_hot, natural_cnt = Counter(non_null).most_common(1)[0]
            assert entry["value"] == natural_hot, key         # hot key = natural dominant (REAL value)
            g, s = len(non_null) / len(before), natural_cnt / len(non_null)
            assert entry["target"] == pytest.approx(0.6)
            assert entry["f1"] == pytest.approx((0.6 / g - s) / (1 - s), abs=1e-9), key
            after = [r[col(ctable, ccol)] for r in read_rows(tmp_path / f"{ctable}.tbl")]
            realized = after.count(entry["value"]) / len(after)
            # Entity-granular domains decide per key VALUE, so the realized share
            # fluctuates with the number of entities in the fixture (binomial
            # over n entities), not with the number of rows.
            n_entities = len(set(non_null))
            tol = 0.03 if entry.get("granularity") != "entity" else max(0.03, 3 * (0.24 / n_entities) ** 0.5)
            assert abs(realized - 0.6) < tol, (key, realized, tol)
            for qualified in entry["columns"]:
                t, c = qualified.split(".")
                if (t, c) == (ctable, ccol):
                    continue
                # Mirror columns realise g'*(s' + f1*(1-s')) of THEIR OWN natural
                # stats (by design not T; documented in the findings).
                bvals = [r[col(t, c)] for r in split_rows(rows[t])]
                nn = [v for v in bvals if v not in NULLISH]
                s2, g2 = nn.count(entry["value"]) / len(nn), len(nn) / len(bvals)
                avals = [r[col(t, c)] for r in read_rows(tmp_path / f"{t}.tbl")]
                expected = g2 * (s2 + entry["f1"] * (1 - s2))
                mtol = 0.04 if entry.get("granularity") != "entity" else max(0.04, 3 * (0.24 / len(set(nn))) ** 0.5)
                assert abs(avals.count(entry["value"]) / len(avals) - expected) < mtol, qualified

    def test_serialized_rules_and_manifest_pairs_are_the_same_set(self, tmp_path):
        write_tables(tmp_path, {**make_channel("store", 60), **make_channel("catalog", 60)})
        payload = stringify.build_rewrite_rules(
            enable_stringify=False, enable_nulls=False, enable_mcv=False,
            enable_key_skew=True,
            key_skew_overrides={**T60, "exclude_pairs": ["store.store"], "domain_caps": {"promo": 0.0}},
            min_ndv_for_injection=0, ndv_cache_dir=str(tmp_path / "_cache"),
            source_data_dir=tmp_path,
        )
        ks = payload["key_skew"]
        assert ks["enabled"] is True
        rule_cols = {f"{t}.{r['name']}" for t, rules in ks["rules"].items() for r in rules}
        pair_cols = {c for e in ks["pairs"].values() for c in e["columns"]}
        assert rule_cols == pair_cols, "manifest pairs != serialized rules"
        for t, rules in ks["rules"].items():
            for r in rules:
                e = ks["pairs"][f"{r['channel']}.{r['domain']}"]
                assert (r["f1"], r["value"]) == (e["f1"], e["value"])
                assert r["index"] == col(t, r["name"])
                assert r["identity_index"] == col(t, channel_of(t)["identity_columns"][t])
        assert "store.store" not in ks["pairs"]                 # exclude_pairs
        assert "store.promo" not in ks["pairs"]                 # domain cap 0.0 ...
        assert "catalog.promo" not in ks["pairs"]               # ... for every channel
        assert not any(k.startswith("web.") for k in ks["pairs"])   # no web files -> no stats -> no pairs
        assert "catalog.customer" in ks["pairs"] and "store.customer" in ks["pairs"]
        # YAML round trip keeps hot values as strings.
        loaded = yaml.safe_load(yaml.safe_dump(payload, sort_keys=False))
        for rules in loaded["key_skew"]["rules"].values():
            assert all(isinstance(r["value"], str) for r in rules)

    def test_counter_cap_never_invents_a_key_but_may_miss_the_true_dominant(self, tmp_path):
        cap = stringify.NATURAL_STATS_COUNTER_CAP
        idx_c, idx_t = col("store_sales", "ss_customer_sk"), col("store_sales", "ss_ticket_number")
        ncols = len(schema()["store_sales"]["columns"])

        def row(ticket: int, customer: int) -> str:
            v = ["1"] * ncols
            v[idx_t], v[idx_c] = str(ticket), str(customer)
            return "|".join(v) + "|\n"

        rows = [row(t, 10_000 + t) for t in range(cap + 10)]          # cap+10 distinct values first
        rows += [row(cap + 10 + i, 77) for i in range(cap)]            # then the TRUE dominant (~50%)
        real = {r.split("|")[idx_c] for r in rows}
        for name in ("a", "b"):
            d = tmp_path / name
            write_tables(d, {"store_sales": rows})
            rewrite(d, enable_key_skew=True, key_skew_overrides=T60)
        a, b = (manifest(tmp_path / n)["key_skew_pairs"]["store.customer"] for n in ("a", "b"))
        assert a == b, "hot key choice is not deterministic across reruns"
        assert a["value"] in real, "invented key"
        assert a["value"] != "77", "documented limitation: cap hides the late true dominant"
        after = [r[idx_c] for r in read_rows(tmp_path / "a" / "store_sales.tbl")]
        # customer is entity-granular: the true dominant (77, half the rows) is
        # either absorbed whole or kept whole, so the realized share is not the
        # target here -- only that no key is invented and entities stay atomic.
        before_vals = [r.split("|")[idx_c] for r in rows]
        outcomes = {}
        for bv, av in zip(before_vals, after):
            outcomes.setdefault(bv, set()).add(av)
        assert all(len(v) == 1 for v in outcomes.values()), "entity split across outcomes"
        assert set(after) <= real

    def test_natural_stats_cache_hit_returns_identical_stats(self, tmp_path, monkeypatch):
        rows = make_channel("store", 40, lines=1)["store_sales"]
        d = tmp_path / "d"
        write_tables(d, {"store_sales": rows})
        eligible = {"store_sales": ["ss_customer_sk", "ss_store_sk"]}
        first = stringify._load_natural_mcv_stats(
            schema(), eligible, source_data_dir=d, cache_dir=tmp_path / "c", scale_factor=None
        )
        assert set(first) == {"store_sales.ss_customer_sk", "store_sales.ss_store_sk"}
        monkeypatch.setattr(
            stringify, "_scan_natural_mcv_stats",
            lambda *a, **k: pytest.fail("cache miss on unchanged data"),
        )
        second = stringify._load_natural_mcv_stats(
            schema(), eligible, source_data_dir=d, cache_dir=tmp_path / "c", scale_factor=None
        )
        assert second == first

    def test_natural_stats_cache_fingerprint_reacts_to_content_size_and_mtime(self, tmp_path):
        rows = make_channel("store", 30, lines=1)["store_sales"]
        d = tmp_path / "d"
        write_tables(d, {"store_sales": rows})
        f = d / "store_sales.tbl"
        eligible = {"store_sales": ["ss_customer_sk"]}

        def fingerprint() -> Path:
            return stringify._natural_stats_cache_path(
                cache_dir=tmp_path / "c", source_data_dir=d, schema=schema(),
                eligible_map=eligible, scale_factor=None,
            )

        base = fingerprint()
        data, st = f.read_bytes(), f.stat()
        assert len(data) < 4096   # whole file inside the hashed head
        # (a) same size, same mtime, different content -> new key
        f.write_bytes(b"2" + data[1:])
        os.utime(f, ns=(st.st_atime_ns, st.st_mtime_ns))
        assert fingerprint() != base
        # (b) same content, different mtime -> new key
        f.write_bytes(data)
        os.utime(f, ns=(st.st_atime_ns, st.st_mtime_ns + 1_000_000))
        assert fingerprint() != base
        # (c) same size, same mtime, same content -> same key (deterministic)
        os.utime(f, ns=(st.st_atime_ns, st.st_mtime_ns))
        assert fingerprint() == base
        # (d) different size -> new key
        f.write_bytes(data + b"\n")
        os.utime(f, ns=(st.st_atime_ns, st.st_mtime_ns))
        assert fingerprint() != base

    def test_partitioned_files_reach_the_same_decisions(self, tmp_path):
        rows = make_channel("store", 120, lines=1)
        single, split = tmp_path / "single", tmp_path / "split"
        write_tables(single, rows)
        split.mkdir()
        half = len(rows["store_sales"]) // 2
        (split / "store_sales_1_2.tbl").write_text("".join(rows["store_sales"][:half]))
        (split / "store_sales_2_2.tbl").write_text("".join(rows["store_sales"][half:]))
        (split / "store_returns.tbl").write_text("".join(rows["store_returns"]))
        for d in (single, split):
            rewrite(d, enable_key_skew=True, key_skew_overrides=T60)
        assert manifest(single)["key_skew_pairs"] == manifest(split)["key_skew_pairs"]
        joined = read_rows(split / "store_sales_1_2.tbl") + read_rows(split / "store_sales_2_2.tbl")
        assert joined == read_rows(single / "store_sales.tbl")


# ---------------------------------------------------------------------------
# Invariant 7: config plumbing precedence
# ---------------------------------------------------------------------------

PROFILE_YAML = """
profiles:
  key_fleet_default:
    max_value_buckets: [{weight: 1.0, min: 0.5, max: 0.5}]
    domain_caps: {date: 0.0, ship_date: 0.0}
    exclude_pairs: ["store.promo"]
  key_low:
    max_value_buckets: [{weight: 1.0, min: 0.1, max: 0.1}]
  key_high:
    max_value_buckets: [{weight: 1.0, min: 0.9, max: 0.9}]
"""
B7 = [{"weight": 1.0, "min": 0.7, "max": 0.7}]
B3 = [{"weight": 1.0, "min": 0.3, "max": 0.3}]


@pytest.fixture
def layered_config(tmp_path, monkeypatch):
    """Synthetic profile YAML + a controllable config.yml `key_skew:` layer."""
    profile_path = tmp_path / "key_skew_profiles.yml"
    profile_path.write_text(PROFILE_YAML, encoding="utf-8")
    monkeypatch.setattr(config_mod, "KEY_SKEW_PROFILES_PATH", profile_path)
    state: Dict[str, dict] = {"key_skew": {}}
    monkeypatch.setattr(config_mod, "load_config", lambda: {"key_skew": dict(state["key_skew"])})

    def configure(**config_yml_key_skew):
        state["key_skew"] = config_yml_key_skew

    return configure


def _field(merged: dict, field: str):
    value = merged
    for part in field.split("."):
        value = value[part]
    return value


class TestConfigPrecedence:
    @pytest.mark.parametrize(
        "config_yml,overrides,profile,field,expected",
        [
            ({}, None, None, "max_value_buckets", [{"weight": 1.0, "min": 0.5, "max": 0.5}]),
            ({"max_value_buckets": B7}, None, None, "max_value_buckets", B7),
            ({"max_value_buckets": B7}, {"max_value_buckets": B3}, None, "max_value_buckets", B3),
            ({}, None, None, "domain_caps", {"date": 0.0, "ship_date": 0.0}),
            ({"domain_caps": {"store": 0.2}}, None, None, "domain_caps.store", 0.2),
            ({"domain_caps": {"store": 0.2}}, {"domain_caps": {"store": 0.4}}, None, "domain_caps.store", 0.4),
            ({}, None, None, "exclude_pairs", ["store.promo"]),
            ({"exclude_pairs": ["web.promo"]}, None, None, "exclude_pairs", ["web.promo"]),
            ({"exclude_pairs": ["web.promo"]}, {"exclude_pairs": ["catalog.promo"]}, None, "exclude_pairs", ["catalog.promo"]),
            ({}, None, None, "seed", 0),
            ({"seed": 5}, None, None, "seed", 5),
            ({"seed": 5}, {"seed": 9}, None, "seed", 9),
            ({}, None, None, "enabled", False),
            ({"enabled": True}, None, None, "enabled", True),
            ({"enabled": True}, {"enabled": False}, None, "enabled", False),
            ({}, None, None, "profile", "key_fleet_default"),
            ({"profile": "low"}, None, None, "profile", "key_low"),
            ({"profile": "low"}, {"profile": "high"}, None, "profile", "key_high"),
            ({"profile": "low"}, {"profile": "high"}, "low", "profile", "key_low"),
            ({}, None, "medium", "tier_alias", "medium"),
            ({}, None, "HIGH", "profile", "key_high"),
            ({}, None, "key_high", "max_value_buckets", [{"weight": 1.0, "min": 0.9, "max": 0.9}]),
        ],
    )
    def test_profile_yaml_lt_config_yml_lt_overrides(
        self, layered_config, config_yml, overrides, profile, field, expected
    ):
        layered_config(**config_yml)
        merged = key_skew_rules(overrides=overrides, profile=profile)
        assert _field(merged, field) == expected

    def test_canonical_profile_names_carry_no_tier_alias(self, layered_config):
        assert key_skew_rules(profile="key_high").get("tier_alias") is None

    @pytest.mark.parametrize("layer", ["config_yml", "overrides"])
    def test_partial_domain_caps_keep_the_profile_caps(self, layered_config, layer):
        if layer == "config_yml":
            layered_config(domain_caps={"store": 0.3})
            merged = key_skew_rules()
        else:
            merged = key_skew_rules(overrides={"domain_caps": {"store": 0.3}})
        assert merged["domain_caps"] == {"date": 0.0, "ship_date": 0.0, "store": 0.3}

    def test_unknown_profile_name_is_not_silently_relabelled(self, layered_config):
        try:
            merged = key_skew_rules(profile="key_hihg")
        except (ValueError, KeyError):
            return
        assert merged["profile"] != "key_hihg"

    def test_missing_profile_file_falls_back_to_the_shipped_medium_tier(self, tmp_path, monkeypatch):
        shipped = key_skew_rules(profile="medium")
        monkeypatch.setattr(config_mod, "KEY_SKEW_PROFILES_PATH", tmp_path / "missing.yml")
        fallback = key_skew_rules(profile="medium")
        assert max(b["max"] for b in fallback["max_value_buckets"]) <= max(
            b["max"] for b in shipped["max_value_buckets"]
        )
        assert fallback.get("domain_caps") == shipped.get("domain_caps")


@needs_tools
class TestRewritePrecedence:
    def test_explicit_rewrite_parameters_beat_overrides(self, tmp_path):
        rows = make_channel("store", 40, lines=1, value_fn=uniform_value)
        cases = [
            (
                dict(enable_key_skew=False, key_skew_overrides={**T60, "enabled": True}),
                lambda m: m["key_skew_enabled"] is False and m["key_skew_pairs"] == {},
            ),
            (
                dict(enable_key_skew=True, key_skew_seed=3, key_skew_overrides={**T60, "seed": 9}),
                lambda m: m["key_skew_seed"] == 3 and m["key_skew_enabled"] is True,
            ),
            (
                dict(key_skew_profile="low", key_skew_overrides={"profile": "high"}),
                lambda m: m["key_skew_profile"] == "key_low" and m["key_skew_tier_alias"] == "low",
            ),
            (
                dict(key_skew_overrides={**T60, "enabled": True}),
                lambda m: m["key_skew_enabled"] is True,
            ),
            (dict(key_skew_overrides=T60), lambda m: m["key_skew_enabled"] is False),   # library default off
        ]
        for i, (kwargs, check) in enumerate(cases):
            d = tmp_path / str(i)
            write_tables(d, rows)
            rewrite(d, **kwargs)
            assert check(manifest(d)), (i, kwargs, manifest(d)["key_skew_profile"])

    def test_disabled_axis_leaves_bytes_untouched(self, tmp_path):
        rows = make_channel("store", 40, lines=1)
        write_tables(tmp_path, rows)
        before = (tmp_path / "store_sales.tbl").read_bytes()
        rewrite(tmp_path, enable_key_skew=False, key_skew_profile="high", key_skew_seed=5)
        assert (tmp_path / "store_sales.tbl").read_bytes() == before


class TestCliPlumbing:
    @pytest.fixture
    def run_main(self, tmp_path, monkeypatch):
        captured: Dict[str, object] = {}
        monkeypatch.setattr(wrap_dsdgen, "_resolve_dsdgen_binary", lambda: Path("/bin/true"))
        monkeypatch.setattr(wrap_dsdgen, "_run_dsdgen", lambda binary, args: 0)
        monkeypatch.setattr(wrap_dsdgen, "_run_rewrite", lambda output_dir, **kw: captured.update(kw))

        def run(argv: Sequence[str]) -> Dict[str, object]:
            captured.clear()
            assert wrap_dsdgen.main([*argv, "-DIR", str(tmp_path / "out")]) == 0
            assert captured, "rewrite stage was not invoked"
            return dict(captured)

        return run

    @pytest.mark.parametrize(
        "argv,expected",
        [
            ([], {"key_skew_enabled": True, "key_skew_profile": None, "key_skew_seed": None}),
            (["--disable-key-skew"], {"key_skew_enabled": False}),
            (
                ["--disable-key-skew", "--skew-profile", "high", "--key-skew-profile", "high", "--key-skew-seed", "4"],
                {"key_skew_enabled": False, "null_enabled": True, "mcv_enabled": True},
            ),
            (["--skew-profile", "high"], {"null_profile": "high", "mcv_profile": "high", "key_skew_profile": "high"}),
            (
                ["--skew-profile", "high", "--key-skew-profile", "low"],
                {"null_profile": "high", "mcv_profile": "high", "key_skew_profile": "low"},
            ),
            (
                ["--skew-profile", "low", "--null-profile", "high", "--mcv-profile", "medium"],
                {"null_profile": "high", "mcv_profile": "medium", "key_skew_profile": "low"},
            ),
            (["--key-skew-seed", "9"], {"key_skew_seed": 9, "key_skew_enabled": True}),
            (["--default"], {"null_profile": "medium", "mcv_profile": "medium", "key_skew_profile": None, "key_skew_enabled": True}),
            (["--disable-null-skew", "--disable-mcv-skew"], {"null_enabled": False, "mcv_enabled": False, "key_skew_enabled": True}),
        ],
    )
    def test_cli_precedence(self, run_main, argv, expected):
        got = run_main(argv)
        for key, value in expected.items():
            assert got[key] == value, (argv, key, got.get(key))

    def test_cli_none_profile_resolves_to_medium(self):
        merged = key_skew_rules(profile=None)
        assert merged["profile"] == "key_fleet_default"
        assert max(b["max"] for b in merged["max_value_buckets"]) <= 0.80


# ---------------------------------------------------------------------------
# Invariant 6: python <-> C++ parity and the stale-binary probe
# ---------------------------------------------------------------------------

def _u64(seed: str, parts: Sequence[str]) -> int:
    """Reference: blake2b-8 over the RAW bytes (surrogate-escaped strings map back
    to their original bytes) -- exactly what the C++ side hashes."""
    hasher = hashlib.blake2b(digest_size=8)
    hasher.update(seed.encode("utf-8"))
    for part in parts:
        hasher.update(b"\x1f")
        hasher.update(part.encode("utf-8", errors="surrogateescape"))
    return int.from_bytes(hasher.digest(), "big")


def _cpp_hash(seed: str, parts: Sequence[str]) -> int:
    argv = [str(CPP_BIN), "--print-hash", seed, *parts]
    raw = subprocess.check_output([a.encode("utf-8", "surrogateescape") for a in argv])
    return int(raw.decode("ascii").strip())


EMPTY_RULES = {
    "stringify": {"enabled": False, "rules": {}},
    "nulls": {"enabled": False, "seed": 0, "null_marker": "", "rules": {}},
    "mcv": {"enabled": False, "seed": 0, "rules": {}},
}

STALE_STUB = '''#!/usr/bin/env python3
"""Mimics a pre-key-skew stringify_cpp: its summary has no key_skew_applied and,
if it is ever pointed at real data, it destroys it (so a probe leak is visible)."""
import json, pathlib, sys
a = sys.argv[1:]
o = {a[i]: a[i + 1] for i in range(0, len(a) - 1, 2) if a[i].startswith("--")}
out = pathlib.Path(o["--output-dir"])
with pathlib.Path(__file__).with_suffix(".log").open("a") as log:
    log.write(str(out) + "\\n")
n = 0
for p in out.glob("*.tbl"):
    p.write_text("")
    n += 1
json.dump({"files_rewritten": n, "rows_rewritten": 0, "duration_s": 0.0}, open(o["--summary-json"], "w"))
'''

CRASHING_STUB = '''#!/usr/bin/env python3
"""Passes the capability probe, then fails AFTER rewriting one real file in place."""
import json, pathlib, sys
a = sys.argv[1:]
o = {a[i]: a[i + 1] for i in range(0, len(a) - 1, 2) if a[i].startswith("--")}
out = pathlib.Path(o["--output-dir"])
files = sorted(out.glob("*.tbl"))
if not files:
    json.dump({"files_rewritten": 0, "rows_rewritten": 0, "duration_s": 0.0,
               "key_skew_applied": True, "key_skew_entity_supported": True},
              open(o["--summary-json"], "w"))
    sys.exit(0)
files[0].write_text(files[0].read_text() + "PARTIAL|\\n")
sys.exit(1)
'''


@needs_tools
@needs_cpp
class TestCppParity:
    @pytest.mark.parametrize(
        "ident", ["7", "007", " 7", "7 ", "7\x1f7", "é7", "-7", "+7", "1e5", "0777", ""]
    )
    def test_hash_parity_for_odd_identities(self, ident):
        parts = ["key-skew", "store", "customer", ident]
        expected = _u64("23", parts)
        assert _cpp_hash("23", parts) == expected
        assert unit_hash(23, *parts) == expected / float(2**64)

    def test_hash_parity_for_invalid_utf8_identity(self):
        parts = ["key-skew", "store", "customer", "7\udcff"]   # b"7\xff" read via surrogateescape
        expected = _u64("23", parts)
        assert _cpp_hash("23", parts) == expected               # C++ hashes the raw bytes
        assert unit_hash(23, *parts) == expected / float(2**64)  # python must agree

    def test_decision_boundary_matches_python_exactly(self, tmp_path):
        """f1 == h, one ulp below and one ulp above h: python (`h < f1` on doubles)
        and C++ (long double division -> double, yaml-cpp float parse) must take
        the same decision for every cell."""
        seed = 23
        domains = [
            ("customer", "ss_customer_sk"), ("cdemo", "ss_cdemo_sk"), ("hdemo", "ss_hdemo_sk"),
            ("addr", "ss_addr_sk"), ("store", "ss_store_sk"), ("promo", "ss_promo_sk"),
        ]
        rows = make_channel("store", 60, lines=1)["store_sales"]
        idx_id = col("store_sales", "ss_ticket_number")
        identities = [r.split("|")[idx_id] for r in rows]
        probes = {domain: identities[7 * k + 3] for k, (domain, _) in enumerate(domains)}
        for variant in ("exact", "below", "above"):
            rules = []
            for k, (domain, column) in enumerate(domains):
                h = unit_hash(seed, "key-skew", "store", domain, probes[domain])
                f1 = {"exact": h, "below": math.nextafter(h, 0.0), "above": math.nextafter(h, 1.0)}[variant]
                rules.append(
                    stringify.KeySkewColumnRule(
                        index=col("store_sales", column), name=column, identity_index=idx_id,
                        channel="store", domain=domain, f1=f1, value=f"HOT{k}",
                    )
                )
            py_dir = tmp_path / f"py_{variant}"
            write_tables(py_dir, {"store_sales": rows})
            injector = stringify.KeySkewInjector(schema(), {"enabled": False})
            injector.enabled, injector.seed, injector.rules = True, seed, {"store_sales": rules}
            stringify.process_tbl(
                py_dir / "store_sales.tbl", py_dir / "out.tbl", "store_sales", {}, key_injector=injector
            )
            cpp_dir = tmp_path / f"cpp_{variant}"
            write_tables(cpp_dir, {"store_sales": rows})
            payload = {
                **EMPTY_RULES,
                "key_skew": {
                    "enabled": True, "seed": seed,
                    "rules": {"store_sales": [
                        {"index": r.index, "name": r.name, "identity_index": r.identity_index,
                         "channel": r.channel, "domain": r.domain, "f1": r.f1, "value": r.value}
                        for r in rules
                    ]},
                },
            }
            rules_path = tmp_path / f"rules_{variant}.yml"
            rules_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
            summary = tmp_path / f"summary_{variant}.json"
            subprocess.run(
                [str(CPP_BIN), "--output-dir", str(cpp_dir), "--rules-file", str(rules_path),
                 "--summary-json", str(summary)],
                check=True,
            )
            assert json.loads(summary.read_text())["key_skew_applied"] is True
            assert (py_dir / "out.tbl").read_bytes() == (cpp_dir / "store_sales.tbl").read_bytes(), variant
            out = {r[idx_id]: r for r in read_rows(py_dir / "out.tbl")}
            for k, (domain, column) in enumerate(domains):
                cell = out[probes[domain]][col("store_sales", column)]
                assert (cell == f"HOT{k}") == (variant == "above"), (variant, domain)

    @pytest.mark.parametrize("hot", ["0777", "1e5", "12:30", "0x1F", "007"])
    def test_numeric_looking_hot_values_survive_yaml_and_apply_verbatim(self, tmp_path, hot):
        def value_fn(domain: str, order: int) -> str:
            if domain == "customer":
                return hot if order % 3 == 0 else f"{hot}{order % 50}"
            return natural_value(domain, order)

        rows = make_channel("store", 90, value_fn=value_fn)
        outs = {}
        for backend in ("python", "cpp"):
            d = tmp_path / backend
            write_tables(d, rows)
            rewrite(d, backend=backend, enable_key_skew=True, key_skew_overrides=T60)
            m = manifest(d)
            assert m["rewrite_backend"] == backend
            assert m["key_skew_pairs"]["store.customer"]["value"] == hot
            outs[backend] = tuple((d / f"{t}.tbl").read_bytes() for t in rows)
        assert outs["python"] == outs["cpp"], hot
        assert assert_documented_contract(tmp_path / "python", rows, seed=7) > 0

    @pytest.mark.parametrize("level", [1, 10])
    def test_full_pipeline_parity_at_str_levels(self, tmp_path, level):
        null_overrides = {
            "column_selection_fraction": 1.0,
            "buckets": [{"weight": 1.0, "min": 0.2, "max": 0.2}], "seed": 21,
        }
        mcv_overrides = {
            "column_selection_fraction": 1.0,
            "max_value_buckets": [{"weight": 1.0, "min": 0.7, "max": 0.7}], "seed": 22,
        }
        rows = {**make_channel("store", 100, mirror_jitter=True), **make_channel("web", 60)}
        outs = {}
        for backend in ("python", "cpp"):
            d = tmp_path / backend
            write_tables(d, rows)
            stringify.rewrite_tbl_directory(
                d, max_workers=2, backend=backend,
                enable_stringify=None, stringification_level=level,
                enable_nulls=True, null_overrides=null_overrides,
                enable_mcv=True, mcv_overrides=mcv_overrides,
                enable_key_skew=True, key_skew_overrides=T60,
                min_ndv_for_injection=0, ndv_cache_dir=str(d / "_cache"),
            )
            assert manifest(d)["rewrite_backend"] == backend
            assert manifest(d)["key_skew_enabled"] is True
            outs[backend] = tuple((d / f"{t}.tbl").read_bytes() for t in sorted(rows))
        assert outs["python"] == outs["cpp"], level

    @staticmethod
    def _install_stub(tmp_path: Path, monkeypatch, source: str) -> Path:
        stub = tmp_path / "stub_stringify_cpp.py"
        stub.write_text(source, encoding="utf-8")
        stub.chmod(0o755)
        monkeypatch.setattr(stringify, "_resolve_cpp_binary", lambda build_if_missing=False: stub)
        return stub

    def test_stale_binary_auto_mode_falls_back_before_touching_data(self, tmp_path, monkeypatch):
        stub = self._install_stub(tmp_path, monkeypatch, STALE_STUB)
        rows = make_channel("store", 60)
        d = tmp_path / "data"
        write_tables(d, rows)
        rewrite(d, backend="auto", enable_key_skew=True, key_skew_overrides=T60)
        m = manifest(d)
        assert m["rewrite_backend"] == "python" and m["key_skew_enabled"] is True
        assert assert_documented_contract(d, rows, seed=7) > 0
        calls = stub.with_suffix(".log").read_text().splitlines()
        assert calls and all("capability_probe" in c for c in calls), calls

    def test_stale_binary_cpp_mode_raises_before_touching_data(self, tmp_path, monkeypatch):
        stub = self._install_stub(tmp_path, monkeypatch, STALE_STUB)
        rows = make_channel("store", 60)
        d = tmp_path / "data"
        write_tables(d, rows)
        before = {t: (d / f"{t}.tbl").read_bytes() for t in rows}
        with pytest.raises(RuntimeError, match="key-skew"):
            rewrite(d, backend="cpp", enable_key_skew=True, key_skew_overrides=T60)
        assert {t: (d / f"{t}.tbl").read_bytes() for t in rows} == before
        calls = stub.with_suffix(".log").read_text().splitlines()
        assert calls and all("capability_probe" in c for c in calls), calls

    def test_cpp_failure_after_partial_rewrite_is_not_silently_retried(self, tmp_path, monkeypatch):
        self._install_stub(tmp_path, monkeypatch, CRASHING_STUB)
        rows = make_channel("store", 60)
        d = tmp_path / "data"
        write_tables(d, rows)
        with pytest.raises(RuntimeError):
            rewrite(d, backend="auto", enable_key_skew=True, key_skew_overrides=T60)

    def test_seed_beyond_int32_works_on_both_backends(self, tmp_path):
        # Since the N8 fix the binary parses seeds as long long: a 64-bit seed
        # must work natively on the cpp path and match python byte for byte.
        rows = make_channel("store", 60)
        d_py = tmp_path / "py"
        d_cpp = tmp_path / "cpp"
        for d in (d_py, d_cpp):
            write_tables(d, rows)
        rewrite(d_py, backend="python", enable_key_skew=True,
                key_skew_overrides={**T60, "seed": 2**40})
        rewrite(d_cpp, backend="cpp", enable_key_skew=True,
                key_skew_overrides={**T60, "seed": 2**40})
        assert manifest(d_cpp)["rewrite_backend"] == "cpp"
        assert manifest(d_cpp)["key_skew_seed"] == 2**40
        for name in sorted(f.name for f in d_py.glob("*.tbl")):
            assert (d_py / name).read_bytes() == (d_cpp / name).read_bytes(), name
        assert assert_documented_contract(d_py, rows, seed=2**40) > 0


# ---------------------------------------------------------------------------
# Invariant 9: reproduce.sh axis isolation (static)
# ---------------------------------------------------------------------------

class TestReproduceScript:
    def test_axis_isolation_flags(self):
        calls = [
            line.strip()
            for line in REPRODUCE_SH.read_text(encoding="utf-8").splitlines()
            if line.strip().startswith("generate_data_variant ")
        ]
        assert len(calls) >= 8, calls

        def calls_for(token: str) -> List[str]:
            subset = [c for c in calls if token in c]
            assert subset, f"no generate_data_variant call for {token}"
            return subset

        # Since the N6 fix, "combined" means NULL+MCV only (the E5 decomposition
        # stays additive), so it disables key skew like the other E5 arms and is
        # generated explicitly rather than symlinked to the prodds default.
        for token in ("tpcds_sf", "sparsity/baseline", "sparsity/sparsity_only",
                      "sparsity/skew_only", "sparsity/combined"):
            assert all("--disable-key-skew" in c for c in calls_for(token)), token
        for token in ("prodds_sf", "str_sweep/str${str}", "_len${len}"):
            assert not any("--disable-key-skew" in c for c in calls_for(token)), token
        vanilla = calls_for("tpcds_sf")[0]
        assert all(f in vanilla for f in ("--disable-null-skew", "--disable-mcv-skew", "--disable-key-skew"))
        assert 'ln -sfn "$(data_dir prodds_sf${SF}_str${PRODDS_STR})"' not in REPRODUCE_SH.read_text()
