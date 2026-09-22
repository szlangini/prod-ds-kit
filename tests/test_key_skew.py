import json
import shutil
import sys
import tempfile
import unittest
from collections import Counter
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from workload import stringification as stringification_cfg
from workload.dsdgen import stringify
from workload.dsdgen.config import key_skew_rules, KEY_SKEW_TIER_ALIASES
from workload.dsdgen.wrap_dsdgen import _parse_wrapper_args


def _column_indexes(schema: dict, table: str, names: list[str]) -> dict[str, int]:
    columns = schema[table]["columns"]
    return {name: columns.index(name) for name in names}


class KeySkewFixtureMixin:
    """Shared synthetic-channel fixture helpers for the key-skew test classes."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.schema = stringify._schema_cache()  # type: ignore[attr-defined]
        cls.ss_idx = _column_indexes(
            cls.schema,
            "store_sales",
            ["ss_sold_date_sk", "ss_customer_sk", "ss_store_sk", "ss_item_sk", "ss_ticket_number"],
        )
        cls.sr_idx = _column_indexes(
            cls.schema,
            "store_returns",
            ["sr_returned_date_sk", "sr_customer_sk", "sr_store_sk", "sr_item_sk", "sr_ticket_number"],
        )

    def _make_channel_rows(self, tickets: int, lines_per_ticket: int = 3) -> tuple[list[str], list[str]]:
        """Coherent store_sales + store_returns rows: every other ticket has one
        return line whose customer/store/item/ticket match the sales side."""
        ss_columns = self.schema["store_sales"]["columns"]
        sr_columns = self.schema["store_returns"]["columns"]
        ss_rows: list[str] = []
        sr_rows: list[str] = []
        for ticket in range(1, tickets + 1):
            customer = 1000 + ticket % 50
            store = 10 + ticket % 7
            sold_date = 2450000 + ticket % 30
            for line in range(lines_per_ticket):
                item = 5000 + (ticket * lines_per_ticket + line) % 400
                values = [str(100 + idx) for idx in range(len(ss_columns))]
                values[self.ss_idx["ss_ticket_number"]] = str(ticket)
                values[self.ss_idx["ss_customer_sk"]] = str(customer)
                values[self.ss_idx["ss_store_sk"]] = str(store)
                values[self.ss_idx["ss_sold_date_sk"]] = str(sold_date)
                values[self.ss_idx["ss_item_sk"]] = str(item)
                ss_rows.append("|".join(values) + "|\n")
                if line == 0 and ticket % 2 == 0:
                    r_values = [str(200 + idx) for idx in range(len(sr_columns))]
                    r_values[self.sr_idx["sr_ticket_number"]] = str(ticket)
                    r_values[self.sr_idx["sr_customer_sk"]] = str(customer)
                    r_values[self.sr_idx["sr_store_sk"]] = str(store)
                    r_values[self.sr_idx["sr_returned_date_sk"]] = str(sold_date + 40)
                    r_values[self.sr_idx["sr_item_sk"]] = str(item)
                    sr_rows.append("|".join(r_values) + "|\n")
        return ss_rows, sr_rows

    def _write_channel(self, directory: Path, ss_rows: list[str], sr_rows: list[str]) -> None:
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "store_sales.tbl").write_text("".join(ss_rows), encoding="utf-8")
        (directory / "store_returns.tbl").write_text("".join(sr_rows), encoding="utf-8")

    def _rewrite(self, directory: Path, *, max_workers: int = 1, stringify_enabled: bool = False, **kwargs) -> None:
        stringify.rewrite_tbl_directory(
            directory,
            max_workers=max_workers,
            enable_stringify=stringify_enabled,
            stringification_level=5 if stringify_enabled else None,
            enable_nulls=False,
            enable_mcv=False,
            min_ndv_for_injection=0,
            ndv_cache_dir=str(directory / "_cache"),
            **kwargs,
        )

    def _read_rows(self, path: Path) -> list[list[str]]:
        return [line.split("|") for line in path.read_text(encoding="utf-8").splitlines()]

    def _manifest(self, directory: Path) -> dict:
        manifest_path = directory / stringification_cfg.DATA_MANIFEST_NAME
        return json.loads(manifest_path.read_text(encoding="utf-8"))


@pytest.mark.needs_tpcds_tools
class KeySkewTests(KeySkewFixtureMixin, unittest.TestCase):
    """Join-key skew: opt-in, identity-coherent across sales/returns, PK-safe."""

    def test_library_default_leaves_data_untouched(self) -> None:
        # Direct rewrite_tbl_directory() callers must opt in explicitly; only the
        # wrap_dsdgen CLI defaults the axis on (see KeySkewCliTests).
        ss_rows, sr_rows = self._make_channel_rows(40)
        with tempfile.TemporaryDirectory() as tmpdir:
            directory = Path(tmpdir)
            self._write_channel(directory, ss_rows, sr_rows)
            before = (directory / "store_sales.tbl").read_bytes()
            self._rewrite(directory)
            self.assertEqual(before, (directory / "store_sales.tbl").read_bytes())
            manifest = self._manifest(directory)
            self.assertFalse(manifest["key_skew_enabled"])
            self.assertEqual(manifest["key_skew_pairs"], {})

    def test_profile_flag_opts_in(self) -> None:
        ss_rows, sr_rows = self._make_channel_rows(60)
        with tempfile.TemporaryDirectory() as tmpdir:
            directory = Path(tmpdir)
            self._write_channel(directory, ss_rows, sr_rows)
            before = (directory / "store_sales.tbl").read_bytes()
            self._rewrite(directory, key_skew_profile="medium", key_skew_seed=7)
            self.assertNotEqual(before, (directory / "store_sales.tbl").read_bytes())
            manifest = self._manifest(directory)
            self.assertTrue(manifest["key_skew_enabled"])
            self.assertEqual(manifest["key_skew_profile"], "key_fleet_default")
            self.assertEqual(manifest["key_skew_tier_alias"], "medium")
            # Only the store channel exists in this fixture, and only the
            # columns the fixture actually varies can materialize: constant
            # columns already sit at share 1.0, so the monotone solve drops
            # their domains (f1 <= 0). Date-family domains are vanilla in the
            # default tier (domain_caps 0.0), so store.date/store.return_date
            # never materialize.
            self.assertEqual(
                sorted(manifest["key_skew_pairs"]),
                ["store.customer", "store.store"],
            )

    def test_coherence_pk_safety_and_ticket_atomicity(self) -> None:
        overrides = {
            "max_value_buckets": [{"weight": 1.0, "min": 0.6, "max": 0.6}],
            "seed": 77,
        }
        ss_rows, sr_rows = self._make_channel_rows(80)
        with tempfile.TemporaryDirectory() as tmpdir:
            directory = Path(tmpdir)
            self._write_channel(directory, ss_rows, sr_rows)
            self._rewrite(directory, enable_key_skew=True, key_skew_overrides=overrides)

            manifest = self._manifest(directory)
            pairs = manifest["key_skew_pairs"]
            hot_customer = pairs["store.customer"]["value"]
            hot_store = pairs["store.store"]["value"]

            ss_after = self._read_rows(directory / "store_sales.tbl")
            sr_after = self._read_rows(directory / "store_returns.tbl")
            ss_before = [line.rstrip("\n").split("|") for line in ss_rows]

            # PK columns and untargeted columns are untouched.
            for before, after in zip(ss_before, ss_after):
                self.assertEqual(before[self.ss_idx["ss_item_sk"]], after[self.ss_idx["ss_item_sk"]])
                self.assertEqual(
                    before[self.ss_idx["ss_ticket_number"]], after[self.ss_idx["ss_ticket_number"]]
                )
                for idx, (b_cell, a_cell) in enumerate(zip(before, after)):
                    if idx in (
                        self.ss_idx["ss_sold_date_sk"],
                        self.ss_idx["ss_customer_sk"],
                        self.ss_idx["ss_store_sk"],
                    ):
                        continue
                    self.assertEqual(b_cell, a_cell)

            # Per-ticket atomicity on the sales side: all lines of a ticket carry
            # the same (customer, store, date) after injection.
            by_ticket: dict[str, set[tuple[str, str, str]]] = {}
            for row in ss_after:
                key = row[self.ss_idx["ss_ticket_number"]]
                by_ticket.setdefault(key, set()).add(
                    (
                        row[self.ss_idx["ss_customer_sk"]],
                        row[self.ss_idx["ss_store_sk"]],
                        row[self.ss_idx["ss_sold_date_sk"]],
                    )
                )
            for ticket, combos in by_ticket.items():
                self.assertEqual(len(combos), 1, f"ticket {ticket} lost line coherence: {combos}")

            # Returns mirror the sales-side decision: same customer/store per ticket.
            ss_by_ticket = {row[self.ss_idx["ss_ticket_number"]]: row for row in ss_after}
            redirected = 0
            for row in sr_after:
                ticket = row[self.sr_idx["sr_ticket_number"]]
                ss_row = ss_by_ticket[ticket]
                self.assertEqual(
                    row[self.sr_idx["sr_customer_sk"]], ss_row[self.ss_idx["ss_customer_sk"]]
                )
                self.assertEqual(row[self.sr_idx["sr_store_sk"]], ss_row[self.ss_idx["ss_store_sk"]])
                if row[self.sr_idx["sr_customer_sk"]] == hot_customer:
                    redirected += 1
            self.assertGreater(redirected, 0, "no returns ticket was redirected at f1~0.6")

            # Redirected cells carry exactly the manifest hot value.
            customer_values = {row[self.ss_idx["ss_customer_sk"]] for row in ss_after}
            self.assertIn(hot_customer, customer_values)
            store_values = {row[self.ss_idx["ss_store_sk"]] for row in ss_after}
            self.assertIn(hot_store, store_values)

            # Date-family domains are vanilla in the default tier: the returned
            # date must be untouched.
            sr_before = [line.rstrip("\n").split("|") for line in sr_rows]
            for before, after in zip(sr_before, sr_after):
                self.assertEqual(
                    before[self.sr_idx["sr_returned_date_sk"]],
                    after[self.sr_idx["sr_returned_date_sk"]],
                )

    def test_target_share_and_value_retention(self) -> None:
        overrides = {
            "max_value_buckets": [{"weight": 1.0, "min": 0.6, "max": 0.6}],
            "seed": 5,
        }
        ss_rows, sr_rows = self._make_channel_rows(600)
        with tempfile.TemporaryDirectory() as tmpdir:
            directory = Path(tmpdir)
            self._write_channel(directory, ss_rows, sr_rows)
            self._rewrite(directory, enable_key_skew=True, key_skew_overrides=overrides)

            manifest = self._manifest(directory)
            hot_customer = manifest["key_skew_pairs"]["store.customer"]["value"]
            ss_after = self._read_rows(directory / "store_sales.tbl")
            counts = Counter(row[self.ss_idx["ss_customer_sk"]] for row in ss_after)
            share = counts[hot_customer] / sum(counts.values())
            # customer is entity-granular: the realized share fluctuates with the
            # number of customers in the fixture (binomial over entities), not rows
            n_customers = len({row.split("|")[self.ss_idx["ss_customer_sk"]] for row in ss_rows})
            self.assertAlmostEqual(share, 0.6, delta=max(0.15, 3 * (0.24 / n_customers) ** 0.5))

            # Customer is an ENTITY-granular domain: a customer is either absorbed
            # completely or keeps every one of its rows -- survivors are never
            # thinned, and roughly (1 - f1) of the customers survive.
            before_counts = Counter(row.split("|")[self.ss_idx["ss_customer_sk"]] for row in ss_rows)
            survivors = [c for c in before_counts if c in counts and c != hot_customer]
            self.assertGreater(len(survivors) / len(before_counts), 0.2)
            self.assertLess(len(survivors) / len(before_counts), 0.7)
            for customer in survivors:
                self.assertEqual(counts[customer], before_counts[customer])

            # Ticket-granular domains (store): per-cell retention is (1 - f1), so
            # (almost) every original store value survives.
            before_stores = {row.split("|")[self.ss_idx["ss_store_sk"]] for row in ss_rows}
            after_stores = {row[self.ss_idx["ss_store_sk"]] for row in ss_after}
            self.assertGreater(len(before_stores & after_stores) / len(before_stores), 0.95)

    def test_exclude_pairs_blacklists_domains(self) -> None:
        overrides = {
            "max_value_buckets": [{"weight": 1.0, "min": 0.6, "max": 0.6}],
            "seed": 77,
            "exclude_pairs": ["store.customer", "store.date"],
        }
        ss_rows, sr_rows = self._make_channel_rows(60)
        with tempfile.TemporaryDirectory() as tmpdir:
            directory = Path(tmpdir)
            self._write_channel(directory, ss_rows, sr_rows)
            self._rewrite(directory, enable_key_skew=True, key_skew_overrides=overrides)
            pairs = self._manifest(directory)["key_skew_pairs"]
            self.assertNotIn("store.customer", pairs)
            self.assertNotIn("store.date", pairs)
            self.assertIn("store.store", pairs)
            ss_after = self._read_rows(directory / "store_sales.tbl")
            ss_before = [line.rstrip("\n").split("|") for line in ss_rows]
            for before, after in zip(ss_before, ss_after):
                self.assertEqual(
                    before[self.ss_idx["ss_customer_sk"]], after[self.ss_idx["ss_customer_sk"]]
                )
                self.assertEqual(
                    before[self.ss_idx["ss_sold_date_sk"]], after[self.ss_idx["ss_sold_date_sk"]]
                )

    def test_deterministic_across_workers(self) -> None:
        ss_rows, sr_rows = self._make_channel_rows(120)
        with tempfile.TemporaryDirectory() as tmpdir:
            run_one = Path(tmpdir) / "one"
            run_two = Path(tmpdir) / "two"
            self._write_channel(run_one, ss_rows, sr_rows)
            self._write_channel(run_two, ss_rows, sr_rows)
            self._rewrite(run_one, max_workers=1, key_skew_profile="medium", key_skew_seed=3)
            self._rewrite(run_two, max_workers=4, key_skew_profile="medium", key_skew_seed=3)
            for name in ("store_sales.tbl", "store_returns.tbl"):
                self.assertEqual((run_one / name).read_bytes(), (run_two / name).read_bytes())

    @pytest.mark.needs_cpp
    def test_cpp_backend_matches_python_bytes(self) -> None:
        # Full pipeline parity: NULL + key skew + STR5 + MCV must be
        # byte-identical across backends, otherwise SF10/SF100 data generated
        # via cpp diverges from the python-tested semantics.
        null_overrides = {
            "column_selection_fraction": 1.0,
            "buckets": [{"weight": 1.0, "min": 0.2, "max": 0.2}],
            "seed": 21,
        }
        mcv_overrides = {
            "column_selection_fraction": 1.0,
            "max_value_buckets": [{"weight": 1.0, "min": 0.7, "max": 0.7}],
            "seed": 22,
        }
        key_overrides = {
            "max_value_buckets": [{"weight": 1.0, "min": 0.6, "max": 0.6}],
            "seed": 23,
            # Exercise the newer rule-shaping features on the serialized path too.
            "domain_caps": {"store": 0.3},
            "exclude_pairs": ["store.promo"],
        }
        ss_rows, sr_rows = self._make_channel_rows(150)
        with tempfile.TemporaryDirectory() as tmpdir:
            runs = {}
            for backend in ("python", "cpp"):
                directory = Path(tmpdir) / backend
                self._write_channel(directory, ss_rows, sr_rows)
                stringify.rewrite_tbl_directory(
                    directory,
                    max_workers=2,
                    backend=backend,
                    enable_stringify=True,
                    stringification_level=5,
                    enable_nulls=True,
                    null_overrides=null_overrides,
                    enable_mcv=True,
                    mcv_overrides=mcv_overrides,
                    enable_key_skew=True,
                    key_skew_overrides=key_overrides,
                    min_ndv_for_injection=0,
                    ndv_cache_dir=str(directory / "_cache"),
                )
                runs[backend] = directory
            for name in ("store_sales.tbl", "store_returns.tbl"):
                self.assertEqual(
                    (runs["python"] / name).read_bytes(),
                    (runs["cpp"] / name).read_bytes(),
                    f"backend divergence in {name}",
                )

    def test_stringification_keeps_join_keys_coherent(self) -> None:
        overrides = {
            "max_value_buckets": [{"weight": 1.0, "min": 0.6, "max": 0.6}],
            "seed": 11,
        }
        ss_rows, sr_rows = self._make_channel_rows(80)
        with tempfile.TemporaryDirectory() as tmpdir:
            directory = Path(tmpdir)
            self._write_channel(directory, ss_rows, sr_rows)
            self._rewrite(
                directory,
                stringify_enabled=True,
                enable_key_skew=True,
                key_skew_overrides=overrides,
            )
            ss_after = self._read_rows(directory / "store_sales.tbl")
            sr_after = self._read_rows(directory / "store_returns.tbl")
            ss_by_ticket = {row[self.ss_idx["ss_ticket_number"]]: row for row in ss_after}
            hot_seen = False
            for row in sr_after:
                ticket = row[self.sr_idx["sr_ticket_number"]]
                ss_row = ss_by_ticket[ticket]
                # Post-STR the customer keys are strings; the redirected hot key
                # must have received the same domain prefix on both sides.
                sr_customer = row[self.sr_idx["sr_customer_sk"]]
                ss_customer = ss_row[self.ss_idx["ss_customer_sk"]]
                self.assertEqual(sr_customer, ss_customer)
                self.assertFalse(sr_customer.isdigit(), "customer key should be stringified at STR5")
                hot_seen = True
            self.assertTrue(hot_seen)


@pytest.mark.needs_tpcds_tools
class KeySkewRobustnessTests(KeySkewFixtureMixin, unittest.TestCase):
    """Guards against the dumb failure classes: invented key values, clobbered
    NULLs, lost rows/fields, ignored seeds, missing channel salt, row-order
    dependence, and cap semantics."""

    OVERRIDES = {
        "max_value_buckets": [{"weight": 1.0, "min": 0.6, "max": 0.6}],
        "seed": 7,
    }

    def _skewed_dir(self, tmpdir: str, ss_rows, sr_rows, **kwargs):
        directory = Path(tmpdir)
        self._write_channel(directory, ss_rows, sr_rows)
        overrides = dict(self.OVERRIDES)
        overrides.update(kwargs.pop("extra_overrides", {}))
        self._rewrite(directory, enable_key_skew=True, key_skew_overrides=overrides, **kwargs)
        return directory

    def test_no_invented_key_values(self) -> None:
        # Every post-skew FK value must already exist pre-skew (reuse-natural-
        # value => referential integrity can never break), and the manifest hot
        # value must be one of them.
        ss_rows, sr_rows = self._make_channel_rows(120)
        with tempfile.TemporaryDirectory() as tmpdir:
            directory = self._skewed_dir(tmpdir, ss_rows, sr_rows)
            pairs = self._manifest(directory)["key_skew_pairs"]
            before_vals = {
                row.split("|")[self.ss_idx["ss_customer_sk"]] for row in ss_rows
            }
            after_vals = {
                row[self.ss_idx["ss_customer_sk"]]
                for row in self._read_rows(directory / "store_sales.tbl")
            }
            self.assertTrue(after_vals <= before_vals, after_vals - before_vals)
            self.assertIn(pairs["store.customer"]["value"], before_vals)

    def test_null_cells_and_null_identities_untouched(self) -> None:
        ss_rows, sr_rows = self._make_channel_rows(60)
        # Blank out the customer FK on some rows and the ticket identity on others.
        edited = []
        for i, row in enumerate(ss_rows):
            fields = row.rstrip("\n").split("|")
            if i % 7 == 0:
                fields[self.ss_idx["ss_customer_sk"]] = ""
            if i % 11 == 0:
                fields[self.ss_idx["ss_ticket_number"]] = ""
            edited.append("|".join(fields) + "\n")
        with tempfile.TemporaryDirectory() as tmpdir:
            directory = self._skewed_dir(tmpdir, edited, sr_rows)
            after = self._read_rows(directory / "store_sales.tbl")
            for i, (before, row) in enumerate(zip(edited, after)):
                b = before.rstrip("\n").split("|")
                if i % 7 == 0:
                    self.assertEqual(row[self.ss_idx["ss_customer_sk"]], "",
                                     "NULL FK cell must never be filled with a hot key")
                if i % 11 == 0:
                    # ticket-granular domains (store) need the identity; the
                    # entity-granular customer decision ignores it by design.
                    self.assertEqual(b[self.ss_idx["ss_store_sk"]],
                                     row[self.ss_idx["ss_store_sk"]],
                                     "rows without identity must not be redirected (ticket domains)")

    def test_structure_preserved_and_foreign_tables_untouched(self) -> None:
        ss_rows, sr_rows = self._make_channel_rows(80)
        item_rows = ["|".join(str(1000 + i + j) for j in range(5)) + "|\n" for i in range(20)]
        with tempfile.TemporaryDirectory() as tmpdir:
            directory = Path(tmpdir)
            self._write_channel(directory, ss_rows, sr_rows)
            # A file whose table has no key rules must come out byte-identical.
            (directory / "warehouse.tbl").write_text("".join(item_rows), encoding="utf-8")
            before_bytes = (directory / "warehouse.tbl").read_bytes()
            self._rewrite(directory, enable_key_skew=True,
                          key_skew_overrides=dict(self.OVERRIDES))
            self.assertEqual(before_bytes, (directory / "warehouse.tbl").read_bytes())
            after = (directory / "store_sales.tbl").read_text(encoding="utf-8").splitlines()
            self.assertEqual(len(after), len(ss_rows), "row count changed")
            n_fields = len(ss_rows[0].rstrip("\n").split("|"))
            for line in after:
                self.assertEqual(len(line.split("|")), n_fields, "field count changed")

    def test_row_order_independence(self) -> None:
        # Decisions hash the logical identity, never the row position: the same
        # returns rows in reverse order must get identical per-ticket outcomes.
        ss_rows, sr_rows = self._make_channel_rows(90)
        with tempfile.TemporaryDirectory() as tmpdir:
            d1 = self._skewed_dir(str(Path(tmpdir) / "a"), ss_rows, sr_rows)
            d2 = self._skewed_dir(str(Path(tmpdir) / "b"), ss_rows, list(reversed(sr_rows)))
            def by_ticket(directory):
                return {
                    (r[self.sr_idx["sr_ticket_number"]], r[self.sr_idx["sr_item_sk"]]):
                        r[self.sr_idx["sr_customer_sk"]]
                    for r in self._read_rows(directory / "store_returns.tbl")
                }
            self.assertEqual(by_ticket(d1), by_ticket(d2))

    def test_seed_changes_output(self) -> None:
        ss_rows, sr_rows = self._make_channel_rows(120)
        outputs = []
        for seed in (1, 2):
            with tempfile.TemporaryDirectory() as tmpdir:
                directory = self._skewed_dir(
                    tmpdir, ss_rows, sr_rows, extra_overrides={"seed": seed})
                outputs.append((directory / "store_sales.tbl").read_bytes())
        self.assertNotEqual(outputs[0], outputs[1], "key_skew seed is ignored")

    def test_channel_salt_in_decision_hash(self) -> None:
        # store tickets and catalog orders share the same numeric identities; if
        # the channel were missing from the hash, both channels would redirect
        # exactly the same identity subset.
        tickets = 80
        ss_rows, _ = self._make_channel_rows(tickets, lines_per_ticket=1)
        cs_columns = self.schema["catalog_sales"]["columns"]
        cs_idx = _column_indexes(
            self.schema, "catalog_sales", ["cs_order_number", "cs_bill_customer_sk"])
        cs_rows = []
        for ticket in range(1, tickets + 1):
            values = [str(300 + i) for i in range(len(cs_columns))]
            values[cs_idx["cs_order_number"]] = str(ticket)
            values[cs_idx["cs_bill_customer_sk"]] = str(1000 + ticket % 50)
            cs_rows.append("|".join(values) + "|\n")
        with tempfile.TemporaryDirectory() as tmpdir:
            directory = Path(tmpdir)
            self._write_channel(directory, ss_rows, [])
            (directory / "catalog_sales.tbl").write_text("".join(cs_rows), encoding="utf-8")
            # The default profile blacklists catalog.customer (q81 avg-poisoning);
            # clear it here -- this test needs both channels' customer domains hot.
            self._rewrite(directory, enable_key_skew=True,
                          key_skew_overrides={**self.OVERRIDES, "exclude_pairs": []})
            pairs = self._manifest(directory)["key_skew_pairs"]
            hot_store = pairs["store.customer"]["value"]
            hot_catalog = pairs["catalog.customer"]["value"]
            ss_after = self._read_rows(directory / "store_sales.tbl")
            cs_after = self._read_rows(directory / "catalog_sales.tbl")
            store_hits = [row[self.ss_idx["ss_customer_sk"]] == hot_store
                          for row in ss_after]
            catalog_hits = [row[cs_idx["cs_bill_customer_sk"]] == hot_catalog
                            for row in cs_after]
            self.assertTrue(any(store_hits) and any(catalog_hits))
            # customer is ENTITY-granular: the decision hashes the customer value
            # WITHOUT the channel, so both channels redirect the same customers
            # (nested by f1). Ticket-granular domains keep the channel salt.
            seed = self.OVERRIDES["seed"]
            f_store, f_cat = pairs["store.customer"]["f1"], pairs["catalog.customer"]["f1"]
            for ticket in range(1, tickets + 1):
                customer = str(1000 + ticket % 50)
                u = stringify._stable_unit_hash(seed, "key-skew-entity", "customer", customer)
                self.assertEqual(store_hits[ticket - 1] or customer == hot_store,
                                 u < f_store or customer == hot_store, (ticket, "store"))
                self.assertEqual(catalog_hits[ticket - 1] or customer == hot_catalog,
                                 u < f_cat or customer == hot_catalog, (ticket, "catalog"))
            self.assertNotEqual(
                stringify._stable_unit_hash(seed, "key-skew", "store", "cdemo", "5"),
                stringify._stable_unit_hash(seed, "key-skew", "catalog", "cdemo", "5"),
                "channel missing from the ticket-granular decision hash?")

    def test_domain_caps_semantics(self) -> None:
        ss_rows, sr_rows = self._make_channel_rows(80)
        with tempfile.TemporaryDirectory() as tmpdir:
            directory = self._skewed_dir(
                tmpdir, ss_rows, sr_rows,
                extra_overrides={"domain_caps": {"customer": 0.35, "store": 0.0}})
            pairs = self._manifest(directory)["key_skew_pairs"]
            self.assertNotIn("store.store", pairs, "cap 0.0 must disable the domain")
            self.assertLessEqual(pairs["store.customer"]["target"], 0.35)


class KeySkewCliTests(unittest.TestCase):
    def test_cli_defaults_key_skew_on(self) -> None:
        parsed, _ = _parse_wrapper_args([])
        self.assertFalse(parsed.disable_key_skew)
        self.assertIsNone(parsed.key_skew_profile)

    def test_cli_disable_flag(self) -> None:
        parsed, _ = _parse_wrapper_args(["--disable-key-skew"])
        self.assertTrue(parsed.disable_key_skew)

    def test_cli_profile_and_seed(self) -> None:
        parsed, _ = _parse_wrapper_args(["--key-skew-profile", "high", "--key-skew-seed", "9"])
        self.assertEqual(parsed.key_skew_profile, "high")
        self.assertEqual(parsed.key_skew_seed, 9)

    def test_cli_umbrella_skew_profile(self) -> None:
        parsed, _ = _parse_wrapper_args(["--skew-profile", "high"])
        self.assertEqual(parsed.skew_profile, "high")
        parsed, _ = _parse_wrapper_args(["--skew-profile", "high", "--mcv-profile", "low"])
        self.assertEqual(parsed.skew_profile, "high")
        self.assertEqual(parsed.mcv_profile, "low")


class KeySkewProfileTests(unittest.TestCase):
    def test_library_config_defaults_off(self) -> None:
        # The CLI passes the toggle explicitly (default on); the library-level
        # config default stays off so programmatic callers opt in consciously.
        cfg = key_skew_rules()
        self.assertFalse(cfg["enabled"])

    def test_alias_resolution_and_buckets(self) -> None:
        for alias, canonical in KEY_SKEW_TIER_ALIASES.items():
            with self.subTest(alias=alias):
                via_alias = key_skew_rules(profile=alias)
                via_canonical = key_skew_rules(profile=canonical)
                self.assertEqual(via_alias["profile"], via_canonical["profile"])
                buckets = via_alias.get("max_value_buckets") or []
                self.assertGreaterEqual(len(buckets), 2)
                total_weight = sum(b["weight"] for b in buckets)
                self.assertAlmostEqual(total_weight, 1.0, delta=0.001)


if __name__ == "__main__":
    unittest.main()


class KeySkewEntityGranularityTests(KeySkewFixtureMixin, unittest.TestCase):
    """Customer keys redirect per ENTITY (key value); other domains per ticket."""

    def test_customer_entities_keep_or_lose_their_whole_history(self) -> None:
        overrides = {"max_value_buckets": [{"weight": 1.0, "min": 0.6, "max": 0.6}], "seed": 11}
        ss_rows, sr_rows = self._make_channel_rows(300)
        with tempfile.TemporaryDirectory() as tmpdir:
            directory = Path(tmpdir)
            self._write_channel(directory, ss_rows, sr_rows)
            self._rewrite(directory, enable_key_skew=True, key_skew_overrides=overrides)
            pairs = self._manifest(directory)["key_skew_pairs"]
            self.assertEqual(pairs["store.customer"]["granularity"], "entity")
            self.assertEqual(pairs["store.store"]["granularity"], "ticket")
            hot = pairs["store.customer"]["value"]
            f1 = pairs["store.customer"]["f1"]
            before = [line.rstrip("\n").split("|") for line in ss_rows]
            after = self._read_rows(directory / "store_sales.tbl")
            outcomes: dict[str, set[str]] = {}
            for b, a in zip(before, after):
                outcomes.setdefault(b[self.ss_idx["ss_customer_sk"]], set()).add(
                    a[self.ss_idx["ss_customer_sk"]]
                )
            redirected = 0
            for customer, values in outcomes.items():
                # entity atomicity: a customer never splits across outcomes
                self.assertEqual(len(values), 1, f"customer {customer} split: {values}")
                expect_hot = stringify._stable_unit_hash(11, "key-skew-entity", "customer", customer) < f1
                self.assertEqual(values == {hot}, expect_hot or customer == hot)
                redirected += int(expect_hot and customer != hot)
            self.assertGreater(redirected, 0)
            self.assertLess(redirected, len(outcomes))
            # returns follow the same per-customer decision (coherence without tickets)
            sr_before = [line.rstrip("\n").split("|") for line in sr_rows]
            for b, a in zip(sr_before, self._read_rows(directory / "store_returns.tbl")):
                self.assertEqual(
                    a[self.sr_idx["sr_customer_sk"]],
                    next(iter(outcomes[b[self.sr_idx["sr_customer_sk"]]])),
                )

    def test_entity_decisions_are_channel_independent(self) -> None:
        stat = stringify.NaturalColumnStat(value="1", share_nn=0.01, null_rate=0.0)
        stats = {"store_sales.ss_customer_sk": stat, "web_sales.ws_bill_customer_sk": stat}
        cfg = {
            "enabled": True,
            "seed": 5,
            "max_value_buckets": [{"weight": 1.0, "min": 0.5, "max": 0.5}],
            "entity_domains": ["customer"],
        }
        injector = stringify.KeySkewInjector(self.schema, cfg, natural_stats=stats)
        ss_cols = self.schema["store_sales"]["columns"]
        ws_cols = self.schema["web_sales"]["columns"]
        ss_c, ss_t = ss_cols.index("ss_customer_sk"), ss_cols.index("ss_ticket_number")
        ws_c, ws_o = ws_cols.index("ws_bill_customer_sk"), ws_cols.index("ws_order_number")
        redirected = 0
        for customer in range(2, 400):
            ss = [""] * len(ss_cols)
            ss[ss_c], ss[ss_t] = str(customer), str(customer * 7)
            ws = [""] * len(ws_cols)
            ws[ws_c], ws[ws_o] = str(customer), str(customer * 13)
            injector.apply_to_row("store_sales", ss)
            injector.apply_to_row("web_sales", ws)
            # same f1 in both channels -> identical decision, regardless of ticket/order
            self.assertEqual(ss[ss_c] == "1", ws[ws_c] == "1", f"customer {customer} diverged")
            redirected += int(ss[ss_c] == "1")
        self.assertGreater(redirected, 60)
        self.assertLess(redirected, 340)

    def test_entity_hot_key_is_shared_across_channels(self) -> None:
        """The whale customer is ONE dimension row: every channel redirects to the
        anchor channel's (store) dominant value, not to its own."""
        def stat(value: str) -> "stringify.NaturalColumnStat":
            return stringify.NaturalColumnStat(value=value, share_nn=0.01, null_rate=0.0)

        stats = {
            "store_sales.ss_customer_sk": stat("1"),
            "catalog_sales.cs_bill_customer_sk": stat("2"),
            "web_sales.ws_bill_customer_sk": stat("3"),
            # a per-ticket domain keeps its own per-channel value
            "store_sales.ss_store_sk": stat("7"),
        }
        cfg = {
            "enabled": True,
            "seed": 5,
            "max_value_buckets": [{"weight": 1.0, "min": 0.5, "max": 0.5}],
            "entity_domains": ["customer"],
        }
        injector = stringify.KeySkewInjector(self.schema, cfg, natural_stats=stats)
        customer_values = {
            rule.channel: rule.value
            for table_rules in injector.rules.values()
            for rule in table_rules
            if rule.domain == "customer"
        }
        self.assertEqual(set(customer_values), {"store", "catalog", "web"})
        self.assertEqual(set(customer_values.values()), {"1"})
        self.assertEqual(injector.pair_targets["catalog.customer"]["entity_anchor"], "store")
        self.assertEqual(injector.pair_targets["store.store"]["value"], "7")
        self.assertIsNone(injector.pair_targets["store.store"]["entity_anchor"])
        # a non-default anchor is honoured
        cfg_web = dict(cfg, entity_anchor_channel="web")
        injector_web = stringify.KeySkewInjector(self.schema, cfg_web, natural_stats=stats)
        self.assertEqual(
            {rule.value for tr in injector_web.rules.values() for rule in tr if rule.domain == "customer"},
            {"3"},
        )
