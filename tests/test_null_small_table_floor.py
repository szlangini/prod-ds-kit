"""Small-dimension NULL floor (stringify.NULL_MIN_NONNULL_ROWS).

A per-column NULL probability is size-independent; on a 12-row dimension a 0.9
probability leaves 0-1 non-NULL rows and every predicate on the column empties.
The floor keeps at least k naturally non-NULL rows per nulled column, realized as
an exact hash threshold so Python and C++ null the very same rows.
"""
import json
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from workload.dsdgen import stringify  # noqa: E402

CPP_BIN = REPO_ROOT / "workload" / "dsdgen" / "stringify_cpp"
TABLE = "warehouse"
COLUMN = "w_state"
MARKER = "\\N"


def _null_cfg(probability: float, **extra):
    cfg = stringify.null_skew_rules()
    cfg.update(
        {
            "enabled": True,
            "seed": 4242,
            "null_marker": MARKER,
            "min_ndv_for_injection": 0,
            "column_selection_fraction": 0.0,  # only the explicit rule below
            "column_probabilities": {f"{TABLE}.{COLUMN}": probability},
        }
    )
    cfg.update(extra)
    return cfg


@pytest.mark.needs_tpcds_tools
class NullSmallTableFloorTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.schema = stringify._schema_cache()  # type: ignore[attr-defined]
        cls.columns = cls.schema[TABLE]["columns"]
        cls.col_index = [c.lower() for c in cls.columns].index(COLUMN)

    def _write_table(self, directory: Path, row_count: int, natural_nulls: int = 0) -> Path:
        directory.mkdir(parents=True, exist_ok=True)
        lines = []
        for i in range(row_count):
            values = [f"v{i}_{idx}" for idx in range(len(self.columns))]
            values[0] = str(i + 1)  # w_warehouse_sk
            if i < natural_nulls:
                values[self.col_index] = ""  # dsdgen-native NULL
            lines.append("|".join(values) + "|\n")
        path = directory / f"{TABLE}_1_4.dat"  # parallel dsdgen naming
        path.write_text("".join(lines), encoding="utf-8")
        return path

    def _surviving(self, path: Path) -> int:
        count = 0
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line:
                continue
            cell = line.split("|")[self.col_index]
            if cell not in ("", MARKER):
                count += 1
        return count

    def _rewrite(self, directory: Path, probability: float, backend: str = "python", **extra) -> None:
        stringify.rewrite_tbl_directory(
            directory,
            max_workers=1,
            backend=backend,
            enable_stringify=False,
            enable_nulls=True,
            null_marker=MARKER,
            null_seed=4242,
            null_overrides=_null_cfg(probability, **extra),
            min_ndv_for_injection=0,
            enable_mcv=False,
            enable_key_skew=False,
        )

    def test_floor_binds_on_tiny_table(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp) / "data"
            path = self._write_table(data_dir, 12)
            injector = stringify.NullInjector(self.schema, _null_cfg(0.9), source_data_dir=data_dir)
            rule = next(r for r in injector.rules[TABLE] if r.name.lower() == COLUMN)
            self.assertLess(rule.probability, 0.9)
            self.assertEqual(rule.target_probability, 0.9)
            self.assertEqual(rule.floor_rows, 12)
            self.assertEqual(len(injector.floor_applied), 1)
            self.assertEqual(injector.floor_applied[0]["nulled_rows"], 12 - stringify.NULL_MIN_NONNULL_ROWS)
            self._rewrite(data_dir, 0.9)
            # Exactly k survivors, not a Bernoulli draw.
            self.assertEqual(self._surviving(path), stringify.NULL_MIN_NONNULL_ROWS)
            # The data manifest records every capped column for auditability.
            manifest = json.loads(
                (data_dir / "stringification_data_manifest.json").read_text(encoding="utf-8")
            )
            floor = manifest["null_small_table_floor"]
            self.assertEqual(floor["min_nonnull_rows"], stringify.NULL_MIN_NONNULL_ROWS)
            self.assertEqual(
                [(c["table"], c["column"], c["nulled_rows"]) for c in floor["capped"]],
                [(TABLE, COLUMN, 12 - stringify.NULL_MIN_NONNULL_ROWS)],
            )

    def test_floor_counts_only_naturally_non_null_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp) / "data"
            path = self._write_table(data_dir, 12, natural_nulls=5)
            injector = stringify.NullInjector(self.schema, _null_cfg(0.9), source_data_dir=data_dir)
            rule = next(r for r in injector.rules[TABLE] if r.name.lower() == COLUMN)
            self.assertEqual(rule.floor_rows, 7)
            self._rewrite(data_dir, 0.9)
            self.assertEqual(self._surviving(path), stringify.NULL_MIN_NONNULL_ROWS)

    def test_floor_disables_rule_when_table_is_too_small(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp) / "data"
            path = self._write_table(data_dir, 3)
            injector = stringify.NullInjector(self.schema, _null_cfg(0.9), source_data_dir=data_dir)
            self.assertNotIn(TABLE, injector.rules)
            self._rewrite(data_dir, 0.9)
            self.assertEqual(self._surviving(path), 3)

    def test_large_table_keeps_bernoulli_probability(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp) / "data"
            self._write_table(data_dir, 2000)
            injector = stringify.NullInjector(self.schema, _null_cfg(0.9), source_data_dir=data_dir)
            rule = next(r for r in injector.rules[TABLE] if r.name.lower() == COLUMN)
            self.assertEqual(rule.probability, 0.9)
            self.assertIsNone(rule.target_probability)
            self.assertEqual(injector.floor_applied, [])

    def test_floor_can_be_disabled_and_exempted(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp) / "data"
            self._write_table(data_dir, 12)
            off = stringify.NullInjector(
                self.schema, _null_cfg(0.9, min_nonnull_rows=0), source_data_dir=data_dir
            )
            self.assertEqual(off.rules[TABLE][0].probability, 0.9)
            exempt = stringify.NullInjector(
                self.schema, _null_cfg(0.9, floor_exempt_tables=[TABLE]), source_data_dir=data_dir
            )
            self.assertEqual(exempt.rules[TABLE][0].probability, 0.9)
            # Without a data directory (library use, unit tests) nothing is counted.
            plain = stringify.NullInjector(self.schema, _null_cfg(0.9))
            self.assertEqual(plain.rules[TABLE][0].probability, 0.9)

    def test_manifest_reports_floor(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp) / "data"
            self._write_table(data_dir, 12)
            payload = stringify.build_rewrite_rules(
                enable_stringify=False,
                enable_nulls=True,
                null_marker=MARKER,
                null_seed=4242,
                null_overrides=_null_cfg(0.9),
                min_ndv_for_injection=0,
                enable_mcv=False,
                enable_key_skew=False,
                source_data_dir=data_dir,
            )
            nulls = payload["nulls"]
            self.assertEqual(nulls["small_table_floor"]["min_nonnull_rows"], stringify.NULL_MIN_NONNULL_ROWS)
            self.assertEqual(len(nulls["small_table_floor"]["capped"]), 1)
            rule = next(r for r in nulls["rules"][TABLE] if r["name"].lower() == COLUMN)
            self.assertEqual(rule["target_probability"], 0.9)
            self.assertLess(rule["probability"], 0.9)

    @pytest.mark.needs_cpp
    @unittest.skipUnless(CPP_BIN.exists(), "stringify_cpp binary not built")
    def test_cpp_backend_nulls_the_same_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            py_dir = Path(tmp) / "py"
            self._write_table(py_dir, 12, natural_nulls=2)
            cpp_dir = Path(tmp) / "cpp"
            shutil.copytree(py_dir, cpp_dir)
            self._rewrite(py_dir, 0.9, backend="python")
            self._rewrite(cpp_dir, 0.9, backend="cpp")
            py_out = (py_dir / f"{TABLE}_1_4.dat").read_bytes()
            cpp_out = (cpp_dir / f"{TABLE}_1_4.dat").read_bytes()
            self.assertEqual(py_out, cpp_out)
            self.assertEqual(self._surviving(py_dir / f"{TABLE}_1_4.dat"), stringify.NULL_MIN_NONNULL_ROWS)


if __name__ == "__main__":
    unittest.main()
