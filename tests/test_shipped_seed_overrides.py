"""Guard for the shipped seed overrides (the no-empty guarantee of the default workload).

configs/seed_overrides_sf<N>.yml pins, per scale factor, the few dsqgen seeds whose
default draw is empty on the default Prod-DS data. wrap_dsqgen.py applies the file
automatically for every stringification level and dialect, so whoever downloads the
kit gets the validated, non-empty default workload without any flag.
"""
import re
import sys
import unittest
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import wrap_dsqgen  # noqa: E402

CONFIGS = REPO_ROOT / "configs"
# Scale factors the kit ships pinned seeds for, with the documented exceptions:
# templates that are empty at that scale for structural reasons (accepted, see README).
SHIPPED = {
    "1": {"query_4.sql"},   # T4 at SF1: three-channel repeat-customer population of 3 rows
    "10": set(),
    "100": set(),
}


class ShippedSeedOverridesTests(unittest.TestCase):
    def _load(self, sf: str) -> dict:
        path = CONFIGS / f"seed_overrides_sf{sf}.yml"
        self.assertTrue(path.exists(), f"missing shipped override file {path}")
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
        self.assertIsInstance(payload, dict)
        return payload

    def test_shipped_files_parse_and_are_minimal(self) -> None:
        for sf, exceptions in SHIPPED.items():
            payload = self._load(sf)
            meta = payload.get("meta") or {}
            # A per-SF file must apply to every STR level: no level pin in its meta.
            self.assertNotIn("stringification_level", meta, f"sf{sf}: per-SF file must not pin STR")
            self.assertEqual(str(meta.get("scale")), sf)
            queries = payload.get("queries") or {}
            self.assertTrue(queries, f"sf{sf}: no pinned queries")
            for name, entry in queries.items():
                self.assertRegex(name, r"^query_\d+\.sql$")
                self.assertIsInstance(entry.get("seed"), int)
                self.assertRegex(str(entry.get("template")), r"^query\d+(_ext)?\.tpl$")
                self.assertTrue(
                    (REPO_ROOT / "query_templates" / entry["template"]).exists(),
                    f"sf{sf}: {name} pins a template that does not exist",
                )
            unresolved = set((payload.get("unresolved") or {}).keys())
            self.assertEqual(unresolved, exceptions, f"sf{sf}: undocumented unresolved templates")

    def test_sf100_file_when_shipped(self) -> None:
        path = CONFIGS / "seed_overrides_sf100.yml"
        if not path.exists():
            self.skipTest("SF100 seed overrides not shipped yet")
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
        self.assertNotIn("stringification_level", payload.get("meta") or {})
        self.assertEqual(set((payload.get("unresolved") or {}).keys()), set())

    def test_per_sf_file_is_applied_first_for_every_level_and_dialect(self) -> None:
        for level in (1, 5, 10):
            for dialect in ("duckdb", "cedardb", "monetdb", "postgres"):
                paths = wrap_dsqgen._resolve_seed_overrides_paths(
                    scale="10", stringification_level=level, dialect=dialect
                )
                self.assertEqual(paths[0], CONFIGS / "seed_overrides_sf10.yml")
                self.assertFalse(wrap_dsqgen._overrides_file_is_level_specific(paths[0]))
                for extra in paths[1:]:
                    self.assertTrue(wrap_dsqgen._overrides_file_is_level_specific(extra))

    def test_no_stale_level_specific_files_shipped(self) -> None:
        stale = [
            p.name for p in CONFIGS.glob("seed_overrides_sf*_str*.yml")
            if re.search(r"_str\d+\.yml$", p.name)  # dialect-neutral per-STR files
        ]
        self.assertEqual(stale, [], "per-STR override files would shadow the per-SF defaults")


if __name__ == "__main__":
    unittest.main()
