"""Per-query dialect fixers must follow the TEMPLATE, not the output position.

In STREAMS mode dsqgen permutes templates into output files; the fixers
(`_rewrite_duckdb_query_fixes`, `_rewrite_query_N`) are keyed by template
number. Applying them by output filename silently mis-fixes the shipped query
set (template 25 at position 9 never received its d_moy fix, "query_25.sql"
got it instead) -- the root cause of a set of always-empty queries.
"""
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import wrap_dsqgen  # noqa: E402

# A query-25-shaped snippet: the duckdb fixer rewrites d1.d_moy = <n> to 3.
Q25_SNIPPET = "select 1 from date_dim d1 where d1.d_moy = 7 and d2.d_moy between 7 and 12"


def test_fixer_key_maps_position_to_template():
    perm = {25: 9, 9: 25, 4: 93}
    assert wrap_dsqgen._fixer_key_for("query_9.sql", perm) == "query_25.sql"
    assert wrap_dsqgen._fixer_key_for("query_93.sql", perm) == "query_4.sql"
    # unknown position / micro-suite names / no permutation -> unchanged
    assert wrap_dsqgen._fixer_key_for("query_50.sql", perm) == "query_50.sql"
    assert wrap_dsqgen._fixer_key_for("query_union_U8.sql", perm) == "query_union_U8.sql"
    assert wrap_dsqgen._fixer_key_for("query_9.sql", None) == "query_9.sql"


def test_postprocess_applies_fixer_to_template_not_position(tmp_path):
    # Template 25 sits at output position 9; template 9 sits at position 25.
    (tmp_path / "query_9.sql").write_text(Q25_SNIPPET, encoding="utf-8")
    (tmp_path / "query_25.sql").write_text(Q25_SNIPPET, encoding="utf-8")
    wrap_dsqgen._postprocess_duckdb(tmp_path, {25: 9, 9: 25})
    fixed = (tmp_path / "query_9.sql").read_text(encoding="utf-8")
    untouched = (tmp_path / "query_25.sql").read_text(encoding="utf-8")
    assert "d1.d_moy = 3" in fixed, "template 25 at position 9 must get the q25 fix"
    assert "d1.d_moy = 7" in untouched, "position 25 (holding template 9) must NOT get it"


def test_postprocess_without_permutation_keeps_legacy_keying(tmp_path):
    (tmp_path / "query_25.sql").write_text(Q25_SNIPPET, encoding="utf-8")
    wrap_dsqgen._postprocess_duckdb(tmp_path, None)
    assert "d1.d_moy = 3" in (tmp_path / "query_25.sql").read_text(encoding="utf-8")
