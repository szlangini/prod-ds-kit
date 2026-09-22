"""dsqgen copies distribution values verbatim into single-quoted literals; the
`countries` distribution contains CÔTE D'IVOIRE, which produced unparseable
SQL for ~1% of country draws. wrap_dsqgen escapes such literals."""
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import wrap_dsqgen  # noqa: E402


def test_known_apostrophe_values_are_loaded():
    assert "CÔTE D'IVOIRE" in wrap_dsqgen._distribution_apostrophe_values()


def test_escape_is_applied_and_idempotent():
    sql = "select 1 where c_birth_country in ('GUAM','CÔTE D'IVOIRE','PERU')"
    once = wrap_dsqgen._escape_distribution_apostrophes(sql)
    assert "'CÔTE D''IVOIRE'" in once
    assert wrap_dsqgen._escape_distribution_apostrophes(once) == once


def test_statement_truncation_survives_escaped_literal():
    sql = "select 1 where x in ('CÔTE D'IVOIRE');\nselect 2;"
    assert wrap_dsqgen._sanitize_generated_sql(sql).rstrip() == "select 1 where x in ('CÔTE D''IVOIRE')"
