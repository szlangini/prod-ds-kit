"""Seed-overridden queries must receive the same scaled LIMIT as the main path.

wrap_dsqgen scales each template's `_LIMIT` by the scale factor in
_postprocess_limits, which keys on dsqgen's "using template" header. Queries
regenerated through the seed-override path never had that header, so they
shipped with the template's unscaled LIMIT (q68: 1000 instead of 10000 at SF10).
"""
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import wrap_dsqgen  # noqa: E402


def _template_dir(tmp_path, body):
    (tmp_path / "query7_ext.tpl").write_text(body, encoding="utf-8")
    return tmp_path


def test_override_limit_is_scaled_like_the_main_path(tmp_path):
    tdir = _template_dir(tmp_path, "define _LIMIT=1000;\n[_LIMITA] select [_LIMITB] 1 from t order by 1 [_LIMITC];\n")
    sql = "select 1 from t order by 1 limit 1000"
    assert wrap_dsqgen._apply_template_limit(sql, "query7_ext.tpl", tdir, "10").rstrip() == "select 1 from t order by 1 limit 10000"
    assert wrap_dsqgen._apply_template_limit(sql, "query7_ext.tpl", tdir, 1).rstrip() == sql


def test_no_macro_strips_limit_and_unknown_template_is_left_alone(tmp_path):
    # mirrors limit_postprocess: an ext template without _LIMIT macros has its
    # LIMIT removed; a template unknown to the mapping is not touched at all
    tdir = _template_dir(tmp_path, "define _LIMIT=1000;\nselect 1 from t;\n")
    sql = "select 1 from t limit 5"
    assert "limit" not in wrap_dsqgen._apply_template_limit(sql, "query7_ext.tpl", tdir, "10").lower()
    assert wrap_dsqgen._apply_template_limit(sql, "query99_ext.tpl", tdir, "10") == sql
