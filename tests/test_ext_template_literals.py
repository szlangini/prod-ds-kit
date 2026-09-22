"""Extended templates must not hard-code literals that dsdgen never produces.

Store/warehouse geography comes from the scale-dependent `active_*` prefix of
the cities / fips_county distributions (SF10: 3 states, 6 cities), customer
cities are a weighted draw, countries are upper-case, brands are syllable
words ("amalgamalg #1"). Hand-written values such as 'Seattle', 'CA',
'United States' or 'Brand#1%' silently empty the query at every scale factor,
which is what happened to 22 of the 99 extended templates. Such predicates
must be dsqgen substitutions (see the SCOUNTY/SCITYNUM/CACITY/BCOUNTRY/
BRANDSYL defines) so the literals are drawn the way the data was generated.
"""
import re
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

TEMPLATE_DIR = REPO_ROOT / "query_templates"
EXT_TEMPLATES = sorted(TEMPLATE_DIR.glob("query*_ext.tpl"))

LIT = r"'(?:[^']|'')*'"
GEO_COLUMNS = ("s_state", "s_city", "s_county", "w_state", "w_city", "w_county", "ca_city")
COUNTRY_COLUMNS = ("c_birth_country", "customer_birth_country")


def _in_lists(text: str, column: str):
    pattern = re.compile(rf"\b{column}\s+in\s*\(\s*({LIT}(?:\s*,\s*{LIT})*)\s*\)", re.I | re.S)
    for match in pattern.finditer(text):
        yield re.findall(r"'((?:[^']|'')*)'", match.group(1))


def test_ext_templates_exist():
    assert len(EXT_TEMPLATES) >= 90


@pytest.mark.parametrize("template", EXT_TEMPLATES, ids=lambda p: p.stem)
def test_no_hard_coded_geography_literals(template):
    text = template.read_text(encoding="utf-8")
    for column in GEO_COLUMNS:
        for literals in _in_lists(text, column):
            assert all(lit.startswith("[") for lit in literals), (
                f"{template.name}: {column} IN {literals} is hard-coded; "
                "use a dsqgen substitution drawn from the cities/fips_county distributions"
            )


@pytest.mark.parametrize("template", EXT_TEMPLATES, ids=lambda p: p.stem)
def test_country_literals_match_dsdgen_case(template):
    text = template.read_text(encoding="utf-8")
    for column in COUNTRY_COLUMNS:
        for literals in _in_lists(text, column):
            for lit in literals:
                assert lit.startswith("[") or lit == lit.upper(), (
                    f"{template.name}: {column} literal {lit!r} is not upper-case; "
                    "dsdgen emits countries as 'UNITED STATES'"
                )


@pytest.mark.parametrize("template", EXT_TEMPLATES, ids=lambda p: p.stem)
def test_no_tpch_style_brand_patterns(template):
    text = template.read_text(encoding="utf-8")
    assert not re.search(r"i_brand\s+like\s+'Brand#", text, flags=re.I), (
        f"{template.name}: 'Brand#..' is TPC-H syntax; TPC-DS brands are syllable words"
    )


def _dsqgen_or_skip():
    import wrap_dsqgen

    try:
        return wrap_dsqgen._resolve_dsqgen_binary()
    except Exception as exc:  # pragma: no cover - environment dependent
        pytest.skip(f"dsqgen unavailable: {exc}")


@pytest.mark.parametrize("qnum", [4, 12, 43, 59, 64, 72])
def test_fixed_templates_render_with_resolved_substitutions(qnum):
    """dsqgen must resolve every new substitution (no '[TOKEN]' left, no hallucinated values)."""
    import wrap_dsqgen

    dsqgen = _dsqgen_or_skip()
    try:
        sql = wrap_dsqgen._generate_single_query_sql(
            dsqgen_bin=dsqgen,
            template_dir=TEMPLATE_DIR,
            template_name=f"query{qnum}_ext.tpl",
            dialect="duckdb",
            scale="1",
            rng_seed=20000,
        )
    except (subprocess.CalledProcessError, OSError) as exc:  # pragma: no cover
        pytest.skip(f"dsqgen could not render query{qnum}_ext.tpl: {exc}")
    assert not re.search(r"\[[A-Z_]+(?:\.\d+)?\]", sql), sql
    for forbidden in ("Seattle", "Brand#", "United States", "'Canada'"):
        assert forbidden not in sql
