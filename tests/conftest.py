import shutil
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
TPCDS_TOOLS = REPO_ROOT / "tpcds-kit" / "tools"


def _has_tpcds_tools() -> bool:
    return (TPCDS_TOOLS / "dsdgen").exists() or (TPCDS_TOOLS / "dsdgen.exe").exists()


def _has_cpp_compiler() -> bool:
    return shutil.which("g++") is not None or shutil.which("clang++") is not None


def pytest_configure(config):
    config.addinivalue_line("markers", "needs_tpcds_tools: requires built TPC-DS toolkit")
    config.addinivalue_line("markers", "needs_engine: requires running database engine")
    config.addinivalue_line("markers", "needs_cpp: requires C++ compiler")


def pytest_collection_modifyitems(config, items):
    for item in items:
        if "needs_tpcds_tools" in [m.name for m in item.iter_markers()]:
            if not _has_tpcds_tools():
                item.add_marker(pytest.mark.skip(reason="TPC-DS toolkit not built (run install.sh)"))
        if "needs_cpp" in [m.name for m in item.iter_markers()]:
            if not _has_cpp_compiler():
                item.add_marker(pytest.mark.skip(reason="No C++ compiler available"))
