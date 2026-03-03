#!/usr/bin/env bash
# install.sh — set up Prod-DS Kit and build the TPC-DS toolkit.
#
# Idempotent: safe to re-run. Auto-detects Linux vs macOS for the build step.
# By running this script you agree to the TPC End User License Agreement
# (see NOTICE.md for details).
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")" && pwd)"
TPCDS_KIT_DIR="$REPO_ROOT/tpcds-kit"
TPCDS_KIT_COMMIT="5a3a81796992b725c2a8b216767e142609966752"
TPCDS_KIT_REPO="https://github.com/gregrahn/tpcds-kit.git"
VENV_DIR="$REPO_ROOT/.venv"

# ---------- helpers ----------
info()  { printf '\033[1;34m[install]\033[0m %s\n' "$*"; }
ok()    { printf '\033[1;32m[install]\033[0m %s\n' "$*"; }
warn()  { printf '\033[1;33m[install]\033[0m %s\n' "$*"; }
fail()  { printf '\033[1;31m[install]\033[0m %s\n' "$*" >&2; exit 1; }

# ---------- Step 1: Check Python >= 3.9 ----------
info "Checking Python version..."
PYTHON=""
for candidate in python3 python; do
    if command -v "$candidate" >/dev/null 2>&1; then
        ver=$("$candidate" -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
        major=${ver%%.*}
        minor=${ver#*.}
        if [ "$major" -ge 3 ] && [ "$minor" -ge 9 ]; then
            PYTHON="$candidate"
            break
        fi
    fi
done
[ -n "$PYTHON" ] || fail "Python >= 3.9 is required. Found none."
ok "Using $PYTHON ($ver)"

# ---------- Step 2: Create virtual environment ----------
if [ ! -d "$VENV_DIR" ]; then
    info "Creating virtual environment in .venv/"
    "$PYTHON" -m venv "$VENV_DIR"
fi
# shellcheck disable=SC1091
source "$VENV_DIR/bin/activate"
ok "Virtual environment active: $VENV_DIR"

# ---------- Step 3: Install Python package ----------
info "Installing Prod-DS Kit (editable) with test dependencies..."
pip install --upgrade pip --quiet
pip install -e ".[test]" --quiet
ok "Python package installed."

# ---------- Step 4: Clone TPC-DS toolkit ----------
if [ ! -d "$TPCDS_KIT_DIR" ]; then
    info "Cloning TPC-DS toolkit (gregrahn/tpcds-kit)..."
    git clone --quiet "$TPCDS_KIT_REPO" "$TPCDS_KIT_DIR"
    (cd "$TPCDS_KIT_DIR" && git checkout --quiet "$TPCDS_KIT_COMMIT")
    ok "TPC-DS toolkit cloned at pinned commit ${TPCDS_KIT_COMMIT:0:12}."
else
    info "TPC-DS toolkit already present at $TPCDS_KIT_DIR"
    CURRENT_COMMIT=$(cd "$TPCDS_KIT_DIR" && git rev-parse HEAD 2>/dev/null || echo "unknown")
    if [ "$CURRENT_COMMIT" != "$TPCDS_KIT_COMMIT" ]; then
        warn "Pinned commit: $TPCDS_KIT_COMMIT"
        warn "Current commit: $CURRENT_COMMIT"
        warn "Consider removing tpcds-kit/ and re-running install.sh to update."
    fi
fi

# ---------- Step 5: Build dsdgen and dsqgen ----------
info "Building TPC-DS tools (dsdgen, dsqgen)..."
case "$(uname -s)" in
    Linux*)  OS_FLAG="LINUX" ;;
    Darwin*) OS_FLAG="MACOS" ;;
    *)       fail "Unsupported OS: $(uname -s). Supported: Linux, macOS." ;;
esac

(cd "$TPCDS_KIT_DIR/tools" && make clean >/dev/null 2>&1 || true && make OS="$OS_FLAG" -j"$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)" >/dev/null 2>&1)
ok "TPC-DS tools built for $OS_FLAG."

# ---------- Step 6: Verify binaries ----------
for bin in dsdgen dsqgen; do
    if [ ! -f "$TPCDS_KIT_DIR/tools/$bin" ]; then
        fail "Expected binary not found: tpcds-kit/tools/$bin"
    fi
done
ok "Verified: dsdgen and dsqgen binaries present."

# ---------- Step 7: Optionally build stringify_cpp ----------
STRINGIFY_CPP_DIR="$REPO_ROOT/workload/dsdgen"
if [ -f "$STRINGIFY_CPP_DIR/Makefile" ]; then
    if command -v g++ >/dev/null 2>&1 || command -v clang++ >/dev/null 2>&1; then
        info "Building stringify_cpp (optional C++ accelerator)..."
        if (cd "$STRINGIFY_CPP_DIR" && make stringify_cpp >/dev/null 2>&1); then
            ok "stringify_cpp built successfully."
        else
            warn "stringify_cpp build failed (non-fatal). Python fallback will be used."
        fi
    else
        info "No C++ compiler found. Skipping stringify_cpp (Python fallback will be used)."
    fi
fi

# ---------- Step 8: Generate templates.lst ----------
info "Generating query_templates/templates.lst from TPC-DS toolkit..."
if [ -f "$TPCDS_KIT_DIR/query_templates/templates.lst" ]; then
    cp "$TPCDS_KIT_DIR/query_templates/templates.lst" "$REPO_ROOT/query_templates/templates.lst"
    ok "templates.lst copied from TPC-DS toolkit."
else
    # Generate from directory listing
    (cd "$TPCDS_KIT_DIR/query_templates" && ls query*.tpl 2>/dev/null | sort -V > "$REPO_ROOT/query_templates/templates.lst")
    ok "templates.lst generated from TPC-DS template directory."
fi

# ---------- Step 9: Smoke test ----------
info "Running smoke test..."
"$PYTHON" -c "from workload import stringification; print('Prod-DS Kit installed successfully.')"

# ---------- Summary ----------
echo ""
echo "============================================="
echo "  Prod-DS Kit installation complete."
echo "============================================="
echo ""
echo "  Python:          $PYTHON ($ver)"
echo "  Virtual env:     $VENV_DIR"
echo "  TPC-DS toolkit:  $TPCDS_KIT_DIR"
echo "  dsdgen:          $TPCDS_KIT_DIR/tools/dsdgen"
echo "  dsqgen:          $TPCDS_KIT_DIR/tools/dsqgen"
echo ""
echo "  Quick start:"
echo "    source .venv/bin/activate"
echo "    python3 wrap_dsdgen.py --stringification-level 10 -DIR ./data/sf1 -SCALE 1"
echo "    python3 wrap_dsqgen.py --output-dir ./queries/sf1"
echo ""
