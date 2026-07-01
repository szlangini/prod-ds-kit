# =============================================================================
#  Prod-DS Kit — reproducibility container (VLDB 2027 EA&B)
#
#  A hermetic Ubuntu 24.04 environment matching the paper's tested stack. The
#  build bakes in the deterministic setup (Python venv + package, the TPC-DS
#  toolkit at its pinned commit, and the stringify_cpp accelerator via
#  install.sh). The database engines (DuckDB / CedarDB / MonetDB) are installed
#  — version-pinned via the ARGs below — by `reproduce.sh --init` on first run,
#  so the image stays lean and the build is free of flaky engine downloads.
#
#  Build:
#    docker build -t prod-ds-kit .
#
#  Run (mount a volume so the multi-GB generated data + results persist and are
#  inspectable on the host):
#    docker run --rm -it -v "$PWD/.reproduce:/opt/prod-ds-kit/.reproduce" prod-ds-kit
#    # then, inside the container:
#    ./reproduce_EAB.sh --quick E1        # SF1 smoke (~15 min, DuckDB)
#    ./reproduce_EAB.sh all               # default scale SF10, all engines
#    ./reproduce_EAB.sh --sf100 all       # published paper scale SF100 (multi-day)
#
#  The engine versions are pinned here, in reproduce.sh, and in REPRODUCIBILITY.md
#  to the SAME values; override at build time with --build-arg if needed.
# =============================================================================
FROM ubuntu:24.04

# ---- Pinned engine versions (kept identical to reproduce.sh + REPRODUCIBILITY.md)
ARG DUCKDB_VERSION=1.4.4
ARG CEDARDB_VERSION=v2026-05-26
ARG MONETDB_VERSION=11.55.5
ENV DUCKDB_VERSION=${DUCKDB_VERSION} \
    CEDARDB_VERSION=${CEDARDB_VERSION} \
    MONETDB_VERSION=${MONETDB_VERSION} \
    DEBIAN_FRONTEND=noninteractive \
    TZ=UTC \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# ---- OS prerequisites -------------------------------------------------------
#  build-essential/bison/flex/cmake: build the TPC-DS dsdgen/dsqgen tools.
#  libyaml-cpp-dev/libsodium-dev:    build the optional stringify_cpp accelerator
#                                    (pre-installed so install.sh skips its sudo path).
#  curl/unzip/git/jq/sudo:           required by reproduce.sh --init + engine installs.
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential gcc g++ make cmake ninja-build pkg-config bison flex \
      git curl wget unzip ca-certificates jq sudo tzdata \
      python3 python3-venv python3-pip python3-dev \
      libyaml-cpp-dev libsodium-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/prod-ds-kit
COPY . /opt/prod-ds-kit

# ---- Deterministic setup: venv + package + TPC-DS toolkit + stringify_cpp ----
#  install.sh is idempotent and clones the TPC-DS toolkit at a pinned commit.
RUN bash install.sh

# Generated data + results live under .reproduce/ at runtime. Mount it explicitly
# (-v "$PWD/.reproduce:/opt/prod-ds-kit/.reproduce") to keep/inspect them on the
# host; no VOLUME directive, so the default run stays on a single overlay fs.

CMD ["bash", "-lc", "\
echo '── Prod-DS Kit reproducibility container ─────────────────────────────'; \
echo 'Engines (pinned):  DuckDB '$DUCKDB_VERSION' · CedarDB '$CEDARDB_VERSION' · MonetDB '$MONETDB_VERSION; \
echo 'Smoke  (SF1, DuckDB, ~15 min):  ./reproduce_EAB.sh --quick E1'; \
echo 'Default scale SF10 (all eng.):  ./reproduce_EAB.sh all'; \
echo 'Paper scale  SF100 (multi-day): ./reproduce_EAB.sh --sf100 all'; \
echo 'Figures from existing results:  ./reproduce_EAB.sh figures'; \
echo '──────────────────────────────────────────────────────────────────────'; \
exec bash"]
