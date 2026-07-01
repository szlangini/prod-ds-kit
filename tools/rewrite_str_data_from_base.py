#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

DEFAULT_HOTPATH_NULL_OVERRIDES = (
    REPO
    / "experiments"
    / "artifacts"
    / "E10b_adjusted"
    / "null_overrides_target30_nullable_only.json"
).resolve()
DEFAULT_HOTPATH_MCV_OVERRIDES = (
    REPO
    / "experiments"
    / "artifacts"
    / "E10b_adjusted"
    / "mcv_overrides_target30_nonkey_only.json"
).resolve()

from workload.dsdgen import stringify


def _positive_int(raw: str) -> int:
    try:
        value = int(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid int value: {raw}") from exc
    if value < 1:
        raise argparse.ArgumentTypeError("value must be >= 1")
    return value


def _load_overrides(path: str | None) -> dict | None:
    if not path:
        return None
    src = Path(path).expanduser().resolve()
    if not src.exists():
        raise SystemExit(f"Overrides file not found: {src}")
    if src.suffix.lower() in {".json"}:
        try:
            payload = json.loads(src.read_text(encoding="utf-8"))
        except Exception as exc:
            raise SystemExit(f"Invalid JSON overrides file: {src}: {exc}") from exc
    else:
        try:
            import yaml
        except Exception as exc:
            raise SystemExit(
                "PyYAML is required for YAML overrides files; install it or provide JSON."
            ) from exc
        try:
            payload = yaml.safe_load(src.read_text(encoding="utf-8"))
        except Exception as exc:
            raise SystemExit(f"Invalid YAML overrides file: {src}: {exc}") from exc
    if payload is None:
        return None
    if not isinstance(payload, dict):
        raise SystemExit(f"Overrides file must parse to a mapping: {src}")
    return payload


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Apply STR-level rewrite (stringification/null/mcv) in-place on pre-generated SF data files."
    )
    p.add_argument("--output-dir", required=True, help="Directory containing *.dat/*.tbl files.")
    p.add_argument(
        "--stringification-level",
        type=_positive_int,
        required=True,
        help="Stringification level (1..10 by default; >10 only with --enable-str-plus).",
    )
    p.add_argument("--stringification-preset", default=None, help="Optional preset name.")
    p.add_argument("--disable-null-skew", action="store_true")
    p.add_argument("--disable-mcv-skew", action="store_true")
    p.add_argument(
        "--inject-hotpath",
        action="store_true",
        help=(
            "Enable targeted hotpath injection overrides. "
            "Without this flag, rewrite uses original/default injection behavior."
        ),
    )
    p.add_argument("--null-marker", default=None)
    p.add_argument("--null-seed", type=int, default=None)
    p.add_argument("--null-profile", default=None)
    p.add_argument(
        "--exclude-hot-paths",
        action="store_true",
        help="Use legacy conservative candidate pool (default includes hot-path columns).",
    )
    p.add_argument(
        "--min-ndv-for-injection",
        type=int,
        default=None,
        help="Minimum NDV required for NULL/MCV injection eligibility (default: 50).",
    )
    p.add_argument(
        "--ndv-reference-duckdb",
        default=None,
        help="Optional DuckDB path used for NDV guard COUNT(DISTINCT) checks.",
    )
    p.add_argument(
        "--ndv-cache-dir",
        default=None,
        help="Optional cache directory for NDV guard results.",
    )
    p.add_argument(
        "--scale-factor",
        type=int,
        default=None,
        help="Optional SF value for NDV cache keying and DB-path inference.",
    )
    p.add_argument(
        "--null-overrides-file",
        default=None,
        help=(
            "Optional JSON/YAML mapping merged into null skew config. "
            "Only valid together with --inject-hotpath."
        ),
    )
    p.add_argument("--mcv-seed", type=int, default=None)
    p.add_argument(
        "--mcv-profile",
        default=None,
        help="MCV profile name or tier alias (low/medium/high). Default: medium (mcv_fleet_default).",
    )
    p.add_argument(
        "--mcv-overrides-file",
        default=None,
        help=(
            "Optional JSON/YAML mapping merged into MCV skew config. "
            "Only valid together with --inject-hotpath."
        ),
    )
    p.add_argument("--max-workers", type=_positive_int, default=None)
    p.add_argument(
        "--backend",
        choices=("auto", "cpp", "python"),
        default="auto",
        help="Rewrite backend preference (default: auto).",
    )
    p.add_argument("--enable-str-plus", action="store_true", help="Enable optional STR>10 extension.")
    p.add_argument(
        "--str-plus-max-level",
        type=_positive_int,
        default=20,
        help="Maximum allowed level when STR+ is enabled (default: 20).",
    )
    p.add_argument(
        "--str-plus-pad-step",
        type=_positive_int,
        default=2,
        help="Suffix growth per level above STR10 (default: 2).",
    )
    p.add_argument("--str-plus-separator", default="~")
    p.add_argument("--str-plus-marker", default="X")
    p.add_argument("--strlen", type=int, default=0,
                   help="String LENGTH axis (0=natural; each step adds --str-plus-pad-step "
                        "chars to stringified values). Orthogonal to the STR coverage level.")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    out_dir = Path(args.output_dir).expanduser().resolve()
    if not out_dir.is_dir():
        raise SystemExit(f"Output dir not found: {out_dir}")
    if args.inject_hotpath:
        null_overrides = None
        mcv_overrides = None
        if (not args.disable_null_skew) or args.null_overrides_file:
            null_override_src = args.null_overrides_file or str(DEFAULT_HOTPATH_NULL_OVERRIDES)
            null_overrides = _load_overrides(null_override_src)
        if (not args.disable_mcv_skew) or args.mcv_overrides_file:
            mcv_override_src = args.mcv_overrides_file or str(DEFAULT_HOTPATH_MCV_OVERRIDES)
            mcv_overrides = _load_overrides(mcv_override_src)
    else:
        if args.null_overrides_file or args.mcv_overrides_file:
            raise SystemExit(
                "--null-overrides-file/--mcv-overrides-file require --inject-hotpath."
            )
        null_overrides = None
        mcv_overrides = None

    files, rows = stringify.rewrite_tbl_directory(
        output_dir=out_dir,
        max_workers=args.max_workers,
        backend=args.backend,
        enable_stringify=None,
        stringification_level=args.stringification_level,
        stringification_preset=args.stringification_preset,
        allow_extended_levels=bool(args.enable_str_plus),
        str_plus_enabled=bool(args.enable_str_plus),
        str_plus_max_level=int(args.str_plus_max_level),
        str_plus_pad_step=int(args.str_plus_pad_step),
        str_plus_separator=str(args.str_plus_separator),
        str_plus_marker=str(args.str_plus_marker),
        strlen=int(args.strlen),
        enable_nulls=not bool(args.disable_null_skew),
        null_seed=args.null_seed,
        null_marker=args.null_marker,
        null_profile=args.null_profile,
        include_hot_paths=not bool(args.exclude_hot_paths),
        min_ndv_for_injection=args.min_ndv_for_injection,
        ndv_reference_duckdb=args.ndv_reference_duckdb,
        ndv_cache_dir=args.ndv_cache_dir,
        scale_factor=args.scale_factor,
        null_overrides=null_overrides,
        enable_mcv=not bool(args.disable_mcv_skew),
        mcv_seed=args.mcv_seed,
        mcv_profile=args.mcv_profile,
        mcv_overrides=mcv_overrides,
    )
    print(
        f"[rewrite] level={args.stringification_level} output_dir={out_dir} files={files} rows={rows}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
