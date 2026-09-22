#!/usr/bin/env python3
"""Check the two CDF exports against each other and against the accepted figure.

Four questions, each answered from the artefact rather than from the diff:

  1. Did the accepted full-width figure move?  Its page is compared by a **path-operand digest**
     of the content stream -- every coordinate of every drawn segment, every stroke colour and
     width -- taken from the delivered PDF and from a fresh render. matplotlib PDFs are never
     byte-reproducible (creation date, random font-subset tag), so a file hash cannot answer this;
     the digest can, and it excludes text placement so that a relabelled legend does not mask a
     moved curve.
  2. Do the two exports plot the same numbers?  Answered inside the renderer, which compares the
     arrays it handed to matplotlib and refuses to finish if they differ. This script re-runs it
     and requires that confirmation line.
  3. Does either page carry a title or a footer?  Answered by rebuilding the figure in process and
     enumerating its text artists. PDF text cannot answer it: with `pdf.fonttype 42` the strings
     are glyph indices, so searching the page for a title finds nothing whether or not one is
     there.
  4. What is the placed height at the 3.337 in manuscript column, against the two figures the
     author named as the visual reference?

Usage:  python3 verify_cdf_exports.py
"""
from __future__ import annotations

import hashlib
import importlib.util
import pathlib
import re
import shutil
import subprocess
import sys
import tempfile
import zlib

HERE = pathlib.Path(__file__).resolve().parent
COLUMN_IN = 3.337
# Placed heights of the two engine figures this one sits beside, measured from their PDFs
# when they were accepted. Quoted for comparison only; nothing here reads those files.
REFERENCES = {"fig9b_cdf_tpcds_vs_prodds": 1.359, "fig11_join_exec_planning": 1.404}
NUM = r"-?\d+(?:\.\d+)?"


def content(pdf: pathlib.Path) -> str:
    out = []
    for m in re.finditer(rb"stream\r?\n(.*?)endstream", pdf.read_bytes(), re.S):
        try:
            out.append(zlib.decompress(m.group(1)).decode("latin-1"))
        except zlib.error:
            pass
    return "\n".join(out)


def paths_digest(pdf: pathlib.Path) -> str:
    """Geometry and ink, without text placement."""
    nums = re.findall(rf"{NUM}(?= (?:m|l|c|re|RG|rg|w|d|G|g)\b)", content(pdf))
    return hashlib.sha256(" ".join(nums).encode()).hexdigest()[:16]


def page_in(pdf: pathlib.Path) -> tuple[float, float]:
    g = re.search(rb"/MediaBox\s*\[\s*([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)", pdf.read_bytes())
    return (round((float(g.group(3)) - float(g.group(1))) / 72, 4),
            round((float(g.group(4)) - float(g.group(2))) / 72, 4))


def load_renderer():
    spec = importlib.util.spec_from_file_location("cdf", HERE / "make_cdf_figure_final.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def main() -> int:
    problems: list[str] = []
    mod = load_renderer()

    # ── 1/2. re-render both exports, keeping the delivered results/ untouched ────────────
    with tempfile.TemporaryDirectory() as td:
        tmp = pathlib.Path(td)
        results = tmp / "results/final"
        results.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(HERE / "results/final", results)
        p = subprocess.run([sys.executable, str(HERE / "make_cdf_figure_final.py"),
                            "--results", str(results), "--out-dir", str(tmp / "out")],
                           capture_output=True, text=True, cwd=HERE)
        if p.returncode != 0:
            print(p.stdout + p.stderr)
            return 1
        confirm = [l for l in p.stdout.splitlines() if "identical coordinates" in l]
        if not confirm:
            problems.append("the renderer did not confirm identical coordinates across exports")
        else:
            print(f"  renderer: {confirm[0].split('] ', 1)[1]}")

        fresh_final = tmp / "out/cdf_crossbench_final.pdf"
        fresh_paper = tmp / "out/cdf_crossbench_paper.pdf"
        delivered = HERE / "figures/cdf_crossbench_final.pdf"

        a, b = paths_digest(delivered), paths_digest(fresh_final)
        print(f"\n  accepted full-width figure, path digest")
        print(f"    delivered {a}\n    re-render {b}    "
              f"{'unchanged' if a == b else '*** MOVED ***'}")
        if a != b:
            problems.append("the accepted full-width figure changed")

        # the tables the same run writes must also be unchanged
        for name in ("cdf_crossbench_summary_final.csv", "cdf_crossbench_tail_final.csv",
                     "cdf_crossbench_summary_final.md"):
            got = (tmp / "out" / name).read_bytes()
            want = (HERE / "figures" / name).read_bytes()
            if got != want:
                problems.append(f"{name} changed")
        print(f"    summary tables: "
              f"{'all three byte-identical' if not problems else 'see problems'}")

        sizes = {"cdf_crossbench_final": page_in(fresh_final),
                 "cdf_crossbench_paper": page_in(fresh_paper)}

    # ── 3. titles and footers, from the artists rather than from the page ───────────────
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    attempts, _, manifest, n_passes, _ = mod.load(HERE / "results/final")
    cls = mod.classify(attempts, n_passes, manifest)
    pr = cls["prodds"]
    cls["prodds_templates"] = dict(
        complete={q: v for q, v in pr["complete"].items()
                  if not q.startswith(mod.MICRO_PREFIXES)},
        partial={q: v for q, v in pr["partial"].items() if not q.startswith(mod.MICRO_PREFIXES)},
        failed={q: v for q, v in pr["failed"].items() if not q.startswith(mod.MICRO_PREFIXES)})
    suites = [s for s in mod.ORDER if s in cls and cls[s]["complete"]]
    print("\n  text artists per export (a title or footer would appear here)")
    for stem, spec in mod.FIGURE_VARIANTS.items():
        fig, ax = plt.subplots(figsize=spec["figsize"])
        mod.draw_cdf(ax, suites, cls, spec["short_labels"])
        extra = [t.get_text() for t in fig.texts if t.get_text().strip()]
        if ax.get_title().strip():
            problems.append(f"{stem} carries a plot title: {ax.get_title()!r}")
        if extra:
            problems.append(f"{stem} carries figure text: {extra}")
        print(f"    {stem:22s} title {ax.get_title()!r}, figure texts {len(extra)}")
        if spec["short_labels"]:
            labels = [h.get_label() for h in ax.get_lines()]
        plt.close(fig)

    # ── 4. placed height ───────────────────────────────────────────────────────────────
    print(f"\n  placed at the {COLUMN_IN} in manuscript column")
    for stem, (w, h) in sizes.items():
        print(f"    {stem:22s} {w:6.3f} x {h:6.3f} in  ->  {COLUMN_IN} x {h * COLUMN_IN / w:.3f} in")
    for name, placed in REFERENCES.items():
        print(f"    {name:22s} reference                ->  {COLUMN_IN} x {placed:.3f} in")
    pw, ph = sizes["cdf_crossbench_paper"]
    placed = ph * COLUMN_IN / pw
    if not 1.30 <= placed <= 1.45:
        problems.append(f"the paper export places at {placed:.3f} in, outside the reference band")

    print("\n  paper legend, as drawn:")
    for lbl in labels:
        print(f"    {lbl}")

    if problems:
        print(f"\n{len(problems)} PROBLEM(S):")
        for q in problems:
            print(f"  - {q}")
        return 1
    print("\nclean")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
