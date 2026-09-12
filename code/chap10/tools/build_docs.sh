#!/usr/bin/env bash
#
# Regenerates data_design_patterns.html and data_design_patterns.pdf from
# data_design_patterns.md (the source of truth) using Pandoc.
#
# Requirements:
#   - pandoc            https://pandoc.org
#   - xelatex on PATH   for the PDF target (e.g. via MacTeX / TeX Live)
#
# Usage (run from anywhere; paths are resolved relative to this script):
#   tools/build_docs.sh          # regenerate both .html and .pdf
#   tools/build_docs.sh html     # regenerate only the .html
#   tools/build_docs.sh pdf      # regenerate only the .pdf
#
# See ../README.md for the underlying pandoc commands, spelled out for
# anyone who wants to run/tweak them by hand instead.

set -euo pipefail

CHAP10_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TOOLS_DIR="$CHAP10_DIR/tools"
SRC_MD="$CHAP10_DIR/data_design_patterns.md"
TARGET="${1:-all}"

build_html() {
  pandoc "$SRC_MD" \
    --to=html5 \
    --standalone \
    --highlight-style=pygments \
    --lua-filter="$TOOLS_DIR/github_ids.lua" \
    --metadata pagetitle="Data Design Patterns" \
    -H "$TOOLS_DIR/pandoc_header.html" \
    -o "$CHAP10_DIR/data_design_patterns.html"
  echo "wrote $CHAP10_DIR/data_design_patterns.html"
}

build_pdf() {
  # The chapter's title/byline (the first 8 lines of the .md) is written
  # as raw <h1>/<h3>/<p align="center"> HTML so it displays nicely on
  # GitHub and in the .html build above. Pandoc's LaTeX writer drops raw
  # HTML blocks, so for the PDF we strip those 8 lines and re-supply the
  # same text as a YAML title/author/date block instead — Pandoc turns
  # that into a proper LaTeX title page ahead of the table of contents.
  local tmp title author bio
  tmp="$(mktemp -t data_design_patterns_pdf_src.XXXXXX).md"
  trap 'rm -f "$tmp"' RETURN

  title="$(sed -n '1p' "$SRC_MD" | sed -E 's/<[^>]+>//g')"
  author="$(sed -n '2p' "$SRC_MD" | sed -E 's/<[^>]+>//g')"
  # lines 4-7: one centered <p align="center">...</p> byline per line
  bio="$(sed -n '4,7p' "$SRC_MD" | sed -E 's/<[^>]+>//g; s/^/  /; s/$/\\/')"

  {
    printf -- '---\n'
    printf 'title: "%s"\n' "$title"
    printf 'author: "%s"\n' "$author"
    printf 'date: |\n'
    printf '%s\n' "$bio"
    printf -- '---\n\n'
    tail -n +9 "$SRC_MD"
  } > "$tmp"

  pandoc "$tmp" \
    --to=pdf \
    --pdf-engine=xelatex \
    --highlight-style=pygments \
    --lua-filter="$TOOLS_DIR/github_ids.lua" \
    -V geometry:margin=1in \
    -V colorlinks=true \
    -V linkcolor=blue \
    -V urlcolor=blue \
    --toc --toc-depth=2 \
    -o "$CHAP10_DIR/data_design_patterns.pdf"
  echo "wrote $CHAP10_DIR/data_design_patterns.pdf"
}

case "$TARGET" in
  html) build_html ;;
  pdf)  build_pdf ;;
  all)  build_html; build_pdf ;;
  *)    echo "usage: $0 [html|pdf|all]" >&2; exit 1 ;;
esac
