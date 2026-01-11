#!/usr/bin/env bash
set -euo pipefail

GRAMMAR_DIR="x8/ql/grammar"
OUT_DIR="x8/ql/generated"

mkdir -p "$OUT_DIR"

# Run ANTLR from inside the grammar directory
pushd "$GRAMMAR_DIR" >/dev/null

antlr -Dlanguage=Python3 \
  -o ../generated \
  X8QLLexer.g4 \
  X8QLParser.g4

popd >/dev/null

# Now we're back at project root; prepend flake8/mypy ignores
for f in X8QLLexer.py X8QLParser.py X8QLParserListener.py; do
  src="$OUT_DIR/$f"
  if [[ -f "$src" ]]; then
    tmp="$src.tmp"
    {
      printf "# flake8: noqa\n"
      printf "# type: ignore\n"
      cat "$src"
    } > "$tmp"
    mv "$tmp" "$src"
  fi
done