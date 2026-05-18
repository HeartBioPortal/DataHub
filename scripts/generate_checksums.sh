#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

output="CHECKSUMS.txt"
tmp="${output}.tmp"

files=(
  "README.md"
  "LICENSE"
  "CITATION.cff"
  ".zenodo.json"
  "RELEASE_NOTES.md"
  "MANIFEST.md"
  "DATA_SOURCES.tsv"
  "DATA_SOURCES.md"
  "ARTIFACT_MANIFEST.tsv"
  "BUILD_METADATA.json"
  "LICENSES.md"
  "PROVENANCE_SCHEMA.md"
)

find docs/schemas -type f -name '*.md' | sort >> "${tmp}.files"
for file in "${files[@]}"; do
  if [[ -f "$file" ]]; then
    printf '%s\n' "$file" >> "${tmp}.files"
  fi
done

sort -u "${tmp}.files" | while IFS= read -r file; do
  shasum -a 256 "$file"
done > "$tmp"

rm -f "${tmp}.files"
mv "$tmp" "$output"
echo "Wrote $output"
