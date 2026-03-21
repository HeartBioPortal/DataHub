# dbVar Raw Structural Variant Inputs

This directory holds raw dbVar structural-variant study exports that are kept in the repository because they are small enough to version and useful for reproducible ingestion runs.

Dataset type:

- upstream dbVar structural-variant study tables
- raw study-level CSV content, optionally stored as `.csv.zip` for Git-friendly compression

Naming convention:

- `dbvar_structural_variants_<study_id>.csv.zip`

Current tracked artifact:

- `dbvar_structural_variants_nstd229.csv.zip`

Notes:

- keep the file contents source-native; do not rewrite columns here
- if a new study is added, follow the same naming pattern
