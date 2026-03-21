# Raw Data Staging

This directory is the checked-in home for small, standalone raw inputs that are useful to keep with the repository.

Dataset scope:

- raw source datasets as received from upstream providers
- typically source-native CSV, TSV, JSON, or compressed equivalents
- no analysis-time reshaping beyond safe archival compression

Use it for:

- source files needed for reproducible local development
- small manually downloaded inputs such as individual dbVar study CSVs
- raw artifacts that are useful for examples, debugging, or lightweight pipeline runs
- compressed source files when the uncompressed upstream artifact is too large to track safely in Git

Do not use it for:

- large bulk downloads
- private or restricted data
- generated outputs

Recommended layout:

- `raw_data/<source_id>/...`

Examples:

- `raw_data/dbvar/dbvar_structural_variants_nstd229.csv.zip`
- `raw_data/clinvar/...`

Guidelines:

- prefer per-source subdirectories over putting files directly in `raw_data/`
- keep tracked files reasonably small and redistributable
- prefer compressed archives for large plain-text raw files that would otherwise exceed Git hosting limits
- name files so they identify the source, dataset kind, and specific study/cohort when applicable
- prefer names like `<source>_<dataset_kind>_<study_or_release>.<ext>`
- point scripts at these files explicitly; do not hardcode this directory as the only input location
