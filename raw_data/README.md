# Raw Data Staging

This directory is the checked-in home for small, standalone raw inputs that are useful to keep with the repository.

Use it for:

- source files needed for reproducible local development
- small manually downloaded inputs such as individual dbVar study CSVs
- raw artifacts that are useful for examples, debugging, or lightweight pipeline runs

Do not use it for:

- large bulk downloads
- private or restricted data
- generated outputs

Recommended layout:

- `raw_data/<source_id>/...`

Examples:

- `raw_data/dbvar/all_variants_for_nstd229.csv`
- `raw_data/clinvar/...`

Guidelines:

- keep filenames close to the upstream source name when possible
- prefer per-source subdirectories over putting files directly in `raw_data/`
- keep tracked files reasonably small and redistributable
- point scripts at these files explicitly; do not hardcode this directory as the only input location
