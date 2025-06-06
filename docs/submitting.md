# Submitting Data

1. Fork the repository and create a new branch for your dataset.
2. Add a folder under `public/` (or `private/` for restricted data) containing your data files.
3. Include a `metadata.json` and `provenance.json` file that match the schemas in the `schemas/` directory. The [`public/example_fh_vcf`](../public/example_fh_vcf) folder is a good starting template.
4. Validate your dataset locally with `tools/hbp-validate <dataset-dir>`.
5. Open a pull request describing your dataset. The review board will verify the metadata and provenance before merging.

For more details, consult [CONTRIBUTING.md](../CONTRIBUTING.md).
