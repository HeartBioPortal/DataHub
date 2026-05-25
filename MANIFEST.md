# Repository Manifest

This manifest describes release-relevant repository areas for the HBP 3.0 NAR manuscript archive.

## Code

- `src/datahub/`: reusable Python package for source manifests, adapters, validation, publication, secondary analyses, and serving helpers.
- `scripts/`: command-line entrypoints for ingestion, association publication, structural-variant ingestion, secondary analyses, QA reporting, and checksum generation.
- `tests/`: focused tests for config validation, manifests, association publication, serving builders, and secondary-analysis helpers.

## Configuration

- `config/sources/`: source-family metadata used by DataHub ingestion and provenance docs.
- `config/profiles/`: source profile definitions for ingestion layers.
- `config/prep_profiles/`: raw-to-prepared profiles for legacy association inputs.
- `config/export_manifests/`: rules for preserving, promoting, deriving, and publishing association fields.
- `config/output_contracts/`: output contracts for serving and legacy artifacts.
- `config/schemas/`: JSON Schemas for DataHub configuration surfaces.

## Documentation and release metadata

- `README.md`: project overview, setup, HBP 3.0 role, and release checklist.
- `docs/`: full DataHub documentation site.
- `DATA_SOURCES.tsv` and `DATA_SOURCES.md`: source inventory and human-readable source summary.
- `ARTIFACT_MANIFEST.tsv`: HBP 3.0 artifact-family inventory.
- `BUILD_METADATA.json`: machine-readable release metadata template.
- `LICENSES.md`: software, output, third-party, and restricted-data license summary.
- `PROVENANCE_SCHEMA.md`: provenance fields expected in HBP 3.0 DataHub outputs.
- `docs/schemas/`: documentation-only schemas for release-facing HBP layers.
- `CITATION.cff` and `.zenodo.json`: citation and archive metadata.
- `RELEASE_NOTES.md`: HBP 3.0 manuscript release notes.

## Data and generated artifacts

- `raw_data/`: small source files or checked-in source seeds only. Controlled, restricted, or license-uncertain bulk source data stay outside this directory.
- `analyzed_data/`: curated or generated artifacts that are small enough and permitted for repository distribution.
- `secondary_analyses/`: generated secondary-analysis outputs may be very large and are not assumed to be release-ready unless explicitly documented and redistributable.
- `datamart/`: serving DuckDB outputs are production artifacts and are archived only when size and redistribution rights are confirmed.

## Generated versus committed

Committed release-support files are metadata, docs, schemas, configs, small seed data, and scripts. Large generated artifacts, production DuckDB files, controlled individual-level data, raw DrugBank full data, and license-uncertain third-party data are generated or staged outside the repository unless release approval is explicit.
