# HeartBioPortal DataHub

HeartBioPortal DataHub is the data integration and publishing layer behind HeartBioPortal. It standardizes heterogeneous cardiovascular genomics and omics datasets, preserves provenance, performs raw-level integration, and emits both legacy-compatible analyzed outputs and newer serving artifacts.

## Documentation

Comprehensive documentation lives under `docs/` and can also be served as a documentation website.

- Start with: `docs/index.md`
- Script standards: `SCRIPT_MANIFESTO.md`
- Architecture guide: `docs/architecture/`
- Pipeline guides: `docs/pipelines/`
- Extension/contributor guides: `docs/extending/` and `docs/contributing.md`

Local docs preview:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[docs]"
mkdocs serve
```

Production docs site:

- `mkdocs.yml` defines the site.
- `.github/workflows/docs.yml` builds and deploys to GitHub Pages.
- Enable GitHub Pages in the repository and choose `GitHub Actions` as the source.

## Quick Start

```bash
git clone https://github.com/HeartBioPortal/DataHub.git
cd DataHub
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -e ".[test]"
python -m pytest
```

For script-only environments, `requirements.txt` is still available. The
package metadata in `pyproject.toml` is the canonical development install path.

## Runtime Dependencies

```bash
pip install -r requirements.txt
```

Current runtime requirements:

- `jsonschema`
- `jsonschema2md`
- `PyGithub`
- `pandas`
- `duckdb`
- `requests`

Test dependencies live under the `test` optional extra in `pyproject.toml`.

## Primary Entry Points

- `scripts/prepare_association_raw.py`
- `scripts/build_legacy_association.py`
- `scripts/run_ingestion.py`
- `scripts/run_structural_variant_ingestion.py`
- `scripts/dataset_specific_scripts/mvp/run_mvp_pipeline.py`
- `scripts/dataset_specific_scripts/unified/run_unified_pipeline.py`
- `scripts/dataset_specific_scripts/unified/run_secondary_analyses.py`
- `scripts/dataset_specific_scripts/unified/run_gene_profile_pipeline.py`
- `scripts/dataset_specific_scripts/unified/build_dbsnp_frequency_index.py`
- `scripts/dataset_specific_scripts/unified/build_dbsnp_frequency_parquet.py`
- `scripts/dataset_specific_scripts/unified/canonicalize_variant_viewer_artifacts.py`
- `scripts/report_artifact_qa.py`

Editable installs also expose console commands such as:

- `datahub-run-ingestion`
- `datahub-run-unified-pipeline`
- `datahub-ingest-mvp-duckdb-fast`
- `datahub-publish-unified-from-duckdb`
- `datahub-build-serving-duckdb`
- `datahub-run-secondary-analyses`
- `datahub-run-gene-profile-pipeline`
- `datahub-build-dbsnp-frequency-index`
- `datahub-build-dbsnp-frequency-parquet`
- `datahub-report-artifact-qa`

## Main Repository Areas

- `src/datahub/`: reusable pipeline, adapter, config, validation, storage, and publisher code
- `config/`: profiles, manifests, runtime configs, phenotype hierarchy, output contracts, and export manifests
- `raw_data/`: small checked-in standalone source files organized by source ID
- `analyzed_data/`: curated analyzed artifacts and merge/metadata seed payloads organized by source ID
- `scripts/`: operational entrypoints for preparation, ingest, publish, and orchestration
- `tests/`: focused coverage for adapters, manifests, publishers, runners, and serving builders
- `docs/`: contributor-facing documentation published at `https://heartbioportal.github.io/DataHub/`

Config JSON files are validated by JSON Schemas in `config/schemas/`.

## Design Principles

- Keep biological/analytical logic in DataHub, not in downstream application layers.
- Preserve provenance as early as possible and avoid throwing detail away during normalization.
- Make source-specific behavior explicit through config and adapters, not hidden conditionals.
- Keep published outputs stable for consumers while allowing additive metadata evolution.
- Separate concerns between raw preparation, canonical ingestion, analyzed publication, and serving artifacts.

## Legacy Compatibility

DataHub still supports legacy HeartBioPortal-compatible analyzed payloads, but the codebase now also maintains a newer serving-artifact path based on DuckDB. The legacy path exists for compatibility; the unified DuckDB-first path is the strategic direction.

## How this repository supports HBP 3.0

DataHub is the canonical HBP 3.0 data-owner repository. It prepares source manifests, normalizes source-specific fields, publishes association and secondary-analysis artifacts, and builds serving datamarts consumed by the HeartBioPortal backend and frontend.

Related HBP 3.0 repositories:

- HeartBioPortal organization: https://github.com/HeartBioPortal
- Live site: https://heartbioportal.org/
- HCG guideline extraction resource: https://github.com/HeartBioPortal/HCG
- HCG-KG guideline knowledge graph resource: https://github.com/HeartBioPortal/HCG-KG

## Manuscript release

This repository supports the HeartBioPortal 3.0 NAR Database Issue manuscript release (`v3.0.0-nar`). The release-support files in this repository describe source provenance, licensing constraints, generated artifacts, reproducibility expectations, and files that should or should not be included in a public archive.

Release metadata and manifests:

- `CITATION.cff`
- `.zenodo.json`
- `RELEASE_NOTES.md`
- `MANIFEST.md`
- `DATA_SOURCES.tsv`
- `DATA_SOURCES.md`
- `ARTIFACT_MANIFEST.tsv`
- `BUILD_METADATA.json`
- `LICENSES.md`
- `PROVENANCE_SCHEMA.md`
- `docs/schemas/*.md`
- `scripts/generate_checksums.sh`

## Preparing the HBP 3.0 NAR release

Use this checklist before creating a GitHub release or Zenodo archive:

1. Confirm the release branch and commit:

```bash
git status --short --branch
git rev-parse HEAD
```

2. Validate the code and docs in the target environment:

```bash
python -m pytest
mkdocs build --strict
```

3. Regenerate or review manifest files:

- Review `DATA_SOURCES.tsv` and `DATA_SOURCES.md` against the current source configs and pipeline inputs.
- Review `ARTIFACT_MANIFEST.tsv` against production QA reports, generated artifact directories, and serving DB tables.
- Update `BUILD_METADATA.json` with the final release commit, build date, schema version, and verified production metrics.

4. Verify counts from production artifacts where available:

```bash
datahub-report-artifact-qa --help
```

Counts that cannot be verified from committed local artifacts should remain `TBD; verify from production QA`.

5. Generate release checksums for release-relevant static files:

```bash
scripts/generate_checksums.sh
```

6. Include in Zenodo:

- repository source code
- config schemas and manifests
- documentation
- small examples or seed metadata that are redistributable
- generated release manifests and checksum files

7. Do not include in Zenodo unless redistribution has been confirmed:

- controlled individual-level human data
- API keys, credentials, tokens, or secrets
- raw DrugBank full database files
- large source datasets with unclear redistribution rights
- controlled-access or license-restricted third-party source files
- massive generated artifacts unless they are intended, permitted, and documented for the release package

## Security and privacy

No controlled individual-level human data should be committed to this repository. Do not commit API keys, credentials, protected data, tokens, or restricted source data. Source-specific licensing controls redistribution of third-party data; if redistribution rights are uncertain, document the source in `DATA_SOURCES.tsv` or `LICENSES.md` rather than committing the data.

## License

See `LICENSE`.
