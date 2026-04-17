# HeartBioPortal DataHub

HeartBioPortal DataHub is the data integration and publishing layer behind HeartBioPortal. It standardizes heterogeneous cardiovascular genomics and omics datasets, preserves provenance, performs raw-level integration, and emits both legacy-compatible analyzed outputs and newer serving artifacts.

## Documentation

Comprehensive documentation lives under `docs/` and can also be served as a documentation website.

- Start with: `docs/index.md`
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
- `scripts/report_artifact_qa.py`

Editable installs also expose console commands such as:

- `datahub-run-ingestion`
- `datahub-run-unified-pipeline`
- `datahub-ingest-mvp-duckdb-fast`
- `datahub-publish-unified-from-duckdb`
- `datahub-build-serving-duckdb`
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

## License

See `LICENSE`.
