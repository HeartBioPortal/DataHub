# Repository Tour

![Generated visual for Repository Tour](assets/visuals/repository-tour.svg){ .doc-visual }

This page is the practical map of the repository.

## Top-level structure

```text
DataHub/
  config/
  data/
  docs/
  raw_data/
  analyzed_data/
  secondary_analyses/
  datamart/
  scripts/
  src/datahub/
  tests/
  pyproject.toml
  README.md
  requirements.txt
  requirements-docs.txt
  mkdocs.yml
```

## `config/`

This directory is the declarative heart of the repository.

### Main subdirectories

- `config/prep_profiles/`: how raw legacy columns are mapped into prepared intermediate columns
- `config/profiles/`: dataset-type validation contracts
- `config/sources/`: source manifests and metadata for onboarded or catalogued sources
- `config/runtime_profiles/`: execution profiles for laptop/AWS/HPC orchestration
- `config/export_manifests/`: analyzed export rules for preservation and derivation between unified data and published artifacts
- `config/output_contracts/`: stable consumer-facing output shapes
- `config/secondary_analyses/`: manifests for per-gene secondary artifacts and expression contracts
- `config/schemas/`: JSON Schemas used to validate config families before runs
- `config/phenotype_tree.json`: canonical phenotype hierarchy used for grouping and path resolution

## `src/datahub/`

This is the reusable library layer.

### Packages and modules

- `models.py`: canonical in-memory record model
- `config.py`: field policies and contract primitives
- `profiles.py`: loader for dataset validation profiles
- `prep/`: raw preparation profiles and preparers
- `adapters/`: source-specific canonical record readers
- `sources/`: source manifests and source registry
- `registry.py`: adapter plugin registry
- `quality.py`: validation against dataset contracts
- `enrichment.py`: enrichment and source-priority logic hooks
- `storage/`: canonical storage backends such as DuckDB + Parquet
- `publishers/`: analyzed artifact emitters
- `axis_normalization.py`: normalization for categorical axes used in charts
- `phenotype_paths.py`: phenotype hierarchy resolution
- `ancestry.py`: ancestry normalization and provenance helpers
- `export_manifest.py`, `export_helpers.py`: manifest-driven preservation/derivation layer between unified data and analyzed outputs
- `pipeline.py`: high-level orchestration for adapter -> validate -> storage -> publish
- `config_schemas.py`: JSON Schema validation helpers for repository config
- `artifact_qa.py`: release QA summaries for source catalog, published outputs, and DuckDB artifacts
- `checkpoints.py`: durable checkpoint models and atomic checkpoint writes
- `expression/`: curated expression v3 discovery, curation, row models, and output builders
- `secondary_analyses/`: per-gene artifact generation and incremental serving updates
- `protein_context.py`: protein-coordinate isoform and feature normalization
- `working_duckdb.py`: lifecycle tables for raw-release inventory, schema drift, and analysis-ready association data
- `unified/`: shared runtime helpers used by unified DuckDB operational scripts

## `scripts/`

These are operational entrypoints.

### General scripts

- `build_legacy_association.py`
- `prepare_association_raw.py`
- `run_ingestion.py`
- `run_structural_variant_ingestion.py`
- `enrich_structural_variant_exons.py`
- `report_artifact_qa.py`

### Dataset-specific operational flows

- `scripts/dataset_specific_scripts/mvp/`
- `scripts/dataset_specific_scripts/unified/`
- `scripts/dataset_specific_scripts/expression/`
- `scripts/slurm/`

These are where practical large-scale workflows live today.

## Data and artifact directories

The repository defines stable roles for data directories, but production-scale
contents are normally generated or copied into place rather than committed:

- `raw_data/`: source-native downloads and licensed/local inputs
- `data/raw/`, `data/interim/`, `data/processed/`: staged expression and other module-specific working data
- `analyzed_data/`: published or legacy-compatible artifacts
- `secondary_analyses/`: versioned companion evidence and per-gene payloads
- `datamart/`: DuckDB serving and analytical databases

Do not infer release inclusion from local file presence. Release manifests,
build metadata, checksums, and QA reports define what a specific build contains.

## `tests/`

Tests are intentionally focused on behavior, not just coverage. They include:

- adapter correctness
- preparation correctness
- source manifest loading
- publisher behavior
- serving builder behavior
- unified pipeline behavior
- export manifest behavior
- checkpoint/resume behavior
- expression, protein-context, gene-profile, SGA, dbSNP-frequency, and structural-variant behavior

## How to navigate the repo efficiently

If you are making a change, identify the layer first.

- New raw-column mapping problem: `config/prep_profiles/` or `src/datahub/prep/`
- New source or source-specific parse logic: `config/sources/` plus `src/datahub/adapters/`
- New validation rule: `config/profiles/` or `src/datahub/config.py`
- New analyzed payload field: `config/export_manifests/` plus publisher/build logic
- New secondary or companion evidence contract: `config/secondary_analyses/`, the owning `src/datahub/` package, and `docs/schemas/`
- Runtime environment issue: `config/runtime_profiles/` or orchestration scripts
- Config validation issue: `config/schemas/` plus `src/datahub/config_schemas.py`
- Release verification issue: `src/datahub/artifact_qa.py` or `scripts/report_artifact_qa.py`
