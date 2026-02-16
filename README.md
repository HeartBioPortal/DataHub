# HeartBioPortal DataHub

HeartBioPortal DataHub is a version-controlled collection of cardiovascular omics datasets. Each dataset includes standardised metadata and provenance information so that analyses can be reproduced and referenced.

## Quick Start

```bash
git clone <repo-url>
cd DataHub
make validate
# or using docker
docker compose up validation
```

## Git Large File Storage

This repository uses [Git LFS](https://git-lfs.github.com/) for storing large binary datasets. Install Git LFS before cloning:

```bash
git lfs install
```

## Dataset Layout

Datasets are organised under `public/` for open data or `private/` for embargoed submissions. A typical dataset directory contains:

```
<dataset>/
  metadata.json      # descriptive metadata
  provenance.json    # processing provenance
  data files...
```

The JSON schemas that describe these files live under `schemas/` and are also rendered in the documentation.

## Validation

Use the helper script to check a dataset before opening a pull request:

```bash
tools/hbp-validate public/example_fh_vcf
```

or run all tests with `make validate`.

## DataHub Pipeline (Legacy Compatibility)

DataHub now includes a modular OOP pipeline under `src/datahub/` with:

- adapters (source-specific ingestion),
- contracts and quality policies (required fields + missing-value handling),
- enrichment hooks with configurable source priority,
- storage backends (`DuckDB + Parquet`),
- publishers that emit legacy HeartBioPortal-compatible association JSON.

To build association outputs from the legacy aggregated CSV while keeping the
current frontend payload shape:

```bash
scripts/build_legacy_association.py \
  --csv-path ../DataManager/raw_data/final_aggregated_results.csv \
  --output-root ../DataManager/analyzed_data \
  --duckdb-path ./artifacts/canonical.duckdb \
  --parquet-path ./artifacts/canonical.parquet \
  --publish-redis
```

Field requirements are configurable via `--required-fields` and phenotype
routing can be overridden via `--phenotype-map-json`. Redis loading uses the
existing DataManager exporter so backend cache behavior remains unchanged.

## Dataset Profiles

Dataset-type validation profiles are first-class JSON configs in
`config/profiles/`:

- `association.json`
- `expression.json`
- `single_cell.json`

These profiles can be loaded through `DatasetProfileLoader` and converted into
runtime `DatasetContract` objects for pipeline validation.

## Ingestion And Source Registry

DataHub includes a pluggable adapter registry so community submissions can add
new ingestion adapters without modifying core orchestration code.

Built-in adapter IDs:

- `legacy_association_csv`
- `gwas_association`
- `ensembl_association`
- `clinvar_association`

DataHub also includes a source-management layer under `src/datahub/sources/`
that maps source manifests in `config/sources/` to adapter instances.

Built-in source IDs:

- `gwas_catalog`
- `ensembl_variation`
- `clinvar`

Additional source manifests are included for cataloging major databases across
these categories:

- `cvd_focused_portals`
- `gwas_statistical_genetics`
- `population_reference_variation`
- `clinical_variant_interpretation`
- `bulk_transcriptomics_qtl`
- `single_cell_spatial`
- `epigenomics_regulatory`
- `proteomics`
- `metabolomics_lipidomics`
- `pathways_interactions_networks`
- `drug_target_translational`
- `ontologies_standards`

Most of these are marked `catalog_only` until a dedicated adapter is wired.

Run configurable ingestion with:

```bash
scripts/run_ingestion.py --config path/to/ingestion.json
```

The JSON config can define profile, adapter list, source list, optional plugin
adapters and source connectors, storage, enrichment source priority, and
publishers.

Example source-driven config:

```json
{
  "profile": "association",
  "sources": [
    {
      "id": "gwas_catalog",
      "params": {
        "input_paths": "/path/to/gwas_results.csv",
        "dataset_id": "hbp_gwas_snapshot_2026_02"
      }
    }
  ],
  "publishers": [
    {
      "name": "legacy_association",
      "params": {
        "output_root": "/path/to/analyzed_data"
      }
    }
  ]
}
```

## Contributing

We welcome new datasets and improvements. See [CONTRIBUTING.md](CONTRIBUTING.md) for a walkthrough of the submission process and consult the files in the `docs/` directory for more details.
