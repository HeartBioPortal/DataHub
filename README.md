# HeartBioPortal DataHub

HeartBioPortal DataHub is a version-controlled collection of cardiovascular omics datasets. Each dataset includes standardised metadata and provenance information so that analyses can be reproduced and referenced.

## Quick Start

```bash
git clone <repo-url>
cd DataHub
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
make validate
# or using docker
docker compose up validation
```

## Server Install (Recommended)

Use this flow on servers (AWS, HPC login node, on-prem VM):

```bash
git clone https://github.com/HeartBioPortal/DataHub.git
cd /DataHub
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

If you run `pip` from the parent directory instead of inside `/DataHub`, use:

```bash
pip install -r DataHub/requirements.txt
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
Use `--ancestry-precision` and default ancestry-point deduplication to reduce
JSON payload size while keeping the same response shape.

## Raw Data Preparation

DataHub includes a profile-driven preparation layer to clean legacy/raw source
columns into a stable schema before aggregation.

Built-in preparation profiles:

- `legacy_cvd_raw`
- `legacy_trait_raw`

Run preparation:

```bash
scripts/prepare_association_raw.py \
  --input-csv /path/to/raw_cvd.csv \
  --output-csv /path/to/prepared_cvd.csv \
  --profile legacy_cvd_raw
```

Prepared output columns are standardized and include:
`rsid,pval,gene,phenotype,functional_class,var_class,clinical_significance,most_severe_consequence,allele_string,protein_start,protein_end,ancestry_data`
plus provenance fields such as `study`, `pmid`, and `study_genome_build`.

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
- `mvp_association`

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

Built-in publisher IDs:

- `legacy_association`
- `legacy_redis`
- `phenotype_rollup`

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

## Dataset-Specific Scripts

Dataset-specific entrypoints are organized under:

- `scripts/dataset_specific_scripts/`

MVP integration scripts are available in:

- `scripts/dataset_specific_scripts/mvp/run_mvp_pipeline.py`
- `scripts/dataset_specific_scripts/mvp/export_mvp_prepared_raw.py`
- `scripts/dataset_specific_scripts/mvp/README.md`

Unified DuckDB-first scripts (legacy raw + MVP) are available in:

- `scripts/dataset_specific_scripts/unified/ingest_legacy_raw_duckdb.py`
- `scripts/dataset_specific_scripts/unified/publish_unified_from_duckdb.py`
- `scripts/dataset_specific_scripts/unified/run_unified_pipeline.py`
- `scripts/dataset_specific_scripts/unified/README.md`

Runtime execution profiles for local/AWS/HPC orchestration:

- `config/runtime_profiles/unified_pipeline_profiles.json`

## Unified Pipeline Orchestration

The unified runner supports the same pipeline on local servers, AWS, and HPC
with profile-based configuration:

```bash
python3 scripts/dataset_specific_scripts/unified/run_unified_pipeline.py \
  --profile local_laptop \
  --step all \
  --reset-publish-output \
  --log-level INFO
```

BigRed200 profile is available as `bigred200_hpc` and supports Slurm
dependency chaining:

```bash
python3 scripts/dataset_specific_scripts/unified/run_unified_pipeline.py \
  --profile bigred200_hpc \
  --mode slurm \
  --submit-slurm \
  --reset-publish-checkpoint \
  --reset-publish-output \
  --log-level INFO
```

Use `--set key=value` for machine-specific overrides without changing source:

```bash
python3 scripts/dataset_specific_scripts/unified/run_unified_pipeline.py \
  --profile bigred200_hpc \
  --dry-run \
  --set publish.per_gene_shards=2048 \
  --set slurm.partition=cpu
```

The runner uses the same Python interpreter that launches it (so running from
an activated `.venv` carries into Slurm jobs). Override with
`--python-executable` when needed.
For HPC module environments, add setup commands per job with
`--slurm-setup-command "module load python/3.11"`.

## Contributing

We welcome new datasets and improvements. See [CONTRIBUTING.md](CONTRIBUTING.md) for a walkthrough of the submission process and consult the files in the `docs/` directory for more details.
