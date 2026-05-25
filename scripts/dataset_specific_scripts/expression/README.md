# Expression v3 Pipeline

Expression v3 rebuilds HeartBioPortal differential-expression evidence from
curated public-source studies. We use Python for orchestration, discovery,
curation manifests, DataHub artifact generation, validation, and DuckDB export.
For approved GEO microarray contrasts, we use GEOquery, limma, and Biobase from
Bioconductor.

## Ubuntu / AWS Setup

The local R packages live under DataHub `.r-lib`, which is ignored by git.
Ubuntu needs system headers for the R `curl` and `xml2` packages before
`GEOquery` can load.

```bash
cd /data/DataHub
mkdir -p .r-lib

sudo apt-get update
sudo apt-get install -y libcurl4-openssl-dev libxml2-dev libssl-dev

R_LIBS_USER="$PWD/.r-lib" Rscript -e ".libPaths(c(Sys.getenv('R_LIBS_USER'), .libPaths())); if (!requireNamespace('BiocManager', quietly=TRUE)) install.packages('BiocManager', repos='https://cloud.r-project.org')"

R_LIBS_USER="$PWD/.r-lib" Rscript -e ".libPaths(c(Sys.getenv('R_LIBS_USER'), .libPaths())); install.packages(c('curl','xml2'), repos='https://cloud.r-project.org')"

R_LIBS_USER="$PWD/.r-lib" Rscript -e ".libPaths(c(Sys.getenv('R_LIBS_USER'), .libPaths())); BiocManager::install(c('GEOquery','limma','Biobase'), ask=FALSE, update=FALSE)"
```

Verify package availability:

```bash
R_LIBS_USER="$PWD/.r-lib" Rscript -e ".libPaths(c(Sys.getenv('R_LIBS_USER'), .libPaths())); pkgs <- c('BiocManager','GEOquery','limma','Biobase'); print(setNames(vapply(pkgs, requireNamespace, logical(1), quietly=TRUE), pkgs))"
```

Expected result:

```text
BiocManager GEOquery limma Biobase
TRUE        TRUE     TRUE  TRUE
```

If `GEOquery` is `FALSE`, the usual cause is a failed `curl` or `xml2`
installation. Re-run the `apt-get install` line, then reinstall `curl`, `xml2`,
and `GEOquery`.

## Build Commands

Download GEOmetadb:

```bash
./.venv/bin/python scripts/dataset_specific_scripts/expression/run_expression_pipeline.py \
  download-geometadb \
  --output-sqlite data/raw/expression/GEOmetadb.sqlite
```

Discover CVD GEO candidates and seed the curation manifest:

```bash
./.venv/bin/python scripts/dataset_specific_scripts/expression/run_expression_pipeline.py \
  discover-geo \
  --geometadb-sqlite data/raw/expression/GEOmetadb.sqlite \
  --phenotype-tree-json config/phenotype_tree.json \
  --output-csv data/interim/expression_v3/geo_cvd_candidates.csv \
  --curation-output-csv data/interim/expression_v3/geo_cvd_curation_manifest.csv
```

The default discovery path uses `config/phenotype_tree.json`, so each candidate
keeps both the original matched GEO search term and the HBP phenotype tree path.
Curate `data/interim/expression_v3/geo_cvd_curation_manifest.csv`. Approved rows
include explicit case/control sample accessions, disease labels, tissue,
platform, sample sizes, contrast direction, and the phenotype tree path we use
for portal alignment.

Run approved GEO/limma contrasts:

```bash
R_LIBS_USER="$PWD/.r-lib" \
./.venv/bin/python scripts/dataset_specific_scripts/expression/run_expression_pipeline.py \
  run-approved-geo-de \
  --curation-csv data/interim/expression_v3/geo_cvd_curation_manifest.csv \
  --output-csv data/interim/expression_v3/geo_limma_results.csv \
  --cache-dir data/cache/expression_v3/geo
```

Import v3 row-level results into DataHub artifacts and DuckDB:

```bash
./.venv/bin/python scripts/dataset_specific_scripts/expression/run_expression_pipeline.py \
  import-v3-results \
  --input-csv data/interim/expression_v3/geo_limma_results.csv \
  --output-root secondary_analyses \
  --duckdb-path datamart/expression_v3.duckdb
```

Outputs:

- `secondary_analyses/final/expression_v3/differential_expression_rows.csv`
- `secondary_analyses/final/expression_v3/gene_phenotype_summary.csv`
- `secondary_analyses/final/expression_v3/gene_phenotype_summary.json`
- `secondary_analyses/final/expression_v3/expression_legacy_compatible.json`
- `secondary_analyses/final/expression_v3/expression_serving_summary.json`
- `datamart/expression_v3.duckdb`
