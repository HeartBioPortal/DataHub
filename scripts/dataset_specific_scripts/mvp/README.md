# MVP Dataset-Specific Scripts

This folder contains scripts specific to Million Veteran Program (MVP)
integration workflows.

## 1) Run full MVP pipeline

```bash
scripts/dataset_specific_scripts/mvp/run_mvp_pipeline.py \
  --input-path "/data/aggregated_phenotypes/*.csv.gz" \
  --output-root /data/hbp/analyzed_data \
  --duckdb-path /data/hbp/datamart/canonical.duckdb \
  --parquet-path /data/hbp/datamart/canonical.parquet \
  --rollup-tree-json /data/hbp/config/phenotype_tree.json \
  --progress-every-rows 200000 \
  --log-level INFO
```

This uses:

- `MVPAssociationAdapter` for long-form ancestry rows,
- `LegacyAssociationPublisher` for backward-compatible HBP payloads,
- `PhenotypeRollupPublisher` for parent-level rollups with rsid dedup.

The script emits:

- startup/config logs,
- per-file start/completion logs,
- periodic row progress logs during ingestion,
- final runtime and record-count summary.

## 2) Export prepared raw CSV (for audit/merge workflows)

```bash
scripts/dataset_specific_scripts/mvp/export_mvp_prepared_raw.py \
  --input-path "/data/aggregated_phenotypes/*.csv.gz" \
  --output-csv /data/mvp/mvp_prepared_raw.csv
```

This produces one row per canonical variant-phenotype association with
standardized columns and `ancestry_data` JSON.
