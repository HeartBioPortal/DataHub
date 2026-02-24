# MVP Dataset-Specific Scripts

This folder contains scripts specific to Million Veteran Program (MVP)
integration workflows.

## 1) Fast ingest (recommended for very large MVP drops)

```bash
scripts/dataset_specific_scripts/mvp/ingest_mvp_duckdb_fast.py \
  --input-path "/data/aggregated_phenotypes/*.csv.gz" \
  --db-path /data/hbp/datamart/mvp_fast.duckdb \
  --threads 16 \
  --log-level INFO
```

This mode is much faster because parsing, normalization, dataset-type mapping,
and per-file dedup happen in DuckDB SQL (vectorized), not Python row loops.

Resume behavior:

- Checkpoint defaults to `<db-path>.mvp_fast_checkpoint.json`.
- Reruns skip completed files automatically.
- Use `--reset-checkpoint` to force full re-ingest.

## 2) Run full MVP legacy-compatible JSON pipeline

```bash
scripts/dataset_specific_scripts/mvp/run_mvp_pipeline.py \
  --input-path "/data/aggregated_phenotypes/*.csv.gz" \
  --output-root /data/hbp/analyzed_data \
  --duckdb-path /data/hbp/datamart/canonical.duckdb \
  --parquet-path /data/hbp/datamart/canonical.parquet \
  --rollup-tree-json /data/hbp/config/phenotype_tree.json \
  --merge-mode chunk \
  --chunksize 100000 \
  --publish-batch-size 50000 \
  --progress-every-rows 200000 \
  --reset-checkpoint \
  --log-level INFO
```

To resume after interruption:

```bash
scripts/dataset_specific_scripts/mvp/run_mvp_pipeline.py \
  --input-path "/data/aggregated_phenotypes/*.csv.gz" \
  --output-root /data/hbp/analyzed_data \
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
- per-batch publish/storage totals,
- final runtime and record-count summary.

For large datasets, keep `--merge-mode chunk` so memory stays bounded while
publishing incrementally.

Resume behavior:

- Checkpoint defaults to `<output-root>/_datahub_state/mvp/checkpoint.json`.
- Completed files are skipped automatically on rerun (unless `--no-resume`).
- Each file is written to staged output first, then merged into final output, which prevents
  partial-file double counting on interruption.
- Use `--reset-checkpoint` to force a clean restart.

## 3) Export prepared raw CSV (for audit/merge workflows)

```bash
scripts/dataset_specific_scripts/mvp/export_mvp_prepared_raw.py \
  --input-path "/data/aggregated_phenotypes/*.csv.gz" \
  --output-csv /data/mvp/mvp_prepared_raw.csv
```

This produces one row per canonical variant-phenotype association with
standardized columns and `ancestry_data` JSON.
