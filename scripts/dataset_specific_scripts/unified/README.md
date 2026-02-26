# Unified Legacy + MVP Pipeline (DuckDB-first)

This workflow keeps aggregation at the raw/point layer and generates legacy JSON
once from a unified table.

## Profile-driven runner (recommended)

Use the profile runner to keep one code path across laptop/AWS/HPC and only
change execution profile:

```bash
python3 scripts/dataset_specific_scripts/unified/run_unified_pipeline.py \
  --profile local_laptop \
  --step all \
  --reset-publish-output \
  --log-level INFO
```

Runtime profiles live in:

- `config/runtime_profiles/unified_pipeline_profiles.json`

Built-in profiles:

- `local_laptop`
- `aws_server`
- `bigred200_hpc`

BigRed200 dry-run (inspect resolved commands + paths):

```bash
python3 scripts/dataset_specific_scripts/unified/run_unified_pipeline.py \
  --profile bigred200_hpc \
  --dry-run
```

BigRed200 submit with Slurm dependency chain (`mvp_ingest -> legacy_ingest -> publish`):

```bash
python3 scripts/dataset_specific_scripts/unified/run_unified_pipeline.py \
  --profile bigred200_hpc \
  --mode slurm \
  --submit-slurm \
  --reset-publish-checkpoint \
  --reset-publish-output \
  --log-level INFO
```

Override any profile value at runtime:

```bash
python3 scripts/dataset_specific_scripts/unified/run_unified_pipeline.py \
  --profile bigred200_hpc \
  --dry-run \
  --set publish.per_gene_shards=2048 \
  --set slurm.partition=cpu \
  --set publish.memory_limit=96GB
```

If your profile uses environment variables (for example `$SCR`), export them
before non-dry runs. The runner fails fast when unresolved variables remain.

## 1) Ingest MVP points (already done in your case)

```bash
python3 scripts/dataset_specific_scripts/mvp/ingest_mvp_duckdb_fast.py \
  --input-path "/data/aggregated_phenotypes/*.csv.gz" \
  --db-path /data/hbp/datamart/mvp_fast.duckdb \
  --checkpoint-path /data/hbp/state/mvp_fast_checkpoint.json \
  --threads 16 \
  --memory-limit 8GB \
  --temp-directory /data/hbp/datamart/duckdb_tmp \
  --log-level INFO
```

## 2) Ingest legacy raw CVD + trait files into the same points table

```bash
python3 scripts/dataset_specific_scripts/unified/ingest_legacy_raw_duckdb.py \
  --cvd-input-path "/home/ubuntu/v3/back/hbpback-2.5/analyzer/DataManager/raw_data/cvd/*.txt" \
  --trait-input-path "/home/ubuntu/v3/back/hbpback-2.5/analyzer/DataManager/raw_data/trait/*.txt" \
  --db-path /data/hbp/datamart/mvp_fast.duckdb \
  --table-name mvp_association_points \
  --checkpoint-path /data/hbp/state/legacy_raw_checkpoint.json \
  --threads 8 \
  --memory-limit 8GB \
  --temp-directory /data/hbp/datamart/duckdb_tmp \
  --log-level INFO
```

Notes:

- This is file-level resumable by checkpoint.
- Ingest is idempotent per file (`source_file + source + dataset_type`).

## 3) Publish unified legacy-compatible JSON from DuckDB (single analysis phase)

```bash
python3 scripts/dataset_specific_scripts/unified/publish_unified_from_duckdb.py \
  --db-path /data/hbp/datamart/mvp_fast.duckdb \
  --source-table mvp_association_points \
  --dedup-mode per_gene \
  --per-gene-shards 512 \
  --working-table association_points_unified \
  --output-root /data/hbp/analyzed_data_unified \
  --rollup-tree-json /data/hbp/config/phenotype_tree.json \
  --source-priority "legacy_cvd_raw,legacy_trait_raw,million_veteran_program" \
  --dataset-types "CVD,TRAIT" \
  --json-compression gzip \
  --json-indent 0 \
  --publish-batch-size 50000 \
  --query-chunk-rows 200000 \
  --checkpoint-path /data/hbp/state/unified_publish_checkpoint.json \
  --state-dir /data/hbp/state/unified_publish_state \
  --threads 8 \
  --memory-limit 8GB \
  --temp-directory /data/hbp/datamart/duckdb_tmp \
  --max-temp-directory-size 100GiB \
  --reset-output \
  --log-level INFO
```

Notes:

- `--dedup-mode per_gene` is the default and recommended for very large datasets.
- This mode avoids creating a full materialized unified table (lower disk pressure).
- In `per_gene` mode, work is processed in deterministic hash shards, not one query per gene.
- Start with `--per-gene-shards 512` (increase to `1024` if units still run too long).
- `global_table` mode is still available for smaller runs.
- With `--json-compression gzip`, outputs are written as `.json.gz` files.
- `--json-indent 0` writes compact JSON for better compression and less disk usage.

After validating outputs, push into Redis (same output snapshot):

```bash
python3 scripts/dataset_specific_scripts/unified/publish_unified_from_duckdb.py \
  --db-path /data/hbp/datamart/mvp_fast.duckdb \
  --source-table mvp_association_points \
  --working-table association_points_unified \
  --output-root /data/hbp/analyzed_data_unified \
  --checkpoint-path /data/hbp/state/unified_publish_checkpoint.json \
  --state-dir /data/hbp/state/unified_publish_state \
  --publish-redis \
  --log-level INFO
```

## 4) Resume behavior

- Legacy raw ingest: rerun without `--reset-checkpoint` to continue.
- Unified publish: rerun without `--reset-checkpoint` to continue from pending units.
- If source table contents change (new ingest), run publish with `--reset-checkpoint`.
