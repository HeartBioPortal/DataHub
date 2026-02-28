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

Parallel publish on HPC (run multiple partition jobs in parallel):

```bash
# Clear output once, then launch N partition jobs without --reset-output.
N=8
for i in $(seq 0 $((N-1))); do
  sbatch --account r01806 --time 24:00:00 --cpus-per-task 32 --mem 128G \
    --job-name "datahub_publish_p${i}" \
    --output "/N/scratch/kvand/hbp/logs/datahub/publish_p${i}_%j.out" \
    --error "/N/scratch/kvand/hbp/logs/datahub/publish_p${i}_%j.err" \
    --wrap "module load python/3.11 && cd /geode2/home/u050/kvand/BigRed200/DataHub && \
      /geode2/home/u050/kvand/BigRed200/DataHub/.venv/bin/python3 \
      scripts/dataset_specific_scripts/unified/publish_unified_from_duckdb.py \
      --db-path /N/scratch/kvand/hbp/datamart/mvp_fast.duckdb \
      --source-table mvp_association_points \
      --output-root /N/scratch/kvand/hbp/analyzed_data_unified \
      --rollup-tree-json /N/scratch/kvand/hbp/config/phenotype_tree.json \
      --source-priority legacy_cvd_raw,legacy_trait_raw,million_veteran_program \
      --dataset-types CVD,TRAIT \
      --dedup-mode per_gene \
      --per-gene-shards 512 \
      --unit-partitions ${N} \
      --unit-partition-index ${i} \
      --resume-seed-checkpoint /N/scratch/kvand/hbp/state/unified_publish_checkpoint.json \
      --publish-batch-size 100000 \
      --query-chunk-rows 500000 \
      --json-compression gzip --json-indent 0 \
      --threads 32 --memory-limit 64GB \
      --temp-directory /N/scratch/kvand/hbp/datamart/duckdb_tmp \
      --max-temp-directory-size 500GiB \
      --log-level INFO"
done
```

Important:

- Do not use `--reset-output` with `--unit-partitions > 1` in parallel jobs.
- Partitioned jobs use partition-specific checkpoint/state files automatically.
- If migrating from a prior single-run checkpoint, use `--resume-seed-checkpoint`
  so partition jobs skip already completed units instead of reprocessing them.

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

Python runtime note:

- The runner uses the same interpreter that launched it (for example `.venv/bin/python3`).
- You can override it explicitly with `--python-executable /path/to/python3`.
- For HPC modules, add pre-step commands with `--slurm-setup-command` (repeatable),
  for example `--slurm-setup-command "module load python/3.11"`.

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
