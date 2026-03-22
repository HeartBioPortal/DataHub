# Scripts Guide

## General entrypoints

### `scripts/prepare_association_raw.py`

Prepare irregular raw association inputs using a prep profile.

Use this when:

- the source has unstable columns or inconsistent raw formatting
- you need an auditable intermediate CSV before canonical ingest

### `scripts/build_legacy_association.py`

Build legacy-compatible association outputs from the modular pipeline.

Use this when:

- you want a simpler association-only pipeline path
- you are not using the full unified DuckDB-first workflow

### `scripts/run_ingestion.py`

Run configurable source-driven ingestion.

Use this when:

- you want a config-driven adapter/source/publisher pipeline
- you are exercising the more general modular ingestion surface

### `scripts/run_structural_variant_ingestion.py`

Run streaming dbVar structural-variant publication through DataHub.

Use this when:

- you need the legacy `structural_variants.json` artifact
- the source file is large enough that the generic in-memory pipeline is the wrong tool
- you want dbVar parsing, Ensembl enrichment, validation, and publication to live in DataHub instead of DataManager

Important contract/config split:

- output shape comes from `config/output_contracts/structural_variant_legacy.json`
- gene metadata reuse is a separate seed input
- merge behavior is a separate existing-output concern
- local gene overlap can come from a pinned GTF, with Ensembl used only as fallback when needed
- resume uses a row-level checkpoint plus periodic output snapshots, so reruns can continue from the last saved raw-row boundary

Recommended repository-local invocation:

```bash
python scripts/run_structural_variant_ingestion.py \
  --input raw_data/dbvar/dbvar_structural_variants_nstd229.csv.zip \
  --gene-annotation-gtf raw_data/gencode.v49.annotation.gtf.gz \
  --output-json analyzed_data/dbvar/dbvar_structural_variants_nstd229.json \
  --gene-metadata-seed analyzed_data/dbvar/dbvar_structural_variants_nstd102_seed.json.zip \
  --merge-source-json analyzed_data/dbvar/dbvar_structural_variants_nstd102_seed.json.zip \
  --merge-existing \
  --report-path analyzed_data/dbvar/dbvar_structural_variants_nstd229.report.json \
  --cache-path analyzed_data/dbvar/dbvar_structural_variant_ensembl_cache.json \
  --checkpoint-every-rows 50000 \
  --skip-row-count \
  --progress-every 5000
```

Resume notes:

- checkpoint defaults to `analyzed_data/dbvar/dbvar_structural_variants_nstd229.json.checkpoint.json`
- rerun the same command to continue from the latest saved checkpoint
- use `--reset-checkpoint` to force a clean restart
- use `--no-resume` to ignore checkpoint state for a one-off fresh run

## MVP scripts

### `scripts/dataset_specific_scripts/mvp/ingest_mvp_duckdb_fast.py`

Fast, resumable MVP ingest into DuckDB points.

### `scripts/dataset_specific_scripts/mvp/run_mvp_pipeline.py`

MVP-specific end-to-end pipeline including legacy-compatible publication.

### `scripts/dataset_specific_scripts/mvp/export_mvp_prepared_raw.py`

Export prepared raw MVP rows for audit or downstream merge workflows.

## Unified scripts

### `scripts/dataset_specific_scripts/unified/ingest_legacy_raw_duckdb.py`

Ingest legacy raw CVD/trait files into the shared DuckDB points table.

### `scripts/dataset_specific_scripts/unified/publish_unified_from_duckdb.py`

Publish legacy-compatible analyzed outputs from unified DuckDB points, with checkpointing and partition support.

Important operational flags:

- `--preflight-validate-units N`
  - validates the first `N` staged units before the rest of the run continues
- `--unit-partitions` / `--unit-partition-index`
  - deterministic parallel publish partitioning
- `--reset-checkpoint`
  - clear resume state before a fresh rerun

### `scripts/dataset_specific_scripts/unified/build_association_serving_duckdb.py`

Build a compact serving DuckDB from published outputs.

### `scripts/dataset_specific_scripts/unified/run_unified_pipeline.py`

Profile-driven orchestration for the unified pipeline across laptop/AWS/HPC.

## Script philosophy

The scripts directory is intentionally operational. Business/scientific logic should live in `src/datahub/` when it can. Scripts should compose that logic and add environment/runtime concerns such as CLI parsing, checkpoints, and scheduler integration.
