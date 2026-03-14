# Unified DuckDB Pipeline

The unified pipeline is the main large-scale integration path for merged MVP and legacy association data.

## Why this pipeline exists

The repository previously had a more fragmented story where different datasets or legacy paths could be processed independently. The unified pipeline exists so that:

- MVP and legacy data can be merged into one points table
- source-priority deduplication happens centrally
- publication can operate from a reproducible working store
- the same logical steps can run on a laptop, cloud VM, or HPC cluster

## Main scripts

- `scripts/dataset_specific_scripts/mvp/ingest_mvp_duckdb_fast.py`
- `scripts/dataset_specific_scripts/unified/ingest_legacy_raw_duckdb.py`
- `scripts/dataset_specific_scripts/unified/publish_unified_from_duckdb.py`
- `scripts/dataset_specific_scripts/unified/run_unified_pipeline.py`

## Stages

### MVP ingest

This step ingests long-form MVP rows into a unified points table in DuckDB. It is optimized for scale and resumability.

### Legacy raw ingest

This step ingests historical raw CVD and trait files into the same points table schema.

### Unified publish

This step reads from DuckDB, applies source-priority deduplication, and publishes legacy-compatible analyzed outputs. It supports resumable unit processing and partitioned HPC execution.

### Optional serving build

A compact serving DuckDB can then be built from published outputs.

## Runtime profiles

The unified pipeline should normally be launched through runtime profiles in `config/runtime_profiles/unified_pipeline_profiles.json`.

These profiles separate:

- scientific/data flow logic
- environment-specific execution details

This is a major design choice. Laptop/AWS/HPC should not require different scientific code paths.

## HPC behavior

The unified publish step supports:

- checkpointed resume
- deterministic partitioning
- per-shard unit distribution
- large temporary spill directories
- long-running batch operation under Slurm

This is why the publish step is written as a streaming, resumable process rather than a monolithic whole-dataset export.
