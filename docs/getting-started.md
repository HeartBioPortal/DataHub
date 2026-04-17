# Getting Started

## Prerequisites

- Python 3.11+ recommended
- `pip`
- Git
- Git LFS if you plan to work with large tracked artifacts
- `duckdb` support via `pyproject.toml` or `requirements.txt`

## Local setup

```bash
git clone https://github.com/HeartBioPortal/DataHub.git
cd DataHub
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -e ".[test,docs]"
python -m pytest
```

Use `python -m pytest` rather than a bare `pytest` when you want to be certain
the active virtual environment is the one running tests and subprocess checks.

## What to read first

1. `README.md`
2. [Repository Tour](repository-tour.md)
3. [Architecture Philosophy](architecture/philosophy.md)
4. [System Overview](architecture/system-overview.md)

## The main working modes

### 1. Library mode

You import code from `src/datahub/` and compose adapters, contracts, storage, and publishers directly.

### 2. Script mode

You use entrypoint scripts under `scripts/` for raw preparation, ingest, publish, or orchestration.

### 3. Environment-profile mode

You run the unified orchestration flow with runtime profiles so the same logical pipeline can target a laptop, AWS, or Slurm/HPC with only profile changes.

## The first practical commands to know

### Prepare irregular legacy raw inputs

```bash
python3 scripts/prepare_association_raw.py \
  --input-csv /path/to/raw.csv \
  --output-csv /path/to/prepared.csv \
  --profile legacy_cvd_raw
```

### Run configurable source-driven ingestion

```bash
datahub-run-ingestion --config path/to/ingestion.json
```

### Run the unified profile-driven pipeline

```bash
datahub-run-unified-pipeline \
  --profile local_laptop \
  --step all \
  --log-level INFO
```

`--step all` initializes the working DuckDB lifecycle tables first, then runs
MVP ingest, legacy ingest, and unified publish. Direct DuckDB-heavy CLIs default
their spill directory to a local-safe `<db-dir>/_duckdb_tmp` path; runtime
profiles can still point production/HPC runs at scratch storage.

### Validate config files

```bash
python -c "from datahub.config_schemas import validate_default_config_tree, format_config_validation_issues; issues = validate_default_config_tree(); print(format_config_validation_issues(issues) if issues else 'config ok')"
```

### Generate an artifact QA report

```bash
datahub-report-artifact-qa \
  --published-root /path/to/analyzed_data_unified \
  --serving-db-path /path/to/association_serving.duckdb \
  --working-db-path /path/to/datahub_working.duckdb \
  --output-json /path/to/datahub_qa_report.json
```

The report summarizes source catalog status, published payload counts and sample
checksums, working DuckDB tables, and serving DuckDB table counts.

## Recommended mental model

Read the repository in this order:

1. **Config surfaces** to understand what is declarative
2. **Adapters** to understand how raw/source-specific logic enters the system
3. **Models and validation** to understand what canonical records are supposed to look like
4. **Publishers** to understand how analyzed outputs are constructed
5. **Operational scripts** to understand how the pieces are executed in practice

## Common mistakes for new contributors

- Adding source-specific logic directly inside publishers instead of adapters or config
- Treating published `.json.gz` as source-of-truth rather than a published artifact
- Reconstructing biological logic in the backend instead of promoting it into DataHub
- Mixing environment/runtime profile concerns with scientific/data-model concerns
- Adding fields ad hoc without defining where they belong: prep, canonical schema, metadata, published payload, or serving artifact
