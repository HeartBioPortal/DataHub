# Scripts Guide

![Generated visual for Scripts Guide](../assets/visuals/scripts-guide.svg){ .doc-visual }

## General entrypoints

Editable installs expose console commands for the main entrypoints. For
example, `datahub-run-ingestion` is the console-command equivalent of
`scripts/run_ingestion.py`.

All operational scripts should follow the repository-level standards in
`SCRIPT_MANIFESTO.md`: visible progress, structured logging, resumable
checkpointing for long jobs, smoke-test controls, explicit output paths, and a
machine-readable summary when practical.

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
- local gene overlap can come from a pinned GTF, with Ensembl overlap fallback disabled by default for speed
- resume uses a row-level checkpoint plus periodic output snapshots, so reruns can continue from the last saved raw-row boundary

Recommended repository-local invocation:

```bash
python scripts/run_structural_variant_ingestion.py \
  --input raw_data/dbvar/dbvar_structural_variants_nstd229.csv.zip \
  --gene-annotation-gtf raw_data/gencode.v49.annotation.gtf.gz \
  --output-json analyzed_data/dbvar/dbvar_structural_variants_nstd229.json.zip \
  --gene-metadata-seed analyzed_data/dbvar/dbvar_structural_variants_nstd102_seed.json.zip \
  --merge-source-json analyzed_data/dbvar/dbvar_structural_variants_nstd102_seed.json.zip \
  --merge-existing \
  --report-path analyzed_data/dbvar/dbvar_structural_variants_nstd229.report.json \
  --cache-path analyzed_data/dbvar/dbvar_structural_variant_ensembl_cache.json \
  --checkpoint-every-rows 50000 \
  --progress-every 5000
```

### `scripts/report_artifact_qa.py`

Build a JSON release QA report for published outputs and DuckDB artifacts.

Use this when:

- you need row counts and checksums for a DataHub release
- you want to verify source-catalog integration status alongside artifacts
- you want a compact handoff report after building the serving DB

Example:

```bash
datahub-report-artifact-qa \
  --published-root /data/hbp/analyzed_data_unified \
  --working-db-path /data/hbp/datamart/mvp_fast.duckdb \
  --serving-db-path /data/hbp/datamart/association_serving.duckdb \
  --output-json /data/hbp/state/datahub_qa_report.json
```

Resume notes:

- checkpoint defaults to `analyzed_data/dbvar/dbvar_structural_variants_nstd229.json.checkpoint.json`
- use `--output-json ...json.zip` for reusable checked-in artifacts; DataHub reads and writes single-file JSON zip artifacts directly
- rerun the same command to continue from the latest saved checkpoint
- use `--reset-checkpoint` to force a clean restart
- use `--no-resume` to ignore checkpoint state for a one-off fresh run
- if you add `--skip-row-count`, progress percent is intentionally unavailable because the total row count is not precomputed
- add `--enable-ensembl-overlap-fallback` only if you want no-hit rows double-checked against Ensembl

### `scripts/dataset_specific_scripts/unified/canonicalize_variant_viewer_artifacts.py`

Canonicalize legacy Protein Consequence Viewer artifacts against
`config/phenotype_tree.json` and remove macOS resource-fork files from raw data.

Use this when:

- legacy `variant_viewer` artifacts contain malformed phenotype slugs such as
  missing leading/trailing letters
- raw inputs contain macOS files such as `._stroke.txt` or `.DS_Store`
- you need the protein consequence viewer to rely on clean DataHub artifacts
  instead of backend display-time correction

The script rewrites row phenotype labels, merges malformed phenotype folders
into canonical folders, drops known resource-fork-derived bogus gene artifacts,
and writes a checkpoint plus a JSON report. It should be run in dry-run mode
first:

```bash
python scripts/dataset_specific_scripts/unified/canonicalize_variant_viewer_artifacts.py \
  --variant-viewer-root analyzed_data/variant_viewer \
  --phenotype-tree-json config/phenotype_tree.json \
  --raw-root raw_data \
  --dry-run \
  --remove-hidden-raw \
  --fail-on-unknown \
  --reset-checkpoint \
  --verbose
```

Apply mode should keep a backup unless the artifact root is disposable:

```bash
python scripts/dataset_specific_scripts/unified/canonicalize_variant_viewer_artifacts.py \
  --variant-viewer-root analyzed_data/variant_viewer \
  --phenotype-tree-json config/phenotype_tree.json \
  --raw-root raw_data \
  --remove-hidden-raw \
  --fail-on-unknown \
  --reset-checkpoint \
  --backup-root analyzed_data/backups \
  --verbose
```

The final dry-run after apply should report zero remaining rewrites, drops, and
unknown phenotype labels.

### `scripts/enrich_structural_variant_exons.py`

Backfill missing canonical transcript exon arrays in a legacy structural variant
artifact using Ensembl `lookup/id?expand=1`. Use this after a large dbVar run
when local GTF metadata supplied gene/transcript spans but not full exon
structure for newly added genes.

Default behavior is conservative:

- genes whose `canonical_transcript[0].Exon` already exists are skipped, so the
  older Ensembl-seeded genes are left alone
- genes are looked up by their existing Ensembl transcript ID
- the output shape stays compatible with the legacy backend/frontend contract

Single-job enrichment:

```bash
python scripts/enrich_structural_variant_exons.py \
  --input-json analyzed_data/dbvar/dbvar_structural_variants_nstd229.json.zip \
  --output-json analyzed_data/dbvar/dbvar_structural_variants_nstd229.exons.json.zip \
  --cache-path analyzed_data/dbvar/dbvar_structural_variant_exon_ensembl_cache.json \
  --report-path analyzed_data/dbvar/dbvar_structural_variants_nstd229.exons.report.json \
  --progress-every 100 \
  --log-level INFO
```

Partitioned HPC-safe fetch/apply mode:

```bash
python scripts/enrich_structural_variant_exons.py \
  --input-json analyzed_data/dbvar/dbvar_structural_variants_nstd229.json.zip \
  --patch-output-json /N/scratch/kvand/hbp/sv_exon_patches/nstd229_p00.json \
  --cache-path /N/scratch/kvand/hbp/cache/sv_exon_ensembl_p00.json \
  --sleep-seconds 0.25 \
  --unit-partitions 32 \
  --unit-partition-index 0

python scripts/enrich_structural_variant_exons.py \
  --input-json analyzed_data/dbvar/dbvar_structural_variants_nstd229.json.zip \
  --output-json analyzed_data/dbvar/dbvar_structural_variants_nstd229.exons.json.zip \
  --patch-input-json /N/scratch/kvand/hbp/sv_exon_patches/nstd229_p*.json
```

For array jobs, keep `--sleep-seconds` nonzero so parallel partitions do not
hit Ensembl in the same burst. The shared API client also retries `429 Too Many
Requests` responses using `Retry-After` when Ensembl provides it.

## Expression scripts

### `scripts/dataset_specific_scripts/expression/run_expression_pipeline.py`

Run the curation-gated expression workflow and its compatibility builders.
Subcommands cover:

- enriching the legacy expression payload with recoverable CardioQuilt/CREEDS-GEO provenance
- downloading GEOmetadb and discovering cardiovascular study candidates
- generating curation suggestions from GEO sample metadata
- running approved GEO disease-versus-control contrasts with GEOquery/limma
- importing reviewed row-level results into expression v3 CSV/JSON artifacts and an optional DuckDB

Automated discovery and curation suggestions do not approve studies. A
production expression v3 row requires an approved curation record with an
explicit contrast direction, case/control samples, phenotype label, tissue and
platform metadata where available, and source provenance.

See [Expression v3 Schema](../schemas/expression_v3.md) and the operational
README at `scripts/dataset_specific_scripts/expression/README.md`.

## MVP scripts

### `scripts/dataset_specific_scripts/mvp/ingest_mvp_duckdb_fast.py`

Fast, resumable MVP ingest into DuckDB points.

Direct use defaults DuckDB temp spill files to `<db-dir>/_duckdb_tmp` so laptop
and test runs do not require `/data`. Runtime profiles can still pass a
production scratch path through `paths.temp_directory`.

### `scripts/dataset_specific_scripts/mvp/run_mvp_pipeline.py`

MVP-specific end-to-end pipeline including legacy-compatible publication.

### `scripts/dataset_specific_scripts/mvp/export_mvp_prepared_raw.py`

Export prepared raw MVP rows for audit or downstream merge workflows.

## Unified scripts

### `scripts/dataset_specific_scripts/unified/ingest_legacy_raw_duckdb.py`

Ingest versioned CVD/trait snapshots derived from the NHGRI-EBI GWAS Catalog
into the shared DuckDB points table. The `legacy_cvd_raw` and
`legacy_trait_raw` values are pipeline source IDs, not upstream database names.

### `scripts/dataset_specific_scripts/unified/manage_working_duckdb.py`

Manage the first concrete implementation of the target DataHub lifecycle model inside a working DuckDB.

Use this when:

- you want to initialize the lifecycle tables in a working DuckDB
- you want to register a source-native raw release and inventory its source columns
- you want schema drift reports before running analysis
- you want to load prepared association CSVs into `source_normalized_association`
- you want to materialize `analysis_ready_association` from the current points table during migration

Subcommands:

- `init`
  - creates `raw_release_registry`, `raw_file_inventory`, `schema_drift_reports`, `source_normalized_association`, and `analysis_ready_association`
- `register-raw-release`
  - inventories raw files, stores ordered columns and schema fingerprints, and writes a drift verdict
  - when `--prep-profile` is provided, field-candidate aliases are treated as alternatives rather than requiring every alias column to exist
- `load-source-normalized-association`
  - loads a prepared association CSV into the source-normalized zone
- `materialize-analysis-ready-association`
  - materializes the current `mvp_association_points` table into the analysis-ready zone for migration/audit

Example:

```bash
python3 scripts/dataset_specific_scripts/unified/manage_working_duckdb.py init \
  --db-path /data/hbp/datamart/datahub_working.duckdb

python3 scripts/dataset_specific_scripts/unified/manage_working_duckdb.py register-raw-release \
  --db-path /data/hbp/datamart/datahub_working.duckdb \
  --source-id legacy_cvd_raw \
  --release-id legacy_v1 \
  --modality association \
  --input-path "/path/to/legacy_raw/cvd/*.txt" \
  --prep-profile legacy_cvd_raw \
  --skip-checksum \
  --fail-on-breaking-drift
```

### `scripts/dataset_specific_scripts/unified/publish_unified_from_duckdb.py`

Publish legacy-compatible analyzed outputs from unified DuckDB points, with checkpointing and partition support.

Important operational flags:

- `--preflight-validate-units N`
  - validates the first `N` staged units before the rest of the run continues
- `--publisher-mode all`
  - publishes association, overall, variant index, and rollups unless disabled
- `--publisher-mode variant-index-only`
  - backfills only `association/final/variant_index` using a separate default
    checkpoint namespace, so existing completed association checkpoints do not
    cause the backfill to skip every unit
  - streams variant-index JSON arrays to disk as canonical records are produced,
    so very large genes do not have to be held in Python memory before writing
- `--variant-index-query-mode grouped`
  - default for variant-index backfills; uses grouped source-priority collapse
    instead of a `row_number()` window over each giant shard
- `--disable-variant-index`
  - emergency compatibility flag for old consumers that do not want the
    filterable variant-index artifacts
- `--unit-partitions` / `--unit-partition-index`
  - deterministic parallel publish partitioning
- `--reset-checkpoint`
  - clear resume state before a fresh rerun

### `scripts/dataset_specific_scripts/unified/build_association_serving_duckdb.py`

Build a compact serving DuckDB from published outputs.

The builder writes both compatibility payload tables and API-shaped summary
tables:

- `association_gene_payloads` / `overall_gene_payloads`
  - full published payloads used for detail views and compatibility
- `association_summary_payloads` / `overall_summary_payloads`
  - lightweight chart-summary payloads used by `/api/search_summary/`

### `scripts/dataset_specific_scripts/unified/upgrade_association_serving_duckdb.py`

Add summary tables to an existing serving DuckDB in place.

Use this when:

- the full association pipeline has already finished
- a production serving DB already exists and is too large to copy or rebuild
- `/api/search_summary/` must stop reading giant full `payload_json` blobs

Example:

```bash
python3 scripts/dataset_specific_scripts/unified/upgrade_association_serving_duckdb.py \
  --db-path /data/DataHub/datamart/association_serving.duckdb \
  --batch-size 1 \
  --payload-source auto \
  --progress-interval 100 \
  --slow-payload-seconds 30 \
  --log-level INFO
```

The command is incremental. If interrupted, rerun it without
`--replace-summary-tables` and it will skip summary rows that already exist.
Use `--max-rows 10` for a quick smoke test before allowing the full run to
continue.

Payload source modes:

- `auto`: use the recorded `source_path` JSON/JSON.GZ files and fall back to
  DuckDB `payload_json`
- `source-path`: require source files and fail fast if any payload file is
  missing
- `duckdb`: read payloads only from the existing serving DB

`auto` is the Big Red upgrade mode because reading each full
payload back from a 400+ GB DuckDB VARCHAR column can be much slower than
streaming the original published JSON.GZ files.

For Big Red 200 / Slurm runs, use the bundled batch script. It processes
partition shards sequentially in one job because DuckDB should not have
multiple concurrent writers to the same database file:

```bash
cd /geode2/home/u050/kvand/BigRed200/DataHub

sbatch --export=ALL,DATAHUB_ROOT=/geode2/home/u050/kvand/BigRed200/DataHub,DB_PATH=/N/scratch/kvand/hbp/datamart/association_serving.duckdb,UNIT_PARTITIONS=16,BATCH_SIZE=1,PAYLOAD_SOURCE=auto,DUCKDB_MEMORY_LIMIT=120GB,DUCKDB_TEMP_DIRECTORY=/N/scratch/kvand/hbp/datamart/duckdb_tmp \
  scripts/slurm/upgrade_association_serving_duckdb.sbatch
```

Do not run the partitions as parallel Slurm array tasks against the same
DuckDB file. DuckDB is a single-writer database; parallel writers can contend
or fail. Use `UNIT_PARTITIONS` for restartable sequential chunks, not
concurrent writes.

### `scripts/dataset_specific_scripts/unified/run_secondary_analyses.py`

Generate or apply secondary-analysis artifacts.

Use this when:

- you want to derive `sga` from the cleaned unified association DuckDB
- you want to normalize `expression` into the standard secondary-analysis artifact layout
- you want to derive `protein_context` artifacts for the Protein Consequence Viewer from Ensembl, EBI Proteins, and InterPro
- you want to update an existing serving DuckDB with secondary analyses without rebuilding association tables

Operational note:

- the `sga` generator is designed for HPC-style runs and streams the unified association table gene-by-gene to avoid loading the full deduplicated working set into Python memory
- for large production SGA runs, use `--unit-partitions` and `--unit-partition-index` to split work across deterministic gene shards; clear the output once before submission rather than using `--replace` inside parallel jobs
- use `--duckdb-memory-limit` and `--duckdb-temp-directory` on HPC so large distinct/order phases can spill to scratch rather than being killed for exceeding Slurm memory
- for `protein_context`, use `--variant-viewer-root`, `--protein-context-cache-path`, and capped Slurm arrays to avoid API rate-limit spikes

Subcommands:

- `generate`
  - create per-gene secondary artifacts under `final/<analysis>/genes/`
- `apply`
  - load those artifacts into an existing serving DB and refresh `gene_catalog`
  - logs file-read/insert progress every `--progress-interval` artifacts so long AWS/HPC updates do not appear stalled
  - runs the table replacement and catalog refresh in one transaction so failed applies roll back cleanly

### `scripts/dataset_specific_scripts/unified/run_gene_profile_pipeline.py`

Download source snapshots and build `gene_profile` secondary-analysis artifacts
for the gene dossier/header.

Use this when:

- gene headers need HGNC/NCBI/UniProt/GOA-backed summaries and identifiers
- gene profile output should fold in existing HBP protein-context evidence
- source downloads need checksum manifests under `raw_data/gene_profile/<release>/`

Example:

```bash
python scripts/dataset_specific_scripts/unified/run_gene_profile_pipeline.py \
  --raw-root raw_data \
  --output-root secondary_analyses \
  --release 2026-05-06 \
  --protein-context-root secondary_analyses/final/protein_context
```

Smoke-test a small gene set before a full source refresh:

```bash
python scripts/dataset_specific_scripts/unified/run_gene_profile_pipeline.py \
  --raw-root raw_data \
  --output-root secondary_analyses \
  --release 2026-05-06 \
  --include-genes TTN,PCSK9,ANK2
```

Operational notes:

- the backend expects `HBP_GENE_PROFILE_PATH` to point at
  `secondary_analyses/final/gene_profile/v1`
- `--skip-download` rebuilds artifacts from existing snapshots
- `--force-download` refreshes the downloaded source files
- GOA can be skipped with `--skip-goa` when building a minimal smoke artifact

### `scripts/dataset_specific_scripts/unified/build_dbsnp_frequency_parquet.py`

Build production-scale dbSNP frequency handoff artifacts as compressed Parquet,
then import those artifacts into the final DuckDB index. This is the
recommended workflow for large dbSNP archive batches because the expensive tar
streaming work can run on HPC, while the AWS/server step becomes a simpler
Parquet-to-DuckDB import.

Use this when:

- dbSNP frequency archives are too large for a single web server run
- HPC should process each archive independently and write portable artifacts
- Parquet files should be copied to AWS with `scp` or `rsync` before the final
  serving index build
- resume checkpoints and manifests are needed for archive-level handoff

Export one archive to Parquet:

```bash
python scripts/dataset_specific_scripts/unified/build_dbsnp_frequency_parquet.py \
  --verbose \
  export-archive \
  --archive raw_data/dbsnp/dbsnp_frequency_data_batch1.tar.gz \
  --output-root analyzed_data/dbsnp_frequency \
  --batch-size 250000 \
  --progress-interval 60
```

Export the existing legacy HBP dbSNP CSV artifacts as a separate provenanced
source:

```bash
python scripts/dataset_specific_scripts/unified/build_dbsnp_frequency_parquet.py \
  --verbose \
  export-legacy \
  --legacy-dbsnp-root analyzed_data/dbSNP \
  --output-root analyzed_data/dbsnp_frequency \
  --batch-size 250000
```

Import the handoff artifacts into the final DuckDB index on the serving host:

```bash
python scripts/dataset_specific_scripts/unified/build_dbsnp_frequency_parquet.py \
  --verbose \
  build-duckdb \
  --parquet-root analyzed_data/dbsnp_frequency \
  --output-db datamart/dbsnp_frequency.duckdb \
  --replace \
  --threads 8 \
  --memory-limit 32GB \
  --temp-directory datamart/duckdb_tmp
```

HPC Slurm export example:

```bash
sbatch --array=0-2 \
  --export=ALL,DATAHUB_ROOT=/geode2/home/u050/kvand/BigRed200/DataHub,RAW_ROOT=/N/scratch/kvand/hbp/raw_data/dbsnp,OUTPUT_ROOT=/N/scratch/kvand/hbp/analyzed_data/dbsnp_frequency,BATCH_SIZE=250000 \
  scripts/slurm/build_dbsnp_frequency_parquet.sbatch
```

After Slurm export finishes, copy only the handoff directory to AWS, not the
expanded raw archive contents:

```bash
rsync -av analyzed_data/dbsnp_frequency/ ubuntu@SERVER:/data/DataHub/analyzed_data/dbsnp_frequency/
```

Operational notes:

- `export-archive` writes records under `records/`, a checkpoint under
  `checkpoints/`, and a manifest under `manifests/`
- rerun export commands with the default resume behavior to continue completed
  archive members; use `--reset` only for an intentional rebuild of that shard
- `--limit-members` provides a bounded smoke test before a full archive run
- keep Parquet manifests with release logs, but do not commit generated Parquet,
  DuckDB, checkpoint, raw archive, or analyzed artifact files

### `scripts/dataset_specific_scripts/unified/build_dbsnp_frequency_index.py`

Build the DuckDB-backed population-frequency index from raw dbSNP frequency
archives and, by default, existing legacy HBP dbSNP CSV artifacts.

Use this when:

- population/ancestry context needs richer frequency evidence than the legacy
  chart CSVs
- new dbSNP archive batches have been placed under `raw_data/dbsnp`
- legacy and new frequency observations should remain separately provenanced
  instead of being silently collapsed

For production-scale archive batches, prefer
`build_dbsnp_frequency_parquet.py`. This direct builder is still useful for
small local runs, smoke tests, and environments where the raw archive streaming
and DuckDB import must happen on the same machine.

Example:

```bash
python scripts/dataset_specific_scripts/unified/build_dbsnp_frequency_index.py \
  --raw-root raw_data/dbsnp \
  --output-db datamart/dbsnp_frequency.duckdb \
  --legacy-dbsnp-root analyzed_data/dbSNP \
  --include-legacy \
  --verbose
```

Smoke-test with a bounded number of archive members:

```bash
python scripts/dataset_specific_scripts/unified/build_dbsnp_frequency_index.py \
  --raw-root raw_data/dbsnp \
  --output-db datamart/dbsnp_frequency.smoke.duckdb \
  --limit-members 5 \
  --reset \
  --verbose
```

Operational notes:

- checkpoint defaults to `<output-db>.checkpoint.json`
- rerun with the default `--resume` to continue from completed archive members
- use `--reset` only for an intentional rebuild
- `--skip-legacy` builds from new dbSNP archives only
- terminal progress and checkpoint progress are on by default; use
  `--no-progress` only for controlled noninteractive jobs

### `scripts/dataset_specific_scripts/unified/run_unified_pipeline.py`

Profile-driven orchestration for the unified pipeline across laptop/AWS/HPC.

`--step all` runs `working_init`, `mvp_ingest`, `legacy_ingest`, and `publish`.
The `working_init` step initializes the target lifecycle tables in the working
DuckDB before the current points-table ingest/publish stages run.

## Script philosophy

The scripts directory is intentionally operational. Business/scientific logic lives in `src/datahub/` when it can. Scripts compose that logic and add environment/runtime concerns such as CLI parsing, checkpoints, and scheduler integration.
