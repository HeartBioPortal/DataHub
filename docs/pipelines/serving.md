# Serving Artifacts

## Why serving artifacts exist

The raw or unified working DuckDB is not the same thing as a runtime-serving artifact.

The working store is optimized for:

- ingest
- merge
- deduplication
- analysis
- reconstruction of published outputs

The serving artifact is optimized for:

- compact read-only access
- predictable backend retrieval
- avoiding large in-memory Redis preloads

## Current serving artifact

The main serving artifact is a compact DuckDB database built by:

- `scripts/dataset_specific_scripts/unified/build_association_serving_duckdb.py`

It currently contains tables such as:

- `association_gene_payloads`
- `overall_gene_payloads`
- `expression_gene_payloads`
- `sga_gene_payloads`
- `gene_catalog`
- `build_metadata`
- `secondary_analysis_metadata`

![Association serving DuckDB schema](../assets/association_serving_duckdb_schema.svg)

## Serving contract

The serving DuckDB contract is declared in:

- `config/output_contracts/association_serving_duckdb.json`

The contract names the primary runtime tables, required columns, query
expectations, and compatibility notes. The most important rule is that serving
payload JSON preserves published semantics; it does not reinterpret association
or overall payloads.

## Important design rule

The serving builder is downstream of publication.

That means:

- it should preserve analyzed semantics defined by publication
- it should not become a second independent scientific transformation layer

The export manifest framework reinforces this rule.

Secondary analyses follow the same principle:

- primary association publication defines the base analyzed contract
- imported or derived secondary analyses can extend the serving artifact later
- those extensions should update only their own tables and serving metadata
- they should not silently rebuild or reinterpret association payloads

## Build metadata

The serving DB records the manifest ID and version used at build time. This allows debugging of:

- which analyzed export contract produced the artifact
- whether a runtime issue is due to artifact staleness vs code mismatch

Incremental secondary-analysis updates are recorded separately in:

- `secondary_analysis_metadata`

This distinguishes:

- base serving artifact construction
- later in-place attachment of secondary analyses such as expression and SGA

The serving builder can also write a DataHub QA report after the DB is built:

```bash
datahub-build-serving-duckdb \
  --input-root /data/hbp/analyzed_data_unified \
  --db-path /data/hbp/datamart/association_serving.duckdb \
  --qa-report-json /data/hbp/state/association_serving.qa.json
```

That report includes row counts for serving tables, the serving DB checksum,
published payload counts, and source-catalog integration status.

## Why this is better than Redis-only bulk load

A compact DuckDB serving artifact is:

- more portable
- less memory-heavy
- easier to version
- better aligned with DataHub's role as an artifact-producing platform

Redis can still exist as a cache or compatibility layer, but it should not be the only production story for large-scale analyzed outputs.

## Incremental secondary-analysis updates

If an existing serving DB already contains association and overall payloads, DataHub can now attach secondary analyses later without rebuilding the whole DB.

That flow is intended for cases such as:

- building the primary serving DB on BigRed
- copying it to AWS
- deriving or importing secondary artifacts later
- updating the production serving DB in place

The current incremental update surface is:

- `scripts/dataset_specific_scripts/unified/run_secondary_analyses.py apply`

See the dedicated secondary-analysis pipeline documentation for details.
