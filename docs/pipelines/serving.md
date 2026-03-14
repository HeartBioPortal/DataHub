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

## Important design rule

The serving builder is downstream of publication.

That means:

- it should preserve analyzed semantics defined by publication
- it should not become a second independent scientific transformation layer

The export manifest framework reinforces this rule.

## Build metadata

The serving DB records the manifest ID and version used at build time. This allows debugging of:

- which analyzed export contract produced the artifact
- whether a runtime issue is due to artifact staleness vs code mismatch

## Why this is better than Redis-only bulk load

A compact DuckDB serving artifact is:

- more portable
- less memory-heavy
- easier to version
- better aligned with DataHub's role as an artifact-producing platform

Redis can still exist as a cache or compatibility layer, but it should not be the only production story for large-scale analyzed outputs.
