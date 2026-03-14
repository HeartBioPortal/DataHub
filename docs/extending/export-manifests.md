# Export Manifest Framework

The export manifest framework defines what moves from unified/canonical data into analyzed outputs.

## Why this layer exists

Before export manifests, the repository could preserve or derive analyzed fields only through hardcoded publisher logic. That does not scale well when:

- new sources add new useful fields
- new charts need new derived outputs
- provenance must survive into analyzed artifacts
- the same analyzed semantics must be preserved in both `.json.gz` and serving DuckDB

## Core files

- `config/export_manifests/association/base.json`
- `config/export_manifests/association/overrides/`
- `src/datahub/export_manifest.py`
- `src/datahub/export_helpers.py`

## Manifest structure

The current association manifest includes:

- `manifest_id`
- `version`
- `dataset_types`
- `promoted_fields`
- `metadata_fields`
- `derived_fields`
- `publish_fields`
- `serving_fields`
- `source_overrides`

## Base + override model

The base manifest defines the common association export contract.

Source overrides are applied on top for source-specific differences. This keeps the common case centralized and source-specific exceptions explicit.

## Named helpers only

Derived fields reference registered helper IDs. Inline code or ad hoc expression snippets are intentionally not allowed in V1.

This keeps the system:

- auditable
- testable
- safe to evolve

## What the framework currently does

For association publication it now supports:

- helper-driven canonical phenotype path resolution
- helper-driven clinical-significance normalization
- source provenance bundling
- ancestry provenance bundling
- coverage stubs for future completeness accounting
- additive `_datahub` metadata blocks in published outputs

## Materialization policy

Broadly reusable fields belong in explicit schema columns only if they are needed for:

- querying
- deduplication
- filtering
- publish logic

Source-specific or lower-reuse fields should remain structured metadata.

## Design rule

Publication is the canonical place where analyzed semantics are defined. The serving builder should preserve those semantics, not invent a new scientific contract.
