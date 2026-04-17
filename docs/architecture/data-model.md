# Data Model

## Canonical record

The central in-memory object is `CanonicalRecord` in `src/datahub/models.py`.

It intentionally keeps a compact, reusable core:

- dataset identity
- dataset type
- source
- gene identifier
- variant identifier
- phenotype
- disease category
- major chart axes
- p-value
- ancestry values
- structured metadata

## Working DuckDB lifecycle tables

The target DataHub lifecycle uses one physical working DuckDB with separate logical zones.

The first implemented tables are:

- `raw_release_registry`
  - one row per source-native raw release
  - stores source, release, modality, URI/license notes, and metadata
- `raw_file_inventory`
  - one row per raw file in a release
  - stores ordered source columns, schema fingerprint, file size, optional checksum, and detected delimiter
- `schema_drift_reports`
  - one row per inspected file
  - records whether a source release is compatible, compatible with additions, breaking, or unchecked
- `source_normalized_association`
  - stable HBP association input rows with source/release/row/file provenance
  - does not perform source priority, final ontology mapping, or variant-centric counting
- `analysis_ready_association`
  - HBP analysis-facing association evidence
  - normalizes identifiers and carries fields required by publication/secondary analysis workflows

The unified profile runner initializes these lifecycle tables through
`working_init` before the current ingest and publish steps when `--step all` is
used. This keeps the lifecycle model on the normal execution path while the
points-table migration continues.

The naming is intentional:

- `source_normalized_*` means "raw source translated into stable HBP columns"
- `analysis_ready_*` means "scientifically interpretable evidence ready for HBP analysis"

## Variant identity semantics

For association publication, `variant_id` is the scientific identity used for deduplication and counting.

This is typically an rsID in current HeartBioPortal association data, but the code treats it generically as the canonical variant identifier.

That means:

- repeated rows for the same `variant_id` are not independent variants
- chart axes such as variation type, clinical significance, and most severe consequence are counted at unique-variant granularity
- when multiple records share the same `variant_id`, publication selects one representative record for counting using the smallest available `p_value`

This is a scientific modeling decision, not just an implementation detail. A row and a variant are not the same thing.

## Why the core model is small

The model is deliberately not a giant union of every possible source field. Instead:

- highly reusable fields get promoted into explicit attributes
- source-specific or lower-reuse values live under `metadata`

This keeps the canonical model stable while allowing the repository to integrate more sources over time.

## Metadata philosophy

Structured metadata is not a dumping ground. It is for values that are important but not yet justified as global schema columns.

Examples include:

- source-specific provenance
- source-file references
- source ancestry identity
- modality-specific details that are not broadly queried yet

## Ancestry model

DataHub distinguishes between:

- canonical ancestry group used in charts or merged outputs
- source ancestry identity preserved for provenance when available

That distinction matters because different sources use different ancestry vocabularies, and collapsing too early destroys useful information.

## Phenotype model

Phenotypes are represented both as leaves and, when available, as canonical paths from `config/phenotype_tree.json`. This allows:

- stable filtering and grouping
- rollups
- consistent public labels
- source-agnostic hierarchy reconstruction

## Export manifest model

The export manifest layer defines which fields move from unified/canonical data into analyzed artifacts.

It separates three concepts:

- **promoted fields**: explicit reusable fields expected to exist in the working schema or publish projection
- **metadata fields**: values preserved into structured metadata
- **derived fields**: additive fields created by named helpers and attached to analyzed outputs

This is the boundary between raw/unified truth and analyzed publication semantics.
