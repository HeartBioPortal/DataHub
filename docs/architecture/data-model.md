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
