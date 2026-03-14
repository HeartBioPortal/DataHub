# Association Pipeline

This page covers the association-oriented path that produces legacy-compatible analyzed outputs.

## Inputs

Association inputs currently arrive from multiple paths:

- legacy aggregated CSVs
- legacy raw CVD / trait files
- MVP long-form aggregated phenotype files
- future association-like external sources through adapters

## The logical stages

### 1. Optional raw preparation

If a raw source is too irregular for direct adapter consumption, it first goes through a preparation profile.

Result:

- stable prepared rows
- auditable field arbitration from raw columns to a normalized intermediate schema

### 2. Canonical ingest

An adapter or ingest script maps the source into canonical records or a canonical DuckDB points table.

This is where source-specific parsing belongs.

### 3. Validation

Dataset contracts enforce minimum quality expectations:

- required fields
- missing-value policy
- exclusion or unknown-value handling for specific axes

### 4. Enrichment and normalization

Categorical axes, phenotype routing, and source-priority behavior are normalized here or immediately before publication.

### 5. Publication

The association publisher emits the legacy-compatible payload shape that the older HeartBioPortal stack expects.

This shape includes:

- phenotype-level association payloads
- overall gene-level payloads
- axes such as variation type, clinical significance, and most severe consequence
- ancestry payloads grouped by population label

### 6. Serving artifact build

The serving builder can read the published outputs and create a compact DuckDB serving artifact. This is downstream of publication.

## Why publication is still needed even with DuckDB

The point of the publication stage is not just file creation. It is the point where DataHub defines the analyzed contract. The serving artifact should preserve that contract, not replace the meaning of it.

## Current additive metadata path

The association export manifest framework adds reserved `_datahub` metadata blocks to published outputs. These blocks are additive and allow the project to carry forward provenance and future coverage fields without breaking existing payload consumers.
