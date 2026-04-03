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

Gene identifiers are also sanity-filtered before publication. DataHub does not publish analyzed outputs for rows whose `gene_id` is clearly not gene-like, such as bare numeric values that leaked in from malformed input rows or shifted columns. The current rule is intentionally permissive: a publishable `gene_id` must contain at least one letter.

### 5. Publication

The association publisher emits the legacy-compatible payload shape that the older HeartBioPortal stack expects.

This shape includes:

- phenotype-level association payloads
- overall gene-level payloads
- axes such as variation type, clinical significance, and most severe consequence
- ancestry payloads grouped by population label

## Scientific counting semantics

Association chart axes are **variant-centric**, not raw-row-centric.

That means the unit of counting for:

- variation type (`vc`)
- most severe consequence (`msc`)
- clinical significance (`cs`)

is the canonical `variant_id` / rsID for the published scope.

### Why this rule exists

The same biological variant can appear multiple times in upstream data because:

- the same rsID is reported by multiple sources
- the same rsID appears in repeated rows within one phenotype dataset
- the same rsID is observed across multiple ancestry rows
- the same rsID survives source-priority selection but still has repeated record representations before publication

If publication simply counts rows, the charts inflate category totals and stop representing unique variant evidence. That is scientifically misleading for category summaries such as variation type.

### Per-phenotype counting rule

Within a single published phenotype entry, DataHub first collapses records by `variant_id`.

If more than one record exists for the same variant inside that phenotype bucket, DataHub keeps the best representative record using the smallest available `p_value`.

After that representative selection step, `vc`, `msc`, and `cs` are counted once per variant.

### Overall gene-level counting rule

The overall payload is also variant-centric.

DataHub does **not** compute overall axis counts by summing already-aggregated phenotype counters. Instead, it re-evaluates the full gene record set, collapses by `variant_id`, selects the best representative record per variant, and then counts categories once per unique variant.

This prevents the same rsID from being counted repeatedly across phenotype buckets in the overall gene summary.

### Ancestry semantics

Ancestry payloads remain rsID-keyed. Population maps are allowed to keep one point per variant per population label. This is different from chart-axis counting:

- chart axes answer: "how many unique variants fall into this category?"
- ancestry answers: "what per-variant population values are available?"

Those are intentionally different analytical questions.

## Axis normalization semantics

Association category axes are normalized before publication so equivalent source labels collapse into a coherent chart vocabulary.

Examples:

- `indel` and `INDEL` become `INDEL`
- `Missense_Variant` and `missense_variant` become `missense variant`
- list-like clinical significance labels are reduced to a canonical category using the configured priority order

This normalization is part of the analyzed scientific contract. It is not a frontend cleanup step.

## What publication intentionally does not preserve

The published `.json` / `.json.gz` outputs are analyzed artifacts, not a raw record dump.

They intentionally preserve:

- canonicalized phenotype-level summaries
- canonicalized overall summaries
- ancestry point payloads
- additive `_datahub` metadata

They intentionally do **not** preserve every raw row or every intermediate duplicate representation from upstream sources.

### 6. Serving artifact build

The serving builder can read the published outputs and create a compact DuckDB serving artifact. This is downstream of publication.

For large full-dataset builds, the serving builder streams payload rows into DuckDB in batches rather than collecting the entire association corpus in Python memory first. This keeps the artifact build operationally feasible on HPC and medium-memory servers without changing the analyzed contract.

If the published outputs were just regenerated from a clean canonical pipeline and no additional normalization is needed, the builder also supports a fast path through `--trust-published-payloads`. In that mode it stores published association/overall JSON text directly instead of reparsing and renormalizing every file. This is substantially faster, but it should only be used when those published outputs are already known to be canonical.

## Why publication is still needed even with DuckDB

The point of the publication stage is not just file creation. It is the point where DataHub defines the analyzed contract. The serving artifact should preserve that contract, not replace the meaning of it.

## Current additive metadata path

The association export manifest framework adds reserved `_datahub` metadata blocks to published outputs. These blocks are additive and allow the project to carry forward provenance and future coverage fields without breaking existing payload consumers.
