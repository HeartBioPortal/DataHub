# Module Schemas

These pages are the human-readable contracts for DataHub artifacts and
companion evidence layers. They complement machine-validated schemas under
`config/schemas/`, but the two collections are not identical: some legacy or
externally owned payloads have a documented interface without a complete JSON
Schema in this repository.

Each page states:

- the scientific unit represented by a row or object
- field and coordinate semantics
- provenance and source ownership
- aggregation and join rules
- limitations that affect interpretation
- whether DataHub builds the artifact, imports it, or only documents a consumer contract

A source manifest marked `catalog_only` is not evidence that the source is in a
production build. Likewise, a documented interface does not imply that DataHub
owns the upstream extraction process.

Current contracts:

| Contract | DataHub role |
| --- | --- |
| [Expression v3](expression_v3.md) | Curation-gated builder and dedicated datamart. |
| [Gene Profile](gene_profile.md) | Versioned multi-source artifact builder. |
| [Protein Context](protein_context.md) | API-backed protein-coordinate artifact builder. |
| [Structural Variant](structural_variant.md) | dbVar normalization and legacy-compatible publisher. |
| [Population Frequency](population_frequency.md) | Parquet handoff and DuckDB index builder. |
| [Drug Discovery](drug_discovery.md) | Imported per-gene payload and source-provenance contract; upstream merge code is not yet a canonical DataHub adapter. |
| [Guideline Signal](guideline_signal.md) | Consumer/interface contract for HCG/HCG-KG outputs; graph extraction is externally owned. |
