# Module Schemas

These pages are documentation schemas for DataHub modules and published
artifacts. They complement machine-validated JSON Schemas under
`config/schemas/`.

For each module, we keep the human-facing contract in this section:

- what each output table or payload represents
- what each field means
- what provenance must be preserved
- which fields are safe for portal display
- which limitations must be reviewed before production use

When we add a new module or a new published artifact shape, we add or update a
page here as part of the same change. The goal is that a future DataHub run can
be understood from the artifact and its schema page without relying on chat
history or source-code archaeology.

Current module schema pages:

- [Expression v3](expression_v3.md)
- [Gene Profile](gene_profile.md)
- [Protein Context](protein_context.md)
- [Structural Variant](structural_variant.md)
- [Population Frequency](population_frequency.md)
- [Drug Discovery](drug_discovery.md)
- [Guideline Signal](guideline_signal.md)

