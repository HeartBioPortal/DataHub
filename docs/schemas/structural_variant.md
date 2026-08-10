# Structural Variant Schema

DataHub publishes dbVar structural variants into the legacy gene-centered HBP
contract declared by `config/output_contracts/structural_variant_legacy.json`.
The top-level JSON object is keyed by gene symbol; it is not a flat table of
unique structural variants.

## Gene object

| Field | Meaning |
| --- | --- |
| `gene_location` | Source/Ensembl gene span when available. |
| `strand` | Gene strand when available. |
| `biotype` | Gene biotype. |
| `canonical_transcript` | Canonical transcript metadata; exon enrichment can add Ensembl exon arrays here. |
| `variants` | Structural-variant records overlapping the gene. |

Because one event can overlap multiple genes, the same source SV may appear in
multiple gene objects. Gene-level record count is therefore not a count of
unique structural variants.

## Variant object

| Field | Meaning |
| --- | --- |
| `variant_id` | Source SV identifier. |
| `study_id` | dbVar study/submission identifier, such as nstd102 or nstd229 context. |
| `variant_type` | Source call/region type normalized to lowercase when available. |
| `phenotype` | Source phenotype labels, split from the dbVar field without imposing an association phenotype hierarchy. |
| `clinical_significance` | Source assertion when supplied; missing does not mean benign. |
| `assembly_name` | Source genome assembly. |
| `variant_region` | Genomic `start-end` interval. Chromosome is represented by the containing gene/source context in the legacy contract. |

The legacy payload does not store a separate `event_length`, per-variant
`transcript_overlap`, or per-variant `exon_overlap` field. The optional exon
enrichment augments the gene's canonical transcript structure; consumers can
compare variant intervals with those exon coordinates.

## Merge and deduplication

When nstd229 output is merged with an nstd102 seed, DataHub deduplicates within
each gene by the tuple:

```text
variant_id + study_id + variant_region + variant_type
```

This preserves study-specific records and does not claim that identifiers from
different submissions are biologically equivalent.

## Provenance and interpretation

Build reports preserve input paths, row counts, filtered/skipped-row counts,
records emitted, genes written, and variants written. The legacy payload itself
has limited provenance, so `variant_id`, `study_id`, `assembly_name`, build
reports, and release manifests must travel together.

Structural-variant presence is not evidence of pathogenicity. Clinical
significance and phenotype values are source annotations and may be absent,
submission-specific, or not directly comparable across studies.
