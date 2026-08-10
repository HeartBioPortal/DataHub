# Protein Context Schema

Protein-context artifacts provide the isoform and protein-feature coordinate
system used by the Protein Consequence Viewer. Their scientific axis is amino-acid
position on a selected translated isoform, not genomic position.

## Artifact layout

New standardized builds write:

```text
secondary_analyses/final/protein_context/
  genes/<GENE>.json.gz
  metadata.json                  # or partition metadata files for sharded runs
```

Older deployment trees may contain flat `protein_context/<GENE>.json.gz` files.
That layout is retained only for consumer compatibility; new DataHub generation
uses the manifest-driven `genes/` directory.

## Gene payload

| Field | Meaning |
| --- | --- |
| `gene` | Normalized gene symbol. |
| `ensembl_gene_id` | Ensembl gene ID resolved for the symbol. |
| `species` | Ensembl species key, normally `homo_sapiens`. |
| `isoforms` | Selected protein-coding transcript/translation records. |
| `isoform_hints` | Transcript or protein identifiers and lengths inferred from existing Protein Consequence Viewer artifacts. |
| `hint_transcript_map` | RefSeq-to-Ensembl mappings resolved during generation. |
| `source_status` | Per-source success, skip, or error status. |

By default DataHub selects at most 12 protein-coding isoforms, prioritizing
isoforms matched to existing viewer hints, Ensembl canonical transcripts, and
GENCODE primary transcripts. The limit is configurable and must be recorded with
the run metadata when changed.

## Isoform fields

Each isoform contains:

- Ensembl `transcript_id` and translated `protein_id`
- display name, RefSeq IDs, and UniProt accessions
- `canonical` and `gencode_primary` flags
- biotype, strand, genomic transcript span, and `length_aa`
- per-source status for Ensembl xrefs/features, EBI Proteins, and InterPro
- feature arrays: `exons`, `domains`, `transmembrane`, `low_complexity`,
  `disorder`, `repeats`, `sites`, `regions`, and `other_features`

The genomic start/end values describe the transcript span. They do not change
the coordinate system of the feature arrays.

## Feature record

A normalized feature contains:

| Field | Meaning |
| --- | --- |
| `start`, `end` | Inclusive amino-acid coordinates on this isoform. |
| `label` | Human-readable feature label. |
| `type` | Source feature type. |
| `source` | `ensembl_translation_exon`, `ensembl_protein_feature`, `ebi_proteins`, or `interpro`. |
| `accession` | Source feature or domain accession when available. |
| `description` | Source description when available. |
| `raw_type` | Original source type retained for audit. |

Categories are display groupings derived from feature type and text. They do not
replace the original `source`, `type`, or accession.

## Partial-source behavior

A gene payload can be valid but incomplete. Missing UniProt resolution can skip
EBI Proteins and InterPro while Ensembl transcript/exon context remains usable.
`source_status`, per-gene reports, and metadata distinguish a true empty result
from an API failure or an intentionally disabled source.
