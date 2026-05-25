# Protein Context Schema

Documentation-only schema for HBP 3.0 protein-context artifacts.

| Field | Description |
| --- | --- |
| `gene` | Gene symbol. |
| `ensembl_gene_id` | Ensembl gene identifier. |
| `transcripts` | Transcript and translation records. |
| `canonical` | Whether the transcript/isoform is canonical when known. |
| `protein_length` | Amino-acid length. |
| `translation_exons` | Exon-to-protein coordinate mapping. |
| `refseq_xrefs` | RefSeq cross-references. |
| `uniprot_xrefs` | UniProt cross-references. |
| `features` | Ensembl, EBI Proteins, UniProt, and InterPro protein features. |
| `provenance` | Source and transformation provenance object. |

Protein features include feature type, source, start/end amino-acid coordinates, IDs, labels, and source license where available.
