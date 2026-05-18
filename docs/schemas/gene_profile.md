# Gene Profile Schema

Documentation-only schema for HBP 3.0 gene-profile artifacts.

| Field | Description |
| --- | --- |
| `gene` | HGNC-approved or HBP-normalized gene symbol. |
| `gene_id` | Source or HBP gene identifier. |
| `aliases` | Known aliases or previous symbols. |
| `summary` | Source-grounded gene summary. |
| `cross_references` | NCBI, Ensembl, UniProt, HGNC, and other IDs. |
| `functions` | GO, pathway, or curated functional annotations. |
| `sources` | Source provenance objects. |
| `hbp_build_version` | HBP build version. |

Required provenance: `source_name`, `source_version`, `source_url_or_endpoint`, `accessed_at`, `input_record_id`, `license`.
