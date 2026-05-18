# Structural Variant Schema

Documentation-only schema for HBP 3.0 structural-variant artifacts.

| Field | Description |
| --- | --- |
| `gene` | Gene overlapped by the structural variant. |
| `sv_id` | Source structural-variant identifier. |
| `sv_type` | Deletion, duplication, insertion, inversion, CNV, or source-provided type. |
| `coordinates` | Genomic coordinates and assembly. |
| `event_length` | Event length when available. |
| `clinical_significance` | Source clinical-significance assertion. |
| `gene_overlap` | Gene overlap annotation. |
| `transcript_overlap` | Transcript overlap annotation when available. |
| `exon_overlap` | Exon overlap annotation when available. |
| `source_db` | Source database, such as dbVar. |
| `study_or_submission` | Source study or submission ID. |
| `provenance` | Source and transformation provenance object. |
