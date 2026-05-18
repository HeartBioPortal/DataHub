# Guideline Signal Schema

Documentation-only schema for HBP 3.0 guideline-derived signals consumed from HCG or HCG-KG.

| Field | Description |
| --- | --- |
| `gene` | Gene symbol linked to guideline context. |
| `guideline_id` | Guideline source identifier. |
| `source_document` | Source document title/path/URL. |
| `recommendation_id` | Recommendation identifier when available. |
| `excerpt_or_snippet` | Source-grounded snippet when redistribution is allowed. |
| `evidence_class` | Class of recommendation. |
| `evidence_level` | Level of evidence. |
| `condition` | Disease or condition context. |
| `biomarker` | Biomarker context. |
| `drug_or_intervention` | Drug or intervention context. |
| `relationship_type` | Graph or extraction relationship label. |
| `provenance` | Source, extraction, and licensing provenance. |

Guideline-derived signals provide context only and are not automated medical advice or direct clinical actionability.
