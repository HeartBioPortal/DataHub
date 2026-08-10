# Guideline Signal Contract

HeartBioPortal consumes gene-centered guideline context produced by HCG and
HCG-KG. DataHub documents the handoff and provenance expectations, but it does
not currently own the PDF extraction, knowledge-graph construction, Neo4j graph
schema, or recommendation-level curation pipeline. Those versioned contracts
belong to the HCG/HCG-KG releases.

The portal payload can project graph entities and relationships into gene-level
summary and detail responses. The fields below are therefore conceptual response
fields, not one flat DataHub table.

## Core fields

| Field | Meaning |
| --- | --- |
| `gene` | Gene linked to guideline context. |
| `guideline_id` | Stable identifier for the source guideline document. |
| `source_document` | Guideline title, organization, year, URL, and document identifier where available. |
| `recommendation_id` | Recommendation or statement identifier. |
| `recommendation_text` | Recommendation text when permitted by source terms. |
| `excerpt_or_snippet` | Supporting source-grounded excerpt when redistribution is permitted. |
| `evidence_class` | Source-reported recommendation class, normalized for display. |
| `evidence_level` | Source-reported level/quality of evidence, normalized for display. |
| `condition` | Related disease or clinical condition entity. |
| `biomarker` | Related biomarker entity. |
| `drug_or_intervention` | Related therapy, procedure, or intervention entity. |
| `relationship_type` | Graph relationship or extraction relation used to connect the entities. |
| `provenance` | Guideline organization, title, year/version, source URL, excerpt location, extraction release, and licensing fields. |

## Evidence semantics

Recommendation class and level of evidence are clinical guideline metadata, not
variant-level clinical significance. Missing values mean that the class or level
was not captured or stated; they do not mean “no evidence.”

If an HCG/HCG-KG release includes extraction-confidence metadata, that value
represents confidence in the automated extraction or relationship assignment.
It is not the guideline's clinical evidence grade and must not be displayed as
one.

## Consumer rules

- Preserve ACC/AHA and ESC source identity instead of merging guideline documents.
- Keep recommendation text linked to its exact source document and supporting excerpt.
- Distinguish gene mentions and contextual relationships from direct clinical actionability.
- Version the imported graph/export so counts can be reproduced.
- Treat guideline-derived signals as interpretation context, not automated medical advice.
