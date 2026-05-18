# Data Sources Summary

`DATA_SOURCES.tsv` is the machine-readable source inventory for the HBP 3.0 NAR release. This companion file summarizes the source families by HBP layer.

## Association and phenotype evidence

DataHub normalizes association rows from HBP legacy CVD/trait layers, Million Veteran Program summary-statistics inputs when available, GWAS Catalog when included, and other association profiles into canonical gene, variant, phenotype, p-value, ancestry, consequence, clinical-significance, and provenance fields. The final artifacts are association JSON/JSON.GZ payloads, variant-index payloads, phenotype rollups, and serving DuckDB tables. Source provenance should preserve input file, source dataset, source version, phenotype path, rsID/variant ID, p-value, genome build, and transformation notes. Controlled or non-public inputs must not be redistributed.

## Population-frequency context

Population-frequency context is expected to combine source-specific allele-frequency observations from resources such as ALFA, gnomAD v4, 1000 Genomes, 1000 Genomes 30X, TOPMed-derived public frequency resources, PAGE, HGDP-CEPH, HapMap, ExAC, SGDP, and 38KJPN where present in the production build. DataHub harmonizes rsID, allele, population label, population group, sample size, study/resource, genome build, and source provenance into population-frequency datamarts. Production totals must be verified from QA before release; the approximate 594.3 million frequency observations across 18.1 million rsIDs and 37 resources are not hard-coded in `ARTIFACT_MANIFEST.tsv` because they were not locally verifiable.

## Variant annotation

Variant annotation uses dbSNP, ClinVar, Ensembl Variation, ClinGen where included, and source-specific legacy fields. DataHub preserves rsID/variant IDs as variant-level keys for chart aggregation and records clinical significance, variation type, most-severe-consequence-like fields, source record IDs, source dataset, genome build, and transformation steps. Source licenses follow the original providers.

## Structural-variant evidence

Structural-variant evidence is currently represented by dbVar nstd102/ClinVar structural-variant seed payloads and dbVar nstd229/TOPMed structural-variant call-set artifacts. DataHub normalizes source DB, study/submission, SV ID, SV type, coordinates, event length, clinical significance when present, gene overlap, transcript overlap, and exon overlap. The local nstd229 report verifies 3,072,942 records, 3,040,582 variants, and 75,192 gene-level payloads. TOPMed-related licensing and redistribution constraints require final review before public archival of source or derived bulk artifacts.

## Protein context

Protein context connects variant associations to protein architecture through Ensembl gene/transcript/translation IDs, canonical and protein-coding isoforms, exon-to-protein coordinate mapping, RefSeq/UniProt cross-references, Ensembl features, EBI Proteins features, and InterPro domains/families/motifs/regions. DataHub harmonizes source-specific API records into gene-level protein-context payloads. Production totals around 66.9k isoforms and more than 3.2 million protein feature annotations require production QA confirmation before release.

## Gene profiles

Gene profiles integrate nomenclature, gene summaries, protein cross-references, ontology/pathway membership, and curated source metadata from sources such as HGNC, NCBI Gene, UniProtKB, GOA/Gene Ontology, Reactome, Human Protein Atlas, and ClinGen where included. DataHub should preserve source IDs, source versions, access dates, cross-reference IDs, and source-specific licensing notes.

## Clinical guidelines / guideline graph links

Clinical guideline artifacts are generated primarily by HCG and HCG-KG. DataHub consumes release JSON, graph exports, or vector/serving artifacts when present and links genes to guideline snippets, recommendations, evidence classes, evidence levels, conditions, biomarkers, drugs/interventions, and source documents. Guideline snippets are context for interpretation and are not automated medical advice.

## Drug-discovery / Drugs & Compounds layer

The drugs and compounds layer uses Open Targets Platform GraphQL API v4 and licensed DrugBank v5.1.12 inputs where available. DataHub should preserve the GraphQL query, variables, access date, source field names, molecule source, molecule ID, target ID, source action type, indication, trial phase/status, source version, and source license. The raw DrugBank full database is license-restricted and must not be committed or archived unless redistribution permission is confirmed. The reported drug-layer total of 17,128 gene-drug records across 1,839 gene files and 1,454 unique molecule names requires production QA confirmation.

## Expression and shared-architecture layers

Expression payloads are imported from source-specific expression resources and existing HBP payloads where present. Shared genetic architecture is derived from association artifacts by comparing gene-level CVD and trait variant overlap. Both layers inherit redistribution constraints from their source data and must preserve source dataset, source version, input file, transformation, and HBP build version.
