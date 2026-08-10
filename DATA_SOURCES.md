# Data Sources Summary

`DATA_SOURCES.tsv` is the machine-readable source inventory for the HBP 3.0 NAR release. This companion file summarizes the source families by HBP layer.

## Association and phenotype evidence

DataHub normalizes association rows from HBP legacy CVD/trait layers, Million Veteran Program summary-statistics inputs when available, GWAS Catalog when included, and other association profiles into canonical gene, variant, phenotype, p-value, ancestry, consequence, clinical-significance, and provenance fields. The final artifacts are association JSON/JSON.GZ payloads, variant-index payloads, phenotype rollups, and serving DuckDB tables. We preserve input file, source dataset, source version, phenotype path, rsID/variant ID, p-value, genome build, and transformation notes in source provenance. Controlled or non-public inputs are not redistributed.

## Population-frequency context

The verified 2026-05-09 population-frequency build contains 594,285,057 source-specific observations across 18,097,122 distinct rsIDs and 37 source-provided study labels. Those labels include 1000Genomes, 1000Genomes_30X, ALFA, gnomAD/gnomAD v4 exome and genome collections, TopMed, PAGE, HGDP-CEPH, HapMap, ExAC, SGDP, 38KJPN, and additional regional or study collections listed in `docs/schemas/population_frequency.md`. DataHub preserves study, population, source group, REF/ALT allele and frequency, sample size, build, archive/member, BioProject/BioSample, and raw allele strings. `population_group` remains source-provided; it is not silently converted into a harmonized ancestry category. The datamart has no disease or phenotype field. HBP joins association-selected rsIDs to frequency observations at query time.

## Variant annotation

Variant annotation uses dbSNP, ClinVar, Ensembl Variation, ClinGen where included, and source-specific legacy fields. DataHub preserves rsID/variant IDs as variant-level keys for chart aggregation and records clinical significance, variation type, most-severe-consequence-like fields, source record IDs, source dataset, genome build, and transformation steps. Source licenses follow the original providers.

## Structural-variant evidence

Structural-variant evidence is currently represented by dbVar nstd102/ClinVar structural-variant seed payloads and dbVar nstd229/TOPMed structural-variant call-set artifacts. DataHub normalizes source DB, study/submission, SV ID, SV type, coordinates, event length, clinical significance when present, gene overlap, transcript overlap, and exon overlap. The local nstd229 report verifies 3,072,942 records, 3,040,582 variants, and 75,192 gene-level payloads. TOPMed-related licensing and redistribution constraints require final review before public archival of source or derived bulk artifacts.

## Protein context

Protein context connects variant associations to protein architecture through Ensembl gene/transcript/translation IDs, canonical and protein-coding isoforms, exon-to-protein coordinate mapping, RefSeq/UniProt cross-references, Ensembl features, EBI Proteins features, and InterPro domains/families/motifs/regions. DataHub harmonizes source-specific API records into gene-level protein-context payloads. Production totals around 66.9k isoforms and more than 3.2 million protein feature annotations require production QA confirmation before release.

## Gene profiles

Gene profiles assemble HGNC identity and nomenclature, NCBI Gene summaries, UniProtKB protein metadata, GOA annotations, and optional HBP protein-context evidence. Reactome IDs are currently carried through UniProtKB cross-references rather than a separate Reactome ingest in the gene-profile builder. Human Protein Atlas and ClinGen are cataloged sources, not inputs to the current gene-profile implementation. Field-level provenance identifies which source supplied each payload section.

## Clinical guidelines / guideline graph links

Clinical guideline extraction and graph construction are owned by HCG and HCG-KG, not by the current DataHub pipeline. DataHub documents the handoff expected by HBP: versioned gene-guideline links, source documents, recommendations, excerpts where permitted, recommendation class, evidence level, related conditions, biomarkers, interventions, and graph relationship types. Extraction confidence, when present, describes extraction or relationship-assignment confidence; it is not a clinical evidence grade. Guideline context is not automated medical advice.

## Drug-discovery / Drugs & Compounds layer

The imported drugs and compounds layer combines Open Targets Platform GraphQL records with licensed DrugBank v5.1.12 records. A payload record labeled `merged` contains complementary fields from both sources for the same gene-molecule entry; it is not a third source or a pooled evidence score. Reproducible builds preserve the Open Targets query/variables/access date and the DrugBank release/license metadata. Raw DrugBank files are license-restricted and are not committed or archived. The upstream merge is not yet implemented as a canonical DataHub adapter, so both source manifests remain `catalog_only` even though imported per-gene HBP payloads exist.

## Expression and shared-architecture layers

DataHub supports two expression paths: imported legacy-compatible per-gene payloads and the curation-gated expression v3 workflow for public disease-versus-control studies. Expression v3 preserves study accession, sample groups, tissue/cell context, platform, effect size, raw and adjusted p-values, direction, method, and provenance at row level before generating gene-phenotype summaries. Shared genetic architecture is derived from cleaned gene-level CVD and trait variant sets and reports cross-phenotype rsID overlap. Both layers inherit source redistribution constraints and preserve their build method and input provenance.
