# Data Sources Summary

`DATA_SOURCES.tsv` is a broader engineering inventory that includes active sources, imported handoffs, release candidates, and catalog entries awaiting confirmation. It is not by itself evidence that every listed source is present in production. This companion file summarizes source families and states current inclusion limits.

## Association and phenotype evidence

The current association artifacts normalize HBP legacy CVD/trait layers and Million Veteran Program summary-statistics inputs into canonical gene, variant, phenotype, p-value, ancestry, consequence, clinical-significance, and provenance fields. The final artifacts are association JSON/JSON.GZ payloads, variant-index payloads, phenotype rollups, and serving DuckDB tables. We preserve input file, source dataset, source version, phenotype path, rsID/variant ID, p-value, genome build, and transformation notes in source provenance. Controlled or non-public inputs are not redistributed.

## Population-frequency context

The verified 2026-05-09 population-frequency build contains 594,285,057 source-specific observations across 18,097,122 distinct rsIDs and 37 source-provided study labels. Those labels include 1000Genomes, 1000Genomes_30X, ALFA, gnomAD/gnomAD v4 exome and genome collections, TopMed, PAGE, HGDP-CEPH, HapMap, ExAC, SGDP, 38KJPN, and additional regional or study collections listed in `docs/schemas/population_frequency.md`. DataHub preserves study, population, source group, REF/ALT allele and frequency, sample size, build, archive/member, BioProject/BioSample, and raw allele strings. `population_group` remains source-provided; it is not silently converted into a harmonized ancestry category. The datamart has no disease or phenotype field. HBP joins association-selected rsIDs to frequency observations at query time.

## Variant annotation

Current association and variant-index artifacts preserve rsID/variant IDs and source-carried clinical significance, variation type, consequence, source record IDs, genome build, and transformation details where supplied by MVP or legacy inputs. Executable or cataloged dbSNP, ClinVar, Ensembl Variation, and ClinGen manifests do not establish that every deployed row was freshly enriched from those services.

## Structural-variant evidence

Structural-variant evidence is currently represented by dbVar nstd102/ClinVar structural-variant seed payloads and dbVar nstd229/TOPMed structural-variant call-set artifacts. The legacy output retains variant ID, study/submission, variant type, phenotype, clinical significance when present, assembly, and variant interval. Optional exon enrichment adds canonical-transcript exon coordinates at gene level. The local nstd229 report verifies 3,072,942 records, 3,040,582 variants, and 75,192 gene-level payloads. TOPMed-related licensing and redistribution constraints require final review before public archival of source or derived bulk artifacts.

## Protein context

Protein context connects variant associations to protein architecture through Ensembl gene/transcript/translation IDs, canonical and protein-coding isoforms, exon-to-protein coordinate mapping, RefSeq/UniProt cross-references, Ensembl features, EBI Proteins features, and InterPro domains/families/motifs/regions. DataHub harmonizes source-specific API records into gene-level protein-context payloads. Production totals around 66.9k isoforms and more than 3.2 million protein feature annotations require production QA confirmation before release.

## Gene profiles

Gene profiles assemble HGNC identity and nomenclature, NCBI Gene summaries, UniProtKB protein metadata, GOA annotations, and optional HBP protein-context evidence. Reactome IDs are currently carried through UniProtKB cross-references rather than a separate Reactome ingest in the gene-profile builder. Human Protein Atlas and ClinGen are not inputs to the current gene-profile implementation. Field-level provenance identifies which source supplied each payload section.

## Clinical guidelines / guideline graph links

Clinical guideline extraction and graph construction are owned by HCG and HCG-KG, not by the current DataHub pipeline. DataHub documents the handoff expected by HBP: versioned gene-guideline links, source documents, recommendations, excerpts where permitted, recommendation class, evidence level, related conditions, biomarkers, interventions, and graph relationship types. Extraction confidence, when present, describes extraction or relationship-assignment confidence; it is not a clinical evidence grade. Guideline context is not automated medical advice.

## Drug-discovery / Drugs & Compounds layer

The imported drugs and compounds layer combines Open Targets Platform GraphQL records with licensed DrugBank v5.1.12 records. A payload record labeled `merged` contains complementary fields from both sources for the same gene-molecule entry; it is not a third source or a pooled evidence score. Reproducible builds preserve the Open Targets query/variables/access date and the DrugBank release/license metadata. Raw DrugBank files are license-restricted and are not committed or archived. The upstream merge is not yet implemented as a canonical DataHub adapter, so both source manifests remain `catalog_only` even though imported per-gene HBP payloads exist.

## Expression and shared-architecture layers

DataHub supports two expression paths: imported legacy-compatible per-gene payloads and expression v3 rows from NCBI GEO studies GSE232911, GSE29532, and GSE7084. Expression v3 preserves study accession, sample groups, tissue/cell context, platform, effect size, raw and adjusted p-values, direction, method, and provenance at row level before generating gene-phenotype summaries. Shared genetic architecture is derived from cleaned gene-level CVD and trait variant sets and reports cross-phenotype rsID overlap. Both layers inherit source redistribution constraints and preserve their build method and input provenance.
