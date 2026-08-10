# Data Sources

This page is the human-readable source inventory for HeartBioPortal DataHub. It
covers sources used by executable adapters and sources registered for planned
integration. Release participation, adapter status, and catalog registration are
different claims and are reported separately.

## How to read source status

| Status | Meaning |
| --- | --- |
| **Integrated** | DataHub has an executable source adapter or pipeline integration. This does not mean the source is included in every release. |
| **Catalog only** | DataHub records source identity, access, license, and intended modalities, but no canonical adapter is active yet. |
| **Imported or external handoff** | HBP consumes an artifact prepared outside the canonical source-adapter path. Drug payloads and HCG/HCG-KG exports currently use this pattern. |
| **Derived** | DataHub calculates the layer from other versioned HBP artifacts rather than ingesting a new external database. |

A source manifest is not proof that its data occur in the current production
build. Release manifests, build metadata, and artifact QA determine what was
actually published.

## Sources represented by current HBP data layers

| HBP layer | Sources or handoffs | Current role |
| --- | --- | --- |
| Association and phenotype evidence | Million Veteran Program summary statistics when authorized; NHGRI-EBI GWAS Catalog when selected; versioned HBP legacy CVD and trait inputs | Canonical associations, phenotype paths, p-values, variant identifiers, annotations, and variant-index artifacts. Inclusion is release-specific. |
| Variant annotation | NCBI ClinVar, Ensembl Variation, NCBI dbSNP, and retained legacy annotations | Clinical significance, consequence, variation type, identifiers, coordinates, and provenance where available. |
| Population-frequency context | NCBI dbSNP frequency exports carrying 37 source-provided study labels, plus separately provenanced legacy rows | Source-specific REF/ALT observations joined to association-selected rsIDs. The verified 2026-05-09 handoff contains 594,285,057 observations across 18,097,122 rsIDs. See [Population Frequency Schema](../schemas/population_frequency.md) for every study label and interpretation rules. |
| Structural variants | NCBI dbVar nstd102/ClinVar SV seed; dbVar nstd229/TOPMed SV call set | Gene-centered SV payloads with event, study, interval, type, phenotype, and clinical assertion fields where supplied. |
| Gene profiles | HGNC, NCBI Gene, UniProtKB, Gene Ontology Annotation, and optional HBP protein context | Gene identity, summaries, protein metadata, GO annotations, cross-references, and provenance. Reactome IDs currently arrive through UniProtKB cross-references. |
| Protein context | Ensembl, EMBL-EBI Proteins API, InterPro, and UniProtKB cross-references | Isoforms, translated exons, protein features, domains, accessions, and amino-acid coordinates. |
| Expression | Imported legacy payloads and source-accession-preserving public disease-versus-control studies selected for expression v3 | Differential-expression rows and gene-phenotype summaries. GEO, ArrayExpress, Expression Atlas, and GTEx are cataloged separately; catalog presence does not establish release inclusion. |
| Drugs and compounds | Open Targets Platform GraphQL API and licensed DrugBank 5.1.12 data | Imported gene-drug records. A `merged` record combines complementary fields from both sources; it is not a third source or pooled score. Canonical adapters remain catalog-only. |
| Clinical guideline context | Official ACC/AHA and ESC documents processed by external HCG/HCG-KG projects | Versioned graph handoffs consumed by HBP. HCG/HCG-KG own extraction and graph construction. |
| Shared genetic architecture | DataHub association and variant-index artifacts | Derived cross-phenotype variant overlap; no additional external database is ingested. |

## Executable source integrations

These 11 manifests currently declare `integration_status: integrated`.
Executable integration does not guarantee inclusion in every deployed release.

| Source | Data category | Modalities | Access | Recorded license/terms |
| --- | --- | --- | --- | --- |
| [EMBL-EBI Proteins API](https://github.com/HeartBioPortal/DataHub/blob/main/config/sources/ebi_proteins.json) | Protein context | proteomics, protein_feature, annotation | api | UniProt/EMBL-EBI terms of use |
| [Ensembl Protein and Transcript Context](https://github.com/HeartBioPortal/DataHub/blob/main/config/sources/ensembl_protein_context.json) | Protein context | genomics, transcriptomics, proteomics, annotation | api | Ensembl Terms of Use |
| [Ensembl Variation](https://github.com/HeartBioPortal/DataHub/blob/main/config/sources/ensembl_variation.json) | Population and reference variation | genomics, population_frequency, annotation | hybrid | Ensembl Terms of Use |
| [Gene Ontology Annotation](https://github.com/HeartBioPortal/DataHub/blob/main/config/sources/goa.json) | Ontologies and standards | ontology, functional_annotation, evidence | hybrid | Gene Ontology Consortium data license |
| [HGNC](https://github.com/HeartBioPortal/DataHub/blob/main/config/sources/hgnc.json) | Ontologies and standards | gene_identity, nomenclature, cross_reference | hybrid | HGNC data reuse terms |
| [InterPro](https://github.com/HeartBioPortal/DataHub/blob/main/config/sources/interpro.json) | Protein context | proteomics, protein_domain, annotation | api | EMBL-EBI terms of use |
| [NCBI ClinVar](https://github.com/HeartBioPortal/DataHub/blob/main/config/sources/clinvar.json) | Clinical variant interpretation | genomics, clinical_annotation | hybrid | NCBI Disclaimer and Copyright Notice |
| [NCBI dbVar](https://github.com/HeartBioPortal/DataHub/blob/main/config/sources/dbvar.json) | Population and reference variation | genomics, structural_variant | download | NCBI Disclaimer and Copyright Notice |
| [NCBI Gene](https://github.com/HeartBioPortal/DataHub/blob/main/config/sources/ncbi_gene.json) | Ontologies and standards | gene_summary, gene_identity, cross_reference | hybrid | NCBI public domain and usage guidelines |
| [NHGRI-EBI GWAS Catalog](https://github.com/HeartBioPortal/DataHub/blob/main/config/sources/gwas_catalog.json) | GWAS and statistical genetics | genomics, association | download | EMBL-EBI Terms of Use |
| [UniProtKB](https://github.com/HeartBioPortal/DataHub/blob/main/config/sources/uniprotkb.json) | Protein context | protein_function, protein_feature, annotation | hybrid | Creative Commons Attribution 4.0 International |

## Registered sources awaiting canonical integration

These 50 manifests declare `integration_status: catalog_only`. They are part
of the source registry and roadmap, but must not be described as production data
without release-level evidence.

### Bulk transcriptomics and molecular QTL

| Source | Modalities | Access | Recorded license/terms |
| --- | --- | --- | --- |
| [ArrayExpress](https://github.com/HeartBioPortal/DataHub/blob/main/config/sources/arrayexpress.json) | bulk_rna, functional_genomics | download | EMBL-EBI terms of use |
| [eQTL Catalogue](https://github.com/HeartBioPortal/DataHub/blob/main/config/sources/eqtl_catalogue.json) | eqtl, sqtl, molecular_qtl | hybrid | EMBL-EBI terms of use |
| [Expression Atlas](https://github.com/HeartBioPortal/DataHub/blob/main/config/sources/expression_atlas.json) | bulk_rna, single_cell, differential_expression | hybrid | EMBL-EBI terms of use |
| [GTEx Portal](https://github.com/HeartBioPortal/DataHub/blob/main/config/sources/gtex.json) | bulk_rna, eqtl, tissue_expression | hybrid | GTEx terms of use |
| [NCBI GEO](https://github.com/HeartBioPortal/DataHub/blob/main/config/sources/geo.json) | bulk_rna, single_cell, epigenomics | hybrid | NCBI Disclaimer and Copyright Notice |

### Clinical variant interpretation

| Source | Modalities | Access | Recorded license/terms |
| --- | --- | --- | --- |
| [ClinGen](https://github.com/HeartBioPortal/DataHub/blob/main/config/sources/clingen.json) | clinical_annotation, genomics, evidence | hybrid | ClinGen terms and attribution policy |

### Cohorts and CVD-focused portals

| Source | Modalities | Access | Recorded license/terms |
| --- | --- | --- | --- |
| [dbGaP](https://github.com/HeartBioPortal/DataHub/blob/main/config/sources/dbgap.json) | genomics, clinical, cohort | hybrid | NCBI dbGaP access and data use policy |
| [FinnGen](https://github.com/HeartBioPortal/DataHub/blob/main/config/sources/finngen.json) | genomics, association, cohort | download | FinnGen data access terms |
| [NHLBI BioData Catalyst](https://github.com/HeartBioPortal/DataHub/blob/main/config/sources/biodata_catalyst.json) | genomics, cohort, multimodal | hybrid | NHLBI BioData Catalyst data access terms |
| [UK Biobank](https://github.com/HeartBioPortal/DataHub/blob/main/config/sources/uk_biobank.json) | genomics, clinical, cohort | hybrid | UK Biobank access policy |

### Drugs, targets, and translational evidence

| Source | Modalities | Access | Recorded license/terms |
| --- | --- | --- | --- |
| [ChEMBL](https://github.com/HeartBioPortal/DataHub/blob/main/config/sources/chembl.json) | drug_target, compound_activity, assay | hybrid | EMBL-EBI terms of use |
| [ClinicalTrials.gov](https://github.com/HeartBioPortal/DataHub/blob/main/config/sources/clinicaltrials_gov.json) | clinical_trials, intervention, outcomes | api | ClinicalTrials.gov data use policy |
| [ClinPGx](https://github.com/HeartBioPortal/DataHub/blob/main/config/sources/clinpgx.json) | pharmacogenomics, clinical_annotation | download | ClinPGx terms of use |
| [DrugBank](https://github.com/HeartBioPortal/DataHub/blob/main/config/sources/drugbank.json) | drug_target, compound_annotation, mechanism | hybrid | DrugBank licensing terms |
| [Open Targets Platform](https://github.com/HeartBioPortal/DataHub/blob/main/config/sources/open_targets.json) | target_disease, evidence_integration, genetics | hybrid | Open Targets data licensing |
| [PharmGKB](https://github.com/HeartBioPortal/DataHub/blob/main/config/sources/pharmgkb.json) | pharmacogenomics, clinical_annotation, drug_gene | hybrid | PharmGKB terms of use |

### Epigenomics and regulatory evidence

| Source | Modalities | Access | Recorded license/terms |
| --- | --- | --- | --- |
| [ENCODE](https://github.com/HeartBioPortal/DataHub/blob/main/config/sources/encode.json) | epigenomics, regulatory, functional_genomics | api | ENCODE data use policy |
| [EpiMap](https://github.com/HeartBioPortal/DataHub/blob/main/config/sources/epimap.json) | epigenomics, enhancer, regulatory_annotation | download | EpiMap data usage terms |
| [Roadmap Epigenomics](https://github.com/HeartBioPortal/DataHub/blob/main/config/sources/roadmap_epigenomics.json) | epigenomics, chromatin_state, methylation | download | NIH Roadmap data usage terms |

### Metabolomics and lipidomics

| Source | Modalities | Access | Recorded license/terms |
| --- | --- | --- | --- |
| [Human Metabolome Database](https://github.com/HeartBioPortal/DataHub/blob/main/config/sources/hmdb.json) | metabolomics, compound_annotation, clinical_reference | download | HMDB license and terms of use |
| [LIPID MAPS](https://github.com/HeartBioPortal/DataHub/blob/main/config/sources/lipidmaps.json) | lipidomics, compound_annotation, pathway | download | LIPID MAPS terms of use |
| [MetaboLights](https://github.com/HeartBioPortal/DataHub/blob/main/config/sources/metabolights.json) | metabolomics, study_archive | hybrid | EMBL-EBI terms of use |
| [Metabolomics Workbench](https://github.com/HeartBioPortal/DataHub/blob/main/config/sources/metabolomics_workbench.json) | metabolomics, study_archive, assay_metadata | hybrid | Metabolomics Workbench data use policy |

### Ontologies and standards

| Source | Modalities | Access | Recorded license/terms |
| --- | --- | --- | --- |
| [Biolink Model](https://github.com/HeartBioPortal/DataHub/blob/main/config/sources/biolink_model.json) | semantic_model, knowledge_graph, standardization | download | Biolink Model license |
| [Experimental Factor Ontology](https://github.com/HeartBioPortal/DataHub/blob/main/config/sources/efo.json) | ontology, trait_mapping, standardization | download | Creative Commons Attribution 4.0 |
| [Human Phenotype Ontology](https://github.com/HeartBioPortal/DataHub/blob/main/config/sources/hpo.json) | ontology, phenotype_mapping, standardization | download | HPO data license |
| [MONDO Disease Ontology](https://github.com/HeartBioPortal/DataHub/blob/main/config/sources/mondo.json) | ontology, disease_mapping, standardization | download | CC-BY 4.0 |
| [NCI Thesaurus](https://github.com/HeartBioPortal/DataHub/blob/main/config/sources/ncit.json) | ontology, terminology, mapping | download | NCI terms of use |
| [Uberon Anatomy Ontology](https://github.com/HeartBioPortal/DataHub/blob/main/config/sources/uberon.json) | ontology, anatomy_mapping, standardization | download | CC-BY 4.0 |

### Pathways, interactions, and networks

| Source | Modalities | Access | Recorded license/terms |
| --- | --- | --- | --- |
| [BioGRID](https://github.com/HeartBioPortal/DataHub/blob/main/config/sources/biogrid.json) | protein_interaction, genetic_interaction, network | download | BioGRID terms of use |
| [IntAct](https://github.com/HeartBioPortal/DataHub/blob/main/config/sources/intact.json) | molecular_interaction, network, curated_evidence | hybrid | EMBL-EBI terms of use |
| [KEGG](https://github.com/HeartBioPortal/DataHub/blob/main/config/sources/kegg.json) | pathway, network, compound_annotation | hybrid | KEGG licensing terms |
| [Pathway Commons](https://github.com/HeartBioPortal/DataHub/blob/main/config/sources/pathway_commons.json) | pathway, interaction_network, knowledge_graph | api | Pathway Commons data source licenses |
| [Reactome](https://github.com/HeartBioPortal/DataHub/blob/main/config/sources/reactome.json) | pathway, network, knowledge_graph | hybrid | Reactome open license |
| [STRING](https://github.com/HeartBioPortal/DataHub/blob/main/config/sources/string_db.json) | protein_interaction, network, functional_association | api | STRING license terms |
| [WikiPathways](https://github.com/HeartBioPortal/DataHub/blob/main/config/sources/wikipathways.json) | pathway, community_curation, knowledge_graph | hybrid | Creative Commons license |

### Population and reference variation

| Source | Modalities | Access | Recorded license/terms |
| --- | --- | --- | --- |
| [1000 Genomes Project](https://github.com/HeartBioPortal/DataHub/blob/main/config/sources/thousand_genomes.json) | genomics, population_frequency, reference | download | 1000 Genomes data use policy |
| [gnomAD](https://github.com/HeartBioPortal/DataHub/blob/main/config/sources/gnomad.json) | genomics, population_frequency, reference | hybrid | gnomAD terms of use |
| [NCBI ALFA](https://github.com/HeartBioPortal/DataHub/blob/main/config/sources/alfa.json) | genomics, population_frequency, reference | hybrid | NCBI Disclaimer and Copyright Notice |
| [NCBI dbSNP](https://github.com/HeartBioPortal/DataHub/blob/main/config/sources/dbsnp.json) | genomics, reference, annotation | hybrid | NCBI Disclaimer and Copyright Notice |
| [TOPMed BRAVO](https://github.com/HeartBioPortal/DataHub/blob/main/config/sources/topmed_bravo.json) | genomics, population_frequency, reference | api | TOPMed / NHLBI data access policy |

### Protein context

| Source | Modalities | Access | Recorded license/terms |
| --- | --- | --- | --- |
| [MobiDB](https://github.com/HeartBioPortal/DataHub/blob/main/config/sources/mobidb.json) | proteomics, protein_disorder, annotation | api | MobiDB terms of use |

### Proteomics

| Source | Modalities | Access | Recorded license/terms |
| --- | --- | --- | --- |
| [CPTAC Data Portal](https://github.com/HeartBioPortal/DataHub/blob/main/config/sources/cptac.json) | proteomics, proteogenomics | hybrid | CPTAC / PDC data usage terms |
| [Human Protein Atlas](https://github.com/HeartBioPortal/DataHub/blob/main/config/sources/human_protein_atlas.json) | proteomics, tissue_expression, antibody_based | download | Human Protein Atlas terms of use |
| [PRIDE](https://github.com/HeartBioPortal/DataHub/blob/main/config/sources/pride.json) | proteomics, mass_spectrometry | hybrid | EMBL-EBI terms of use |
| [ProteomeXchange](https://github.com/HeartBioPortal/DataHub/blob/main/config/sources/proteomexchange.json) | proteomics, repository_index | hybrid | ProteomeXchange terms and partner repository licenses |

### Single-cell and spatial evidence

| Source | Modalities | Access | Recorded license/terms |
| --- | --- | --- | --- |
| [CZ CELLxGENE](https://github.com/HeartBioPortal/DataHub/blob/main/config/sources/cellxgene.json) | single_cell, rna_seq, cell_metadata | api | CELLxGENE terms of use |
| [HuBMAP](https://github.com/HeartBioPortal/DataHub/blob/main/config/sources/hubmap.json) | single_cell, spatial, multiomics | hybrid | HuBMAP data usage policy |
| [Human Cell Atlas](https://github.com/HeartBioPortal/DataHub/blob/main/config/sources/human_cell_atlas.json) | single_cell, rna_seq, cell_atlas | hybrid | Human Cell Atlas data use policy |
| [Single Cell Expression Atlas](https://github.com/HeartBioPortal/DataHub/blob/main/config/sources/single_cell_expression_atlas.json) | single_cell, rna_seq, atlas | hybrid | EMBL-EBI terms of use |

## Machine-readable inventories and update rule

- [`DATA_SOURCES.tsv`](https://github.com/HeartBioPortal/DataHub/blob/main/DATA_SOURCES.tsv) records release-facing source, access, licensing, artifact, and provenance information.
- [`DATA_SOURCES.md`](https://github.com/HeartBioPortal/DataHub/blob/main/DATA_SOURCES.md) summarizes source families by modality.
- [`config/sources/`](https://github.com/HeartBioPortal/DataHub/tree/main/config/sources) contains one manifest per registered source.

When a source is added, removed, upgraded, or integrated, update its manifest
and this page together. A release must separately record the actual version,
access date, input checksum or source-record identity, license/terms,
transformation version, and published artifact. Unknown values remain explicit;
they must not be inferred from a catalog entry.
