# Data Sources

This page lists external databases, studies, licensed datasets, and versioned
handoffs that are demonstrably represented in current HeartBioPortal artifacts
or active DataHub pipelines. It intentionally excludes databases that exist only
as roadmap entries under `config/sources/`.

A source being registered in configuration does not mean HBP uses its data.
Release manifests, build metadata, source paths, and deployed artifact contents
are the evidence for inclusion.

## Current source summary

| Source | HBP layer | How it enters HBP |
| --- | --- | --- |
| Million Veteran Program (MVP) summary statistics | CVD association evidence | Authorized summary-statistics inputs are normalized into gene-variant-phenotype rows. Current variant-index rows identify `million_veteran_program` explicitly. Controlled raw data are not redistributed. |
| NHGRI-EBI GWAS Catalog-derived legacy CVD and trait files | Association and trait evidence | Versioned GWAS Catalog-derived snapshots are imported through the compatibility pipeline. They retain source files, phenotype labels, rsIDs, p-values, ancestry fields, consequences, and clinical annotations where supplied. Current rows identify the pipeline source as `legacy_cvd_raw` or `legacy_trait_raw`. |
| NCBI dbSNP frequency exports | Population frequency | Three dbSNP frequency archive batches plus separately provenanced HBP legacy rows are normalized into the population-frequency datamart. Source-specific study and population labels remain separate. |
| NCBI dbVar nstd102 and nstd229 | Structural variants | nstd102/ClinVar structural-variant seed records and the nstd229/TOPMed call set are published in the gene-centered SV contract. |
| HGNC | Gene profiles | Approved symbols, names, aliases, locus information, HGNC identifiers, and external cross-references. |
| NCBI Gene | Gene profiles | Long gene summaries and NCBI identifiers. |
| UniProtKB | Gene and protein profiles | Reviewed protein names, accessions, lengths, function text, and cross-references. Reactome identifiers currently arrive through UniProtKB xrefs rather than a separate Reactome import. |
| Gene Ontology Annotation (GOA) | Gene profiles | GO terms and evidence-aware annotation records. |
| Ensembl REST | Protein context | Gene, transcript, translation, exon, canonical-isoform, and protein-coordinate context. |
| EMBL-EBI Proteins API | Protein context | Protein sequence features and annotations linked through UniProt accessions. |
| InterPro | Protein context | Protein domains, families, motifs, sites, and region annotations. |
| NCBI GEO: GSE232911, GSE29532, and GSE7084 | Expression v3 | Curated disease-versus-control differential-expression rows. Study accession, platform, tissue, contrast, sample counts, effect size, p-values, and processing provenance are retained. |
| Legacy CardioQuilt expression payload | Legacy-compatible expression | Imported and repackaged per gene for compatibility; it remains distinct from expression v3. |
| Open Targets Platform GraphQL API | Drugs and compounds | Target, disease, mechanism, indication, trial, and molecule evidence imported into gene-drug payloads. |
| DrugBank 5.1.12 licensed academic dataset | Drugs and compounds | Licensed molecule, target, action, pharmacology, classification, pathway, and identifier fields. Raw DrugBank files are not redistributed. |
| Official ACC/AHA and ESC guideline documents | Clinical guideline context | Processed by the external HCG/HCG-KG projects and transferred to HBP as versioned graph artifacts. DataHub does not perform the guideline extraction itself. |

## Association evidence

The current association build combines MVP summary-statistics rows with
versioned HBP legacy CVD and trait snapshots derived from NHGRI-EBI GWAS
Catalog records. Representative deployed
`variant_index` artifacts record these source labels:

- `million_veteran_program`
- `legacy_cvd_raw`
- `legacy_trait_raw`

The two legacy labels identify DataHub ingestion routes, not separate upstream
databases. Their upstream source is the NHGRI-EBI GWAS Catalog; DataHub ingests
versioned local snapshots rather than downloading the catalog again during each
association build.

The artifacts retain source-file provenance and phenotype keys. Fields such as
rsID, variation type, consequence, clinical significance, ancestry, and p-value
can be supplied by those upstream rows. The presence of a dbSNP, ClinVar, or
Ensembl source manifest does not mean every current association row was freshly
queried from that service.

## Population-frequency evidence

The verified 2026-05-09 population-frequency handoff contains:

- 594,285,057 source-specific frequency observations
- 18,097,122 distinct rsIDs
- 37 source-provided study labels
- three NCBI dbSNP frequency archive batches
- 7,893 separately provenanced legacy observations covering 228 rsIDs

The study labels carried by dbSNP include collections such as ALFA, gnomAD,
TOPMed, 1000 Genomes, PAGE, ExAC, HapMap, HGDP-CEPH, SGDP, 38KJPN, and regional
studies. HBP imported these as records from the dbSNP frequency export; this does
not imply that DataHub independently downloaded and harmonized every upstream
study.

See [Population Frequency Schema](../schemas/population_frequency.md) for the
complete 37-label list, fields, and interpretation rules.

## Structural-variant evidence

The structural-variant layer uses:

- NCBI dbVar nstd102 / ClinVar structural-variant seed data
- NCBI dbVar nstd229 / TOPMed structural-variant call-set data

The same event can occur in more than one gene payload when it overlaps multiple
genes. Source presence is not a pathogenicity claim.

## Gene and protein context

The gene-profile build directly records snapshot paths for HGNC, NCBI Gene,
reviewed UniProtKB, GOA, and the HBP protein-context output. The protein-context
pipeline uses Ensembl, EMBL-EBI Proteins, and InterPro APIs, with UniProt
accessions as a principal cross-reference.

Human Protein Atlas, ClinGen, Reactome, and other registered resources are not
separate inputs to the current gene-profile build. Reactome IDs shown in gene
profiles are currently carried through UniProtKB cross-references.

## Expression evidence

Expression v3 currently contains three NCBI GEO studies:

| Source database | Study accession |
| --- | --- |
| GEO | GSE232911 |
| GEO | GSE29532 |
| GEO | GSE7084 |

The expression v3 manifest reports 518,987 differential-expression rows,
336,001 gene-phenotype summary rows, and seven normalized phenotypes. Legacy
CardioQuilt-compatible expression is served separately and must not be described
as part of the expression v3 evidence contract.

ArrayExpress, Expression Atlas, GTEx, and single-cell resources are not inputs
to the current expression v3 build.

## Drugs, compounds, and guidelines

The drugs and compounds layer combines imported Open Targets Platform GraphQL
records with licensed DrugBank 5.1.12 records. A payload marked `merged`
contains complementary fields from both sources for the same gene-molecule
entry; it is not a third database or a pooled evidence score.

Clinical guideline context comes from official ACC/AHA and ESC documents through
versioned HCG/HCG-KG graph exports. Recommendation class and level of evidence
are source-guideline metadata. Extraction confidence, when present, describes
the extraction or relationship-assignment process and is not a clinical evidence
grade.

## Derived layers

Cross-phenotype relationships / shared genetic architecture is derived from
DataHub association and variant-index artifacts. It does not introduce another
external database.

## Developer source registry

The files under
[`config/sources/`](https://github.com/HeartBioPortal/DataHub/tree/main/config/sources)
include both active integrations and catalog-only roadmap candidates. They are
useful for engineering planning, but catalog-only entries are deliberately not
listed above as HBP data sources.

The broader
[`DATA_SOURCES.tsv`](https://github.com/HeartBioPortal/DataHub/blob/main/DATA_SOURCES.tsv)
also contains release candidates and entries awaiting source/version/license
confirmation. It must not be used alone to claim production inclusion.
