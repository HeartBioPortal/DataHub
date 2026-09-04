# Data Sources

This page maps the evidence shown in HeartBioPortal to its upstream source and
to the DataHub transformation that produces the displayed value. It lists only
sources demonstrably represented in current artifacts or active pipelines.
Configuration entries that are only roadmap candidates are excluded.

!!! important "Source data and derived views are different"
    A visualization can combine source-reported fields with HBP-derived
    normalization, filtering, or aggregation. The source column below identifies
    where the underlying observation or annotation originated; the transformation
    column identifies what HBP calculates for display. A source being registered
    under `config/sources/` does not prove that it contributes to a release.

## Visualization-to-source crosswalk

| Portal visualization or section | Upstream source data | Source fields used | HBP/DataHub transformation and serving layer |
| --- | --- | --- | --- |
| **Gene Dossier** | [HGNC](https://www.genenames.org/), [NCBI Gene](https://www.ncbi.nlm.nih.gov/gene/), reviewed [UniProtKB](https://www.uniprot.org/), and [GOA](https://www.ebi.ac.uk/GOA/) | Approved symbol/name, aliases, locus type, identifiers, NCBI summary, protein name/function/length, GO annotations, and UniProt cross-references | DataHub resolves the requested symbol against HGNC, selects the documented summary fallback, and publishes one `gene_profile.v1` record per approved HGNC entry. Reactome IDs shown here are UniProtKB cross-references, not a separate Reactome import. |
| **Clinical Signal / Guideline Intelligence** | Official [ACC/AHA](https://www.acc.org/guidelines) and [ESC](https://www.escardio.org/Guidelines) clinical documents | Source document metadata, recommendations, supporting excerpts, recommendation class/level when stated, and linked clinical entities | HCG/HCG-KG extracts and represents the document content as a versioned graph; HBP projects gene-centered summaries and details. DataHub documents the handoff but does not perform the PDF extraction. |
| **Association Signal and phenotype summaries** | Authorized Million Veteran Program (MVP) association summaries and NHGRI-EBI [GWAS Catalog](https://www.ebi.ac.uk/gwas/)-derived legacy CVD/trait snapshots | Gene, rsID/variant ID, exact phenotype context, p-value, study/PMID and other fields when retained by the source artifact | DataHub keeps MVP source-summary associations and legacy source observations separate. Distinct-variant cards count an exact variant ID once within the active phenotype and p-value scope; this is not a meta-analysis or evidence-quality score. |
| **Variant type annotations** | NCBI [dbSNP](https://www.ncbi.nlm.nih.gov/snp/) fields embedded in legacy association snapshots | `dbsnp.vartype` | DataHub normalizes labels for chart display while retaining source records in the association evidence model. MVP summaries do not receive a fabricated variant type when one was not retained. |
| **Molecular consequence annotations** | [SnpEff](https://pcingola.github.io/SnpEff/) annotation fields embedded in the GWAS Catalog-derived legacy snapshots; Ensembl consequence fields in the separate Ensembl-derived stream | Legacy: `snpeff.ann.effect` and `snpeff.ann.putative_impact`; Ensembl stream: `most_severe_consequence` and `consequence_terms` | Association summaries normalize labels and count distinct variants. Versioned evidence-model records preserve source-specific consequence annotations; HBP does not reinterpret the minimum p-value as consequence severity. |
| **Clinical significance annotations** | Legacy ClinVar-derived fields retained in association snapshots | `clinvar.rcv.clinical_significance` | DataHub parses the audited source strings into 15 exact normalized terms while preserving raw values and provider provenance. These are legacy variant-level classifications and are not automatically the classification for the HBP phenotype currently selected. |
| **Population Frequency** | NCBI dbSNP frequency exports, which carry source-study labels including gnomAD, TOPMed, 1000 Genomes, ALFA, PAGE, ExAC, HapMap, HGDP-CEPH, and others | rsID, study, population/group, sample size, REF/ALT, source-specific frequencies, build, position, release, BioProject/BioSample, and archive/member provenance | HBP first selects association-linked rsIDs, then retrieves source-specific frequency observations. The current linkage is **rsID only**; the tested association allele is unresolved. Rows remain allele-, build-, study-, and population-specific. Approximate map points are curated display centroids, not recruitment locations. |
| **Structural variation** | NCBI [dbVar](https://www.ncbi.nlm.nih.gov/dbvar/) nstd102/ClinVar seed records and nstd229/TOPMed call-set records; Ensembl or a pinned GTF for gene/transcript overlap context | Source variant/accession, coordinates, type, clinical significance where present, and call-set metadata | DataHub publishes gene-centered SV records and transcript/exon context. One event may appear under multiple genes when it overlaps them. Association filters do not turn this independent layer into GWAS evidence. |
| **Protein consequence viewer: lollipops** | SnpEff-annotated legacy GWAS rows, an Ensembl-derived variation stream, and NCBI ClinVar/Variation Viewer molecular-consequence enrichment represented in the inherited viewer artifacts | SnpEff: `snpeff.ann.effect`, `snpeff.ann.hgvs_p`, `snpeff.ann.feature_id`, `snpeff.ann.protein.length`; Ensembl: `consequence_terms`, `protein_start`, `amino_acids`, `protein_id`, `protein_length`; NCBI enrichment: protein HGVS and `Molecularconsequence` | The inherited parser converts protein HGVS to amino-acid position/change and publishes compact per-gene/per-phenotype lollipop rows. The active merged viewer artifact preserves mutation label, amino-acid coordinate, protein/transcript identifier, phenotype, PMID, and counts, but not reliable row-level annotator provenance or rsID for every row. Therefore a displayed point must not be described as uniformly VEP-derived or uniformly SnpEff-derived. |
| **Protein consequence viewer: exon/domain/feature tracks** | [Ensembl REST](https://rest.ensembl.org/), [EMBL-EBI Proteins API](https://www.ebi.ac.uk/proteins/api/doc/), [InterPro](https://www.ebi.ac.uk/interpro/), and UniProt cross-references | Gene/transcript/translation/exon coordinates, canonical transcript, protein length, topology/sequence features, domains, families, motifs, sites, and regions | DataHub builds a separate per-gene `protein_context` payload aligned to protein coordinates. These contextual tracks do not assign the lollipop consequence label; they provide the protein structure/feature context around it. |
| **Cross-phenotype relationships** | The same published MVP and legacy association/variant-index artifacts used by Association Signal | Gene, phenotype paths, and exact variant IDs | DataHub computes shared variant sets, intersection counts, Jaccard similarity, and overlap measures. This is a release-wide precomputed derivative, not a new external association source and not evidence that two phenotypes are causally related. |
| **Expression** | Active legacy-compatible CardioQuilt/CREEDS-GEO payload and the separate expression-v3 candidate built from NCBI GEO studies GSE232911, GSE29532, and GSE7084 | Study accession, platform, tissue, contrast, sample counts, fold change, p-values, processing metadata, and direction where available | The active imported expression layer remains distinct from expression v3. Expression v3 creates row-level differential-expression evidence and gene-phenotype summaries only from curation-approved disease-versus-control contrasts. |
| **Drugs & Compounds** | [Open Targets Platform](https://platform.opentargets.org/) GraphQL API and licensed DrugBank 5.1.12 academic data | Molecule/target identifiers, mechanism/action, indication, trial phase/status, pharmacology, classification, and source-specific descriptions where available | Per-gene records are normalized and may be marked `merged` when the same gene-molecule entry has complementary fields from both sources. `merged` is not a third source or a stronger evidence grade. DrugBank-derived content remains subject to its recorded non-commercial access terms. |

## Association and annotation provenance

### Association sources

The current association build contains three source families:

- `million_veteran_program`: MVP source-summary association evidence
- `legacy_cvd_raw`: CVD snapshots derived from NHGRI-EBI GWAS Catalog records
- `legacy_trait_raw`: trait snapshots derived from NHGRI-EBI GWAS Catalog records

The two `legacy_*` labels name DataHub ingestion routes, not upstream databases.
Their base association records came from GWAS Catalog-derived files and were
previously enriched with fields whose column names identify dbSNP, SnpEff,
ClinVar, and gnomAD provenance.

### Field-level source map for legacy association rows

| Published concept | Retained legacy source field | Source/annotator represented by the field |
| --- | --- | --- |
| Associated gene | `snpeff.ann.gene_id` | SnpEff annotation embedded in the legacy file |
| Variant identifier | `dbsnp.rsid`, falling back to `MarkerID` | dbSNP identifier / source marker identifier |
| Variation type | `dbsnp.vartype` | dbSNP |
| Molecular consequence | `snpeff.ann.effect` | SnpEff |
| Functional impact | `snpeff.ann.putative_impact` | SnpEff |
| Protein feature/transcript | `snpeff.ann.feature_id` | SnpEff annotation target |
| Protein change | `snpeff.ann.hgvs_p` | SnpEff protein HGVS annotation |
| Annotated protein length | `snpeff.ann.protein.length` | SnpEff annotation target metadata |
| Clinical classification | `clinvar.rcv.clinical_significance` | ClinVar-derived legacy annotation |
| Population-frequency columns in legacy files | `gnomad_genome.af.*` | gnomAD fields embedded in the legacy snapshot |
| Association statistic | `pval` | Association source record |
| Study/publication context | `Study`, `studyID`, `PMID`, `StudyGenomeBuild` | GWAS Catalog-derived legacy record |

The source/annotation version is not consistently retained for every legacy
field. In particular, the current artifacts do not establish a single SnpEff,
Ensembl, ClinVar, or gnomAD release for all inherited rows. Documentation and
exports must not imply a release that is absent from provenance.

MVP is represented as first-class source-summary association evidence. Missing
effect alleles, effect sizes, sample sizes, fine-mapping fields, or consequence
annotations remain not provided when those fields were not retained; their
absence does not make the MVP association itself unavailable.

## Protein consequence and protein context

The Protein consequence viewer contains two scientifically different layers:

1. **Variant lollipops** are compact consequence observations positioned on a
   protein/transcript coordinate. In legacy GWAS-derived rows, mutation category
   comes from `snpeff.ann.effect`, amino-acid position/change comes from
   `snpeff.ann.hgvs_p`, and the feature/protein metadata comes from the associated
   SnpEff columns. The inherited viewer also contains Ensembl consequence terms
   and NCBI molecular-consequence enrichment from separate processing paths.
2. **Protein context tracks** are separately derived from Ensembl transcript,
   translation, and exon data, EMBL-EBI Proteins/UniProt features, and InterPro
   entries. They locate domains, motifs, topology, low-complexity regions, exons,
   and related features around the lollipops.

SnpEff and Ensembl are therefore not interchangeable labels on this chart.
SnpEff is an annotation tool represented in legacy association/viewer fields;
Ensembl, EBI Proteins, and InterPro primarily supply the coordinate and feature
context. The term "VEP-style" may describe Sequence Ontology labels, but DataHub
does not have evidence that every active lollipop was produced by Ensembl VEP.

The active compact viewer schema currently includes:

```text
amino_acid, mutation, value, pmid, protein_id, max,
phenotype, index_count, max_count, count
```

Because it does not consistently retain `rsID`, annotation source/version, raw
HGVS, or a provider-record identifier, exact per-point provenance is limited.
Those omissions should be treated as an inherited artifact limitation, not
filled by inference.

See [Protein Context Schema](../schemas/protein_context.md) for the feature-track
contract.

## Population-frequency evidence

The verified 2026-05-09 population-frequency handoff contains:

- 594,285,057 source-specific frequency observations
- 18,097,122 distinct rsIDs
- 37 source-provided study labels
- three NCBI dbSNP frequency archive batches
- 7,893 separately provenanced legacy observations covering 228 rsIDs

The study labels carried by dbSNP include collections such as ALFA, gnomAD,
TOPMed, 1000 Genomes, PAGE, ExAC, HapMap, HGDP-CEPH, SGDP, 38KJPN, and regional
studies. HBP imported these as records from the dbSNP frequency export; DataHub
did not independently download and harmonize each named contributing study.

Population-frequency records have no disease or phenotype field. A phenotype
shown in this portal section is association context used to select rsIDs before
the frequency lookup. It was not measured as part of the frequency observation.
The production linkage is `rsID`-only and the association allele is unresolved;
frequency-source REF, ALT, build, and position values must not be presented as a
confirmed match to the tested association allele.

See [Population Frequency Schema](../schemas/population_frequency.md) for the
complete 37-label list, fields, and interpretation rules.

## Structural-variant evidence

The structural-variant layer uses:

- NCBI dbVar nstd102 / ClinVar structural-variant seed data
- NCBI dbVar nstd229 / TOPMed structural-variant call-set data

DataHub retains the source event identifiers and coordinate fields, then adds
gene/transcript overlap context from a pinned GTF when configured, with Ensembl
REST as an optional fallback. The same event can appear in more than one gene
payload when it overlaps multiple genes. Presence in the layer is not a
pathogenicity or phenotype-association claim.

See [Structural Variant Schema](../schemas/structural_variant.md) for the exact
record and mapping contract.

## Gene-profile sources

The gene-profile v1 build uses versioned snapshots of:

| Source | Contribution |
| --- | --- |
| HGNC | Approved symbols, names, aliases, previous symbols, locus fields, HGNC IDs, and external cross-references |
| NCBI Gene | Long gene summaries and NCBI identifiers |
| reviewed UniProtKB | Protein names, accessions, lengths, function text, GO IDs, and Reactome cross-references |
| GOA | Evidence-aware Gene Ontology annotations |
| HBP protein context | Canonical-isoform length fallback and protein-context availability |

Human Protein Atlas, ClinGen, Reactome, and other registered resources are not
separate inputs to the current gene-profile build. Reactome IDs displayed in the
profile arrive through UniProtKB cross-references.

See [Gene Profile Schema](../schemas/gene_profile.md).

## Expression evidence

Expression v3 contains three curated NCBI GEO studies:

| Source database | Study accession |
| --- | --- |
| GEO | GSE232911 |
| GEO | GSE29532 |
| GEO | GSE7084 |

Its manifest reports 518,987 differential-expression rows, 336,001
gene-phenotype summary rows, and seven normalized phenotypes. The active
legacy-compatible CardioQuilt/CREEDS-GEO payload is imported separately and must
not be described as part of expression v3.

ArrayExpress, Expression Atlas, GTEx, and single-cell resources are not inputs
to the current expression-v3 build. Their appearance in schemas or configuration
as possible future source roles does not establish production use.

See [Expression v3 Schema](../schemas/expression_v3.md).

## Drugs, compounds, and guidelines

The drugs and compounds layer combines imported Open Targets Platform GraphQL
records with licensed DrugBank 5.1.12 records. A payload marked `merged`
contains complementary fields from both sources for the same gene-molecule
entry; it is not a third database, pooled estimate, or evidence-strength score.

Raw DrugBank files are not redistributed. The merged layer records DrugBank's
academic/non-commercial access terms and Open Targets source metadata. See
[Drug Discovery Schema](../schemas/drug_discovery.md).

Clinical guideline context comes from official ACC/AHA and ESC documents through
versioned HCG/HCG-KG graph exports. Recommendation class and level of evidence
are source-guideline metadata. Extraction confidence, when present, describes
the extraction or relationship-assignment process and is not a clinical evidence
grade. See [Guideline Signal Contract](../schemas/guideline_signal.md).

## Derived layers

Cross-phenotype relationships / shared genetic architecture is derived from
published association and variant-index artifacts. For each gene, DataHub
compares phenotype-specific variant-ID sets and reports set-overlap measures.
It does not introduce another external database, pool effect estimates, or infer
causal relationships between phenotypes.

## Source-registry boundary

The files under
[`config/sources/`](https://github.com/HeartBioPortal/DataHub/tree/main/config/sources)
include active integrations and catalog-only roadmap candidates. They are useful
for engineering planning, but catalog-only entries are deliberately excluded
from the tables above.

The broader
[`DATA_SOURCES.tsv`](https://github.com/HeartBioPortal/DataHub/blob/main/DATA_SOURCES.tsv)
also contains release candidates and entries awaiting source, version, or license
confirmation. It must not be used alone to claim production inclusion. Release
manifests, build metadata, active artifact paths, and field-level provenance are
the evidence for inclusion.
