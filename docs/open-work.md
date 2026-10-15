# Open Work

This page records the data work that remains open for the HBP 3.0 platform,
with the measurements behind each item. Measurements were taken on the
production serving artifacts in October 2026. Builds for these items run on
BigRed through Slurm and are published only after review.

## Significance-aware default view

**Where we are.** The gene dossier opens on all indexed evidence. Under that
scope, phenotype summaries count every reported variant, and MVP source
summaries report each tested variant for many phenotypes. For PCSK9 the most
represented CVD phenotype is hypertension (562 reported variants) under all
indexed evidence and coronary atherosclerosis (26 variants) at p < 5e-8. The
portal offers one-click ranking by genome-wide significance and ranks Lead
variants by p-value under any scope.

Filtered association reads push the p-value threshold into the DuckDB partition
read (see [Association Evidence v2](schemas/association_evidence_v2.md#filtered-reads)).
Cold filtered builds now take 7.2 s for TTN at p < 5e-8, 2.2 s for PCSK9 and
13.0 s for ANK2 at p < 1e-5. The remaining cost is loading each gene's compact
MVP artifact: 2.6 s for TTN and 7.7 s for ANK2.

Of 77,985 genes, 13,279 (17.0%) have at least one variant at p < 5e-8 and
44.6% have one at p < 1e-5.

**Next step.** A compact-entry table holding the MVP entries at p < 1e-4
(about 1.45 million of roughly 3 billion entries, about 110 MB) removes the
whole-artifact load for every filtered request. The estimated build is about
10 CPU-hours, run as 32 Slurm tasks. Estimated cold latency is 1.7–1.9 s for
TTN, 1.3 s for ANK2 at p < 1e-5 and under 2 s for PCSK9. The dossier then opens
at p < 5e-8 when a gene has such variants, otherwise at p < 1e-5, otherwise on
all indexed evidence, with the applied threshold labelled. A per-gene
precomputed summary for those default scopes (about 0.6 GB) brings the landing
view to about 0.1 s.

## Cross-phenotype relationships v2

**Where we are.** The release-wide SGA artifact was built from
`mvp_association_points` in `mvp_fast.duckdb` (44,523 genes). The builder
counts a variant as shared when it appears in both phenotypes, with no p-value
condition, and each phenotype's stored set holds the rsIDs it shares with at
least one phenotype of the other type (see
[SGA semantics](pipelines/secondary-analyses.md#sga-semantics)). Shared counts
in this release therefore describe co-reporting. For PCSK9 the artifact holds
8,248 trait–CVD pairs; about 140 of its 155 CVD phenotype sets each hold
198–229 variants, and hemoglobin levels (21 variants) falls inside all of them.

An in-memory prototype that keeps a variant only when its best p-value clears
the threshold in both phenotypes gives:

| | Current | Both p < 1e-5 | Both p < 5e-8 |
| --- | --- | --- | --- |
| PCSK9 pairs | 8,248 | 32 | 14 |
| PCSK9 top pair | atrial fibrillation ↔ total cholesterol (133) | coronary atherosclerosis ↔ LDL (42) | coronary atherosclerosis ↔ LDL (25) |
| Hemoglobin pairs | 147 | 0 | 0 |

LDLR changes in the same way, from 7,136 pairs to 53.

**Next step.** The v2 builder carries the p-value, keeps a variant only when it
clears the threshold in each phenotype, and emits one record per phenotype pair
with the shared variants, each phenotype's full significant-set size, Jaccard
and overlap coefficient. Output goes to `secondary_analyses/final/sga_v2/` with
a small serving DB, alongside the current artifact, and runs as 32 Slurm
partitions. Before the build we confirm that TRAIT rows in
`mvp_association_points` carry p-values.

## Consequence annotation coverage

**Where we are.** Across 77,985 genes and 36,460,765 gene–variant pairs, 54.8%
of pairs carry a consequence: 10,223 genes are fully annotated, 36,174 partly
and 31,588 not at all. The 16,419,054 rsIDs without a consequence are MVP rows,
which carry no consequence field. Existing consequence labels are legacy
SnpEff-derived values without a recorded version. For PCSK9, 329 of 641
association variants have no consequence.

The August 2025 Ensembl VEP 114 run over 23,425,933 rsIDs already covers
16,152,948 (98.4%) of the unannotated rsIDs, including all 329 for PCSK9.

**Next step.** From the all-transcript VEP output, we store every VEP 114
consequence on the gene's own transcripts, with MANE transcripts flagged and the
source and version recorded, as a new `consequence_annotations` version. No
severity selection is applied; display order is a presentation choice. The
remaining 266,106 rsIDs are positioned from the dbSNP VCF and annotated with
offline VEP on BigRed. The new version is swapped into serving after review.

## Population-frequency coverage

**Where we are.** The dbSNP frequency index holds 594.3 million rows for
18,097,122 rsIDs. On a 1-in-64 gene sample, 55.9% of association rsIDs have
frequency rows; MVP-linked rsIDs are covered more completely than legacy-only
ones (PCSK9: 248 of 329 MVP-only rsIDs against 13 of 81 legacy-only). The
index has rows for 436 of PCSK9's 641 association rsIDs.

The portal looks up at most 80 rsIDs per request, the strongest by p-value (see
[Population Frequency](schemas/population_frequency.md#serving-scope)). The
PCSK9 response for those 80 is 16.6 MB. Of the 5,485 population labels, 5,425
are PharmGKB accession IDs stored by dbSNP in the population field; 60 are
named populations.

**Next step.** We measure the share of PharmGKB accession rows in the payload
and separate them from named-population rows, then page frequency results
beyond the first 80 rsIDs. Missing rsIDs are backfilled from the ALFA release
VCF and the gnomAD v4.1 sites files by rsID on BigRed. The population map
dictionary is completed for all 60 named labels, matching the portal's
ancestry grouping.
