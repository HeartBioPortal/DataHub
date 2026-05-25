# DataHub Agent Notes

DataHub is the long-term data management layer for HeartBioPortal. It owns raw
data organization, preparation, canonical association tables, analyzed artifacts,
secondary analyses, serving DuckDBs, and data documentation.

This repo is nested inside the backend tree but is treated as its own
repo. Check git status here separately.

## Core Commands

- Run tests: `PYTHONPATH=src python -m pytest`
- Build docs: `mkdocs build --strict`
- Common script root: `scripts/dataset_specific_scripts/unified`
- Slurm scripts: `scripts/slurm`

Large production jobs run on BigRed through Slurm, not directly on a
login node.

## Documentation Voice

DataHub documentation is written as our own project documentation, not as an
external review memo. Use `we` when a subject is needed, and describe the system
in direct terms: what we ingest, what we preserve, what we generate, what is
validated, and what remains open.

Keep assistant-like or advisor-like phrasing out of README files, docs, audits,
plans, and handoff notes. In particular, keep out "I recommend", "suggested",
"should be", "future improvements include", and similar language unless the
user explicitly asks for a critique. Use present-tense project language:
"we preserve provenance", "v3 uses a curation manifest", "production runs use a
pinned environment", "this remains unresolved".

## Data Model Direction

The intended layered model is:

- `raw_data`: source files organized by database/source/date/version.
- prepared/source-normalized layer: source-specific raw columns are cleaned,
  typed, and normalized enough for ingestion.
- canonical association layer: shared HBP fields such as gene, variant, phenotype
  tree path, dataset type, p-value, ancestry, variation type, consequence, and
  clinical significance.
- export manifest layer: explicit rules for what fields are preserved, promoted,
  derived, published, or copied into serving.
- analyzed artifacts: JSON/JSON.GZ payloads used by frontend/backend.
- serving datamart: DuckDB tables optimized for backend request paths.

Do not collapse source-specific raw data into undocumented generic blobs unless
there is a manifest/provenance trail.

## Association Pipeline

Important scripts:

- `publish_unified_from_duckdb.py`: publishes association artifacts from DuckDB.
- `build_association_serving_duckdb.py`: builds serving DuckDB from published
  artifacts.
- `upgrade_association_serving_duckdb.py`: adds lightweight summary tables to an
  existing serving DB.
- `run_secondary_analyses.py`: generate/apply secondary analyses such as SGA,
  expression, and protein context.

Important publishers/modules:

- `src/datahub/publishers/variant_index.py`
- `src/datahub/publishers/phenotype_rollup.py`
- `src/datahub/publishers/legacy_association.py`
- `src/datahub/export_manifest.py`
- `src/datahub/export_helpers.py`

## Association Aggregation Rules

For VC/MSC/CS/ancestry, the desired scientific contract is unique variant-level
aggregation:

- deduplicate by `variant_id`
- if duplicate variant rows exist, use the representative row with the lowest
  `p_value`
- no-filter aggregation uses the same algorithm as filtered aggregation
- filtered aggregation selects variants by canonical phenotype path

Do not cap filtered counts to overall counts. If monotonicity fails, inspect
whether no-filter and filtered data came from different sources or algorithms.

## Variant Index

Variant index artifacts live under:

```text
association/final/variant_index/CVD/<GENE>.json.gz
association/final/variant_index/TRAIT/<GENE>.json.gz
```

They are intended for backend filtered aggregation. Generation can be expensive
on the full association DB. Jobs are partitioned and checkpointed. Preserve
state dirs when resuming.

Typical HPC source DB:

```text
/N/scratch/kvand/hbp/datamart/mvp_fast.duckdb
```

Typical output root:

```text
/N/scratch/kvand/hbp/analyzed_data_unified
```

## Secondary Analyses

`run_secondary_analyses.py` supports:

- `sga`: shared genetic architecture generated from overlap between CVD and
  trait rsIDs per gene.
- `expression`: imports existing expression JSON into serving DB.
- `protein_context`: protein metadata/context payloads.

SGA generation is partitioned. If a partition is missing, genes assigned to that
partition will have no SGA artifact. The AWS apply step replaces rows; it does
not duplicate records.

## Protein Context

Key files:

- `src/datahub/protein_context.py`
- `src/datahub/secondary_analyses/protein_context.py`
- `src/datahub/apis/ensembl.py`
- `src/datahub/apis/proteins.py`
- `src/datahub/apis/interpro.py`

Protein context enriches the protein consequence viewer with:

- Ensembl gene/transcript/translation IDs
- canonical and protein-coding isoforms
- translation exons and protein features
- RefSeq and UniProt cross-references
- EBI Proteins features
- InterPro domains, motifs, families, and regions
- source status/provenance per isoform

## Field Provenance

For legacy association files:

- `variation_type` comes from `dbsnp.vartype`
- `clinical_significance` comes from `clinvar.rcv.clinical_significance`
- `most_severe_consequence` comes from `snpeff.ann.effect`

MVP currently has `variation_type`, but generally does not populate
`clinical_significance` or `most_severe_consequence`.

Be careful with the phrase "most severe consequence": legacy files expose a
SnpEff effect field, but the original undocumented pipeline may not have
documented how multiple transcript effects were collapsed.

## Documentation

DataHub docs are first-class. When changing algorithms, manifests, data contracts,
or pipeline behavior, update docs under `docs/` and run:

```bash
mkdocs build --strict
```
