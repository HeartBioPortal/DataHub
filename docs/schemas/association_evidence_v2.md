# Association Evidence v2

Association evidence v2 separates source observations from variant annotations and
visualization summaries. It is an additive DuckDB sidecar; it does not replace the
legacy association serving database during release-candidate validation.

## Scientific entity model

| Table | Unit | Contract |
| --- | --- | --- |
| `variants` | Canonical variant identifier | Stores unordered source allele and coordinate contexts. REF, ALT, and effect allele remain unresolved unless an authoritative association source supplies them. |
| `provider_records` | Exact archived input row | Stable ID from source, logical file, line number, and row checksum. Raw source columns are preserved without cross-row field filling. |
| `association_records` | Source association evidence | Recoverable legacy rows use `record_kind=source_association_observation` and link to exact provider rows. MVP compact rows use `record_kind=source_summary_association`, `evidence_granularity=source_summary`, source `million_veteran_program`, display name `MVP`, and `provider_detail_status=not_applicable`. Neither contract fills absent fields across records. |
| `source_summary_artifacts` | Retained MVP gene/dataset artifact | Registers the immutable compact artifact supplying MVP source-summary associations. Provider-row lineage is not applicable, but every retained variant-phenotype summary is valid association evidence. The gene-keyed serving index preserves deterministic record IDs, exact phenotype paths, p-values, variation type and other retained compact fields without creating study/provider records. |
| `consequence_annotations` | Source/version-scoped annotation | Preserves each consequence/transcript/source combination. The version field is explicit and may be unavailable for legacy snapshots. No lexical minimum or implicit severity selection is performed. |
| `clinical_assertions` | Source clinical assertion | Preserves each source-reported term independently, with condition and version when recoverable. |
| `population_observations` | Source/cohort/allele/build observation | Keeps recoverable association-source frequency fields separate. The API joins the full dbSNP frequency sidecar by rsID and labels the association allele unresolved. |
| `variant_phenotype_summaries` | Dataset type + gene + exact phenotype path + variant ID | Derived visualization summary with source/association counts and minimum reported p-value plus the supplying record ID. |

## Counting and filters

Variant Annotation Composition counts distinct `variant_id` values under the
active CVD/trait phenotype paths and an explicitly selected p-value threshold.
Variation type contributes one derived or unresolved class per variant. A variant
may contribute to more than one consequence or clinical-significance term because
all source-linked annotations are retained.

The same serialized association-filter object is used for summary, detail, and
association export. Provider association rows pass an explicit p-value filter only
when their reported p-value is present and below the threshold. Variant annotations
are version-scoped entities and remain attached to a selected variant rather than being
recast as association measurements. If their exact provider row does not pass the
association p-value filter, API and export bundles retain it only as
`annotation_provenance_only`; it is never presented as a matching association. All
provider-link tables in a bundle resolve to an included provider row.

## Summary count fields

- `source_observation_count` counts exact recoverable provider rows.
- `association_record_count` counts both provider-level legacy associations and first-class MVP source-summary associations.
- `provider_association_record_count` counts only associations with provider-row lineage.
- `source_summary_association_count` counts MVP summary associations; `retained_source_summary_count` remains a compatibility alias during migration.
- `minimum_reported_p_value` is an aggregate;
  `minimum_reported_p_value_association_record_id` and the tied-record ID list identify
  its exact supplier.

## MVP evidence contract

MVP is one source: machine identifier million_veteran_program and display label MVP.
Each natural key consists of source, dataset type, gene, variant ID, and exact
phenotype path, plus retained artifact context. MVP and legacy records remain separate
even when they share a variant and phenotype. Distinct-variant charts may count the
variant once within an active scope, but drill-downs and exports retain every
source-specific record. Missing effect alleles, effects, standard errors, sample
sizes, study ancestry, fine-mapping values, study IDs, and provider rows are recorded
as not provided in the current MVP summary; they do not make MVP evidence unavailable.
Source-summary consequence, clinical-significance, disease/trait, source-context,
ancestry, and metadata fields are exposed explicitly. Any unforeseen compact fields
remain in a small extras object, so mapped values are not duplicated in every row.

## Source completeness

The archived AWS CVD and trait files are GWAS Catalog-derived and preserve exact
provider rows, but their exact upstream snapshot release is unavailable. They do not
contain effect allele, effect size, standard error, sample size, association ancestry,
or fine-mapping fields. Those values and statuses are explicitly unavailable.

Provider-level MVP inputs are not present in the AWS snapshot. MVP records use the
same v2 contract with:

- `provider_detail_status: unavailable`
- a reason and missing-field list
- one `source_summary_artifacts` registry row for each retained gene/dataset
  `variant_index` artifact
- a gene-keyed serving summary with its logical artifact and stable summary ID
- zero reconstructed provider, study, association, consequence, or clinical records

The compact entry is not duplicated into the global association tables. Runtime
responses expose it under `unavailable_provider_summaries`; its compact p-value is
usable only when the retained entry identifies MVP as its representative source.
Compact variation, consequence, and clinical fields are not promoted into v2
annotations because provider/version provenance is unavailable. If a compact-summary
variant also has a recoverable provider/versioned annotation elsewhere in the archived
provider rows, that annotation can be attached by variant and gene and is labeled as
annotation provenance only; it does not become an MVP association record. The immutable
source artifact remains the evidence available for checksum/count verification.

## Build

The production-scale release candidate is built with:

```bash
PYTHONPATH=src .venv/bin/python \
  scripts/dataset_specific_scripts/unified/build_association_evidence_v2.py \
  --cvd-root /data/DataHub/raw_data/cvd \
  --trait-root /data/DataHub/raw_data/trait \
  --variant-index-root /data/DataHub/analyzed_data/association_new/final/variant_index \
  --output-db /path/to/staging/association_evidence_v2.duckdb \
  --checkpoint-path /path/to/staging/checkpoint.json \
  --manifest-path /path/to/staging/manifest.json \
  --release-id RELEASE_ID \
  --threads 4 \
  --memory-limit 8GB \
  --provider-chunk-rows 0 \
  --temp-directory /path/to/staging/duckdb_tmp \
  --progress-interval 1 \
  --log-path /path/to/staging/build.log
```

The command is resumable by source file and derived phase. With
`--provider-chunk-rows 0`, inputs up to 2 GB use DuckDB's direct single-pass insert;
larger inputs are streamed once through CSV-aware one-million-row temporary chunks.
A positive value forces that bounded row count for every provider file. Temporary
chunks are deleted immediately after their transaction commits, and provider IDs
remain based on the original logical path, record ordinal, and canonical row hash.

Derived publication is also bounded and resumable. Provider-to-association
relationships are partitioned by stable association-record hash; variants,
consequences, clinical assertions, and recoverable population observations are
partitioned by stable variant-ID hash. Summary inputs and annotation joins use the
same variant buckets. Each bucket is one atomic transaction and is recorded in a
phase-specific checkpoint, so an interrupted run resumes only uncommitted buckets
without rescanning or deleting completed buckets. MVP compact artifacts are first registered from immutable file metadata without creating provider records. A separate resumable serving step streams each registered artifact and writes only deduplicated source-summary IDs, variant IDs, phenotype paths, representative compact p-values, completeness fields, and artifact provenance into gene-keyed Parquet. Primary-key and serving indexes
are deferred until bulk publication completes; deterministic ID uniqueness is a
release QA gate.

The builder writes a standard sibling `.duckdb.sha256` file from the same full-file digest recorded in the JSON manifest. The serving publisher records this digest so normalized and serving artifacts share one immutable source identity.

The manifest records the
Git base, dirty status, transformation-file checksums, phenotype-tree checksum,
input sizes/checksum status, runtime versions, counts, output checksum, and exact
command. Input checksums are optional because requesting them performs a second
full read of large source files.

## Bounded serving package

The backend does not use global joins across the production-scale sidecar during
requests. After the normalized database passes build QA, create a separate immutable
serving package:

```bash
PYTHONPATH=src .venv/bin/python \\
  scripts/dataset_specific_scripts/unified/build_association_evidence_v2_serving.py \\
  --source-db /path/to/staging/association_evidence_v2.duckdb \\
  --intermediate-root /path/to/staging/duckdb_tmp \\
  --output-root /path/to/staging/association_evidence_v2_serving \\
  --coarse-serving-root /path/to/staging/association_evidence_v2_serving_256 \\
  --threads 1 \\
  --memory-limit 8GB \\
  --variant-bucket-characters 3 \\
  --verbose
```

Then build the first-class MVP source-summary association index from the registered immutable artifacts:

```bash
PYTHONPATH=src .venv/bin/python \
  scripts/dataset_specific_scripts/unified/build_association_evidence_v2_source_summary_index.py \
  --source-db /path/to/staging/association_evidence_v2.duckdb \
  --association-artifact-root /data/DataHub/analyzed_data/association_new/final \
  --serving-root /path/to/staging/association_evidence_v2_serving \
  --workers 3 \
  --progress-interval 1000 \
  --verbose
```

This step is resumable per registered artifact. It validates input size and modification time, preserves one stable source-summary ID per source/dataset/gene/variant/phenotype/artifact identity, and records selected and deduplicated row counts plus per-file SHA-256 checksums. It does not infer studies, alleles, sample sizes, effects, ancestries, or fine-mapping fields.

After the complete 2.7 source-summary index verifies, build the default-search rollups:

~~~bash
PYTHONPATH=src .venv/bin/python \
  scripts/dataset_specific_scripts/unified/build_association_evidence_v2_source_summary_rollup.py \
  --serving-root /path/to/staging/association_evidence_v2_serving \
  --threads 2 \
  --memory-limit 4GB \
  --progress-interval 1 \
  --verbose
~~~

The rollup advances the serving schema to **2.10.0-rc2** and writes two immutable,
gene-keyed physical projections. **source_summary_association_base_by_gene**
contains one row per gene, dataset type, and variant for default chart summaries.
**source_summary_association_phenotype_counts_by_gene** contains one row per gene,
dataset type, and exact phenotype path, including the exact sorted variant-ID set.
Those sets permit a deduplicated union with recoverable provider evidence; counts
must never be added across the two sources. The rollups are query accelerators only.
The complete source-summary index remains authoritative for phenotype and explicit
p-value filters, row drill-downs, exports, and source provenance.


The serving manifest records table paths, partition keys, bucket functions,
row/file counts, runtime versions, and known limitations. Request-time projections
include `summary_base_by_gene`, `consequence_annotations_by_gene`, and
variant-keyed phenotype summaries, association records, provider records,
clinical assertions, population observations, and provenance partitions.

The production release candidate uses a three-character SHA-256 hexadecimal
prefix for provider, association, phenotype-summary, consequence, clinical, and
population records, yielding 4,096 variant buckets.
The publisher reads each verified two-character coarse partition once, writes
its 16 fine partitions under one temporary unit, and atomically checkpoints the
completed coarse unit. It does not rescan the normalized database for every
fine bucket. An interrupted run removes only its incomplete temporary unit and
resumes after the last committed coarse bucket.
Each table declares its own prefix width in the manifest; gene-scoped and reused
annotation projections retain their declared two-character prefix. The backend
must read the table-specific value rather than assume one global bucket width.

Summary requests read one gene bucket, apply exact phenotype paths and an explicit
p-value threshold, and count distinct variant IDs. Detail and export requests read
only selected variant buckets. The backend must be configured with both the
normalized database and serving manifest.

Provider rows lacking `variant_id_raw` remain preserved in the normalized sidecar but are excluded from variant-keyed serving partitions. The serving manifest records the normalized total, excluded count, and exclusion reason; no unkeyed row is silently presented as variant evidence.


## Full runtime count audit

The portal-wide comparison must include MVP source-summary association membership
without converting compact entries into association records. Run the resumable audit
in staging:

~~~bash
PYTHONPATH=src .venv/bin/python \
  scripts/dataset_specific_scripts/unified/audit_association_evidence_v2_release.py \
  --v2-db /path/to/staging/association_evidence_v2.duckdb \
  --v1-overall-root /data/DataHub/analyzed_data/association_new/final/overall \
  --v1-variant-index-root /data/DataHub/analyzed_data/association_new/final/variant_index \
  --artifact-root /data/DataHub/analyzed_data/association_new/final \
  --source-summary-serving-root /path/to/staging/association_evidence_v2_serving \
  --output-dir /path/to/staging/release-audit \
  --progress-interval 1000 \
  --workers 3
~~~

When a completed schema 2.8 serving root is supplied, the audit verifies and
reuses its exact gene/dataset/variant rollup in 256 resumable gene buckets. A compatible
schema 2.7 package falls back to the complete gene-keyed source-summary index. Without
either serving package, the audit streams every registered compact JSON array once.
All modes store only distinct `(gene, dataset_type, variant_id)` membership in a
resumable staging DuckDB, and
combines that membership with recoverable provider summaries. Variation type comes
only from normalized provider allele contexts; consequence and clinical categories
come only from source/versioned provider annotations. Compact representative
annotation fields are never counted as v2 annotations.

## Migration and rollback

1. Build and validate the sidecar outside `datamart/`.
2. Compare every v1/v2 chart category and document expected semantic differences.
3. Test the backend with `HBP_ASSOC_EVIDENCE_V2_PATH` and
   `HBP_ASSOC_EVIDENCE_V2_SERVING_MANIFEST` pointing to staged artifacts.
4. Copy the verified immutable sidecar and complete serving package to one versioned
   release path.
5. Set `HBP_ASSOC_EVIDENCE_MODEL=v2`, the versioned sidecar path, and versioned
   serving-manifest path during an approved deployment.
6. Retain the v1 serving DB and release manifest.

Rollback does not transform data. Set `HBP_ASSOC_EVIDENCE_MODEL=v1` (or restore
the prior service environment) and restart only during an approved deployment
window. The legacy serving DB remains intact.

## Known limitations

- Association REF/ALT/effect-allele identity is unresolved in current AWS inputs.
- Full dbSNP population observations are joined by rsID only; frequency-source
  REF/ALT/build values are not confirmed as the tested association allele.
- Legacy SnpEff- and ClinVar-derived fields do not retain authoritative upstream
  release versions in the archived rows.
- MVP provider-level study rows cannot be reconstructed from compact summaries.
- This sidecar does not claim that the entire production portal can be rebuilt
  from public inputs with one command.
