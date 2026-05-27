# Expression v3 Schema

Documentation schema for the HeartBioPortal expression v3 differential-expression
datamart and exported artifacts.

Expression v3 is a curated public-source expression layer. The current build
path starts with approved GEO study contrasts and writes row-level
differential-expression evidence, gene-by-phenotype summaries, JSON serving
artifacts, and an optional DuckDB datamart. Future imports can add Expression
Atlas or ArrayExpress evidence under the same contract. GTEx belongs to baseline
normal-tissue expression unless a separate disease/control contrast is provided.

## Output Tables

The DuckDB export contains three tables:

| Table | Meaning |
| --- | --- |
| `expression_differential_results` | Row-level evidence. One row represents one tested gene-like feature in one study contrast for one phenotype label. |
| `expression_gene_phenotype_summary` | Portal-oriented aggregation. One row summarizes one gene-like feature against one phenotype across included studies. |
| `expression_pipeline_metadata` | Build metadata, thresholds, output paths, and limitations for the run. |

CSV and JSON outputs mirror these concepts under
`secondary_analyses/final/expression_v3/`.

## `expression_differential_results`

Each row is the most detailed evidence unit we expose from expression v3. In the
GEO/limma path, it means a gene/probe/platform feature was tested in a
disease-vs-control contrast for one approved GEO study.

| Field | Description |
| --- | --- |
| `gene_id` | Primary identifier for the tested feature. For portal-facing gene summaries we use HGNC-compatible gene identifiers once mapping is available. In early builds this field can still contain a platform probe or transcript-cluster identifier. |
| `gene_symbol` | Display gene symbol for the tested feature. We use this for portal search only after gene mapping has been reviewed. |
| `gene_id_source` | Source of `gene_id`, such as `symbol`, `hgnc`, `ensembl`, or `platform_annotation`. |
| `study_accession` | Source study accession, for example a GEO `GSE` accession. |
| `source_database` | Source database, such as `GEO`, `Expression Atlas`, or `ArrayExpress`. |
| `source_url` | Stable URL for the source study or source result. |
| `assay_type` | Assay/input class, such as `microarray_or_processed_matrix`, `rnaseq_or_processed_matrix`, `miRNA_microarray`, or `circRNA_microarray`. |
| `platform` | Source platform identifier when available, such as a GEO `GPL` accession. |
| `species` | Species for the samples. Production CVD disease/control counts should be human unless a specific non-human layer is intentionally added. |
| `tissue` | Tissue label from source metadata or curation. |
| `cell_type` | Cell type label when available, for example monocytes or peripheral blood cells. |
| `disease_id` | Controlled disease or phenotype identifier when available. We preserve blanks until a controlled mapping is confirmed. |
| `disease_name` | Human-readable disease or phenotype label used for display and summaries. |
| `phenotype_label_original` | Original curated or source phenotype label before normalization. |
| `phenotype_label_normalized` | Machine-friendly phenotype label used for grouping, usually lowercase with underscores. |
| `contrast_name` | Name of the analyzed contrast, usually `case_vs_control`. |
| `case_group_label` | Source or curated label for the disease/case group. |
| `control_group_label` | Source or curated label for the control group. |
| `n_case` | Number of case samples included in the contrast. |
| `n_control` | Number of control samples included in the contrast. |
| `log2_fold_change` | Estimated log2 fold change for case relative to control. Positive values mean higher expression in the case group. |
| `p_value` | Raw p-value from the differential-expression test when available. |
| `adjusted_p_value` | Multiple-testing adjusted p-value. We use this for significance calls. |
| `fdr_method` | Multiple-testing correction method, default `BH` for Benjamini-Hochberg FDR. |
| `direction` | Direction call: `up`, `down`, or `not_significant`. |
| `significance_threshold` | Adjusted p-value threshold used for this row, currently `0.05` by default. |
| `analysis_method` | Analysis method or importer, for example `GEOquery_limma` or `expression_v3_public_geo`. |
| `analysis_package_version` | Package versions used by the analysis when captured, such as limma and GEOquery versions. |
| `preprocessing_method` | Source or pipeline preprocessing notes, such as normalization method. |
| `covariates_used` | Model covariates if included. Empty means no covariates were recorded for the contrast. |
| `batch_correction_method` | Batch correction method if applied or recorded. |
| `date_processed` | Date the row was generated or imported. |
| `provenance_hash` | Deterministic hash for tracing the row identity across rebuilds. |
| `quality_score` | Optional quality score reserved for curation or source-quality scoring. |
| `notes` | Free-text curation or processing notes. |

## Direction Semantics

`up` and `down` are always interpreted relative to the stated contrast.

For the standard `case_vs_control` contrast:

- `up`: expression is higher in the disease/case group and passes the adjusted
  p-value threshold.
- `down`: expression is lower in the disease/case group and passes the adjusted
  p-value threshold.
- `not_significant`: the gene-like feature was tested but did not pass the
  configured threshold.

We do not use `unregulated` for this category. The scientifically safer label
is `not significant` or `not significantly differentially expressed`.

## `expression_gene_phenotype_summary`

This table is the compact portal-facing summary. Each row summarizes all
included row-level evidence for one gene-like feature and one normalized
phenotype.

| Field | Description |
| --- | --- |
| `gene_id` | Gene or feature identifier being summarized. |
| `phenotype_id` | Normalized phenotype identifier. In current builds this is the normalized label when no ontology ID is available. |
| `phenotype_name` | Human-readable phenotype name selected from contributing rows. |
| `number_of_studies_upregulated` | Count of distinct source studies where the feature is significantly upregulated. |
| `number_of_studies_downregulated` | Count of distinct source studies where the feature is significantly downregulated. |
| `number_of_studies_not_significant` | Count of distinct source studies where the feature was tested but not significant. |
| `number_of_total_studies` | Count of distinct source studies contributing evidence for this gene/phenotype pair. |
| `direction_consistency_score` | Direction agreement among significant studies. `1.0` means all significant evidence points in the same direction; `0.0` means no significant directional evidence or balanced conflict. |
| `median_log2_fold_change` | Median log2 fold change across contributing rows with effect-size values. |
| `minimum_adjusted_p_value` | Smallest adjusted p-value among contributing rows. |
| `evidence_strength` | Coarse summary label based on study count and direction consistency: `limited`, `moderate`, or `strong`. |
| `source_study_accessions` | Source accessions behind the summary row. |

## `expression_pipeline_metadata`

This table stores build-level metadata as key-value rows.

| Field | Description |
| --- | --- |
| `key` | Metadata key, such as `analysis_id`, `pipeline_version`, `row_count`, `study_count`, or `thresholds`. |
| `value` | JSON-encoded or string value for the metadata key. |

Important metadata entries include:

- `analysis_id`
- `pipeline_name`
- `pipeline_version`
- `built_at`
- `row_count`
- `summary_row_count`
- `gene_count`
- `phenotype_count`
- `study_count`
- `thresholds`
- `outputs`
- `limitations`

## Curation Requirements

Expression v3 is intentionally curation-gated. A study should not enter the
production evidence layer unless the curation manifest confirms:

- source study accession and URL
- human disease/control contrast
- case and control sample accessions
- sample sizes
- tissue and cell type when available
- assay type and platform
- phenotype label and normalized phenotype path
- contrast direction
- analysis method and threshold

Automated curation suggestions can fill likely values from GEO sample metadata,
but they do not approve studies by themselves.

## Current Production Readiness Checks

Before a build is wired into the portal, we check:

- every displayed count traces back to source study accessions
- `up` and `down` calls match the sign of `log2_fold_change`
- significant directions pass the row threshold
- `not_significant` rows do not pass the row threshold
- sample sizes are populated
- disease/control direction is explicit
- duplicate platform probes are collapsed or intentionally retained
- platform probe IDs are mapped to HGNC-compatible gene symbols or excluded from
  portal gene-level summaries
- overlapping phenotype labels are collapsed to the intended portal phenotype

The first v3 DuckDB builds are useful proof-of-pipeline artifacts. We treat
them as production candidates only after phenotype normalization and probe/gene
mapping have been reviewed.

## Related Files

- `config/secondary_analyses/expression_v3.json`
- `config/schemas/expression_differential_result.schema.json`
- `src/datahub/expression/records.py`
- `src/datahub/expression/curation.py`
- `scripts/dataset_specific_scripts/expression/README.md`
