# DataHub Provenance Schema

DataHub artifacts preserve enough provenance to trace an HBP field back to the source family, source version, access method, input record, and transformation.

## Standard fields

| Field | Description |
| --- | --- |
| `source_name` | Human-readable source name. |
| `source_version` | Source release, database version, or API version. |
| `source_url_or_endpoint` | Source URL, FTP path, API endpoint, or DOI. |
| `access_method` | Download, REST API, GraphQL API, licensed local file, or derived DataHub artifact. |
| `accessed_at` | API access or web access date/time. |
| `downloaded_at` | File download date/time. |
| `license` | Source license or terms label. |
| `input_file` | Raw or prepared input path when file-based. |
| `input_record_id` | Stable input record identifier or row key. |
| `source_record_id` | Source database identifier. |
| `source_dataset` | Dataset, study, cohort, or source subcollection. |
| `cohort_or_study` | Cohort, study, submission, or trial label. |
| `population_label` | Original population label. |
| `population_group` | Harmonized population group when available. |
| `sample_size` | Sample count, allele count, or source-provided denominator. |
| `genome_build` | Genome assembly, such as GRCh38. |
| `transformation` | DataHub transformation label. |
| `normalization_steps` | Summary of normalization applied. |
| `hbp_artifact` | Generated HBP artifact path or family. |
| `hbp_build_version` | HBP release/build version, for example `3.0.0-nar`. |

## Drug records

Drug-discovery payloads also preserve:

- `molecule_source`
- `molecule_id`
- `target_id`
- `source_action_type`
- `source_indication`
- `source_trial_phase_or_status`
- `source_license`

For Open Targets records, also preserve the GraphQL endpoint, query, variables, access date, and source field names where available. For DrugBank-derived records, preserve the DrugBank release version and non-commercial/license-restricted status.

## Structural variants

Structural-variant payloads also preserve:

- `source_db`
- `study_or_submission`
- `sv_id`
- `sv_type`
- `coordinates`
- `event_length`
- `clinical_significance`
- `gene_overlap`
- `transcript_overlap`
- `exon_overlap`
- `zygosity_if_available`

## Population frequencies

Population-frequency rows preserve:

- `rsid`
- `allele`
- `frequency`
- `population_label`
- `population_group`
- `sample_size`
- `study` or `resource`
- `BioProject` or `BioSample` if available
- `build`
- `provenance`
