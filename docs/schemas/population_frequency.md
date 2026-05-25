# Population Frequency Schema

Documentation-only schema for HBP 3.0 population-frequency artifacts.

| Field | Description |
| --- | --- |
| `rsid` | dbSNP rsID when available. |
| `variant_id` | HBP or source variant identifier. |
| `allele` | Allele being counted. |
| `frequency` | Allele frequency. |
| `population_label` | Source population label. |
| `population_group` | Harmonized population group. |
| `sample_size` | Source-provided denominator or sample size. |
| `source_name` | Frequency source or resource. |
| `genome_build` | Genome assembly. |
| `provenance` | Source and transformation provenance object. |

Rows preserve source-specific labels and keep distinct cohorts separate unless a provenance trail records the collapse.
