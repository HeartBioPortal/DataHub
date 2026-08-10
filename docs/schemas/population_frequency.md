# Population Frequency Schema

The population-frequency datamart stores source-specific allele-frequency
observations imported from NCBI dbSNP frequency archives, with optional legacy
HBP rows retained as a separately provenanced source. One row is one reported
rsID/study/population/allele observation, not one unique variant and not one
participant.

## Scientific boundary

These records contain no disease or phenotype association field. HBP first uses
association artifacts to select rsIDs for the active gene, phenotype, and
p-value filters, then joins those rsIDs to this datamart. Any phenotype shown
beside a frequency value is association context attached by the application; it
was not measured as part of the population-frequency observation.

Allele frequency is not a patient count, case/control frequency, ancestry-specific
disease risk, or whole-gene population distribution.

## Primary table: `dbsnp_frequency_records`

| Field | Meaning |
| --- | --- |
| `rsid` | dbSNP rsID. Required in the current index. |
| `study` | Source-provided study/resource label. |
| `population` | Source-provided population or cohort label. |
| `population_group` | Source-provided group field. It is not a DataHub-harmonized ancestry class. |
| `sample_size` | Source-provided sample size or denominator where available; interpretation can differ by source. |
| `ref_allele`, `alt_allele` | Parsed reference and alternate allele strings. |
| `ref_frequency`, `alt_frequency` | Parsed frequencies on the 0-1 scale when valid. |
| `ref_allele_raw`, `alt_allele_raw` | Original source strings retained for audit and parser recovery. |
| `bioproject_id`, `biosample_id` | NCBI project/sample identifiers when present. |
| `source_system` | Import family, currently `ncbi_dbsnp_frequency` or `hbp_legacy_dbsnp`. |
| `source_archive`, `source_member` | Archive and per-rsID member provenance. |
| `source_url` | Source URL when supplied. |
| `ncbi_build` | Source build label. |
| `released` | Source release value. |
| `organism` | Source organism label. |
| `position` | Source position text. It is retained as text because source formatting is not uniform. |
| `variation_type` | Source variation type when present. |

Rows from different studies or population definitions are not silently merged.
A UI may group them for navigation, but source-specific rows remain the
scientific evidence unit.

## Derived frequency measures

The datamart stores REF AF and ALT AF. It does not store a generic `frequency`
or a precomputed MAF column. Consumers may calculate:

```text
MAF = min(ref_frequency, alt_frequency)
```

only when both values are present and each is between 0 and 1. For multiallelic
or partial records, `ref_frequency + alt_frequency` need not equal 1; such rows
must not be forced into a biallelic MAF interpretation.

## Summary and metadata objects

| Object | Meaning |
| --- | --- |
| `dbsnp_frequency_rsid_summary` | Per-rsID row, study, population, group, source, sample-size, and ALT-AF range summary. |
| `dbsnp_frequency_population_summary` | Per rsID + study + population + group + ALT allele summary; it does not combine unrelated source cohorts. |
| `dbsnp_frequency_sources` | Per imported archive/source-system load counts. |
| `dbsnp_frequency_build_metadata` | Build time, paths, total observations, distinct rsIDs, and serialized source summaries. |

## Verified production snapshot

The build metadata in the current production handoff
(`datamart/dbsnp_frequency.next.duckdb`, built 2026-05-09) reports:

- 594,285,057 frequency-observation rows
- 18,097,122 distinct rsIDs
- 37 distinct source-provided study labels
- three NCBI dbSNP archive batches
- 7,893 separately provenanced legacy rows covering 228 rsIDs

??? note "Study labels in the verified 2026-05-09 build"
    These labels are preserved from the dbSNP frequency export. They name
    studies or source collections, not 37 mutually exclusive ancestry groups:

    - 1000Genomes
    - 1000Genomes_30X
    - 38KJPN
    - A Vietnamese Genetic Variation Database
    - ALFA
    - Allele Frequency Aggregator
    - Ancient Sardinia genome-wide 1240k capture data generation and analysis
    - Chileans
    - CNV burdens in cranial meningiomas
    - ExAC
    - FINRISK
    - Genetic variation in the Estonian population
    - Genographic Project
    - Genome of the Netherlands Release 5
    - Genome-wide autozygosity in Daghestan
    - gnomAD - Exomes
    - gnomAD - Genomes
    - gnomAD v4 - Exomes
    - gnomAD v4 - Genomes
    - GO Exome Sequencing Project
    - HapMap
    - HGDP-CEPH-db Supplement 1
    - Korean Genome Project
    - Korean Genome Project 4K
    - KOREAN population from KRGDB
    - Medical Genome Project healthy controls from Spanish population
    - MxGDAR/Encodat-PGx
    - Northern Sweden
    - PharmGKB Aggregated
    - Qatari
    - SGDP_PRJ
    - Siberian
    - The Avon Longitudinal Study of Parents and Children
    - The Danish reference pan genome
    - The PAGE Study
    - TopMed
    - UK 10K study - Twins

These are release/build statistics, not stable schema constants. A future build
must report its own counts from `dbsnp_frequency_build_metadata`.
